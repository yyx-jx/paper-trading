import WebSocket from "ws";
import type { Agent } from "node:http";
import { createProxyDispatcher, createProxyWsAgent, fetchJsonWithTimeout } from "./network";
import type { BinanceConnectorState, CandleBar, CandleInterval, CandlePoint, SourceHealth } from "../../domain/types";

const BAR_LIMITS: Record<CandleInterval, number> = {
  "1m": 30,
  "5m": 6,
  "1d": 2
};

const INTERVAL_MS: Record<CandleInterval, number> = {
  "1m": 60_000,
  "5m": 5 * 60_000,
  "1d": 24 * 60 * 60_000
};

function emptyStatus(symbol: string): SourceHealth {
  const now = Date.now();
  return {
    source: "Binance",
    symbol,
    state: "reconnecting",
    reconnectCount: 0,
    sourceEventTs: now,
    serverRecvTs: now,
    normalizedTs: now,
    serverPublishTs: now,
    acquireLatencyMs: 0,
    publishLatencyMs: 0,
    frontendLatencyMs: 0,
    message: "Waiting for Binance stream."
  };
}

function emptyBar(interval: CandleInterval, now: number): CandleBar {
  const startTs = Math.floor(now / INTERVAL_MS[interval]) * INTERVAL_MS[interval];
  return {
    interval,
    startTs,
    endTs: startTs + INTERVAL_MS[interval] - 1,
    open: 0,
    high: 0,
    low: 0,
    close: 0,
    volume: 0
  };
}

function createEmptyCandles(now: number) {
  return {
    "1m": [emptyBar("1m", now)],
    "5m": [emptyBar("5m", now)],
    "1d": [emptyBar("1d", now)]
  } satisfies Record<CandleInterval, CandleBar[]>;
}

function roundNumber(value: number, digits = 2) {
  return Number(value.toFixed(digits));
}

function toRecentCandlePoints(bars: CandleBar[]) {
  return bars.map((bar) => ({
    ts: bar.endTs,
    price: bar.close
  }));
}

function normalizeBar(interval: CandleInterval, bar: CandleBar): CandleBar {
  const bucketSize = INTERVAL_MS[interval];
  const startTs = Math.floor(bar.startTs / bucketSize) * bucketSize;
  return {
    interval,
    startTs,
    endTs: startTs + bucketSize - 1,
    open: Number(bar.open),
    high: Number(bar.high),
    low: Number(bar.low),
    close: Number(bar.close),
    volume: roundNumber(Number(bar.volume ?? 0), 6)
  };
}

function normalizeBars(interval: CandleInterval, bars: CandleBar[]) {
  const deduped = new Map<number, CandleBar>();
  for (const bar of bars) {
    const normalizedBar = normalizeBar(interval, bar);
    deduped.set(normalizedBar.startTs, normalizedBar);
  }

  return [...deduped.values()]
    .sort((left, right) => left.startTs - right.startTs)
    .slice(-BAR_LIMITS[interval]);
}

function aggregateBars(interval: "5m", sourceBars: CandleBar[]) {
  const grouped = new Map<number, CandleBar>();
  for (const bar of normalizeBars("1m", sourceBars)) {
    const bucketSize = INTERVAL_MS[interval];
    const startTs = Math.floor(bar.startTs / bucketSize) * bucketSize;
    const existing = grouped.get(startTs);
    if (!existing) {
      grouped.set(startTs, {
        interval,
        startTs,
        endTs: startTs + bucketSize - 1,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: bar.volume
      });
      continue;
    }

    existing.high = Math.max(existing.high, bar.high);
    existing.low = existing.low === 0 ? bar.low : Math.min(existing.low, bar.low);
    existing.close = bar.close;
    existing.volume = roundNumber(existing.volume + bar.volume, 6);
  }

  return normalizeBars(interval, [...grouped.values()]);
}

export class BinanceConnector {
  private ws?: WebSocket;
  private reconnectTimer?: NodeJS.Timeout;
  private restPollTimer?: NodeJS.Timeout;
  private staleTimer?: NodeJS.Timeout;
  private reconnectCount = 0;
  private lastWsMessageAt = 0;
  private readonly listeners = new Set<(state: BinanceConnectorState) => void>();
  private state: BinanceConnectorState;
  private readonly symbolPair: string;
  private readonly proxyDispatcher;
  private readonly proxyWsAgent;

  constructor(
    private readonly config: {
      symbol: string;
      wsUrl: string;
      restUrl: string;
      requestTimeoutMs: number;
      restPollMs: number;
      wsStaleMs: number;
      upstreamProxyUrl?: string;
    }
  ) {
    const now = Date.now();
    this.symbolPair = `${config.symbol.toUpperCase()}USDT`;
    this.proxyDispatcher = createProxyDispatcher(config.upstreamProxyUrl);
    this.proxyWsAgent = createProxyWsAgent(config.upstreamProxyUrl);
    this.state = {
      price: 0,
      candles: [],
      latestTick: {
        ts: now,
        price: 0
      },
      candlesByInterval: createEmptyCandles(now),
      status: emptyStatus(config.symbol)
    };
  }

  start() {
    void this.bootstrapFromRest("Bootstrapping Binance REST snapshot.");
    this.restPollTimer = setInterval(() => {
      void this.pollRestTicker();
    }, this.config.restPollMs);
    this.staleTimer = setInterval(() => {
      this.checkWsStale();
    }, Math.max(Math.floor(this.config.wsStaleMs / 3), 2000));
    this.connect();
  }

  stop() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    if (this.restPollTimer) {
      clearInterval(this.restPollTimer);
      this.restPollTimer = undefined;
    }
    if (this.staleTimer) {
      clearInterval(this.staleTimer);
      this.staleTimer = undefined;
    }
    if (this.ws) {
      this.ws.removeAllListeners();
      this.ws.close();
      this.ws = undefined;
    }
  }

  subscribe(listener: (state: BinanceConnectorState) => void) {
    this.listeners.add(listener);
    listener(this.state);
    return () => {
      this.listeners.delete(listener);
    };
  }

  getState() {
    return this.state;
  }

  private emit() {
    for (const listener of this.listeners) {
      listener(this.state);
    }
  }

  private async bootstrapFromRest(message: string) {
    try {
      const [candles1m, candles1d, pricePayload] = await Promise.all([
        this.fetchKlines("1m"),
        this.fetchKlines("1d"),
        this.fetchJson<{ price: string }>(
          `${this.config.restUrl}/api/v3/ticker/price?symbol=${encodeURIComponent(this.symbolPair)}`
        )
      ]);
      const latestPrice = Number(pricePayload.price);
      const now = Date.now();
      const normalized1m = normalizeBars("1m", candles1m);
      const normalized1d = normalizeBars("1d", candles1d);
      this.state = {
        ...this.state,
        price: latestPrice > 0 ? roundNumber(latestPrice, 2) : this.state.price,
        candlesByInterval: {
          "1m": normalized1m,
          "5m": aggregateBars("5m", normalized1m),
          "1d": normalized1d
        },
        candles: toRecentCandlePoints(normalized1m),
        latestTick: {
          ts: normalized1m.at(-1)?.endTs ?? now,
          price: normalized1m.at(-1)?.close ?? latestPrice ?? this.state.price
        },
        status: {
          ...this.state.status,
          state: this.lastWsMessageAt > 0 ? "healthy" : "reconnecting",
          sourceEventTs: normalized1m.at(-1)?.endTs ?? now,
          serverRecvTs: now,
          normalizedTs: now,
          serverPublishTs: now,
          acquireLatencyMs: normalized1m.at(-1)?.endTs ? Math.max(now - normalized1m.at(-1)!.endTs, 0) : 0,
          publishLatencyMs: 0,
          message
        }
      };
      this.emit();
    } catch (error) {
      this.state = {
        ...this.state,
        status: {
          ...this.state.status,
          state: this.state.price > 0 ? "degraded" : "reconnecting",
          message: error instanceof Error ? error.message : "Failed to bootstrap Binance REST data."
        }
      };
      this.emit();
    }
  }

  private async pollRestTicker() {
    try {
      const [ticker, candle] = await Promise.all([
        this.fetchJson<{ price: string }>(
          `${this.config.restUrl}/api/v3/ticker/price?symbol=${encodeURIComponent(this.symbolPair)}`
        ),
        this.fetchKlines("1m", 1)
      ]);
      const now = Date.now();
      const price = Number(ticker.price);
      const latestBar = candle[0];
      if (latestBar) {
        this.upsertBar("1m", latestBar);
      }
      if (price > 0) {
        this.applyTradeTick(price, 0, now);
      }
      if (this.lastWsMessageAt === 0 || now - this.lastWsMessageAt > this.config.wsStaleMs) {
        this.state = {
          ...this.state,
          status: {
            ...this.state.status,
            state: "degraded",
            sourceEventTs: latestBar?.endTs ?? now,
            serverRecvTs: now,
            normalizedTs: now,
            serverPublishTs: now,
            acquireLatencyMs: latestBar?.endTs ? Math.max(now - latestBar.endTs, 0) : 0,
            publishLatencyMs: 0,
            message: "Binance WebSocket stale; serving REST fallback data."
          }
        };
        this.emit();
      }
    } catch (error) {
      this.state = {
        ...this.state,
        status: {
          ...this.state.status,
          state: this.state.price > 0 ? "degraded" : "reconnecting",
          message: error instanceof Error ? error.message : "Failed to poll Binance REST ticker."
        }
      };
      this.emit();
    }
  }

  private connect() {
    this.state = {
      ...this.state,
      status: {
        ...this.state.status,
        state: "reconnecting",
        reconnectCount: this.reconnectCount,
        message: "Connecting to Binance stream."
      }
    };
    this.emit();

    this.ws = new WebSocket(
      this.config.wsUrl,
      this.proxyWsAgent ? { agent: this.proxyWsAgent as Agent } : undefined
    );
    this.ws.on("open", () => {
      this.state = {
        ...this.state,
        status: {
          ...this.state.status,
          state: "healthy",
          reconnectCount: this.reconnectCount,
          message: "Connected to Binance WebSocket."
        }
      };
      this.emit();
    });

    this.ws.on("message", (buffer) => {
      try {
        const parsed = JSON.parse(buffer.toString()) as {
          data?: Record<string, unknown>;
        };
        const data = parsed.data ?? {};
        const now = Date.now();
        const sourceEventTs = Number(
          data.E ??
            (typeof data.k === "object" && data.k && "T" in data.k ? (data.k.T as number) : now)
        );
        const serverRecvTs = now;
        this.lastWsMessageAt = now;

        if (data.e === "aggTrade") {
          const price = Number(data.p ?? this.state.price);
          const qty = Number(data.q ?? 0);
          this.applyTradeTick(price, qty, sourceEventTs || now);
        } else if (data.e === "kline" && typeof data.k === "object" && data.k) {
          const kline = data.k as {
            t?: number;
            T?: number;
            o?: string;
            h?: string;
            l?: string;
            c?: string;
            v?: string;
            i?: string;
          };
          const interval = kline.i as CandleInterval | undefined;
          if (interval === "1m" || interval === "1d") {
            this.upsertBar(interval, {
              interval,
              startTs: Number(kline.t ?? sourceEventTs),
              endTs: Number(kline.T ?? sourceEventTs),
              open: Number(kline.o ?? 0),
              high: Number(kline.h ?? 0),
              low: Number(kline.l ?? 0),
              close: Number(kline.c ?? 0),
              volume: Number(kline.v ?? 0)
            });
          }
          const close = Number(kline.c ?? this.state.price);
          if (close > 0) {
            this.state.price = roundNumber(close, 2);
            this.state.latestTick = {
              ts: sourceEventTs || now,
              price: roundNumber(close, 2)
            };
          }
        }

        this.state = {
          ...this.state,
          status: {
            source: "Binance",
            symbol: this.config.symbol,
            state: "healthy",
            reconnectCount: this.reconnectCount,
            sourceEventTs,
            serverRecvTs,
            normalizedTs: now,
            serverPublishTs: now,
            acquireLatencyMs: Math.max(serverRecvTs - sourceEventTs, 0),
            publishLatencyMs: 0,
            frontendLatencyMs: 0,
            message: "Receiving Binance live market data."
          }
        };
        this.emit();
      } catch (error) {
        this.state = {
          ...this.state,
          status: {
            ...this.state.status,
            state: "degraded",
            message: error instanceof Error ? error.message : "Failed to parse Binance payload."
          }
        };
        this.emit();
      }
    });

    this.ws.on("close", () => {
      this.scheduleReconnect("Binance stream closed.");
    });

    this.ws.on("error", (error) => {
      this.scheduleReconnect(error.message);
    });
  }

  private checkWsStale() {
    if (!this.ws || this.lastWsMessageAt === 0) {
      return;
    }
    const now = Date.now();
    if (now - this.lastWsMessageAt <= this.config.wsStaleMs) {
      return;
    }
    this.scheduleReconnect("Binance WebSocket became stale.");
  }

  private applyTradeTick(price: number, qty: number, ts: number) {
    if (price <= 0) {
      return;
    }
    this.state.price = roundNumber(price, 2);
    this.state.latestTick = {
      ts,
      price: roundNumber(price, 2)
    };
  }

  private upsertBar(interval: CandleInterval, bar: CandleBar) {
    this.state.candlesByInterval = {
      ...this.state.candlesByInterval,
      [interval]: normalizeBars(interval, [...this.state.candlesByInterval[interval], bar])
    };
    this.syncDerivedCandles();
  }

  private syncDerivedCandles() {
    const normalized1m = normalizeBars("1m", this.state.candlesByInterval["1m"]);
    const normalized1d = normalizeBars("1d", this.state.candlesByInterval["1d"]);
    this.state.candlesByInterval = {
      ...this.state.candlesByInterval,
      "1m": normalized1m,
      "5m": aggregateBars("5m", normalized1m),
      "1d": normalized1d
    };
    this.state.candles = toRecentCandlePoints(normalized1m);
  }

  private async fetchKlines(interval: CandleInterval, limit = BAR_LIMITS[interval]): Promise<CandleBar[]> {
    const url = `${this.config.restUrl}/api/v3/klines?symbol=${this.symbolPair}&interval=${interval}&limit=${limit}`;
    const payload = await this.fetchJson<
      Array<[number, string, string, string, string, string, number, string, number, string, string, string]>
    >(url);
    return normalizeBars(
      interval,
      payload.map((item) => ({
        interval,
        startTs: Number(item[0]),
        endTs: Number(item[6]),
        open: Number(item[1]),
        high: Number(item[2]),
        low: Number(item[3]),
        close: Number(item[4]),
        volume: Number(item[5])
      }))
    );
  }

  private async fetchJson<T>(url: string): Promise<T> {
    try {
      return await fetchJsonWithTimeout<T>(url, this.config.requestTimeoutMs, this.proxyDispatcher);
    } catch (error) {
      throw new Error(`Binance request failed for ${url}: ${error instanceof Error ? error.message : "unknown error"}`);
    }
  }

  private scheduleReconnect(message: string) {
    this.reconnectCount += 1;
    this.lastWsMessageAt = 0;
    if (this.ws) {
      this.ws.removeAllListeners();
      this.ws.close();
      this.ws = undefined;
    }
    this.state = {
      ...this.state,
      status: {
        ...this.state.status,
        state: this.state.price > 0 ? "degraded" : "reconnecting",
        reconnectCount: this.reconnectCount,
        message
      }
    };
    this.emit();

    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
    }
    const delayMs = Math.min(2000 * this.reconnectCount, 10_000);
    this.reconnectTimer = setTimeout(() => {
      void this.bootstrapFromRest("Refreshing Binance REST snapshot while reconnecting WebSocket.");
      this.connect();
    }, delayMs);
  }
}

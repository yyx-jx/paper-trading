import WebSocket from "ws";
import type { Agent } from "node:http";
import { createProxyDispatcher, createProxyWsAgent, fetchJsonWithTimeout } from "./network";
import type { CandleBar, CandleInterval, ChainlinkConnectorState, SourceHealth } from "../../domain/types";

const RTDS_STALE_MS = 10_000;
const RTDS_DEAD_MS = 15_000;
const HISTORY_REQUEST_TIMEOUT_MS = 6000;
const HISTORY_INTERVALS: CandleInterval[] = ["30s", "1m", "5m", "15m", "1h"];
const INTERVAL_MS: Record<CandleInterval, number> = {
  "30s": 30_000,
  "1m": 60_000,
  "5m": 5 * 60_000,
  "15m": 15 * 60_000,
  "1h": 60 * 60_000,
  "1d": 24 * 60 * 60_000
};

function emptyStatus(symbol: string): SourceHealth {
  const now = Date.now();
  return {
    source: "Chainlink",
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
    message: "Waiting for Chainlink RTDS WebSocket."
  };
}

function normalizeSymbol(symbol: string) {
  const trimmed = symbol.trim().toLowerCase();
  return trimmed.includes("/") ? trimmed : `${trimmed}/usd`;
}

function toNumber(value: unknown) {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : undefined;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
}

function roundNumber(value: number, digits = 2) {
  return Number(value.toFixed(digits));
}

function normalizeBar(interval: CandleInterval, bar: CandleBar): CandleBar {
  const bucketSize = INTERVAL_MS[interval];
  const startTs = Math.floor(bar.startTs / bucketSize) * bucketSize;
  return {
    interval,
    startTs,
    endTs: startTs + bucketSize - 1,
    open: roundNumber(bar.open, 2),
    high: roundNumber(bar.high, 2),
    low: roundNumber(bar.low, 2),
    close: roundNumber(bar.close, 2),
    volume: roundNumber(bar.volume ?? 0, 6)
  };
}

function normalizeBars(interval: CandleInterval, bars: CandleBar[], limit = 240) {
  const deduped = new Map<number, CandleBar>();
  for (const bar of bars) {
    const normalizedBar = normalizeBar(interval, bar);
    if (normalizedBar.close > 0) {
      deduped.set(normalizedBar.startTs, normalizedBar);
    }
  }
  return [...deduped.values()].sort((left, right) => left.startTs - right.startTs).slice(-limit);
}

function aggregateBars(interval: "5m" | "15m" | "1h", sourceBars: CandleBar[]) {
  const grouped = new Map<number, CandleBar>();
  for (const bar of normalizeBars("1m", sourceBars, 300)) {
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
    existing.low = Math.min(existing.low, bar.low);
    existing.close = bar.close;
    existing.volume = roundNumber(existing.volume + bar.volume, 6);
  }
  return normalizeBars(interval, [...grouped.values()], interval === "5m" ? 60 : 48);
}

function splitOneMinuteBarToThirtySeconds(bar: CandleBar) {
  if (!bar.close) {
    return [] as CandleBar[];
  }
  const firstStart = Math.floor(bar.startTs / INTERVAL_MS["30s"]) * INTERVAL_MS["30s"];
  const secondStart = firstStart + INTERVAL_MS["30s"];
  const midpoint = roundNumber((bar.open + bar.close) / 2, 2);
  return normalizeBars(
    "30s",
    [
      {
        interval: "30s",
        startTs: firstStart,
        endTs: firstStart + INTERVAL_MS["30s"] - 1,
        open: bar.open,
        high: Math.max(bar.open, bar.high, midpoint),
        low: Math.min(bar.open, bar.low, midpoint),
        close: midpoint,
        volume: roundNumber((bar.volume ?? 0) / 2, 6)
      },
      {
        interval: "30s",
        startTs: secondStart,
        endTs: secondStart + INTERVAL_MS["30s"] - 1,
        open: midpoint,
        high: Math.max(midpoint, bar.high, bar.close),
        low: Math.min(midpoint, bar.low, bar.close),
        close: bar.close,
        volume: roundNumber((bar.volume ?? 0) / 2, 6)
      }
    ],
    480
  );
}

function valueFromCandlestick(candlestick: string, key: "open" | "high" | "low" | "close") {
  const match = candlestick.match(new RegExp(`${key}:\\([^)]*val:([0-9.]+)`));
  return match ? Number(match[1]) : 0;
}

function parseHistoryNodes(nodes: Array<{ bucket?: string; candlestick?: string; attributeName?: string }>) {
  return normalizeBars(
    "1m",
    nodes
      .filter((node) => !node.attributeName || node.attributeName === "benchmark")
      .map((node) => {
        const startTs = node.bucket ? Date.parse(node.bucket) : 0;
        const candlestick = node.candlestick ?? "";
        return {
          interval: "1m" as const,
          startTs,
          endTs: startTs + INTERVAL_MS["1m"] - 1,
          open: valueFromCandlestick(candlestick, "open"),
          high: valueFromCandlestick(candlestick, "high"),
          low: valueFromCandlestick(candlestick, "low"),
          close: valueFromCandlestick(candlestick, "close"),
          volume: 0
        };
      }),
    240
  );
}

function buildCandlesByInterval(oneMinuteBars: CandleBar[]) {
  const normalized1m = normalizeBars("1m", oneMinuteBars, 240);
  return {
    "30s": normalizeBars("30s", normalized1m.flatMap((bar) => splitOneMinuteBarToThirtySeconds(bar)), 480),
    "1m": normalized1m,
    "5m": aggregateBars("5m", normalized1m),
    "15m": aggregateBars("15m", normalized1m),
    "1h": aggregateBars("1h", normalized1m)
  } satisfies Partial<Record<CandleInterval, CandleBar[]>>;
}

function findPriceCandidate(value: unknown, path: string[] = []): number | undefined {
  if (typeof value === "number" || typeof value === "string") {
    const price = toNumber(value);
    const source = path.join(".").toLowerCase();
    const priceLike =
      source.includes("price") ||
      source.includes("value") ||
      source.includes("answer") ||
      source.includes("close") ||
      source.includes("mid");
    const excluded = source.includes("timestamp") || source.includes("time") || source.includes("id");
    return price && price > 1000 && price < 1_000_000 && priceLike && !excluded ? price : undefined;
  }
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index += 1) {
      const price = findPriceCandidate(value[index], [...path, String(index)]);
      if (price) return price;
    }
  } else if (value && typeof value === "object") {
    for (const [key, nested] of Object.entries(value as Record<string, unknown>)) {
      const price = findPriceCandidate(nested, [...path, key]);
      if (price) return price;
    }
  }
  return undefined;
}

function findTimestampCandidate(value: unknown, path: string[] = []): number | undefined {
  if (typeof value === "number" || typeof value === "string") {
    const parsed = toNumber(value);
    const source = path.join(".").toLowerCase();
    if (!parsed || (!source.includes("time") && !source.includes("timestamp") && !source.includes("ts"))) {
      return undefined;
    }
    return parsed < 10_000_000_000 ? parsed * 1000 : parsed;
  }
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index += 1) {
      const timestamp = findTimestampCandidate(value[index], [...path, String(index)]);
      if (timestamp) return timestamp;
    }
  } else if (value && typeof value === "object") {
    for (const [key, nested] of Object.entries(value as Record<string, unknown>)) {
      const timestamp = findTimestampCandidate(nested, [...path, key]);
      if (timestamp) return timestamp;
    }
  }
  return undefined;
}

function parseJsonMessage(raw: WebSocket.RawData) {
  const text = raw.toString();
  if (!text || text.toUpperCase() === "PONG") {
    return undefined;
  }
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return undefined;
  }
}

export class ChainlinkConnector {
  private ws?: WebSocket;
  private pingTimer?: NodeJS.Timeout;
  private staleTimer?: NodeJS.Timeout;
  private historyTimer?: NodeJS.Timeout;
  private reconnectTimer?: NodeJS.Timeout;
  private reconnectCount = 0;
  private lastMessageAt = 0;
  private readonly listeners = new Set<(state: ChainlinkConnectorState) => void>();
  private state: ChainlinkConnectorState;
  private readonly rtdsSymbol: string;
  private readonly proxyWsAgent;

  constructor(
    private readonly config: {
      symbol: string;
      rtdsWsUrl: string;
      rtdsSymbol: string;
      rtdsPingMs: number;
      historyUrl: string;
      historyFeedId: string;
      historyPollMs: number;
      upstreamProxyUrl?: string;
    }
  ) {
    this.rtdsSymbol = normalizeSymbol(config.rtdsSymbol || config.symbol);
    this.proxyWsAgent = createProxyWsAgent(config.upstreamProxyUrl);
    this.proxyDispatcher = createProxyDispatcher(config.upstreamProxyUrl);
    this.state = {
      price: 0,
      updatedAt: 0,
      status: emptyStatus(config.symbol)
    };
  }

  private readonly proxyDispatcher;

  start() {
    this.connect();
    void this.pollHistoryFallback();
    this.historyTimer = setInterval(
      () => void this.pollHistoryFallback(),
      Math.max(this.config.historyPollMs, 5000)
    );
    this.staleTimer = setInterval(() => this.markStaleIfNeeded(), 1000);
  }

  stop() {
    if (this.pingTimer) clearInterval(this.pingTimer);
    if (this.staleTimer) clearInterval(this.staleTimer);
    if (this.historyTimer) clearInterval(this.historyTimer);
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.pingTimer = undefined;
    this.staleTimer = undefined;
    this.historyTimer = undefined;
    this.reconnectTimer = undefined;
    if (this.ws) {
      this.ws.removeAllListeners();
      this.ws.close();
      this.ws = undefined;
    }
  }

  subscribe(listener: (state: ChainlinkConnectorState) => void) {
    this.listeners.add(listener);
    listener(this.state);
    return () => {
      this.listeners.delete(listener);
    };
  }

  getState() {
    return this.state;
  }

  private connect() {
    if (this.ws) {
      this.ws.removeAllListeners();
      this.ws.close();
    }
    const requestStartTs = Date.now();
    const wsOptions = this.proxyWsAgent ? { agent: this.proxyWsAgent as Agent } : undefined;
    this.ws = new WebSocket(this.config.rtdsWsUrl, wsOptions);
    this.ws.on("open", () => {
      const now = Date.now();
      this.lastMessageAt = now;
      this.state = {
        ...this.state,
        status: {
          ...this.state.status,
          state: "reconnecting",
          reconnectCount: this.reconnectCount,
          serverRecvTs: now,
          normalizedTs: now,
          serverPublishTs: now,
          acquireLatencyMs: Math.max(now - requestStartTs, 0),
          message: `Connected to Chainlink RTDS; waiting for ${this.rtdsSymbol}.`
        }
      };
      this.sendSubscription();
      if (this.pingTimer) clearInterval(this.pingTimer);
      this.pingTimer = setInterval(() => {
        if (this.ws?.readyState === WebSocket.OPEN) {
          this.ws.send("PING");
        }
      }, Math.max(this.config.rtdsPingMs, 1000));
      this.emit();
    });
    this.ws.on("message", (raw) => this.handleMessage(raw));
    this.ws.on("error", () => this.scheduleReconnect("Chainlink RTDS WebSocket error."));
    this.ws.on("close", () => this.scheduleReconnect("Chainlink RTDS WebSocket closed."));
  }

  private sendSubscription() {
    const payload = {
      action: "subscribe",
      subscriptions: [
        {
          topic: "crypto_prices_chainlink",
          type: "*",
          filters: JSON.stringify({ symbol: this.rtdsSymbol })
        }
      ]
    };
    this.ws?.send(JSON.stringify(payload));
  }

  private async pollHistoryFallback() {
    if (!this.config.historyUrl || !this.config.historyFeedId) {
      return;
    }
    try {
      const query = new URLSearchParams({
        feedId: this.config.historyFeedId,
        abiIndex: "0",
        timeRange: "1D"
      });
      const payload = await fetchJsonWithTimeout<{
        data?: {
          allStreamValuesGeneric1Minutes?: {
            nodes?: Array<{ bucket?: string; candlestick?: string; attributeName?: string }>;
          };
        };
      }>(`${this.config.historyUrl}?${query.toString()}`, HISTORY_REQUEST_TIMEOUT_MS, this.proxyDispatcher);
      const nodes = payload.data?.allStreamValuesGeneric1Minutes?.nodes ?? [];
      const oneMinuteBars = parseHistoryNodes(nodes);
      const latest = oneMinuteBars.at(-1);
      if (!latest || latest.close <= 0) {
        return;
      }
      const now = Date.now();
      const candlesByInterval = buildCandlesByInterval(oneMinuteBars);
      const keepRtdsStatus = this.state.status.state === "healthy" && now - this.lastMessageAt <= RTDS_STALE_MS;
      this.state = {
        ...this.state,
        price: keepRtdsStatus && this.state.price > 0 ? this.state.price : latest.close,
        updatedAt: keepRtdsStatus && this.state.updatedAt > 0 ? this.state.updatedAt : latest.endTs,
        candlesByInterval,
        status: keepRtdsStatus
          ? this.state.status
          : {
              source: "Chainlink",
              symbol: this.config.symbol,
              state: "degraded",
              reconnectCount: this.reconnectCount,
              sourceEventTs: latest.endTs,
              serverRecvTs: now,
              normalizedTs: now,
              serverPublishTs: now,
              acquireLatencyMs: Math.max(now - latest.endTs, 0),
              publishLatencyMs: 0,
              frontendLatencyMs: 0,
              message: "Chainlink RTDS unavailable; using data.chain.link public 1-minute history fallback."
            }
      };
      this.emit();
    } catch (error) {
      if (this.state.status.state === "healthy") {
        return;
      }
      const now = Date.now();
      this.state = {
        ...this.state,
        status: {
          ...this.state.status,
          state: "reconnecting",
          serverRecvTs: now,
          normalizedTs: now,
          serverPublishTs: now,
          message: `Chainlink history fallback failed: ${error instanceof Error ? error.message : "unknown error"}.`
        }
      };
      this.emit();
    }
  }

  private handleMessage(raw: WebSocket.RawData) {
    const payload = parseJsonMessage(raw);
    if (!payload) {
      return;
    }
    const now = Date.now();
    this.lastMessageAt = now;
    const price = findPriceCandidate(payload);
    if (!price) {
      return;
    }
    const sourceEventTs = findTimestampCandidate(payload) ?? now;
    this.state = {
      price,
      updatedAt: sourceEventTs,
      candlesByInterval: this.state.candlesByInterval,
      status: {
        source: "Chainlink",
        symbol: this.config.symbol,
        state: "healthy",
        reconnectCount: this.reconnectCount,
        sourceEventTs,
        serverRecvTs: now,
        normalizedTs: now,
        serverPublishTs: now,
        acquireLatencyMs: Math.max(now - sourceEventTs, 0),
        publishLatencyMs: 0,
        frontendLatencyMs: 0,
        message: `Chainlink RTDS live ${this.rtdsSymbol}.`
      }
    };
    this.emit();
  }

  private markStaleIfNeeded() {
    if (!this.lastMessageAt) {
      return;
    }
    const now = Date.now();
    const ageMs = now - this.lastMessageAt;
    if (ageMs <= RTDS_STALE_MS && this.state.status.state === "healthy") {
      return;
    }
    const nextState = ageMs > RTDS_DEAD_MS ? "reconnecting" : ageMs > RTDS_STALE_MS ? "degraded" : this.state.status.state;
    if (nextState === this.state.status.state && this.state.status.message?.includes("stale")) {
      return;
    }
    this.state = {
      ...this.state,
      status: {
        ...this.state.status,
        state: nextState,
        serverRecvTs: now,
        normalizedTs: now,
        serverPublishTs: now,
        publishLatencyMs: 0,
        message: `Chainlink RTDS stale for ${Math.round(ageMs)}ms; strict RTDS mode keeps the last RTDS price.`
      }
    };
    this.emit();
  }

  private scheduleReconnect(message: string) {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = undefined;
    }
    this.reconnectCount += 1;
    const now = Date.now();
    this.state = {
      ...this.state,
      status: {
        ...this.state.status,
        state: this.state.price > 0 ? "degraded" : "reconnecting",
        reconnectCount: this.reconnectCount,
        serverRecvTs: now,
        normalizedTs: now,
        serverPublishTs: now,
        message: `${message} Strict RTDS mode does not fall back to AggregatorV3.`
      }
    };
    this.emit();
    if (this.reconnectTimer) {
      return;
    }
    const delayMs = Math.min(1000 * this.reconnectCount, 10_000);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      this.connect();
    }, delayMs);
  }

  private emit() {
    for (const listener of this.listeners) {
      listener(this.state);
    }
  }
}

import WebSocket from "ws";
import type { Agent } from "node:http";
import { createProxyDispatcher, createProxyWsAgent, fetchJsonWithTimeout } from "./network";
import type { CoinbaseConnectorState, SourceHealth } from "../../domain/types";

const TICKER_CHANNEL = "ticker";
const HEARTBEATS_CHANNEL = "heartbeats";

function emptyStatus(symbol: string): SourceHealth {
  const now = Date.now();
  return {
    source: "Coinbase",
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
    message: "Waiting for Coinbase WebSocket."
  };
}

function roundNumber(value: number, digits = 2) {
  return Number(value.toFixed(digits));
}

function toCoinbaseProduct(symbol: string) {
  return `${symbol.trim().toUpperCase()}-USD`;
}

function parseIsoTimestampMs(iso: string | undefined, fallback: number): number {
  if (!iso) return fallback;
  const ts = Date.parse(iso);
  return Number.isFinite(ts) ? ts : fallback;
}

interface CoinbaseTickerEvent {
  type?: string;
  tickers?: Array<{
    type?: string;
    product_id?: string;
    price?: string;
    best_bid?: string;
    best_ask?: string;
  }>;
}

interface CoinbaseMessage {
  channel?: string;
  timestamp?: string;
  sequence_num?: number;
  events?: CoinbaseTickerEvent[];
  type?: string;
  message?: string;
}

export class CoinbaseConnector {
  private ws?: WebSocket;
  private reconnectTimer?: NodeJS.Timeout;
  private restPollTimer?: NodeJS.Timeout;
  private staleTimer?: NodeJS.Timeout;
  private reconnectCount = 0;
  private lastWsMessageAt = 0;
  private readonly listeners = new Set<(state: CoinbaseConnectorState) => void>();
  private state: CoinbaseConnectorState;
  private readonly product: string;
  private readonly proxyDispatcher;
  private readonly proxyWsAgent;

  constructor(
    private readonly config: {
      symbol: string;
      wsUrl: string;
      restUrl: string;
      restPollMs: number;
      requestTimeoutMs: number;
      wsStaleMs: number;
      upstreamProxyUrl?: string;
    }
  ) {
    const now = Date.now();
    this.product = toCoinbaseProduct(config.symbol);
    this.proxyDispatcher = createProxyDispatcher(config.upstreamProxyUrl);
    this.proxyWsAgent = createProxyWsAgent(config.upstreamProxyUrl);
    this.state = {
      price: 0,
      updatedAt: now,
      candlesByInterval: {},
      status: emptyStatus(config.symbol)
    };
  }

  start() {
    void this.bootstrapFromRest("Bootstrapping Coinbase REST snapshot.");
    this.restPollTimer = setInterval(() => {
      void this.pollRestTicker();
    }, Math.max(this.config.restPollMs, 5_000));
    this.staleTimer = setInterval(() => {
      this.checkWsStale();
    }, Math.max(Math.floor(this.config.wsStaleMs / 3), 2_000));
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

  subscribe(listener: (state: CoinbaseConnectorState) => void) {
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
      const payload = await this.fetchJson<{ price?: string; time?: string }>(
        `${this.config.restUrl}/products/${this.product}/ticker`
      );
      const price = Number(payload.price);
      const now = Date.now();
      const sourceEventTs = parseIsoTimestampMs(payload.time, now);
      if (Number.isFinite(price) && price > 0) {
        this.state = {
          ...this.state,
          price: roundNumber(price, 2),
          updatedAt: sourceEventTs,
          status: {
            ...this.state.status,
            state: this.lastWsMessageAt > 0 ? "healthy" : "reconnecting",
            sourceEventTs,
            serverRecvTs: now,
            normalizedTs: now,
            serverPublishTs: now,
            acquireLatencyMs: Math.max(now - sourceEventTs, 0),
            publishLatencyMs: 0,
            message
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
          message: error instanceof Error ? error.message : "Failed to bootstrap Coinbase REST data."
        }
      };
      this.emit();
    }
  }

  private async pollRestTicker() {
    const wsAlive = this.lastWsMessageAt > 0 && Date.now() - this.lastWsMessageAt <= this.config.wsStaleMs;
    if (wsAlive) {
      return;
    }
    try {
      const payload = await this.fetchJson<{ price?: string; time?: string }>(
        `${this.config.restUrl}/products/${this.product}/ticker`
      );
      const price = Number(payload.price);
      const now = Date.now();
      const sourceEventTs = parseIsoTimestampMs(payload.time, now);
      if (Number.isFinite(price) && price > 0) {
        this.state = {
          ...this.state,
          price: roundNumber(price, 2),
          updatedAt: sourceEventTs,
          status: {
            ...this.state.status,
            state: "degraded",
            sourceEventTs,
            serverRecvTs: now,
            normalizedTs: now,
            serverPublishTs: now,
            acquireLatencyMs: Math.max(now - sourceEventTs, 0),
            publishLatencyMs: 0,
            message: "Coinbase WebSocket stale; serving REST fallback data."
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
          message: error instanceof Error ? error.message : "Failed to poll Coinbase REST ticker."
        }
      };
      this.emit();
    }
  }

  private async fetchJson<T>(url: string): Promise<T> {
    try {
      return await fetchJsonWithTimeout<T>(url, this.config.requestTimeoutMs, this.proxyDispatcher);
    } catch (error) {
      throw new Error(
        `Coinbase request failed for ${url}: ${error instanceof Error ? error.message : "unknown error"}`
      );
    }
  }

  private connect() {
    this.state = {
      ...this.state,
      status: {
        ...this.state.status,
        state: "reconnecting",
        reconnectCount: this.reconnectCount,
        message: "Connecting to Coinbase WebSocket."
      }
    };
    this.emit();

    this.ws = new WebSocket(
      this.config.wsUrl,
      this.proxyWsAgent ? { agent: this.proxyWsAgent as Agent } : undefined
    );

    this.ws.on("open", () => {
      try {
        this.ws?.send(
          JSON.stringify({
            type: "subscribe",
            product_ids: [this.product],
            channel: TICKER_CHANNEL
          })
        );
        this.ws?.send(
          JSON.stringify({
            type: "subscribe",
            product_ids: [this.product],
            channel: HEARTBEATS_CHANNEL
          })
        );
      } catch (error) {
        this.scheduleReconnect(
          `Failed to subscribe to Coinbase channels: ${error instanceof Error ? error.message : "unknown error"}`
        );
        return;
      }
      this.state = {
        ...this.state,
        status: {
          ...this.state.status,
          state: "healthy",
          reconnectCount: this.reconnectCount,
          message: "Connected to Coinbase WebSocket."
        }
      };
      this.emit();
    });

    this.ws.on("message", (buffer) => {
      try {
        const parsed = JSON.parse(buffer.toString()) as CoinbaseMessage;
        const now = Date.now();
        this.lastWsMessageAt = now;

        if (parsed.type === "error") {
          this.state = {
            ...this.state,
            status: {
              ...this.state.status,
              state: "degraded",
              message: `Coinbase server error: ${parsed.message ?? "unknown"}`
            }
          };
          this.emit();
          return;
        }

        if (parsed.channel === HEARTBEATS_CHANNEL) {
          return;
        }

        if (parsed.channel !== TICKER_CHANNEL || !Array.isArray(parsed.events)) {
          return;
        }

        const sourceEventTs = parseIsoTimestampMs(parsed.timestamp, now);
        let appliedPrice = 0;
        for (const ev of parsed.events) {
          for (const tick of ev.tickers ?? []) {
            if (tick.product_id !== this.product) continue;
            const price = Number(tick.price);
            if (Number.isFinite(price) && price > 0) {
              appliedPrice = price;
            }
          }
        }

        if (appliedPrice > 0) {
          this.state = {
            ...this.state,
            price: roundNumber(appliedPrice, 2),
            updatedAt: sourceEventTs,
            status: {
              source: "Coinbase",
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
              message: "Receiving Coinbase live ticker data."
            }
          };
          this.emit();
        }
      } catch (error) {
        this.state = {
          ...this.state,
          status: {
            ...this.state.status,
            state: "degraded",
            message: error instanceof Error ? error.message : "Failed to parse Coinbase payload."
          }
        };
        this.emit();
      }
    });

    this.ws.on("close", () => {
      this.scheduleReconnect("Coinbase WebSocket closed.");
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
    this.scheduleReconnect("Coinbase WebSocket became stale.");
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
      void this.bootstrapFromRest("Refreshing Coinbase REST snapshot while reconnecting WebSocket.");
      this.connect();
    }, delayMs);
  }
}

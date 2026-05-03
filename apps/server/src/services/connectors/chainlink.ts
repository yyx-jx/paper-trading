import WebSocket from "ws";
import type { Agent } from "node:http";
import { createProxyWsAgent } from "./network";
import type { ChainlinkConnectorState, SourceHealth } from "../../domain/types";

const RTDS_STALE_MS = 10_000;
const RTDS_DEAD_MS = 15_000;

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
      upstreamProxyUrl?: string;
    }
  ) {
    this.rtdsSymbol = normalizeSymbol(config.rtdsSymbol || config.symbol);
    this.proxyWsAgent = createProxyWsAgent(config.upstreamProxyUrl);
    this.state = {
      price: 0,
      updatedAt: 0,
      status: emptyStatus(config.symbol)
    };
  }

  start() {
    this.connect();
    this.staleTimer = setInterval(() => this.markStaleIfNeeded(), 1000);
  }

  stop() {
    if (this.pingTimer) clearInterval(this.pingTimer);
    if (this.staleTimer) clearInterval(this.staleTimer);
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.pingTimer = undefined;
    this.staleTimer = undefined;
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

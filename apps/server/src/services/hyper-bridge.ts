import WebSocket from "ws";
import type { OrderAction, OrderBookSnapshot, PaperOrderKind, TradeSide, UserRecord } from "../domain/types";

export type HyperBridgeMode = "direct" | "filtered";

export interface HyperBridgeConfig {
  enabled: boolean;
  gatewayUrl: string;
  reconnectMs: number;
  maxQueue: number;
  defaultMode: HyperBridgeMode;
}

export interface HyperBridgeSocket {
  on(event: string, listener: (...args: any[]) => void): this;
  send(payload: string): void;
  close(): void;
  terminate?(): void;
}

export interface SignalEmittedPayload {
  traceId: string;
  orderId: string;
  user: UserRecord;
  payload: {
    action?: OrderAction;
    side: TradeSide;
    amount?: number;
    qty?: number;
    orderKind?: PaperOrderKind;
    limitPrice?: number;
    clientOrderId?: string;
  };
  bookSnapshot: OrderBookSnapshot;
  midPrice?: number;
  estimatedFee?: number;
  marketId: string;
  emittedAt: number;
}

export interface PaperFilledPayload {
  traceId: string;
  fillPrice: number | null;
  slippageBps: number | null;
  status: "filled" | "pending" | "failed";
  feeUsdc: number;
  filledQty: number | null;
  partial: boolean;
  filledAt: number;
}

export interface HyperSignal {
  signal_id: string;
  client_order_id: string;
  hyper_user_id: string;
  market_id: string;
  token_id: string | null;
  side: "BUY" | "SELL";
  direction: TradeSide;
  order_kind: "MARKET" | "LIMIT";
  amount: number;
  amount_unit: "USDC" | "SHARES";
  notional_usdc: number;
  estimated_fee_usdc: number;
  limit_price: number | null;
  emitted_at: number;
  mode: HyperBridgeMode;
  signal_source: "BRIDGE";
  quality_grade: string | null;
  market_snapshot: OrderBookSnapshot;
}

export interface HyperPaperFill {
  signal_id: string;
  paper_status: "filled" | "pending" | "failed";
  paper_fill_price: number | null;
  paper_fee_usdc: number;
  paper_slippage_bps: number | null;
  paper_filled_qty: number | null;
  paper_partial: boolean;
  paper_filled_at: number;
}

interface QueuedMessage {
  type: "signal" | "paper_fill";
  payload: HyperSignal | HyperPaperFill;
}

function textEnv(value: string | undefined, fallback: string) {
  const trimmed = value?.trim();
  return trimmed ? trimmed : fallback;
}

function positiveIntEnv(value: string | undefined, fallback: number) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
}

function modeEnv(value: string | undefined): HyperBridgeMode {
  return value === "direct" ? "direct" : "filtered";
}

function roundNumber(value: number, decimals = 8) {
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}

export function loadHyperBridgeConfig(env: NodeJS.ProcessEnv = process.env): HyperBridgeConfig {
  return {
    enabled: env.HYPER_BRIDGE_ENABLED === "true",
    gatewayUrl: textEnv(env.HYPER_GATEWAY_URL, "ws://127.0.0.1:8770"),
    reconnectMs: positiveIntEnv(env.HYPER_BRIDGE_RECONNECT_MS, 3000),
    maxQueue: positiveIntEnv(env.HYPER_BRIDGE_MAX_QUEUE, 5000),
    defaultMode: modeEnv(env.HYPER_BRIDGE_MODE)
  };
}

export function buildHyperSignal(input: SignalEmittedPayload, mode: HyperBridgeMode): HyperSignal {
  const action = input.payload.action ?? "buy";
  const side = action === "sell" ? "SELL" : "BUY";
  const orderKind = input.payload.orderKind === "limit" ? "LIMIT" : "MARKET";
  const amount = side === "BUY" ? input.payload.amount ?? 0 : input.payload.qty ?? 0;
  const midPrice = input.midPrice ?? input.bookSnapshot.midPrice ?? 0.5;
  const notionalUsdc = side === "BUY" ? amount : roundNumber(amount * midPrice);

  return {
    signal_id: input.traceId,
    client_order_id: input.payload.clientOrderId ?? `${input.traceId}_real`,
    hyper_user_id: input.user.id,
    market_id: input.marketId,
    token_id: null,
    side,
    direction: input.payload.side,
    order_kind: orderKind,
    amount,
    amount_unit: side === "BUY" ? "USDC" : "SHARES",
    notional_usdc: notionalUsdc,
    estimated_fee_usdc: input.estimatedFee ?? 0,
    limit_price: input.payload.limitPrice ?? null,
    emitted_at: input.emittedAt,
    mode,
    signal_source: "BRIDGE",
    quality_grade: null,
    market_snapshot: input.bookSnapshot
  };
}

export function buildHyperPaperFill(input: PaperFilledPayload): HyperPaperFill {
  return {
    signal_id: input.traceId,
    paper_status: input.status,
    paper_fill_price: input.fillPrice,
    paper_fee_usdc: input.feeUsdc,
    paper_slippage_bps: input.slippageBps,
    paper_filled_qty: input.filledQty,
    paper_partial: input.partial,
    paper_filled_at: input.filledAt
  };
}

export class HyperBridge {
  private socket?: HyperBridgeSocket;
  private connected = false;
  private stopped = false;
  private reconnectTimer?: NodeJS.Timeout;
  private readonly queue: QueuedMessage[] = [];
  private sentSignals = 0;
  private sentPaperFills = 0;
  private droppedMessages = 0;
  private receivedMessagesIgnored = 0;

  constructor(
    private readonly config: HyperBridgeConfig,
    private readonly logger: { info(message: string): void; warn(message: string): void },
    private readonly deps: { createSocket?: (url: string) => HyperBridgeSocket } = {}
  ) {}

  start() {
    this.stopped = false;
    if (!this.config.enabled) {
      this.logger.info("[hyper-bridge] disabled");
      return;
    }
    this.connect();
  }

  stop() {
    this.stopped = true;
    this.connected = false;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    if (this.socket) {
      try {
        this.socket.close();
      } catch {
        this.socket.terminate?.();
      }
      this.socket = undefined;
    }
  }

  onSignalEmitted(payload: SignalEmittedPayload) {
    if (!this.config.enabled) {
      return;
    }
    this.enqueue("signal", buildHyperSignal(payload, this.config.defaultMode));
  }

  onPaperFilled(payload: PaperFilledPayload) {
    if (!this.config.enabled) {
      return;
    }
    this.enqueue("paper_fill", buildHyperPaperFill(payload));
  }

  getStats() {
    return {
      enabled: this.config.enabled,
      connected: this.connected,
      queueDepth: this.queue.length,
      sentSignals: this.sentSignals,
      sentPaperFills: this.sentPaperFills,
      droppedMessages: this.droppedMessages,
      receivedMessagesIgnored: this.receivedMessagesIgnored
    };
  }

  private connect() {
    if (this.stopped || !this.config.enabled) {
      return;
    }
    this.socket?.terminate?.();
    this.socket = this.deps.createSocket?.(this.config.gatewayUrl) ?? new WebSocket(this.config.gatewayUrl);
    this.socket.on("open", () => {
      this.connected = true;
      this.logger.info(`[hyper-bridge] connected ${this.config.gatewayUrl}, queued=${this.queue.length}`);
      this.flushQueue();
    });
    this.socket.on("message", () => {
      this.receivedMessagesIgnored += 1;
    });
    this.socket.on("close", () => {
      this.connected = false;
      this.scheduleReconnect();
    });
    this.socket.on("error", (error: Error) => {
      this.logger.warn(`[hyper-bridge] websocket error: ${error.message}`);
    });
  }

  private scheduleReconnect() {
    if (this.stopped || !this.config.enabled || this.reconnectTimer) {
      return;
    }
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      this.connect();
    }, this.config.reconnectMs);
  }

  private enqueue(type: QueuedMessage["type"], payload: QueuedMessage["payload"]) {
    if (this.queue.length >= this.config.maxQueue) {
      this.queue.shift();
      this.droppedMessages += 1;
      this.logger.warn("[hyper-bridge] queue full, dropped oldest message");
    }
    this.queue.push({ type, payload });
    if (this.connected) {
      this.flushQueue();
    }
  }

  private flushQueue() {
    if (!this.connected || !this.socket) {
      return;
    }
    while (this.queue.length > 0) {
      const item = this.queue.shift()!;
      try {
        this.socket.send(
          JSON.stringify(item.type === "signal" ? { type: "signal", signal: item.payload } : { type: "paper_fill", paper_fill: item.payload })
        );
        if (item.type === "signal") {
          this.sentSignals += 1;
        } else {
          this.sentPaperFills += 1;
        }
      } catch (error) {
        this.queue.unshift(item);
        this.logger.warn(`[hyper-bridge] send failed: ${error instanceof Error ? error.message : String(error)}`);
        return;
      }
    }
  }
}

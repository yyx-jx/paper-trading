export type Language = "zh-CN" | "en-US";
export type Role = "Tester" | "Senior Tester" | "Test Engineer" | "Admin";
export type TradeSide = "UP" | "DOWN";
export type OrderAction = "buy" | "sell";
export type PaperOrderKind = "market" | "limit";
export type CandleInterval = "1m" | "5m" | "1d";
export type RoundStatus =
  | "Trading"
  | "Frozen"
  | "Settling"
  | "Polling"
  | "Settled"
  | "Redeeming"
  | "Closed"
  | "Manual";
export type MarketSwitchState = "active" | "prefetching_next" | "next_ready" | "market_not_ready";

export interface PublicUser {
  id: string;
  displayName: string;
  role: Role;
  language: Language;
  permissionCodes: string[];
}

export interface SourceHealth {
  source: "Binance" | "Chainlink" | "CLOB";
  symbol: string;
  state: "healthy" | "reconnecting" | "stale" | "degraded" | "disabled";
  reconnectCount: number;
  sourceEventTs: number;
  serverRecvTs: number;
  normalizedTs: number;
  serverPublishTs: number;
  acquireLatencyMs: number;
  publishLatencyMs: number;
  frontendLatencyMs: number;
  clientRecvTs?: number;
  message?: string;
}

export interface BookLevel {
  price: number;
  qty: number;
}

export interface OrderBookSnapshot {
  snapshotId: string;
  snapshotTs: number;
  bestBid: number;
  bestAsk: number;
  midPrice: number;
  bids: BookLevel[];
  asks: BookLevel[];
}

export interface MarketTrade {
  id: string;
  side: TradeSide;
  price: number;
  qty: number;
  ts: number;
}

export interface CandleBar {
  interval: CandleInterval;
  startTs: number;
  endTs: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface MarketSnapshot {
  symbol: string;
  marketId: string;
  marketSlug?: string;
  eventId?: string;
  eventSlug?: string;
  conditionId?: string;
  seriesSlug?: string;
  serverNow: number;
  binancePrice: number;
  chainlinkPrice: number;
  currentPrice: number;
  priceToBeat: number;
  upPrice: number;
  downPrice: number;
  sources: Record<"binance" | "chainlink" | "clob", SourceHealth>;
  orderBooks: Record<TradeSide, OrderBookSnapshot>;
  recentTrades: MarketTrade[];
  candles: Array<{ ts: number; price: number }>;
  binance: {
    spotPrice: number;
    latestTick?: { ts: number; price: number };
    candlesByInterval: Record<CandleInterval, CandleBar[]>;
  };
  chainlink: {
    referencePrice: number;
    settlementReference: number;
  };
  clob: {
    delta: number;
    volume: number;
    upBook: OrderBookSnapshot;
    downBook: OrderBookSnapshot;
    recentTrades: MarketTrade[];
    bestBidAskSummary: Record<TradeSide, { bestBid: number; bestAsk: number }>;
  };
  uiMeta: {
    marketTitle: string;
    marketSubtitle?: string;
    countdownMs: number;
    acceptingOrders: boolean;
    marketSwitchState: MarketSwitchState;
    sourceStatusSummary: Array<{ source: SourceHealth["source"]; state: SourceHealth["state"] }>;
  };
}

export interface RoundRecord {
  id: string;
  marketId: string;
  symbol: string;
  eventId?: string;
  marketSlug?: string;
  eventSlug?: string;
  conditionId?: string;
  seriesSlug?: string;
  upTokenId?: string;
  downTokenId?: string;
  title?: string;
  resolutionSource?: string;
  startAt: number;
  endAt: number;
  priceToBeat: number;
  status: RoundStatus;
  pollCount: number;
  pollStartAt?: number;
  lastPollAt?: number;
  closingSpotPrice?: number;
  settledSide?: TradeSide;
  settlementPrice?: number;
  settlementTs?: number;
  settlementSource?: "Polymarket" | "Gamma" | "Chainlink";
  polymarketSettlementPrice?: number;
  polymarketSettlementStatus?: "pending" | "resolved" | "fallback" | "manual";
  polymarketOpenPrice?: number;
  polymarketClosePrice?: number;
  polymarketOpenPriceSource?: string;
  polymarketClosePriceSource?: string;
  settlementReceivedAt?: number;
  redeemScheduledAt?: number;
  binanceOpenPrice?: number;
  binanceClosePrice?: number;
  redeemStartTs?: number;
  redeemFinishTs?: number;
  manualReason?: string;
  acceptingOrders?: boolean;
  closingPriceSource?: "Chainlink" | "Gamma";
}

export interface HistoryRound extends RoundRecord {
  userPnl: number;
}

export interface ProfileOverview {
  totalEquity: number;
  availableUsdc: number;
  positionValue: number;
  realizedPnlToday: number;
  unrealizedPnl: number;
  winRate: number;
  roundsParticipatedToday: number;
}

export interface PositionRecord {
  id: string;
  userId: string;
  roundId: string;
  side: TradeSide;
  qty: number;
  lockedQty?: number;
  averageEntry: number;
  notionalSpent: number;
  currentMark: number;
  currentBid?: number;
  currentAsk?: number;
  currentMid?: number;
  currentValue?: number;
  sourceLatencyMs?: number;
  unrealizedPnl: number;
  realizedPnl: number;
  status: "open" | "closed";
  displayStatus?: "open" | "pending_settlement" | "settled" | "sold";
  openedAt: number;
  closedAt?: number;
  settlementResult?: "win" | "loss" | "sold";
}

export interface OrderRecord {
  id: string;
  traceId: string;
  userId: string;
  roundId: string;
  symbol: string;
  marketId: string;
  action: OrderAction;
  side: TradeSide;
  status: "pending" | "filled" | "partial" | "failed" | "cancelled";
  orderKind?: PaperOrderKind;
  timeInForce?: "FOK" | "GTC";
  limitPrice?: number;
  lifecycleStatus?: OrderRecord["status"];
  resultType?: "pending" | "all_filled" | "all_failed" | "cancelled";
  tokenId?: string;
  bookKey?: string;
  bookHash?: string;
  requestedAmountUsdc?: number;
  requestedQty?: number;
  frozenUsdc?: number;
  frozenQty?: number;
  fills?: Array<Record<string, unknown>>;
  sourceLatencyMs?: number;
  marketSlug?: string;
  notionalUsdc: number;
  expectedQty: number;
  filledQty: number;
  unfilledQty: number;
  avgFillPrice?: number;
  bestBid: number;
  bestAsk: number;
  midPrice: number;
  bookSnapshotTs: number;
  partialFilled: boolean;
  slippageBps?: number;
  matchLatencyMs: number;
  failureReason?: string;
  clientSendTs?: number;
  serverRecvTs: number;
  serverPublishTs: number;
  createdAt: number;
}

export interface AuditEvent {
  eventId: string;
  traceId: string;
  category: "operation" | "matching" | "settlement" | "latency";
  actionType: string;
  actionStatus: "success" | "failed" | "timeout";
  userId?: string;
  role?: Role;
  pageName: string;
  moduleName: string;
  symbol?: string;
  roundId?: string;
  resultCode: string;
  resultMessage: string;
  clientSendTs?: number;
  serverRecvTs: number;
  engineStartTs?: number;
  engineFinishTs?: number;
  serverPublishTs: number;
  backendLatencyMs: number;
  frontendLatencyMs?: number;
  details?: Record<string, unknown>;
}

export interface BehaviorActionLog {
  logId: string;
  timestampMs: number;
  assetClass: "BTC_5M_UPDOWN";
  actionType: string;
  actionStatus: "success" | "failed" | "timeout";
  roundId?: string;
  direction?: TradeSide;
  entryOdds?: number;
  deltaClob: number;
  volumeClob: number;
  positionNotional?: number;
  exitType?: string;
  exitOdds?: number;
  settlementResult?: PositionRecord["settlementResult"];
  testerIdAnon: string;
  traceId?: string;
  orderId?: string;
  marketId?: string;
  marketSlug?: string;
  roundStatus?: RoundStatus;
  countdownMs?: number;
  binanceSpotPrice: number;
  binance1mLastClose: number;
  binance5mLastClose: number;
  binance1dLastClose: number;
  chainlinkPrice: number;
  priceToBeat: number;
  upPrice: number;
  downPrice: number;
  upBookTop5: BookLevel[];
  downBookTop5: BookLevel[];
  recentTradesTop20: MarketTrade[];
  bookSnapshotEntry: {
    snapshotId: string;
    snapshotTs: number;
    topBids: BookLevel[];
    topAsks: BookLevel[];
  };
  actualFillPrice?: number;
  slippageBps?: number;
  partialFilled?: boolean;
  unfilledQty?: number;
  executionLatencyMs?: number;
  settlementDirection?: TradeSide;
  settlementTimeMs?: number;
  gammaPollCount?: number;
  redeemFinishTimeMs?: number;
  contextJson?: Record<string, unknown>;
}

export interface AuditLogQuery {
  from?: number;
  to?: number;
  userId?: string;
  roundId?: string;
  category?: "operation" | "matching" | "settlement" | "latency";
  actionType?: string;
  actionStatus?: "success" | "failed" | "timeout";
  traceId?: string;
  orderId?: string;
  positionId?: string;
  resultCode?: string;
}

export interface BehaviorLogQuery {
  from?: number;
  to?: number;
  userId?: string;
  roundId?: string;
  actionType?: string;
  actionStatus?: "success" | "failed" | "timeout";
  traceId?: string;
  orderId?: string;
  marketId?: string;
  marketSlug?: string;
}

export interface MatchingReplayResult {
  bookKey: string;
  latest?: Record<string, unknown>;
  steps: Array<Record<string, unknown>>;
}

export interface TradeTimeline {
  order: OrderRecord;
  position?: PositionRecord;
  auditEvents: AuditEvent[];
  behaviorLogs: BehaviorActionLog[];
  matchingReplay?: MatchingReplayResult;
}

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8787";

async function request<T>(path: string, token?: string, init?: RequestInit): Promise<T> {
  const hasBody = typeof init?.body !== "undefined";
  const response = await fetch(`${API_BASE_URL}${path}`, {
    ...init,
    headers: {
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...(hasBody ? { "Content-Type": "application/json" } : {}),
      ...(init?.headers ?? {})
    }
  });

  const text = await response.text();
  let data: (T & { error?: boolean; message?: string }) | undefined;
  if (text) {
    try {
      data = JSON.parse(text) as T & { error?: boolean; message?: string };
    } catch {
      throw new Error(text || "Request failed.");
    }
  }
  if (!response.ok || data?.error) {
    throw new Error(data?.message ?? "Request failed.");
  }
  return (data ?? ({} as T)) as T;
}

async function requestText(path: string, token?: string): Promise<string> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: {
      ...(token ? { Authorization: `Bearer ${token}` } : {})
    }
  });
  const text = await response.text();
  if (!response.ok) {
    try {
      const parsed = JSON.parse(text) as { message?: string };
      throw new Error(parsed.message ?? "Request failed.");
    } catch {
      throw new Error(text || "Request failed.");
    }
  }
  return text;
}

function buildQuery(query?: object) {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(query ?? {})) {
    if ((typeof value === "string" || typeof value === "number") && value !== "") {
      search.set(key, String(value));
    }
  }
  return search.toString() ? `?${search.toString()}` : "";
}

export const api = {
  baseUrl: API_BASE_URL,
  createWsUrl(path: string, token: string) {
    const base = API_BASE_URL.replace("http://", "ws://").replace("https://", "wss://");
    return `${base}${path}?token=${token}`;
  },
  login(username: string, password: string) {
    return request<{
      token: string;
      user_id: string;
      role: Role;
      language: Language;
      display_name: string;
      permission_codes: string[];
    }>("/api/auth/login", undefined, {
      method: "POST",
      body: JSON.stringify({ username, password })
    });
  },
  getMe(token: string) {
    return request<PublicUser>("/api/me", token);
  },
  setLanguage(token: string, language: Language) {
    return request<PublicUser>("/api/me/language", token, {
      method: "POST",
      body: JSON.stringify({ language })
    });
  },
  getCurrentRound(token: string) {
    return request<{ currentRound?: RoundRecord; snapshot: MarketSnapshot }>("/api/rounds/current", token);
  },
  getHistory(token: string, limit = 60) {
    return request<HistoryRound[]>(`/api/rounds/history?limit=${limit}`, token);
  },
  getProfile(token: string) {
    return request<ProfileOverview>("/api/profile/me", token);
  },
  getPositions(token: string) {
    return request<PositionRecord[]>("/api/positions/me", token);
  },
  getOrders(token: string) {
    return request<OrderRecord[]>("/api/orders/me", token);
  },
  getLogs(token: string) {
    return request<AuditEvent[]>("/api/logs/me", token);
  },
  getSourceStatus(token: string) {
    return request<SourceHealth[]>("/api/system/sources/status", token);
  },
  getTrainingLogs(token: string, query?: BehaviorLogQuery) {
    const suffix = buildQuery(query);
    return request<BehaviorActionLog[]>(`/api/logs/training${suffix}`, token);
  },
  exportTrainingLogs(token: string, query?: BehaviorLogQuery) {
    const suffix = buildQuery(query);
    return requestText(`/api/logs/training/export${suffix}`, token);
  },
  getAuditLogs(token: string, query?: AuditLogQuery) {
    const suffix = buildQuery(query);
    return request<AuditEvent[]>(`/api/logs/audit${suffix}`, token);
  },
  exportAuditLogs(token: string, query?: AuditLogQuery) {
    const suffix = buildQuery(query);
    return requestText(`/api/logs/audit/export${suffix}`, token);
  },
  getTradeTimeline(token: string, orderId: string) {
    return request<TradeTimeline>(`/api/logs/trade-timeline${buildQuery({ orderId })}`, token);
  },
  placeOrder(
    token: string,
    input: {
      action: OrderAction;
      side: TradeSide;
      orderKind: PaperOrderKind;
      amount?: number;
      qty?: number;
      limitPrice?: number;
    }
  ) {
    return request<{ order: OrderRecord }>("/api/orders", token, {
      method: "POST",
      body: JSON.stringify({
        ...input,
        clientSendTs: Date.now()
      })
    });
  },
  cancelOrder(token: string, orderId: string) {
    return request<OrderRecord>(`/api/orders/${orderId}/cancel`, token, {
      method: "POST"
    });
  },
  sellPosition(token: string, positionId: string) {
    return request<OrderRecord>(`/api/positions/${positionId}/sell`, token, {
      method: "POST"
    });
  },
  closeSide(token: string, side: TradeSide) {
    return request<{
      closedPositionsCount: number;
      totalQty: number;
      totalProceeds: number;
      avgFillPrice?: number;
      failures: Array<{ positionId: string; message: string }>;
    }>("/api/positions/close-side", token, {
      method: "POST",
      body: JSON.stringify({
        side,
        clientSendTs: Date.now()
      })
    });
  },
  reverseSide(token: string, side: TradeSide) {
    return request<{
      closeResult: {
        closedPositionsCount: number;
        totalQty: number;
        totalProceeds: number;
        avgFillPrice?: number;
        failures: Array<{ positionId: string; message: string }>;
      };
      reverseSide: TradeSide;
      reverseOrder: OrderRecord;
    }>("/api/positions/reverse-side", token, {
      method: "POST",
      body: JSON.stringify({
        side,
        clientSendTs: Date.now()
      })
    });
  }
};

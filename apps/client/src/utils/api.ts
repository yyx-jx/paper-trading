import { redactNetworkAddresses } from "./redaction";

export type Language = "zh-CN" | "en-US";
export type Role = "Tester" | "Senior Tester" | "Test Engineer" | "Admin";
export type PermissionLevel = "Initial" | "Standard";
export type TradeSide = "UP" | "DOWN";
export type OrderAction = "buy" | "sell";
export type PaperOrderKind = "market" | "limit";
export type CandleInterval = "30s" | "1m" | "5m" | "15m" | "1h" | "1d";
export type FeeCurrency = "USD";
export type DisplayPriceSource = "mid" | "last_trade" | "outcome_price";
export type LogSystem = "all" | "audit" | "training" | "matching";
export type LogCategory = "operation" | "matching" | "settlement" | "latency";
export type MatchingEventType = "external_book_synced" | "order_executed" | "order_cancelled";
export type LogGroup = "operation" | "settlement" | "market_latency" | "system_latency" | "matching_action";
export type LatencySource = "binance" | "coinbase" | "clob" | "system";
export type ConnectionState = "healthy" | "reconnecting" | "stale" | "degraded" | "disabled";
export type LatencyPhase = "backend" | "acquire" | "publish" | "frontend";
export type MatchingLogKind = "action" | "engine";
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
  username: string;
  displayName: string;
  role: Role;
  language: Language;
  permissionCodes: string[];
  availableUsdc: number;
  isActive: boolean;
  seniorTesterId?: string;
  managerUserId?: string;
  permissionLevel?: PermissionLevel;
  disabledAt?: number;
  disabledBy?: string;
  createdAt: number;
  updatedAt: number;
}

export interface SourceHealth {
  source: "Binance" | "Coinbase" | "CLOB";
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
  components?: Record<string, SourceComponentHealth>;
}

export interface SourceComponentHealth {
  name: string;
  label?: string;
  state: SourceHealth["state"];
  sourceEventTs: number;
  serverRecvTs: number;
  message?: string;
}

export interface MarketTransportMeta {
  serverPublishTs: number;
  payloadSeq: number;
  coalescedCount?: number;
  serverQueueMs?: number;
  snapshotBuildTs?: number;
  wsSendStartTs?: number;
  broadcastBuildMs?: number;
  broadcastFanoutSize?: number;
  droppedForBackpressure?: boolean;
}

export interface ClobMarketInfo {
  conditionId?: string;
  minimumTickSize: number;
  minimumOrderSize: number;
  makerFeeRate: number;
  takerFeeRate: number;
  platformFeeRate?: number;
  platformFeeExponent?: number;
  platformFeeTakerOnly?: boolean;
  feeRateAvailable?: boolean;
  feeRateBps?: number;
  feeDetails?: Record<string, unknown>;
  tokens?: Array<{ tokenId: string; outcome?: string; minimumTickSize?: number; minimumOrderSize?: number }>;
  rfqEnabled?: boolean;
  source: "clob" | "gamma" | "conservative";
  conservative: boolean;
  updatedAt: number;
}

export interface FeeBreakdown {
  role: "maker" | "taker";
  feeRate: number;
  formula: "C * feeRate * p * (1 - p)";
  price: number;
  quantity: number;
  notional: number;
  platformFee?: number;
  platformFeeExponent?: number;
  platformFeeTakerOnly?: boolean;
  totalFee?: number;
  amount: number;
}

export interface LatencyBreakdown {
  sourceEventAge: Record<"binance" | "coinbase" | "clob", number>;
  serverIngressLatency: Record<"binance" | "coinbase" | "clob", number>;
  serverComputeLatency: number;
  clientTransportLatency?: number;
}

export interface SettlementPreview {
  roundId: string;
  state: "preliminary" | "confirmed" | "manual";
  side?: TradeSide;
  price?: number;
  source: "CLOB" | "Gamma" | "Polymarket" | "Coinbase";
  detectedAt?: number;
  upPrice?: number;
  downPrice?: number;
  tokenSide?: TradeSide;
  binanceSide?: TradeSide;
  binancePrice?: number;
  priceToBeat?: number;
  confidence?: "aligned" | "token_only" | "binance_only" | "conflict";
  conflictReason?: string;
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

export interface CandlePoint {
  ts: number;
  price: number;
}

export interface CandleBar {
  interval: CandleInterval | "5s";
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
  coinbasePrice: number;
  currentPrice: number;
  priceToBeat: number;
  displayPriceToBeat?: number;
  displayPriceToBeatSource?: "official" | "binance_open_fallback";
  upPrice: number;
  downPrice: number;
  displayPrices: Record<TradeSide, number>;
  displayPriceSource: Record<TradeSide, DisplayPriceSource>;
  displayPriceSpread: Record<TradeSide, number>;
  latencyBreakdown: LatencyBreakdown;
  sources: Record<"binance" | "coinbase" | "clob", SourceHealth>;
  orderBooks: Record<TradeSide, OrderBookSnapshot>;
  recentTrades: MarketTrade[];
  candles: Array<{ ts: number; price: number }>;
  binance: {
    spotPrice: number;
    latestTick?: { ts: number; price: number };
    candlesByInterval: Record<CandleInterval, CandleBar[]>;
  };
  coinbase: {
    referencePrice: number;
    settlementReference: number;
    currentRoundOpenReference?: number;
    candles5s: CandleBar[];
    candlesByInterval: Record<CandleInterval, CandleBar[]>;
  };
  clob: {
    delta: number;
    volume: number;
    upBook: OrderBookSnapshot;
    downBook: OrderBookSnapshot;
    recentTrades: MarketTrade[];
    currentRoundUpPriceSeries: CandlePoint[];
    marketInfo: ClobMarketInfo;
    bestBidAskSummary: Record<TradeSide, { bestBid: number; bestAsk: number }>;
  };
  uiMeta: {
    marketTitle: string;
    marketSubtitle?: string;
    countdownMs: number;
    countdownTargetTs?: number;
    acceptingOrders: boolean;
    marketSwitchState: MarketSwitchState;
    sourceStatusSummary: Array<{ source: SourceHealth["source"]; state: SourceHealth["state"] }>;
  };
}

export interface MarketPayload {
  viewedUserId: string;
  currentRound?: RoundRecord;
  history: HistoryRound[];
  snapshot: MarketSnapshot;
  settlementPreview?: SettlementPreview;
  transportMeta?: MarketTransportMeta;
}

export interface MarketRealtimeTick {
  symbol: string;
  marketId: string;
  marketSlug?: string;
  serverNow: number;
  currentPrice: number;
  binancePrice: number;
  coinbasePrice: number;
  priceToBeat: number;
  displayPriceToBeat?: number;
  displayPriceToBeatSource?: "official" | "binance_open_fallback";
  upPrice: number;
  downPrice: number;
  displayPrices: Record<TradeSide, number>;
  displayPriceSource: Record<TradeSide, DisplayPriceSource>;
  displayPriceSpread: Record<TradeSide, number>;
  latencyBreakdown: LatencyBreakdown;
  sources: Record<"binance" | "coinbase" | "clob", SourceHealth>;
  binance: {
    spotPrice: number;
    latestTick?: CandlePoint;
    candleUpdates?: Partial<Record<CandleInterval, CandleBar>>;
  };
  coinbase: {
    referencePrice: number;
    settlementReference: number;
    currentRoundOpenReference?: number;
    latestTick?: CandlePoint;
    candleUpdates?: Partial<Record<CandleInterval, CandleBar>>;
  };
  clob: {
    delta: number;
    volume: number;
    currentRoundUpPricePoint?: CandlePoint;
    bestBidAskSummary: Record<TradeSide, { bestBid: number; bestAsk: number }>;
    topLevels?: Record<TradeSide, { bids: BookLevel[]; asks: BookLevel[] }>;
  };
  uiMeta: {
    countdownMs: number;
    countdownTargetTs?: number;
    acceptingOrders: boolean;
    marketSwitchState: MarketSnapshot["uiMeta"]["marketSwitchState"];
    sourceStatusSummary: Array<{ source: SourceHealth["source"]; state: SourceHealth["state"] }>;
  };
}

export interface MarketTickPayload {
  viewedUserId: string;
  currentRound?: RoundRecord;
  tick: MarketRealtimeTick;
  settlementPreview?: SettlementPreview;
  transportMeta?: MarketTransportMeta;
}

export interface UserPayload {
  viewedUserId: string;
  viewedUser: PublicUser;
  profile: ProfileOverview;
  operatedHistory?: HistoryRound[];
  positions: PositionRecord[];
  orders: OrderRecord[];
  orderLifecycles: OrderLifecycleRecord[];
  logs: AuditEvent[];
}

export interface UserTradePayload {
  viewedUserId: string;
  profile: ProfileOverview;
  positions: PositionRecord[];
  orders: OrderRecord[];
  orderLifecycles: OrderLifecycleRecord[];
}

export interface BootstrapPayload extends MarketPayload, UserPayload {
  me: PublicUser;
  viewedUser: PublicUser;
  sourceStatus: SourceHealth[];
}

export interface LoginResponse extends PublicUser {
  token: string;
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
  priceToBeatSource?: string;
  priceToBeatCapturedAt?: number;
  status: RoundStatus;
  pollCount: number;
  pollStartAt?: number;
  lastPollAt?: number;
  closingSpotPrice?: number;
  settledSide?: TradeSide;
  settlementPrice?: number;
  settlementTs?: number;
  settlementSource?: "Polymarket" | "Gamma" | "Coinbase" | "CLOB";
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
  coinbaseOpenPrice?: number;
  coinbaseClosePrice?: number;
  redeemStartTs?: number;
  redeemFinishTs?: number;
  manualReason?: string;
  acceptingOrders?: boolean;
  closingPriceSource?: "Coinbase" | "Gamma";
  settlementPreview?: SettlementPreview;
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
  roundsParticipatedTotal?: number;
  roundsParticipatedToday: number;
}

export interface PositionRecord {
  id: string;
  buyOrderId?: string;
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
  entryFeeUsdc?: number;
  exitFeeUsdc?: number;
  totalFeeUsdc?: number;
  costBasisUsdc?: number;
  markPnlUsdc?: number;
  executablePnlUsdc?: number;
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
  estimatedFee?: number;
  actualFee?: number;
  feeBreakdown?: FeeBreakdown;
  feeCurrency?: FeeCurrency;
  sourceLatencyMs?: number;
  marketSlug?: string;
  orderBookSnapshotRef?: string;
  orderBookSnapshot?: OrderBookSnapshot;
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
  bookAcquireLatencyMs?: number;
  localMatchLatencyMs?: number;
  persistLatencyMs?: number;
  totalOrderLatencyMs?: number;
  failureReason?: string;
  clientOrderId?: string;
  clientSendTs?: number;
  serverRecvTs: number;
  serverPublishTs: number;
  createdAt: number;
}

export type OrderLifecycleExitType = "manual_sell" | "close_side" | "settlement" | "mixed";

export interface OrderLifecycleRecord {
  id: string;
  buyOrderId: string;
  traceId: string;
  userId: string;
  testerId: string;
  roundId: string;
  symbol: string;
  assetClass: "BTC";
  marketId: string;
  marketSlug?: string;
  direction: TradeSide;
  orderTimestampMs: number;
  entryTokenPrice?: number;
  btcTradePrice?: number;
  btcOpenPriceToBeat?: number;
  deltaBtc?: number;
  volumeTokenQty: number;
  remainingTokenQty: number;
  closedTokenQty: number;
  positionNotional: number;
  exitType?: OrderLifecycleExitType;
  exitTokenPrice?: number;
  exitNotional: number;
  settlementResult?: "win" | "loss";
  settlementTimeMs?: number;
  settlementDirection?: TradeSide;
  actualFillPrice?: number;
  slippageBps?: number;
  matchLatencyMs: number;
  entryFee?: number;
  exitFee?: number;
  feeCurrency?: FeeCurrency;
  createdAt: number;
  updatedAt: number;
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
  coinbasePrice: number;
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
  qualityGrade?: string;
  strategyClusterLabel?: string;
  marketRegimeLabel?: string;
}

export interface AuditLogQuery {
  from?: number;
  to?: number;
  viewUserId?: string;
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
  viewUserId?: string;
  userId?: string;
  roundId?: string;
  actionType?: string;
  actionStatus?: "success" | "failed" | "timeout";
  traceId?: string;
  orderId?: string;
  marketId?: string;
  marketSlug?: string;
}

export interface LogSearchQuery {
  system?: LogSystem;
  systems?: Array<Exclude<LogSystem, "all">>;
  from?: number;
  to?: number;
  viewUserId?: string;
  userId?: string;
  userIds?: string[];
  role?: Role;
  category?: LogCategory;
  actionType?: string;
  actionStatus?: "success" | "failed" | "timeout";
  moduleName?: string;
  pageName?: string;
  symbol?: string;
  roundId?: string;
  marketId?: string;
  marketSlug?: string;
  orderId?: string;
  positionId?: string;
  traceId?: string;
  resultCode?: string;
  direction?: TradeSide;
  roundStatus?: RoundStatus;
  settlementResult?: PositionRecord["settlementResult"];
  bookKey?: string;
  bookSide?: TradeSide;
  eventType?: MatchingEventType;
  sequenceFrom?: number;
  sequenceTo?: number;
  logGroup?: LogGroup;
  latencySource?: LatencySource;
  connectionState?: ConnectionState;
  latencyPhase?: LatencyPhase;
  latencyMinMs?: number;
  latencyMaxMs?: number;
  matchingLogKind?: MatchingLogKind;
  limit?: number;
  cursor?: string;
}

export interface UnifiedLogRow {
  id: string;
  system: Exclude<LogSystem, "all">;
  timestampMs: number;
  userId?: string;
  username?: string;
  displayName?: string;
  role?: Role;
  category?: LogCategory;
  actionType: string;
  actionStatus?: "success" | "failed" | "timeout";
  moduleName?: string;
  pageName?: string;
  symbol?: string;
  roundId?: string;
  marketId?: string;
  marketSlug?: string;
  orderId?: string;
  positionId?: string;
  traceId?: string;
  resultCode?: string;
  resultMessage?: string;
  direction?: TradeSide;
  roundStatus?: RoundStatus;
  settlementResult?: PositionRecord["settlementResult"];
  bookKey?: string;
  bookSide?: TradeSide;
  eventType?: MatchingEventType;
  sequence?: number;
  logGroup?: LogGroup;
  latencySource?: LatencySource;
  connectionState?: ConnectionState;
  latencyPhaseMetrics?: Partial<Record<LatencyPhase, number>>;
  matchingLogKind?: MatchingLogKind;
  payload?: Record<string, unknown>;
}

export interface LogSearchResult {
  rows: UnifiedLogRow[];
  nextCursor?: string;
  limit: number;
  system: LogSystem;
}

export interface LogFacets {
  audit: {
    categories: LogCategory[];
    actionTypes: string[];
    fields: string[];
    logGroups?: LogGroup[];
    latencySources?: LatencySource[];
    connectionStates?: ConnectionState[];
    latencyPhases?: LatencyPhase[];
  };
  training: {
    actionTypes: string[];
    fields: string[];
  };
  matching: {
    eventTypes: MatchingEventType[];
    fields: string[];
    kinds?: MatchingLogKind[];
  };
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

export interface CreateUserInput {
  username: string;
  password: string;
  displayName: string;
  role: Role;
  language?: Language;
  seniorTesterId?: string;
  managerUserId?: string;
  permissionLevel?: PermissionLevel;
  availableUsdc?: number;
}

export interface UpdateUserInput {
  displayName?: string;
  role?: Role;
  language?: Language;
  seniorTesterId?: string | null;
  managerUserId?: string | null;
  permissionLevel?: PermissionLevel;
  availableUsdc?: number;
  isActive?: boolean;
}

export interface BulkCreateUserInput {
  username: string;
  password: string;
  displayName?: string;
  role?: Role;
  language?: Language;
  seniorTesterId?: string;
  managerUserId?: string;
  permissionLevel?: PermissionLevel;
  mustChangePassword?: boolean;
  availableUsdc?: number;
}

export interface BulkCreateUserPreviewRow extends BulkCreateUserInput {
  rowNumber: number;
  displayName: string;
  role: Role;
  language: Language;
  permissionLevel: PermissionLevel;
  mustChangePassword: boolean;
  availableUsdc: number;
}

export interface BulkCreateUsersResult {
  total: number;
  created: Array<{
    rowNumber: number;
    user: PublicUser;
  }>;
  failed: Array<{
    rowNumber: number;
    username?: string;
      error: string;
  }>;
}

export interface BulkCreateUsersPreviewResult {
  total: number;
  valid: BulkCreateUserPreviewRow[];
  failed: BulkCreateUsersResult["failed"];
}

const RAW_API_BASE_URL = (import.meta.env.VITE_API_BASE_URL ?? "").trim();
const API_BASE_URL = RAW_API_BASE_URL || (import.meta.env.DEV ? "" : "http://127.0.0.1:8787");

function wsBaseUrl() {
  if (API_BASE_URL) {
    return API_BASE_URL.replace("http://", "ws://").replace("https://", "wss://");
  }
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${protocol}//${window.location.host}`;
}

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
  let data: (T & { message?: string; code?: string }) | undefined;
  if (text) {
    try {
      data = JSON.parse(text) as T & { message?: string; code?: string };
    } catch {
      throw new Error(redactNetworkAddresses(text || "Request failed."));
    }
  }
  if (!response.ok) {
    const error = new Error(redactNetworkAddresses(data?.message ?? "Request failed.")) as Error & { code?: string };
    if (data?.code) {
      error.code = data.code;
    }
    throw error;
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
      throw new Error(redactNetworkAddresses(parsed.message ?? "Request failed."));
    } catch {
      throw new Error(redactNetworkAddresses(text || "Request failed."));
    }
  }
  return text;
}

async function requestBlob(path: string, token?: string): Promise<Blob> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: {
      ...(token ? { Authorization: `Bearer ${token}` } : {})
    }
  });
  if (!response.ok) {
    const text = await response.text();
    let message = text || "Request failed.";
    try {
      const parsed = JSON.parse(text) as { message?: string };
      message = parsed.message ?? message;
    } catch {
      // Keep the plain response body as the error message.
    }
    throw new Error(redactNetworkAddresses(message));
  }
  return response.blob();
}

async function requestBlobPost(path: string, token: string, body: unknown): Promise<Blob> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });
  if (!response.ok) {
    const text = await response.text();
    let message = text || "Request failed.";
    try {
      const parsed = JSON.parse(text) as { message?: string };
      message = parsed.message ?? message;
    } catch {
      // Keep the plain response body as the error message.
    }
    throw new Error(redactNetworkAddresses(message));
  }
  return response.blob();
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

type LoginWireResponse = {
  token: string;
  user_id: string;
  role: Role;
  language: Language;
  display_name: string;
  permission_codes: string[];
  username: string;
  available_usdc: number;
  is_active: boolean;
  senior_tester_id?: string;
  manager_user_id?: string;
  permission_level?: PermissionLevel;
  created_at: number;
  updated_at: number;
};

function mapLoginResponse(input: LoginWireResponse): LoginResponse {
  return {
    token: input.token,
    id: input.user_id,
    username: input.username,
    displayName: input.display_name,
    role: input.role,
    language: input.language,
    permissionCodes: input.permission_codes,
    availableUsdc: input.available_usdc,
    isActive: input.is_active,
    seniorTesterId: input.senior_tester_id,
    managerUserId: input.manager_user_id ?? input.senior_tester_id,
    permissionLevel: input.permission_level ?? "Standard",
    createdAt: input.created_at,
    updatedAt: input.updated_at
  };
}

export const api = {
  async checkHealth() {
    const response = await fetch(`${API_BASE_URL}/health`, { signal: AbortSignal.timeout(2000) });
    return response.ok;
  },
  async sampleClockOffset() {
    const startedAt = Date.now();
    const response = await fetch(`${API_BASE_URL}/health`, { signal: AbortSignal.timeout(2000) });
    const receivedAt = Date.now();
    if (!response.ok) {
      return undefined;
    }
    const data = (await response.json()) as { serverNow?: number };
    if (typeof data.serverNow !== "number") {
      return undefined;
    }
    return Math.round((startedAt + receivedAt) / 2 - data.serverNow);
  },
  createWsUrl(path: string, token: string, viewUserId?: string) {
    const base = wsBaseUrl();
    const params = new URLSearchParams({ token });
    if (viewUserId) {
      params.set("viewUserId", viewUserId);
    }
    return `${base}${path}?${params.toString()}`;
  },
  createWsTicketUrl(path: string, ticket: string) {
    const base = wsBaseUrl();
    return `${base}${path}?ticket=${ticket}`;
  },
  createWsTicket(token: string, channel: "market" | "user", viewUserId?: string) {
    return request<{ ticket: string; expiresAt: number }>("/api/ws/tickets", token, {
      method: "POST",
      body: JSON.stringify({ channel, viewUserId })
    });
  },
  async login(username: string, password: string) {
    const data = await request<LoginWireResponse>("/api/auth/login", undefined, {
      method: "POST",
      body: JSON.stringify({ username, password })
    });
    return mapLoginResponse(data);
  },
  getBootstrap(token: string, viewUserId?: string) {
    return request<BootstrapPayload>(`/api/bootstrap/full${buildQuery({ viewUserId })}`, token);
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
  updateMe(token: string, input: { displayName?: string; language?: Language }) {
    return request<PublicUser>("/api/me", token, {
      method: "PATCH",
      body: JSON.stringify(input)
    });
  },
  changeMyPassword(token: string, input: { currentPassword: string; password: string; confirmPassword: string }) {
    return request<PublicUser>("/api/me/password", token, {
      method: "POST",
      body: JSON.stringify(input)
    });
  },
  getCurrentRound(token: string, viewUserId?: string) {
    return request<{
      viewedUserId?: string;
      currentRound?: RoundRecord;
      snapshot: MarketSnapshot;
      settlementPreview?: SettlementPreview;
      transportMeta?: MarketTransportMeta;
    }>(`/api/rounds/current${buildQuery({ viewUserId })}`, token);
  },
  getHistory(token: string, limit = 60, viewUserId?: string) {
    return request<HistoryRound[]>(`/api/rounds/history${buildQuery({ limit, viewUserId })}`, token);
  },
  manualSettleRound(token: string, roundId: string, input: { side: TradeSide; price?: number; reason?: string }) {
    return request<RoundRecord>(`/api/rounds/${roundId}/manual-settlement`, token, {
      method: "POST",
      body: JSON.stringify(input)
    });
  },
  getOperatedHistory(token: string, limit = 500, viewUserId?: string) {
    return request<HistoryRound[]>(`/api/profile/rounds/operated${buildQuery({ limit, viewUserId })}`, token);
  },
  getProfile(token: string, viewUserId?: string) {
    return request<ProfileOverview>(`/api/profile/me${buildQuery({ viewUserId })}`, token);
  },
  getPositions(token: string, viewUserId?: string) {
    return request<PositionRecord[]>(`/api/positions/me${buildQuery({ viewUserId })}`, token);
  },
  getOrders(token: string, viewUserId?: string) {
    return request<OrderRecord[]>(`/api/orders/me${buildQuery({ viewUserId })}`, token);
  },
  getOrderLifecycles(token: string, viewUserId?: string) {
    return request<OrderLifecycleRecord[]>(`/api/order-lifecycles/me${buildQuery({ viewUserId })}`, token);
  },
  getLogs(token: string, viewUserId?: string) {
    return request<AuditEvent[]>(`/api/logs/me${buildQuery({ viewUserId })}`, token);
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
  searchLogs(token: string, query?: LogSearchQuery) {
    const suffix = buildQuery(query);
    return request<LogSearchResult>(`/api/logs/search${suffix}`, token);
  },
  getLogFacets(token: string) {
    return request<LogFacets>("/api/logs/facets", token);
  },
  getRoundActivity(token: string, roundId: string, viewUserId?: string) {
    return request<{ auditLogs: AuditEvent[]; behaviorLogs: BehaviorActionLog[] }>(
      `/api/logs/round-activity${buildQuery({ roundId, viewUserId })}`,
      token
    );
  },
  exportAuditLogs(token: string, query?: AuditLogQuery) {
    const suffix = buildQuery(query);
    return requestText(`/api/logs/audit/export${suffix}`, token);
  },
  exportLogsZip(token: string, query?: LogSearchQuery) {
    const suffix = buildQuery(query);
    return requestBlob(`/api/logs/export${suffix}`, token);
  },
  exportLogsZipPost(token: string, query?: LogSearchQuery) {
    return requestBlobPost("/api/logs/export", token, query ?? {});
  },
  getTradeTimeline(token: string, orderId: string) {
    return request<TradeTimeline>(`/api/logs/trade-timeline${buildQuery({ orderId })}`, token);
  },
  getUsers(token: string) {
    return request<PublicUser[]>("/api/users", token);
  },
  createUser(token: string, input: CreateUserInput) {
    return request<PublicUser>("/api/users", token, {
      method: "POST",
      body: JSON.stringify(input)
    });
  },
  updateUser(token: string, userId: string, input: UpdateUserInput) {
    return request<PublicUser>(`/api/users/${userId}`, token, {
      method: "PATCH",
      body: JSON.stringify(input)
    });
  },
  changeUserGroup(token: string, userId: string, managerUserId: string) {
    return request<PublicUser>(`/api/users/${userId}/group`, token, {
      method: "PATCH",
      body: JSON.stringify({ managerUserId })
    });
  },
  bulkCreateUsers(token: string, users: BulkCreateUserInput[]) {
    return request<BulkCreateUsersResult>("/api/users/bulk", token, {
      method: "POST",
      body: JSON.stringify({ users })
    });
  },
  downloadBulkUsersTemplate(token: string) {
    return requestText("/api/users/bulk/template.csv", token);
  },
  previewBulkUsersCsv(token: string, csv: string) {
    return request<BulkCreateUsersPreviewResult>("/api/users/bulk/csv/preview", token, {
      method: "POST",
      body: JSON.stringify({ csv })
    });
  },
  bulkCreateUsersCsv(token: string, csv: string) {
    return request<BulkCreateUsersResult>("/api/users/bulk/csv", token, {
      method: "POST",
      body: JSON.stringify({ csv })
    });
  },
  disableUser(token: string, userId: string) {
    return request<PublicUser>(`/api/users/${userId}/disable`, token, {
      method: "POST"
    });
  },
  enableUser(token: string, userId: string) {
    return request<PublicUser>(`/api/users/${userId}/enable`, token, {
      method: "POST"
    });
  },
  resetUserPassword(token: string, userId: string, input: { currentPassword: string; password: string; confirmPassword: string }) {
    return request<PublicUser>(`/api/users/${userId}/reset-password`, token, {
      method: "POST",
      body: JSON.stringify(input)
    });
  },
  setUserBalance(token: string, userId: string, availableUsdc: number) {
    return request<PublicUser>(`/api/users/${userId}/balance`, token, {
      method: "POST",
      body: JSON.stringify({ availableUsdc })
    });
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
      clientOrderId?: string;
    }
  ) {
    const clientOrderId =
      input.clientOrderId ??
      (globalThis.crypto?.randomUUID
        ? globalThis.crypto.randomUUID()
        : `client_${Date.now()}_${Math.random().toString(36).slice(2)}`);
    return request<{ order: OrderRecord; tradePatch?: UserTradePayload }>("/api/orders", token, {
      method: "POST",
      body: JSON.stringify({
        ...input,
        clientOrderId,
        clientSendTs: Date.now()
      })
    });
  },
  cancelOrder(token: string, orderId: string) {
    return request<{ order: OrderRecord; tradePatch?: UserTradePayload }>(`/api/orders/${orderId}/cancel`, token, {
      method: "POST"
    });
  },
  sellPosition(token: string, positionId: string) {
    return request<{ order: OrderRecord; tradePatch?: UserTradePayload }>(`/api/positions/${positionId}/sell`, token, {
      method: "POST"
    });
  },
  closeSide(token: string, side: TradeSide) {
    return request<{
      closedPositionsCount: number;
      totalQty: number;
      totalProceeds: number;
      avgFillPrice?: number;
      matchLatencyMs: number;
      failures: Array<{ positionId: string; message: string }>;
      tradePatch?: UserTradePayload;
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
        matchLatencyMs: number;
        failures: Array<{ positionId: string; message: string }>;
      };
      reverseSide: TradeSide;
      reverseOrder: OrderRecord;
      tradePatch?: UserTradePayload;
    }>("/api/positions/reverse-side", token, {
      method: "POST",
      body: JSON.stringify({
        side,
        clientSendTs: Date.now()
      })
    });
  }
};

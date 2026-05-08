export type Language = "zh-CN" | "en-US";
export type Role = "Tester" | "Senior Tester" | "Test Engineer" | "Admin";
export type PermissionLevel = "Initial" | "Standard";
export type PermissionCode =
  | "trade:view"
  | "trade:order"
  | "trade:cancel"
  | "trade:sell"
  | "profile:view"
  | "profile:update"
  | "profile:password:change"
  | "system:status:view"
  | "audit:view"
  | "audit:export"
  | "users:list"
  | "users:create"
  | "users:bulk-create"
  | "users:update"
  | "users:disable"
  | "users:enable"
  | "users:reset-password"
  | "users:balance:set"
  | "users:manager:update"
  | "users:permission-level:update"
  | "logs:view:all"
  | "logs:view:team"
  | "logs:view:self"
  | "logs:view:managed"
  | "data:export:self"
  | "data:export:managed"
  | "data:export:all"
  | "data:export:include-d"
  | "quality:review"
  | "strategy:config"
  | "market:config";
export type TradeSide = "UP" | "DOWN";
export type OrderAction = "buy" | "sell";
export type OrderStatus = "pending" | "filled" | "partial" | "failed" | "cancelled";
export type PaperOrderKind = "market" | "limit";
export type PaperTimeInForce = "FOK" | "GTC";
export type DisplayPriceSource = "mid" | "last_trade" | "outcome_price";
export type PaperOrderResult = "pending" | "all_filled" | "all_failed" | "cancelled";
export type FeeCurrency = "USD";
export type MatchingOrderDirection = "bid" | "ask";
export type MatchingOrderType = "market" | "limit";
export type MatchingTimeInForce = "IOC" | "GTC";
export type MatchingOrderSource = "external" | "user";
export type MatchingEventType = "external_book_synced" | "order_executed" | "order_cancelled";
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
export type ConnectionState = "healthy" | "reconnecting" | "stale" | "degraded" | "disabled";
export type LogCategory = "operation" | "matching" | "settlement" | "latency";
export type LogSystem = "all" | "audit" | "training" | "matching";
export type LogGroup = "operation" | "settlement" | "market_latency" | "system_latency" | "matching_action";
export type LatencySource = "binance" | "chainlink" | "clob" | "system";
export type LatencyPhase = "backend" | "acquire" | "publish" | "frontend";
export type MatchingLogKind = "action" | "engine";

export interface UserRecord {
  id: string;
  username: string;
  password: string;
  displayName: string;
  role: Role;
  language: Language;
  permissionCodes: PermissionCode[];
  availableUsdc: number;
  isActive: boolean;
  seniorTesterId?: string;
  managerUserId?: string;
  permissionLevel?: PermissionLevel;
  failedLoginCount?: number;
  lockedUntil?: number;
  passwordChangedAt?: number;
  lastLoginAt?: number;
  mustChangePassword?: boolean;
  disabledAt?: number;
  disabledBy?: string;
  createdAt: number;
  updatedAt: number;
}

export interface PublicUser {
  id: string;
  username: string;
  displayName: string;
  role: Role;
  language: Language;
  permissionCodes: PermissionCode[];
  availableUsdc: number;
  isActive: boolean;
  seniorTesterId?: string;
  managerUserId?: string;
  permissionLevel?: PermissionLevel;
  lockedUntil?: number;
  passwordChangedAt?: number;
  lastLoginAt?: number;
  mustChangePassword?: boolean;
  disabledAt?: number;
  disabledBy?: string;
  createdAt: number;
  updatedAt: number;
}

export interface SourceHealth {
  source: "Binance" | "Chainlink" | "CLOB";
  symbol: string;
  state: ConnectionState;
  reconnectCount: number;
  sourceEventTs: number;
  serverRecvTs: number;
  normalizedTs: number;
  serverPublishTs: number;
  acquireLatencyMs: number;
  publishLatencyMs: number;
  frontendLatencyMs: number;
  message?: string;
}

export interface MarketTransportMeta {
  serverPublishTs: number;
  payloadSeq: number;
  coalescedCount?: number;
  serverQueueMs?: number;
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
  sourceEventAge: Record<"binance" | "chainlink" | "clob", number>;
  serverIngressLatency: Record<"binance" | "chainlink" | "clob", number>;
  serverComputeLatency: number;
  clientTransportLatency?: number;
}

export interface SettlementPreview {
  roundId: string;
  state: "preliminary" | "confirmed" | "manual";
  side?: TradeSide;
  price?: number;
  source: "CLOB" | "Gamma" | "Polymarket" | "Chainlink";
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

export interface OrderBookSnapshotRecord {
  ref: string;
  snapshotId: string;
  snapshotTs: number;
  bestBid: number;
  bestAsk: number;
  midPrice: number;
  snapshot: OrderBookSnapshot;
  createdAt: number;
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

export type CandleInterval = "30s" | "1m" | "5m" | "15m" | "1h" | "1d";

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

export interface MatchingBookOrder {
  id: string;
  ownerId: string;
  ownerType: MatchingOrderSource;
  bookKey: string;
  roundId?: string;
  marketId?: string;
  bookSide: TradeSide;
  direction: MatchingOrderDirection;
  orderType: MatchingOrderType;
  timeInForce: MatchingTimeInForce;
  price?: number;
  originalQty: number;
  remainingQty: number;
  createdAt: number;
  prioritySequence: number;
  meta?: Record<string, unknown>;
}

export interface MatchingFill {
  fillId: string;
  makerOrderId: string;
  takerOrderId: string;
  price: number;
  qty: number;
  notional: number;
  makerOwnerId: string;
  makerOwnerType: MatchingOrderSource;
  executedAt: number;
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
  displayPrices: Record<TradeSide, number>;
  displayPriceSource: Record<TradeSide, DisplayPriceSource>;
  displayPriceSpread: Record<TradeSide, number>;
  latencyBreakdown: LatencyBreakdown;
  sources: Record<"binance" | "chainlink" | "clob", SourceHealth>;
  orderBooks: Record<TradeSide, OrderBookSnapshot>;
  recentTrades: MarketTrade[];
  candles: CandlePoint[];
  binance: {
    spotPrice: number;
    latestTick?: CandlePoint;
    candlesByInterval: Record<CandleInterval, CandleBar[]>;
  };
  chainlink: {
    referencePrice: number;
    settlementReference: number;
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
    acceptingOrders: boolean;
    marketSwitchState: MarketSwitchState;
    sourceStatusSummary: Array<{ source: SourceHealth["source"]; state: ConnectionState }>;
  };
}

export interface MatchingBookState {
  bookKey: string;
  roundId?: string;
  marketId?: string;
  bookSide: TradeSide;
  sequence: number;
  prioritySequence: number;
  snapshot: OrderBookSnapshot;
  bids: MatchingBookOrder[];
  asks: MatchingBookOrder[];
  sourceSnapshotId?: string;
  updatedAt: number;
}

export interface MatchingSyncRequest {
  bookKey: string;
  roundId?: string;
  marketId?: string;
  bookSide: TradeSide;
  source: "Polymarket";
  sourceSnapshot: OrderBookSnapshot;
  syncedAt: number;
}

export interface MatchingExecutionRequest {
  orderId: string;
  traceId: string;
  userId: string;
  roundId?: string;
  marketId?: string;
  bookKey: string;
  bookSide: TradeSide;
  action: OrderAction;
  orderType: MatchingOrderType;
  timeInForce: MatchingTimeInForce;
  notional?: number;
  qty?: number;
  limitPrice?: number;
  createdAt: number;
  meta?: Record<string, unknown>;
}

export interface MatchingExecutionResult {
  request: MatchingExecutionRequest;
  status: "filled" | "partial" | "failed" | "resting";
  fills: MatchingFill[];
  filledQty: number;
  remainingQty: number;
  matchedNotional: number;
  remainingNotional?: number;
  avgPrice?: number;
  restingOrder?: MatchingBookOrder;
  beforeSnapshot: OrderBookSnapshot;
  afterSnapshot: OrderBookSnapshot;
  sequence: number;
  matchedAt: number;
  failureReason?: string;
}

export interface MatchingCancelResult {
  bookKey: string;
  orderId: string;
  cancelled: boolean;
  reason?: string;
  cancelledOrder?: MatchingBookOrder;
  beforeSnapshot: OrderBookSnapshot;
  afterSnapshot: OrderBookSnapshot;
  sequence: number;
  cancelledAt: number;
}

export interface MatchingEventRecord {
  eventId: string;
  bookKey: string;
  roundId?: string;
  marketId?: string;
  bookSide: TradeSide;
  sequence: number;
  eventType: MatchingEventType;
  orderId?: string;
  traceId?: string;
  payload: Record<string, unknown>;
  createdAt: number;
}

export interface MatchingReplayStep {
  event: MatchingEventRecord;
  snapshot: MatchingBookState;
}

export interface MatchingReplayResult {
  bookKey: string;
  latest?: MatchingBookState;
  steps: MatchingReplayStep[];
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
  marketInfo?: ClobMarketInfo;
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
  settlementSource?: "Polymarket" | "Gamma" | "Chainlink" | "CLOB";
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

export interface OrderRecord {
  id: string;
  traceId: string;
  userId: string;
  roundId: string;
  symbol: string;
  marketId: string;
  action: OrderAction;
  side: TradeSide;
  status: OrderStatus;
  orderKind?: PaperOrderKind;
  timeInForce?: PaperTimeInForce;
  limitPrice?: number;
  lifecycleStatus?: OrderStatus;
  resultType?: PaperOrderResult;
  tokenId?: string;
  bookKey?: string;
  bookHash?: string;
  requestedAmountUsdc?: number;
  requestedQty?: number;
  frozenUsdc?: number;
  frozenQty?: number;
  fills?: MatchingFill[];
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
  orderBookSnapshotRef?: string;
  orderBookSnapshot?: OrderBookSnapshot;
  actualFillPrice?: number;
  slippageBps?: number;
  matchLatencyMs: number;
  settlementTimeMs?: number;
  settlementDirection?: TradeSide;
  entryFee?: number;
  exitFee?: number;
  feeCurrency?: FeeCurrency;
  createdAt: number;
  updatedAt: number;
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

export interface AuditEvent {
  eventId: string;
  traceId: string;
  category: LogCategory;
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

export interface AuditLogQuery {
  from?: number;
  to?: number;
  userId?: string;
  userIds?: string[];
  roundId?: string;
  category?: LogCategory;
  actionType?: string;
  actionStatus?: AuditEvent["actionStatus"];
  traceId?: string;
  orderId?: string;
  positionId?: string;
  resultCode?: string;
}

export interface BehaviorLogQuery {
  from?: number;
  to?: number;
  userId?: string;
  userIds?: string[];
  roundId?: string;
  actionType?: string;
  actionStatus?: BehaviorActionLog["actionStatus"];
  traceId?: string;
  orderId?: string;
  marketId?: string;
  marketSlug?: string;
}

export interface MatchingLogQuery {
  from?: number;
  to?: number;
  userId?: string;
  userIds?: string[];
  roundId?: string;
  marketId?: string;
  bookKey?: string;
  bookSide?: TradeSide;
  eventType?: MatchingEventType;
  traceId?: string;
  orderId?: string;
  sequenceFrom?: number;
  sequenceTo?: number;
  limit?: number;
  offset?: number;
}

export interface LogSearchQuery {
  system?: LogSystem;
  systems?: Array<Exclude<LogSystem, "all">>;
  from?: number;
  to?: number;
  userId?: string;
  userIds?: string[];
  role?: Role;
  category?: LogCategory;
  actionType?: string;
  actionStatus?: AuditEvent["actionStatus"];
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
  actionStatus?: AuditEvent["actionStatus"];
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

export interface DatasetExportRequest {
  from?: number;
  to?: number;
  userIds?: string[];
  includeDGrade?: boolean;
  format?: "zip" | "parquet";
}

export interface DatasetExportPreview {
  recordCount: number;
  filteredDGradeCount: number;
  missingQualityCount: number;
  userCount: number;
  formats: Array<"csv" | "jsonl">;
}

export interface DatasetExportManifest {
  exportId: string;
  generatedAt: number;
  generatedAtIso: string;
  formats: Array<"csv" | "jsonl">;
  recordCount: number;
  filteredDGradeCount: number;
  missingQualityCount: number;
  userCount: number;
  scope: {
    userIds: string[];
    includeDGrade: boolean;
    from?: number;
    to?: number;
  };
  files: Array<{ path: string; sha256: string; rowCount?: number }>;
}

export interface DatasetExportAuditEvent {
  exportId: string;
  actorUserId: string;
  actorRole: Role;
  scope: DatasetExportManifest["scope"];
  formats: Array<"csv" | "jsonl">;
  recordCount: number;
  filteredDGradeCount: number;
  missingQualityCount: number;
  sha256: string;
  createdAtMs: number;
}

export interface LogSearchResult {
  rows: UnifiedLogRow[];
  nextCursor?: string;
  limit: number;
  system: LogSystem;
}

export interface TradeTimeline {
  order: OrderRecord;
  position?: PositionRecord;
  auditEvents: AuditEvent[];
  behaviorLogs: BehaviorActionLog[];
  matchingReplay?: MatchingReplayResult;
}

export interface ProfileOverview {
  totalEquity: number;
  availableUsdc: number;
  positionValue: number;
  realizedPnlToday: number;
  unrealizedPnl: number;
  winRate: number;
  roundsParticipatedTotal: number;
  roundsParticipatedToday: number;
}

export interface MarketPayload {
  snapshot: MarketSnapshot;
  currentRound?: RoundRecord;
  history: Array<RoundRecord & { userPnl: number; settlementPreview?: SettlementPreview }>;
  settlementPreview?: SettlementPreview;
  transportMeta: MarketTransportMeta;
}

export interface UserPayload {
  profile: ProfileOverview;
  operatedHistory: Array<RoundRecord & { userPnl: number }>;
  positions: PositionRecord[];
  orders: OrderRecord[];
  logs: AuditEvent[];
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
  estimatedFee?: number;
  actualFee?: number;
  feeBreakdown?: FeeBreakdown;
  feeCurrency?: FeeCurrency;
  settlementDirection?: TradeSide;
  settlementTimeMs?: number;
  gammaPollCount?: number;
  redeemFinishTimeMs?: number;
  sourceStates: Record<
    "binance" | "chainlink" | "clob",
    Pick<SourceHealth, "source" | "state" | "sourceEventTs" | "serverRecvTs" | "serverPublishTs">
  >;
  strategyClusterLabel?: string;
  marketRegimeLabel?: string;
  qualityGrade?: string;
  contextJson?: Record<string, unknown>;
}

export interface PolymarketMarketDetail {
  id: string;
  conditionId: string;
  slug: string;
  title: string;
  startAt: number;
  endAt: number;
  eventId: string;
  eventSlug: string;
  seriesSlug?: string;
  upTokenId: string;
  downTokenId: string;
  upOutcome: string;
  downOutcome: string;
  outcomePrices: [number, number];
  referencePrice?: number;
  referencePriceSource?: string;
  referenceOpenPrice?: number;
  referenceOpenPriceSource?: string;
  referenceClosePrice?: number;
  referenceClosePriceSource?: string;
  winningTokenId?: string;
  winningOutcome?: string;
  settlementPrice?: number;
  settlementStatus?: "pending" | "resolved" | "fallback" | "manual";
  settlementReceivedAt?: number;
  automaticallyResolved?: boolean;
  bestBid: number;
  bestAsk: number;
  lastTradePrice: number;
  acceptingOrders: boolean;
  closed: boolean;
  resolutionSource?: string;
  marketInfo?: ClobMarketInfo;
}

export interface BinanceConnectorState {
  price: number;
  candles: CandlePoint[];
  latestTick?: CandlePoint;
  candlesByInterval: Record<CandleInterval, CandleBar[]>;
  status: SourceHealth;
}

export interface ChainlinkConnectorState {
  price: number;
  updatedAt: number;
  status: SourceHealth;
}

export interface PolymarketConnectorState {
  currentMarket?: PolymarketMarketDetail;
  nextMarket?: PolymarketMarketDetail;
  discoveredRounds: RoundRecord[];
  orderBooks: Record<TradeSide, OrderBookSnapshot>;
  recentTrades: MarketTrade[];
  delta: number;
  volume: number;
  lastResolvedMarket?: PolymarketResolvedMarket;
  resolvedMarkets?: PolymarketResolvedMarket[];
  status: SourceHealth;
}

export interface PolymarketResolvedMarket {
    marketId: string;
    marketSlug: string;
    conditionId?: string;
    winningTokenId?: string;
    winningOutcome?: string;
    settledSide?: TradeSide;
    settlementPrice?: number;
    receivedAt: number;
}

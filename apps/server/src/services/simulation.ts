import type {
  AuditEvent,
  BehaviorActionLog,
  BinanceConnectorState,
  CandlePoint,
  CandleBar,
  CoinbaseConnectorState,
  ClobMarketInfo,
  DisplayPriceSource,
  FeeBreakdown,
  Language,
  MatchingBookState,
  MatchingFill,
  MarketCandleRecord,
  MarketSnapshot,
  MarketTrade,
  OrderLifecycleExitType,
  OrderRecord,
  OrderBookSnapshot,
  OrderAction,
  PaperOrderKind,
  PolymarketConnectorState,
  PolymarketMarketDetail,
  PolymarketResolvedMarket,
  PositionRecord,
  RoundRecord,
  RoundStatus,
  SettlementPreview,
  SourceHealth,
  TradeSide,
  UserRecord
} from "../domain/types";
import { BinanceConnector } from "./connectors/binance";
import { CoinbaseConnector } from "./connectors/coinbase";
import { estimateClobExecution, type ClobExecutionEstimate } from "./clob-execution";
import { calculateClobFees } from "./clob-fees";
import { MatchingServiceClient } from "./matching/client";
import { PolymarketConnector } from "./connectors/polymarket";
import { AppStore } from "./store";
import { appMetrics } from "./metrics";

const LATENCY_LOG_INTERVAL_MS = 15000;
const REDEEM_DELAY_MS = 2000;
const PRELIMINARY_SETTLEMENT_THRESHOLD = 0.9;
const GAMMA_SETTLED_WIN_PRICE_THRESHOLD = 0.995;
const GAMMA_SETTLED_LOSE_PRICE_THRESHOLD = 1 - GAMMA_SETTLED_WIN_PRICE_THRESHOLD;
const QTY_EPSILON = 0.0001;
const FIVE_MINUTE_MS = 5 * 60_000;
const EXECUTION_BOOK_FRESHNESS_FLOOR_MS = 5000;
const GAMMA_PREFETCH_START_MS = 180_000;
const GAMMA_PREFETCH_FAST_START_MS = 60_000;
const GAMMA_PREFETCH_END_MS = 0;
const GAMMA_PREFETCH_INTERVAL_MS = 2000;
const CONSERVATIVE_CLOB_MARKET_INFO: ClobMarketInfo = {
  minimumTickSize: 0.01,
  minimumOrderSize: 1,
  makerFeeRate: 0,
  takerFeeRate: 0,
  platformFeeRate: 0,
  platformFeeExponent: 1,
  platformFeeTakerOnly: true,
  feeRateAvailable: false,
  source: "conservative",
  conservative: true,
  updatedAt: 0
};
const TRADE_CHART_INTERVALS = ["30s", "1m", "5m", "15m", "1h"] as const;
const COINBASE_BAR_LIMITS: Record<(typeof TRADE_CHART_INTERVALS)[number], number> = {
  "30s": 120,
  "1m": 60,
  "5m": 30,
  "15m": 24,
  "1h": 24
};

type TradeLogTask = () => Promise<void>;
type TradePersistSegmentName =
  | "persistOrderBookSnapshot"
  | "persistOrder"
  | "persistPosition"
  | "persistUser"
  | "persistOrderLifecycle"
  | "commitAndOverhead"
  | "transactionTotal";
type TradePersistSegments = Partial<Record<TradePersistSegmentName, number>>;

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isClientOrderConflict(error: unknown) {
  if (!error || typeof error !== "object") {
    return false;
  }
  const candidate = error as { code?: unknown; constraint?: unknown; detail?: unknown; message?: unknown };
  if (candidate.code !== "23505") {
    return false;
  }
  return [candidate.constraint, candidate.detail, candidate.message]
    .filter(Boolean)
    .some((value) => String(value).includes("idx_orders_user_client_order_id"));
}

const COINBASE_INTERVAL_MS: Record<(typeof TRADE_CHART_INTERVALS)[number], number> = {
  "30s": 30_000,
  "1m": 60_000,
  "5m": 5 * 60_000,
  "15m": 15 * 60_000,
  "1h": 60 * 60_000
};
const COINBASE_MARKET_CANDLE_FLUSH_MS = 5000;
const COINBASE_MARKET_CANDLE_FLUSH_SIZE = 50;
const COINBASE_MARKET_CANDLE_RESTORE_MS = 24 * 60 * 60_000;
const COINBASE_HISTORY_CANDLE_SYNC_MIN_MS = 60_000;
const COINBASE_MARKET_CANDLE_PRIORITY: Record<MarketCandleRecord["origin"], number> = {
  history_1m_split: 1,
  rtds_30s: 2
};

function isPositivePrice(value?: number): value is number {
  return typeof value === "number" && Number.isFinite(value) && value > 0;
}

function latestTradePrice(trades: MarketTrade[], side: TradeSide): number | undefined {
  for (let index = trades.length - 1; index >= 0; index -= 1) {
    const trade = trades[index];
    if (trade.side === side && isPositivePrice(trade.price)) {
      return trade.price;
    }
  }
  return undefined;
}

function resolveDisplayPrice(input: {
  bestBid: number;
  bestAsk: number;
  lastTradePrice?: number;
  outcomePrice?: number;
}): { value: number; source: DisplayPriceSource; spread: number } {
  const bestBid = isPositivePrice(input.bestBid) ? input.bestBid : undefined;
  const bestAsk = isPositivePrice(input.bestAsk) ? input.bestAsk : undefined;
  if (bestBid === undefined || bestAsk === undefined) {
    return {
      value: 0,
      source: "outcome_price",
      spread: 0
    };
  }
  const spread = bestBid !== undefined && bestAsk !== undefined ? Math.max(bestAsk - bestBid, 0) : 0;
  const midPrice = bestBid !== undefined && bestAsk !== undefined ? (bestBid + bestAsk) / 2 : undefined;

  if (midPrice !== undefined) {
    if (spread > 0.1) {
      if (isPositivePrice(input.lastTradePrice)) {
        return {
          value: roundNumber(input.lastTradePrice, 4),
          source: "last_trade",
          spread: roundNumber(spread, 4)
        };
      }
      return {
        value: 0,
        source: "outcome_price",
        spread: roundNumber(spread, 4)
      };
    }
    return {
      value: roundNumber(midPrice, 4),
      source: "mid",
      spread: roundNumber(spread, 4)
    };
  }

  if (isPositivePrice(input.lastTradePrice)) {
    return {
      value: roundNumber(input.lastTradePrice, 4),
      source: "last_trade",
      spread: roundNumber(spread, 4)
    };
  }

  if (isPositivePrice(input.outcomePrice)) {
    return {
      value: roundNumber(input.outcomePrice, 4),
      source: "outcome_price",
      spread: roundNumber(spread, 4)
    };
  }

  return {
    value: 0,
    source: "outcome_price",
    spread: roundNumber(spread, 4)
  };
}

function resolvePairedDisplayPrices(input: {
  upBook: OrderBookSnapshot;
  downBook: OrderBookSnapshot;
  recentTrades: MarketTrade[];
  outcomePrices?: [number, number];
}): Record<TradeSide, { value: number; source: DisplayPriceSource; spread: number }> {
  const upAskDepthAvailable = input.upBook.asks.length > 0 && isPositivePrice(input.upBook.bestAsk);
  const downAskDepthAvailable = input.downBook.asks.length > 0 && isPositivePrice(input.downBook.bestAsk);
  if (!upAskDepthAvailable && !downAskDepthAvailable) {
    return {
      UP: { value: 0, source: "outcome_price", spread: 0 },
      DOWN: { value: 0, source: "outcome_price", spread: 0 }
    };
  }
  if (!upAskDepthAvailable) {
    return {
      UP: { value: 0, source: "outcome_price", spread: 0 },
      DOWN: { value: 0.01, source: "outcome_price", spread: 0 }
    };
  }
  if (!downAskDepthAvailable) {
    return {
      UP: { value: 0.01, source: "outcome_price", spread: 0 },
      DOWN: { value: 0, source: "outcome_price", spread: 0 }
    };
  }
  return {
    UP: resolveDisplayPrice({
      bestBid: input.upBook.bestBid,
      bestAsk: input.upBook.bestAsk,
      lastTradePrice: latestTradePrice(input.recentTrades, "UP"),
      outcomePrice: input.outcomePrices?.[0]
    }),
    DOWN: resolveDisplayPrice({
      bestBid: input.downBook.bestBid,
      bestAsk: input.downBook.bestAsk,
      lastTradePrice: latestTradePrice(input.recentTrades, "DOWN"),
      outcomePrice: input.outcomePrices?.[1]
    })
  };
}

function isMarketResolved(detail: PolymarketMarketDetail): boolean {
  if (detail.automaticallyResolved) return true;
  if (detail.winningTokenId) return true;
  if (detail.winningOutcome) return true;
  if (detail.closed) {
    const [up, down] = detail.outcomePrices;
    if (up >= GAMMA_SETTLED_WIN_PRICE_THRESHOLD && down <= GAMMA_SETTLED_LOSE_PRICE_THRESHOLD) return true;
    if (down >= GAMMA_SETTLED_WIN_PRICE_THRESHOLD && up <= GAMMA_SETTLED_LOSE_PRICE_THRESHOLD) return true;
  }
  return false;
}

function isBtcReferencePrice(value?: number): value is number {
  return typeof value === "number" && Number.isFinite(value) && value > 1000;
}

function isOfficialPtbSource(source?: string) {
  const normalized = source?.toLowerCase() ?? "";
  return normalized.includes("coinbase");
}

const roundNumber = (value: number, digits = 2) => Number(value.toFixed(digits));
const roundCurrency = (value: number) => roundNumber(value, 6);

function clobMarketInfoFor(market?: PolymarketMarketDetail): ClobMarketInfo {
  return market?.marketInfo ?? {
    ...CONSERVATIVE_CLOB_MARKET_INFO,
    conditionId: market?.conditionId,
    updatedAt: Date.now()
  };
}

function isAlignedToTick(price: number, tickSize: number) {
  if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(tickSize) || tickSize <= 0) {
    return false;
  }
  const units = price / tickSize;
  return Math.abs(units - Math.round(units)) < 0.000001;
}

function calculateClobFee(input: {
  role: "maker" | "taker";
  marketInfo: ClobMarketInfo;
  price?: number;
  quantity: number;
  notional: number;
}): { fee: number; breakdown?: FeeBreakdown } {
  const price = input.price ?? input.notional / Math.max(input.quantity, QTY_EPSILON);
  const result = calculateClobFees({
    role: input.role,
    feeRate: input.marketInfo.takerFeeRate,
    platformFeeRate: input.marketInfo.platformFeeRate ?? input.marketInfo.takerFeeRate,
    platformFeeExponent: input.marketInfo.platformFeeExponent,
    platformFeeTakerOnly: input.marketInfo.platformFeeTakerOnly,
    price,
    quantity: input.quantity,
    notional: input.notional,
    digits: 6
  });
  return { fee: roundCurrency(result.fee), breakdown: result.breakdown };
}

function createEmptyCoinbaseIntervalBars() {
  return {
    "30s": [] as CandleBar[],
    "1m": [] as CandleBar[],
    "5m": [] as CandleBar[],
    "15m": [] as CandleBar[],
    "1h": [] as CandleBar[]
  };
}

function normalizeCoinbaseBar(interval: (typeof TRADE_CHART_INTERVALS)[number], bar: CandleBar): CandleBar {
  const bucketSize = COINBASE_INTERVAL_MS[interval];
  const startTs = Math.floor(bar.startTs / bucketSize) * bucketSize;
  return {
    interval,
    startTs,
    endTs: startTs + bucketSize,
    open: roundNumber(bar.open, 2),
    high: roundNumber(bar.high, 2),
    low: roundNumber(bar.low, 2),
    close: roundNumber(bar.close, 2),
    volume: roundNumber(bar.volume ?? 0, 6)
  };
}

function mergeCoinbaseHistoryBars(
  current: CandleBar[],
  incoming: CandleBar[] | undefined,
  interval: (typeof TRADE_CHART_INTERVALS)[number]
) {
  if (!incoming?.length) {
    return current;
  }

  const barsByStartTs = new Map<number, CandleBar>();
  for (const bar of incoming) {
    if (isPositivePrice(bar.close) && isPositivePrice(bar.high) && isPositivePrice(bar.low)) {
      const normalized = normalizeCoinbaseBar(interval, bar);
      barsByStartTs.set(normalized.startTs, normalized);
    }
  }
  for (const bar of current) {
    if (isPositivePrice(bar.close) && isPositivePrice(bar.high) && isPositivePrice(bar.low)) {
      const normalized = normalizeCoinbaseBar(interval, bar);
      barsByStartTs.set(normalized.startTs, normalized);
    }
  }

  return [...barsByStartTs.values()]
    .sort((left, right) => left.startTs - right.startTs)
    .slice(-COINBASE_BAR_LIMITS[interval]);
}

function aggregateCoinbaseBars(
  interval: Exclude<(typeof TRADE_CHART_INTERVALS)[number], "30s">,
  sourceBars: CandleBar[]
) {
  const bucketSize = COINBASE_INTERVAL_MS[interval];
  const grouped = new Map<number, CandleBar>();
  for (const bar of [...sourceBars].sort((left, right) => left.startTs - right.startTs)) {
    if (!isPositivePrice(bar.close) || !isPositivePrice(bar.high) || !isPositivePrice(bar.low)) {
      continue;
    }
    const startTs = Math.floor(bar.startTs / bucketSize) * bucketSize;
    const existing = grouped.get(startTs);
    if (!existing) {
      grouped.set(startTs, {
        interval,
        startTs,
        endTs: startTs + bucketSize,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: bar.volume
      });
      continue;
    }
    existing.high = roundNumber(Math.max(existing.high, bar.high), 2);
    existing.low = roundNumber(Math.min(existing.low, bar.low), 2);
    existing.close = roundNumber(bar.close, 2);
    existing.volume = roundNumber(existing.volume + bar.volume, 6);
  }
  return [...grouped.values()]
    .sort((left, right) => left.startTs - right.startTs)
    .slice(-COINBASE_BAR_LIMITS[interval]);
}

function marketCandleToBar(candle: MarketCandleRecord): CandleBar {
  return {
    interval: "30s",
    startTs: candle.openTs,
    endTs: candle.closeTs,
    open: candle.open,
    high: candle.high,
    low: candle.low,
    close: candle.close,
    volume: candle.volume
  };
}

function shouldReplaceCoinbaseMarketCandle(existing: MarketCandleRecord | undefined, incoming: MarketCandleRecord) {
  if (!existing) {
    return true;
  }
  const existingPriority = COINBASE_MARKET_CANDLE_PRIORITY[existing.origin];
  const incomingPriority = COINBASE_MARKET_CANDLE_PRIORITY[incoming.origin];
  return incomingPriority > existingPriority || (incomingPriority === existingPriority && incoming.updatedAt >= existing.updatedAt);
}

function cloneOrderBookSnapshot(snapshot: OrderBookSnapshot): OrderBookSnapshot {
  return {
    snapshotId: snapshot.snapshotId,
    snapshotTs: snapshot.snapshotTs,
    bestBid: snapshot.bestBid,
    bestAsk: snapshot.bestAsk,
    midPrice: snapshot.midPrice,
    bids: snapshot.bids.map((level) => ({ ...level })),
    asks: snapshot.asks.map((level) => ({ ...level }))
  };
}

function hasOrderBookDepth(snapshot?: OrderBookSnapshot) {
  return Boolean(snapshot && (snapshot.bids.length > 0 || snapshot.asks.length > 0));
}

function cloneCandlePoint(point: CandlePoint): CandlePoint {
  return {
    ts: point.ts,
    price: point.price
  };
}

type ExecutionBookResult = {
  book: OrderBookSnapshot;
  source: "cache" | "stale_cache" | "rest" | "rest_empty_cache_fallback";
  ageMs: number;
  fallbackReason?: string;
};

function pad2(value: number) {
  return String(value).padStart(2, "0");
}

function utcRangeText(startAt: number, endAt: number) {
  const start = new Date(startAt);
  const end = new Date(endAt);
  return `${pad2(start.getUTCHours())}:${pad2(start.getUTCMinutes())}-${pad2(end.getUTCHours())}:${pad2(end.getUTCMinutes())} UTC`;
}

function createDisabledCoinbaseState(symbol: string): CoinbaseConnectorState {
  const now = Date.now();
  return {
    price: 0,
    updatedAt: 0,
    status: {
      source: "Coinbase",
      symbol,
      state: "disabled",
      reconnectCount: 0,
      sourceEventTs: now,
      serverRecvTs: now,
      normalizedTs: now,
      serverPublishTs: now,
      acquireLatencyMs: 0,
      publishLatencyMs: 0,
      frontendLatencyMs: 0,
      message: "Coinbase is disabled in local testing mode."
    }
  };
}

export class SimulationEngine {
  private readonly binanceConnector: BinanceConnector;
  private readonly coinbaseConnector: CoinbaseConnector;
  private readonly polymarketConnector: PolymarketConnector;
  private binanceState: BinanceConnectorState;
  private coinbaseState: CoinbaseConnectorState;
  private coinbaseCandles5s: CandleBar[] = [];
  private coinbaseCandlesByInterval = createEmptyCoinbaseIntervalBars();
  private polymarketState: PolymarketConnectorState;
  private currentRoundUpPriceSeries: CandlePoint[] = [];
  private currentRoundUpPriceSeriesRoundId?: string;
  private currentRoundBinanceOpenReferences = new Map<string, number>();
  private lastCoinbaseSampleKey?: string;
  private lastCoinbaseHistorySyncKey?: string;
  private lastCoinbaseHistorySyncAt = 0;
  private pendingCoinbaseMarketCandles = new Map<number, MarketCandleRecord>();
  private coinbaseMarketCandleFlushTimer?: NodeJS.Timeout;
  private coinbaseMarketCandleFlushRunning = false;
  private readonly unsubscribers: Array<() => void> = [];
  private reconcileTimer?: NodeJS.Timeout;
  private reconcileRunning = false;
  private reconcileQueued = false;
  private snapshotRefreshRunning = false;
  private snapshotRefreshQueued = false;
  private snapshotRefreshQueuedAt?: number;
  private readonly snapshotRefreshSources = new Set<"binance" | "coinbase" | "clob">();
  private readonly pollLocks = new Set<string>();
  private redeemLocks = new Set<string>();
  private readonly lastLatencyLogAt = new Map<string, number>();
  private readonly lastLatencyState = new Map<string, string>();
  private queuedLatencySnapshot?: MarketSnapshot;
  private latencyLogsRunning = false;
  private readonly matchingBooks = new Map<string, MatchingBookState>();
  private readonly currentBookKeys = new Map<TradeSide, string>();
  private readonly lastSyncedSnapshotIds = new Map<string, string>();
  private preliminarySettlements = new Map<string, SettlementPreview>();
  private gammaOutcomeConfirmations = new Map<string, { side: TradeSide; count: number; observedAt: number }>();
  private gammaSettlementDiagnostics = new Set<string>();
  private marketSyncSlug?: string;
  private pendingOrdersRunning = false;
  private tradeLogQueue: TradeLogTask[] = [];
  private tradeLogFlushScheduled = false;
  private tradeLogFlushRunning = false;

  constructor(
    private readonly store: AppStore,
    private readonly matchingClient: MatchingServiceClient,
    private readonly config: {
      symbol: string;
      marketId: string;
      freezeWindowMs: number;
      pollDelayMs: number;
      gammaPollIntervalMs: number;
      binanceRestUrl: string;
      binanceFallbackRestUrl: string;
      binanceFallbackRestPollMs: number;
      binanceWsUrl: string;
      binanceRequestTimeoutMs: number;
      binanceRestPollMs: number;
      binanceWsStaleMs: number;
      upstreamProxyUrl?: string;
      coinbaseEnabled: boolean;
      coinbaseWsUrl: string;
      coinbaseRestUrl: string;
      coinbaseRestPollMs: number;
      coinbaseRequestTimeoutMs: number;
      coinbaseWsStaleMs: number;
      gammaBaseUrl: string;
      clobBaseUrl: string;
      dataApiBaseUrl: string;
      polymarketMarketId?: string;
      polymarketMarketSlug?: string;
      polymarketSearchQuery: string;
      polymarketSeriesSlug: string;
      polymarketDiscoveryTimeoutMs: number;
      polymarketDiscoveryKeywords: string[];
      marketDiscoveryIntervalMs: number;
      marketSnapshotIntervalMs: number;
      marketFullReconcileIntervalMs: number;
      polymarketBookPollMs: number;
      polymarketBookCalibrationMs: number;
      polymarketTradesPollMs: number;
    }
  ) {
    this.binanceConnector = new BinanceConnector({
      symbol: config.symbol,
      wsUrl: config.binanceWsUrl,
      restUrl: config.binanceRestUrl,
      fallbackRestUrl: config.binanceFallbackRestUrl,
      fallbackRestPollMs: config.binanceFallbackRestPollMs,
      requestTimeoutMs: config.binanceRequestTimeoutMs,
      restPollMs: config.binanceRestPollMs,
      wsStaleMs: config.binanceWsStaleMs,
      upstreamProxyUrl: config.upstreamProxyUrl
    });
    this.coinbaseConnector = new CoinbaseConnector({
      symbol: config.symbol,
      wsUrl: config.coinbaseWsUrl,
      restUrl: config.coinbaseRestUrl,
      restPollMs: config.coinbaseRestPollMs,
      requestTimeoutMs: config.coinbaseRequestTimeoutMs,
      wsStaleMs: config.coinbaseWsStaleMs,
      upstreamProxyUrl: config.upstreamProxyUrl
    });
    this.polymarketConnector = new PolymarketConnector({
      symbol: config.symbol,
      gammaBaseUrl: config.gammaBaseUrl,
      clobBaseUrl: config.clobBaseUrl,
      dataApiBaseUrl: config.dataApiBaseUrl,
      marketId: config.polymarketMarketId,
      marketSlug: config.polymarketMarketSlug,
      searchQuery: config.polymarketSearchQuery,
      seriesSlug: config.polymarketSeriesSlug,
      discoveryKeywords: config.polymarketDiscoveryKeywords,
      discoveryTimeoutMs: config.polymarketDiscoveryTimeoutMs,
      discoveryIntervalMs: config.marketDiscoveryIntervalMs,
      bookPollMs: config.polymarketBookCalibrationMs,
      tradesPollMs: config.polymarketTradesPollMs,
      upstreamProxyUrl: config.upstreamProxyUrl
    });
    this.binanceState = this.binanceConnector.getState();
    this.coinbaseState = config.coinbaseEnabled
      ? this.coinbaseConnector.getState()
      : createDisabledCoinbaseState(config.symbol);
    this.polymarketState = this.polymarketConnector.getState();
  }

  async start() {
    if (this.config.coinbaseEnabled) {
      await this.restoreCoinbaseMarketCandles();
    }
    this.unsubscribers.push(
      this.binanceConnector.subscribe((state) => {
        this.binanceState = state;
        this.scheduleSnapshotOnlyRefresh("binance");
      }),
      this.polymarketConnector.subscribe((state) => {
        this.polymarketState = state;
        this.scheduleSnapshotOnlyRefresh("clob");
      })
    );
    if (this.config.coinbaseEnabled) {
      this.unsubscribers.push(
        this.coinbaseConnector.subscribe((state) => {
          this.coinbaseState = state;
          this.recordCoinbaseSample(state.price, state.updatedAt || Date.now());
          this.syncCoinbaseHistoryCandles(state.candlesByInterval);
          this.scheduleSnapshotOnlyRefresh("coinbase");
        })
      );
    }

    this.binanceConnector.start();
    if (this.config.coinbaseEnabled) {
      this.coinbaseConnector.start();
    }
    this.polymarketConnector.start();
    this.reconcileTimer = setInterval(
      () => this.scheduleFullReconcile(),
      Math.max(this.config.marketFullReconcileIntervalMs, 250)
    );
    this.scheduleFullReconcile();
  }

  async stop() {
    if (this.reconcileTimer) {
      clearInterval(this.reconcileTimer);
      this.reconcileTimer = undefined;
    }
    while (this.unsubscribers.length > 0) {
      this.unsubscribers.pop()?.();
    }
    this.binanceConnector.stop();
    if (this.config.coinbaseEnabled) {
      this.coinbaseConnector.stop();
    }
    if (this.coinbaseMarketCandleFlushTimer) {
      clearTimeout(this.coinbaseMarketCandleFlushTimer);
      this.coinbaseMarketCandleFlushTimer = undefined;
    }
    await this.flushPendingCoinbaseMarketCandles();
    this.polymarketConnector.stop();
  }

  getSettlementPreview(round?: RoundRecord): SettlementPreview | undefined {
    if (!round) {
      return undefined;
    }
    if (round.settledSide) {
      return {
        roundId: round.id,
        state: "confirmed",
        side: round.settledSide,
        price: round.settlementPrice,
        source: round.settlementSource ?? "Gamma",
        detectedAt: round.settlementReceivedAt ?? round.settlementTs,
        message: `${round.settlementSource ?? "Gamma"} confirmed ${round.settledSide}.`
      };
    }
    if (round.status === "Manual") {
      return {
        roundId: round.id,
        state: "manual",
        source: "Gamma",
        detectedAt: round.lastPollAt,
        message: round.manualReason ?? "Gamma polling timed out."
      };
    }
    return this.getPreliminarySettlements().get(round.id) ?? this.createPreliminarySettlementPreview(round, Date.now());
  }

  getLatestSettlementPreview(rounds: RoundRecord[] = this.store.getHistory(10)): SettlementPreview | undefined {
    return rounds
      .map((round) => this.getSettlementPreview(round))
      .filter((preview): preview is SettlementPreview => Boolean(preview))
      .sort((left, right) => (right.detectedAt ?? 0) - (left.detectedAt ?? 0))[0];
  }

  async manualSettleRound(actor: UserRecord, input: { roundId: string; side: TradeSide; price?: number; reason?: string }) {
    const round = this.store.getRoundById(input.roundId);
    if (!round) {
      throw new Error("Round was not found.");
    }
    if (round.status === "Closed" || round.redeemFinishTs) {
      throw new Error("Round is already closed.");
    }
    const now = Date.now();
    round.settledSide = input.side;
    round.polymarketSettlementPrice = typeof input.price === "number" ? input.price : input.side === "UP" ? 1 : 0;
    round.polymarketSettlementStatus = "manual";
    round.settlementPrice = round.polymarketSettlementPrice;
    round.settlementTs = now;
    round.settlementReceivedAt = now;
    round.settlementSource = "Gamma";
    round.status = "Settled";
    round.acceptingOrders = false;
    round.manualReason = input.reason || `Manual settlement entered by ${actor.username}.`;
    round.redeemStartTs = now;
    round.redeemScheduledAt = now + REDEEM_DELAY_MS;
    this.getPreliminarySettlements().delete(round.id);
    await this.store.upsertRound(round);
    await this.writeAuditLog({
      eventId: this.store.newId("evt"),
      traceId: this.store.newTraceId(),
      category: "settlement",
      actionType: "manual_settlement",
      actionStatus: "success",
      userId: actor.id,
      role: actor.role,
      pageName: "trade.main",
      moduleName: "settlement.manual",
      symbol: round.symbol,
      roundId: round.id,
      serverRecvTs: now,
      serverPublishTs: now,
      backendLatencyMs: 0,
      resultCode: "MANUAL_SETTLEMENT_CONFIRMED",
      resultMessage: `Manual settlement confirmed ${input.side}.`,
      details: {
        roundId: round.id,
        marketId: round.marketId,
        marketSlug: round.marketSlug,
        settlementSide: input.side,
        settlementPrice: round.settlementPrice,
        reason: input.reason
      }
    });
    for (const userId of this.collectRoundPositionUsers(round.id)) {
      this.store.emitUserPayload(userId);
    }
    this.scheduleReconcile();
    return round;
  }

  private getPreliminarySettlements() {
    this.preliminarySettlements ??= new Map<string, SettlementPreview>();
    return this.preliminarySettlements;
  }

  private async runTradeWriteTransaction<T>(handler: () => Promise<T>) {
    if (
      typeof this.store.withTransaction !== "function" ||
      typeof this.store.captureTradeMutationSnapshot !== "function" ||
      typeof this.store.restoreTradeMutationSnapshot !== "function"
    ) {
      return handler();
    }
    const memorySnapshot = this.store.captureTradeMutationSnapshot();
    try {
      return await this.store.withTransaction(handler);
    } catch (error) {
      this.store.restoreTradeMutationSnapshot(memorySnapshot);
      throw error;
    }
  }

  private async measureTradePersistSegment<T>(
    segments: TradePersistSegments | undefined,
    name: TradePersistSegmentName,
    handler: () => Promise<T> | T
  ) {
    const startedAt = Date.now();
    try {
      return await handler();
    } finally {
      if (segments) {
        segments[name] = (segments[name] ?? 0) + Math.max(Date.now() - startedAt, 0);
      }
    }
  }

  async placeOrder(
    user: UserRecord,
    payload: {
      action?: OrderAction;
      side: TradeSide;
      amount?: number;
      qty?: number;
      orderKind?: PaperOrderKind;
      limitPrice?: number;
      clientOrderId?: string;
      clientSendTs?: number;
      positionIds?: string[];
      exitType?: Exclude<OrderLifecycleExitType, "settlement" | "mixed">;
    }
  ): Promise<{ order: OrderRecord }> {
    const snapshot = this.captureActionSnapshot();
    const traceId = this.store.newTraceId();
    const now = Date.now();
    const currentRound = this.getActiveRound(now);
    const action = payload.action ?? "buy";
    const orderKind = payload.orderKind ?? "market";
    const clientOrderId = payload.clientOrderId?.trim();
    try {
      const existingOrder =
        typeof this.store.findOrderByClientOrderId === "function"
          ? await this.store.findOrderByClientOrderId(user.id, clientOrderId)
          : undefined;
      if (existingOrder) {
        return { order: existingOrder };
      }
      if (action === "buy") {
        this.assertCanBuyOrder(currentRound, now);
      } else {
        this.assertCanSellOrder(currentRound, now);
      }
      if (orderKind === "limit" && (!payload.limitPrice || payload.limitPrice <= 0)) {
        throw new Error("Limit orders require a positive limit price.");
      }
      if (action === "buy" && (!payload.amount || payload.amount <= 0)) {
        throw new Error("Buy orders require a positive USDC amount.");
      }
      if (action === "sell" && (!payload.qty || payload.qty <= 0)) {
        throw new Error("Sell orders require a positive quantity.");
      }
      if (action === "sell") {
        const availableQty = this.availableSellQty(user.id, currentRound.id, payload.side, payload.positionIds);
        if (availableQty + QTY_EPSILON < (payload.qty ?? 0)) {
          throw new Error("Insufficient unlocked position quantity.");
        }
      }

      const serverRecvTs = Date.now();
      const orderId = this.store.newId("ord");
      const engineStartTs = Date.now();
      const bookAcquireStartTs = Date.now();
      const executionBook = await this.fetchExecutionBook(payload.side, currentRound);
      const bookAcquireFinishTs = Date.now();
      const book = executionBook.book;
      const orderBookSnapshot = cloneOrderBookSnapshot(book);
      const tokenId = this.resolveTokenId(payload.side, currentRound);
      const { bookKey, marketId } = this.resolveBookContext(payload.side, currentRound);
      const marketInfo = clobMarketInfoFor(this.polymarketState.currentMarket);
      if (orderKind === "limit" && !isAlignedToTick(payload.limitPrice ?? 0, marketInfo.minimumTickSize)) {
        throw new Error(`Limit price must align to CLOB tick size ${marketInfo.minimumTickSize}.`);
      }
      const requestedSize = action === "buy" ? payload.amount ?? 0 : payload.qty ?? 0;
      if (requestedSize + QTY_EPSILON < marketInfo.minimumOrderSize) {
        throw new Error(`Order size must be at least CLOB minimum order size ${marketInfo.minimumOrderSize}.`);
      }
      const expectedQty =
        action === "buy"
          ? roundNumber((payload.amount ?? 0) / Math.max(payload.limitPrice ?? book.bestAsk, 0.0001), 4)
          : roundNumber(payload.qty ?? 0, 4);
      const localMatchStartTs = Date.now();
      const estimate = estimateClobExecution({
        action,
        book,
        orderId,
        notional: action === "buy" ? payload.amount : undefined,
        qty: action === "sell" ? payload.qty : undefined,
        limitPrice: payload.limitPrice,
        feeRate: marketInfo.takerFeeRate,
        platformFeeRate: marketInfo.platformFeeRate ?? marketInfo.takerFeeRate,
        platformFeeExponent: marketInfo.platformFeeExponent,
        platformFeeTakerOnly: marketInfo.platformFeeTakerOnly,
        feeRole: "taker",
        executedAt: engineStartTs
      });
      const engineFinishTs = Date.now();
      const sourceLatencyMs = Math.max(engineFinishTs - book.snapshotTs, 0);
      const bookAcquireLatencyMs = Math.max(bookAcquireFinishTs - bookAcquireStartTs, 0);
      const localMatchLatencyMs = Math.max(engineFinishTs - localMatchStartTs, 1);
      const shouldRest = orderKind === "limit" && !estimate.fullyMatched;
      const status = shouldRest ? "pending" : estimate.fullyMatched ? "filled" : "failed";
      const pendingFeeEstimate = shouldRest
        ? calculateClobFee({
            role: "taker",
            marketInfo,
            price: action === "buy" ? Math.max(marketInfo.minimumTickSize, 0.0001) : payload.limitPrice,
            quantity:
              action === "buy"
                ? roundNumber((payload.amount ?? 0) / Math.max(marketInfo.minimumTickSize, 0.0001), 4)
                : expectedQty,
            notional: payload.amount ?? expectedQty * (payload.limitPrice ?? book.bestBid)
          })
        : undefined;
      const orderFee = status === "filled" ? estimate.estimatedFee : status === "pending" ? pendingFeeEstimate?.fee ?? 0 : 0;
      if (action === "buy" && status !== "failed" && user.availableUsdc < (payload.amount ?? 0) + orderFee) {
        throw new Error("Insufficient virtual balance.");
      }
      const order: OrderRecord = {
        id: orderId,
        traceId,
        userId: user.id,
        roundId: currentRound.id,
        symbol: this.config.symbol,
        marketId,
        action,
        side: payload.side,
        status,
        orderKind,
        timeInForce: orderKind === "limit" ? "GTC" : "FOK",
        limitPrice: payload.limitPrice,
        lifecycleStatus: status,
        resultType: status === "pending" ? "pending" : status === "filled" ? "all_filled" : "all_failed",
        tokenId,
        bookKey,
        bookHash: book.snapshotId,
        requestedAmountUsdc: action === "buy" ? roundNumber(payload.amount ?? 0, 2) : undefined,
        requestedQty: action === "sell" ? roundNumber(payload.qty ?? 0, 4) : expectedQty,
        frozenUsdc: 0,
        frozenQty: 0,
        fills: estimate.fills,
        estimatedFee: orderFee,
        actualFee: status === "filled" ? orderFee : 0,
        feeBreakdown: status === "filled" ? estimate.feeBreakdown : pendingFeeEstimate?.breakdown,
        feeCurrency: "USD",
        sourceLatencyMs,
        marketSlug: currentRound.marketSlug,
        orderBookSnapshot,
        notionalUsdc: roundNumber(
          action === "buy"
            ? (status === "filled" ? estimate.matchedNotional : payload.amount ?? 0)
            : status === "filled"
              ? estimate.matchedNotional
              : (payload.qty ?? 0) * (payload.limitPrice ?? book.bestBid),
          2
        ),
        expectedQty,
        filledQty: status === "filled" ? roundNumber(estimate.filledQty, 4) : 0,
        unfilledQty: status === "filled" ? 0 : expectedQty,
        avgFillPrice: status === "filled" && estimate.avgPrice ? roundNumber(estimate.avgPrice, 4) : undefined,
        bestBid: book.bestBid,
        bestAsk: book.bestAsk,
        midPrice: book.midPrice,
        bookSnapshotTs: book.snapshotTs,
        partialFilled: false,
        slippageBps:
          status === "filled" && estimate.avgPrice && book.midPrice > 0
            ? roundNumber(((estimate.avgPrice - book.midPrice) / book.midPrice) * 10000, 2)
            : undefined,
        matchLatencyMs: localMatchLatencyMs,
        bookAcquireLatencyMs,
        localMatchLatencyMs,
        failureReason: status === "failed" ? estimate.failureReason : undefined,
        clientOrderId,
        clientSendTs: payload.clientSendTs,
        serverRecvTs,
        serverPublishTs: Date.now(),
        createdAt: Date.now()
      };

      const tradePersistSegments: TradePersistSegments = {};
      try {
        const transactionStartedAt = Date.now();
        await this.runTradeWriteTransaction(async () => {
          const persistStartTs = Date.now();
          await this.measureTradePersistSegment(tradePersistSegments, "persistOrderBookSnapshot", () => {
            this.store.prepareOrderBookSnapshotForOrder(order);
          });
          if (status === "pending") {
            if (action === "buy") {
              const frozen = roundNumber((payload.amount ?? 0) + orderFee, 2);
              user.availableUsdc = roundNumber(user.availableUsdc - frozen, 2);
              order.frozenUsdc = frozen;
            } else {
              const frozenQty = roundNumber(payload.qty ?? 0, 4);
              await this.lockSellQty(user.id, currentRound.id, payload.side, frozenQty, payload.positionIds);
              order.frozenQty = frozenQty;
            }
            await Promise.all([
              this.measureTradePersistSegment(tradePersistSegments, "persistOrder", () => this.store.persistOrder(order)),
              ...(action === "buy"
                ? [this.measureTradePersistSegment(tradePersistSegments, "persistUser", () => this.store.persistUser(user))]
                : [])
            ]);
          } else if (status === "filled" && estimate.avgPrice) {
            await this.applyFilledOrder(
              user,
              currentRound,
              order,
              estimate,
              payload.positionIds,
              payload.exitType ?? "manual_sell",
              false,
              tradePersistSegments
            );
            if (order.action === "buy") {
              await this.recordBuyLifecycle(user, currentRound, order, snapshot, tradePersistSegments);
            }
          } else {
            await this.measureTradePersistSegment(tradePersistSegments, "persistOrder", () => this.store.persistOrder(order));
          }
          order.persistLatencyMs = Math.max(Date.now() - persistStartTs, 0);
          order.totalOrderLatencyMs = Math.max(Date.now() - serverRecvTs, 1);
        });
        tradePersistSegments.transactionTotal = Math.max(Date.now() - transactionStartedAt, 0);
        const measuredSegmentTotal = Object.entries(tradePersistSegments)
          .filter(([name]) => name !== "transactionTotal" && name !== "commitAndOverhead")
          .reduce((sum, [, value]) => sum + (value ?? 0), 0);
        tradePersistSegments.commitAndOverhead = Math.max(tradePersistSegments.transactionTotal - measuredSegmentTotal, 0);
      } catch (writeError) {
        if (clientOrderId && isClientOrderConflict(writeError)) {
          const existingOrderAfterConflict =
            typeof this.store.findOrderByClientOrderId === "function"
              ? await this.store.findOrderByClientOrderId(user.id, clientOrderId)
              : undefined;
          if (existingOrderAfterConflict) {
            return { order: existingOrderAfterConflict };
          }
        }
        throw writeError;
      }

      const successAuditEvent: AuditEvent = {
        eventId: this.store.newId("evt"),
        traceId,
        category: "matching",
        actionType: "place_order",
        actionStatus: status === "failed" ? "failed" : "success",
        userId: user.id,
        role: user.role,
        pageName: "trade.main",
        moduleName: "order.panel",
        symbol: this.config.symbol,
        roundId: currentRound.id,
        clientSendTs: payload.clientSendTs,
        serverRecvTs,
        engineStartTs,
        engineFinishTs,
        serverPublishTs: order.serverPublishTs,
        backendLatencyMs: order.totalOrderLatencyMs ?? order.matchLatencyMs,
        resultCode: status === "failed" ? "ORDER_FAILED" : status === "pending" ? "ORDER_PENDING" : "ORDER_FILLED",
        resultMessage:
          status === "failed"
            ? estimate.failureReason ?? "Polymarket CLOB depth was insufficient."
            : status === "pending"
              ? "Limit order is pending against future Polymarket CLOB depth."
              : "Order fully matched against the current Polymarket CLOB snapshot.",
        details: {
          traceId,
          roundId: currentRound.id,
          marketId,
          marketSlug: currentRound.marketSlug,
          orderId: order.id,
          clientOrderId,
          positionId: payload.positionIds?.[0],
          bookKey,
          bookHash: book.snapshotId,
          bookSnapshotId: book.snapshotId,
          matchingSequence: undefined,
          tokenId,
          action,
          side: payload.side,
          orderKind,
          timeInForce: order.timeInForce,
          limitPrice: payload.limitPrice,
          notionalUsdc: payload.amount,
          requestedQty: payload.qty,
          filledQty: order.filledQty,
          unfilledQty: order.unfilledQty,
          avgFillPrice: order.avgFillPrice,
          bookAcquireLatencyMs: order.bookAcquireLatencyMs,
          localMatchLatencyMs: order.localMatchLatencyMs,
          persistLatencyMs: order.persistLatencyMs,
          totalOrderLatencyMs: order.totalOrderLatencyMs,
          tradePersistSegments,
          executionBookSource: executionBook.source,
          executionBookAgeMs: executionBook.ageMs,
          executionBookFallbackReason: executionBook.fallbackReason,
          sourceLatencyMs,
          slippageBps: order.slippageBps,
          estimatedFee: order.estimatedFee,
          actualFee: order.actualFee,
          feeBreakdown: order.feeBreakdown,
          feeCurrency: order.feeCurrency,
          marketInfo,
          failureReason: order.failureReason
        }
      };
      const successBehaviorLog = this.createBehaviorLog({
        user,
        actionType: "place_order",
        actionStatus: status === "failed" ? "failed" : "success",
        traceId,
        orderId: order.id,
        round: currentRound,
        snapshot,
        direction: payload.side,
        entryOdds: snapshot[payload.side === "UP" ? "upPrice" : "downPrice"],
        positionNotional: order.notionalUsdc,
        bookSnapshot: book,
        order,
        actualFillPrice: order.avgFillPrice,
        slippageBps: order.slippageBps,
        partialFilled: order.partialFilled,
        unfilledQty: order.unfilledQty,
        executionLatencyMs: order.matchLatencyMs,
        estimatedFee: order.estimatedFee,
        actualFee: order.actualFee,
        feeBreakdown: order.feeBreakdown,
        feeCurrency: order.feeCurrency,
        failureReason: order.failureReason,
        contextJson: {
          roundStatus: currentRound.status,
          acceptingOrders: currentRound.acceptingOrders,
          requestAction: action,
          requestSide: payload.side,
          requestAmount: payload.amount,
          requestQty: payload.qty,
          clientOrderId,
          orderType: orderKind,
          isAccepted: status !== "failed",
          bookSnapshotId: book.snapshotId,
          bookKey,
          bookAcquireLatencyMs: order.bookAcquireLatencyMs,
          localMatchLatencyMs: order.localMatchLatencyMs,
          persistLatencyMs: order.persistLatencyMs,
          totalOrderLatencyMs: order.totalOrderLatencyMs,
          tradePersistSegments,
          executionBookSource: executionBook.source,
          executionBookAgeMs: executionBook.ageMs,
          executionBookFallbackReason: executionBook.fallbackReason,
          marketInfo
        }
      });
      this.enqueueTradeLog(async () => {
        await this.writeAuditLog(successAuditEvent, { emitUserPayload: false });
        await this.writeBehaviorLog(successBehaviorLog);
      });
      this.store.emitUserPayload(user.id, "trade");
      return { order };
    } catch (error) {
      const message = error instanceof Error ? error.message : "Order failed.";
      const serverNow = Date.now();
      const failureAuditEvent: AuditEvent = {
        eventId: this.store.newId("evt"),
        traceId,
        category: "matching",
        actionType: "place_order",
        actionStatus: "failed",
        userId: user.id,
        role: user.role,
        pageName: "trade.main",
        moduleName: "order.panel",
        symbol: this.config.symbol,
        roundId: currentRound?.id,
        clientSendTs: payload.clientSendTs,
        serverRecvTs: serverNow,
        serverPublishTs: serverNow,
        backendLatencyMs: 0,
        resultCode: "ORDER_FAILED",
        resultMessage: message,
        details: {
          traceId,
          roundId: currentRound?.id,
          marketId: snapshot.marketId,
          marketSlug: currentRound?.marketSlug ?? snapshot.marketSlug,
          action,
          side: payload.side,
          orderKind: payload.orderKind ?? "market",
          clientOrderId,
          limitPrice: payload.limitPrice,
          notionalUsdc: payload.amount,
          requestedQty: payload.qty,
          failureReason: message
        }
      };
      const failureBehaviorLog = this.createBehaviorLog({
        user,
        actionType: "place_order",
        actionStatus: "failed",
        traceId,
        round: currentRound,
        snapshot,
        direction: payload.side,
        entryOdds: snapshot[payload.side === "UP" ? "upPrice" : "downPrice"],
        positionNotional: payload.amount,
        failureReason: message,
        contextJson: {
          requestAction: action,
          requestSide: payload.side,
          requestAmount: payload.amount,
          requestQty: payload.qty,
          clientOrderId,
          orderType: payload.orderKind ?? "market",
          limitPrice: payload.limitPrice,
          failureReason: message
        }
      });
      this.enqueueTradeLog(async () => {
        await this.writeAuditLog(failureAuditEvent, { emitUserPayload: false });
        await this.writeBehaviorLog(failureBehaviorLog);
      });
      throw error;
    }
  }

  async cancelOrder(user: UserRecord, orderId: string) {
    const snapshot = this.captureActionSnapshot();
    const traceId = this.store.newTraceId();
    const order = this.store.orders.find((item) => item.id === orderId && item.userId === user.id);
    try {
      if (!order) {
        throw new Error("Order not found.");
      }
      if (order.status !== "pending") {
        throw new Error("Only pending limit orders can be cancelled.");
      }

      const releasedFrozenUsdc = order.frozenUsdc ?? 0;
      const releasedFrozenQty = order.frozenQty ?? 0;
      await this.runTradeWriteTransaction(async () => {
        order.status = "cancelled";
        order.lifecycleStatus = "cancelled";
        order.resultType = "cancelled";
        if (order.frozenUsdc && order.frozenUsdc > 0) {
          user.availableUsdc = roundNumber(user.availableUsdc + order.frozenUsdc, 2);
          order.frozenUsdc = 0;
          await this.store.persistUser(user);
        }
        if (order.frozenQty && order.frozenQty > 0) {
          await this.unlockSellQty(user.id, order.roundId, order.side, order.frozenQty);
          order.frozenQty = 0;
        }
        order.serverPublishTs = Date.now();
        await this.store.persistOrder(order);
        await Promise.allSettled([
          this.writeAuditLog({
            eventId: this.store.newId("evt"),
            traceId: order.traceId,
            category: "operation",
            actionType: "cancel_order",
            actionStatus: "success",
            userId: user.id,
            role: user.role,
            pageName: "trade.main",
            moduleName: "order.panel",
            symbol: order.symbol,
            roundId: order.roundId,
            serverRecvTs: Date.now(),
            serverPublishTs: Date.now(),
            backendLatencyMs: 1,
            resultCode: "ORDER_CANCELLED",
            resultMessage: "Pending paper limit order was cancelled and frozen assets were released.",
            details: {
              traceId: order.traceId,
              roundId: order.roundId,
              marketId: order.marketId,
              marketSlug: order.marketSlug,
              orderId,
              orderKind: order.orderKind,
              releasedFrozenUsdc,
              releasedFrozenQty,
              bookKey: order.bookKey,
              bookSnapshotId: order.bookHash
            }
          }),
          this.writeBehaviorLog(
            this.createBehaviorLog({
              user,
              actionType: "cancel_order",
              actionStatus: "success",
              traceId: order.traceId,
              orderId: order.id,
              round: this.store.getRoundById(order.roundId),
              snapshot,
              direction: order.side,
              entryOdds: snapshot[order.side === "UP" ? "upPrice" : "downPrice"],
              positionNotional: order.notionalUsdc,
              bookSnapshot: snapshot.orderBooks[order.side],
              order,
              frozenAssetRelease: {
                releasedFrozenUsdc,
                releasedFrozenQty
              },
              partialFilled: order.partialFilled,
              unfilledQty: order.unfilledQty,
              contextJson: {
                cancelledRemainingQty: order.unfilledQty,
                bookKey: order.bookKey,
                bookSnapshotId: order.bookHash,
                marketSlug: order.marketSlug
              }
            })
          )
        ]);
      });
      this.store.emitUserPayload(user.id, "trade");
      return order;
    } catch (error) {
      const message = error instanceof Error ? error.message : "Cancel order failed.";
      const serverNow = Date.now();
      await this.writeAuditLog({
        eventId: this.store.newId("evt"),
        traceId: order?.traceId ?? traceId,
        category: "operation",
        actionType: "cancel_order",
        actionStatus: "failed",
        userId: user.id,
        role: user.role,
        pageName: "trade.main",
        moduleName: "order.panel",
        symbol: order?.symbol ?? this.config.symbol,
        roundId: order?.roundId,
        serverRecvTs: serverNow,
        serverPublishTs: serverNow,
        backendLatencyMs: 0,
        resultCode: "ORDER_CANCEL_FAILED",
        resultMessage: message,
        details: {
          traceId: order?.traceId ?? traceId,
          roundId: order?.roundId,
          marketId: order?.marketId,
          marketSlug: order?.marketSlug,
          orderId,
          failureReason: message
        }
      });
      await this.writeBehaviorLog(
        this.createBehaviorLog({
          user,
          actionType: "cancel_order",
          actionStatus: "failed",
          traceId: order?.traceId ?? traceId,
          orderId,
          round: order ? this.store.getRoundById(order.roundId) : this.getActiveRound(serverNow),
          snapshot,
          direction: order?.side,
          positionNotional: order?.notionalUsdc,
          order,
          failureReason: message,
          contextJson: {
            failureReason: message
          }
        })
      );
      throw error;
    }
  }

  async sellPosition(
    user: UserRecord,
    positionId: string,
    exitType: Exclude<OrderLifecycleExitType, "settlement" | "mixed"> = "manual_sell"
  ) {
    const snapshot = this.captureActionSnapshot();
    const traceId = this.store.newTraceId();
    const position = this.store.positions.find((item) => item.id === positionId && item.userId === user.id);
    try {
      if (!position) {
        throw new Error("Position not found.");
      }
      if (position.status !== "open") {
        throw new Error("Position is already closed.");
      }

      const currentRound = this.store.getRoundById(position.roundId);
      this.assertCanSellPosition(position, currentRound, Date.now());

      const availableQty = roundNumber(Math.max(position.qty - (position.lockedQty ?? 0), 0), 4);
      if (availableQty <= QTY_EPSILON) {
        throw new Error("Position has no unlocked quantity available to sell.");
      }
      const { order } = await this.placeOrder(user, {
        action: "sell",
        side: position.side,
        qty: availableQty,
        orderKind: "market",
        clientSendTs: undefined,
        positionIds: [positionId],
        exitType
      });
      if (order.status !== "filled") {
        throw new Error(order.failureReason ?? "Sell order was not fully filled.");
      }
      this.enqueueTradeLog(async () => {
        await this.writeAuditLog({
          eventId: this.store.newId("evt"),
          traceId: order.traceId,
          category: "matching",
          actionType: "sell_position",
          actionStatus: "success",
          userId: user.id,
          role: user.role,
          pageName: "profile.main",
          moduleName: "position.table",
          symbol: order.symbol,
          roundId: order.roundId,
          serverRecvTs: order.serverRecvTs,
          serverPublishTs: Date.now(),
          backendLatencyMs: order.matchLatencyMs,
          resultCode: "SELL_POSITION_FILLED",
          resultMessage: "Position was sold against the current Polymarket CLOB snapshot.",
          details: {
            traceId: order.traceId,
            roundId: order.roundId,
            marketId: order.marketId,
            marketSlug: order.marketSlug,
            orderId: order.id,
            positionId,
            bookKey: order.bookKey,
            bookSnapshotId: order.bookHash,
            avgFillPrice: order.avgFillPrice,
            filledQty: order.filledQty,
            slippageBps: order.slippageBps
          }
        }, { emitUserPayload: false });
        await this.writeBehaviorLog(
          this.createBehaviorLog({
          user,
          actionType: "sell_position",
          actionStatus: "success",
          traceId: order.traceId,
          orderId: order.id,
          round: currentRound,
          snapshot,
          direction: position.side,
          positionNotional: order.notionalUsdc,
          exitType,
          exitOdds: order.avgFillPrice,
          settlementResult: position.settlementResult,
          bookSnapshot: snapshot.orderBooks[position.side],
          order,
          actualFillPrice: order.avgFillPrice,
          slippageBps: order.slippageBps,
          partialFilled: order.partialFilled,
          unfilledQty: order.unfilledQty,
          executionLatencyMs: order.matchLatencyMs,
          contextJson: {
            positionId
          }
        })
        );
      });
      return order;
    } catch (error) {
      const message = error instanceof Error ? error.message : "Sell position failed.";
      const currentRound = position ? this.store.getRoundById(position.roundId) : this.getActiveRound(Date.now());
      const serverNow = Date.now();
      await this.writeAuditLog({
        eventId: this.store.newId("evt"),
        traceId,
        category: "matching",
        actionType: "sell_position",
        actionStatus: "failed",
        userId: user.id,
        role: user.role,
        pageName: "profile.main",
        moduleName: "position.table",
        symbol: this.config.symbol,
        roundId: currentRound?.id,
        serverRecvTs: serverNow,
        serverPublishTs: serverNow,
        backendLatencyMs: 0,
        resultCode: "SELL_FAILED",
        resultMessage: message,
        details: {
          traceId,
          roundId: currentRound?.id,
          marketId: currentRound?.marketId ?? snapshot.marketId,
          marketSlug: currentRound?.marketSlug ?? snapshot.marketSlug,
          positionId,
          failureReason: message
        }
      });
      await this.writeBehaviorLog(
        this.createBehaviorLog({
          user,
          actionType: "sell_position",
          actionStatus: "failed",
          traceId,
          round: currentRound,
          snapshot,
          direction: position?.side,
          exitType: "manual_sell",
          settlementResult: position?.settlementResult,
          positionNotional: position?.notionalSpent,
          failureReason: message,
          contextJson: {
            positionId,
            failureReason: message
          }
        })
      );
      throw error;
    }
  }

  async updateLanguage(user: UserRecord, language: Language) {
    await this.store.setUserLanguage(user.id, language);
    await this.writeAuditLog({
      eventId: this.store.newId("evt"),
      traceId: this.store.newTraceId(),
      category: "operation",
      actionType: "switch_language",
      actionStatus: "success",
      userId: user.id,
      role: user.role,
      pageName: "shell.topbar",
      moduleName: "language.switch",
      serverRecvTs: Date.now(),
      serverPublishTs: Date.now(),
      backendLatencyMs: 1,
      resultCode: "LANGUAGE_UPDATED",
      resultMessage: "Language preference updated.",
      details: {
        language
      }
    });
  }

  async getMatchingHealth() {
    return this.matchingClient.health();
  }

  async searchMatchingEvents(input: Parameters<MatchingServiceClient["searchEvents"]>[0]) {
    return this.matchingClient.searchEvents(input);
  }

  async getCurrentMatchingBookState(input: {
    side?: TradeSide;
    bookKey?: string;
    roundId?: string;
    marketId?: string;
  }) {
    const bookKey = this.resolveReplayBookKey(input);
    const response = await this.matchingClient.getCurrentBook(bookKey);
    if (response.book) {
      this.cacheMatchingBook(response.book);
    }
    return {
      bookKey,
      book: response.book
    };
  }

  async getMatchingReplay(input: {
    side?: TradeSide;
    bookKey?: string;
    roundId?: string;
    marketId?: string;
    fromSequence?: number;
    toSequence?: number;
    limit?: number;
  }) {
    const bookKey = this.resolveReplayBookKey(input);
    return this.matchingClient.replay(bookKey, {
      fromSequence: input.fromSequence,
      toSequence: input.toSequence,
      limit: input.limit
    });
  }

  async closeSide(user: UserRecord, payload: { side: TradeSide; clientSendTs?: number }) {
    const traceId = this.store.newTraceId();
    const snapshot = this.captureActionSnapshot();
    const now = Date.now();
    const currentRound = this.getActiveRound(now);
    try {
      this.assertCanSellOrder(currentRound, now);
      const positions = this.store.positions
        .filter(
          (position) =>
            position.userId === user.id &&
            position.roundId === currentRound.id &&
            position.side === payload.side &&
            position.status === "open"
        )
        .sort((left, right) => left.openedAt - right.openedAt);
      if (positions.length === 0) {
        throw new Error(`No open ${payload.side} positions were found in the current round.`);
      }
      const requestedQty = roundNumber(positions.reduce((sum, position) => sum + position.qty, 0), 4);

      const failures: Array<{ positionId: string; message: string }> = [];
      const { order } = await this.placeOrder(user, {
        action: "sell",
        side: payload.side,
        qty: requestedQty,
        orderKind: "market",
        clientSendTs: payload.clientSendTs,
        positionIds: positions.map((position) => position.id),
        exitType: "close_side"
      });
      if (order.status !== "filled") {
        const message = order.failureReason ?? "Close side sell order was not fully filled.";
        failures.push(...positions.map((position) => ({ positionId: position.id, message })));
        throw new Error(message);
      }

      const totalQty = roundNumber(order.filledQty, 4);
      const totalProceeds = roundCurrency(order.notionalUsdc - (order.actualFee ?? 0));
      const avgFillPrice = totalQty > 0 ? roundNumber(order.notionalUsdc / totalQty, 4) : undefined;
      const closedPositionsCount = positions.filter((position) => position.status === "closed").length;
      const serverNow = Date.now();

      this.enqueueTradeLog(async () => {
        await this.writeAuditLog({
          eventId: this.store.newId("evt"),
          traceId,
          category: "operation",
          actionType: "close_side",
          actionStatus: failures.length > 0 ? "timeout" : "success",
          userId: user.id,
          role: user.role,
          pageName: "trade.main",
          moduleName: "quick.actions",
          symbol: this.config.symbol,
          roundId: currentRound.id,
          clientSendTs: payload.clientSendTs,
          serverRecvTs: serverNow,
          serverPublishTs: serverNow,
          backendLatencyMs: 1,
          resultCode: failures.length > 0 ? "CLOSE_SIDE_PARTIAL" : "CLOSE_SIDE_COMPLETED",
          resultMessage:
            failures.length > 0
              ? "Close side completed with partial failures."
              : "All positions on the selected side were closed.",
          details: {
            side: payload.side,
            closedPositionsCount,
            totalQty,
            totalProceeds,
            avgFillPrice,
            failures
          }
        }, { emitUserPayload: false });

        await this.writeBehaviorLog(
          this.createBehaviorLog({
          user,
          actionType: "close_side",
          actionStatus: failures.length > 0 ? "timeout" : "success",
          traceId,
          round: currentRound,
          snapshot,
          direction: payload.side,
          positionNotional: totalProceeds,
          exitType: "close_side",
          exitOdds: avgFillPrice,
          actualFillPrice: avgFillPrice,
          executionLatencyMs: order.matchLatencyMs,
          partialFilled: failures.length > 0,
          unfilledQty: roundNumber(Math.max(requestedQty - totalQty, 0), 4),
          contextJson: {
            orderId: order.id,
            failures,
            closedPositionsCount
          }
        })
        );
      });

      return {
        closedPositionsCount,
        totalQty,
        totalProceeds,
        avgFillPrice,
        matchLatencyMs: order.matchLatencyMs,
        failures
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : "Close side failed.";
      const serverNow = Date.now();
      await this.writeAuditLog({
        eventId: this.store.newId("evt"),
        traceId,
        category: "operation",
        actionType: "close_side",
        actionStatus: "failed",
        userId: user.id,
        role: user.role,
        pageName: "trade.main",
        moduleName: "quick.actions",
        symbol: this.config.symbol,
        roundId: currentRound?.id,
        clientSendTs: payload.clientSendTs,
        serverRecvTs: serverNow,
        serverPublishTs: serverNow,
        backendLatencyMs: 0,
        resultCode: "CLOSE_SIDE_FAILED",
        resultMessage: message,
        details: {
          side: payload.side
        }
      });
      await this.writeBehaviorLog(
        this.createBehaviorLog({
          user,
          actionType: "close_side",
          actionStatus: "failed",
          traceId,
          round: currentRound,
          snapshot,
          direction: payload.side,
          exitType: "close_side",
          contextJson: {
            failureReason: message
          }
        })
      );
      throw error;
    }
  }

  async reverseSide(user: UserRecord, payload: { side: TradeSide; clientSendTs?: number }) {
    const traceId = this.store.newTraceId();
    const snapshot = this.captureActionSnapshot();
    const now = Date.now();
    const currentRound = this.getActiveRound(now);
    try {
      this.assertCanSellOrder(currentRound, now);
      this.assertCanBuyOrder(currentRound, now);
      const closeResult = await this.closeSide(user, payload);
      if (closeResult.totalProceeds <= 0) {
        throw new Error("Reverse side requires positive proceeds from the close action.");
      }
      const reverseSide = payload.side === "UP" ? "DOWN" : "UP";
      const result = await this.placeOrder(user, {
        side: reverseSide,
        amount: closeResult.totalProceeds,
        clientSendTs: payload.clientSendTs
      });
      const serverNow = Date.now();
      this.enqueueTradeLog(async () => {
        await this.writeAuditLog({
          eventId: this.store.newId("evt"),
          traceId,
          category: "operation",
          actionType: "reverse_side",
          actionStatus: "success",
          userId: user.id,
          role: user.role,
          pageName: "trade.main",
          moduleName: "quick.actions",
          symbol: this.config.symbol,
          roundId: currentRound?.id,
          clientSendTs: payload.clientSendTs,
          serverRecvTs: serverNow,
          serverPublishTs: serverNow,
          backendLatencyMs: result.order.matchLatencyMs,
          resultCode: "REVERSE_SIDE_COMPLETED",
          resultMessage: "Side was closed and the opposite side was bought.",
          details: {
            requestedSide: payload.side,
            reverseSide,
            closeResult,
            reverseOrderId: result.order.id
          }
        }, { emitUserPayload: false });
        await this.writeBehaviorLog(
          this.createBehaviorLog({
          user,
          actionType: "reverse_side",
          actionStatus: "success",
          traceId,
          round: currentRound,
          snapshot,
          direction: reverseSide,
          positionNotional: closeResult.totalProceeds,
          entryOdds: snapshot[reverseSide === "UP" ? "upPrice" : "downPrice"],
          actualFillPrice: result.order.avgFillPrice,
          slippageBps: result.order.slippageBps,
          partialFilled: result.order.partialFilled,
          unfilledQty: result.order.unfilledQty,
          executionLatencyMs: result.order.matchLatencyMs,
          contextJson: {
            requestedSide: payload.side,
            reverseSide,
            closeResult,
            reverseOrderId: result.order.id
          }
        })
        );
      });

      return {
        closeResult,
        reverseSide,
        reverseOrder: result.order
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : "Reverse side failed.";
      const serverNow = Date.now();
      await this.writeAuditLog({
        eventId: this.store.newId("evt"),
        traceId,
        category: "operation",
        actionType: "reverse_side",
        actionStatus: "failed",
        userId: user.id,
        role: user.role,
        pageName: "trade.main",
        moduleName: "quick.actions",
        symbol: this.config.symbol,
        roundId: currentRound?.id,
        clientSendTs: payload.clientSendTs,
        serverRecvTs: serverNow,
        serverPublishTs: serverNow,
        backendLatencyMs: 0,
        resultCode: "REVERSE_SIDE_FAILED",
        resultMessage: message,
        details: {
          side: payload.side
        }
      });
      await this.writeBehaviorLog(
        this.createBehaviorLog({
          user,
          actionType: "reverse_side",
          actionStatus: "failed",
          traceId,
          round: currentRound,
          snapshot,
          direction: payload.side === "UP" ? "DOWN" : "UP",
          exitType: "reverse_side",
          contextJson: {
            failureReason: message
          }
        })
      );
      throw error;
    }
  }

  private captureActionSnapshot() {
    const snapshot = this.store.marketSnapshot;
    if (snapshot.marketId) {
      return snapshot;
    }
    return this.buildSnapshot();
  }

  private createBehaviorLog(input: {
    user: UserRecord;
    actionType: string;
    actionStatus: BehaviorActionLog["actionStatus"];
    traceId?: string;
    orderId?: string;
    round?: RoundRecord;
    snapshot: MarketSnapshot;
    direction?: TradeSide;
    entryOdds?: number;
    positionNotional?: number;
    exitType?: string;
    exitOdds?: number;
    settlementResult?: PositionRecord["settlementResult"];
    bookSnapshot?: OrderBookSnapshot;
    actualFillPrice?: number;
    slippageBps?: number;
    partialFilled?: boolean;
    unfilledQty?: number;
    executionLatencyMs?: number;
    estimatedFee?: number;
    actualFee?: number;
    feeBreakdown?: FeeBreakdown;
    feeCurrency?: "USD";
    settlementDirection?: TradeSide;
    settlementTimeMs?: number;
    gammaPollCount?: number;
    redeemFinishTimeMs?: number;
    order?: OrderRecord;
    failureReason?: string;
    frozenAssetRelease?: Record<string, unknown>;
    contextJson?: Record<string, unknown>;
  }): BehaviorActionLog {
    const direction = input.direction;
    const round = input.round;
    const snapshot = input.snapshot;
    const bookSnapshot =
      input.bookSnapshot ??
      (direction ? snapshot.orderBooks[direction] : snapshot.orderBooks.UP);
    const candles = snapshot.binance.candlesByInterval;
    const contextJson = {
      ...(input.order
        ? {
            requestAction: input.order.action,
            requestedAmountUsdc: input.order.requestedAmountUsdc,
            requestedQty: input.order.requestedQty,
            orderKind: input.order.orderKind,
            timeInForce: input.order.timeInForce,
            limitPrice: input.order.limitPrice,
            lifecycleStatus: input.order.lifecycleStatus,
            resultType: input.order.resultType,
            bookKey: input.order.bookKey,
            bookSnapshotId: input.order.bookHash,
            marketId: input.order.marketId,
            marketSlug: input.order.marketSlug,
            fills: input.order.fills,
            estimatedFee: input.order.estimatedFee,
            actualFee: input.order.actualFee,
            feeBreakdown: input.order.feeBreakdown,
            feeCurrency: input.order.feeCurrency,
            failureReason: input.order.failureReason
          }
        : {}),
      ...(input.frozenAssetRelease ? { frozenAssetRelease: input.frozenAssetRelease } : {}),
      ...(input.failureReason ? { failureReason: input.failureReason } : {}),
      ...(input.contextJson ?? {})
    };
    return {
      logId: this.store.newId("blog"),
      timestampMs: Date.now(),
      assetClass: "BTC_5M_UPDOWN",
      actionType: input.actionType,
      actionStatus: input.actionStatus,
      roundId: round?.id,
      direction,
      entryOdds: input.entryOdds,
      deltaClob: snapshot.clob.delta,
      volumeClob: snapshot.clob.volume,
      positionNotional: input.positionNotional,
      exitType: input.exitType,
      exitOdds: input.exitOdds,
      settlementResult: input.settlementResult,
      testerIdAnon: this.store.anonymizeUserId(input.user.id),
      traceId: input.traceId,
      orderId: input.orderId,
      marketId: snapshot.marketId,
      marketSlug: snapshot.marketSlug,
      roundStatus: round?.status,
      countdownMs: snapshot.uiMeta.countdownMs,
      binanceSpotPrice: snapshot.binance.spotPrice,
      binance1mLastClose: candles["1m"].at(-1)?.close ?? 0,
      binance5mLastClose: candles["5m"].at(-1)?.close ?? 0,
      binance1dLastClose: candles["1d"].at(-1)?.close ?? 0,
      coinbasePrice: snapshot.coinbase.referencePrice,
      priceToBeat: snapshot.priceToBeat,
      upPrice: snapshot.upPrice,
      downPrice: snapshot.downPrice,
      upBookTop5: snapshot.orderBooks.UP.bids.slice(0, 5),
      downBookTop5: snapshot.orderBooks.DOWN.bids.slice(0, 5),
      recentTradesTop20: snapshot.recentTrades.slice(0, 20),
      bookSnapshotEntry: {
        snapshotId: bookSnapshot.snapshotId,
        snapshotTs: bookSnapshot.snapshotTs,
        topBids: bookSnapshot.bids.slice(0, 5),
        topAsks: bookSnapshot.asks.slice(0, 5)
      },
      actualFillPrice: input.actualFillPrice,
      slippageBps: input.slippageBps,
      partialFilled: input.partialFilled,
      unfilledQty: input.unfilledQty,
      executionLatencyMs: input.executionLatencyMs,
      estimatedFee: input.estimatedFee,
      actualFee: input.actualFee,
      feeBreakdown: input.feeBreakdown,
      feeCurrency: input.feeCurrency,
      settlementDirection: input.settlementDirection,
      settlementTimeMs: input.settlementTimeMs,
      gammaPollCount: input.gammaPollCount,
      redeemFinishTimeMs: input.redeemFinishTimeMs,
      sourceStates: {
        binance: this.pickSourceState(snapshot.sources.binance),
        coinbase: this.pickSourceState(snapshot.sources.coinbase),
        clob: this.pickSourceState(snapshot.sources.clob)
      },
      contextJson
    };
  }

  private pickSourceState(source: SourceHealth) {
    return {
      source: source.source,
      state: source.state,
      sourceEventTs: source.sourceEventTs,
      serverRecvTs: source.serverRecvTs,
      serverPublishTs: source.serverPublishTs
    };
  }

  private async writeBehaviorLog(log: BehaviorActionLog) {
    await this.store.recordBehaviorLog(log);
  }

  private enqueueTradeLog(task: TradeLogTask) {
    this.tradeLogQueue ??= [];
    this.tradeLogQueue.push(task);
    if (this.tradeLogFlushScheduled || this.tradeLogFlushRunning) {
      return;
    }
    this.tradeLogFlushScheduled = true;
    setImmediate(() => {
      this.tradeLogFlushScheduled = false;
      void this.flushTradeLogQueue();
    });
  }

  private async flushTradeLogQueue() {
    if (this.tradeLogFlushRunning) {
      return;
    }
    this.tradeLogFlushRunning = true;
    try {
      while (this.tradeLogQueue.length > 0) {
        const task = this.tradeLogQueue.shift();
        if (!task) {
          continue;
        }
        try {
          await task();
        } catch (error) {
          console.warn("[simulation] Background trade log write failed:", error);
        }
      }
    } finally {
      this.tradeLogFlushRunning = false;
      if (this.tradeLogQueue.length > 0) {
        this.enqueueTradeLog(async () => undefined);
      }
    }
  }

  private scheduleSnapshotOnlyRefresh(source: "binance" | "coinbase" | "clob") {
    this.snapshotRefreshSources.add(source);
    this.snapshotRefreshQueued = true;
    this.snapshotRefreshQueuedAt ??= Date.now();
    if (this.snapshotRefreshRunning) {
      return;
    }

    this.snapshotRefreshRunning = true;
    setImmediate(() => {
      void this.snapshotOnlyRefreshLoop();
    });
  }

  private async snapshotOnlyRefreshLoop() {
    try {
      while (this.snapshotRefreshQueued) {
        const queuedAt = this.snapshotRefreshQueuedAt;
        this.snapshotRefreshQueued = false;
        this.snapshotRefreshQueuedAt = undefined;
        this.snapshotRefreshSources.clear();
        try {
          await this.refreshMarketSnapshotOnly(queuedAt);
        } catch (error) {
          if (this.store.isPersistenceUnavailableError(error)) {
            console.warn("[simulation] Snapshot-only refresh deferred while persistence is unavailable:", error);
            await sleep(250);
            this.snapshotRefreshQueued = true;
            continue;
          }
          throw error;
        }
      }
    } finally {
      this.snapshotRefreshRunning = false;
      if (this.snapshotRefreshQueued) {
        this.snapshotRefreshRunning = true;
        setImmediate(() => {
          void this.snapshotOnlyRefreshLoop();
        });
      }
    }
  }

  private async refreshMarketSnapshotOnly(queuedAt?: number) {
    const startedAt = Date.now();
    try {
      const snapshot = this.buildSnapshot();
      await this.store.setMarketSnapshot(snapshot);
      this.scheduleLatencyLogs(snapshot);
    } finally {
      appMetrics.recordMarketSnapshotRefresh({
        mode: "snapshot_only",
        durationMs: Date.now() - startedAt,
        queueAgeMs: queuedAt ? Math.max(startedAt - queuedAt, 0) : undefined
      });
    }
  }

  private scheduleReconcile() {
    this.scheduleFullReconcile();
  }

  private scheduleFullReconcile() {
    if (this.reconcileRunning) {
      this.reconcileQueued = true;
      return;
    }

    this.reconcileRunning = true;
    void this.reconcileLoop();
  }

  private async reconcileLoop() {
    try {
      do {
        this.reconcileQueued = false;
        try {
          await this.reconcileOnce();
        } catch (error) {
          if (this.store.isPersistenceUnavailableError(error)) {
            console.warn("[simulation] Reconcile deferred while PostgreSQL is unavailable:", error);
          await sleep(250);
          continue;
        }
          throw error;
        }
      } while (this.reconcileQueued);
    } finally {
      this.reconcileRunning = false;
    }
  }

  private getActiveRound(now = Date.now()) {
    return this.store.rounds
      .filter((round) => round.startAt <= now && round.endAt > now)
      .sort((left, right) => right.startAt - left.startAt)[0];
  }

  private resolveCurrentRoundBinanceOpenReference(round: RoundRecord | undefined, now: number) {
    if (!round || round.startAt > now) {
      return undefined;
    }
    if (isBtcReferencePrice(round.binanceOpenPrice)) {
      const persistedReference = roundNumber(round.binanceOpenPrice, 2);
      this.currentRoundBinanceOpenReferences ??= new Map<string, number>();
      this.currentRoundBinanceOpenReferences.set(round.id, persistedReference);
      return persistedReference;
    }
    this.currentRoundBinanceOpenReferences ??= new Map<string, number>();
    const cachedReference = this.currentRoundBinanceOpenReferences.get(round.id);
    if (isBtcReferencePrice(cachedReference)) {
      return cachedReference;
    }
    if (!this.binanceState.candlesByInterval) {
      return undefined;
    }
    for (const interval of ["5m", "1m", "30s"] as const) {
      const bar = this.binanceState.candlesByInterval[interval].find(
        (candidate) => candidate.startTs === round.startAt && isBtcReferencePrice(candidate.open)
      );
      if (bar && bar.startTs === round.startAt && isBtcReferencePrice(bar.open)) {
        const reference = roundNumber(bar.open, 2);
        this.currentRoundBinanceOpenReferences.set(round.id, reference);
        this.pruneCurrentRoundBinanceOpenReferences(round.id, now);
        return reference;
      }
    }
    return undefined;
  }

  withCurrentRoundBinanceOpenReference<T extends RoundRecord | undefined>(round: T, now = Date.now()): T {
    if (!round) {
      return round;
    }
    const reference = this.resolveCurrentRoundBinanceOpenReference(round, now);
    if (!isBtcReferencePrice(reference) || isBtcReferencePrice(round.binanceOpenPrice)) {
      return round;
    }
    return { ...round, binanceOpenPrice: reference };
  }

  private resolveCurrentRoundCoinbaseOpenReference(round: RoundRecord | undefined, now: number) {
    if (!round || round.startAt > now) {
      return undefined;
    }
    if (isBtcReferencePrice(round.coinbaseOpenPrice)) {
      return roundNumber(round.coinbaseOpenPrice, 2);
    }
    const bar = this.coinbaseCandlesByInterval?.["30s"]?.find(
      (candidate) => candidate.startTs === round.startAt && isBtcReferencePrice(candidate.open)
    );
    return bar ? roundNumber(bar.open, 2) : undefined;
  }

  private resolveRoundCoinbaseCloseReference(round: RoundRecord | undefined, now = Date.now()) {
    if (!round || now < round.endAt) {
      return undefined;
    }
    if (isBtcReferencePrice(round.coinbaseClosePrice)) {
      return roundNumber(round.coinbaseClosePrice, 2);
    }
    const bar = this.coinbaseCandlesByInterval?.["30s"]?.find(
      (candidate) => candidate.endTs === round.endAt && isBtcReferencePrice(candidate.close)
    );
    return bar ? roundNumber(bar.close, 2) : undefined;
  }

  withCurrentRoundCoinbaseOpenReference<T extends RoundRecord | undefined>(round: T, now = Date.now()): T {
    if (!round) {
      return round;
    }
    const reference = this.resolveCurrentRoundCoinbaseOpenReference(round, now);
    if (!isBtcReferencePrice(reference) || isBtcReferencePrice(round.coinbaseOpenPrice)) {
      return round;
    }
    return { ...round, coinbaseOpenPrice: reference };
  }

  private pruneCurrentRoundBinanceOpenReferences(activeRoundId: string, now: number) {
    if (this.currentRoundBinanceOpenReferences.size <= 24) {
      return;
    }
    const recentRoundIds = new Set(
      this.store.rounds
        .filter((round) => round.id === activeRoundId || round.endAt >= now - 2 * 60 * 60 * 1000)
        .map((round) => round.id)
    );
    for (const roundId of this.currentRoundBinanceOpenReferences.keys()) {
      if (!recentRoundIds.has(roundId)) {
        this.currentRoundBinanceOpenReferences.delete(roundId);
      }
    }
  }

  private canCreateNewOrders(round: RoundRecord | undefined, now = Date.now()) {
    if (!round) {
      return false;
    }
    if (now < round.startAt || now >= round.endAt) {
      return false;
    }
    if (round.acceptingOrders === false || round.status !== "Trading") {
      return false;
    }
    return round.endAt - now > this.config.freezeWindowMs;
  }

  private assertActiveTradableRound(round: RoundRecord | undefined, now = Date.now(), freezeMessage: string) {
    if (!round || now < round.startAt || now >= round.endAt) {
      throw new Error("No active round is available.");
    }
    if (round.endAt - now <= this.config.freezeWindowMs) {
      throw new Error(freezeMessage);
    }
    if (round.status !== "Trading") {
      throw new Error(`Round is ${round.status}.`);
    }
  }

  private assertCanBuyOrder(round: RoundRecord | undefined, now = Date.now()) {
    this.assertActiveTradableRound(round, now, "Round entered final 10-second order freeze window.");
    if (!round) {
      throw new Error("No active round is available.");
    }
    if (round.acceptingOrders === false) {
      throw new Error("Round is not accepting new orders.");
    }
  }

  private assertCanSellOrder(round: RoundRecord | undefined, now = Date.now()) {
    this.assertActiveTradableRound(round, now, "Current round entered the final 10-second sell freeze window.");
  }

  private assertCanSellPosition(position: PositionRecord, round: RoundRecord | undefined, now = Date.now()) {
    if (!round) {
      throw new Error("Position round is unavailable.");
    }
    if (position.status !== "open") {
      throw new Error("Position is already closed.");
    }
    const activeRound = this.getActiveRound(now);
    if (!activeRound || activeRound.id !== round.id || now < round.startAt || now >= round.endAt) {
      throw new Error("This position does not belong to the current tradable round.");
    }
    this.assertCanSellOrder(round, now);
  }

  private resolveBookContext(side: TradeSide, round?: RoundRecord, marketId?: string) {
    const resolvedMarketId = marketId ?? this.polymarketState.currentMarket?.id ?? round?.marketId ?? this.config.marketId;
    const resolvedRound = round ?? this.getActiveRound() ?? this.store.getCurrentRound();
    return {
      bookKey: `${resolvedMarketId}:${side}`,
      marketId: resolvedMarketId,
      roundId: resolvedRound?.id
    };
  }

  private resolveReplayBookKey(input: {
    side?: TradeSide;
    bookKey?: string;
    roundId?: string;
    marketId?: string;
  }) {
    if (input.bookKey) {
      return input.bookKey;
    }
    const round = input.roundId ? this.store.getRoundById(input.roundId) : undefined;
    return this.resolveBookContext(input.side ?? "UP", round, input.marketId).bookKey;
  }

  private async syncMatchingBooks() {
    const activeRound = this.getActiveRound();
    await Promise.all([this.syncMatchingBook("UP", activeRound), this.syncMatchingBook("DOWN", activeRound)]);
  }

  private async ensureCurrentMatchingBook(side: TradeSide, round?: RoundRecord, forceSync = false) {
    const state = await this.syncMatchingBook(side, round, forceSync);
    return state?.snapshot ?? this.polymarketState.orderBooks[side];
  }

  private async syncMatchingBook(side: TradeSide, round?: RoundRecord, forceSync = false) {
    const sourceBook = this.polymarketState.orderBooks[side];
    const context = this.resolveBookContext(side, round);
    const cached = this.matchingBooks.get(context.bookKey);
    this.currentBookKeys.set(side, context.bookKey);

    if (!forceSync && cached && this.lastSyncedSnapshotIds.get(context.bookKey) === sourceBook.snapshotId) {
      return cached;
    }

    try {
      const response = await this.matchingClient.syncBook({
        bookKey: context.bookKey,
        roundId: context.roundId,
        marketId: context.marketId,
        bookSide: side,
        source: "Polymarket",
        sourceSnapshot: sourceBook,
        syncedAt: Date.now()
      });
      this.cacheMatchingBook(response.book);
      this.lastSyncedSnapshotIds.set(context.bookKey, sourceBook.snapshotId);
      return response.book;
    } catch {
      if (cached) {
        return cached;
      }

      const current = await this.matchingClient.getCurrentBook(context.bookKey).catch(() => ({ book: undefined }));
      if (current.book) {
        this.cacheMatchingBook(current.book);
        return current.book;
      }
      return undefined;
    }
  }

  private async refreshMatchingBook(bookKey: string) {
    const response = await this.matchingClient.getCurrentBook(bookKey);
    if (response.book) {
      this.cacheMatchingBook(response.book);
    }
    return response.book;
  }

  private cacheMatchingBook(book: MatchingBookState) {
    this.matchingBooks.set(book.bookKey, book);
    this.currentBookKeys.set(book.bookSide, book.bookKey);
  }

  private getDisplayedBook(side: TradeSide): OrderBookSnapshot {
    return this.polymarketState.orderBooks[side];
  }

  private async fetchExecutionBook(side: TradeSide, round: RoundRecord): Promise<ExecutionBookResult> {
    const tokenId = this.resolveTokenId(side, round);
    const fallback = this.polymarketState.orderBooks[side];
    const fallbackTokenId =
      side === "UP" ? this.polymarketState.currentMarket?.upTokenId : this.polymarketState.currentMarket?.downTokenId;
    const fallbackMatchesToken = !tokenId || !fallbackTokenId || tokenId === fallbackTokenId;
      const fallbackFreshMs = Math.max(
        (this.config.polymarketBookCalibrationMs || this.config.polymarketBookPollMs) * 3,
        EXECUTION_BOOK_FRESHNESS_FLOOR_MS
      );
    const fallbackAgeMs = Math.max(Date.now() - fallback.snapshotTs, 0);

    if (fallbackMatchesToken && hasOrderBookDepth(fallback)) {
      return {
        book: cloneOrderBookSnapshot(fallback),
        source: fallbackAgeMs <= fallbackFreshMs ? "cache" : "stale_cache",
        ageMs: fallbackAgeMs,
        fallbackReason: fallbackAgeMs <= fallbackFreshMs ? undefined : "Cached book exceeded freshness target; using it to keep order execution off the external REST critical path."
      };
    }

    try {
      const book = tokenId
        ? await this.polymarketConnector.fetchBookByToken(tokenId)
        : await this.polymarketConnector.fetchBookForSide(side);
      if (hasOrderBookDepth(book)) {
        return {
          book,
          source: "rest",
          ageMs: Math.max(Date.now() - book.snapshotTs, 0)
        };
      }
    } catch (error) {
      if (!fallbackMatchesToken || !hasOrderBookDepth(fallback)) {
        throw error;
      }
      console.warn(
        "[simulation] Falling back to cached Polymarket order book after execution book fetch failed:",
        error instanceof Error ? error.message : error
      );
    }

    if (fallbackMatchesToken && hasOrderBookDepth(fallback)) {
      return {
        book: cloneOrderBookSnapshot(fallback),
        source: "rest_empty_cache_fallback",
        ageMs: fallbackAgeMs,
        fallbackReason: "External REST book was unavailable or empty; using cached Polymarket book."
      };
    }
    throw new Error("Polymarket CLOB depth is unavailable.");
  }

  private resolveTokenId(side: TradeSide, round?: RoundRecord) {
    return side === "UP"
      ? round?.upTokenId ?? this.polymarketState.currentMarket?.upTokenId
      : round?.downTokenId ?? this.polymarketState.currentMarket?.downTokenId;
  }

  private scopedPositions(userId: string, roundId: string, side: TradeSide, positionIds?: string[]) {
    const allowed = positionIds ? new Set(positionIds) : undefined;
    return this.store.positions
      .filter(
        (position) =>
          position.userId === userId &&
          position.roundId === roundId &&
          position.side === side &&
          position.status === "open" &&
          (!allowed || allowed.has(position.id))
      )
      .sort((left, right) => left.openedAt - right.openedAt);
  }

  private availableSellQty(userId: string, roundId: string, side: TradeSide, positionIds?: string[]) {
    return roundNumber(
      this.scopedPositions(userId, roundId, side, positionIds).reduce(
        (sum, position) => sum + Math.max(position.qty - (position.lockedQty ?? 0), 0),
        0
      ),
      4
    );
  }

  private async lockSellQty(
    userId: string,
    roundId: string,
    side: TradeSide,
    qty: number,
    positionIds?: string[]
  ) {
    let remaining = qty;
    for (const position of this.scopedPositions(userId, roundId, side, positionIds)) {
      if (remaining <= QTY_EPSILON) {
        break;
      }
      const available = Math.max(position.qty - (position.lockedQty ?? 0), 0);
      const take = roundNumber(Math.min(available, remaining), 4);
      if (take <= QTY_EPSILON) {
        continue;
      }
      position.lockedQty = roundNumber((position.lockedQty ?? 0) + take, 4);
      remaining = roundNumber(Math.max(remaining - take, 0), 4);
      await this.store.persistPosition(position);
    }
    if (remaining > QTY_EPSILON) {
      throw new Error("Insufficient unlocked position quantity.");
    }
  }

  private async unlockSellQty(userId: string, roundId: string, side: TradeSide, qty: number) {
    let remaining = qty;
    for (const position of this.scopedPositions(userId, roundId, side)) {
      if (remaining <= QTY_EPSILON) {
        break;
      }
      const locked = position.lockedQty ?? 0;
      const release = roundNumber(Math.min(locked, remaining), 4);
      if (release <= QTY_EPSILON) {
        continue;
      }
      position.lockedQty = roundNumber(Math.max(locked - release, 0), 4);
      remaining = roundNumber(Math.max(remaining - release, 0), 4);
      await this.store.persistPosition(position);
    }
  }

  private async applyFilledOrder(
    user: UserRecord,
    round: RoundRecord,
    order: OrderRecord,
    estimate: ClobExecutionEstimate,
    positionIds?: string[],
    exitType: Exclude<OrderLifecycleExitType, "settlement" | "mixed"> = "manual_sell",
    emitUserPayload = true,
    tradePersistSegments?: TradePersistSegments
  ) {
    order.status = "filled";
    order.lifecycleStatus = "filled";
    order.resultType = "all_filled";
    order.fills = estimate.fills;
    order.filledQty = roundNumber(estimate.filledQty, 4);
    order.unfilledQty = 0;
    order.notionalUsdc = roundNumber(estimate.matchedNotional, 2);
    order.avgFillPrice = estimate.avgPrice ? roundNumber(estimate.avgPrice, 4) : undefined;
    order.actualFee = roundCurrency(estimate.estimatedFee ?? order.actualFee ?? 0);
    order.estimatedFee = roundCurrency(order.actualFee);
    order.feeBreakdown = estimate.feeBreakdown ?? order.feeBreakdown;
    order.feeCurrency = "USD";
    order.failureReason = undefined;
    order.partialFilled = false;
    order.serverPublishTs = Date.now();

    if (order.action === "buy") {
      const spend = roundNumber(estimate.matchedNotional, 2);
      const totalSpend = roundCurrency(spend + (order.actualFee ?? 0));
      if (order.frozenUsdc && order.frozenUsdc > 0) {
        user.availableUsdc = roundCurrency(user.availableUsdc + order.frozenUsdc - totalSpend);
        order.frozenUsdc = 0;
      } else {
        user.availableUsdc = roundCurrency(user.availableUsdc - totalSpend);
      }
      const position = this.upsertBuyPosition(
        order.id,
        user.id,
        round.id,
        order.side,
        order.filledQty,
        roundCurrency(estimate.matchedNotional + (order.actualFee ?? 0)),
        order.midPrice || order.avgFillPrice || 0,
        order.actualFee ?? 0
      );
      await Promise.all([
        this.measureTradePersistSegment(tradePersistSegments, "persistPosition", () => this.store.persistPosition(position)),
        this.measureTradePersistSegment(tradePersistSegments, "persistUser", () => this.store.persistUser(user)),
        this.measureTradePersistSegment(tradePersistSegments, "persistOrder", () => this.store.persistOrder(order))
      ]);
      if (emitUserPayload) {
        this.store.emitUserPayload(user.id, "trade");
      }
      return;
    }

    let remainingQty = order.filledQty;
    const feePerQty = (order.actualFee ?? 0) / Math.max(order.filledQty, QTY_EPSILON);
    const proceedsPerQty = estimate.matchedNotional / Math.max(order.filledQty, QTY_EPSILON);
    const positions = this.scopedPositions(user.id, round.id, order.side, positionIds);
    const changedPositions: PositionRecord[] = [];
    for (const position of positions) {
      if (remainingQty <= QTY_EPSILON) {
        break;
      }
      const available = order.frozenQty && order.frozenQty > 0
        ? position.lockedQty ?? 0
        : Math.max(position.qty - (position.lockedQty ?? 0), 0);
      const take = roundNumber(Math.min(available, remainingQty), 4);
      if (take <= QTY_EPSILON) {
        continue;
      }
      const grossProceeds = roundNumber(take * proceedsPerQty, 8);
      const allocatedFee = roundNumber(take * feePerQty, 8);
      const proceeds = roundNumber(grossProceeds - allocatedFee, 8);
      const releasedCost = roundNumber(position.averageEntry * take, 8);
      const realizedPnl = proceeds - releasedCost;
      position.exitFeeUsdc = roundNumber((position.exitFeeUsdc ?? 0) + allocatedFee, 8);
      position.totalFeeUsdc = roundNumber((position.entryFeeUsdc ?? 0) + (position.exitFeeUsdc ?? 0), 8);
      position.qty = roundNumber(Math.max(position.qty - take, 0), 4);
      position.lockedQty = roundNumber(Math.max((position.lockedQty ?? 0) - take, 0), 4);
      position.notionalSpent = roundNumber(Math.max(position.notionalSpent - releasedCost, 0), 4);
      position.costBasisUsdc = position.notionalSpent;
      position.realizedPnl = roundNumber(position.realizedPnl + realizedPnl, 2);
      position.currentMark = roundNumber(order.midPrice || order.avgFillPrice || 0, 4);
      position.unrealizedPnl = roundNumber(position.qty * position.currentMark - position.notionalSpent, 2);
      position.markPnlUsdc = position.unrealizedPnl;
      position.executablePnlUsdc = roundNumber(((position.currentBid ?? position.currentMark) * position.qty) - position.notionalSpent, 2);
      if (position.qty <= QTY_EPSILON) {
        position.qty = 0;
        position.lockedQty = 0;
        position.notionalSpent = 0;
        position.costBasisUsdc = 0;
        position.status = "closed";
        position.closedAt = Date.now();
        position.currentMark = roundNumber(order.avgFillPrice ?? 0, 4);
        position.currentBid = undefined;
        position.currentAsk = undefined;
        position.currentMid = undefined;
        position.currentValue = 0;
        position.sourceLatencyMs = undefined;
        position.unrealizedPnl = 0;
        position.markPnlUsdc = 0;
        position.executablePnlUsdc = 0;
        position.settlementResult = "sold";
      }
      remainingQty = roundNumber(Math.max(remainingQty - take, 0), 4);
      changedPositions.push(position);
    }
    if (remainingQty > QTY_EPSILON) {
      throw new Error("Filled sell order could not be applied to local positions.");
    }
    const buyOrderIds = [...new Set(changedPositions.map((position) => position.buyOrderId).filter(Boolean) as string[])];
    user.availableUsdc = roundCurrency(user.availableUsdc + estimate.matchedNotional - (order.actualFee ?? 0));
    order.frozenQty = 0;
    await Promise.all([
      ...changedPositions.map((position) =>
        this.measureTradePersistSegment(tradePersistSegments, "persistPosition", () => this.store.persistPosition(position))
      ),
      this.measureTradePersistSegment(tradePersistSegments, "persistUser", () => this.store.persistUser(user)),
      this.measureTradePersistSegment(tradePersistSegments, "persistOrder", () => this.store.persistOrder(order)),
      this.measureTradePersistSegment(tradePersistSegments, "persistOrderLifecycle", () => this.store.applyLifecycleExit({
        userId: user.id,
        roundId: round.id,
        side: order.side,
        qty: order.filledQty,
        exitType,
        exitTokenPrice: order.avgFillPrice,
        exitFee: order.actualFee ?? 0,
        buyOrderIds: buyOrderIds.length > 0 ? buyOrderIds : undefined
      }))
    ]);
    if (emitUserPayload) {
      this.store.emitUserPayload(user.id, "trade");
    }
  }

  private async recordBuyLifecycle(
    user: UserRecord,
    round: RoundRecord,
    order: OrderRecord,
    snapshot: MarketSnapshot,
    tradePersistSegments?: TradePersistSegments
  ) {
    if (order.action !== "buy" || order.resultType !== "all_filled" || order.filledQty <= QTY_EPSILON) {
      return;
    }
    const now = Date.now();
    const btcTradePrice =
      snapshot.binance.spotPrice > 0
        ? snapshot.binance.spotPrice
        : snapshot.currentPrice > 0
          ? snapshot.currentPrice
          : undefined;
    const btcOpenPriceToBeat = round.priceToBeat > 0 ? round.priceToBeat : snapshot.priceToBeat || undefined;
    await this.measureTradePersistSegment(tradePersistSegments, "persistOrderLifecycle", () => this.store.persistOrderLifecycle({
      id: `ol_${order.id}`,
      buyOrderId: order.id,
      traceId: order.traceId,
      userId: user.id,
      testerId: user.id,
      roundId: order.roundId,
      symbol: order.symbol,
      assetClass: "BTC",
      marketId: order.marketId,
      marketSlug: order.marketSlug,
      direction: order.side,
      orderTimestampMs: order.createdAt,
      entryTokenPrice: order.avgFillPrice,
      btcTradePrice,
      btcOpenPriceToBeat,
      deltaBtc:
        typeof btcTradePrice === "number" && typeof btcOpenPriceToBeat === "number"
          ? roundNumber(btcTradePrice - btcOpenPriceToBeat, 2)
          : undefined,
      volumeTokenQty: order.filledQty,
      remainingTokenQty: order.filledQty,
      closedTokenQty: 0,
      positionNotional: roundCurrency(order.notionalUsdc + (order.actualFee ?? 0)),
      exitNotional: 0,
      orderBookSnapshotRef: order.orderBookSnapshotRef,
      actualFillPrice: order.avgFillPrice,
      slippageBps: order.slippageBps,
      matchLatencyMs: order.matchLatencyMs,
      entryFee: order.actualFee ?? 0,
      feeCurrency: order.feeCurrency,
      createdAt: now,
      updatedAt: now
    }));
  }

  private async processPendingOrders() {
    const pendingOrders = this.store.orders.filter((order) => order.status === "pending");
    if (pendingOrders.length === 0) {
      return;
    }

    const books: Record<TradeSide, OrderBookSnapshot> = {
      UP: this.polymarketState.orderBooks.UP,
      DOWN: this.polymarketState.orderBooks.DOWN
    };

    for (const order of pendingOrders) {
      const round = this.store.getRoundById(order.roundId);
      const user = this.store.getUserById(order.userId);
      if (!round || !user) {
        continue;
      }

      if (!this.canCreateNewOrders(round)) {
        await this.failPendingOrder(
          user,
          order,
          "Round entered the final order freeze window before the limit order fully matched."
        );
        continue;
      }

      const book = books[order.side];
      if (!book || (book.bids.length === 0 && book.asks.length === 0)) {
        continue;
      }

      const currentOdds = order.action === "buy" ? book.bestAsk : book.bestBid;
      const limitPrice = order.limitPrice ?? 0;
      const triggerReached =
        currentOdds > 0 && (order.action === "buy" ? currentOdds <= limitPrice : currentOdds >= limitPrice);
      if (!triggerReached) {
        continue;
      }
      const marketInfo = clobMarketInfoFor(this.polymarketState.currentMarket);
      const estimate = this.createTriggeredLimitEstimate(
        order,
        currentOdds,
        Date.now(),
        marketInfo
      );

      await this.runTradeWriteTransaction(async () => {
        order.bookHash = book.snapshotId;
        order.bestBid = book.bestBid;
        order.bestAsk = book.bestAsk;
        order.midPrice = book.midPrice;
        order.bookSnapshotTs = book.snapshotTs;
        order.sourceLatencyMs = Math.max(Date.now() - book.snapshotTs, 0);
        order.matchLatencyMs = Math.max(Date.now() - order.createdAt, 1);
        const actionSnapshot = this.captureActionSnapshot();
        await this.applyFilledOrder(user, round, order, estimate, undefined, "manual_sell", false);
        if (order.action === "buy") {
          await this.recordBuyLifecycle(user, round, order, actionSnapshot);
        }
        await Promise.allSettled([
          this.writeAuditLog({
            eventId: this.store.newId("evt"),
            traceId: order.traceId,
            category: "matching",
            actionType: "limit_order_triggered",
            actionStatus: "success",
            userId: user.id,
            role: user.role,
            pageName: "trade.main",
            moduleName: "pending.orders",
            symbol: order.symbol,
            roundId: order.roundId,
            serverRecvTs: Date.now(),
            serverPublishTs: Date.now(),
            backendLatencyMs: order.matchLatencyMs,
            resultCode: "LIMIT_ORDER_FILLED",
            resultMessage: "Pending paper limit order triggered by live odds.",
            details: {
              traceId: order.traceId,
              roundId: order.roundId,
              marketId: order.marketId,
              marketSlug: order.marketSlug,
              orderId: order.id,
              bookKey: order.bookKey,
              bookHash: order.bookHash,
              bookSnapshotId: order.bookHash,
              sourceLatencyMs: order.sourceLatencyMs,
              avgFillPrice: order.avgFillPrice,
              filledQty: order.filledQty,
              slippageBps: order.slippageBps
            }
          }),
          this.writeBehaviorLog(
            this.createBehaviorLog({
              user,
              actionType: "limit_order_triggered",
              actionStatus: "success",
              traceId: order.traceId,
              orderId: order.id,
              round,
              snapshot: actionSnapshot,
              direction: order.side,
              positionNotional: order.notionalUsdc,
              bookSnapshot: book,
              order,
              actualFillPrice: order.avgFillPrice,
              slippageBps: order.slippageBps,
              partialFilled: order.partialFilled,
              unfilledQty: order.unfilledQty,
              executionLatencyMs: order.matchLatencyMs
            })
          )
        ]);
      });
      this.store.emitUserPayload(user.id, "trade");
    }
  }

  private createTriggeredLimitEstimate(
    order: OrderRecord,
    price: number,
    executedAt: number,
    marketInfo: ClobMarketInfo
  ): ClobExecutionEstimate {
    const qty =
      order.action === "buy"
        ? roundNumber((order.requestedAmountUsdc || order.notionalUsdc) / Math.max(price, 0.0001), 4)
        : roundNumber(order.frozenQty || order.requestedQty || order.expectedQty, 4);
    const matchedNotional = roundNumber(qty * price, 2);
    const feeResult = calculateClobFee({
      role: "taker",
      marketInfo,
      price,
      quantity: qty,
      notional: matchedNotional
    });
    return {
      fullyMatched: true,
      fills: [
        {
          fillId: `${order.id}:limit-trigger:1`,
          makerOrderId: `${order.bookHash ?? order.bookKey ?? "live"}:${order.action === "buy" ? "ask" : "bid"}`,
          takerOrderId: order.id,
          price: roundNumber(price, 4),
          qty,
          notional: matchedNotional,
          makerOwnerId: "external:polymarket",
          makerOwnerType: "external",
          executedAt
        }
      ],
      filledQty: qty,
      matchedNotional,
      remainingQty: 0,
      remainingNotional: 0,
      avgPrice: roundNumber(price, 4),
      worstPrice: roundNumber(price, 4),
      estimatedFee: feeResult.fee,
      feeBreakdown: feeResult.breakdown
    };
  }

  private async failPendingOrder(user: UserRecord, order: OrderRecord, reason: string) {
    await this.runTradeWriteTransaction(async () => {
      const releasedFrozenUsdc = order.frozenUsdc ?? 0;
      const releasedFrozenQty = order.frozenQty ?? 0;
      if (order.frozenUsdc && order.frozenUsdc > 0) {
        user.availableUsdc = roundNumber(user.availableUsdc + order.frozenUsdc, 2);
        order.frozenUsdc = 0;
        await this.store.persistUser(user);
      }
      if (order.frozenQty && order.frozenQty > 0) {
        await this.unlockSellQty(user.id, order.roundId, order.side, order.frozenQty);
        order.frozenQty = 0;
      }
      order.status = "failed";
      order.lifecycleStatus = "failed";
      order.resultType = "all_failed";
      order.failureReason = reason;
      order.serverPublishTs = Date.now();
      await this.store.persistOrder(order);
      const now = Date.now();
      const snapshot = this.captureActionSnapshot();
      const round = this.store.getRoundById(order.roundId);
      await Promise.allSettled([
        this.writeAuditLog({
          eventId: this.store.newId("evt"),
          traceId: order.traceId,
          category: "matching",
          actionType: "limit_order_failed",
          actionStatus: "failed",
          userId: user.id,
          role: user.role,
          pageName: "trade.main",
          moduleName: "pending.orders",
          symbol: order.symbol,
          roundId: order.roundId,
          serverRecvTs: now,
          serverPublishTs: now,
          backendLatencyMs: order.matchLatencyMs,
          resultCode: "LIMIT_ORDER_FAILED",
          resultMessage: reason,
          details: {
            traceId: order.traceId,
            roundId: order.roundId,
            marketId: order.marketId,
            marketSlug: order.marketSlug,
            orderId: order.id,
            bookKey: order.bookKey,
            bookSnapshotId: order.bookHash,
            releasedFrozenUsdc,
            releasedFrozenQty,
            failureReason: reason
          }
        }),
        this.writeBehaviorLog(
          this.createBehaviorLog({
            user,
            actionType: "limit_order_failed",
            actionStatus: "failed",
            traceId: order.traceId,
            orderId: order.id,
            round,
            snapshot,
            direction: order.side,
            positionNotional: order.notionalUsdc,
            bookSnapshot: snapshot.orderBooks[order.side],
            order,
            partialFilled: order.partialFilled,
            unfilledQty: order.unfilledQty,
            failureReason: reason,
            frozenAssetRelease: {
              releasedFrozenUsdc,
              releasedFrozenQty
            }
          })
        )
      ]);
    });
    this.store.emitUserPayload(user.id, "trade");
  }

  private schedulePendingOrderProcessing() {
    if (this.pendingOrdersRunning) {
      return;
    }
    this.pendingOrdersRunning = true;
    void this.processPendingOrders()
      .catch((error) => {
        console.warn("[simulation] Pending order processing failed:", error);
      })
      .finally(() => {
        this.pendingOrdersRunning = false;
      });
  }

  private async reconcileOnce() {
    await this.fullReconcileOnce();
  }

  private async fullReconcileOnce() {
    const startedAt = Date.now();
    try {
      await this.syncDiscoveredRounds();
      await this.syncCurrentRoundMarket();
      const roundChangedUsers = await this.processRounds();
      const snapshot = this.buildSnapshot();
      const changedUsers = this.refreshOpenPositions(snapshot);
      await this.store.setMarketSnapshot(snapshot);
      this.scheduleLatencyLogs(snapshot);
      for (const userId of new Set([...roundChangedUsers, ...changedUsers])) {
        this.store.emitUserPayload(userId);
      }
      this.schedulePendingOrderProcessing();
    } finally {
      appMetrics.recordMarketSnapshotRefresh({
        mode: "full_reconcile",
        durationMs: Date.now() - startedAt
      });
    }
  }

  private scheduleLatencyLogs(snapshot: MarketSnapshot) {
    this.queuedLatencySnapshot = snapshot;
    if (this.latencyLogsRunning) {
      return;
    }
    this.latencyLogsRunning = true;
    setImmediate(() => {
      void this.flushLatencyLogs();
    });
  }

  private async flushLatencyLogs() {
    try {
      while (this.queuedLatencySnapshot) {
        const snapshot = this.queuedLatencySnapshot;
        this.queuedLatencySnapshot = undefined;
        await this.emitLatencyLogs(snapshot);
      }
    } catch (error) {
      console.warn("[simulation] Latency log flush failed:", error);
    } finally {
      this.latencyLogsRunning = false;
      if (this.queuedLatencySnapshot) {
        this.scheduleLatencyLogs(this.queuedLatencySnapshot);
      }
    }
  }

  private async syncDiscoveredRounds() {
    const now = Date.now();
    for (const discovered of this.polymarketState.discoveredRounds) {
      const currentMarket = this.polymarketState.currentMarket;
      const isCurrentMarket = currentMarket?.slug === discovered.marketSlug;
      const existing = this.store.getRoundById(discovered.id);
      const merged: RoundRecord = {
        ...discovered,
        marketId: discovered.marketId || existing?.marketId || this.config.marketId,
        eventId: discovered.eventId ?? existing?.eventId,
        marketSlug: discovered.marketSlug ?? existing?.marketSlug,
        eventSlug: discovered.eventSlug ?? existing?.eventSlug,
        conditionId: discovered.conditionId ?? existing?.conditionId,
        seriesSlug: discovered.seriesSlug ?? existing?.seriesSlug,
        upTokenId: discovered.upTokenId ?? existing?.upTokenId,
        downTokenId: discovered.downTokenId ?? existing?.downTokenId,
        title: discovered.title ?? existing?.title,
        resolutionSource: discovered.resolutionSource ?? existing?.resolutionSource,
        priceToBeat: existing?.priceToBeat && existing.priceToBeat > 0 ? existing.priceToBeat : 0,
        priceToBeatSource: existing?.priceToBeat && existing.priceToBeat > 0 ? existing.priceToBeatSource : undefined,
        priceToBeatCapturedAt:
          existing?.priceToBeat && existing.priceToBeat > 0 ? existing.priceToBeatCapturedAt : undefined,
        status: existing?.status ?? discovered.status,
        pollCount: existing?.pollCount ?? discovered.pollCount,
        pollStartAt: existing?.pollStartAt,
        lastPollAt: existing?.lastPollAt,
        closingSpotPrice: existing?.closingSpotPrice,
        settledSide: existing?.settledSide,
        settlementPrice: existing?.settlementPrice,
        settlementTs: existing?.settlementTs,
        settlementSource: existing?.settlementSource,
        polymarketSettlementPrice: existing?.polymarketSettlementPrice,
        polymarketSettlementStatus: existing?.polymarketSettlementStatus,
        polymarketOpenPrice: isBtcReferencePrice(existing?.polymarketOpenPrice) ? existing.polymarketOpenPrice : undefined,
        polymarketClosePrice: isBtcReferencePrice(existing?.polymarketClosePrice) ? existing.polymarketClosePrice : undefined,
        polymarketOpenPriceSource: isBtcReferencePrice(existing?.polymarketOpenPrice) ? existing?.polymarketOpenPriceSource : undefined,
        polymarketClosePriceSource: isBtcReferencePrice(existing?.polymarketClosePrice) ? existing?.polymarketClosePriceSource : undefined,
        settlementReceivedAt: existing?.settlementReceivedAt,
        redeemScheduledAt: existing?.redeemScheduledAt,
        binanceOpenPrice: existing?.binanceOpenPrice,
        binanceClosePrice: existing?.binanceClosePrice,
        coinbaseOpenPrice: existing?.coinbaseOpenPrice,
        coinbaseClosePrice: existing?.coinbaseClosePrice,
        redeemStartTs: existing?.redeemStartTs,
        redeemFinishTs: existing?.redeemFinishTs,
        manualReason: existing?.manualReason,
        acceptingOrders:
          isCurrentMarket && currentMarket
            ? currentMarket.acceptingOrders
            : discovered.acceptingOrders ?? existing?.acceptingOrders,
        closingPriceSource: existing?.closingPriceSource
      };
      if (this.roundChanged(existing, merged)) {
        await this.store.upsertRound(merged);
      }
    }
  }

  private async processRounds() {
    const now = Date.now();
    const changedUsers = new Set<string>();
    const rounds = [...this.store.rounds]
      .filter((round) => round.endAt >= now - 2 * 60 * 60 * 1000)
      .sort((left, right) => left.startAt - right.startAt);

    for (const round of rounds) {
      const before = this.roundSignature(round);
      const liveDetail =
        this.polymarketState.currentMarket?.slug === round.marketSlug ? this.polymarketState.currentMarket : undefined;

      if (liveDetail) {
        this.applyMarketMetadata(round, liveDetail);
      }

      const binanceOpenReference = this.resolveCurrentRoundBinanceOpenReference(round, now);
      if (!round.binanceOpenPrice && isBtcReferencePrice(binanceOpenReference)) {
        round.binanceOpenPrice = binanceOpenReference;
      }

      if (!round.binanceClosePrice && now >= round.endAt && this.binanceState.price > 0) {
        round.binanceClosePrice = roundNumber(this.binanceState.price, 2);
      }

      const coinbaseOpenReference = this.resolveCurrentRoundCoinbaseOpenReference(round, now);
      if (!round.coinbaseOpenPrice && isBtcReferencePrice(coinbaseOpenReference)) {
        round.coinbaseOpenPrice = coinbaseOpenReference;
      }

      const coinbaseCloseReference = this.resolveRoundCoinbaseCloseReference(round);
      if (!round.coinbaseClosePrice && isBtcReferencePrice(coinbaseCloseReference)) {
        round.coinbaseClosePrice = coinbaseCloseReference;
      }

      if (!round.closingSpotPrice && now >= round.endAt && this.binanceState.price > 0) {
        round.closingSpotPrice = roundNumber(this.binanceState.price, 2);
        round.closingPriceSource = "Gamma";
      }
      this.syncPriceToBeatFromPolymarketOpenPrice(round, now);
      this.refreshPreliminarySettlement(round, now);

      const resolved = this.findResolvedMarketForRound(round);
      if (
        resolved &&
        !round.settledSide &&
        this.roundMatchesResolvedMarket(round, resolved)
      ) {
        if (now >= round.endAt && round.status !== "Closed") {
          const confirmed = this.confirmSettlementFromResolved(round, resolved);
          if (confirmed) {
            await this.writeSettlementLog(round, "success", "Polymarket market_resolved event confirmed settlement.");
          }
        }
      }

      const nextStatus = this.computeRoundStatus(round, now);
      if (nextStatus !== round.status) {
        round.status = nextStatus;
      }

      if (round.status === "Polling" || this.shouldPrefetchGamma(round, now)) {
        this.scheduleSettlementPoll(round, now);
      }

      this.scheduleRedeem(round, now);

      if (round.status === "Redeeming" && round.redeemStartTs && !round.redeemScheduledAt) {
        round.redeemScheduledAt = round.redeemStartTs + REDEEM_DELAY_MS;
      }

      if (
        round.status === "Redeeming" &&
        !round.redeemFinishTs &&
        round.redeemScheduledAt &&
        now >= round.redeemScheduledAt
      ) {
        await this.applyRedeem(round);
      }

      if (this.roundSignature(round) !== before) {
        await this.store.upsertRound(round);
        for (const userId of this.collectRoundPositionUsers(round.id)) {
          changedUsers.add(userId);
        }
      }
    }

    return changedUsers;
  }

  private scheduleSettlementPoll(round: RoundRecord, now: number) {
    if (this.pollLocks.has(round.id)) {
      return;
    }
    const before = this.roundSignature(round);
    void this.pollSettlement(round, now)
      .then(async () => {
        if (this.roundSignature(round) !== before) {
          await this.store.upsertRound(round);
          if (typeof (this.store as unknown as { getCurrentRound?: unknown }).getCurrentRound === "function") {
            try {
              await this.store.setMarketSnapshot(this.buildSnapshot());
            } catch (error) {
              console.warn(`[simulation] Settlement market snapshot refresh failed for ${round.id}:`, error);
            }
          }
          for (const userId of this.collectRoundPositionUsers(round.id)) {
            this.store.emitUserPayload(userId);
          }
          this.scheduleReconcile();
        }
      })
      .catch((error) => {
        console.warn(`[simulation] Settlement polling failed for ${round.id}:`, error);
      });
  }

  private shouldPrefetchGamma(round: RoundRecord, now: number) {
    const remainingMs = round.endAt - now;
    return remainingMs <= GAMMA_PREFETCH_START_MS && remainingMs >= GAMMA_PREFETCH_END_MS && !round.settledSide;
  }

  private gammaPollIntervalFor(round: RoundRecord, now: number) {
    const remainingMs = round.endAt - now;
    if (remainingMs > GAMMA_PREFETCH_FAST_START_MS) {
      return GAMMA_PREFETCH_INTERVAL_MS;
    }
    return this.config.gammaPollIntervalMs;
  }

  private async restoreCoinbaseMarketCandles(now = Date.now()) {
    const candles = this.store.getMarketCandles({
      source: "coinbase",
      symbol: this.config.symbol,
      interval: "30s",
      fromOpenTs: now - COINBASE_MARKET_CANDLE_RESTORE_MS
    });
    if (candles.length === 0) {
      return;
    }
    this.mergeCoinbaseThirtySecondBars(candles.map((candle) => marketCandleToBar(candle)));
  }

  private mergeCoinbaseThirtySecondBars(bars: CandleBar[]) {
    this.coinbaseCandlesByInterval["30s"] = mergeCoinbaseHistoryBars(
      this.coinbaseCandlesByInterval["30s"],
      bars,
      "30s"
    );
    this.refreshCoinbaseAggregatesFromThirtySecondBars();
  }

  private refreshCoinbaseAggregatesFromThirtySecondBars() {
    const thirtySecondBars = this.coinbaseCandlesByInterval["30s"];
    this.coinbaseCandlesByInterval["1m"] = aggregateCoinbaseBars("1m", thirtySecondBars);
    this.coinbaseCandlesByInterval["5m"] = aggregateCoinbaseBars("5m", thirtySecondBars);
    this.coinbaseCandlesByInterval["15m"] = aggregateCoinbaseBars("15m", thirtySecondBars);
    this.coinbaseCandlesByInterval["1h"] = aggregateCoinbaseBars("1h", thirtySecondBars);
  }

  private refreshCoinbaseAggregateBucketFromThirtySecondBar(bar: CandleBar) {
    for (const interval of ["1m", "5m", "15m", "1h"] as const) {
      const bucketSize = COINBASE_INTERVAL_MS[interval];
      const startTs = Math.floor(bar.startTs / bucketSize) * bucketSize;
      const endTs = startTs + bucketSize;
      const sourceBars = this.coinbaseCandlesByInterval["30s"].filter(
        (candidate) => candidate.startTs >= startTs && candidate.startTs < endTs
      );
      const [aggregate] = aggregateCoinbaseBars(interval, sourceBars);
      if (!aggregate) {
        continue;
      }
      const existing = this.coinbaseCandlesByInterval[interval].filter((candidate) => candidate.startTs !== startTs);
      this.coinbaseCandlesByInterval[interval] = [...existing, aggregate]
        .sort((left, right) => left.startTs - right.startTs)
        .slice(-COINBASE_BAR_LIMITS[interval]);
    }
  }

  private coinbaseMarketCandleFromBar(bar: CandleBar, origin: MarketCandleRecord["origin"]): MarketCandleRecord {
    const openTs = Math.floor(bar.startTs / COINBASE_INTERVAL_MS["30s"]) * COINBASE_INTERVAL_MS["30s"];
    return {
      source: "coinbase",
      symbol: this.config.symbol,
      interval: "30s",
      openTs,
      closeTs: openTs + COINBASE_INTERVAL_MS["30s"],
      open: roundNumber(bar.open, 2),
      high: roundNumber(bar.high, 2),
      low: roundNumber(bar.low, 2),
      close: roundNumber(bar.close, 2),
      volume: roundNumber(bar.volume ?? 0, 6),
      origin,
      updatedAt: Date.now()
    };
  }

  private queueCoinbaseMarketCandle(candle: MarketCandleRecord) {
    const existing = this.pendingCoinbaseMarketCandles.get(candle.openTs);
    if (shouldReplaceCoinbaseMarketCandle(existing, candle)) {
      this.pendingCoinbaseMarketCandles.set(candle.openTs, candle);
    }
    if (this.pendingCoinbaseMarketCandles.size >= COINBASE_MARKET_CANDLE_FLUSH_SIZE) {
      void this.flushPendingCoinbaseMarketCandles();
      return;
    }
    if (!this.coinbaseMarketCandleFlushTimer) {
      this.coinbaseMarketCandleFlushTimer = setTimeout(() => {
        this.coinbaseMarketCandleFlushTimer = undefined;
        void this.flushPendingCoinbaseMarketCandles();
      }, COINBASE_MARKET_CANDLE_FLUSH_MS);
    }
  }

  private async flushPendingCoinbaseMarketCandles() {
    if (this.coinbaseMarketCandleFlushRunning || this.pendingCoinbaseMarketCandles.size === 0) {
      return;
    }
    this.coinbaseMarketCandleFlushRunning = true;
    const batch = [...this.pendingCoinbaseMarketCandles.values()];
    try {
      await this.store.upsertMarketCandles(batch);
      for (const candle of batch) {
        const current = this.pendingCoinbaseMarketCandles.get(candle.openTs);
        if (current && current.updatedAt <= candle.updatedAt) {
          this.pendingCoinbaseMarketCandles.delete(candle.openTs);
        }
      }
    } catch (error) {
      console.warn("[simulation] Coinbase market candle flush failed:", error);
    } finally {
      this.coinbaseMarketCandleFlushRunning = false;
      if (this.pendingCoinbaseMarketCandles.size > 0 && !this.coinbaseMarketCandleFlushTimer) {
        this.coinbaseMarketCandleFlushTimer = setTimeout(() => {
          this.coinbaseMarketCandleFlushTimer = undefined;
          void this.flushPendingCoinbaseMarketCandles();
        }, COINBASE_MARKET_CANDLE_FLUSH_MS);
      }
    }
  }

  private recordCoinbaseSample(price: number, ts = Date.now()) {
    if (!this.config.coinbaseEnabled || !Number.isFinite(price) || price <= 0) {
      return;
    }
    const sampleKey = `${ts}:${roundNumber(price, 2)}`;
    if (this.lastCoinbaseSampleKey === sampleKey) {
      return;
    }
    this.lastCoinbaseSampleKey = sampleKey;
    const bucketStart = Math.floor(ts / COINBASE_INTERVAL_MS["30s"]) * COINBASE_INTERVAL_MS["30s"];
    const bucketEnd = bucketStart + COINBASE_INTERVAL_MS["30s"];
    const current30s = this.coinbaseCandlesByInterval["30s"].at(-1);
    let next30s: CandleBar;
    if (current30s && current30s.startTs === bucketStart) {
      current30s.high = roundNumber(Math.max(current30s.high, price), 2);
      current30s.low = roundNumber(Math.min(current30s.low, price), 2);
      current30s.close = roundNumber(price, 2);
      current30s.volume = roundNumber(current30s.volume + 1, 6);
      next30s = current30s;
    } else {
      next30s = {
        interval: "30s",
        startTs: bucketStart,
        endTs: bucketEnd,
        open: roundNumber(price, 2),
        high: roundNumber(price, 2),
        low: roundNumber(price, 2),
        close: roundNumber(price, 2),
        volume: 1
      };
      this.coinbaseCandlesByInterval["30s"] = [...this.coinbaseCandlesByInterval["30s"], next30s].slice(
        -COINBASE_BAR_LIMITS["30s"]
      );
    }
    this.refreshCoinbaseAggregateBucketFromThirtySecondBar(next30s);
    this.queueCoinbaseMarketCandle(this.coinbaseMarketCandleFromBar(next30s, "rtds_30s"));
    const sampleBucketStart = Math.floor(ts / 5000) * 5000;
    const sampleBucketEnd = sampleBucketStart + 5000;
    const current = this.coinbaseCandles5s.at(-1);
    if (current && current.startTs === sampleBucketStart) {
      current.high = roundNumber(Math.max(current.high, price), 2);
      current.low = roundNumber(Math.min(current.low, price), 2);
      current.close = roundNumber(price, 2);
      current.volume += 1;
      return;
    }
    this.coinbaseCandles5s.push({
      interval: "5s",
      startTs: sampleBucketStart,
      endTs: sampleBucketEnd,
      open: roundNumber(price, 2),
      high: roundNumber(price, 2),
      low: roundNumber(price, 2),
      close: roundNumber(price, 2),
      volume: 1
    });
    if (this.coinbaseCandles5s.length > 50) {
      this.coinbaseCandles5s = this.coinbaseCandles5s.slice(-50);
    }
  }

  private syncCoinbaseHistoryCandles(candlesByInterval?: CoinbaseConnectorState["candlesByInterval"]) {
    if (!candlesByInterval) {
      return;
    }
    const bars = candlesByInterval["30s"];
    if (!bars?.length) {
      return;
    }
    const latestBar = bars.at(-1);
    const now = Date.now();
    if (now - this.lastCoinbaseHistorySyncAt < COINBASE_HISTORY_CANDLE_SYNC_MIN_MS) {
      return;
    }
    const historyKey = `${bars.length}:${latestBar?.startTs ?? 0}:${latestBar?.close ?? 0}`;
    if (this.lastCoinbaseHistorySyncKey === historyKey) {
      return;
    }
    this.lastCoinbaseHistorySyncKey = historyKey;
    this.lastCoinbaseHistorySyncAt = now;
    const historyCandles = bars.map((bar) => this.coinbaseMarketCandleFromBar(bar, "history_1m_split"));
    this.mergeCoinbaseThirtySecondBars(historyCandles.map((candle) => marketCandleToBar(candle)));
    for (const candle of historyCandles) {
      this.queueCoinbaseMarketCandle(candle);
    }
  }

  private recordCurrentRoundUpPricePoint(round: RoundRecord | undefined, price: number, ts: number) {
    if (!round || !Number.isFinite(price) || price <= 0) {
      this.currentRoundUpPriceSeries = [];
      this.currentRoundUpPriceSeriesRoundId = undefined;
      return;
    }
    if (this.currentRoundUpPriceSeriesRoundId !== round.id) {
      this.currentRoundUpPriceSeriesRoundId = round.id;
      this.currentRoundUpPriceSeries = [];
    }
    const clampedTs = Math.max(round.startAt, Math.min(ts, round.endAt));
    const normalizedPrice = roundNumber(price, 4);
    const lastPoint = this.currentRoundUpPriceSeries.at(-1);
    if (!lastPoint) {
      this.currentRoundUpPriceSeries = [{ ts: round.startAt, price: normalizedPrice }];
      if (clampedTs !== round.startAt) {
        this.currentRoundUpPriceSeries.push({ ts: clampedTs, price: normalizedPrice });
      }
      return;
    }
    if (clampedTs <= lastPoint.ts) {
      lastPoint.ts = Math.max(lastPoint.ts, clampedTs);
      lastPoint.price = normalizedPrice;
      return;
    }
    if (lastPoint.price === normalizedPrice && clampedTs - lastPoint.ts < 1000) {
      lastPoint.ts = clampedTs;
      return;
    }
    this.currentRoundUpPriceSeries.push({ ts: clampedTs, price: normalizedPrice });
  }

  private currentRoundUpPriceSeriesSnapshot(round: RoundRecord | undefined, now: number) {
    if (!round || this.currentRoundUpPriceSeriesRoundId !== round.id || this.currentRoundUpPriceSeries.length === 0) {
      return [] as CandlePoint[];
    }
    const points = this.currentRoundUpPriceSeries
      .filter((point) => point.ts >= round.startAt && point.ts <= round.endAt)
      .map(cloneCandlePoint);
    const lastPoint = points.at(-1);
    if (lastPoint) {
      const trailingTs = Math.max(round.startAt, Math.min(now, round.endAt));
      if (trailingTs > lastPoint.ts) {
        points.push({
          ts: trailingTs,
          price: lastPoint.price
        });
      }
    }
    return points;
  }

  private buildSnapshot(): MarketSnapshot {
    const now = Date.now();
    const currentRound = this.store.getCurrentRound(now);
    const currentMarket = this.polymarketState.currentMarket;
    const matchedMarket = this.roundMatchesMarket(currentRound, currentMarket) ? currentMarket : undefined;
    const upBook = matchedMarket ? this.getDisplayedBook("UP") : this.createEmptyOrderBook("UP");
    const downBook = matchedMarket ? this.getDisplayedBook("DOWN") : this.createEmptyOrderBook("DOWN");
    const upPrice = isPositivePrice(upBook.bestAsk) ? upBook.bestAsk : 0;
    const downPrice = isPositivePrice(downBook.bestAsk) ? downBook.bestAsk : 0;
    const recentTrades = matchedMarket ? [...this.polymarketState.recentTrades] : [];
    const displayPrices = resolvePairedDisplayPrices({
      upBook,
      downBook,
      recentTrades,
      outcomePrices: matchedMarket?.outcomePrices
    });
    const upDisplayPrice = displayPrices.UP;
    const downDisplayPrice = displayPrices.DOWN;
    const coinbasePrice =
      this.config.coinbaseEnabled && this.coinbaseState.price > 0 ? roundNumber(this.coinbaseState.price, 2) : 0;
    const binancePrice = this.binanceState.price > 0 ? roundNumber(this.binanceState.price, 2) : 0;
    const currentRoundBinanceOpenReference = this.resolveCurrentRoundBinanceOpenReference(currentRound, now);
    const currentRoundCoinbaseOpenReference = this.resolveCurrentRoundCoinbaseOpenReference(currentRound, now);
    const countdownTargetTs = currentRound
      ? currentRound.startAt > now
        ? currentRound.startAt
        : currentRound.endAt
      : undefined;
    const countdownMs = countdownTargetTs ? Math.max(Math.min(countdownTargetTs - now, FIVE_MINUTE_MS), 0) : 0;
      const marketTitle = currentRound
      ? `${this.config.symbol} 5-Min Round ${utcRangeText(currentRound.startAt, currentRound.endAt)}`
      : matchedMarket
        ? `${this.config.symbol} 5-Min Round ${utcRangeText(matchedMarket.startAt, matchedMarket.endAt)}`
        : `${this.config.symbol} 5-Min Round UTC`;
      const candlesByInterval = this.binanceState.candlesByInterval;
      const clobDelta = matchedMarket ? roundNumber(this.polymarketState.delta, 4) : 0;
      const clobVolume = matchedMarket ? roundNumber(this.polymarketState.volume, 4) : 0;
      const currentRoundUpPrice = upDisplayPrice.value;
      this.recordCurrentRoundUpPricePoint(currentRound, currentRoundUpPrice, now);
      const currentRoundUpPriceSeries = this.currentRoundUpPriceSeriesSnapshot(currentRound, now);
      const marketInfo = clobMarketInfoFor(matchedMarket);
      const sources = {
        binance: this.normalizeSourceHealth(this.binanceState.status, now),
        coinbase: this.normalizeSourceHealth(this.coinbaseState.status, now),
        clob: this.normalizeSourceHealth(this.polymarketState.status, now)
      };
      const latencyBreakdown = {
        sourceEventAge: {
          binance: Math.max(now - sources.binance.sourceEventTs, 0),
          coinbase: Math.max(now - sources.coinbase.sourceEventTs, 0),
          clob: Math.max(now - sources.clob.sourceEventTs, 0)
        },
        serverIngressLatency: {
          binance: Math.max(sources.binance.serverRecvTs - sources.binance.sourceEventTs, 0),
          coinbase: Math.max(sources.coinbase.serverRecvTs - sources.coinbase.sourceEventTs, 0),
          clob: Math.max(sources.clob.serverRecvTs - sources.clob.sourceEventTs, 0)
        },
        serverComputeLatency: Math.max(Date.now() - now, 0)
      };

    const officialPriceToBeat =
      currentRound && isBtcReferencePrice(currentRound.priceToBeat) && isOfficialPtbSource(currentRound.priceToBeatSource)
        ? roundNumber(currentRound.priceToBeat, 2)
        : undefined;
    const fallbackDisplayPriceToBeat =
      !officialPriceToBeat && currentRound && isBtcReferencePrice(currentRoundBinanceOpenReference)
        ? roundNumber(currentRoundBinanceOpenReference, 2)
        : undefined;

    return {
      symbol: this.config.symbol,
      marketId: currentRound?.marketId ?? matchedMarket?.id ?? this.config.marketId,
      marketSlug: currentRound?.marketSlug ?? matchedMarket?.slug,
      eventId: currentRound?.eventId ?? matchedMarket?.eventId,
      eventSlug: currentRound?.eventSlug ?? matchedMarket?.eventSlug,
      conditionId: currentRound?.conditionId ?? matchedMarket?.conditionId,
      seriesSlug: currentRound?.seriesSlug ?? matchedMarket?.seriesSlug,
      serverNow: now,
      binancePrice,
      coinbasePrice,
      currentPrice: binancePrice || coinbasePrice,
      priceToBeat: officialPriceToBeat ?? 0,
      displayPriceToBeat: officialPriceToBeat ?? fallbackDisplayPriceToBeat,
      displayPriceToBeatSource: officialPriceToBeat
        ? "official"
        : fallbackDisplayPriceToBeat
          ? "binance_open_fallback"
          : undefined,
      upPrice: roundNumber(upPrice, 4),
      downPrice: roundNumber(downPrice, 4),
      displayPrices: {
        UP: upDisplayPrice.value,
        DOWN: downDisplayPrice.value
      },
      displayPriceSource: {
        UP: upDisplayPrice.source,
        DOWN: downDisplayPrice.source
      },
      displayPriceSpread: {
        UP: upDisplayPrice.spread,
        DOWN: downDisplayPrice.spread
      },
      latencyBreakdown,
      sources,
      orderBooks: {
        UP: upBook,
        DOWN: downBook
      },
      recentTrades,
      candles: [...this.binanceState.candles],
      binance: {
        spotPrice: binancePrice,
        latestTick: this.binanceState.latestTick,
        candlesByInterval
      },
      coinbase: {
        referencePrice: coinbasePrice,
        settlementReference: currentRound?.settlementPrice ?? coinbasePrice,
        currentRoundOpenReference: currentRoundCoinbaseOpenReference,
        candles5s: [...this.coinbaseCandles5s],
        candlesByInterval: {
          "30s": [...this.coinbaseCandlesByInterval["30s"]],
          "1m": [...this.coinbaseCandlesByInterval["1m"]],
          "5m": [...this.coinbaseCandlesByInterval["5m"]],
          "15m": [...this.coinbaseCandlesByInterval["15m"]],
          "1h": [...this.coinbaseCandlesByInterval["1h"]],
          "1d": []
        }
      },
        clob: {
          delta: clobDelta,
          volume: clobVolume,
          upBook,
          downBook,
          recentTrades,
          currentRoundUpPriceSeries,
          marketInfo,
          bestBidAskSummary: {
          UP: {
            bestBid: upBook.bestBid,
            bestAsk: upBook.bestAsk
          },
          DOWN: {
            bestBid: downBook.bestBid,
            bestAsk: downBook.bestAsk
          }
        }
      },
      uiMeta: {
        marketTitle,
        marketSubtitle: currentRound ? utcRangeText(currentRound.startAt, currentRound.endAt) : matchedMarket?.slug,
        countdownMs,
        countdownTargetTs,
        acceptingOrders:
          this.canCreateNewOrders(currentRound, now) &&
          (matchedMarket?.acceptingOrders ?? currentRound?.acceptingOrders ?? false),
        marketSwitchState: this.getMarketSwitchState(currentRound, matchedMarket, now),
        sourceStatusSummary: [
          { source: "Binance", state: this.binanceState.status.state },
          { source: "Coinbase", state: this.coinbaseState.status.state },
          { source: "CLOB", state: this.polymarketState.status.state }
        ]
      }
    };
  }

  private getMarketSwitchState(
    currentRound: RoundRecord | undefined,
    matchedMarket: PolymarketMarketDetail | undefined,
    now: number
  ) {
    if (!currentRound) {
      return "market_not_ready" as const;
    }
    const nextMarket = this.polymarketState.nextMarket;
    if (currentRound.startAt > now) {
      return this.roundMatchesMarket(currentRound, this.polymarketState.currentMarket) ||
        this.roundMatchesMarket(currentRound, nextMarket)
        ? ("next_ready" as const)
        : ("prefetching_next" as const);
    }
    if (!matchedMarket) {
      return "prefetching_next" as const;
    }
    return nextMarket && nextMarket.startAt === currentRound.endAt ? ("next_ready" as const) : ("active" as const);
  }

  private refreshOpenPositions(snapshot: MarketSnapshot) {
    const changedUsers = new Set<string>();
    const activeRound = this.getActiveRound(snapshot.serverNow);
    for (const position of this.store.positions) {
      if (position.status !== "open") {
        continue;
      }
      if (!activeRound || position.roundId !== activeRound.id) {
        continue;
      }
      const nextMark = position.side === "UP" ? snapshot.upPrice : snapshot.downPrice;
      if (nextMark <= 0) {
        continue;
      }
      const mark = roundNumber(nextMark, 4);
      const unrealizedPnl = roundNumber(position.qty * mark - position.notionalSpent, 2);
      const book = snapshot.orderBooks[position.side];
      const currentValue = roundNumber(position.qty * mark, 2);
      const executablePnl = roundNumber(((book.bestBid || mark) * position.qty) - position.notionalSpent, 2);
      if (
        position.currentMark !== mark ||
        position.unrealizedPnl !== unrealizedPnl ||
        position.currentValue !== currentValue ||
        position.currentBid !== book.bestBid ||
        position.currentAsk !== book.bestAsk ||
        position.executablePnlUsdc !== executablePnl
      ) {
        position.currentMark = mark;
        position.currentBid = book.bestBid;
        position.currentAsk = book.bestAsk;
        position.currentMid = book.midPrice;
        position.currentValue = currentValue;
        position.sourceLatencyMs = Math.max(snapshot.serverNow - book.snapshotTs, 0);
        position.unrealizedPnl = unrealizedPnl;
        position.costBasisUsdc = position.notionalSpent;
        position.markPnlUsdc = unrealizedPnl;
        position.executablePnlUsdc = executablePnl;
        changedUsers.add(position.userId);
      }
    }
    return changedUsers;
  }

  private normalizeSourceHealth(source: SourceHealth, publishedAt: number): SourceHealth {
    if (source.state === "disabled") {
      return {
        ...source,
        normalizedTs: publishedAt,
        serverPublishTs: publishedAt,
        acquireLatencyMs: 0,
        publishLatencyMs: 0,
        frontendLatencyMs: 0
      };
    }
    return {
      ...source,
      normalizedTs: source.normalizedTs || publishedAt,
      serverPublishTs: publishedAt,
      publishLatencyMs: Math.max(publishedAt - (source.normalizedTs || source.serverRecvTs), 0)
    };
  }

  private computeRoundStatus(round: RoundRecord, now: number): RoundStatus {
    if (round.status === "Closed" || round.status === "Manual") {
      return round.status;
    }
    if (round.redeemFinishTs) {
      return "Closed";
    }
    if (round.redeemStartTs) {
      return "Redeeming";
    }
    if (round.settledSide) {
      return "Settled";
    }

    const freezeStart = Math.max(round.endAt - this.config.freezeWindowMs, round.startAt);
    if (now < round.startAt) {
      return round.acceptingOrders === false ? "Frozen" : "Trading";
    }
    if (now < freezeStart && round.acceptingOrders !== false) {
      return "Trading";
    }
    if (now < round.endAt) {
      return "Frozen";
    }
    if (now < round.endAt + this.config.pollDelayMs) {
      return "Settling";
    }
    return "Polling";
  }

  private async pollSettlement(round: RoundRecord, now: number) {
    if (round.settledSide || round.redeemFinishTs || round.status === "Closed" || round.status === "Manual") {
      this.gammaOutcomeConfirmations.delete(round.id);
      return;
    }
    if ((!round.marketSlug && !round.marketId) || this.pollLocks.has(round.id)) {
      return;
    }
    const pollIntervalMs = this.gammaPollIntervalFor(round, now);
    const formalPollJustOpened = now >= round.endAt && Boolean(round.lastPollAt && round.lastPollAt < round.endAt);
    if (!formalPollJustOpened && round.lastPollAt && now - round.lastPollAt < pollIntervalMs) {
      return;
    }
    this.pollLocks.add(round.id);
    round.pollCount += 1;
    round.lastPollAt = now;
    round.pollStartAt = round.pollStartAt ?? now;
    this.gammaSettlementDiagnostics ??= new Set<string>();
    if (now >= round.endAt && !this.gammaSettlementDiagnostics.has(`${round.id}:formal_poll_started`)) {
      this.gammaSettlementDiagnostics.add(`${round.id}:formal_poll_started`);
      void this.writeSettlementDiagnostic(round, "GAMMA_FORMAL_POLL_STARTED", "Gamma formal settlement polling started.");
    }

    try {
      const detail = await this.fetchBestGammaSettlementDetail(round, now);
      if (!detail) {
        return;
      }
      this.applyMarketMetadata(round, detail);
      const settlement = this.resolveTrustedGammaSettlement(round, detail, now);
      if (settlement) {
        const confirmed = this.confirmGammaSettlement(round, detail, settlement.side, settlement.price, now);
        if (confirmed) {
          await this.writeSettlementLog(round, "success", settlement.message);
        }
      }
      // If not yet settled, keep polling indefinitely until result comes in.
    } catch (error) {
      await this.writeAuditLog({
        eventId: this.store.newId("evt"),
        traceId: this.store.newTraceId(),
        category: "settlement",
        actionType: "poll_settlement",
        actionStatus: "failed",
        pageName: "trade.main",
        moduleName: "settlement.engine",
        symbol: round.symbol,
        roundId: round.id,
        serverRecvTs: now,
        serverPublishTs: now,
        backendLatencyMs: 0,
        resultCode: "POLL_FAILED",
        resultMessage: error instanceof Error ? error.message : "Gamma market lookup failed.",
        details: {
          roundId: round.id,
          marketId: round.marketId,
          marketSlug: round.marketSlug,
          pollCount: round.pollCount,
          failureReason: error instanceof Error ? error.message : "Gamma market lookup failed."
        }
      });
    } finally {
      this.pollLocks.delete(round.id);
    }
  }

  private async fetchBestGammaSettlementDetail(round: RoundRecord, now: number) {
    const tasks: Array<Promise<PolymarketMarketDetail>> = [];
    const seen = new Set<string>();
    const marketId = String(round.marketId ?? "").trim();
    if (marketId) {
      seen.add(`id:${marketId}`);
      tasks.push(this.polymarketConnector.fetchMarketById(marketId));
    }
    const marketSlug = String(round.marketSlug ?? "").trim();
    if (marketSlug && !seen.has(`slug:${marketSlug}`)) {
      tasks.push(this.polymarketConnector.fetchMarketBySlug(marketSlug));
    }
    if (!tasks.length) {
      return undefined;
    }
    const settled = await Promise.allSettled(tasks);
    const details = settled
      .filter((result): result is PromiseFulfilledResult<PolymarketMarketDetail> => result.status === "fulfilled")
      .map((result) => result.value);
    if (!details.length) {
      const firstError = settled.find((result): result is PromiseRejectedResult => result.status === "rejected");
      if (firstError?.reason) {
        throw firstError.reason;
      }
      return undefined;
    }
    const winnerDetail = details.find((detail) => this.resolveWinnerSettledSide(detail));
    if (winnerDetail) {
      await this.writeGammaObservationOnce(round, "winner", "Gamma winner field was observed.");
      return winnerDetail;
    }
    const exactDetail = details.find((detail) => this.resolveExactOutcomeSettledSide(detail));
    if (exactDetail && now >= round.endAt) {
      await this.writeGammaObservationOnce(round, "exact_outcome", "Gamma exact 1/0 outcome prices were observed.");
      return exactDetail;
    }
    return details[0];
  }

  private confirmGammaSettlement(
    round: RoundRecord,
    detail: PolymarketMarketDetail | undefined,
    settledSide: TradeSide | undefined,
    settlementPrice: number,
    now: number
  ) {
    if (!settledSide || !detail || !Number.isFinite(settlementPrice)) {
      return false;
    }
    this.gammaOutcomeConfirmations ??= new Map<string, { side: TradeSide; count: number; observedAt: number }>();
    if (round.redeemFinishTs || round.status === "Closed") {
      this.gammaOutcomeConfirmations.delete(round.id);
      void this.writeSettlementDiagnostic(round, "SETTLEMENT_ALREADY_CLOSED", "Gamma settlement confirmation skipped because the round is already closed.");
      return false;
    }
    if (round.settledSide) {
      this.gammaOutcomeConfirmations.delete(round.id);
      if (round.settledSide !== settledSide) {
        void this.writeSettlementDiagnostic(round, "SETTLEMENT_CONFLICT_SKIPPED", `Gamma settlement conflict skipped: existing ${round.settledSide}, incoming ${settledSide}.`);
      } else {
        void this.writeSettlementDiagnostic(round, "SETTLEMENT_DUPLICATE_SKIPPED", "Duplicate Gamma settlement confirmation skipped.");
      }
      return false;
    }
    if (!round.closingSpotPrice && this.binanceState.price > 0) {
      round.closingSpotPrice = roundNumber(this.binanceState.price, 2);
      round.closingPriceSource = "Gamma";
    }
    round.settledSide = settledSide;
    round.polymarketSettlementPrice = settlementPrice;
    round.polymarketSettlementStatus = detail.settlementStatus ?? "resolved";
    round.settlementPrice = round.polymarketSettlementPrice;
    round.settlementTs = now;
    round.settlementReceivedAt = round.settlementReceivedAt ?? now;
    round.settlementSource = "Gamma";
    round.status = "Settled";
    round.acceptingOrders = false;
    round.manualReason = undefined;
    this.getPreliminarySettlements().delete(round.id);
    this.applyMarketMetadata(round, detail);
    this.gammaOutcomeConfirmations.delete(round.id);
    this.scheduleRedeem(round, now);
    return true;
  }

  private confirmSettlementFromResolved(
    round: RoundRecord,
    resolved: NonNullable<PolymarketConnectorState["lastResolvedMarket"]>
  ) {
    if (!resolved.settledSide) {
      return false;
    }
    this.gammaOutcomeConfirmations ??= new Map<string, { side: TradeSide; count: number; observedAt: number }>();
    const receivedAt = resolved.receivedAt;
    if (round.redeemFinishTs || round.status === "Closed") {
      void this.writeSettlementDiagnostic(round, "SETTLEMENT_ALREADY_CLOSED", "Polymarket resolved event skipped because the round is already closed.");
      return false;
    }
    if (round.settledSide) {
      if (round.settledSide !== resolved.settledSide) {
        void this.writeSettlementDiagnostic(round, "SETTLEMENT_CONFLICT_SKIPPED", `Polymarket resolved event conflict skipped: existing ${round.settledSide}, incoming ${resolved.settledSide}.`);
      } else {
        void this.writeSettlementDiagnostic(round, "SETTLEMENT_DUPLICATE_SKIPPED", "Duplicate Polymarket resolved event skipped.");
      }
      return false;
    }
    if (!round.closingSpotPrice && this.binanceState.price > 0) {
      round.closingSpotPrice = roundNumber(this.binanceState.price, 2);
      round.closingPriceSource = "Gamma";
    }
    round.settledSide = resolved.settledSide;
    round.polymarketSettlementPrice =
      resolved.settlementPrice ?? (resolved.settledSide === "UP" ? 1 : 0);
    round.polymarketSettlementStatus = "resolved";
    round.settlementPrice = round.polymarketSettlementPrice;
    round.settlementTs = receivedAt;
    round.settlementReceivedAt = receivedAt;
    round.settlementSource = "Polymarket";
    round.status = "Settled";
    round.acceptingOrders = false;
    round.manualReason = undefined;
    round.lastPollAt = undefined;
    this.getPreliminarySettlements().delete(round.id);
    this.gammaOutcomeConfirmations.delete(round.id);
    this.scheduleRedeem(round, receivedAt);
    return true;
  }

  private scheduleRedeem(round: RoundRecord, now: number) {
    if (!round.settledSide || round.redeemFinishTs || round.status === "Closed") {
      return;
    }
    const start = round.redeemStartTs ?? round.settlementReceivedAt ?? round.settlementTs ?? now;
    round.redeemStartTs = start;
    round.redeemScheduledAt = round.redeemScheduledAt ?? start + REDEEM_DELAY_MS;
    round.status = "Redeeming";
  }

  private async applyRedeem(round: RoundRecord) {
    this.redeemLocks ??= new Set<string>();
    if (!round.settledSide || round.redeemFinishTs || this.redeemLocks.has(round.id)) {
      return;
    }
    const settledSide = round.settledSide;
    this.redeemLocks.add(round.id);
    const originalRound = { ...round };
    const originalPositions = new Map<string, PositionRecord>();
    const originalUsers = new Map<string, UserRecord>();
    try {
      const closedAt = Date.now();
      const userIds = new Set<string>();
      let redeemedPositionCount = 0;
      const positions = this.store.positions.filter(
        (position) => position.roundId === round.id && position.status === "open"
      );

      await this.store.withTransaction(async () => {
        for (const position of positions) {
          const user = this.store.getUserById(position.userId);
          if (!user) {
            continue;
          }
          originalPositions.set(position.id, { ...position });
          originalUsers.set(user.id, { ...user });
          const snapshot = this.captureActionSnapshot();
          const isWinner = position.side === settledSide;
          const redeemAmount = isWinner ? position.qty : 0;
          const realizedPnl = redeemAmount - position.notionalSpent;
          const settlementResult = isWinner ? "win" : "loss";
          const claimed = await this.store.claimRedeemLedger({
            roundId: round.id,
            userId: user.id,
            positionId: position.id,
            redeemAmountUsdc: redeemAmount,
            realizedPnlUsdc: roundNumber(realizedPnl, 2),
            settlementResult,
            createdAtMs: closedAt,
            details: {
              side: position.side,
              settledSide,
              marketId: round.marketId,
              marketSlug: round.marketSlug
            }
          });
          if (!claimed) {
            continue;
          }

          user.availableUsdc = roundNumber(user.availableUsdc + redeemAmount, 2);
          position.realizedPnl = roundNumber(position.realizedPnl + realizedPnl, 2);
          position.unrealizedPnl = 0;
          position.costBasisUsdc = 0;
          position.markPnlUsdc = 0;
          position.executablePnlUsdc = 0;
          position.status = "closed";
          position.closedAt = closedAt;
          position.currentMark = isWinner ? 1 : 0;
          position.currentBid = undefined;
          position.currentAsk = undefined;
          position.currentMid = undefined;
          position.sourceLatencyMs = undefined;
          position.lockedQty = 0;
          position.currentValue = redeemAmount;
          position.settlementResult = settlementResult;
          await this.store.persistUser(user);
          await this.store.persistPosition(position);
          await this.store.settleOpenOrderLifecycles({
            userId: user.id,
            roundId: round.id,
            side: position.side,
            settlementResult: position.settlementResult,
            settlementDirection: settledSide,
            settlementTimeMs: round.settlementTs ?? closedAt,
            exitTokenPrice: isWinner ? 1 : 0
          });
          await this.writeAuditLog({
            eventId: this.store.newId("evt"),
            traceId: this.store.newTraceId(),
            category: "settlement",
            actionType: "redeem_position",
            actionStatus: "success",
            userId: user.id,
            role: user.role,
            pageName: "profile.main",
            moduleName: "position.table",
            symbol: round.symbol,
            roundId: round.id,
            serverRecvTs: closedAt,
            serverPublishTs: closedAt,
            backendLatencyMs: 0,
            resultCode: "POSITION_SETTLED",
            resultMessage: `Position ${position.id} settled as ${position.settlementResult}.`,
            details: {
              roundId: round.id,
              marketId: round.marketId,
              marketSlug: round.marketSlug,
              positionId: position.id,
              side: position.side,
              settlementResult: position.settlementResult,
              redeemAmount,
              realizedPnl: position.realizedPnl
            }
          });
          await this.writeBehaviorLog(
            this.createBehaviorLog({
              user,
              actionType: "redeem_position",
              actionStatus: "success",
              traceId: this.store.newTraceId(),
              round,
              snapshot,
              direction: position.side,
              entryOdds: position.averageEntry,
              positionNotional: position.notionalSpent,
              exitType: "settlement",
              exitOdds: position.currentMark,
              settlementResult: position.settlementResult,
              settlementDirection: settledSide,
              settlementTimeMs: round.settlementTs ?? closedAt,
              gammaPollCount: round.pollCount,
              redeemFinishTimeMs: closedAt,
              contextJson: {
                positionId: position.id,
                roundId: round.id,
                marketId: round.marketId,
                marketSlug: round.marketSlug,
                redeemAmount
              }
            })
          );
          redeemedPositionCount += 1;
          userIds.add(user.id);
        }

        round.redeemFinishTs = closedAt;
        round.redeemScheduledAt = round.redeemScheduledAt ?? closedAt;
        round.status = "Closed";
        await this.store.upsertRound(round);
        await this.writeAuditLog({
          eventId: this.store.newId("evt"),
          traceId: this.store.newTraceId(),
          category: "settlement",
          actionType: "round_closed",
          actionStatus: "success",
          pageName: "trade.main",
          moduleName: "settlement.engine",
          symbol: round.symbol,
          roundId: round.id,
          serverRecvTs: closedAt,
          serverPublishTs: closedAt,
          backendLatencyMs: 0,
          resultCode: "ROUND_CLOSED",
          resultMessage: "Round was closed after redeem processing completed.",
          details: {
            roundId: round.id,
            marketId: round.marketId,
            marketSlug: round.marketSlug,
            settledSide,
            settlementPrice: round.settlementPrice,
            redeemFinishTs: round.redeemFinishTs,
            redeemedPositionCount
          }
        });
      });

      for (const userId of userIds) {
        this.store.emitUserPayload(userId);
      }
      await this.publishSettlementMarketSnapshot(round, "redeem_completed");
    } catch (error) {
      Object.assign(round, originalRound);
      for (const [positionId, original] of originalPositions) {
        const position = this.store.positions.find((item) => item.id === positionId);
        if (position) {
          Object.assign(position, original);
        }
      }
      for (const [userId, original] of originalUsers) {
        const user = this.store.getUserById(userId);
        if (user) {
          Object.assign(user, original);
        }
      }
      throw error;
    } finally {
      this.redeemLocks.delete(round.id);
    }
  }

  private upsertBuyPosition(
    buyOrderId: string,
    userId: string,
    roundId: string,
    side: TradeSide,
    filledQty: number,
    spent: number,
    mark: number,
    entryFee = 0
  ) {
    const position: PositionRecord = {
      id: this.store.newId("pos"),
      buyOrderId,
      userId,
      roundId,
      side,
      qty: 0,
      lockedQty: 0,
      averageEntry: 0,
      notionalSpent: 0,
      currentMark: mark,
      unrealizedPnl: 0,
      realizedPnl: 0,
      entryFeeUsdc: 0,
      exitFeeUsdc: 0,
      totalFeeUsdc: 0,
      costBasisUsdc: 0,
      markPnlUsdc: 0,
      executablePnlUsdc: 0,
      status: "open",
      openedAt: Date.now()
    };

    const totalCost = spent;
    const totalQty = filledQty;
    position.averageEntry = roundNumber(totalCost / Math.max(totalQty, QTY_EPSILON), 4);
    position.qty = roundNumber(totalQty, 4);
    position.notionalSpent = roundNumber(totalCost, 4);
    position.entryFeeUsdc = roundNumber((position.entryFeeUsdc ?? 0) + entryFee, 8);
    position.totalFeeUsdc = roundNumber((position.entryFeeUsdc ?? 0) + (position.exitFeeUsdc ?? 0), 8);
    position.costBasisUsdc = position.notionalSpent;
    position.currentMark = roundNumber(mark, 4);
    position.currentValue = roundNumber(position.qty * position.currentMark, 2);
    position.unrealizedPnl = roundNumber(position.qty * position.currentMark - position.notionalSpent, 2);
    position.markPnlUsdc = position.unrealizedPnl;
    position.executablePnlUsdc = roundNumber(((position.currentBid ?? position.currentMark) * position.qty) - position.notionalSpent, 2);
    return position;
  }

  private resolveWinnerSettledSide(detail: PolymarketMarketDetail): TradeSide | undefined {
    if (detail.winningTokenId) {
      if (detail.winningTokenId === detail.upTokenId) return "UP";
      if (detail.winningTokenId === detail.downTokenId) return "DOWN";
    }
    if (detail.winningOutcome) {
      const normalized = detail.winningOutcome.toLowerCase();
      if (normalized === detail.upOutcome.toLowerCase() || normalized.includes("up") || normalized.includes("above")) return "UP";
      if (normalized === detail.downOutcome.toLowerCase() || normalized.includes("down") || normalized.includes("below")) return "DOWN";
    }
    return undefined;
  }

  private resolveExactOutcomeSettledSide(detail: PolymarketMarketDetail): TradeSide | undefined {
    const [upPrice, downPrice] = detail.outcomePrices;
    if (upPrice >= GAMMA_SETTLED_WIN_PRICE_THRESHOLD && downPrice <= GAMMA_SETTLED_LOSE_PRICE_THRESHOLD) return "UP";
    if (downPrice >= GAMMA_SETTLED_WIN_PRICE_THRESHOLD && upPrice <= GAMMA_SETTLED_LOSE_PRICE_THRESHOLD) return "DOWN";
    return undefined;
  }

  private settlementPriceForSide(detail: PolymarketMarketDetail, side: TradeSide) {
    return typeof detail.settlementPrice === "number" && Number.isFinite(detail.settlementPrice)
      ? detail.settlementPrice
      : side === "UP"
        ? 1
        : 0;
  }

  private confirmExactGammaOutcome(roundId: string, side: TradeSide, now: number) {
    this.gammaOutcomeConfirmations ??= new Map<string, { side: TradeSide; count: number; observedAt: number }>();
    const previous = this.gammaOutcomeConfirmations.get(roundId);
    if (!previous || previous.side !== side || now - previous.observedAt > 10_000) {
      this.gammaOutcomeConfirmations.set(roundId, { side, count: 1, observedAt: now });
      return false;
    }
    const next = { side, count: previous.count + 1, observedAt: now };
    this.gammaOutcomeConfirmations.set(roundId, next);
    return next.count >= 2;
  }

  private resolveTrustedGammaSettlement(
    round: RoundRecord,
    detail: PolymarketMarketDetail,
    now: number
  ): { side: TradeSide; price: number; message: string } | undefined {
    if (now < round.endAt) {
      return undefined;
    }

    const winnerSide = this.resolveWinnerSettledSide(detail);
    if (winnerSide) {
      this.gammaOutcomeConfirmations.delete(round.id);
      return {
        side: winnerSide,
        price: this.settlementPriceForSide(detail, winnerSide),
        message: "Gamma winner field confirmed settlement."
      };
    }

    const exactOutcomeSide = this.resolveExactOutcomeSettledSide(detail);
    if (!exactOutcomeSide) {
      this.gammaOutcomeConfirmations.delete(round.id);
      return undefined;
    }

    if (detail.closed || detail.settlementStatus === "resolved" || detail.automaticallyResolved) {
      this.gammaOutcomeConfirmations.delete(round.id);
      return {
        side: exactOutcomeSide,
        price: this.settlementPriceForSide(detail, exactOutcomeSide),
        message: "Gamma resolved market detail confirmed settlement."
      };
    }

    if (!this.confirmExactGammaOutcome(round.id, exactOutcomeSide, now)) {
      return undefined;
    }
    return {
      side: exactOutcomeSide,
      price: this.settlementPriceForSide(detail, exactOutcomeSide),
      message: "Gamma exact outcome prices confirmed settlement on consecutive polls."
    };
  }

  private refreshPreliminarySettlement(round: RoundRecord, now: number) {
    if (round.settledSide || round.status === "Closed" || round.status === "Manual" || now < round.endAt) {
      if (round.settledSide || round.status === "Closed" || round.status === "Manual") {
        this.getPreliminarySettlements().delete(round.id);
      }
      return;
    }
    const preview = this.createPreliminarySettlementPreview(round, now);
    if (preview) {
      this.getPreliminarySettlements().set(round.id, preview);
    }
  }

  private createPreliminarySettlementPreview(round: RoundRecord, now: number): SettlementPreview | undefined {
    if (round.settledSide || round.status === "Closed" || round.status === "Manual" || now < round.endAt) {
      return undefined;
    }
    const prices = this.pricesForPreliminarySettlement(round);
    const binancePrice = this.preliminaryBinancePrice(round);
    const binanceSide = this.preliminaryBinanceSide(round, binancePrice);
    if (!prices && !binanceSide) {
      return undefined;
    }
    const tokenSide = prices
      ? prices.upPrice > PRELIMINARY_SETTLEMENT_THRESHOLD
        ? "UP"
        : prices.downPrice > PRELIMINARY_SETTLEMENT_THRESHOLD
          ? "DOWN"
          : undefined
      : undefined;
    if (!tokenSide && !binanceSide) {
      return undefined;
    }
    const side = tokenSide === binanceSide ? tokenSide : tokenSide ?? binanceSide;
    if (!side) return undefined;
    const price = prices ? (side === "UP" ? prices.upPrice : prices.downPrice) : undefined;
    const confidence =
      tokenSide && binanceSide
        ? tokenSide === binanceSide
          ? "aligned"
          : "conflict"
        : tokenSide
          ? "token_only"
          : "binance_only";
    return {
      roundId: round.id,
      state: "preliminary",
      side,
      price,
      source: "CLOB",
      detectedAt: now,
      upPrice: prices?.upPrice,
      downPrice: prices?.downPrice,
      tokenSide,
      binanceSide,
      binancePrice,
      priceToBeat: round.priceToBeat,
      confidence,
      conflictReason:
        confidence === "conflict"
          ? `Token indicates ${tokenSide}; Binance close ${binancePrice ?? "--"} vs PTB ${round.priceToBeat} indicates ${binanceSide}.`
          : undefined,
      message:
        confidence === "conflict"
          ? "Preliminary conflict between CLOB token price and Binance/PTB direction."
          : `Preliminary ${side} from ${confidence === "aligned" ? "CLOB token price and Binance/PTB" : tokenSide ? "CLOB token price" : "Binance/PTB"}.`
    };
  }

  private preliminaryBinancePrice(round: RoundRecord) {
    const price = round.closingSpotPrice ?? round.binanceClosePrice ?? this.binanceState.price;
    return isBtcReferencePrice(price) ? roundNumber(price, 2) : undefined;
  }

  private preliminaryBinanceSide(round: RoundRecord, price?: number): TradeSide | undefined {
    if (!isBtcReferencePrice(price) || !isBtcReferencePrice(round.priceToBeat)) {
      return undefined;
    }
    return price >= round.priceToBeat ? "UP" : "DOWN";
  }

  private pricesForPreliminarySettlement(round: RoundRecord) {
    if (this.roundMatchesMarket(round, this.polymarketState.currentMarket)) {
      const upBook = this.getDisplayedBook("UP");
      const downBook = this.getDisplayedBook("DOWN");
      return {
        upPrice: roundNumber(upBook.bestAsk || upBook.midPrice || this.polymarketState.currentMarket?.outcomePrices[0] || 0, 4),
        downPrice: roundNumber(downBook.bestAsk || downBook.midPrice || this.polymarketState.currentMarket?.outcomePrices[1] || 0, 4)
      };
    }
    if (this.store.marketSnapshot.marketSlug === round.marketSlug || this.store.marketSnapshot.marketId === round.marketId) {
      return {
        upPrice: roundNumber(this.store.marketSnapshot.upPrice, 4),
        downPrice: roundNumber(this.store.marketSnapshot.downPrice, 4)
      };
    }
    return undefined;
  }

  private applyMarketMetadata(round: RoundRecord, detail: PolymarketMarketDetail) {
    round.marketId = detail.id;
    round.eventId = detail.eventId;
    round.marketSlug = detail.slug;
    round.eventSlug = detail.eventSlug;
    round.conditionId = detail.conditionId;
    round.seriesSlug = detail.seriesSlug;
    round.upTokenId = detail.upTokenId;
    round.downTokenId = detail.downTokenId;
    round.title = detail.title;
    round.resolutionSource = detail.resolutionSource;
    round.acceptingOrders = detail.acceptingOrders;
    round.polymarketSettlementPrice = detail.settlementPrice ?? round.polymarketSettlementPrice;
    round.polymarketSettlementStatus = detail.settlementStatus ?? round.polymarketSettlementStatus;
    round.settlementReceivedAt = detail.settlementReceivedAt ?? round.settlementReceivedAt;
    if (!isBtcReferencePrice(round.polymarketOpenPrice)) {
      round.polymarketOpenPrice = undefined;
      round.polymarketOpenPriceSource = undefined;
    }
    if (!isBtcReferencePrice(round.polymarketClosePrice)) {
      round.polymarketClosePrice = undefined;
      round.polymarketClosePriceSource = undefined;
    }
  }

  private async syncCurrentRoundMarket() {
    const currentRound = this.store.getCurrentRound(Date.now());
    if (!currentRound?.marketSlug) {
      return;
    }
    if (this.roundMatchesMarket(currentRound, this.polymarketState.currentMarket)) {
      return;
    }
    if (this.marketSyncSlug === currentRound.marketSlug) {
      return;
    }

    this.marketSyncSlug = currentRound.marketSlug;
    try {
      await this.polymarketConnector.focusMarket(currentRound);
      this.polymarketState = this.polymarketConnector.getState();
    } catch {
      // Keep the previous market snapshot until the next reconcile retries the switch.
    } finally {
      if (this.marketSyncSlug === currentRound.marketSlug) {
        this.marketSyncSlug = undefined;
      }
    }
  }

  private findResolvedMarketForRound(round: Pick<RoundRecord, "marketId" | "marketSlug" | "conditionId">) {
    const candidates = [
      ...(this.polymarketState.resolvedMarkets ?? []),
      ...(this.polymarketState.lastResolvedMarket ? [this.polymarketState.lastResolvedMarket] : [])
    ].sort((left, right) => right.receivedAt - left.receivedAt);
    return candidates.find((resolved) => this.roundMatchesResolvedMarket(round, resolved));
  }

  private roundMatchesResolvedMarket(
    round?: Pick<RoundRecord, "marketId" | "marketSlug" | "conditionId">,
    resolved?: PolymarketResolvedMarket
  ) {
    if (!round || !resolved) {
      return false;
    }
    return Boolean(
      (round.marketSlug && resolved.marketSlug === round.marketSlug) ||
        (round.marketId && resolved.marketId === round.marketId) ||
        (round.conditionId && resolved.conditionId === round.conditionId)
    );
  }

  private roundMatchesMarket(round?: Pick<RoundRecord, "marketId" | "marketSlug" | "conditionId">, market?: Pick<PolymarketMarketDetail, "id" | "slug" | "conditionId">) {
    if (!round || !market) {
      return false;
    }
    return Boolean(
      (round.marketSlug && market.slug === round.marketSlug) ||
        (round.marketId && market.id === round.marketId) ||
        (round.conditionId && market.conditionId === round.conditionId)
    );
  }

  private createEmptyOrderBook(side: TradeSide): OrderBookSnapshot {
    const now = Date.now();
    return {
      snapshotId: `empty_${side}_${now}`,
      snapshotTs: now,
      bestBid: 0,
      bestAsk: 0,
      midPrice: 0,
      bids: [],
      asks: []
    };
  }

  private syncPriceToBeatFromPolymarketOpenPrice(round: RoundRecord, now: number) {
    if (!isBtcReferencePrice(round.polymarketOpenPrice) || !isOfficialPtbSource(round.polymarketOpenPriceSource)) {
      if (round.priceToBeat || round.priceToBeatSource || round.priceToBeatCapturedAt) {
        round.priceToBeat = 0;
        round.priceToBeatSource = undefined;
        round.priceToBeatCapturedAt = undefined;
      }
      return;
    }
    const officialPriceToBeat = roundNumber(round.polymarketOpenPrice, 2);
    const isSamePrice =
      isBtcReferencePrice(round.priceToBeat) && Math.abs(round.priceToBeat - officialPriceToBeat) < 0.005;
    if (isSamePrice && round.priceToBeatSource && round.priceToBeatCapturedAt) {
      return;
    }
    round.priceToBeat = officialPriceToBeat;
    round.priceToBeatSource = round.polymarketOpenPriceSource ?? "Gamma";
    round.priceToBeatCapturedAt = now;
  }

  private collectRoundPositionUsers(roundId: string) {
    return new Set(
      this.store.positions
        .filter((position) => position.roundId === roundId)
        .map((position) => position.userId)
    );
  }

  private roundSignature(round: RoundRecord) {
    return JSON.stringify([
      round.id,
      round.marketId,
      round.symbol,
      round.eventId,
      round.marketSlug,
      round.eventSlug,
      round.conditionId,
      round.seriesSlug,
      round.upTokenId,
      round.downTokenId,
      round.title,
      round.resolutionSource,
      round.startAt,
      round.endAt,
      round.priceToBeat,
      round.priceToBeatSource,
      round.priceToBeatCapturedAt,
      round.status,
      round.pollCount,
      round.pollStartAt,
      round.lastPollAt,
      round.closingSpotPrice,
      round.settledSide,
      round.settlementPrice,
      round.settlementTs,
      round.settlementSource,
      round.polymarketSettlementPrice,
      round.polymarketSettlementStatus,
      round.polymarketOpenPrice,
      round.polymarketClosePrice,
      round.polymarketOpenPriceSource,
      round.polymarketClosePriceSource,
      round.settlementReceivedAt,
      round.redeemScheduledAt,
      round.binanceOpenPrice,
      round.binanceClosePrice,
      round.coinbaseOpenPrice,
      round.coinbaseClosePrice,
      round.redeemStartTs,
      round.redeemFinishTs,
      round.manualReason,
      round.acceptingOrders,
      round.closingPriceSource
    ]);
  }

  private roundChanged(left: RoundRecord | undefined, right: RoundRecord) {
    return !left || this.roundSignature(left) !== this.roundSignature(right);
  }

  private async emitLatencyLogs(snapshot: MarketSnapshot) {
    const now = Date.now();
    for (const source of Object.values(snapshot.sources)) {
      const lastLoggedAt = this.lastLatencyLogAt.get(source.source) ?? 0;
      const lastState = this.lastLatencyState.get(source.source);
      if (now - lastLoggedAt < LATENCY_LOG_INTERVAL_MS && lastState === source.state) {
        continue;
      }
      this.lastLatencyLogAt.set(source.source, now);
      this.lastLatencyState.set(source.source, source.state);
      await this.writeAuditLog({
        eventId: this.store.newId("evt"),
        traceId: this.store.newTraceId(),
        category: "latency",
        actionType: "market_latency",
        actionStatus: "success",
        pageName: "trade.main",
        moduleName: source.source.toLowerCase(),
        symbol: snapshot.symbol,
        serverRecvTs: source.serverRecvTs,
        serverPublishTs: source.serverPublishTs,
        backendLatencyMs: source.acquireLatencyMs + source.publishLatencyMs,
        resultCode: "OK",
        resultMessage: `${source.source} ${source.state}`,
        details: {
          acquireLatencyMs: source.acquireLatencyMs,
          publishLatencyMs: source.publishLatencyMs,
          frontendLatencyMs: source.frontendLatencyMs,
          connectionState: source.state,
          reconnectCount: source.reconnectCount,
          message: source.message
        }
      });
    }
  }

  private async writeAuditLog(event: AuditEvent, options?: { emitUserPayload?: boolean }) {
    await this.store.recordLog(event, options);
  }

  private async publishSettlementMarketSnapshot(round: RoundRecord, reason: string) {
    if (typeof (this.store as unknown as { getCurrentRound?: unknown }).getCurrentRound !== "function") {
      return;
    }
    try {
      await this.store.setMarketSnapshot(this.buildSnapshot());
    } catch (error) {
      console.warn(`[simulation] Settlement market snapshot publish failed for ${round.id} (${reason}):`, error);
    }
  }

  private async writeSettlementDiagnostic(round: RoundRecord, code: string, message: string) {
    this.gammaSettlementDiagnostics ??= new Set<string>();
    const key = `${round.id}:${code}:${round.settledSide ?? "none"}:${round.status}`;
    if (this.gammaSettlementDiagnostics.has(key)) {
      return;
    }
    this.gammaSettlementDiagnostics.add(key);
    try {
      const now = Date.now();
      await this.writeAuditLog({
        eventId: this.store.newId("evt"),
        traceId: this.store.newTraceId(),
        category: "settlement",
        actionType: "poll_settlement",
        actionStatus: "success",
        pageName: "trade.main",
        moduleName: "settlement.engine",
        symbol: round.symbol,
        roundId: round.id,
        serverRecvTs: now,
        serverPublishTs: now,
        backendLatencyMs: 0,
        resultCode: code,
        resultMessage: message,
        details: {
          roundId: round.id,
          marketId: round.marketId,
          marketSlug: round.marketSlug,
          pollCount: round.pollCount,
          status: round.status,
          settledSide: round.settledSide
        }
      });
    } catch (error) {
      console.warn(`[simulation] Settlement diagnostic log failed for ${round.id}:`, error);
    }
  }

  private async writeGammaObservationOnce(round: RoundRecord, observation: string, message: string) {
    await this.writeSettlementDiagnostic(round, `GAMMA_${observation.toUpperCase()}_OBSERVED`, message);
  }

  private async writeSettlementLog(
    round: RoundRecord,
    status: "success" | "failed" | "timeout",
    message: string
  ) {
    const now = Date.now();
    await this.writeAuditLog({
      eventId: this.store.newId("evt"),
      traceId: this.store.newTraceId(),
      category: "settlement",
      actionType: status === "success" ? "settlement_confirmed" : "poll_settlement",
      actionStatus: status === "success" ? "success" : status === "timeout" ? "timeout" : "failed",
      pageName: "trade.main",
      moduleName: "settlement.engine",
      symbol: round.symbol,
      roundId: round.id,
      serverRecvTs: now,
      serverPublishTs: now,
      backendLatencyMs: 0,
      resultCode: status === "success" ? "SETTLED" : "MANUAL",
      resultMessage: message,
      details: {
        roundId: round.id,
        marketId: round.marketId,
        marketSlug: round.marketSlug,
        pollCount: round.pollCount,
        settlementPrice: round.settlementPrice,
        settledSide: round.settledSide,
        manualReason: round.manualReason
      }
    });
  }
}

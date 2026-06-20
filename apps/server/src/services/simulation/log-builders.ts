import type {
  BehaviorActionLog,
  FeeBreakdown,
  MarketSnapshot,
  OrderBookSnapshot,
  OrderRecord,
  PositionRecord,
  RoundRecord,
  SourceHealth,
  TradeSide,
  UserRecord
} from "../../domain/types";

export type BehaviorLogBuildInput = {
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
};

function pickSourceState(source: SourceHealth) {
  return {
    source: source.source,
    state: source.state,
    sourceEventTs: source.sourceEventTs,
    serverRecvTs: source.serverRecvTs,
    serverPublishTs: source.serverPublishTs
  };
}

export function buildBehaviorLog(input: BehaviorLogBuildInput & {
  logId: string;
  timestampMs: number;
  testerIdAnon: string;
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
    logId: input.logId,
    timestampMs: input.timestampMs,
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
    testerIdAnon: input.testerIdAnon,
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
      binance: pickSourceState(snapshot.sources.binance),
      coinbase: pickSourceState(snapshot.sources.coinbase),
      clob: pickSourceState(snapshot.sources.clob)
    },
    contextJson
  };
}

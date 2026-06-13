import type { OrderLifecycleRecord, OrderRecord } from "../domain/types";

export const USER_TRADE_ORDER_LIMIT = 12;
export const USER_TRADE_LIFECYCLE_LIMIT = 24;

export function toTradeOrderPayload(order: OrderRecord): OrderRecord {
  return {
    id: order.id,
    traceId: order.traceId,
    userId: order.userId,
    roundId: order.roundId,
    symbol: order.symbol,
    marketId: order.marketId,
    action: order.action,
    side: order.side,
    status: order.status,
    orderKind: order.orderKind,
    timeInForce: order.timeInForce,
    limitPrice: order.limitPrice,
    lifecycleStatus: order.lifecycleStatus,
    resultType: order.resultType,
    requestedAmountUsdc: order.requestedAmountUsdc,
    estimatedFee: order.estimatedFee,
    actualFee: order.actualFee,
    feeCurrency: order.feeCurrency,
    marketSlug: order.marketSlug,
    notionalUsdc: order.notionalUsdc,
    expectedQty: order.expectedQty,
    filledQty: order.filledQty,
    unfilledQty: order.unfilledQty,
    avgFillPrice: order.avgFillPrice,
    bestBid: order.bestBid,
    bestAsk: order.bestAsk,
    midPrice: order.midPrice,
    bookSnapshotTs: order.bookSnapshotTs,
    partialFilled: order.partialFilled,
    slippageBps: order.slippageBps,
    matchLatencyMs: order.matchLatencyMs,
    totalOrderLatencyMs: order.totalOrderLatencyMs,
    failureReason: order.failureReason,
    serverRecvTs: order.serverRecvTs,
    serverPublishTs: order.serverPublishTs,
    createdAt: order.createdAt
  };
}

export function toTradeLifecyclePayload(log: OrderLifecycleRecord): OrderLifecycleRecord {
  return {
    id: log.id,
    buyOrderId: log.buyOrderId,
    traceId: log.traceId,
    userId: log.userId,
    testerId: log.testerId,
    roundId: log.roundId,
    symbol: log.symbol,
    assetClass: log.assetClass,
    marketId: log.marketId,
    marketSlug: log.marketSlug,
    direction: log.direction,
    orderTimestampMs: log.orderTimestampMs,
    entryTokenPrice: log.entryTokenPrice,
    volumeTokenQty: log.volumeTokenQty,
    remainingTokenQty: log.remainingTokenQty,
    closedTokenQty: log.closedTokenQty,
    positionNotional: log.positionNotional,
    exitType: log.exitType,
    exitTokenPrice: log.exitTokenPrice,
    exitNotional: log.exitNotional,
    settlementResult: log.settlementResult,
    actualFillPrice: log.actualFillPrice,
    slippageBps: log.slippageBps,
    matchLatencyMs: log.matchLatencyMs,
    settlementTimeMs: log.settlementTimeMs,
    entryFee: log.entryFee,
    exitFee: log.exitFee,
    createdAt: log.createdAt,
    updatedAt: log.updatedAt
  };
}

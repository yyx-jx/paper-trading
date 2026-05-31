import type {
  AuditEvent,
  BehaviorActionLog,
  MarketCandleRecord,
  OrderBookSnapshot,
  OrderBookSnapshotRecord,
  OrderLifecycleExitType,
  OrderLifecycleRecord,
  OrderRecord,
  PositionRecord,
  Role,
  RoundRecord,
  RoundStatus,
  TradeSide
} from "../../domain/types";
import { cloneOrderBookSnapshot } from "./order-book-snapshots";

function parseJson<T>(value: unknown, fallback: T): T {
  if (typeof value === "string") {
    try {
      return JSON.parse(value) as T;
    } catch {
      return fallback;
    }
  }
  return (value as T) ?? fallback;
}

function numberOrUndefined(value: unknown) {
  return value === null || typeof value === "undefined" ? undefined : Number(value);
}

function sanitizePolymarketBtcReference(price?: number) {
  return typeof price === "number" && Number.isFinite(price) && price > 1000 ? price : undefined;
}

export function rowToMarketCandle(row: Record<string, unknown>): MarketCandleRecord {
  return {
    source: "coinbase",
    symbol: String(row.symbol),
    interval: "30s",
    openTs: Number(row.open_ts),
    closeTs: Number(row.close_ts),
    open: Number(row.open),
    high: Number(row.high),
    low: Number(row.low),
    close: Number(row.close),
    volume: Number(row.volume ?? 0),
    origin: String(row.origin) === "rtds_30s" ? "rtds_30s" : "history_1m_split",
    updatedAt: Number(row.updated_at)
  };
}

export function rowToRound(row: Record<string, unknown>): RoundRecord {
  const polymarketOpenPrice = sanitizePolymarketBtcReference(numberOrUndefined(row.polymarket_open_price));
  const polymarketClosePrice = sanitizePolymarketBtcReference(numberOrUndefined(row.polymarket_close_price));
  return {
    id: String(row.id),
    marketId: String(row.market_id),
    symbol: String(row.symbol),
    eventId: row.event_id ? String(row.event_id) : undefined,
    marketSlug: row.market_slug ? String(row.market_slug) : undefined,
    eventSlug: row.event_slug ? String(row.event_slug) : undefined,
    conditionId: row.condition_id ? String(row.condition_id) : undefined,
    seriesSlug: row.series_slug ? String(row.series_slug) : undefined,
    upTokenId: row.up_token_id ? String(row.up_token_id) : undefined,
    downTokenId: row.down_token_id ? String(row.down_token_id) : undefined,
    title: row.title ? String(row.title) : undefined,
    resolutionSource: row.resolution_source ? String(row.resolution_source) : undefined,
    startAt: Number(row.start_at),
    endAt: Number(row.end_at),
    priceToBeat: Number(row.price_to_beat),
    priceToBeatSource: row.price_to_beat_source ? String(row.price_to_beat_source) : undefined,
    priceToBeatCapturedAt: row.price_to_beat_captured_at ? Number(row.price_to_beat_captured_at) : undefined,
    status: String(row.status) as RoundStatus,
    pollCount: Number(row.poll_count),
    pollStartAt: row.poll_start_at ? Number(row.poll_start_at) : undefined,
    lastPollAt: row.last_poll_at ? Number(row.last_poll_at) : undefined,
    closingSpotPrice: row.closing_spot_price !== null ? Number(row.closing_spot_price) : undefined,
    settledSide: row.settled_side ? (String(row.settled_side) as RoundRecord["settledSide"]) : undefined,
    settlementPrice: row.settlement_price !== null ? Number(row.settlement_price) : undefined,
    settlementTs: row.settlement_ts ? Number(row.settlement_ts) : undefined,
    settlementSource: row.settlement_source ? (String(row.settlement_source) as RoundRecord["settlementSource"]) : undefined,
    polymarketSettlementPrice: numberOrUndefined(row.polymarket_settlement_price),
    polymarketSettlementStatus: row.polymarket_settlement_status
      ? (String(row.polymarket_settlement_status) as RoundRecord["polymarketSettlementStatus"])
      : undefined,
    polymarketOpenPrice,
    polymarketClosePrice,
    polymarketOpenPriceSource:
      polymarketOpenPrice && row.polymarket_open_price_source ? String(row.polymarket_open_price_source) : undefined,
    polymarketClosePriceSource:
      polymarketClosePrice && row.polymarket_close_price_source ? String(row.polymarket_close_price_source) : undefined,
    settlementReceivedAt: numberOrUndefined(row.settlement_received_at),
    redeemScheduledAt: numberOrUndefined(row.redeem_scheduled_at),
    binanceOpenPrice: numberOrUndefined(row.binance_open_price),
    binanceClosePrice: numberOrUndefined(row.binance_close_price),
    coinbaseOpenPrice: numberOrUndefined(row.chainlink_open_price),
    coinbaseClosePrice: numberOrUndefined(row.chainlink_close_price),
    redeemStartTs: row.redeem_start_ts ? Number(row.redeem_start_ts) : undefined,
    redeemFinishTs: row.redeem_finish_ts ? Number(row.redeem_finish_ts) : undefined,
    manualReason: row.manual_reason ? String(row.manual_reason) : undefined,
    acceptingOrders: row.accepting_orders !== null ? Boolean(row.accepting_orders) : undefined,
    closingPriceSource: row.closing_price_source
      ? (String(row.closing_price_source) as RoundRecord["closingPriceSource"])
      : undefined
  };
}

export function rowToOrderBookSnapshotRecord(row: Record<string, unknown>): OrderBookSnapshotRecord {
  const snapshot = parseJson<OrderBookSnapshot>(row.snapshot, {
    snapshotId: String(row.snapshot_id),
    snapshotTs: Number(row.snapshot_ts),
    bestBid: Number(row.best_bid),
    bestAsk: Number(row.best_ask),
    midPrice: Number(row.mid_price),
    bids: [],
    asks: []
  });
  return {
    ref: String(row.ref),
    snapshotId: String(row.snapshot_id),
    snapshotTs: Number(row.snapshot_ts),
    bestBid: Number(row.best_bid),
    bestAsk: Number(row.best_ask),
    midPrice: Number(row.mid_price),
    snapshot: cloneOrderBookSnapshot(snapshot),
    createdAt: Number(row.created_at)
  };
}

export function rowToOrderLifecycle(row: Record<string, unknown>): OrderLifecycleRecord {
  return {
    id: String(row.id),
    buyOrderId: String(row.buy_order_id),
    traceId: String(row.trace_id),
    userId: String(row.user_id),
    testerId: String(row.tester_id),
    roundId: String(row.round_id),
    symbol: String(row.symbol),
    assetClass: "BTC",
    marketId: String(row.market_id),
    marketSlug: row.market_slug ? String(row.market_slug) : undefined,
    direction: row.direction as TradeSide,
    orderTimestampMs: Number(row.order_timestamp_ms),
    entryTokenPrice: numberOrUndefined(row.entry_token_price),
    btcTradePrice: numberOrUndefined(row.btc_trade_price),
    btcOpenPriceToBeat: numberOrUndefined(row.btc_open_price_to_beat),
    deltaBtc: numberOrUndefined(row.delta_btc),
    volumeTokenQty: Number(row.volume_token_qty),
    remainingTokenQty: Number(row.remaining_token_qty),
    closedTokenQty: Number(row.closed_token_qty),
    positionNotional: Number(row.position_notional),
    exitType: row.exit_type ? (String(row.exit_type) as OrderLifecycleExitType) : undefined,
    exitTokenPrice: numberOrUndefined(row.exit_token_price),
    exitNotional: Number(row.exit_notional),
    settlementResult: row.settlement_result
      ? (String(row.settlement_result) as OrderLifecycleRecord["settlementResult"])
      : undefined,
    orderBookSnapshotRef: row.order_book_snapshot_ref ? String(row.order_book_snapshot_ref) : undefined,
    actualFillPrice: numberOrUndefined(row.actual_fill_price),
    slippageBps: numberOrUndefined(row.slippage_bps),
    matchLatencyMs: Number(row.match_latency_ms),
    settlementTimeMs: numberOrUndefined(row.settlement_time_ms),
    settlementDirection: row.settlement_direction ? (String(row.settlement_direction) as TradeSide) : undefined,
    entryFee: numberOrUndefined(row.entry_fee),
    exitFee: numberOrUndefined(row.exit_fee),
    feeCurrency: row.fee_currency ? "USD" : undefined,
    createdAt: Number(row.created_at),
    updatedAt: Number(row.updated_at)
  };
}

export function rowToOrder(row: Record<string, unknown>): OrderRecord {
  return {
    id: String(row.id),
    traceId: String(row.trace_id),
    userId: String(row.user_id),
    roundId: String(row.round_id),
    symbol: String(row.symbol),
    marketId: String(row.market_id),
    action: row.action as OrderRecord["action"],
    side: row.side as OrderRecord["side"],
    status: row.status as OrderRecord["status"],
    orderKind: row.order_kind ? (String(row.order_kind) as OrderRecord["orderKind"]) : undefined,
    timeInForce: row.time_in_force ? (String(row.time_in_force) as OrderRecord["timeInForce"]) : undefined,
    limitPrice: numberOrUndefined(row.limit_price),
    lifecycleStatus: row.lifecycle_status ? (String(row.lifecycle_status) as OrderRecord["lifecycleStatus"]) : undefined,
    resultType: row.result_type ? (String(row.result_type) as OrderRecord["resultType"]) : undefined,
    tokenId: row.token_id ? String(row.token_id) : undefined,
    bookKey: row.book_key ? String(row.book_key) : undefined,
    bookHash: row.book_hash ? String(row.book_hash) : undefined,
    requestedAmountUsdc: numberOrUndefined(row.requested_amount_usdc),
    requestedQty: numberOrUndefined(row.requested_qty),
    frozenUsdc: numberOrUndefined(row.frozen_usdc),
    frozenQty: numberOrUndefined(row.frozen_qty),
    fills: parseJson(row.fills, []),
    estimatedFee: numberOrUndefined(row.estimated_fee),
    actualFee: numberOrUndefined(row.actual_fee),
    feeBreakdown: parseJson(row.fee_breakdown, undefined),
    feeCurrency: row.fee_currency ? "USD" : undefined,
    sourceLatencyMs: numberOrUndefined(row.source_latency_ms),
    marketSlug: row.market_slug ? String(row.market_slug) : undefined,
    orderBookSnapshotRef: row.order_book_snapshot_ref ? String(row.order_book_snapshot_ref) : undefined,
    orderBookSnapshot: parseJson<OrderBookSnapshot | undefined>(row.order_book_snapshot, undefined),
    notionalUsdc: Number(row.notional_usdc),
    expectedQty: Number(row.expected_qty),
    filledQty: Number(row.filled_qty),
    unfilledQty: Number(row.unfilled_qty),
    avgFillPrice: row.avg_fill_price !== null ? Number(row.avg_fill_price) : undefined,
    bestBid: Number(row.best_bid),
    bestAsk: Number(row.best_ask),
    midPrice: Number(row.mid_price),
    bookSnapshotTs: Number(row.book_snapshot_ts),
    partialFilled: Boolean(row.partial_filled),
    slippageBps: row.slippage_bps !== null ? Number(row.slippage_bps) : undefined,
    matchLatencyMs: Number(row.match_latency_ms),
    bookAcquireLatencyMs: numberOrUndefined(row.book_acquire_latency_ms),
    localMatchLatencyMs: numberOrUndefined(row.local_match_latency_ms),
    persistLatencyMs: numberOrUndefined(row.persist_latency_ms),
    totalOrderLatencyMs: numberOrUndefined(row.total_order_latency_ms),
    failureReason: row.failure_reason ? String(row.failure_reason) : undefined,
    clientOrderId: row.client_order_id ? String(row.client_order_id) : undefined,
    clientSendTs: row.client_send_ts ? Number(row.client_send_ts) : undefined,
    serverRecvTs: Number(row.server_recv_ts),
    serverPublishTs: Number(row.server_publish_ts),
    createdAt: Number(row.created_at)
  };
}

export function rowToPosition(row: Record<string, unknown>): PositionRecord {
  return {
    id: String(row.id),
    buyOrderId: row.buy_order_id ? String(row.buy_order_id) : undefined,
    userId: String(row.user_id),
    roundId: String(row.round_id),
    side: row.side as PositionRecord["side"],
    qty: Number(row.qty),
    lockedQty: numberOrUndefined(row.locked_qty),
    averageEntry: Number(row.average_entry),
    notionalSpent: Number(row.notional_spent),
    currentMark: Number(row.current_mark),
    currentBid: numberOrUndefined(row.current_bid),
    currentAsk: numberOrUndefined(row.current_ask),
    currentMid: numberOrUndefined(row.current_mid),
    currentValue: numberOrUndefined(row.current_value),
    sourceLatencyMs: numberOrUndefined(row.source_latency_ms),
    unrealizedPnl: Number(row.unrealized_pnl),
    realizedPnl: Number(row.realized_pnl),
    entryFeeUsdc: numberOrUndefined(row.entry_fee_usdc),
    exitFeeUsdc: numberOrUndefined(row.exit_fee_usdc),
    totalFeeUsdc: numberOrUndefined(row.total_fee_usdc),
    costBasisUsdc: numberOrUndefined(row.cost_basis_usdc) ?? Number(row.notional_spent),
    markPnlUsdc: numberOrUndefined(row.mark_pnl_usdc),
    executablePnlUsdc: numberOrUndefined(row.executable_pnl_usdc),
    status: row.status as PositionRecord["status"],
    openedAt: Number(row.opened_at),
    closedAt: row.closed_at ? Number(row.closed_at) : undefined,
    settlementResult: row.settlement_result
      ? (String(row.settlement_result) as PositionRecord["settlementResult"])
      : undefined
  };
}

export function rowToAuditEvent(row: Record<string, unknown>): AuditEvent {
  return {
    eventId: String(row.event_id),
    traceId: String(row.trace_id),
    category: row.category as AuditEvent["category"],
    actionType: String(row.action_type),
    actionStatus: row.action_status as AuditEvent["actionStatus"],
    userId: row.user_id ? String(row.user_id) : undefined,
    role: row.role ? (String(row.role) as Role) : undefined,
    pageName: String(row.page_name),
    moduleName: String(row.module_name),
    symbol: row.symbol ? String(row.symbol) : undefined,
    roundId: row.round_id ? String(row.round_id) : undefined,
    resultCode: String(row.result_code),
    resultMessage: String(row.result_message),
    clientSendTs: row.client_send_ts ? Number(row.client_send_ts) : undefined,
    serverRecvTs: Number(row.server_recv_ts),
    engineStartTs: row.engine_start_ts ? Number(row.engine_start_ts) : undefined,
    engineFinishTs: row.engine_finish_ts ? Number(row.engine_finish_ts) : undefined,
    serverPublishTs: Number(row.server_publish_ts),
    backendLatencyMs: Number(row.backend_latency_ms),
    frontendLatencyMs: row.frontend_latency_ms !== null ? Number(row.frontend_latency_ms) : undefined,
    details: (row.details as Record<string, unknown> | null) ?? undefined
  };
}

export function rowToBehaviorLog(row: Record<string, unknown>): BehaviorActionLog {
  return {
    logId: String(row.log_id),
    timestampMs: Number(row.timestamp_ms),
    assetClass: "BTC_5M_UPDOWN",
    actionType: String(row.action_type),
    actionStatus: row.action_status as BehaviorActionLog["actionStatus"],
    roundId: row.round_id ? String(row.round_id) : undefined,
    direction: row.direction ? (String(row.direction) as BehaviorActionLog["direction"]) : undefined,
    entryOdds: row.entry_odds !== null ? Number(row.entry_odds) : undefined,
    deltaClob: Number(row.delta_clob),
    volumeClob: Number(row.volume_clob),
    positionNotional: row.position_notional !== null ? Number(row.position_notional) : undefined,
    exitType: row.exit_type ? String(row.exit_type) : undefined,
    exitOdds: row.exit_odds !== null ? Number(row.exit_odds) : undefined,
    settlementResult: row.settlement_result
      ? (String(row.settlement_result) as BehaviorActionLog["settlementResult"])
      : undefined,
    testerIdAnon: String(row.tester_id_anon),
    traceId: row.trace_id ? String(row.trace_id) : undefined,
    orderId: row.order_id ? String(row.order_id) : undefined,
    marketId: row.market_id ? String(row.market_id) : undefined,
    marketSlug: row.market_slug ? String(row.market_slug) : undefined,
    roundStatus: row.round_status ? (String(row.round_status) as RoundStatus) : undefined,
    countdownMs: row.countdown_ms !== null ? Number(row.countdown_ms) : undefined,
    binanceSpotPrice: Number(row.binance_spot_price),
    binance1mLastClose: Number(row.binance_1m_last_close),
    binance5mLastClose: Number(row.binance_5m_last_close),
    binance1dLastClose: Number(row.binance_1d_last_close),
    coinbasePrice: Number(row.chainlink_price),
    priceToBeat: Number(row.price_to_beat),
    upPrice: Number(row.up_price),
    downPrice: Number(row.down_price),
    upBookTop5: parseJson(row.up_book_top5, []),
    downBookTop5: parseJson(row.down_book_top5, []),
    recentTradesTop20: parseJson(row.recent_trades_top20, []),
    bookSnapshotEntry: parseJson(row.book_snapshot_entry, {
      snapshotId: "",
      snapshotTs: 0,
      topBids: [],
      topAsks: []
    }),
    actualFillPrice: row.actual_fill_price !== null ? Number(row.actual_fill_price) : undefined,
    slippageBps: row.slippage_bps !== null ? Number(row.slippage_bps) : undefined,
    partialFilled: row.partial_filled !== null ? Boolean(row.partial_filled) : undefined,
    unfilledQty: row.unfilled_qty !== null ? Number(row.unfilled_qty) : undefined,
    executionLatencyMs: row.execution_latency_ms !== null ? Number(row.execution_latency_ms) : undefined,
    settlementDirection: row.settlement_direction
      ? (String(row.settlement_direction) as TradeSide)
      : undefined,
    settlementTimeMs: row.settlement_time_ms !== null ? Number(row.settlement_time_ms) : undefined,
    gammaPollCount: row.gamma_poll_count !== null ? Number(row.gamma_poll_count) : undefined,
    redeemFinishTimeMs: row.redeem_finish_time_ms !== null ? Number(row.redeem_finish_time_ms) : undefined,
    sourceStates: parseJson(row.source_states, {
      binance: { source: "Binance", state: "reconnecting", sourceEventTs: 0, serverRecvTs: 0, serverPublishTs: 0 },
      coinbase: { source: "Coinbase", state: "reconnecting", sourceEventTs: 0, serverRecvTs: 0, serverPublishTs: 0 },
      clob: { source: "CLOB", state: "reconnecting", sourceEventTs: 0, serverRecvTs: 0, serverPublishTs: 0 }
    }),
    strategyClusterLabel: row.strategy_cluster_label ? String(row.strategy_cluster_label) : undefined,
    marketRegimeLabel: row.market_regime_label ? String(row.market_regime_label) : undefined,
    qualityGrade: row.quality_grade ? String(row.quality_grade) : undefined,
    contextJson: parseJson(row.context_json, {})
  };
}

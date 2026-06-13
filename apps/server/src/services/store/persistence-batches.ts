import type { OrderLifecycleRecord, OrderRecord, PositionRecord, UserRecord } from "../../domain/types";

const roundNumber = (value: number, digits = 8) => Number(value.toFixed(digits));

export type BulkUpsertSpec<T> = {
  columns: readonly string[];
  conflictTarget: string;
  mapRecord: (record: T) => readonly unknown[];
  table: string;
  updateAssignments: readonly string[];
};

export type BulkUpsertSection<T> = {
  alias: string;
  records: readonly T[];
  spec: BulkUpsertSpec<T>;
};

type BulkUpsertQuery = {
  query: string;
  nextParamIndex: number;
};

function buildValuesPlaceholders(rowCount: number, columnCount: number, startParamIndex: number) {
  let nextParamIndex = startParamIndex;
  const placeholders: string[] = [];
  for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
    const rowPlaceholders: string[] = [];
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex += 1) {
      rowPlaceholders.push(`$${nextParamIndex}`);
      nextParamIndex += 1;
    }
    placeholders.push(`(${rowPlaceholders.join(",")})`);
  }
  return {
    nextParamIndex,
    valuesSql: placeholders.join(",\n")
  };
}

function buildBulkUpsertBody<T>(spec: BulkUpsertSpec<T>, rowCount: number, startParamIndex = 1): BulkUpsertQuery {
  if (rowCount <= 0) {
    throw new Error("Bulk upsert requires at least one record.");
  }
  const { valuesSql, nextParamIndex } = buildValuesPlaceholders(rowCount, spec.columns.length, startParamIndex);
  return {
    query: `
      INSERT INTO ${spec.table} (
        ${spec.columns.join(", ")}
      ) VALUES
        ${valuesSql}
      ON CONFLICT ${spec.conflictTarget} DO UPDATE SET
        ${spec.updateAssignments.join(",\n        ")}
    `,
    nextParamIndex
  };
}

export function flattenBulkUpsertParams<T>(spec: BulkUpsertSpec<T>, records: readonly T[]) {
  return records.flatMap((record) => [...spec.mapRecord(record)]);
}

export function buildBulkUpsertQuery<T>(spec: BulkUpsertSpec<T>, records: readonly T[]) {
  const { query } = buildBulkUpsertBody(spec, records.length);
  return query;
}

export function buildCombinedBulkUpsertQuery(sections: readonly BulkUpsertSection<any>[]) {
  let nextParamIndex = 1;
  const params: unknown[] = [];
  const ctes = sections
    .filter((section) => section.records.length > 0)
    .map((section) => {
      const { query, nextParamIndex: nextIndex } = buildBulkUpsertBody(section.spec, section.records.length, nextParamIndex);
      nextParamIndex = nextIndex;
      params.push(...flattenBulkUpsertParams(section.spec, section.records));
      return `${section.alias} AS (\n${query}\n        RETURNING 1\n      )`;
    });

  return ctes.length > 0
    ? {
        params,
        query: `WITH\n      ${ctes.join(",\n      ")}\n      SELECT 1;`
      }
    : {
        params,
        query: ""
      };
}

export const userPersistenceSpec: BulkUpsertSpec<UserRecord> = {
  table: "users",
  columns: [
    "id",
    "username",
    "password",
    "display_name",
    "role",
    "language",
    "permission_codes",
    "available_usdc",
    "is_active",
    "senior_tester_id",
    "disabled_at",
    "disabled_by",
    "manager_user_id",
    "permission_level",
    "failed_login_count",
    "locked_until",
    "password_changed_at",
    "last_login_at",
    "must_change_password",
    "created_at",
    "updated_at"
  ],
  conflictTarget: "(id)",
  updateAssignments: [
    "username = EXCLUDED.username",
    "password = EXCLUDED.password",
    "display_name = EXCLUDED.display_name",
    "role = EXCLUDED.role",
    "language = EXCLUDED.language",
    "permission_codes = EXCLUDED.permission_codes",
    "available_usdc = EXCLUDED.available_usdc",
    "is_active = EXCLUDED.is_active",
    "senior_tester_id = EXCLUDED.senior_tester_id",
    "disabled_at = EXCLUDED.disabled_at",
    "disabled_by = EXCLUDED.disabled_by",
    "manager_user_id = EXCLUDED.manager_user_id",
    "permission_level = EXCLUDED.permission_level",
    "failed_login_count = EXCLUDED.failed_login_count",
    "locked_until = EXCLUDED.locked_until",
    "password_changed_at = EXCLUDED.password_changed_at",
    "last_login_at = EXCLUDED.last_login_at",
    "must_change_password = EXCLUDED.must_change_password",
    "updated_at = EXCLUDED.updated_at"
  ],
  mapRecord: (user) => [
    user.id,
    user.username,
    user.password,
    user.displayName,
    user.role,
    user.language,
    JSON.stringify(user.permissionCodes),
    user.availableUsdc,
    user.isActive,
    user.seniorTesterId ?? null,
    user.disabledAt ?? null,
    user.disabledBy ?? null,
    user.managerUserId ?? null,
    user.permissionLevel ?? "Standard",
    user.failedLoginCount ?? 0,
    user.lockedUntil ?? null,
    user.passwordChangedAt ?? null,
    user.lastLoginAt ?? null,
    user.mustChangePassword ?? false,
    user.createdAt,
    user.updatedAt
  ]
};

export const orderLifecyclePersistenceSpec: BulkUpsertSpec<OrderLifecycleRecord> = {
  table: "order_lifecycle_logs",
  columns: [
    "id",
    "buy_order_id",
    "trace_id",
    "user_id",
    "tester_id",
    "round_id",
    "symbol",
    "asset_class",
    "market_id",
    "market_slug",
    "direction",
    "order_timestamp_ms",
    "entry_token_price",
    "btc_trade_price",
    "btc_open_price_to_beat",
    "delta_btc",
    "volume_token_qty",
    "remaining_token_qty",
    "closed_token_qty",
    "position_notional",
    "exit_type",
    "exit_token_price",
    "exit_notional",
    "settlement_result",
    "order_book_snapshot_ref",
    "actual_fill_price",
    "slippage_bps",
    "match_latency_ms",
    "settlement_time_ms",
    "settlement_direction",
    "entry_fee",
    "exit_fee",
    "fee_currency",
    "created_at",
    "updated_at"
  ],
  conflictTarget: "(id)",
  updateAssignments: [
    "remaining_token_qty = EXCLUDED.remaining_token_qty",
    "closed_token_qty = EXCLUDED.closed_token_qty",
    "exit_type = EXCLUDED.exit_type",
    "exit_token_price = EXCLUDED.exit_token_price",
    "exit_notional = EXCLUDED.exit_notional",
    "settlement_result = EXCLUDED.settlement_result",
    "settlement_time_ms = EXCLUDED.settlement_time_ms",
    "settlement_direction = EXCLUDED.settlement_direction",
    "exit_fee = EXCLUDED.exit_fee",
    "fee_currency = EXCLUDED.fee_currency",
    "updated_at = EXCLUDED.updated_at"
  ],
  mapRecord: (log) => [
    log.id,
    log.buyOrderId,
    log.traceId,
    log.userId,
    log.testerId,
    log.roundId,
    log.symbol,
    log.assetClass,
    log.marketId,
    log.marketSlug ?? null,
    log.direction,
    log.orderTimestampMs,
    log.entryTokenPrice ?? null,
    log.btcTradePrice ?? null,
    log.btcOpenPriceToBeat ?? null,
    log.deltaBtc ?? null,
    log.volumeTokenQty,
    log.remainingTokenQty,
    log.closedTokenQty,
    log.positionNotional,
    log.exitType ?? null,
    log.exitTokenPrice ?? null,
    log.exitNotional,
    log.settlementResult ?? null,
    log.orderBookSnapshotRef ?? null,
    log.actualFillPrice ?? null,
    log.slippageBps ?? null,
    log.matchLatencyMs,
    log.settlementTimeMs ?? null,
    log.settlementDirection ?? null,
    log.entryFee ?? null,
    log.exitFee ?? null,
    log.feeCurrency ?? null,
    log.createdAt,
    log.updatedAt
  ]
};

export const orderPersistenceSpec: BulkUpsertSpec<OrderRecord> = {
  table: "orders",
  columns: [
    "id",
    "trace_id",
    "user_id",
    "round_id",
    "symbol",
    "market_id",
    "order_kind",
    "time_in_force",
    "limit_price",
    "lifecycle_status",
    "result_type",
    "token_id",
    "book_key",
    "book_hash",
    "requested_amount_usdc",
    "requested_qty",
    "frozen_usdc",
    "frozen_qty",
    "fills",
    "estimated_fee",
    "actual_fee",
    "fee_breakdown",
    "fee_currency",
    "source_latency_ms",
    "market_slug",
    "order_book_snapshot_ref",
    "order_book_snapshot",
    "action",
    "side",
    "status",
    "notional_usdc",
    "expected_qty",
    "filled_qty",
    "unfilled_qty",
    "avg_fill_price",
    "best_bid",
    "best_ask",
    "mid_price",
    "book_snapshot_ts",
    "partial_filled",
    "slippage_bps",
    "match_latency_ms",
    "book_acquire_latency_ms",
    "local_match_latency_ms",
    "persist_latency_ms",
    "total_order_latency_ms",
    "failure_reason",
    "client_order_id",
    "client_send_ts",
    "server_recv_ts",
    "server_publish_ts",
    "created_at"
  ],
  conflictTarget: "(id)",
  updateAssignments: [
    "lifecycle_status = EXCLUDED.lifecycle_status",
    "result_type = EXCLUDED.result_type",
    "status = EXCLUDED.status",
    "filled_qty = EXCLUDED.filled_qty",
    "unfilled_qty = EXCLUDED.unfilled_qty",
    "avg_fill_price = EXCLUDED.avg_fill_price",
    "notional_usdc = EXCLUDED.notional_usdc",
    "partial_filled = EXCLUDED.partial_filled",
    "slippage_bps = EXCLUDED.slippage_bps",
    "match_latency_ms = EXCLUDED.match_latency_ms",
    "book_acquire_latency_ms = EXCLUDED.book_acquire_latency_ms",
    "local_match_latency_ms = EXCLUDED.local_match_latency_ms",
    "persist_latency_ms = EXCLUDED.persist_latency_ms",
    "total_order_latency_ms = EXCLUDED.total_order_latency_ms",
    "frozen_usdc = EXCLUDED.frozen_usdc",
    "frozen_qty = EXCLUDED.frozen_qty",
    "fills = EXCLUDED.fills",
    "estimated_fee = EXCLUDED.estimated_fee",
    "actual_fee = EXCLUDED.actual_fee",
    "fee_breakdown = EXCLUDED.fee_breakdown",
    "fee_currency = EXCLUDED.fee_currency",
    "order_book_snapshot_ref = EXCLUDED.order_book_snapshot_ref",
    "failure_reason = EXCLUDED.failure_reason",
    "client_order_id = EXCLUDED.client_order_id",
    "server_publish_ts = EXCLUDED.server_publish_ts"
  ],
  mapRecord: (order) => [
    order.id,
    order.traceId,
    order.userId,
    order.roundId,
    order.symbol,
    order.marketId,
    order.orderKind ?? null,
    order.timeInForce ?? null,
    order.limitPrice ?? null,
    order.lifecycleStatus ?? order.status,
    order.resultType ?? null,
    order.tokenId ?? null,
    order.bookKey ?? null,
    order.bookHash ?? null,
    order.requestedAmountUsdc ?? null,
    order.requestedQty ?? null,
    order.frozenUsdc ?? null,
    order.frozenQty ?? null,
    JSON.stringify(order.fills ?? []),
    order.estimatedFee ?? null,
    order.actualFee ?? null,
    JSON.stringify(order.feeBreakdown ?? null),
    order.feeCurrency ?? null,
    order.sourceLatencyMs ?? null,
    order.marketSlug ?? null,
    order.orderBookSnapshotRef ?? null,
    null,
    order.action,
    order.side,
    order.status,
    order.notionalUsdc,
    order.expectedQty,
    order.filledQty,
    order.unfilledQty,
    order.avgFillPrice ?? null,
    order.bestBid,
    order.bestAsk,
    order.midPrice,
    order.bookSnapshotTs,
    order.partialFilled,
    order.slippageBps ?? null,
    order.matchLatencyMs,
    order.bookAcquireLatencyMs ?? null,
    order.localMatchLatencyMs ?? null,
    order.persistLatencyMs ?? null,
    order.totalOrderLatencyMs ?? null,
    order.failureReason ?? null,
    order.clientOrderId ?? null,
    order.clientSendTs ?? null,
    order.serverRecvTs,
    order.serverPublishTs,
    order.createdAt
  ]
};

export const positionPersistenceSpec: BulkUpsertSpec<PositionRecord> = {
  table: "positions",
  columns: [
    "id",
    "buy_order_id",
    "user_id",
    "round_id",
    "side",
    "qty",
    "locked_qty",
    "average_entry",
    "notional_spent",
    "current_mark",
    "current_bid",
    "current_ask",
    "current_mid",
    "current_value",
    "source_latency_ms",
    "unrealized_pnl",
    "realized_pnl",
    "entry_fee_usdc",
    "exit_fee_usdc",
    "total_fee_usdc",
    "cost_basis_usdc",
    "mark_pnl_usdc",
    "executable_pnl_usdc",
    "status",
    "opened_at",
    "closed_at",
    "settlement_result"
  ],
  conflictTarget: "(id)",
  updateAssignments: [
    "buy_order_id = EXCLUDED.buy_order_id",
    "qty = EXCLUDED.qty",
    "locked_qty = EXCLUDED.locked_qty",
    "average_entry = EXCLUDED.average_entry",
    "notional_spent = EXCLUDED.notional_spent",
    "current_mark = EXCLUDED.current_mark",
    "current_bid = EXCLUDED.current_bid",
    "current_ask = EXCLUDED.current_ask",
    "current_mid = EXCLUDED.current_mid",
    "current_value = EXCLUDED.current_value",
    "source_latency_ms = EXCLUDED.source_latency_ms",
    "unrealized_pnl = EXCLUDED.unrealized_pnl",
    "realized_pnl = EXCLUDED.realized_pnl",
    "entry_fee_usdc = EXCLUDED.entry_fee_usdc",
    "exit_fee_usdc = EXCLUDED.exit_fee_usdc",
    "total_fee_usdc = EXCLUDED.total_fee_usdc",
    "cost_basis_usdc = EXCLUDED.cost_basis_usdc",
    "mark_pnl_usdc = EXCLUDED.mark_pnl_usdc",
    "executable_pnl_usdc = EXCLUDED.executable_pnl_usdc",
    "status = EXCLUDED.status",
    "closed_at = EXCLUDED.closed_at",
    "settlement_result = EXCLUDED.settlement_result"
  ],
  mapRecord: (position) => [
    position.id,
    position.buyOrderId ?? null,
    position.userId,
    position.roundId,
    position.side,
    position.qty,
    position.lockedQty ?? 0,
    position.averageEntry,
    position.notionalSpent,
    position.currentMark,
    position.currentBid ?? null,
    position.currentAsk ?? null,
    position.currentMid ?? null,
    position.currentValue ?? null,
    position.sourceLatencyMs ?? null,
    position.unrealizedPnl,
    position.realizedPnl,
    position.entryFeeUsdc ?? 0,
    position.exitFeeUsdc ?? 0,
    position.totalFeeUsdc ?? roundNumber((position.entryFeeUsdc ?? 0) + (position.exitFeeUsdc ?? 0), 8),
    position.costBasisUsdc ?? position.notionalSpent,
    position.markPnlUsdc ?? roundNumber((position.currentValue ?? position.qty * position.currentMark) - (position.costBasisUsdc ?? position.notionalSpent), 2),
    position.executablePnlUsdc ?? roundNumber(((position.currentBid ?? position.currentMark) * position.qty) - (position.costBasisUsdc ?? position.notionalSpent), 2),
    position.status,
    position.openedAt,
    position.closedAt ?? null,
    position.settlementResult ?? null
  ]
};

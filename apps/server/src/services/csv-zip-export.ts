import type {
  AuditEvent,
  BehaviorActionLog,
  LogSearchQuery,
  MatchingEventRecord,
  LogCategory,
  OrderLifecycleRecord,
  PositionRecord,
  ProfileOverview,
  PublicUser,
  Role,
  RoundRecord,
  UserRecord
} from "../domain/types";

export interface CsvColumn<T> {
  header: string;
  value: (row: T) => unknown;
}

export interface ZipEntry {
  path: string;
  content: string | Buffer;
}

export interface ExportUser extends PublicUser {
  anonId: string;
}

export interface ExportQuery {
  system?: LogSearchQuery["system"];
  systems?: LogSearchQuery["systems"];
  from?: number;
  to?: number;
  viewUserId?: string;
  userId?: string;
  userIds?: string[];
  role?: Role;
  roundId?: string;
  category?: LogCategory;
  actionType?: string;
  actionStatus?: AuditEvent["actionStatus"];
  moduleName?: string;
  pageName?: string;
  symbol?: string;
  traceId?: string;
  orderId?: string;
  positionId?: string;
  resultCode?: string;
  marketId?: string;
  marketSlug?: string;
  direction?: LogSearchQuery["direction"];
  roundStatus?: LogSearchQuery["roundStatus"];
  settlementResult?: LogSearchQuery["settlementResult"];
  bookKey?: string;
  bookSide?: LogSearchQuery["bookSide"];
  eventType?: LogSearchQuery["eventType"];
  sequenceFrom?: number;
  sequenceTo?: number;
  logGroup?: LogSearchQuery["logGroup"];
  latencySource?: LogSearchQuery["latencySource"];
  connectionState?: LogSearchQuery["connectionState"];
  latencyPhase?: LogSearchQuery["latencyPhase"];
  latencyMinMs?: number;
  latencyMaxMs?: number;
  matchingLogKind?: LogSearchQuery["matchingLogKind"];
}

export interface UserExportData {
  user: ExportUser;
  auditLogs: AuditEvent[];
  trainingLogs: BehaviorActionLog[];
  matchingEvents: MatchingEventRecord[];
  matchingActionLogs?: AuditEvent[];
  orders: OrderLifecycleRecord[];
  positions: PositionRecord[];
  operatedRounds: Array<RoundRecord & { userPnl: number }>;
  profile: ProfileOverview;
}

export interface ExportManifestFile {
  path: string;
  rowCount: number;
  system: "audit" | "training" | "matching" | "matching_actions" | "orders" | "positions" | "rounds" | "profile";
  userId?: string;
}

export interface ExportManifest {
  generatedAt: number;
  generatedAtIso: string;
  query: LogSearchQuery;
  actor: {
    id: string;
    username: string;
    role: Role;
  };
  users: Array<{
    id: string;
    username: string;
    role: Role;
    anonId: string;
  }>;
  files: ExportManifestFile[];
  summary?: {
    auditRows: number;
    latencyRows: number;
    matchingActionRows: number;
    matchingEngineRows: number;
  };
}

function iso(ts?: number) {
  return typeof ts === "number" && Number.isFinite(ts) ? new Date(ts).toISOString() : "";
}

function stableJson(value: unknown) {
  if (value === undefined || value === null) {
    return "";
  }
  return JSON.stringify(value);
}

function csvCell(value: unknown) {
  const text =
    value === undefined || value === null
      ? ""
      : typeof value === "object"
        ? stableJson(value)
        : String(value);
  return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function dash(value: unknown) {
  return value === undefined || value === null || value === "" ? "--" : value;
}

function dashIso(ts?: number) {
  return typeof ts === "number" && Number.isFinite(ts) ? new Date(ts).toISOString() : "--";
}

function detailString(details: Record<string, unknown> | undefined, key: string) {
  const value = details?.[key];
  return typeof value === "string" || typeof value === "number" ? String(value) : "";
}

function detailNumber(details: Record<string, unknown> | undefined, key: string) {
  const value = details?.[key];
  return typeof value === "number" && Number.isFinite(value)
    ? value
    : typeof value === "string" && Number.isFinite(Number(value))
      ? Number(value)
      : "";
}

function auditLogGroup(row: AuditEvent) {
  if (row.category === "matching") {
    return "matching_action";
  }
  if (row.category === "settlement") {
    return "settlement";
  }
  if (row.category === "latency") {
    return ["binance", "coinbase", "clob"].includes(row.moduleName) ? "market_latency" : "system_latency";
  }
  return "operation";
}

function latencySource(row: AuditEvent) {
  if (row.category !== "latency") {
    return "";
  }
  return ["binance", "coinbase", "clob"].includes(row.moduleName) ? row.moduleName : "system";
}

export function toCsv<T>(rows: T[], columns: CsvColumn<T>[]) {
  const header = columns.map((column) => csvCell(column.header)).join(",");
  const body = rows.map((row) => columns.map((column) => csvCell(column.value(row))).join(","));
  return `${[header, ...body].join("\r\n")}\r\n`;
}

function userColumns<T extends { user: ExportUser }>(): CsvColumn<T>[] {
  return [
    { header: "user_id", value: (row) => row.user.id },
    { header: "username", value: (row) => row.user.username },
    { header: "display_name", value: (row) => row.user.displayName },
    { header: "role", value: (row) => row.user.role },
    { header: "is_active", value: (row) => row.user.isActive },
    { header: "disabled_at", value: (row) => row.user.disabledAt },
    { header: "disabled_at_iso", value: (row) => iso(row.user.disabledAt) }
  ];
}

export function auditLogsCsv(user: ExportUser | undefined, logs: AuditEvent[]) {
  type Row = AuditEvent & { user?: ExportUser };
  const rows: Row[] = logs.map((log) => ({ ...log, user }));
  const columns: CsvColumn<Row>[] = [
    { header: "event_id", value: (row) => row.eventId },
    { header: "trace_id", value: (row) => row.traceId },
    { header: "category", value: (row) => row.category },
    { header: "log_group", value: (row) => auditLogGroup(row) },
    { header: "action_type", value: (row) => row.actionType },
    { header: "action_status", value: (row) => row.actionStatus },
    { header: "user_id", value: (row) => row.user?.id ?? row.userId },
    { header: "username", value: (row) => row.user?.username },
    { header: "display_name", value: (row) => row.user?.displayName },
    { header: "role", value: (row) => row.user?.role ?? row.role },
    { header: "is_active", value: (row) => row.user?.isActive },
    { header: "page_name", value: (row) => row.pageName },
    { header: "module_name", value: (row) => row.moduleName },
    { header: "symbol", value: (row) => row.symbol },
    { header: "round_id", value: (row) => row.roundId },
    { header: "result_code", value: (row) => row.resultCode },
    { header: "result_message", value: (row) => row.resultMessage },
    { header: "client_send_ts", value: (row) => row.clientSendTs },
    { header: "client_send_iso", value: (row) => iso(row.clientSendTs) },
    { header: "server_recv_ts", value: (row) => row.serverRecvTs },
    { header: "server_recv_iso", value: (row) => iso(row.serverRecvTs) },
    { header: "engine_start_ts", value: (row) => row.engineStartTs },
    { header: "engine_start_iso", value: (row) => iso(row.engineStartTs) },
    { header: "engine_finish_ts", value: (row) => row.engineFinishTs },
    { header: "engine_finish_iso", value: (row) => iso(row.engineFinishTs) },
    { header: "server_publish_ts", value: (row) => row.serverPublishTs },
    { header: "server_publish_iso", value: (row) => iso(row.serverPublishTs) },
    { header: "backend_latency_ms", value: (row) => row.backendLatencyMs },
    { header: "frontend_latency_ms", value: (row) => row.frontendLatencyMs },
    { header: "latency_source", value: (row) => latencySource(row) },
    { header: "connection_state", value: (row) => detailString(row.details, "connectionState") },
    { header: "acquire_latency_ms", value: (row) => detailNumber(row.details, "acquireLatencyMs") },
    { header: "publish_latency_ms", value: (row) => detailNumber(row.details, "publishLatencyMs") },
    { header: "reconnect_count", value: (row) => detailNumber(row.details, "reconnectCount") },
    { header: "details_json", value: (row) => row.details }
  ];
  return toCsv(rows, columns);
}

export function trainingLogsCsv(user: ExportUser, logs: BehaviorActionLog[]) {
  type Row = BehaviorActionLog & { user: ExportUser };
  const rows: Row[] = logs.map((log) => ({ ...log, user }));
  const columns: CsvColumn<Row>[] = [
    { header: "log_id", value: (row) => row.logId },
    { header: "timestamp_ms", value: (row) => row.timestampMs },
    { header: "timestamp_iso", value: (row) => iso(row.timestampMs) },
    ...userColumns<Row>(),
    { header: "asset_class", value: (row) => row.assetClass },
    { header: "action_type", value: (row) => row.actionType },
    { header: "action_status", value: (row) => row.actionStatus },
    { header: "round_id", value: (row) => row.roundId },
    { header: "direction", value: (row) => row.direction },
    { header: "entry_odds", value: (row) => row.entryOdds },
    { header: "delta_clob", value: (row) => row.deltaClob },
    { header: "volume_clob", value: (row) => row.volumeClob },
    { header: "position_notional", value: (row) => row.positionNotional },
    { header: "exit_type", value: (row) => row.exitType },
    { header: "exit_odds", value: (row) => row.exitOdds },
    { header: "settlement_result", value: (row) => row.settlementResult },
    { header: "tester_id_anon", value: (row) => row.testerIdAnon },
    { header: "trace_id", value: (row) => row.traceId },
    { header: "order_id", value: (row) => row.orderId },
    { header: "market_id", value: (row) => row.marketId },
    { header: "market_slug", value: (row) => row.marketSlug },
    { header: "round_status", value: (row) => row.roundStatus },
    { header: "countdown_ms", value: (row) => row.countdownMs },
    { header: "binance_spot_price", value: (row) => row.binanceSpotPrice },
    { header: "binance_1m_last_close", value: (row) => row.binance1mLastClose },
    { header: "binance_5m_last_close", value: (row) => row.binance5mLastClose },
    { header: "binance_1d_last_close", value: (row) => row.binance1dLastClose },
    { header: "coinbase_price", value: (row) => row.coinbasePrice },
    { header: "price_to_beat", value: (row) => row.priceToBeat },
    { header: "up_price", value: (row) => row.upPrice },
    { header: "down_price", value: (row) => row.downPrice },
    { header: "actual_fill_price", value: (row) => row.actualFillPrice },
    { header: "slippage_bps", value: (row) => row.slippageBps },
    { header: "partial_filled", value: (row) => row.partialFilled },
    { header: "unfilled_qty", value: (row) => row.unfilledQty },
    { header: "execution_latency_ms", value: (row) => row.executionLatencyMs },
    { header: "settlement_direction", value: (row) => row.settlementDirection },
    { header: "settlement_time_ms", value: (row) => row.settlementTimeMs },
    { header: "gamma_poll_count", value: (row) => row.gammaPollCount },
    { header: "redeem_finish_time_ms", value: (row) => row.redeemFinishTimeMs },
    { header: "strategy_cluster_label", value: (row) => row.strategyClusterLabel },
    { header: "market_regime_label", value: (row) => row.marketRegimeLabel },
    { header: "quality_grade", value: (row) => row.qualityGrade },
    { header: "up_book_top5_json", value: (row) => row.upBookTop5 },
    { header: "down_book_top5_json", value: (row) => row.downBookTop5 },
    { header: "recent_trades_top20_json", value: (row) => row.recentTradesTop20 },
    { header: "book_snapshot_entry_json", value: (row) => row.bookSnapshotEntry },
    { header: "source_states_json", value: (row) => row.sourceStates },
    { header: "context_json", value: (row) => row.contextJson }
  ];
  return toCsv(rows, columns);
}

function matchingEventUserId(event: MatchingEventRecord) {
  const request = event.payload.request as { userId?: string } | undefined;
  return request?.userId;
}

export function matchingEventsCsv(user: ExportUser | undefined, events: MatchingEventRecord[]) {
  type Row = MatchingEventRecord & { user?: ExportUser; eventUserId?: string; status?: string };
  const rows: Row[] = events.map((event) => ({
    ...event,
    user,
    eventUserId: matchingEventUserId(event),
    status: typeof event.payload.status === "string" ? event.payload.status : undefined
  }));
  const columns: CsvColumn<Row>[] = [
    { header: "event_id", value: (row) => row.eventId },
    { header: "created_at", value: (row) => row.createdAt },
    { header: "created_at_iso", value: (row) => iso(row.createdAt) },
    { header: "user_id", value: (row) => row.user?.id ?? row.eventUserId },
    { header: "username", value: (row) => row.user?.username },
    { header: "display_name", value: (row) => row.user?.displayName },
    { header: "role", value: (row) => row.user?.role },
    { header: "book_key", value: (row) => row.bookKey },
    { header: "round_id", value: (row) => row.roundId },
    { header: "market_id", value: (row) => row.marketId },
    { header: "book_side", value: (row) => row.bookSide },
    { header: "sequence", value: (row) => row.sequence },
    { header: "event_type", value: (row) => row.eventType },
    { header: "order_id", value: (row) => row.orderId },
    { header: "trace_id", value: (row) => row.traceId },
    { header: "status", value: (row) => row.status },
    { header: "payload_json", value: (row) => row.payload }
  ];
  return toCsv(rows, columns);
}

export function ordersCsv(user: ExportUser, orders: OrderLifecycleRecord[]) {
  type Row = OrderLifecycleRecord & { user: ExportUser };
  const rows: Row[] = orders.map((order) => ({ ...order, user }));
  const columns: CsvColumn<Row>[] = [
    ...userColumns<Row>(),
    { header: "order_id", value: (row) => row.buyOrderId },
    { header: "trace_id", value: (row) => row.traceId },
    { header: "order_timestamp_ms", value: (row) => row.orderTimestampMs },
    { header: "order_timestamp_iso", value: (row) => dashIso(row.orderTimestampMs) },
    { header: "asset_class", value: (row) => row.assetClass },
    { header: "round_id", value: (row) => row.roundId },
    { header: "direction", value: (row) => row.direction },
    { header: "entry_token_price", value: (row) => dash(row.entryTokenPrice) },
    { header: "btc_trade_price", value: (row) => dash(row.btcTradePrice) },
    { header: "btc_open_price_to_beat", value: (row) => dash(row.btcOpenPriceToBeat) },
    { header: "delta_btc", value: (row) => dash(row.deltaBtc) },
    { header: "volume_token_qty", value: (row) => row.volumeTokenQty },
    { header: "position_notional", value: (row) => row.positionNotional },
    { header: "exit_type", value: (row) => dash(row.exitType) },
    { header: "exit_token_price", value: (row) => dash(row.exitTokenPrice) },
    { header: "settlement_result", value: (row) => dash(row.settlementResult) },
    { header: "tester_id", value: (row) => row.testerId },
    { header: "order_book_snapshot_ref", value: (row) => dash(row.orderBookSnapshotRef) },
    { header: "order_book_snapshot_json", value: (row) => dash(row.orderBookSnapshot) },
    { header: "actual_fill_price", value: (row) => dash(row.actualFillPrice) },
    { header: "slippage_bps", value: (row) => dash(row.slippageBps) },
    { header: "match_latency_ms", value: (row) => row.matchLatencyMs },
    { header: "settlement_time_ms", value: (row) => dash(row.settlementTimeMs) },
    { header: "settlement_time_iso", value: (row) => row.settlementTimeMs ? dashIso(row.settlementTimeMs) : "--" },
    { header: "settlement_direction", value: (row) => dash(row.settlementDirection) },
    { header: "market_id", value: (row) => row.marketId },
    { header: "market_slug", value: (row) => dash(row.marketSlug) }
  ];
  return toCsv(rows, columns);
}

export function positionsCsv(user: ExportUser, positions: PositionRecord[]) {
  type Row = PositionRecord & { user: ExportUser };
  const rows: Row[] = positions.map((position) => ({ ...position, user }));
  const columns: CsvColumn<Row>[] = [
    ...userColumns<Row>(),
    { header: "position_id", value: (row) => row.id },
    { header: "round_id", value: (row) => row.roundId },
    { header: "side", value: (row) => row.side },
    { header: "qty", value: (row) => row.qty },
    { header: "locked_qty", value: (row) => row.lockedQty },
    { header: "average_entry", value: (row) => row.averageEntry },
    { header: "notional_spent", value: (row) => row.notionalSpent },
    { header: "current_mark", value: (row) => row.currentMark },
    { header: "current_bid", value: (row) => row.currentBid },
    { header: "current_ask", value: (row) => row.currentAsk },
    { header: "current_mid", value: (row) => row.currentMid },
    { header: "current_value", value: (row) => row.currentValue },
    { header: "source_latency_ms", value: (row) => row.sourceLatencyMs },
    { header: "unrealized_pnl", value: (row) => row.unrealizedPnl },
    { header: "realized_pnl", value: (row) => row.realizedPnl },
    { header: "status", value: (row) => row.status },
    { header: "display_status", value: (row) => row.displayStatus },
    { header: "opened_at", value: (row) => row.openedAt },
    { header: "opened_at_iso", value: (row) => iso(row.openedAt) },
    { header: "closed_at", value: (row) => row.closedAt },
    { header: "closed_at_iso", value: (row) => iso(row.closedAt) },
    { header: "settlement_result", value: (row) => row.settlementResult }
  ];
  return toCsv(rows, columns);
}

export function operatedRoundsCsv(user: ExportUser, rounds: Array<RoundRecord & { userPnl: number }>) {
  type Row = RoundRecord & { userPnl: number; user: ExportUser };
  const rows: Row[] = rounds.map((round) => ({ ...round, user }));
  const columns: CsvColumn<Row>[] = [
    ...userColumns<Row>(),
    { header: "round_id", value: (row) => row.id },
    { header: "market_id", value: (row) => row.marketId },
    { header: "symbol", value: (row) => row.symbol },
    { header: "market_slug", value: (row) => row.marketSlug },
    { header: "event_slug", value: (row) => row.eventSlug },
    { header: "condition_id", value: (row) => row.conditionId },
    { header: "title", value: (row) => row.title },
    { header: "start_at", value: (row) => row.startAt },
    { header: "start_at_iso", value: (row) => iso(row.startAt) },
    { header: "end_at", value: (row) => row.endAt },
    { header: "end_at_iso", value: (row) => iso(row.endAt) },
    { header: "price_to_beat", value: (row) => row.priceToBeat },
    { header: "status", value: (row) => row.status },
    { header: "poll_count", value: (row) => row.pollCount },
    { header: "closing_spot_price", value: (row) => row.closingSpotPrice },
    { header: "settled_side", value: (row) => row.settledSide },
    { header: "settlement_price", value: (row) => row.settlementPrice },
    { header: "settlement_ts", value: (row) => row.settlementTs },
    { header: "settlement_ts_iso", value: (row) => iso(row.settlementTs) },
    { header: "settlement_source", value: (row) => row.settlementSource },
    { header: "binance_open_price", value: (row) => row.binanceOpenPrice },
    { header: "binance_close_price", value: (row) => row.binanceClosePrice },
    { header: "coinbase_open_price", value: (row) => row.coinbaseOpenPrice },
    { header: "coinbase_close_price", value: (row) => row.coinbaseClosePrice },
    { header: "polymarket_open_price", value: (row) => row.polymarketOpenPrice },
    { header: "polymarket_close_price", value: (row) => row.polymarketClosePrice },
    { header: "accepting_orders", value: (row) => row.acceptingOrders },
    { header: "closing_price_source", value: (row) => row.closingPriceSource },
    { header: "manual_reason", value: (row) => row.manualReason },
    { header: "user_pnl", value: (row) => row.userPnl }
  ];
  return toCsv(rows, columns);
}

export function profileCsv(user: ExportUser, profile: ProfileOverview, generatedAt = Date.now()) {
  const row = { user, profile, generatedAt };
  return toCsv([row], [
    ...userColumns<typeof row>(),
    { header: "generated_at", value: (item) => item.generatedAt },
    { header: "generated_at_iso", value: (item) => iso(item.generatedAt) },
    { header: "total_equity", value: (item) => item.profile.totalEquity },
    { header: "available_usdc", value: (item) => item.profile.availableUsdc },
    { header: "position_value", value: (item) => item.profile.positionValue },
    { header: "realized_pnl_today", value: (item) => item.profile.realizedPnlToday },
    { header: "unrealized_pnl", value: (item) => item.profile.unrealizedPnl },
    { header: "win_rate", value: (item) => item.profile.winRate },
    { header: "rounds_participated_total", value: (item) => item.profile.roundsParticipatedTotal },
    { header: "rounds_participated_today", value: (item) => item.profile.roundsParticipatedToday }
  ]);
}

function safePathSegment(value: string) {
  const cleaned = value.trim().replace(/[<>:"/\\|?*\x00-\x1F]/g, "_").replace(/\.+/g, ".");
  return cleaned || "unknown";
}

export function resolveExportUsers(actor: UserRecord, allUsers: ExportUser[], targetUserId?: string) {
  const visibleUsers =
    actor.role === "Admin"
      ? allUsers
      : actor.role === "Senior Tester" || actor.role === "Test Engineer"
        ? allUsers.filter((user) => user.id === actor.id || (user.role === "Tester" && (user.managerUserId ?? user.seniorTesterId) === actor.id))
        : allUsers.filter((user) => user.id === actor.id);
  if (!targetUserId) {
    return visibleUsers;
  }
  const target = visibleUsers.find((user) => user.id === targetUserId);
  if (!target) {
    throw new Error("Logs are not available for this user.");
  }
  return [target];
}

function inRange(ts: number | undefined, query: ExportQuery) {
  if (typeof ts !== "number") {
    return false;
  }
  if (typeof query.from === "number" && ts < query.from) {
    return false;
  }
  if (typeof query.to === "number" && ts > query.to) {
    return false;
  }
  return true;
}

export function filterOrdersForExport(orders: OrderLifecycleRecord[], query: ExportQuery) {
  return orders.filter((order) => {
    if (query.roundId && order.roundId !== query.roundId) {
      return false;
    }
    if (query.marketId && order.marketId !== query.marketId) {
      return false;
    }
    if (query.marketSlug && order.marketSlug !== query.marketSlug) {
      return false;
    }
    if (query.traceId && order.traceId !== query.traceId) {
      return false;
    }
    if (query.direction && order.direction !== query.direction) {
      return false;
    }
    if (query.settlementResult && order.settlementResult !== query.settlementResult) {
      return false;
    }
    if (query.orderId && order.buyOrderId !== query.orderId && order.id !== query.orderId) {
      return false;
    }
    return inRange(order.orderTimestampMs, query);
  });
}

export function filterPositionsForExport(positions: PositionRecord[], query: ExportQuery) {
  return positions.filter((position) => {
    if (query.roundId && position.roundId !== query.roundId) {
      return false;
    }
    if (query.direction && position.side !== query.direction) {
      return false;
    }
    if (query.settlementResult && position.settlementResult !== query.settlementResult) {
      return false;
    }
    if (query.positionId && position.id !== query.positionId) {
      return false;
    }
    return inRange(position.closedAt ?? position.openedAt, query);
  });
}

export function filterRoundsForExport(rounds: Array<RoundRecord & { userPnl: number }>, query: ExportQuery) {
  return rounds.filter((round) => {
    if (query.roundId && round.id !== query.roundId) {
      return false;
    }
    if (query.marketId && round.marketId !== query.marketId) {
      return false;
    }
    if (query.marketSlug && round.marketSlug !== query.marketSlug) {
      return false;
    }
    if (query.symbol && round.symbol !== query.symbol) {
      return false;
    }
    if (query.roundStatus && round.status !== query.roundStatus) {
      return false;
    }
    if (query.direction && round.settledSide !== query.direction) {
      return false;
    }
    return inRange(round.startAt, query);
  });
}

export function buildExportEntries(input: {
  actor: ExportUser;
  users: UserExportData[];
  systemMatchingEvents: MatchingEventRecord[];
  systemMatchingActionLogs?: AuditEvent[];
  systemLatencyLogs: AuditEvent[];
  query: LogSearchQuery;
  generatedAt: number;
  dateLabel: string;
  singleUser: boolean;
}) {
  const root = `export-${input.dateLabel}`;
  const entries: ZipEntry[] = [];
  const files: ExportManifestFile[] = [];
  const addEntry = (
    entry: ZipEntry,
    meta: Omit<ExportManifestFile, "path">
  ) => {
    entries.push(entry);
    files.push({
      path: entry.path,
      ...meta
    });
  };

  for (const data of input.users) {
    const userDir = input.singleUser
      ? `${root}/${safePathSegment(data.user.username)}`
      : `${root}/users/${safePathSegment(data.user.username)}`;
    addEntry(
      { path: `${userDir}/audit_logs.csv`, content: auditLogsCsv(data.user, data.auditLogs) },
      { rowCount: data.auditLogs.length, system: "audit", userId: data.user.id }
    );
    addEntry(
      { path: `${userDir}/training_logs.csv`, content: trainingLogsCsv(data.user, data.trainingLogs) },
      { rowCount: data.trainingLogs.length, system: "training", userId: data.user.id }
    );
    addEntry(
      { path: `${userDir}/matching_events.csv`, content: matchingEventsCsv(data.user, data.matchingEvents) },
      { rowCount: data.matchingEvents.length, system: "matching", userId: data.user.id }
    );
    const matchingActionLogs = data.matchingActionLogs ?? [];
    addEntry(
      { path: `${userDir}/matching_actions.csv`, content: auditLogsCsv(data.user, matchingActionLogs) },
      { rowCount: matchingActionLogs.length, system: "matching_actions", userId: data.user.id }
    );
    addEntry(
      { path: `${userDir}/orders.csv`, content: ordersCsv(data.user, data.orders) },
      { rowCount: data.orders.length, system: "orders", userId: data.user.id }
    );
    addEntry(
      { path: `${userDir}/positions.csv`, content: positionsCsv(data.user, data.positions) },
      { rowCount: data.positions.length, system: "positions", userId: data.user.id }
    );
    addEntry(
      { path: `${userDir}/operated_rounds.csv`, content: operatedRoundsCsv(data.user, data.operatedRounds) },
      { rowCount: data.operatedRounds.length, system: "rounds", userId: data.user.id }
    );
    addEntry(
      { path: `${userDir}/profile.csv`, content: profileCsv(data.user, data.profile, input.generatedAt) },
      { rowCount: 1, system: "profile", userId: data.user.id }
    );
  }
  if (!input.singleUser && input.systemLatencyLogs.length > 0) {
    addEntry(
      { path: `${root}/system/latency.csv`, content: auditLogsCsv(undefined, input.systemLatencyLogs) },
      { rowCount: input.systemLatencyLogs.length, system: "audit" }
    );
  }
  if (!input.singleUser && input.systemMatchingEvents.length > 0) {
    addEntry(
      { path: `${root}/system/matching_events.csv`, content: matchingEventsCsv(undefined, input.systemMatchingEvents) },
      { rowCount: input.systemMatchingEvents.length, system: "matching" }
    );
  }
  const systemMatchingActionLogs = input.systemMatchingActionLogs ?? [];
  if (!input.singleUser && systemMatchingActionLogs.length > 0) {
    addEntry(
      { path: `${root}/system/matching_actions.csv`, content: auditLogsCsv(undefined, systemMatchingActionLogs) },
      { rowCount: systemMatchingActionLogs.length, system: "matching_actions" }
    );
  }
  const manifest: ExportManifest = {
    generatedAt: input.generatedAt,
    generatedAtIso: iso(input.generatedAt),
    query: input.query,
    actor: {
      id: input.actor.id,
      username: input.actor.username,
      role: input.actor.role
    },
    users: input.users.map((data) => ({
      id: data.user.id,
      username: data.user.username,
      role: data.user.role,
      anonId: data.user.anonId
    })),
    files,
    summary: {
      auditRows: files.filter((file) => file.system === "audit").reduce((sum, file) => sum + file.rowCount, 0),
      latencyRows: files
        .filter((file) => file.system === "audit")
        .reduce((sum, file) => sum + file.rowCount, 0),
      matchingActionRows: files
        .filter((file) => file.system === "matching_actions")
        .reduce((sum, file) => sum + file.rowCount, 0),
      matchingEngineRows: files
        .filter((file) => file.system === "matching")
        .reduce((sum, file) => sum + file.rowCount, 0)
    }
  };
  entries.unshift({
    path: `${root}/manifest.json`,
    content: `${JSON.stringify(manifest, null, 2)}\n`
  });
  return entries;
}

const crcTable = new Uint32Array(256).map((_, index) => {
  let crc = index;
  for (let bit = 0; bit < 8; bit += 1) {
    crc = crc & 1 ? 0xedb88320 ^ (crc >>> 1) : crc >>> 1;
  }
  return crc >>> 0;
});

function crc32(buffer: Buffer) {
  let crc = 0xffffffff;
  for (const byte of buffer) {
    crc = crcTable[(crc ^ byte) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function dosDateTime(date = new Date()) {
  const year = Math.max(date.getUTCFullYear(), 1980);
  const dosTime = (date.getUTCHours() << 11) | (date.getUTCMinutes() << 5) | Math.floor(date.getUTCSeconds() / 2);
  const dosDate = ((year - 1980) << 9) | ((date.getUTCMonth() + 1) << 5) | date.getUTCDate();
  return { dosDate, dosTime };
}

function u16(value: number) {
  const buffer = Buffer.alloc(2);
  buffer.writeUInt16LE(value & 0xffff, 0);
  return buffer;
}

function u32(value: number) {
  const buffer = Buffer.alloc(4);
  buffer.writeUInt32LE(value >>> 0, 0);
  return buffer;
}

export function createZipArchive(entries: ZipEntry[]) {
  const locals: Buffer[] = [];
  const centrals: Buffer[] = [];
  let offset = 0;
  const { dosDate, dosTime } = dosDateTime();

  for (const entry of entries) {
    const name = Buffer.from(entry.path.replace(/\\/g, "/"), "utf8");
    const data = Buffer.isBuffer(entry.content) ? entry.content : Buffer.from(entry.content, "utf8");
    const checksum = crc32(data);
    const local = Buffer.concat([
      u32(0x04034b50),
      u16(20),
      u16(0x0800),
      u16(0),
      u16(dosTime),
      u16(dosDate),
      u32(checksum),
      u32(data.length),
      u32(data.length),
      u16(name.length),
      u16(0),
      name,
      data
    ]);
    const central = Buffer.concat([
      u32(0x02014b50),
      u16(20),
      u16(20),
      u16(0x0800),
      u16(0),
      u16(dosTime),
      u16(dosDate),
      u32(checksum),
      u32(data.length),
      u32(data.length),
      u16(name.length),
      u16(0),
      u16(0),
      u16(0),
      u16(0),
      u32(0),
      u32(offset),
      name
    ]);
    locals.push(local);
    centrals.push(central);
    offset += local.length;
  }

  const centralDirectory = Buffer.concat(centrals);
  const end = Buffer.concat([
    u32(0x06054b50),
    u16(0),
    u16(0),
    u16(entries.length),
    u16(entries.length),
    u32(centralDirectory.length),
    u32(offset),
    u16(0)
  ]);
  return Buffer.concat([...locals, centralDirectory, end]);
}

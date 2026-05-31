import type { Language, LogFacets, LogSearchQuery, LogSystem, Role } from "../../utils/api";

export const LOG_EXPORT_SYSTEMS: Array<Exclude<LogSystem, "all">> = ["audit", "training", "matching"];
export const ROLE_OPTIONS: Role[] = ["Tester", "Senior Tester", "Test Engineer", "Admin"];
export const LANGUAGE_OPTIONS: Language[] = ["zh-CN", "en-US"];
export const ACTION_STATUS_OPTIONS: Array<NonNullable<LogSearchQuery["actionStatus"]>> = ["success", "failed", "timeout"];
export const LOG_GROUP_OPTIONS: Array<NonNullable<LogSearchQuery["logGroup"]>> = [
  "operation",
  "settlement",
  "market_latency",
  "system_latency",
  "matching_action"
];
export const LATENCY_SOURCE_OPTIONS: Array<NonNullable<LogSearchQuery["latencySource"]>> = ["binance", "coinbase", "clob", "system"];
export const CONNECTION_STATE_OPTIONS: Array<NonNullable<LogSearchQuery["connectionState"]>> = [
  "healthy",
  "reconnecting",
  "stale",
  "degraded",
  "disabled"
];
export const LATENCY_PHASE_OPTIONS: Array<NonNullable<LogSearchQuery["latencyPhase"]>> = ["backend", "acquire", "publish", "frontend"];
export const MATCHING_KIND_OPTIONS: Array<NonNullable<LogSearchQuery["matchingLogKind"]>> = ["action", "engine"];
export const MATCHING_EVENT_OPTIONS: Array<NonNullable<LogSearchQuery["eventType"]>> = [
  "external_book_synced",
  "order_executed",
  "order_cancelled"
];

export const DEFAULT_LOG_FACETS: LogFacets = {
  audit: {
    categories: ["operation", "matching", "settlement", "latency"],
    actionTypes: [
      "login",
      "switch_language",
      "place_order",
      "cancel_order",
      "sell_position",
      "close_side",
      "reverse_side",
      "limit_order_triggered",
      "limit_order_failed",
      "capture_price_to_beat",
      "poll_settlement",
      "settlement_confirmed",
      "redeem_position",
      "round_closed",
      "market_latency",
      "user.create",
      "user.bulkCreate",
      "user.disable",
      "user.enable",
      "user.resetPassword",
      "user.balance.set",
      "user.changePassword"
    ],
    fields: [
      "eventId",
      "traceId",
      "category",
      "actionType",
      "actionStatus",
      "userId",
      "role",
      "pageName",
      "moduleName",
      "roundId",
      "resultCode",
      "details"
    ],
    logGroups: LOG_GROUP_OPTIONS,
    latencySources: LATENCY_SOURCE_OPTIONS,
    connectionStates: CONNECTION_STATE_OPTIONS,
    latencyPhases: LATENCY_PHASE_OPTIONS
  },
  training: {
    actionTypes: [
      "place_order",
      "cancel_order",
      "sell_position",
      "close_side",
      "reverse_side",
      "limit_order_triggered",
      "limit_order_failed",
      "redeem_position"
    ],
    fields: [
      "logId",
      "timestampMs",
      "actionType",
      "actionStatus",
      "testerIdAnon",
      "roundId",
      "direction",
      "orderId",
      "marketId",
      "bookSnapshotEntry",
      "sourceStates",
      "contextJson"
    ]
  },
  matching: {
    eventTypes: MATCHING_EVENT_OPTIONS,
    fields: ["eventId", "bookKey", "roundId", "marketId", "bookSide", "sequence", "eventType", "orderId", "traceId", "payload"],
    kinds: MATCHING_KIND_OPTIONS
  }
};

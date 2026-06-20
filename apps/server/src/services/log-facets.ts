import type { LogCategory, MatchingEventType } from "../domain/types";

export const AUDIT_LOG_CATEGORIES: LogCategory[] = ["operation", "matching", "settlement", "latency"];

export const AUDIT_LOG_ACTION_TYPES = [
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
] as const;

export const TRAINING_LOG_ACTION_TYPES = [
  "place_order",
  "cancel_order",
  "sell_position",
  "close_side",
  "reverse_side",
  "limit_order_triggered",
  "limit_order_failed",
  "redeem_position"
] as const;

export const MATCHING_LOG_EVENT_TYPES: MatchingEventType[] = [
  "external_book_synced",
  "order_executed",
  "order_cancelled"
];

export const LOG_FACETS = {
  audit: {
    categories: AUDIT_LOG_CATEGORIES,
    actionTypes: AUDIT_LOG_ACTION_TYPES,
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
      "symbol",
      "roundId",
      "resultCode",
      "resultMessage",
      "serverRecvTs",
      "serverPublishTs",
      "backendLatencyMs",
      "logGroup",
      "latencySource",
      "connectionState",
      "latencyPhaseMetrics",
      "matchingLogKind",
      "details"
    ],
    logGroups: ["operation", "settlement", "market_latency", "system_latency", "matching_action"],
    latencySources: ["binance", "coinbase", "clob", "system"],
    connectionStates: ["healthy", "reconnecting", "stale", "degraded", "disabled"],
    latencyPhases: ["backend", "acquire", "publish", "frontend"]
  },
  training: {
    actionTypes: TRAINING_LOG_ACTION_TYPES,
    fields: [
      "logId",
      "timestampMs",
      "actionType",
      "actionStatus",
      "testerIdAnon",
      "roundId",
      "direction",
      "orderId",
      "traceId",
      "marketId",
      "marketSlug",
      "roundStatus",
      "entryOdds",
      "positionNotional",
      "actualFillPrice",
      "slippageBps",
      "settlementResult",
      "bookSnapshotEntry",
      "sourceStates",
      "contextJson"
    ]
  },
  matching: {
    eventTypes: MATCHING_LOG_EVENT_TYPES,
    fields: [
      "eventId",
      "bookKey",
      "roundId",
      "marketId",
      "bookSide",
      "sequence",
      "eventType",
      "orderId",
      "traceId",
      "payload",
      "createdAt"
    ],
    kinds: ["action", "engine"]
  }
};

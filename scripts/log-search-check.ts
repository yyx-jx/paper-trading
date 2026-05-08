import assert from "node:assert/strict";
import type { AuditEvent, BehaviorActionLog, MatchingBookState, MatchingEventRecord, Role } from "../apps/server/src/domain/types";
import { MatchingStore } from "../apps/server/src/services/matching/store";
import { AppStore } from "../apps/server/src/services/store";
import { LOG_FACETS } from "../apps/server/src/services/log-facets";

const store = new AppStore({
  initialBalance: 1000,
  logRetentionMs: 60_000,
  snapshotRetentionSeconds: 60,
  symbol: "BTC",
  databaseUrl: "",
  redisUrl: "",
  chainlinkEnabled: false
});

async function create(username: string, role: Role, seniorTesterId?: string) {
  return store.createUser({
    username,
    password: `${username}123`,
    displayName: username,
    role,
    language: "zh-CN",
    seniorTesterId,
    availableUsdc: 1000
  });
}

function audit(input: {
  id: string;
  ts: number;
  userId?: string;
  role?: Role;
  category?: AuditEvent["category"];
  actionType?: string;
  roundId?: string;
  details?: Record<string, unknown>;
}): AuditEvent {
  return {
    eventId: input.id,
    traceId: `trace-${input.id}`,
    category: input.category ?? "operation",
    actionType: input.actionType ?? "place_order",
    actionStatus: "success",
    userId: input.userId,
    role: input.role,
    pageName: "trade.main",
    moduleName: "order-entry",
    symbol: "BTC",
    roundId: input.roundId ?? "round-a",
    resultCode: "OK",
    resultMessage: "ok",
    serverRecvTs: input.ts,
    serverPublishTs: input.ts + 1,
    backendLatencyMs: 1,
    details: input.details ?? {}
  };
}

function behavior(input: {
  id: string;
  ts: number;
  userId: string;
  actionType?: string;
  roundId?: string;
  marketId?: string;
  direction?: "UP" | "DOWN";
}): BehaviorActionLog {
  return {
    logId: input.id,
    timestampMs: input.ts,
    assetClass: "BTC_5M_UPDOWN",
    actionType: input.actionType ?? "place_order",
    actionStatus: "success",
    roundId: input.roundId ?? "round-a",
    direction: input.direction ?? "UP",
    deltaClob: 0,
    volumeClob: 0,
    testerIdAnon: store.anonymizeUserId(input.userId),
    traceId: `trace-${input.id}`,
    orderId: `order-${input.id}`,
    marketId: input.marketId ?? "market-a",
    marketSlug: "market-slug-a",
    roundStatus: "Trading",
    binanceSpotPrice: 0,
    binance1mLastClose: 0,
    binance5mLastClose: 0,
    binance1dLastClose: 0,
    chainlinkPrice: 0,
    priceToBeat: 0,
    upPrice: 0,
    downPrice: 0,
    upBookTop5: [],
    downBookTop5: [],
    recentTradesTop20: [],
    bookSnapshotEntry: {
      snapshotId: "snapshot-a",
      snapshotTs: input.ts,
      topBids: [],
      topAsks: []
    },
    sourceStates: {
      binance: { source: "Binance", state: "disabled", sourceEventTs: 0, serverRecvTs: 0, serverPublishTs: 0 },
      chainlink: { source: "Chainlink", state: "disabled", sourceEventTs: 0, serverRecvTs: 0, serverPublishTs: 0 },
      clob: { source: "CLOB", state: "disabled", sourceEventTs: 0, serverRecvTs: 0, serverPublishTs: 0 }
    },
    settlementResult: "win",
    contextJson: { positionId: `pos-${input.id}` }
  };
}

function matchingState(sequence: number): MatchingBookState {
  return {
    bookKey: "book-a",
    roundId: "round-a",
    marketId: "market-a",
    bookSide: "UP",
    sequence,
    prioritySequence: sequence,
    snapshot: {
      snapshotId: `snapshot-${sequence}`,
      snapshotTs: 1_700_000_000_000 + sequence,
      bestBid: 0.49,
      bestAsk: 0.5,
      midPrice: 0.495,
      bids: [],
      asks: []
    },
    bids: [],
    asks: [],
    updatedAt: 1_700_000_000_000 + sequence
  };
}

async function main() {
  const senior = await create("log-senior", "Senior Tester");
  const tester = await create("log-tester", "Tester", senior.id);
  const other = await create("log-other", "Tester");
  const engineer = await create("log-engineer", "Test Engineer");
  assert.equal(engineer.permissionCodes.includes("logs:view:all"), false);
  assert.equal(engineer.permissionCodes.includes("logs:view:team"), true);

  store.logs.unshift(
    audit({
      id: "evt-a",
      ts: 1_700_000_000_300,
      userId: tester.id,
      role: "Tester",
      details: { orderId: "order-a", marketId: "market-a", direction: "UP", roundStatus: "Trading" }
    }),
    audit({
      id: "evt-b",
      ts: 1_700_000_000_200,
      userId: other.id,
      role: "Tester",
      category: "settlement",
      actionType: "settlement_confirmed",
      details: { marketId: "market-b", settlementResult: "loss" }
    }),
    audit({
      id: "evt-system",
      ts: 1_700_000_000_100,
      category: "latency",
      actionType: "market_latency",
      details: { marketId: "market-a" }
    })
  );
  store.behaviorLogs.unshift(
    behavior({ id: "blog-a", ts: 1_700_000_000_350, userId: tester.id, marketId: "market-a", direction: "UP" }),
    behavior({ id: "blog-b", ts: 1_700_000_000_250, userId: other.id, marketId: "market-b", direction: "DOWN" })
  );

  const auditByOrder = await store.searchAuditLogs({ orderId: "order-a" });
  assert.deepEqual(auditByOrder.map((event) => event.eventId), ["evt-a"]);
  const auditByMarket = await store.searchAuditLogs({ marketId: "market-b" });
  assert.deepEqual(auditByMarket.map((event) => event.eventId), ["evt-b"]);
  const auditRoleMatch = await store.searchAuditLogs({ userId: tester.id, role: "Tester" });
  assert.deepEqual(auditRoleMatch.map((event) => event.eventId), ["evt-a"]);
  const auditRoleMismatch = await store.searchAuditLogs({ userId: tester.id, role: "Admin" });
  assert.equal(auditRoleMismatch.length, 0);
  const auditPage = await store.searchAuditLogs({}, { limit: 1, offset: 1 });
  assert.deepEqual(auditPage.map((event) => event.eventId), ["evt-b"]);

  const teamTraining = await store.searchBehaviorLogs({ userIds: [senior.id, tester.id], marketId: "market-a" });
  assert.deepEqual(teamTraining.map((log) => log.logId), ["blog-a"]);
  const trainingRoleMatch = await store.searchBehaviorLogs({ userId: tester.id, role: "Tester" });
  assert.deepEqual(trainingRoleMatch.map((log) => log.logId), ["blog-a"]);
  const trainingRoleMismatch = await store.searchBehaviorLogs({ userId: tester.id, role: "Admin" });
  assert.equal(trainingRoleMismatch.length, 0);
  const directionTraining = await store.searchBehaviorLogs({ direction: "DOWN", settlementResult: "win" });
  assert.deepEqual(directionTraining.map((log) => log.logId), ["blog-b"]);
  const unsupportedTraining = await store.searchBehaviorLogs({ category: "latency" });
  assert.equal(unsupportedTraining.length, 0);
  assert.deepEqual(LOG_FACETS.audit.categories, ["operation", "matching", "settlement", "latency"]);
  assert.ok(LOG_FACETS.audit.actionTypes.includes("place_order"));
  assert.ok(LOG_FACETS.audit.actionTypes.includes("user.bulkCreate"));
  assert.ok(LOG_FACETS.training.actionTypes.includes("redeem_position"));
  assert.deepEqual(LOG_FACETS.matching.eventTypes, ["external_book_synced", "order_executed", "order_cancelled"]);

  const matching = new MatchingStore({
    databaseUrl: "",
    redisUrl: "",
    persistenceMode: "memory",
    redisSnapshotSeconds: 60,
    strictPersistence: false,
    pgConnectionTimeoutMs: 100,
    pgIdleTimeoutMs: 100,
    pgMaxConnections: 1,
    pgKeepAlive: false,
    pgReconnectIntervalMs: 100,
    pgReconnectMaxIntervalMs: 100,
    eventsMemoryMax: 100,
    eventsMemoryMaxAgeMs: 60_000,
    booksMemoryMax: 10
  });
  const userEvent: MatchingEventRecord = {
    eventId: "mevt-user",
    bookKey: "book-a",
    roundId: "round-a",
    marketId: "market-a",
    bookSide: "UP",
    sequence: 1,
    eventType: "order_executed",
    orderId: "order-a",
    traceId: "trace-a",
    payload: { request: { userId: tester.id }, status: "filled" },
    createdAt: 1_700_000_000_400
  };
  const systemEvent: MatchingEventRecord = {
    eventId: "mevt-system",
    bookKey: "book-a",
    roundId: "round-a",
    marketId: "market-a",
    bookSide: "UP",
    sequence: 2,
    eventType: "external_book_synced",
    payload: { sourceSnapshotId: "snapshot-system" },
    createdAt: 1_700_000_000_500
  };
  await matching.saveStep(userEvent, matchingState(1));
  await matching.saveStep(systemEvent, matchingState(2));
  const scopedMatching = await matching.searchEvents({ userIds: [tester.id], limit: 10 });
  assert.deepEqual(scopedMatching.map((event) => event.eventId), ["mevt-user"]);
  const pagedMatching = await matching.searchEvents({ bookKey: "book-a", limit: 1, offset: 1 });
  assert.deepEqual(pagedMatching.map((event) => event.eventId), ["mevt-user"]);

  console.log("log-search-check ok");
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

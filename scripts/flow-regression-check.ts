import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

type PositionRecord = {
  id: string;
  userId: string;
  roundId: string;
  side: "UP" | "DOWN";
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
  status: "open" | "closed";
  displayStatus?: "open" | "pending_settlement" | "settled" | "sold";
  openedAt: number;
  closedAt?: number;
  settlementResult?: "win" | "loss" | "sold";
};

type RoundStub = {
  id: string;
  marketId?: string;
  marketSlug?: string;
  conditionId?: string;
  symbol?: string;
  startAt: number;
  endAt: number;
  status: string;
  pollCount?: number;
  settledSide?: "UP" | "DOWN";
  settlementReceivedAt?: number;
  settlementSource?: "Polymarket" | "Gamma" | "Chainlink";
  redeemScheduledAt?: number;
  polymarketOpenPrice?: number;
  polymarketClosePrice?: number;
  binanceOpenPrice?: number;
  binanceClosePrice?: number;
};

function createStoreStub() {
  const store = Object.create((globalThis as { __AppStore: { prototype: object } }).__AppStore.prototype) as {
    users: Map<string, { id: string; availableUsdc: number }>;
    rounds: RoundStub[];
    positions: PositionRecord[];
    orders: Array<Record<string, unknown>>;
    logs: Array<Record<string, unknown>>;
    behaviorLogs: Array<Record<string, unknown>>;
    getRoundById: (roundId: string) => RoundStub | undefined;
    getOperatedHistory: (limit: number, userId: string) => Array<RoundStub & { userPnl: number }>;
  } & Record<string, unknown>;
  store.users = new Map();
  store.rounds = [];
  store.positions = [];
  store.orders = [];
  store.logs = [];
  store.behaviorLogs = [];
  store.getRoundById = function getRoundById(roundId: string) {
    return this.rounds.find((round) => round.id === roundId);
  };
  return store;
}

function createEngineStub() {
  return Object.create((globalThis as { __SimulationEngine: { prototype: object } }).__SimulationEngine.prototype) as Record<string, unknown>;
}

async function testPendingSettlementProfileIsolation() {
  const now = Date.now();
  const store = createStoreStub();
  store.users.set("u1", { id: "u1", availableUsdc: 100 });
  store.rounds.push(
    { id: "current-round", startAt: now - 60_000, endAt: now + 4 * 60_000, status: "Trading" },
    { id: "past-round", startAt: now - 10 * 60_000, endAt: now - 5 * 60_000, status: "Settling" }
  );
  store.positions.push(
    {
      id: "pos-current",
      userId: "u1",
      roundId: "current-round",
      side: "UP",
      qty: 10,
      averageEntry: 0.5,
      notionalSpent: 5,
      currentMark: 0.6,
      currentValue: 6,
      unrealizedPnl: 1,
      realizedPnl: 0,
      status: "open",
      openedAt: now - 30_000
    },
    {
      id: "pos-pending",
      userId: "u1",
      roundId: "past-round",
      side: "DOWN",
      qty: 8,
      averageEntry: 0.45,
      notionalSpent: 3.6,
      currentMark: 0.8,
      currentValue: 6.4,
      unrealizedPnl: 2.8,
      realizedPnl: 0,
      status: "open",
      openedAt: now - 8 * 60_000
    }
  );

  const positions = store.getPositions("u1") as PositionRecord[];
  const pendingPosition = positions.find((position) => position.id === "pos-pending");
  assert.equal(pendingPosition?.displayStatus, "pending_settlement");

  const profile = store.getProfile("u1") as {
    totalEquity: number;
    positionValue: number;
    unrealizedPnl: number;
    roundsParticipatedTotal: number;
    roundsParticipatedToday: number;
  };
  assert.equal(profile.positionValue, 6);
  assert.equal(profile.unrealizedPnl, 1);
  assert.equal(profile.roundsParticipatedTotal, 2);
  assert.equal(profile.roundsParticipatedToday, 2);

  const history = store.getHistory(10, "u1") as Array<{ id: string; userPnl: number }>;
  const pastRound = history.find((round) => round.id === "past-round");
  assert.equal(pastRound?.userPnl, 0);
}

async function testHistoryKeepsMarketOpenCloseFields() {
  const now = Date.now();
  const store = createStoreStub();
  store.rounds.push({
    id: "history-round",
    startAt: now - 10 * 60_000,
    endAt: now - 5 * 60_000,
    status: "Closed",
    polymarketOpenPrice: 77510.25,
    polymarketClosePrice: 77630.75,
    binanceOpenPrice: 77500.25,
    binanceClosePrice: 77620.75
  });

  const history = store.getHistory(10, "u1") as Array<RoundStub & { userPnl: number }>;
  assert.equal(history[0]?.polymarketOpenPrice, 77510.25);
  assert.equal(history[0]?.polymarketClosePrice, 77630.75);
  assert.equal(history[0]?.binanceOpenPrice, 77500.25);
  assert.equal(history[0]?.binanceClosePrice, 77620.75);
  assert.equal(history[0]?.userPnl, 0);
}

async function testOperatedHistoryReturnsUserRoundsOutsideRecentHistory() {
  const now = Date.now();
  const store = createStoreStub();
  store.rounds.push(
    { id: "recent-round", startAt: now - 5 * 60_000, endAt: now, status: "Closed" },
    { id: "older-operated-round", startAt: now - 60 * 60_000, endAt: now - 55 * 60_000, status: "Closed" }
  );
  store.orders.push({
    id: "older-order",
    userId: "u1",
    roundId: "older-operated-round",
    createdAt: now - 58 * 60_000
  });
  store.orders.push({
    id: "missing-round-order",
    userId: "u1",
    roundId: "btc-updown-5m-1777000000",
    symbol: "BTC",
    marketId: "missing-market",
    marketSlug: "btc-updown-5m-1777000000",
    createdAt: 1777000000 * 1000
  });
  store.positions.push({
    id: "older-position",
    userId: "u1",
    roundId: "older-operated-round",
    side: "UP",
    qty: 10,
    averageEntry: 0.4,
    notionalSpent: 4,
    currentMark: 1,
    unrealizedPnl: 0,
    realizedPnl: 6,
    status: "closed",
    openedAt: now - 58 * 60_000,
    closedAt: now - 56 * 60_000,
    settlementResult: "win"
  });

  const recentHistory = store.getHistory(1, "u1") as Array<RoundStub & { userPnl: number }>;
  assert.equal(recentHistory.some((round) => round.id === "older-operated-round"), false);

  const operatedHistory = store.getOperatedHistory(10, "u1");
  assert.equal(operatedHistory.length, 2);
  assert.equal(operatedHistory[0]?.id, "older-operated-round");
  assert.equal(operatedHistory[0]?.userPnl, 6);
  assert.equal(operatedHistory[1]?.id, "btc-updown-5m-1777000000");
  assert.equal(operatedHistory[1]?.startAt, 1777000000 * 1000);
}

async function testRefreshOpenPositionsScopesToActiveRound() {
  const now = Date.now();
  const engine = createEngineStub();
  engine.store = {
    positions: [
      {
        id: "current-open",
        userId: "u1",
        roundId: "current-round",
        side: "UP",
        qty: 10,
        averageEntry: 0.5,
        notionalSpent: 5,
        currentMark: 0.5,
        currentValue: 5,
        unrealizedPnl: 0,
        realizedPnl: 0,
        status: "open",
        openedAt: now - 30_000
      },
      {
        id: "past-open",
        userId: "u1",
        roundId: "past-round",
        side: "UP",
        qty: 10,
        averageEntry: 0.5,
        notionalSpent: 5,
        currentMark: 0.4,
        currentValue: 4,
        unrealizedPnl: -1,
        realizedPnl: 0,
        status: "open",
        openedAt: now - 8 * 60_000
      }
    ]
  };
  engine.getActiveRound = () => ({ id: "current-round" });

  const changedUsers = engine.refreshOpenPositions({
    upPrice: 0.7,
    downPrice: 0.3,
    serverNow: now,
    orderBooks: {
      UP: { snapshotId: "up-1", snapshotTs: now - 1000, bestBid: 0.69, bestAsk: 0.71, midPrice: 0.7, bids: [], asks: [] },
      DOWN: { snapshotId: "down-1", snapshotTs: now - 1000, bestBid: 0.29, bestAsk: 0.31, midPrice: 0.3, bids: [], asks: [] }
    }
  });

  const [currentPosition, pastPosition] = engine.store.positions as PositionRecord[];
  assert.deepEqual([...changedUsers], ["u1"]);
  assert.equal(currentPosition.currentMark, 0.7);
  assert.equal(currentPosition.currentValue, 7);
  assert.equal(currentPosition.unrealizedPnl, 2);
  assert.equal(pastPosition.currentMark, 0.4);
  assert.equal(pastPosition.currentValue, 4);
  assert.equal(pastPosition.unrealizedPnl, -1);
}

async function testRedeemWritesWalletPositionAndAudit() {
  const now = Date.now();
  const user = {
    id: "u1",
    username: "tester",
    password: "tester123",
    displayName: "Tester",
    role: "Tester",
    language: "zh-CN",
    permissionCodes: [],
    availableUsdc: 100,
    createdAt: now - 60_000
  };
  const position: PositionRecord = {
    id: "pos-1",
    userId: user.id,
    roundId: "round-1",
    side: "UP",
    qty: 12,
    averageEntry: 0.55,
    notionalSpent: 6.6,
    currentMark: 0.52,
    currentValue: 6.24,
    unrealizedPnl: -0.36,
    realizedPnl: 0,
    status: "open",
    openedAt: now - 2 * 60_000
  };

  const auditEvents: Array<{ actionType: string; userId?: string }> = [];
  const behaviorLogs: Array<{ actionType: string }> = [];
  const persistedUsers: string[] = [];
  const persistedPositions: string[] = [];
  const settledLifecycles: Array<{ userId: string; roundId: string; side: string; settlementResult: string }> = [];
  const emittedUsers: string[] = [];

  const engine = createEngineStub();
  engine.store = {
    positions: [position],
    getUserById: (userId: string) => (userId === user.id ? user : undefined),
    persistUser: async (nextUser: { id: string }) => persistedUsers.push(nextUser.id),
    persistPosition: async (nextPosition: { id: string }) => persistedPositions.push(nextPosition.id),
    settleOpenOrderLifecycles: async (input: { userId: string; roundId: string; side: string; settlementResult: string }) =>
      settledLifecycles.push({
        userId: input.userId,
        roundId: input.roundId,
        side: input.side,
        settlementResult: input.settlementResult
      }),
    emitUserPayload: (userId: string) => emittedUsers.push(userId),
    newId: (prefix: string) => `${prefix}-1`,
    newTraceId: () => "trace-1"
  };
  engine.captureActionSnapshot = () => ({
    marketId: "m1",
    orderBooks: {
      UP: { snapshotId: "up", snapshotTs: now, bestBid: 0.99, bestAsk: 1, midPrice: 1, bids: [], asks: [] },
      DOWN: { snapshotId: "down", snapshotTs: now, bestBid: 0, bestAsk: 0.01, midPrice: 0, bids: [], asks: [] }
    },
    upPrice: 1,
    downPrice: 0,
    serverNow: now,
    binance: { candlesByInterval: { "1m": [], "5m": [], "1d": [] } },
    clob: {
      upBook: { snapshotId: "up", snapshotTs: now, bestBid: 0.99, bestAsk: 1, midPrice: 1, bids: [], asks: [] },
      downBook: { snapshotId: "down", snapshotTs: now, bestBid: 0, bestAsk: 0.01, midPrice: 0, bids: [], asks: [] },
      recentTrades: [],
      delta: 0,
      volume: 0,
      bestBidAskSummary: {
        UP: { bestBid: 0.99, bestAsk: 1 },
        DOWN: { bestBid: 0, bestAsk: 0.01 }
      }
    },
    recentTrades: [],
    sources: {},
    uiMeta: { marketTitle: "", countdownMs: 0, acceptingOrders: false, sourceStatusSummary: [] },
    symbol: "BTC",
    binancePrice: 0,
    chainlinkPrice: 0,
    currentPrice: 0,
    priceToBeat: 0,
    candles: [],
    chainlink: { referencePrice: 0, settlementReference: 0 }
  });
  engine.createBehaviorLog = (input: { actionType: string }) => input;
  engine.writeBehaviorLog = async (log: { actionType: string }) => behaviorLogs.push(log);
  engine.writeAuditLog = async (event: { actionType: string; userId?: string }) => auditEvents.push(event);

  const round = {
    id: "round-1",
    symbol: "BTC",
    settledSide: "UP",
    pollCount: 3,
    settlementTs: now - 3000
  };

  await engine.applyRedeem(round);

  assert.equal(user.availableUsdc, 112);
  assert.equal(position.status, "closed");
  assert.equal(position.currentValue, 12);
  assert.equal(position.unrealizedPnl, 0);
  assert.equal(position.realizedPnl, 5.4);
  assert.equal(position.settlementResult, "win");
  assert.deepEqual(settledLifecycles, [{ userId: user.id, roundId: "round-1", side: "UP", settlementResult: "win" }]);
  assert.equal(round.status, "Closed");
  assert.ok(round.redeemFinishTs);
  assert.ok(auditEvents.some((event) => event.actionType === "redeem_position" && event.userId === user.id));
  assert.ok(behaviorLogs.some((log) => log.actionType === "redeem_position"));
  assert.deepEqual(persistedUsers, [user.id]);
  assert.deepEqual(persistedPositions, [position.id]);
  assert.deepEqual(emittedUsers, [user.id]);
}

async function testAuditSearchAndTimelineAggregation() {
  const now = Date.now();
  const store = createStoreStub();
  store.rounds.push({ id: "round-1", startAt: now - 60_000, endAt: now + 60_000, status: "Trading" });
  store.orders.push({
    id: "order-1",
    traceId: "trace-1",
    userId: "u1",
    roundId: "round-1",
    symbol: "BTC",
    marketId: "market-1",
    action: "buy",
    side: "UP",
    status: "filled",
    marketSlug: "btc-updown",
    notionalUsdc: 10,
    expectedQty: 20,
    filledQty: 20,
    unfilledQty: 0,
    bestBid: 0.49,
    bestAsk: 0.51,
    midPrice: 0.5,
    bookSnapshotTs: now,
    partialFilled: false,
    matchLatencyMs: 2,
    serverRecvTs: now,
    serverPublishTs: now,
    createdAt: now
  });
  store.positions.push({
    id: "pos-1",
    userId: "u1",
    roundId: "round-1",
    side: "UP",
    qty: 20,
    averageEntry: 0.5,
    notionalSpent: 10,
    currentMark: 0.52,
    currentValue: 10.4,
    unrealizedPnl: 0.4,
    realizedPnl: 0,
    status: "open",
    openedAt: now
  });
  store.logs.push(
    {
      eventId: "evt-1",
      traceId: "trace-1",
      category: "matching",
      actionType: "place_order",
      actionStatus: "success",
      userId: "u1",
      role: "Tester",
      pageName: "trade.main",
      moduleName: "order.panel",
      roundId: "round-1",
      resultCode: "ORDER_FILLED",
      resultMessage: "filled",
      serverRecvTs: now,
      serverPublishTs: now,
      backendLatencyMs: 1,
      details: { orderId: "order-1", positionId: "pos-1", bookSnapshotId: "book-1" }
    },
    {
      eventId: "evt-2",
      traceId: "trace-2",
      category: "matching",
      actionType: "place_order",
      actionStatus: "success",
      userId: "u2",
      role: "Tester",
      pageName: "trade.main",
      moduleName: "order.panel",
      roundId: "round-1",
      resultCode: "ORDER_FILLED",
      resultMessage: "other",
      serverRecvTs: now,
      serverPublishTs: now,
      backendLatencyMs: 1,
      details: { orderId: "order-2" }
    }
  );
  store.behaviorLogs.push({
    logId: "blog-1",
    timestampMs: now,
    actionType: "place_order",
    actionStatus: "success",
    testerIdAnon: store.anonymizeUserId("u1"),
    traceId: "trace-1",
    orderId: "order-1",
    marketId: "market-1",
    marketSlug: "btc-updown",
    contextJson: { positionId: "pos-1", bookSnapshotId: "book-1" }
  });

  const auditLogs = store.getAuditLogs({ userId: "u1", orderId: "order-1" });
  const behaviorLogs = store.getBehaviorLogs({ userId: "u1", actionStatus: "success", marketSlug: "btc-updown" });
  const timeline = store.getTradeTimeline("order-1");

  assert.equal(auditLogs.length, 1);
  assert.equal(auditLogs[0]?.details?.bookSnapshotId, "book-1");
  assert.equal(behaviorLogs.length, 1);
  assert.equal(timeline?.order.id, "order-1");
  assert.equal(timeline?.position?.id, "pos-1");
  assert.equal(timeline?.auditEvents.length, 1);
  assert.equal(timeline?.behaviorLogs.length, 1);
}

async function testSettlementPollIsScheduledOffFastPath() {
  const engine = createEngineStub();
  const events: string[] = [];
  const round = {
    id: "round-background-poll",
    status: "Polling",
    pollCount: 0
  };

  engine.pollLocks = new Set<string>();
  engine.roundSignature = (target: { status: string; pollCount: number }) => `${target.status}:${target.pollCount}`;
  engine.pollSettlement = async (target: { status: string; pollCount: number }) => {
    await new Promise((resolve) => setTimeout(resolve, 30));
    target.status = "Settled";
    target.pollCount = 1;
  };
  engine.collectRoundPositionUsers = () => ["u1"];
  engine.scheduleReconcile = () => events.push("reconcile");
  engine.store = {
    upsertRound: async () => events.push("upsert"),
    emitUserPayload: (userId: string) => events.push(`emit:${userId}`)
  };

  const startedAt = Date.now();
  engine.scheduleSettlementPoll(round, Date.now());
  assert.ok(Date.now() - startedAt < 10, "settlement poll scheduling should not block the fast path");
  assert.deepEqual(events, []);

  await new Promise((resolve) => setTimeout(resolve, 80));
  assert.deepEqual(events, ["upsert", "emit:u1", "reconcile"]);
}

async function testResolvedEventSchedulesTwoSecondRedeem() {
  const now = Date.now();
  const round: RoundStub = {
    id: "btc-updown-5m-123",
    marketId: "market-1",
    marketSlug: "btc-updown-5m-123",
    conditionId: "condition-1",
    symbol: "BTC",
    startAt: now - 6 * 60_000,
    endAt: now - 60_000,
    status: "Manual",
    pollCount: 60
  };
  const events: string[] = [];
  const engine = createEngineStub();
  engine.store = {
    rounds: [round],
    upsertRound: async (nextRound: RoundStub) => events.push(`upsert:${nextRound.status}`),
    getRoundById: (roundId: string) => (roundId === round.id ? round : undefined)
  };
  engine.polymarketState = {
    lastResolvedMarket: {
      marketId: "market-1",
      marketSlug: "btc-updown-5m-123",
      conditionId: "condition-1",
      settledSide: "DOWN",
      settlementPrice: 0,
      receivedAt: now
    }
  };
  engine.binanceState = { price: 0 };
  engine.hydrateRoundPolymarketReferencePrices = async () => undefined;
  engine.writeSettlementLog = async (target: RoundStub, status: string) => events.push(`settlement:${status}:${target.status}`);
  engine.collectRoundPositionUsers = () => [];

  await engine.processRounds();

  assert.equal(round.settledSide, "DOWN");
  assert.equal(round.settlementSource, "Polymarket");
  assert.equal(round.settlementReceivedAt, now);
  assert.equal(round.redeemScheduledAt, now + 2000);
  assert.equal(round.status, "Redeeming");
  assert.deepEqual(events, ["settlement:success:Settled", "upsert:Redeeming"]);
}

async function testFrontendLatencyUsesReceiptTimestamp() {
  const appSource = readFileSync("apps/client/src/App.tsx", "utf8");
  assert.doesNotMatch(appSource, /function latencyFor\(source\?: SourceHealth, now = Date\.now\(\), clientRecvTs = now\)/);
  assert.match(
    appSource,
    /Math\.max\(clientRecvTs - source\.serverPublishTs,\s*0\)/
  );
  assert.doesNotMatch(appSource, /source\?\.clientRecvTs \?\? props\.clientRecvTs \?\? props\.nowMs/);
  assert.doesNotMatch(appSource, /sourceClob\?\.clientRecvTs \?\? props\.lastMarketRecvTs \?\? nowMs/);
  assert.doesNotMatch(appSource, /nowMs - orderBook\.snapshotTs/);
  assert.match(appSource, /sourceToBackendLatencyMs: Math\.max\(source\.acquireLatencyMs,\s*0\)/);
  assert.doesNotMatch(appSource, /sourceToBackendLatencyMs: Math\.max\(source\.serverRecvTs - source\.sourceEventTs,\s*0\)/);
  assert.match(appSource, /const endToEndAlert =/);
  assert.match(appSource, /latency\.endToEndLatencyMs > 3000/);
  assert.match(appSource, /source-latency-alert/);
  assert.match(appSource, /const marketStaleMs = 15000/);
  assert.match(appSource, /const marketPayloadRejectMs = 5000/);
  assert.match(appSource, /extractMarketPayloadPublishTs/);
  assert.match(appSource, /const marketReconnectStaleMs = 45000/);
  assert.match(appSource, /scheduleMarketReconnect/);
  assert.match(appSource, /refreshMarketSnapshot/);
  assert.match(appSource, /api\.getCurrentRound\(token\)/);
  assert.match(appSource, /Polymarket BTC 开\/收/);
  assert.match(appSource, /Δ Open \/ Δ Close/);
  assert.match(appSource, /lastMarketRecvTs/);
  assert.match(appSource, /function isBtcReferencePrice\(value\?: number\): value is number/);
  assert.match(appSource, /isBtcReferencePrice\(round\.polymarketOpenPrice\)/);
  const storeSource = readFileSync("apps/client/src/store/useAppStore.ts", "utf8");
  assert.match(storeSource, /stampSnapshotReceipt/);
  assert.match(storeSource, /clientRecvTs/);
  assert.match(storeSource, /lastMarketPayloadSeq/);
  assert.match(storeSource, /lastMarketServerPublishTs/);
  assert.match(storeSource, /shouldAcceptMarketPayload/);
  assert.match(storeSource, /transportMeta\.serverPublishTs/);
  const i18nSource = readFileSync("apps/client/src/i18n/index.ts", "utf8");
  assert.match(i18nSource, /dataAge: "源数据距今"/);
  assert.match(i18nSource, /dataAge: "Source Data Age"/);
  assert.match(i18nSource, /endToEnd: "源数据到前端"/);
  assert.match(i18nSource, /endToEnd: "Source Data to Frontend"/);
  assert.match(i18nSource, /latencyOver3s: "延迟超过 3 秒"/);
  assert.match(i18nSource, /latencyOver3s: "Latency over 3s"/);
  assert.doesNotMatch(i18nSource, /源事件到前端/);
  assert.doesNotMatch(i18nSource, /Source Event/);
}

async function testChainlinkAcquireLatencyUsesRpcDuration() {
  const { buildChainlinkHealthyStatus } = require("../apps/server/src/services/connectors/chainlink.ts") as {
    buildChainlinkHealthyStatus: (input: {
      symbol: string;
      rpcUrl: string;
      reconnectCount: number;
      updatedAt: number;
      requestStartTs: number;
      requestFinishTs: number;
    }) => {
      sourceEventTs: number;
      serverRecvTs: number;
      normalizedTs: number;
      serverPublishTs: number;
      acquireLatencyMs: number;
      message?: string;
    };
  };
  const status = buildChainlinkHealthyStatus({
    symbol: "BTC",
    rpcUrl: "https://rpc.example/mainnet",
    reconnectCount: 2,
    updatedAt: 1_700_000_000_000,
    requestStartTs: 1_700_000_300_000,
    requestFinishTs: 1_700_000_300_048
  });

  assert.equal(status.sourceEventTs, 1_700_000_000_000);
  assert.equal(status.serverRecvTs, 1_700_000_300_048);
  assert.equal(status.normalizedTs, 1_700_000_300_048);
  assert.equal(status.serverPublishTs, 1_700_000_300_048);
  assert.equal(status.acquireLatencyMs, 48);
  assert.notEqual(status.acquireLatencyMs, status.serverRecvTs - status.sourceEventTs);
  assert.match(status.message ?? "", /rpc\.example/);
}

async function testProfileUsesOperatedGroupedRoundViews() {
  const appSource = readFileSync("apps/client/src/App.tsx", "utf8");
  const apiSource = readFileSync("apps/client/src/utils/api.ts", "utf8");
  const stylesSource = readFileSync("apps/client/src/styles.css", "utf8");
  const indexSource = readFileSync("apps/server/src/index.ts", "utf8");
  const storeSource = readFileSync("apps/server/src/services/store.ts", "utf8");
  assert.match(appSource, /type OperatedEquityWindow = 10 \| 30 \| 60 \| "all"/);
  assert.match(appSource, /function buildOperatedHistory\(history: HistoryRound\[\], orders: OrderRecord\[\]\)/);
  assert.match(appSource, /function buildGroupedPositions\(history: HistoryRound\[\], positions: PositionRecord\[\], orders: OrderRecord\[\]\)/);
  assert.match(appSource, /function buildGroupedOrders\(history: HistoryRound\[\], orders: OrderRecord\[\]\)/);
  assert.match(appSource, /const \[equityWindow, setEquityWindow\] = useState<OperatedEquityWindow>\(30\)/);
  assert.match(appSource, /api\.getOperatedHistory\(token\)/);
  assert.match(appSource, /function datedRoundTimeRangeText\(round: Pick<RoundRecord, "startAt" \| "endAt">\)/);
  assert.match(appSource, /const equityCurve = buildEquityCurve\(props\.operatedHistory, props\.orders, equityWindow\)/);
  assert.match(appSource, /let runningProfit = 0/);
  assert.match(appSource, /runningProfit \+= round\.userPnl/);
  assert.doesNotMatch(appSource, /baselineEquity/);
  assert.doesNotMatch(appSource, /profile\?\.totalEquity.*cumulativePnl/);
  assert.match(appSource, /const roundCalendarItems = buildRoundCalendarItems\(props\.operatedHistory, props\.orders\)/);
  assert.match(appSource, /datedLabel: datedRoundTimeRangeText\(round\)/);
  assert.match(appSource, /function FastEquityCurve/);
  assert.match(appSource, /const width = 1080/);
  assert.match(appSource, /const height = 420/);
  assert.match(appSource, /<FastEquityCurve points=\{equityCurve\} minValue=\{curveDomainMin\} maxValue=\{curveDomainMax\} \/>/);
  assert.match(appSource, /handlePointerMove/);
  assert.match(appSource, /onMouseMove=\{handlePointerMove\}/);
  assert.match(appSource, /hover-tooltip/);
  assert.match(appSource, /hoverPoint\.datedLabel/);
  assert.match(appSource, /<title>/);
  assert.match(appSource, /first\.datedLabel/);
  assert.match(appSource, /last\.datedLabel/);
  assert.match(appSource, /<strong>\{item\.datedLabel\}<\/strong>/);
  assert.match(appSource, /roundsParticipatedTotal/);
  assert.match(stylesSource, /--workspace-width: 1920px/);
  assert.match(stylesSource, /width: var\(--workspace-width\)/);
  assert.match(stylesSource, /min-width: var\(--workspace-width\)/);
  assert.match(stylesSource, /grid-template-columns: 1120px 1fr/);
  assert.match(stylesSource, /height: 420px/);
  assert.match(stylesSource, /profile-fast-curve \.hover-tooltip/);
  assert.doesNotMatch(stylesSource, /@media \(max-width: 1440px\)/);
  assert.doesNotMatch(stylesSource, /@media \(max-width: 920px\)/);
  assert.doesNotMatch(appSource, /from "recharts"/);
  assert.doesNotMatch(appSource, /<ResponsiveContainer/);
  assert.doesNotMatch(appSource, /<LineChart/);
  assert.match(appSource, /const displayProfilePositionGroups = paginateRows\(groupedPositions, profilePositionsPageSafe\)/);
  assert.match(appSource, /const displayProfileOrderGroups = paginateRows\(groupedOrders, profileOrdersPageSafe\)/);
  assert.match(appSource, /api\.getRoundActivity\(token, item\.roundId\)/);
  assert.match(appSource, /behaviorLogs: \[\.\.\.activity\.behaviorLogs\]/);
  assert.match(appSource, /state\.behaviorLogs\.map/);
  assert.match(appSource, /source: "Behavior"/);
  assert.doesNotMatch(appSource, /const equityCurve = buildEquityCurve\(props\.history, props\.profile\)/);
  assert.match(apiSource, /getOperatedHistory\(token: string, limit = 500\)/);
  assert.match(apiSource, /getRoundActivity\(token: string, roundId: string\)/);
  assert.match(indexSource, /\/api\/logs\/round-activity/);
  assert.match(indexSource, /behaviorLogs: store\.getBehaviorLogs\(\{ userId: user\.id, roundId: parsed\.roundId \}\)/);
  assert.match(apiSource, /roundsParticipatedTotal\?: number/);
  assert.match(indexSource, /\/api\/profile\/rounds\/operated/);
  assert.match(storeSource, /getOperatedHistory\(limit = 500, userId: string\)/);
  assert.match(storeSource, /roundsParticipatedTotal/);
}

async function testSettlementUsesResolvedQueueAndFiveSecondGammaPolling() {
  const simulationSource = readFileSync("apps/server/src/services/simulation.ts", "utf8");
  const connectorSource = readFileSync("apps/server/src/services/connectors/polymarket.ts", "utf8");
  const typesSource = readFileSync("apps/server/src/domain/types.ts", "utf8");
  const configSource = readFileSync("apps/server/src/config.ts", "utf8");
  const apiSource = readFileSync("apps/client/src/utils/api.ts", "utf8");
  const appSource = readFileSync("apps/client/src/App.tsx", "utf8");

  assert.match(typesSource, /resolvedMarkets\?: PolymarketResolvedMarket\[\]/);
  assert.match(typesSource, /export interface SettlementPreview/);
  assert.match(typesSource, /state: "preliminary" \| "confirmed" \| "manual"/);
  assert.match(connectorSource, /resolvedMarkets: \[\]/);
  assert.match(connectorSource, /resolvedMarkets: \[\.\.\.\(this\.state\.resolvedMarkets \?\? \[\]\), resolvedEvent\]\.slice\(-20\)/);
  assert.match(configSource, /export function buildServerConfig\(env: NodeJS\.ProcessEnv = process\.env\)/);
  assert.match(configSource, /pollDelayMs: Number\(env\.POLL_DELAY_MS \?\? 120000\)/);
  assert.match(simulationSource, /const PRELIMINARY_SETTLEMENT_THRESHOLD = 0\.97/);
  assert.match(simulationSource, /now - round\.lastPollAt < this\.config\.gammaPollIntervalMs/);
  assert.doesNotMatch(simulationSource, /HOT_SETTLEMENT_POLL_MS/);
  assert.doesNotMatch(simulationSource, /HOT_SETTLEMENT_WINDOW_MS/);
  assert.doesNotMatch(simulationSource, /MANUAL_SETTLEMENT_RETRY_MS/);
  assert.match(simulationSource, /findResolvedMarketForRound\(round\)/);
  assert.match(simulationSource, /refreshPreliminarySettlement\(round, now\)/);
  assert.match(simulationSource, /this\.getPreliminarySettlements\(\)\.delete\(round\.id\)/);
  assert.match(apiSource, /export interface SettlementPreview/);
  assert.match(apiSource, /settlementPreview\?: SettlementPreview/);
  assert.match(appSource, /settlementPreviewLabel/);
  assert.match(appSource, /settlementPreviewHelpText/);
  assert.match(appSource, /settlement-preview-note/);
  assert.doesNotMatch(appSource, /status-pill tone-\$\{settlementPreviewTone\(settlementPreview\)\}/);
  assert.match(appSource, /Manual Review/);
  assert.match(appSource, /Preliminary/);
}

async function testBackendTransportStampingKeepsLatencySeparateFromAge() {
  const indexSource = readFileSync("apps/server/src/index.ts", "utf8");
  assert.match(indexSource, /function stampSnapshotForTransport\(snapshot: MarketSnapshot, serverPublishTs = Date\.now\(\)\)/);
  assert.match(indexSource, /serverPublishTs,/);
  assert.match(indexSource, /MarketTransportMeta/);
  assert.match(indexSource, /payloadSeq: marketPayloadSeq/);
  assert.match(indexSource, /snapshot: stampSnapshotForTransport\(store\.marketSnapshot, transportMeta\.serverPublishTs\)/);
  assert.match(indexSource, /bufferedAmount > 0/);
  assert.match(indexSource, /pendingLatest/);
  assert.match(indexSource, /lastSentSeq/);
  assert.doesNotMatch(indexSource, /snapshot: store\.marketSnapshot/);
}

async function testPolymarketReferencePricesDoNotUseOutcomeOdds() {
  const simulationSource = readFileSync("apps/server/src/services/simulation.ts", "utf8");
  assert.doesNotMatch(simulationSource, /getRoundUpMarketPrice/);
  assert.doesNotMatch(simulationSource, /getRoundPolymarketBtcReference/);
  assert.match(simulationSource, /hydrateRoundPolymarketReferencePrices/);
  assert.match(simulationSource, /PolymarketReferenceResolver/);
  assert.match(simulationSource, /resolveBoundaryPrice\(round\.startAt/);
  assert.match(simulationSource, /resolveBoundaryPrice\(round\.endAt/);
  assert.match(simulationSource, /isBtcReferencePrice\(existing\?\.polymarketOpenPrice\)/);
  assert.match(simulationSource, /if \(!isBtcReferencePrice\(round\.polymarketOpenPrice\)\)/);

  const connectorSource = readFileSync("apps/server/src/services/connectors/polymarket.ts", "utf8");
  assert.match(connectorSource, /isBtcPrice\(value: number\)/);
  assert.match(connectorSource, /value > 1000/);
  assert.match(connectorSource, /outcomePrices:\s*\[toFloat\(outcomePrices\[0\]\), toFloat\(outcomePrices\[1\]\)\]/);
  assert.match(connectorSource, /referenceOpenPrice/);
  assert.match(connectorSource, /referenceClosePrice/);

  const storeSource = readFileSync("apps/server/src/services/store.ts", "utf8");
  assert.match(storeSource, /sanitizePolymarketBtcReference/);
  assert.match(storeSource, /polymarketOpenPrice = sanitizePolymarketBtcReference/);
}

async function testPolymarketMarketSelectionUsesSlugTime() {
  const connector = Object.create((globalThis as { __PolymarketConnector: { prototype: object } }).__PolymarketConnector.prototype) as Record<string, unknown>;
  connector.config = {
    symbol: "BTC",
    seriesSlug: "btc-up-or-down-5m"
  };
  connector.state = {};
  const now = Date.now();
  const currentStart = Math.floor(now / (5 * 60_000)) * 5 * 60_000;
  const payloadFor = (startAt: number, slugPrefix = "btc-updown-5m") => ({
    id: `market-${startAt}`,
    conditionId: `condition-${startAt}`,
    slug: `${slugPrefix}-${Math.floor(startAt / 1000)}`,
    question: "Bitcoin Up or Down - 5 Minute",
    endDate: new Date(startAt + 60 * 60_000).toISOString(),
    resolutionSource: "https://data.chain.link/streams/btc-usd",
    acceptingOrders: true,
    closed: false,
    outcomes: JSON.stringify(["Up", "Down"]),
    outcomePrices: JSON.stringify(["0.5", "0.5"]),
    clobTokenIds: JSON.stringify([`up-${startAt}`, `down-${startAt}`]),
    events: [
      {
        id: `event-${startAt}`,
        slug: `${slugPrefix}-${Math.floor(startAt / 1000)}`,
        title: "Bitcoin Up or Down",
        seriesSlug: "btc-up-or-down-5m",
        startTime: new Date(startAt - 60 * 60_000).toISOString()
      }
    ]
  });

  const current = connector.toMarketDetail(payloadFor(currentStart));
  const next = connector.toMarketDetail(payloadFor(currentStart + 5 * 60_000));
  const later = connector.toMarketDetail(payloadFor(currentStart + 20 * 60_000));
  const nonBtc = connector.toMarketDetail(payloadFor(currentStart, "eth-updown-5m"));

  assert.equal(current.startAt, currentStart);
  assert.equal(current.endAt, currentStart + 5 * 60_000);
  assert.equal(connector.matchesDetail(current), true);
  assert.equal(connector.matchesDetail(nonBtc), false);

  const selected = connector.selectTrackedMarkets([current, next, later]);
  assert.equal(selected.currentMarket.slug, current.slug);
  assert.equal(selected.nextMarket.slug, next.slug);

  const futureOnly = connector.selectTrackedMarkets([later]);
  assert.equal(futureOnly.currentMarket, undefined);
}

async function testChainlinkReferenceResolverHelpers() {
  const {
    parseChainlinkStreamMetadataFromHtml,
    parseChainlinkCandlestickSamples,
    pickFirstSampleAtOrAfter
  } = require("../apps/server/src/services/connectors/polymarket-reference.ts") as {
    parseChainlinkStreamMetadataFromHtml: (html: string, fallbackStreamUrl?: string) => {
      feedId: string;
      schema: string;
      streamSlug: string;
      streamUrl: string;
    };
    parseChainlinkCandlestickSamples: (
      candlestick?: string,
      bucket?: string
    ) => Array<{ ts: number; value: number; kind: "open" | "close"; bucket?: string }>;
    pickFirstSampleAtOrAfter: (
      samples: Array<{ ts: number; value: number; kind: "open" | "close"; bucket?: string }>,
      boundaryTs: number,
      maxSkewMs?: number
    ) => { ts: number; value: number; kind: "open" | "close"; bucket?: string } | undefined;
  };

  const html = `
    <html>
      <body>
        <script id="__NEXT_DATA__" type="application/json">
          {"query":{"slug":"btc-usd-cexprice-streams"},"props":{"pageProps":{"streamData":{"streamMetadata":{"feedId":"0xfeed","docs":{"schema":"v3"}}}}}}
        </script>
      </body>
    </html>
  `;
  const metadata = parseChainlinkStreamMetadataFromHtml(html, "https://data.chain.link/streams/btc-usd");
  assert.equal(metadata.feedId, "0xfeed");
  assert.equal(metadata.schema, "v3");
  assert.equal(metadata.streamSlug, "btc-usd-cexprice-streams");
  assert.equal(metadata.streamUrl, "https://data.chain.link/streams/btc-usd");

  const samples = parseChainlinkCandlestickSamples(
    '(version:1,open:(ts:"2026-04-26 03:16:00.538449+00",val:77545.92057),high:(ts:"2026-04-26 03:16:28.541437+00",val:77557.5963),low:(ts:"2026-04-26 03:16:01.102151+00",val:77545.91801),close:(ts:"2026-04-26 03:16:59.559728+00",val:77580.07448),volume:Missing())',
    "2026-04-26T03:16:00+00:00"
  );
  assert.equal(samples.length, 2);
  assert.equal(samples[0]?.kind, "open");
  assert.equal(samples[1]?.kind, "close");
  assert.equal(samples[0]?.value, 77545.92057);
  assert.equal(samples[1]?.value, 77580.07448);

  const boundaryTs = Date.parse("2026-04-26T03:16:30.000Z");
  const picked = pickFirstSampleAtOrAfter(samples, boundaryTs, 60_000);
  assert.equal(picked?.kind, "close");
  assert.equal(picked?.value, 77580.07448);
}

async function main() {
  const { AppStore } = (require("../apps/server/src/services/store.ts") as {
    AppStore: new (...args: never[]) => unknown;
  });
  const { SimulationEngine } = (require("../apps/server/src/services/simulation.ts") as {
    SimulationEngine: new (...args: never[]) => unknown;
  });
  const { PolymarketConnector } = (require("../apps/server/src/services/connectors/polymarket.ts") as {
    PolymarketConnector: new (...args: never[]) => unknown;
  });
  (globalThis as { __AppStore: typeof AppStore }).__AppStore = AppStore;
  (globalThis as { __SimulationEngine: typeof SimulationEngine }).__SimulationEngine = SimulationEngine;
  (globalThis as { __PolymarketConnector: typeof PolymarketConnector }).__PolymarketConnector = PolymarketConnector;

  await testPendingSettlementProfileIsolation();
  await testHistoryKeepsMarketOpenCloseFields();
  await testOperatedHistoryReturnsUserRoundsOutsideRecentHistory();
  await testRefreshOpenPositionsScopesToActiveRound();
  await testRedeemWritesWalletPositionAndAudit();
  await testAuditSearchAndTimelineAggregation();
  await testSettlementPollIsScheduledOffFastPath();
  await testResolvedEventSchedulesTwoSecondRedeem();
  await testFrontendLatencyUsesReceiptTimestamp();
  await testChainlinkAcquireLatencyUsesRpcDuration();
  await testProfileUsesOperatedGroupedRoundViews();
  await testSettlementUsesResolvedQueueAndFiveSecondGammaPolling();
  await testBackendTransportStampingKeepsLatencySeparateFromAge();
  await testPolymarketReferencePricesDoNotUseOutcomeOdds();
  await testPolymarketMarketSelectionUsesSlugTime();
  await testChainlinkReferenceResolverHelpers();

  console.log("flow-regression-check ok");
}

void main();

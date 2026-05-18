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
  chainlinkOpenPrice?: number;
  chainlinkClosePrice?: number;
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
  const engine = Object.create((globalThis as { __SimulationEngine: { prototype: object } }).__SimulationEngine.prototype) as Record<string, unknown>;
  engine.config = { chainlinkEnabled: false };
  engine.chainlinkState = { price: 0 };
  return engine;
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
    binanceClosePrice: 77620.75,
    chainlinkOpenPrice: 77495.5,
    chainlinkClosePrice: 77615.5
  });

  const history = store.getHistory(10, "u1") as Array<RoundStub & { userPnl: number }>;
  assert.equal(history[0]?.polymarketOpenPrice, 77510.25);
  assert.equal(history[0]?.polymarketClosePrice, 77630.75);
  assert.equal(history[0]?.binanceOpenPrice, 77500.25);
  assert.equal(history[0]?.binanceClosePrice, 77620.75);
  assert.equal(history[0]?.chainlinkOpenPrice, 77495.5);
  assert.equal(history[0]?.chainlinkClosePrice, 77615.5);
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
  const claimedRedeems: string[] = [];
  const settledLifecycles: Array<{ userId: string; roundId: string; side: string; settlementResult: string }> = [];
  const emittedUsers: string[] = [];

  const engine = createEngineStub();
  engine.store = {
    positions: [position],
    withTransaction: async (handler: () => Promise<unknown>) => handler(),
    claimRedeemLedger: async (input: { roundId: string; userId: string; positionId: string }) => {
      claimedRedeems.push(`${input.roundId}:${input.userId}:${input.positionId}`);
      return true;
    },
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
    upsertRound: async () => undefined,
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
    binance: { candlesByInterval: { "30s": [], "1m": [], "5m": [], "15m": [], "1h": [], "1d": [] } },
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
    chainlink: {
      referencePrice: 0,
      settlementReference: 0,
      candles5s: [],
      candlesByInterval: { "30s": [], "1m": [], "5m": [], "15m": [], "1h": [], "1d": [] }
    }
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
  assert.deepEqual(claimedRedeems, ["round-1:u1:pos-1"]);
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
  assert.deepEqual(events, ["settlement:success:Redeeming", "upsert:Redeeming"]);
}

async function testFrontendLatencyUsesReceiptTimestamp() {
  const appSource = readFileSync("apps/client/src/App.tsx", "utf8");
  assert.doesNotMatch(appSource, /function latencyFor\(source\?: SourceHealth, now = Date\.now\(\), clientRecvTs = now\)/);
  assert.match(
    appSource,
    /Math\.max\(clientRecvTs - source\.serverPublishTs - clientClockOffsetMs,\s*0\)/
  );
  assert.match(appSource, /function transportAgeMs\(receivedAt: number, publishTs: number, clientClockOffsetMs = 0\)/);
  assert.match(appSource, /transportAgeMs\(receivedAt, publishTs, clientClockOffsetMsRef\.current\)/);
  assert.match(appSource, /transportAgeMs\(pending\.receivedAt, publishTs, clientClockOffsetMsRef\.current\)/);
  assert.match(appSource, /clientClockOffsetMs=\{clientClockOffsetMsRef\.current\}/);
  assert.doesNotMatch(appSource, /source\?\.clientRecvTs \?\? props\.clientRecvTs \?\? props\.nowMs/);
  assert.doesNotMatch(appSource, /sourceClob\?\.clientRecvTs \?\? props\.lastMarketRecvTs \?\? nowMs/);
  assert.doesNotMatch(appSource, /nowMs - orderBook\.snapshotTs/);
  assert.match(appSource, /sourceToBackendLatencyMs: Math\.max\(source\.acquireLatencyMs,\s*0\)/);
  assert.doesNotMatch(appSource, /sourceToBackendLatencyMs: Math\.max\(source\.serverRecvTs - source\.sourceEventTs,\s*0\)/);
  assert.match(appSource, /const topLatency = \[\.\.\.latencyRows\]/);
  assert.match(appSource, /monitor-latency-breakdown/);
  assert.match(appSource, /latency-mini-list/);
  assert.match(appSource, /const marketStaleMs = 3000/);
  assert.match(appSource, /const marketPayloadRejectMs = 5000/);
  assert.match(appSource, /type: "market" \| "market:tick"/);
  assert.match(appSource, /setMarketTickPayload/);
  assert.match(appSource, /requestAnimationFrame/);
  assert.match(appSource, /markMarketRenderCommit/);
  assert.match(appSource, /extractMarketPayloadPublishTs/);
  assert.match(appSource, /const marketReconnectStaleMs = 10000/);
  assert.match(appSource, /window\.setInterval\(\(\) => setNowMs\(Date\.now\(\)\), 250\)/);
  assert.match(appSource, /transitionRealtimeChannel/);
  assert.match(appSource, /REALTIME_STATUS_MIN_HOLD_MS = 1500/);
  assert.match(appSource, /MARKET_LIVE_RECOVERY_PAYLOADS = 2/);
  assert.match(appSource, /snapshot\.uiMeta\.countdownTargetTs \+ clientClockOffsetMsRef\.current/);
  assert.doesNotMatch(appSource, /lastMarketRecvTs \+ snapshot\.uiMeta\.countdownMs/);
  assert.match(appSource, /countdownTargetMs\?: number/);
  assert.match(appSource, /api\.sampleClockOffset/);
  assert.match(appSource, /scheduleMarketReconnect/);
  assert.match(appSource, /refreshMarketSnapshot/);
  assert.match(appSource, /api\.getCurrentRound\(token, activeViewUserId\)/);
  assert.match(appSource, /polymarketOpenPrice/);
  assert.match(appSource, /polymarketClosePrice/);
  assert.match(appSource, /lastMarketRecvTs/);
  assert.match(appSource, /function isBtcReferencePrice\(value\?: number\): value is number/);
  assert.match(appSource, /isBtcReferencePrice\(round\.polymarketOpenPrice\)/);
  const storeSource = readFileSync("apps/client/src/store/useAppStore.ts", "utf8");
  assert.match(storeSource, /stampSnapshotReceipt/);
  assert.match(storeSource, /clientRecvTs/);
  assert.match(storeSource, /lastMarketPayloadSeq/);
  assert.match(storeSource, /lastMarketServerPublishTs/);
  assert.match(storeSource, /shouldAcceptMarketPayload/);
  assert.match(storeSource, /mergeRealtimeTick/);
  assert.match(storeSource, /setMarketTickPayload/);
  assert.match(storeSource, /lastMarketRenderLatencyMs/);
  assert.match(storeSource, /transportMeta\.serverPublishTs/);
  assert.match(storeSource, /clientRecvTs - transportMeta\.serverPublishTs - clientClockOffsetMs/);
  const i18nSource = readFileSync("apps/client/src/i18n/index.ts", "utf8");
  assert.match(i18nSource, /dataAge:/);
  assert.match(i18nSource, /dataAge: "Data Age"/);
  assert.match(i18nSource, /endToEnd:/);
  assert.match(i18nSource, /endToEnd: "Source to Frontend"/);
  assert.match(i18nSource, /latencyOver3s:/);
  assert.match(i18nSource, /latencyOver3s: "Latency over 3s"/);
  assert.doesNotMatch(i18nSource, /源事件到前端/);
  assert.doesNotMatch(i18nSource, /Source Event/);
  assert.doesNotMatch(i18nSource, /\uFFFD/);
  assert.doesNotMatch(i18nSource, /锟|鍒|涓|寰|绛|鐧|瀵|妯|杞/);
}

async function testChainlinkDisplayUsesStrictRtds() {
  const chainlinkSource = readFileSync("apps/server/src/services/connectors/chainlink.ts", "utf8");
  const configSource = readFileSync("apps/server/src/config.ts", "utf8");
  const appSource = readFileSync("apps/client/src/App.tsx", "utf8");

  assert.match(configSource, /CHAINLINK_RTDS_WS_URL/);
  assert.match(configSource, /wss:\/\/ws-live-data\.polymarket\.com/);
  assert.match(chainlinkSource, /topic: "crypto_prices_chainlink"/);
  assert.match(chainlinkSource, /filters: JSON\.stringify\(\{ symbol: this\.rtdsSymbol \}\)/);
  assert.match(chainlinkSource, /this\.ws\.send\("PING"\)/);
  assert.match(chainlinkSource, /Strict RTDS mode does not fall back to AggregatorV3/);
  assert.doesNotMatch(chainlinkSource, /createPublicClient/);
  assert.doesNotMatch(chainlinkSource, /latestRoundData/);
  assert.match(appSource, /sourceChainlink/);
  assert.match(appSource, /ChainLink VS PTB/);
}

async function testProfileUsesOperatedGroupedRoundViews() {
  const appSource = readFileSync("apps/client/src/App.tsx", "utf8");
  const apiSource = readFileSync("apps/client/src/utils/api.ts", "utf8");
  const stylesSource = readFileSync("apps/client/src/styles.css", "utf8");
  const indexSource = readFileSync("apps/server/src/index.ts", "utf8");
  const storeSource = readFileSync("apps/server/src/services/store.ts", "utf8");
  assert.match(appSource, /type AnalyticsPeriod = "all" \| "year" \| "month" \| "week" \| "day" \| "trades"/);
  assert.match(appSource, /type AnalyticsResult = "WIN" \| "LOSE" \| "SOLD" \| "OPEN" \| "UNFILLED"/);
  assert.match(appSource, /interface AnalyticsTradeRow/);
  assert.match(apiSource, /export interface OrderLifecycleRecord/);
  assert.match(apiSource, /orderLifecycles: OrderLifecycleRecord\[\]/);
  assert.match(appSource, /function buildAnalyticsRows\(\s*history: HistoryRound\[\],\s*positions: PositionRecord\[\],\s*orders: OrderRecord\[\],\s*orderLifecycles: OrderLifecycleRecord\[\],\s*language: Language\s*\)/);
  assert.match(appSource, /positionNotional/);
  assert.match(appSource, /actualFillPrice/);
  assert.match(appSource, /exitNotional/);
  assert.match(appSource, /function filterAnalyticsPeriod\(rows: AnalyticsTradeRow\[\], period: AnalyticsPeriod\)/);
  assert.match(appSource, /function analyticsSummary\(rows: AnalyticsTradeRow\[\]\)/);
  assert.doesNotMatch(appSource, /function analyticsConclusion/);
  assert.match(appSource, /function AnalyticsPage/);
  assert.match(appSource, /<AnalyticsPage/);
  assert.match(appSource, /api\.getOperatedHistory\(token, 500, effectiveViewUserId\)/);
  assert.match(appSource, /function inferAnalyticsRoundStartAt/);
  assert.match(appSource, /Math\.floor\(fallbackTs \/ \(5 \* 60_000\)\) \* \(5 \* 60_000\)/);
  assert.doesNotMatch(appSource, /roundLabel: analyticsRoundLabel\(round\?\.endAt, log\.orderTimestampMs\)/);
  assert.match(appSource, /roundLabel: analyticsRoundLabel\(roundStartAt\)/);
  assert.match(appSource, /dateTimeText\(row\.ts\)/);
  assert.match(appSource, /HT-\$\{dateTimeText\(roundStartAt\)\}/);
  assert.match(appSource, /analysisText: analysis\.text/);
  assert.match(appSource, /settlementState: "UNSETTLED"/);
  assert.match(appSource, /ANALYTICS_INITIAL_TRADE_LIMIT = 200/);
  assert.match(appSource, /\{ id: "all", label: analyticsPeriodLabel\("all", language\) \}/);
  assert.match(appSource, /\{ id: "trades", label: analyticsPeriodLabel\("trades", language\) \}/);
  assert.match(appSource, /<select value="BTC" disabled>/);
  assert.match(appSource, /<option value="WIN">\{analyticsResultLabel\("WIN", language\)\}<\/option>/);
  assert.match(appSource, /<option value="LOSE">\{analyticsResultLabel\("LOSE", language\)\}<\/option>/);
  assert.match(appSource, /<option value="SOLD">\{analyticsResultLabel\("SOLD", language\)\}<\/option>/);
  assert.match(appSource, /<option value="OPEN">\{analyticsResultLabel\("OPEN", language\)\}<\/option>/);
  assert.match(appSource, /<option value="UNFILLED">\{analyticsResultLabel\("UNFILLED", language\)\}<\/option>/);
  assert.match(appSource, /isClobDepthFailure\(order\)/);
  assert.match(appSource, /row\.result !== "OPEN" && row\.result !== "UNFILLED"/);
  assert.doesNotMatch(appSource, /props\.t\("accountSecurity"\)/);
  assert.match(appSource, /analyticsSettlementLabel\(row\.settlementState, language\)/);
  assert.match(appSource, /analyticsResultLabel\(row\.result, language\)/);
  assert.match(appSource, /analytics-row-analysis/);
  assert.match(appSource, /function isManualSettlementPermissionError\(message: string\)/);
  assert.match(appSource, /if \(!isManualSettlementPermissionError\(message\)\) \{\s*setError\(message\);/s);
  assert.doesNotMatch(appSource, /analyticsTimelineKey/);
  assert.doesNotMatch(appSource, /analyticsTimelineLabel/);
  assert.doesNotMatch(appSource, /groupVisibleLimits/);
  assert.match(appSource, /analytics-table-panel/);
  assert.match(appSource, /analytics-table-wrap/);
  assert.doesNotMatch(appSource, /function ProfilePage/);
  assert.doesNotMatch(appSource, /const \[equityWindow, setEquityWindow\]/);
  assert.doesNotMatch(appSource, /<FastEquityCurve/);
  assert.doesNotMatch(appSource, /baselineEquity/);
  assert.match(stylesSource, /analytics-terminal-page/);
  assert.match(stylesSource, /analytics-summary/);
  assert.match(stylesSource, /analytics-period-tabs/);
  assert.match(stylesSource, /analytics-table-panel/);
  assert.match(stylesSource, /analytics-table-wrap/);
  assert.match(stylesSource, /analytics-row-analysis/);
  assert.match(stylesSource, /analytics-load-more/);
  assert.doesNotMatch(stylesSource, /analytics-day-group/);
  assert.doesNotMatch(stylesSource, /analytics-day-head/);
  assert.doesNotMatch(stylesSource, /analytics-security/);
  assert.match(stylesSource, /app-shell:not\(\.page-trade\)/);
  assert.doesNotMatch(stylesSource, /@media \(max-width: 1440px\)/);
  assert.doesNotMatch(stylesSource, /@media \(max-width: 920px\)/);
  assert.doesNotMatch(appSource, /from "recharts"/);
  assert.doesNotMatch(appSource, /<ResponsiveContainer/);
  assert.doesNotMatch(appSource, /<LineChart/);
  assert.match(appSource, /api\.getRoundActivity\(token, item\.roundId, effectiveViewUserId\)/);
  assert.match(appSource, /behaviorLogs: \[\.\.\.activity\.behaviorLogs\]/);
  assert.match(appSource, /state\.behaviorLogs\.map/);
  assert.match(appSource, /source: "Behavior"/);
  assert.doesNotMatch(appSource, /const equityCurve = buildEquityCurve\(props\.history, props\.profile\)/);
  assert.match(apiSource, /getOperatedHistory\(token: string, limit = 500, viewUserId\?: string\)/);
  assert.match(apiSource, /getRoundActivity\(token: string, roundId: string, viewUserId\?: string\)/);
  assert.match(indexSource, /\/api\/logs\/round-activity/);
  assert.match(indexSource, /behaviorLogs: store\.getBehaviorLogs\(\{ userId: viewedUser\.id, roundId: parsed\.roundId \}\)/);
  assert.match(apiSource, /roundsParticipatedTotal\?: number/);
  assert.match(indexSource, /\/api\/profile\/rounds\/operated/);
  assert.match(indexSource, /\/api\/users\/:id\/group/);
  assert.match(indexSource, /actionType: "user\.group\.update"/);
  assert.match(indexSource, /Only Admin can change user groups/);
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
  assert.match(configSource, /pollDelayMs: Number\(env\.POLL_DELAY_MS \?\? 0\)/);
  assert.match(simulationSource, /const PRELIMINARY_SETTLEMENT_THRESHOLD = 0\.9/);
  assert.match(simulationSource, /const GAMMA_PREFETCH_START_MS = 180_000/);
  assert.match(simulationSource, /const GAMMA_PREFETCH_FAST_START_MS = 60_000/);
  assert.match(simulationSource, /const GAMMA_PREFETCH_END_MS = 0/);
  assert.match(simulationSource, /const GAMMA_PREFETCH_INTERVAL_MS = 2000/);
  assert.match(simulationSource, /this\.shouldPrefetchGamma\(round, now\)/);
  assert.match(simulationSource, /fetchBestGammaSettlementDetail\(round, now\)/);
  assert.match(simulationSource, /Promise\.allSettled\(tasks\)/);
  assert.match(simulationSource, /resolveTrustedGammaSettlement\(round, detail, now\)/);
  assert.match(simulationSource, /confirmGammaSettlement\(round, detail, settlement\.side, settlement\.price, now\)/);
  assert.match(simulationSource, /confirmExactGammaOutcome\(round\.id, exactOutcomeSide, now\)/);
  assert.match(simulationSource, /function resolvePairedDisplayPrices/);
  assert.match(simulationSource, /input\.upBook\.asks\.length > 0 && isPositivePrice\(input\.upBook\.bestAsk\)/);
  assert.match(simulationSource, /input\.downBook\.asks\.length > 0 && isPositivePrice\(input\.downBook\.bestAsk\)/);
  assert.match(simulationSource, /if \(!upAskDepthAvailable && !downAskDepthAvailable\)/);
  assert.match(simulationSource, /UP: \{ value: 0, source: "outcome_price", spread: 0 \}/);
  assert.match(simulationSource, /DOWN: \{ value: 0\.01, source: "outcome_price", spread: 0 \}/);
  assert.match(simulationSource, /const upPrice = isPositivePrice\(upBook\.bestAsk\) \? upBook\.bestAsk : 0/);
  assert.match(appSource, /BINANCE VS PTB/);
  assert.match(appSource, /ChainLink VS PTB/);
  assert.match(appSource, /spreadToneClass\(binancePtbSpread\)/);
  assert.match(appSource, /spreadToneClass\(chainlinkPtbSpread\)/);
  assert.match(appSource, /return spread > 0 \? "terminal-green" : "terminal-red";/);
  assert.doesNotMatch(appSource, /B5/);
  assert.doesNotMatch(appSource, /Binance 对比 CL/);
  assert.doesNotMatch(appSource, /Binance vs CL/);
  assert.match(appSource, /function parseLimitPriceCentsInput\(value: string\)/);
  assert.match(appSource, /\^\\d\+\$/);
  assert.match(appSource, /parsed < 1 \|\| parsed > 99/);
  assert.match(appSource, /limitPriceCents! \/ 100/);
  assert.match(appSource, /min=\{1\}/);
  assert.match(appSource, /max=\{99\}/);
  assert.match(appSource, /commitChartVisibleDraft/);
  assert.match(appSource, /event\.key === "Enter"/);
  assert.match(appSource, /parseBarCountInput\(chartVisibleDraft\)/);
  assert.match(simulationSource, /this\.scheduleRedeem\(round, now\)/);
  assert.match(simulationSource, /this\.publishSettlementMarketSnapshot\(round, "redeem_completed"\)/);
  assert.match(simulationSource, /SETTLEMENT_DUPLICATE_SKIPPED/);
  assert.match(simulationSource, /now - round\.lastPollAt < pollIntervalMs/);
  assert.doesNotMatch(simulationSource, /HOT_SETTLEMENT_POLL_MS/);
  assert.doesNotMatch(simulationSource, /HOT_SETTLEMENT_WINDOW_MS/);
  assert.doesNotMatch(simulationSource, /MANUAL_SETTLEMENT_RETRY_MS/);
  assert.match(simulationSource, /findResolvedMarketForRound\(round\)/);
  assert.match(simulationSource, /refreshPreliminarySettlement\(round, now\)/);
  assert.match(simulationSource, /this\.getPreliminarySettlements\(\)\.delete\(round\.id\)/);
  assert.match(apiSource, /export interface SettlementPreview/);
  assert.match(apiSource, /settlementPreview\?: SettlementPreview/);
  assert.match(appSource, /function preliminarySideFromRound\(round: RoundRecord\)/);
  assert.match(appSource, /function recentRoundOutcome/);
  assert.match(appSource, /preview\?\.state === "preliminary"/);
  const clientStoreSource = readFileSync("apps/client/src/store/useAppStore.ts", "utf8");
  assert.match(clientStoreSource, /function shouldAcceptMarketPayload/);
  assert.match(clientStoreSource, /transportMeta\.payloadSeq > state\.lastMarketPayloadSeq/);
  assert.match(appSource, /function spreadDisplayText/);
  assert.match(appSource, /return "--";/);
  assert.match(appSource, /title=\{title\}/);
  assert.match(appSource, /round\.settledSide/);
  assert.doesNotMatch(appSource, /status-pill tone-\$\{settlementPreviewTone\(settlementPreview\)\}/);
  assert.match(appSource, /Manual Review/);
  const i18nSource = readFileSync("apps/client/src/i18n/index.ts", "utf8");
  assert.match(i18nSource, /preliminary: "Preliminary"/);
  assert.match(i18nSource, /manualReview: "Manual Review"/);
}

async function testBackendTransportStampingKeepsLatencySeparateFromAge() {
  const indexSource = readFileSync("apps/server/src/index.ts", "utf8");
  assert.match(indexSource, /function stampSnapshotForTransport\(snapshot: MarketSnapshot, serverPublishTs = Date\.now\(\)\)/);
  assert.match(indexSource, /serverPublishTs,/);
  assert.match(indexSource, /MarketTransportMeta/);
  assert.match(indexSource, /payloadSeq: marketPayloadSeq/);
  assert.match(indexSource, /snapshot: stampSnapshotForTransport\(store\.marketSnapshot, transportMeta\.serverPublishTs\)/);
  assert.match(indexSource, /bufferedAmount > 0/);
  assert.match(indexSource, /function markTransportSendStart\(transportMeta: MarketTransportMeta\)/);
  assert.match(indexSource, /serverQueueMs = Math\.max\(sendStartedAt - transportMeta\.serverPublishTs, 0\)/);
  assert.doesNotMatch(indexSource, /marketBroadcastPendingSince/);
  assert.match(indexSource, /marketBroadcastCoalescedCount/);
  assert.match(indexSource, /MarketBroadcastFrame/);
  assert.match(indexSource, /market:tick/);
  assert.match(indexSource, /wsSendStartTs/);
  assert.doesNotMatch(indexSource, /snapshot: store\.marketSnapshot/);
}

async function testRealtimeLatencyPacingContracts() {
  const indexSource = readFileSync("apps/server/src/index.ts", "utf8");
  const storeSource = readFileSync("apps/server/src/services/store.ts", "utf8");
  const simulationSource = readFileSync("apps/server/src/services/simulation.ts", "utf8");
  const appSource = readFileSync("apps/client/src/App.tsx", "utf8");
  const typesSource = readFileSync("apps/server/src/domain/types.ts", "utf8");
  const apiSource = readFileSync("apps/client/src/utils/api.ts", "utf8");
  const configSource = readFileSync("apps/server/src/config.ts", "utf8");

  assert.match(configSource, /marketWsMinIntervalMs: Number\(env\.MARKET_WS_MIN_INTERVAL_MS \?\? 50\)/);
  assert.match(configSource, /marketSnapshotIntervalMs: Number\(env\.MARKET_SNAPSHOT_INTERVAL_MS \?\? 500\)/);
  assert.match(configSource, /marketHistoryCacheMaxUsers: Number\(env\.MARKET_HISTORY_CACHE_MAX_USERS \?\? 200\)/);
  assert.match(configSource, /polymarketBookCalibrationMs: Number\(env\.POLYMARKET_BOOK_CALIBRATION_MS \?\? env\.POLYMARKET_BOOK_POLL_MS \?\? 5000\)/);
  assert.match(typesSource, /coalescedCount\?: number/);
  assert.match(apiSource, /serverQueueMs\?: number/);
  assert.match(indexSource, /const MARKET_WS_MIN_INTERVAL_MS = Math\.max\(serverConfig\.marketWsMinIntervalMs, 0\)/);
  assert.match(indexSource, /const MARKET_WS_FULL_SNAPSHOT_STAGGER_MS = 40/);
  assert.match(indexSource, /const MARKET_WS_INITIAL_FULL_SNAPSHOT_SLOTS = Math\.max/);
  assert.match(indexSource, /const marketHistoryCache = new Map/);
  assert.match(indexSource, /store\.getHistoryRevision\(\)/);
  assert.match(indexSource, /createMarketPayload\(client\.viewedUserId\)/);
  assert.match(indexSource, /createMarketTickPayload\(coalescedCount\)/);
  assert.match(indexSource, /elapsedSinceLastSend < MARKET_WS_MIN_INTERVAL_MS/);
  assert.match(indexSource, /marketBroadcastTickTimer = setInterval/);
  assert.match(indexSource, /scheduleFullSnapshotForClient/);
  assert.match(indexSource, /initialFullSnapshotDelayMs\(client\)/);
  assert.match(indexSource, /clearInterval\(marketBroadcastTickTimer\)/);
  assert.match(indexSource, /clearTimeout\(client\.fullTimer\)/);
  assert.match(storeSource, /this\.emitter\.emit\("market:update", snapshot\)/);
  assert.match(storeSource, /this\.queuedMarketSnapshot = snapshot/);
  assert.match(storeSource, /void this\.flushMarketSnapshotCache\(\)/);
  assert.match(storeSource, /await this\.persistMarketSnapshotCache\(snapshot\)/);
  assert.match(storeSource, /getHistoryRevision\(\)/);
  assert.match(storeSource, /bumpHistoryRevision\(\)/);
  assert.match(simulationSource, /scheduleLatencyLogs\(snapshot\)/);
  assert.match(simulationSource, /setImmediate\(\(\) =>/);
  assert.match(appSource, /requestAnimationFrame/);
  assert.match(appSource, /pendingMarketTick/);
  assert.match(appSource, /marketTickFrame/);
  assert.match(appSource, /setMarketTickPayload\(pending\.data, pending\.receivedAt/);
  assert.match(appSource, /function TradePageRestored\(props:/);
}

async function testOrderFastPathUsesLightUserTradePayloads() {
  const simulationSource = readFileSync("apps/server/src/services/simulation.ts", "utf8");
  const storeSource = readFileSync("apps/server/src/services/store.ts", "utf8");
  const indexSource = readFileSync("apps/server/src/index.ts", "utf8");
  const apiSource = readFileSync("apps/client/src/utils/api.ts", "utf8");
  const appStoreSource = readFileSync("apps/client/src/store/useAppStore.ts", "utf8");
  const appSource = readFileSync("apps/client/src/App.tsx", "utf8");

  assert.match(simulationSource, /enqueueTradeLog/);
  assert.match(simulationSource, /void this\.flushTradeLogQueue\(\)/);
  assert.match(simulationSource, /this\.store\.emitUserPayload\(user\.id, "trade"\)/);
  assert.doesNotMatch(simulationSource, /await Promise\.all\(\[\s*this\.writeAuditLog\(/);
  assert.match(storeSource, /export type UserPayloadScope = "full" \| "trade"/);
  assert.match(storeSource, /emitUserPayload\(userId: string, scope: UserPayloadScope = "full"\)/);
  assert.doesNotMatch(storeSource, /const payload: UserPayload = \{\s*profile: this\.getProfile\(userId\),\s*operatedHistory: this\.getOperatedHistory\(500, userId\),/);
  assert.match(indexSource, /type: scope === "trade" \? "user:trade" : "user"/);
  assert.match(indexSource, /orders: store\.getRecentTradeOrders\(user\.id/);
  assert.match(indexSource, /let userSendInFlight = false/);
  assert.match(indexSource, /let pendingUserPayloadScope: UserPayloadScope \| undefined/);
  assert.match(indexSource, /function queueUserPayload/);
  assert.match(indexSource, /mergeUserPayloadScope/);
  assert.match(indexSource, /socket\.bufferedAmount > 0/);
  assert.match(indexSource, /appMetrics\.recordWsSend\("user"/);
  assert.match(apiSource, /export interface UserTradePayload/);
  assert.match(appStoreSource, /setUserTradePayload: \(data: UserTradePayload\) => void/);
  assert.match(appStoreSource, /setUserTradePayload: \(data\) =>/);
  assert.match(appSource, /type: "user" \| "user:trade"/);
  assert.match(appSource, /requestAnimationFrame\(\(\) => \{/);
  assert.match(appSource, /startTransition\(\(\) => \{/);
  assert.match(appSource, /setUserTradePayload\(parsed\.data as UserTradePayload\)/);
}

async function testClobWsFirstMarketDataContracts() {
  const connectorSource = readFileSync("apps/server/src/services/connectors/polymarket.ts", "utf8");
  const simulationSource = readFileSync("apps/server/src/services/simulation.ts", "utf8");
  const indexSource = readFileSync("apps/server/src/index.ts", "utf8");

  assert.match(connectorSource, /custom_feature_enabled: true/);
  assert.match(connectorSource, /eventType === "best_bid_ask"/);
  assert.match(connectorSource, /eventType === "price_change"/);
  assert.match(connectorSource, /function recomputeBookTop/);
  assert.match(connectorSource, /function applyTopToBook/);
  assert.match(connectorSource, /function applyPriceChangeToBook/);
  assert.match(connectorSource, /input\.snapshotTs < book\.snapshotTs/);
  assert.match(connectorSource, /size=0|input\.qty > 0/);
  assert.match(connectorSource, /Streaming best bid\/ask/);
  assert.match(connectorSource, /Streaming price changes/);
  assert.match(simulationSource, /bookPollMs: config\.polymarketBookCalibrationMs/);
  assert.match(simulationSource, /polymarketBookCalibrationMs \|\| this\.config\.polymarketBookPollMs/);
  assert.match(indexSource, /polymarketBookCalibrationMs: serverConfig\.polymarketBookCalibrationMs/);
}

async function testClobV2FeeMarketInfoAndLatencyContracts() {
  const typesSource = readFileSync("apps/server/src/domain/types.ts", "utf8");
  const simulationSource = readFileSync("apps/server/src/services/simulation.ts", "utf8");
  const connectorSource = readFileSync("apps/server/src/services/connectors/polymarket.ts", "utf8");
  const storeSource = readFileSync("apps/server/src/services/store.ts", "utf8");
  const apiSource = readFileSync("apps/client/src/utils/api.ts", "utf8");
  const appSource = readFileSync("apps/client/src/App.tsx", "utf8");

  assert.match(typesSource, /export interface ClobMarketInfo/);
  assert.match(typesSource, /minimumTickSize: number/);
  assert.match(typesSource, /minimumOrderSize: number/);
  assert.match(typesSource, /makerFeeRate: number/);
  assert.match(typesSource, /takerFeeRate: number/);
  assert.match(typesSource, /export interface LatencyBreakdown/);
  assert.match(typesSource, /sourceEventAge/);
  assert.match(typesSource, /serverIngressLatency/);
  assert.match(typesSource, /serverComputeLatency/);
  assert.match(typesSource, /clientTransportLatency/);
  assert.match(typesSource, /estimatedFee\?: number/);
  assert.match(typesSource, /actualFee\?: number/);
  assert.match(typesSource, /feeCurrency\?: FeeCurrency/);
  assert.match(connectorSource, /fetchClobMarketInfo/);
  assert.match(connectorSource, /minimum_tick_size/);
  assert.match(connectorSource, /minimum_order_size/);
  assert.match(connectorSource, /input\.fd/);
  assert.match(connectorSource, /feeSchedule\?\.rate/);
  assert.match(connectorSource, /Array\.isArray\(input\.t\)/);
  assert.match(connectorSource, /upBookPayload\.snapshotTs >= existingUpBook\.snapshotTs/);
  assert.match(simulationSource, /isAlignedToTick/);
  assert.match(simulationSource, /Order size must be at least CLOB minimum order size/);
  assert.match(simulationSource, /user\.availableUsdc = roundCurrency\(user\.availableUsdc - totalSpend\)/);
  assert.match(simulationSource, /user\.availableUsdc = roundCurrency\(user\.availableUsdc \+ estimate\.matchedNotional - \(order\.actualFee \?\? 0\)\)/);
  assert.match(simulationSource, /sourceEventAge/);
  assert.match(simulationSource, /serverIngressLatency/);
  assert.match(storeSource, /estimated_fee DOUBLE PRECISION/);
  assert.match(storeSource, /actual_fee DOUBLE PRECISION/);
  assert.match(storeSource, /fee_breakdown JSONB/);
  assert.match(apiSource, /export interface ClobMarketInfo/);
  assert.match(apiSource, /latencyBreakdown: LatencyBreakdown/);
  assert.match(appSource, /USD/);
  assert.match(appSource, /estimatedOrderFee/);
  assert.match(appSource, /Latency Split/);
  assert.match(appSource, /parsedAmount \+ \(estimatedFee \?\? 0\) > \(profile\?\.availableUsdc \?\? 0\) \+ 0\.0001/);
}

async function testPolymarketReferencePricesDoNotUseOutcomeOdds() {
  const simulationSource = readFileSync("apps/server/src/services/simulation.ts", "utf8");
  const referenceSource = readFileSync("apps/server/src/services/connectors/polymarket-reference.ts", "utf8");
  const appSource = readFileSync("apps/client/src/App.tsx", "utf8");
  const indexSource = readFileSync("apps/server/src/index.ts", "utf8");
  const storeSource = readFileSync("apps/server/src/services/store.ts", "utf8");
  assert.doesNotMatch(simulationSource, /getRoundUpMarketPrice/);
  assert.doesNotMatch(simulationSource, /getRoundPolymarketBtcReference/);
  assert.doesNotMatch(simulationSource, /captureReferencePrice/);
  assert.match(simulationSource, /hydrateRoundPolymarketReferencePrices/);
  assert.match(simulationSource, /syncPriceToBeatFromPolymarketOpenPrice/);
  assert.match(simulationSource, /const officialPriceToBeat = roundNumber\(round\.polymarketOpenPrice, 2\)/);
  assert.match(simulationSource, /round\.priceToBeat = officialPriceToBeat/);
  assert.match(simulationSource, /round\.priceToBeatSource = round\.polymarketOpenPriceSource \?\? "Gamma"/);
  assert.match(simulationSource, /currentRoundChainlinkOpenReferences/);
  assert.match(simulationSource, /resolveCurrentRoundChainlinkOpenReference/);
  assert.match(simulationSource, /withCurrentRoundChainlinkOpenReference/);
  assert.match(simulationSource, /return \{ \.\.\.round, chainlinkOpenPrice: reference \}/);
  assert.doesNotMatch(simulationSource, /syncRoundChainlinkReferencePrices/);
  assert.doesNotMatch(simulationSource, /currentRoundOpenReference/);
  assert.match(simulationSource, /PolymarketReferenceResolver/);
  assert.match(simulationSource, /resolveBoundaryPrice\(round\.startAt/);
  assert.match(simulationSource, /resolveBoundaryPrice\(round\.endAt/);
  assert.match(simulationSource, /historyCacheMs: isActiveRound \? 2_000 : undefined/);
  assert.match(simulationSource, /isBtcReferencePrice\(existing\?\.polymarketOpenPrice\)/);
  assert.match(simulationSource, /if \(!isBtcReferencePrice\(round\.polymarketOpenPrice\)\)/);
  assert.doesNotMatch(simulationSource, /capture_price_to_beat/);
  assert.match(referenceSource, /historyCacheMs\?: number/);
  assert.match(referenceSource, /getHistoricalSamples\(metadata, input\?\.historyCacheMs\)/);
  assert.match(appSource, /function btcMoneyOrDash\(value\?: number\)/);
  assert.match(appSource, /const displayPriceToBeat = isBtcReferencePrice\(snapshot\?\.displayPriceToBeat\) \? snapshot\.displayPriceToBeat : undefined;/);
  assert.match(appSource, /PTB \(Binance open\)/);
  assert.match(appSource, /`PTB: \$\{btcMoneyOrDash\(round\.priceToBeat\)\}`/);
  assert.doesNotMatch(appSource, /PTB \$\{money\(snapshot\?\.priceToBeat \?\? 0\)\}/);
  assert.match(indexSource, /decorateCurrentRoundForTransport/);
  assert.match(indexSource, /engine\.withCurrentRoundChainlinkOpenReference\(round\)/);
  assert.match(indexSource, /currentRound: decorateCurrentRoundForTransport\(currentRound\)/);
  assert.doesNotMatch(indexSource, /currentRoundOpenReference: stamped\.chainlink\.currentRoundOpenReference/);

  const connectorSource = readFileSync("apps/server/src/services/connectors/polymarket.ts", "utf8");
  assert.match(connectorSource, /function normalizeMarketOutcomes\(payload: DetailedMarketPayload\)/);
  assert.match(connectorSource, /function outcomeSide\(value\?: string\): TradeSide \| undefined/);
  assert.match(connectorSource, /const normalizedOutcomes = normalizeMarketOutcomes\(payload\)/);
  assert.match(connectorSource, /const parsedEventStartAt = Date\.parse\(payload\.eventStartTime \?\? ""\)/);
  assert.match(connectorSource, /resolutionSource: payload\.resolutionSource/);
  assert.match(connectorSource, /outcomePrices:\s*\[normalizedOutcomes\.upPrice, normalizedOutcomes\.downPrice\]/);
  assert.match(connectorSource, /referenceOpenPrice: round\.polymarketOpenPrice/);
  assert.match(connectorSource, /referenceClosePrice: round\.polymarketClosePrice/);

  assert.match(storeSource, /sanitizePolymarketBtcReference/);
  assert.match(storeSource, /polymarketOpenPrice = sanitizePolymarketBtcReference/);
  assert.match(storeSource, /chainlink_open_price DOUBLE PRECISION/);
  assert.match(storeSource, /chainlink_close_price DOUBLE PRECISION/);
  assert.match(storeSource, /round\.chainlinkOpenPrice \?\? null/);
  assert.match(storeSource, /chainlinkOpenPrice: numberOrUndefined\(row\.chainlink_open_price\)/);
}

async function testRtdsLoginAuditAndBestAskUiRequirements() {
  const appSource = readFileSync("apps/client/src/App.tsx", "utf8");
  const styleSource = readFileSync("apps/client/src/styles.css", "utf8");
  const simulationSource = readFileSync("apps/server/src/services/simulation.ts", "utf8");
  const binanceSource = readFileSync("apps/server/src/services/connectors/binance.ts", "utf8");

  assert.match(simulationSource, /const upPrice = isPositivePrice\(upBook\.bestAsk\) \? upBook\.bestAsk : 0/);
  assert.match(simulationSource, /const downPrice = isPositivePrice\(downBook\.bestAsk\) \? downBook\.bestAsk : 0/);
  assert.match(binanceSource, /"1m": 180/);
  assert.match(binanceSource, /"5m": 30/);
  assert.match(binanceSource, /"15m": 24/);
  assert.match(binanceSource, /"1h": 24/);
  assert.match(appSource, /terminal-login-page/);
  assert.match(appSource, /terminal-login-tabs/);
  assert.match(appSource, /ht_saved_users/);
  assert.match(styleSource, /\.terminal-login-card/);
  assert.match(appSource, /AUDIT_ACTION_LABELS/);
  assert.match(appSource, /auditActionLabel\(actionType, language\)/);
  assert.match(appSource, /api\.getHistory\(token, 200, props\.viewedUserId\)/);
  assert.match(appSource, /const TRADE_INTERVAL_OPTIONS = \["30s", "1m", "5m", "15m", "1h"\]/);
  assert.match(appSource, /snapshot\?\.chainlink\.candlesByInterval\[selectedInterval\]/);
  assert.match(appSource, /defaultVisibleCountForInterval\(selectedInterval\)/);
  assert.match(simulationSource, /private chainlinkCandlesByInterval = createEmptyChainlinkIntervalBars\(\)/);
  const i18nSource = readFileSync("apps/client/src/i18n/index.ts", "utf8");
  assert.match(i18nSource, /traceId: "Trace ID"/);
  assert.match(i18nSource, /orderId: "Order ID"/);
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
    eventStartTime: new Date(startAt).toISOString(),
    endDate: new Date(startAt + 5 * 60_000).toISOString(),
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
    normalizeStreamUrl,
    parseChainlinkStreamMetadataFromHtml,
    parseChainlinkCandlestickSamples,
    pickFirstSampleAtOrAfter
  } = require("../apps/server/src/services/connectors/polymarket-reference.ts") as {
    normalizeStreamUrl: (source?: string) => string;
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

  assert.equal(
    normalizeStreamUrl("https://data.chain.link/streams/btc-usd."),
    "https://data.chain.link/streams/btc-usd-cexprice-streams"
  );
  assert.equal(
    normalizeStreamUrl("Chainlink Data Streams: https://data.chain.link/streams/btc-usd-cexprice-streams."),
    "https://data.chain.link/streams/btc-usd-cexprice-streams"
  );
  assert.equal(normalizeStreamUrl("Chainlink Data Streams btc/usd"), "https://data.chain.link/streams/btc-usd-cexprice-streams");

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
  assert.equal(metadata.streamUrl, "https://data.chain.link/streams/btc-usd-cexprice-streams");

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
  await testChainlinkDisplayUsesStrictRtds();
  await testProfileUsesOperatedGroupedRoundViews();
  await testSettlementUsesResolvedQueueAndFiveSecondGammaPolling();
  await testBackendTransportStampingKeepsLatencySeparateFromAge();
  await testRealtimeLatencyPacingContracts();
  await testOrderFastPathUsesLightUserTradePayloads();
  await testClobWsFirstMarketDataContracts();
  await testClobV2FeeMarketInfoAndLatencyContracts();
  await testPolymarketReferencePricesDoNotUseOutcomeOdds();
  await testRtdsLoginAuditAndBestAskUiRequirements();
  await testPolymarketMarketSelectionUsesSlugTime();
  await testChainlinkReferenceResolverHelpers();

  console.log("flow-regression-check ok");
}

void main();

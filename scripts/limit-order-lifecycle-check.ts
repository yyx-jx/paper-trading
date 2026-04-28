import assert from "node:assert/strict";
import { SimulationEngine } from "../apps/server/src/services/simulation";
import type {
  AuditEvent,
  BehaviorActionLog,
  MarketSnapshot,
  OrderBookSnapshot,
  OrderRecord,
  PositionRecord,
  RoundRecord,
  SourceHealth,
  TradeSide,
  UserRecord
} from "../apps/server/src/domain/types";

const now = () => Date.now();

function book(id: string, bids: Array<[number, number]>, asks: Array<[number, number]>): OrderBookSnapshot {
  const bestBid = bids[0]?.[0] ?? 0;
  const bestAsk = asks[0]?.[0] ?? 0;
  return {
    snapshotId: id,
    snapshotTs: now(),
    bestBid,
    bestAsk,
    midPrice: bestBid > 0 && bestAsk > 0 ? Number(((bestBid + bestAsk) / 2).toFixed(4)) : bestBid || bestAsk,
    bids: bids.map(([price, qty]) => ({ price, qty })),
    asks: asks.map(([price, qty]) => ({ price, qty }))
  };
}

function source(source: SourceHealth["source"]): SourceHealth {
  const ts = now();
  return {
    source,
    symbol: "BTC",
    state: "healthy",
    reconnectCount: 0,
    sourceEventTs: ts,
    serverRecvTs: ts,
    normalizedTs: ts,
    serverPublishTs: ts,
    acquireLatencyMs: 0,
    publishLatencyMs: 0,
    frontendLatencyMs: 0
  };
}

function snapshot(upBook: OrderBookSnapshot, downBook: OrderBookSnapshot, round: RoundRecord): MarketSnapshot {
  return {
    symbol: "BTC",
    marketId: round.marketId,
    marketSlug: round.marketSlug,
    serverNow: now(),
    roundId: round.id,
    currentMarket: {
      id: round.marketId,
      slug: round.marketSlug ?? round.id,
      conditionId: round.conditionId,
      question: "BTC Up or Down",
      startAt: round.startAt,
      endAt: round.endAt,
      upTokenId: round.upTokenId,
      downTokenId: round.downTokenId
    },
    priceToBeat: 80_000,
    upPrice: upBook.midPrice || upBook.bestAsk,
    downPrice: downBook.midPrice || downBook.bestAsk,
    clob: {
      upBook,
      downBook,
      bestBidAskSummary: {
        UP: { bestBid: upBook.bestBid, bestAsk: upBook.bestAsk },
        DOWN: { bestBid: downBook.bestBid, bestAsk: downBook.bestAsk }
      },
      delta: 0,
      volume: 0
    },
    orderBooks: {
      UP: upBook,
      DOWN: downBook
    },
    recentTrades: [],
    binance: {
      spotPrice: 80_000,
      candlesByInterval: {
        "1m": [],
        "5m": [],
        "1d": []
      }
    },
    chainlink: {
      referencePrice: 0,
      settlementReference: 0
    },
    sources: {
      binance: source("Binance"),
      chainlink: source("Chainlink"),
      clob: source("CLOB")
    },
    uiMeta: {
      marketTitle: "BTC 5m",
      marketSubtitle: "test",
      countdownMs: Math.max(round.endAt - now(), 0),
      acceptingOrders: round.acceptingOrders !== false,
      marketSwitchState: "active",
      sourceStatusSummary: []
    }
  };
}

function round(overrides: Partial<RoundRecord> = {}): RoundRecord {
  const startedAt = now() - 60_000;
  return {
    id: `btc-updown-5m-${Math.floor(startedAt / 1000)}`,
    symbol: "BTC",
    marketId: "market-1",
    marketSlug: "btc-updown-5m-test",
    conditionId: "condition-1",
    startAt: startedAt,
    endAt: now() + 240_000,
    status: "Trading",
    acceptingOrders: true,
    openingPrice: 80_000,
    upTokenId: "up-token",
    downTokenId: "down-token",
    ...overrides
  };
}

function user(overrides: Partial<UserRecord> = {}): UserRecord {
  return {
    id: "u1",
    username: "tester",
    password: "tester123",
    displayName: "Tester",
    role: "Tester",
    language: "zh-CN",
    permissionCodes: ["trade:view", "trade:order", "trade:cancel", "trade:sell", "profile:view"],
    availableUsdc: 100,
    createdAt: now() - 60_000,
    ...overrides
  };
}

function position(input: Partial<PositionRecord> = {}): PositionRecord {
  return {
    id: input.id ?? "pos-1",
    userId: input.userId ?? "u1",
    roundId: input.roundId ?? "round-1",
    side: input.side ?? "UP",
    qty: input.qty ?? 10,
    lockedQty: input.lockedQty ?? 0,
    averageEntry: input.averageEntry ?? 0.4,
    notionalSpent: input.notionalSpent ?? 4,
    currentMark: input.currentMark ?? 0.4,
    currentValue: input.currentValue ?? 4,
    unrealizedPnl: input.unrealizedPnl ?? 0,
    realizedPnl: input.realizedPnl ?? 0,
    status: input.status ?? "open",
    openedAt: input.openedAt ?? now() - 30_000,
    ...input
  };
}

function createFixture(input?: {
  availableUsdc?: number;
  upBook?: OrderBookSnapshot;
  downBook?: OrderBookSnapshot;
  round?: RoundRecord;
  positions?: PositionRecord[];
}) {
  const currentRound = input?.round ?? round();
  const books: Record<TradeSide, OrderBookSnapshot> = {
    UP: input?.upBook ?? book("up-initial", [[0.45, 100]], [[0.55, 100]]),
    DOWN: input?.downBook ?? book("down-initial", [[0.45, 100]], [[0.55, 100]])
  };
  const currentUser = user({ availableUsdc: input?.availableUsdc ?? 100 });
  const emittedUsers: string[] = [];
  let id = 0;
  const store = {
    users: new Map([[currentUser.id, currentUser]]),
    rounds: [currentRound],
    orders: [] as OrderRecord[],
    orderLifecycleLogs: [] as Array<Record<string, unknown>>,
    orderBookSnapshots: new Map<string, OrderBookSnapshot>(),
    positions: input?.positions ?? [],
    logs: [] as AuditEvent[],
    behaviorLogs: [] as BehaviorActionLog[],
    marketSnapshot: snapshot(books.UP, books.DOWN, currentRound),
    newTraceId: () => `trace-${++id}`,
    newId: (prefix: string) => `${prefix}-${++id}`,
    anonymizeUserId: (userId: string) => `anon-${userId}`,
    getUserById: (userId: string) => store.users.get(userId),
    getRoundById: (roundId: string) => store.rounds.find((item) => item.id === roundId),
    getCurrentRound: () => currentRound,
    persistUser: async (nextUser: UserRecord) => {
      store.users.set(nextUser.id, nextUser);
    },
    persistOrder: async (order: OrderRecord) => {
      if (order.orderBookSnapshot) {
        order.orderBookSnapshotRef = `obs-${order.orderBookSnapshot.snapshotId}`;
        store.orderBookSnapshots.set(order.orderBookSnapshotRef, order.orderBookSnapshot);
        order.orderBookSnapshot = undefined;
      }
      const index = store.orders.findIndex((item) => item.id === order.id);
      if (index >= 0) {
        store.orders[index] = order;
      } else {
        store.orders.push(order);
      }
      store.orders.sort((left, right) => right.createdAt - left.createdAt);
    },
    persistOrderLifecycle: async (log: Record<string, unknown>) => {
      const index = store.orderLifecycleLogs.findIndex((item) => item.id === log.id);
      if (index >= 0) {
        store.orderLifecycleLogs[index] = log;
      } else {
        store.orderLifecycleLogs.push(log);
      }
    },
    applyLifecycleExit: async (input: { qty: number; exitType: string; exitTokenPrice?: number }) => {
      store.orderLifecycleLogs.push({ id: `exit-${store.orderLifecycleLogs.length}`, ...input });
    },
    settleOpenOrderLifecycles: async () => undefined,
    persistPosition: async (nextPosition: PositionRecord) => {
      const index = store.positions.findIndex((item) => item.id === nextPosition.id);
      if (index >= 0) {
        store.positions[index] = nextPosition;
      } else {
        store.positions.push(nextPosition);
      }
    },
    recordLog: async (event: AuditEvent) => {
      store.logs.unshift(event);
    },
    recordBehaviorLog: async (event: BehaviorActionLog) => {
      store.behaviorLogs.unshift(event);
    },
    emitUserPayload: (userId: string) => {
      emittedUsers.push(userId);
    }
  };

  const engine = Object.create(SimulationEngine.prototype) as SimulationEngine & Record<string, unknown>;
  engine.store = store;
  engine.config = {
    symbol: "BTC",
    marketId: currentRound.marketId,
    freezeWindowMs: 10_000
  };
  engine.polymarketState = {
    currentMarket: {
      id: currentRound.marketId,
      slug: currentRound.marketSlug,
      conditionId: currentRound.conditionId,
      upTokenId: currentRound.upTokenId,
      downTokenId: currentRound.downTokenId
    },
    orderBooks: books
  };
  engine.polymarketConnector = {
    fetchBookByToken: async (tokenId: string) => (tokenId === "up-token" ? books.UP : books.DOWN),
    fetchBookForSide: async (side: TradeSide) => books[side]
  };

  const setBook = (side: TradeSide, nextBook: OrderBookSnapshot) => {
    books[side] = nextBook;
    (engine.polymarketState as { orderBooks: Record<TradeSide, OrderBookSnapshot> }).orderBooks = books;
    store.marketSnapshot = snapshot(books.UP, books.DOWN, currentRound);
  };

  return { engine, store, user: currentUser, round: currentRound, setBook, emittedUsers };
}

async function expectRejectsWithMessage(action: () => Promise<unknown>, pattern: RegExp) {
  let message = "";
  try {
    await action();
  } catch (error) {
    message = error instanceof Error ? error.message : String(error);
  }
  assert.match(message, pattern);
}

async function testLimitBuyRestsAndCancelReleasesUsdc() {
  const { engine, store, user: currentUser } = createFixture({
    upBook: book("up-rest-buy", [[0.45, 100]], [[0.6, 100]])
  });

  const { order } = await engine.placeOrder(currentUser, {
    action: "buy",
    side: "UP",
    orderKind: "limit",
    amount: 50,
    limitPrice: 0.5
  });

  assert.equal(order.status, "pending");
  assert.equal(order.timeInForce, "GTC");
  assert.equal(order.frozenUsdc, 50);
  assert.equal(order.expectedQty, 100);
  assert.equal(currentUser.availableUsdc, 50);
  assert.equal(store.orders.length, 1);

  await engine.cancelOrder(currentUser, order.id);
  assert.equal(order.status, "cancelled");
  assert.equal(order.frozenUsdc, 0);
  assert.equal(currentUser.availableUsdc, 100);
}

async function testLimitBuyImmediateFillCreatesPosition() {
  const { engine, user: currentUser, store } = createFixture({
    upBook: book("up-fill-buy", [[0.48, 100]], [[0.5, 40]])
  });

  const { order } = await engine.placeOrder(currentUser, {
    action: "buy",
    side: "UP",
    orderKind: "limit",
    amount: 20,
    limitPrice: 0.55
  });

  assert.equal(order.status, "filled");
  assert.equal(order.frozenUsdc, 0);
  assert.equal(order.filledQty, 40);
  assert.equal(order.avgFillPrice, 0.5);
  assert.equal(currentUser.availableUsdc, 80);
  assert.equal(store.positions[0]?.qty, 40);
  assert.equal(store.positions[0]?.averageEntry, 0.5);
  assert.equal(store.orderLifecycleLogs.length, 1);
  assert.equal(store.orderLifecycleLogs[0]?.buyOrderId, order.id);
  assert.equal(store.orderLifecycleLogs[0]?.orderBookSnapshotRef, "obs-up-fill-buy");
}

async function testPendingLimitBuyTriggersFromFutureBook() {
  const { engine, user: currentUser, store, setBook } = createFixture({
    upBook: book("up-rest-then-trigger", [[0.45, 100]], [[0.7, 100]])
  });

  const { order } = await engine.placeOrder(currentUser, {
    action: "buy",
    side: "UP",
    orderKind: "limit",
    amount: 50,
    limitPrice: 0.5
  });
  assert.equal(order.status, "pending");

  setBook("UP", book("up-trigger-buy", [[0.45, 100]], [[0.4, 200]]));
  await engine.processPendingOrders();

  assert.equal(order.status, "filled");
  assert.equal(order.frozenUsdc, 0);
  assert.equal(order.filledQty, 125);
  assert.equal(order.avgFillPrice, 0.4);
  assert.equal(currentUser.availableUsdc, 50);
  assert.equal(store.positions[0]?.qty, 125);
  assert.equal(store.orderLifecycleLogs.some((log) => log.buyOrderId === order.id), true);
}

async function testOrderCapturesFullBookSnapshotWithoutReferenceLeak() {
  const sourceBook = book(
    "up-capture-source",
    [
      [0.48, 100],
      [0.47, 80]
    ],
    [
      [0.5, 40],
      [0.51, 70]
    ]
  );
  const { engine, user: currentUser, store } = createFixture({ upBook: sourceBook });

  const { order } = await engine.placeOrder(currentUser, {
    action: "buy",
    side: "UP",
    orderKind: "market",
    amount: 20
  });

  assert.equal(order.status, "filled");
  assert.equal(order.orderBookSnapshot, undefined);
  assert.equal(order.orderBookSnapshotRef, "obs-up-capture-source");
  const storedSnapshot = store.orderBookSnapshots.get(order.orderBookSnapshotRef ?? "");
  assert.equal(storedSnapshot?.snapshotId, "up-capture-source");
  assert.equal(storedSnapshot?.bids.length, 2);
  assert.equal(storedSnapshot?.asks.length, 2);
  assert.notEqual(storedSnapshot, sourceBook);
  assert.notEqual(storedSnapshot?.bids, sourceBook.bids);
  assert.notEqual(storedSnapshot?.asks, sourceBook.asks);

  sourceBook.bids[0]!.price = 0.01;
  sourceBook.asks.push({ price: 0.99, qty: 1 });

  assert.equal(storedSnapshot?.bids[0]?.price, 0.48);
  assert.equal(storedSnapshot?.asks.length, 2);
  assert.equal(store.orders[0]?.orderBookSnapshot, undefined);
}

async function testPendingOrderKeepsInitialBookSnapshotAfterTrigger() {
  const initialBook = book("up-initial-pending-snapshot", [[0.45, 100]], [[0.7, 100]]);
  const { engine, user: currentUser, store, setBook } = createFixture({ upBook: initialBook });

  const { order } = await engine.placeOrder(currentUser, {
    action: "buy",
    side: "UP",
    orderKind: "limit",
    amount: 50,
    limitPrice: 0.5
  });
  assert.equal(order.status, "pending");
  assert.equal(order.orderBookSnapshotRef, "obs-up-initial-pending-snapshot");

  setBook("UP", book("up-future-trigger-snapshot", [[0.45, 100]], [[0.4, 200]]));
  await engine.processPendingOrders();

  assert.equal(order.status, "filled");
  assert.equal(order.bookHash, "up-future-trigger-snapshot");
  assert.equal(order.orderBookSnapshotRef, "obs-up-initial-pending-snapshot");
  assert.equal(store.orderBookSnapshots.get(order.orderBookSnapshotRef ?? "")?.bestAsk, 0.7);
}

async function testLimitSellRestsLocksQtyAndCancelReleasesQty() {
  const currentRound = round({ id: "round-sell-rest" });
  const currentPosition = position({ roundId: currentRound.id, qty: 10, notionalSpent: 4 });
  const { engine, user: currentUser } = createFixture({
    round: currentRound,
    upBook: book("up-rest-sell", [[0.3, 100]], [[0.55, 100]]),
    positions: [currentPosition]
  });

  const { order } = await engine.placeOrder(currentUser, {
    action: "sell",
    side: "UP",
    orderKind: "limit",
    qty: 5,
    limitPrice: 0.6
  });

  assert.equal(order.status, "pending");
  assert.equal(order.frozenQty, 5);
  assert.equal(currentPosition.lockedQty, 5);

  await expectRejectsWithMessage(
    () =>
      engine.placeOrder(currentUser, {
        action: "sell",
        side: "UP",
        orderKind: "limit",
        qty: 6,
        limitPrice: 0.6
      }),
    /Insufficient unlocked position quantity/
  );

  await engine.cancelOrder(currentUser, order.id);
  assert.equal(order.status, "cancelled");
  assert.equal(order.frozenQty, 0);
  assert.equal(currentPosition.lockedQty, 0);
}

async function testPendingLimitSellTriggersFromFutureBook() {
  const currentRound = round({ id: "round-sell-trigger" });
  const currentPosition = position({ roundId: currentRound.id, qty: 10, averageEntry: 0.4, notionalSpent: 4 });
  const { engine, user: currentUser, store, setBook } = createFixture({
    round: currentRound,
    upBook: book("up-rest-sell-trigger", [[0.3, 100]], [[0.55, 100]]),
    positions: [currentPosition]
  });

  const { order } = await engine.placeOrder(currentUser, {
    action: "sell",
    side: "UP",
    orderKind: "limit",
    qty: 5,
    limitPrice: 0.6
  });

  setBook("UP", book("up-trigger-sell", [[0.65, 100]], [[0.7, 100]]));
  await engine.processPendingOrders();

  assert.equal(order.status, "filled");
  assert.equal(order.frozenQty, 0);
  assert.equal(order.filledQty, 5);
  assert.equal(order.avgFillPrice, 0.65);
  assert.equal(currentPosition.qty, 5);
  assert.equal(currentPosition.lockedQty, 0);
  assert.equal(currentUser.availableUsdc, 103.25);
  assert.equal(currentPosition.realizedPnl, 1.25);
}

async function testInsufficientBuyBalanceDoesNotCreateOrder() {
  const { engine, user: currentUser, store } = createFixture({ availableUsdc: 25 });

  await expectRejectsWithMessage(
    () =>
      engine.placeOrder(currentUser, {
        action: "buy",
        side: "UP",
        orderKind: "limit",
        amount: 50,
        limitPrice: 0.5
      }),
    /Insufficient virtual balance/
  );
  assert.equal(store.orders.length, 0);
  assert.equal(store.orderLifecycleLogs.length, 0);
  assert.equal(currentUser.availableUsdc, 25);
}

async function testPendingOrderFailsAndReleasesAssetsInFreezeWindow() {
  const currentRound = round({ id: "round-freeze" });
  const { engine, user: currentUser, store, setBook } = createFixture({
    round: currentRound,
    upBook: book("up-freeze-rest", [[0.45, 100]], [[0.7, 100]])
  });

  const { order } = await engine.placeOrder(currentUser, {
    action: "buy",
    side: "UP",
    orderKind: "limit",
    amount: 50,
    limitPrice: 0.5
  });
  assert.equal(order.status, "pending");
  assert.equal(currentUser.availableUsdc, 50);

  currentRound.endAt = now() + 5_000;
  setBook("UP", book("up-freeze-crossed", [[0.45, 100]], [[0.4, 200]]));
  await engine.processPendingOrders();

  assert.equal(order.status, "failed");
  assert.equal(order.frozenUsdc, 0);
  assert.equal(currentUser.availableUsdc, 100);
  assert.match(order.failureReason ?? "", /final order freeze window/);
  assert.equal(store.orderLifecycleLogs.length, 0);
}

async function main() {
  await testLimitBuyRestsAndCancelReleasesUsdc();
  await testLimitBuyImmediateFillCreatesPosition();
  await testPendingLimitBuyTriggersFromFutureBook();
  await testOrderCapturesFullBookSnapshotWithoutReferenceLeak();
  await testPendingOrderKeepsInitialBookSnapshotAfterTrigger();
  await testLimitSellRestsLocksQtyAndCancelReleasesQty();
  await testPendingLimitSellTriggersFromFutureBook();
  await testInsufficientBuyBalanceDoesNotCreateOrder();
  await testPendingOrderFailsAndReleasesAssetsInFreezeWindow();

  console.log("limit-order-lifecycle-check ok");
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

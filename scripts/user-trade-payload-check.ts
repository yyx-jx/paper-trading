import { strict as assert } from "node:assert";
import { createUserPayloadBuilder } from "../apps/server/src/payloads/user";
import type {
  OrderLifecycleRecord,
  OrderRecord,
  PositionRecord,
  ProfileOverview,
  UserRecord
} from "../apps/server/src/domain/types";

const EXPECTED_TRADE_ORDER_LIMIT = 12;
const EXPECTED_TRADE_LIFECYCLE_LIMIT = 24;

function createUser(id: string): UserRecord {
  const now = Date.now();
  return {
    id,
    username: id,
    password: "hash",
    displayName: id,
    role: "Tester",
    language: "zh-CN",
    permissionCodes: ["trade:view", "profile:view"],
    availableUsdc: 1_000,
    isActive: true,
    createdAt: now,
    updatedAt: now
  };
}

function createOrder(userId: string, index: number): OrderRecord {
  return {
    id: `order-${index}`,
    traceId: `trace-${index}`,
    userId,
    roundId: `round-${Math.floor(index / 2)}`,
    symbol: "BTC",
    marketId: "market-1",
    action: index % 2 === 0 ? "buy" : "sell",
    side: index % 2 === 0 ? "UP" : "DOWN",
    status: index % 3 === 0 ? "filled" : "pending",
    orderKind: "market",
    timeInForce: "GTC",
    lifecycleStatus: "pending",
    resultType: "pending",
    limitPrice: 0.53,
    tokenId: `token-${index}`,
    bookKey: `book-${index}`,
    bookHash: `hash-${index}`,
    requestedAmountUsdc: 25 + index,
    requestedQty: 50 + index,
    frozenUsdc: 10 + index,
    frozenQty: 20 + index,
    fills: [{ fillId: `fill-${index}` }],
    estimatedFee: 0.12,
    actualFee: 0.23,
    feeBreakdown: { executionFee: 0.1, slippageCost: 0.13 },
    feeCurrency: "USDC",
    sourceLatencyMs: 8,
    marketSlug: "btc-5m-updown",
    orderBookSnapshotRef: `snapshot-${index}`,
    orderBookSnapshot: {
      generatedAt: 1_700_000_000_000 + index,
      bids: [{ price: 0.49, size: 100 }],
      asks: [{ price: 0.51, size: 90 }]
    },
    notionalUsdc: 25 + index,
    expectedQty: 48 + index,
    filledQty: index % 3 === 0 ? 48 + index : 0,
    unfilledQty: index % 3 === 0 ? 0 : 48 + index,
    avgFillPrice: 0.51,
    bestBid: 0.49,
    bestAsk: 0.51,
    midPrice: 0.5,
    bookSnapshotTs: 1_700_000_000_000 + index,
    partialFilled: false,
    slippageBps: 4,
    matchLatencyMs: 17,
    bookAcquireLatencyMs: 3,
    localMatchLatencyMs: 4,
    persistLatencyMs: 5,
    totalOrderLatencyMs: 26,
    failureReason: index % 5 === 0 ? "insufficient CLOB depth" : undefined,
    clientOrderId: `client-${index}`,
    clientSendTs: 1_700_000_000_100 + index,
    serverRecvTs: 1_700_000_000_200 + index,
    serverPublishTs: 1_700_000_000_250 + index,
    createdAt: 1_700_000_000_300 + index
  };
}

function createLifecycle(userId: string, index: number): OrderLifecycleRecord {
  return {
    id: `lifecycle-${index}`,
    buyOrderId: `order-${index}`,
    traceId: `trace-${index}`,
    userId,
    testerId: "tester-1",
    roundId: `round-${Math.floor(index / 2)}`,
    symbol: "BTC",
    assetClass: "BTC",
    marketId: "market-1",
    marketSlug: "btc-5m-updown",
    direction: index % 2 === 0 ? "UP" : "DOWN",
    orderTimestampMs: 1_700_000_010_000 + index,
    entryTokenPrice: 0.51,
    btcTradePrice: 67_000,
    btcOpenPriceToBeat: 66_800,
    deltaBtc: 200,
    volumeTokenQty: 48 + index,
    remainingTokenQty: index % 3 === 0 ? 0 : 12,
    closedTokenQty: index % 3 === 0 ? 48 + index : 36,
    positionNotional: 25 + index,
    exitType: "manual_sell",
    exitTokenPrice: 0.56,
    exitNotional: 27 + index,
    settlementResult: index % 3 === 0 ? "win" : undefined,
    orderBookSnapshotRef: `snapshot-${index}`,
    orderBookSnapshot: {
      generatedAt: 1_700_000_020_000 + index,
      bids: [{ price: 0.49, size: 100 }],
      asks: [{ price: 0.51, size: 90 }]
    },
    actualFillPrice: 0.52,
    slippageBps: 7,
    matchLatencyMs: 19,
    settlementTimeMs: 1_700_000_030_000 + index,
    settlementDirection: index % 2 === 0 ? "UP" : "DOWN",
    entryFee: 0.1,
    exitFee: 0.2,
    feeCurrency: "USDC",
    createdAt: 1_700_000_040_000 + index,
    updatedAt: 1_700_000_050_000 + index
  };
}

function createProfile(userId: string): ProfileOverview {
  return {
    userId,
    availableUsdc: 1000,
    allocatedUsdc: 120,
    withdrawnUsdc: 0,
    realizedPnl: 15,
    totalEquity: 1015
  };
}

function createPosition(userId: string): PositionRecord {
  return {
    id: "position-1",
    buyOrderId: "order-0",
    userId,
    roundId: "round-0",
    side: "UP",
    qty: 12,
    averageEntry: 0.51,
    notionalSpent: 6.12,
    currentMark: 0.56,
    pnl: 0.6,
    openedAt: 1_700_000_060_000,
    entryFeeUsdc: 0.1
  };
}

function createPositionTwo(userId: string): PositionRecord {
  return {
    id: "position-2",
    buyOrderId: "order-2",
    userId,
    roundId: "round-1",
    side: "DOWN",
    qty: 8,
    averageEntry: 0.47,
    notionalSpent: 3.76,
    currentMark: 0.42,
    pnl: -0.4,
    openedAt: 1_700_000_070_000,
    entryFeeUsdc: 0.08
  };
}

const user = createUser("tester-1");
const orders = Array.from({ length: 30 }, (_, index) => createOrder(user.id, index));
const lifecycles = Array.from({ length: 40 }, (_, index) => createLifecycle(user.id, index));

const builder = createUserPayloadBuilder({
  store: {
    getProfile: () => createProfile(user.id),
    getPositions: () => [createPosition(user.id), createPositionTwo(user.id)],
    getTradePositions: (_userId: string, positionIds?: string[]) =>
      [createPosition(user.id), createPositionTwo(user.id)].filter((position) =>
        positionIds ? positionIds.includes(position.id) : true
      ),
    getRecentTradeOrders: (_userId: string, limit?: number) => orders.slice(0, limit),
    getOrderLifecycleLogs: (_userId: string, options?: { limit?: number }) => lifecycles.slice(0, options?.limit)
  } as never,
  marketPayloads: {
    getOperatedHistoryWithSettlementPreview: () => []
  }
});

const payload = builder.createUserTradePayload(user);
const deltaPayload = builder.createUserTradePayload(user, { positionIds: ["position-2"] });

assert.equal(payload.orders.length, EXPECTED_TRADE_ORDER_LIMIT);
assert.equal(payload.orderLifecycles.length, EXPECTED_TRADE_LIFECYCLE_LIMIT);
assert.equal(payload.positionsMode, "replace");
assert.equal(deltaPayload.positionsMode, "delta");
assert.deepEqual(deltaPayload.positions.map((position) => position.id), ["position-2"]);

const firstOrder = payload.orders[0];
assert.ok(firstOrder, "trade payload must include recent orders");
assert.equal("fills" in firstOrder, false);
assert.equal("feeBreakdown" in firstOrder, false);
assert.equal("orderBookSnapshot" in firstOrder, false);
assert.equal("orderBookSnapshotRef" in firstOrder, false);
assert.equal("bookKey" in firstOrder, false);
assert.equal("bookHash" in firstOrder, false);
assert.equal("tokenId" in firstOrder, false);
assert.equal("frozenUsdc" in firstOrder, false);
assert.equal("frozenQty" in firstOrder, false);
assert.equal("requestedQty" in firstOrder, false);
assert.equal("clientOrderId" in firstOrder, false);
assert.equal("clientSendTs" in firstOrder, false);
assert.equal("sourceLatencyMs" in firstOrder, false);
assert.equal("bookAcquireLatencyMs" in firstOrder, false);
assert.equal("localMatchLatencyMs" in firstOrder, false);
assert.equal("persistLatencyMs" in firstOrder, false);

const firstLifecycle = payload.orderLifecycles[0];
assert.ok(firstLifecycle, "trade payload must include recent lifecycle records");
assert.equal("btcTradePrice" in firstLifecycle, false);
assert.equal("btcOpenPriceToBeat" in firstLifecycle, false);
assert.equal("deltaBtc" in firstLifecycle, false);
assert.equal("orderBookSnapshotRef" in firstLifecycle, false);
assert.equal("orderBookSnapshot" in firstLifecycle, false);
assert.equal("settlementDirection" in firstLifecycle, false);
assert.equal("feeCurrency" in firstLifecycle, false);

console.log("[user-trade-payload-check] ok");

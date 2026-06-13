import { strict as assert } from "node:assert";
import { useAppStore } from "../apps/client/src/store/useAppStore";
import type { OrderLifecycleRecord, OrderRecord, PositionRecord, ProfileOverview, UserPayload, UserTradePayload } from "../apps/client/src/utils/api";

function createProfile(userId: string, availableUsdc: number): ProfileOverview {
  return {
    userId,
    availableUsdc,
    allocatedUsdc: 0,
    withdrawnUsdc: 0,
    realizedPnl: 0,
    totalEquity: availableUsdc
  };
}

function createPosition(
  id: string,
  qty: number,
  status: PositionRecord["status"] = "open",
  openedAt = 1_700_000_000_000 + qty
): PositionRecord {
  return {
    id,
    buyOrderId: `buy-${id}`,
    userId: "user-1",
    roundId: "round-1",
    side: "UP",
    qty,
    averageEntry: 0.5,
    notionalSpent: qty * 0.5,
    currentMark: 0.52,
    pnl: 0,
    unrealizedPnl: 0,
    realizedPnl: 0,
    status,
    openedAt
  };
}

function createOrder(id: string, createdAt: number): OrderRecord {
  return {
    id,
    traceId: `trace-${id}`,
    userId: "user-1",
    roundId: "round-1",
    symbol: "BTC",
    marketId: "market-1",
    action: "buy",
    side: "UP",
    status: "filled",
    orderKind: "market",
    timeInForce: "FOK",
    lifecycleStatus: "filled",
    resultType: "all_filled",
    requestedAmountUsdc: 1,
    estimatedFee: 0,
    actualFee: 0,
    feeCurrency: "USD",
    marketSlug: "btc-5m-updown",
    notionalUsdc: 1,
    expectedQty: 2,
    filledQty: 2,
    unfilledQty: 0,
    bestBid: 0.49,
    bestAsk: 0.51,
    midPrice: 0.5,
    bookSnapshotTs: 1_700_000_000_000,
    partialFilled: false,
    matchLatencyMs: 1,
    serverRecvTs: createdAt - 30,
    serverPublishTs: createdAt - 10,
    createdAt
  };
}

function createLifecycle(id: string, orderTimestampMs: number): OrderLifecycleRecord {
  return {
    id,
    buyOrderId: "order-1",
    traceId: `trace-${id}`,
    userId: "user-1",
    testerId: "tester-1",
    roundId: "round-1",
    symbol: "BTC",
    assetClass: "BTC",
    marketId: "market-1",
    marketSlug: "btc-5m-updown",
    direction: "UP",
    orderTimestampMs,
    entryTokenPrice: 0.5,
    volumeTokenQty: 2,
    remainingTokenQty: 0,
    closedTokenQty: 2,
    positionNotional: 1,
    actualFillPrice: 0.5,
    slippageBps: 0,
    matchLatencyMs: 1,
    createdAt: orderTimestampMs + 10,
    updatedAt: orderTimestampMs + 20
  };
}

const userPayload: UserPayload = {
  viewedUserId: "user-1",
  viewedUser: {
    id: "user-1",
    username: "user-1",
    displayName: "user-1",
    role: "Tester",
    language: "zh-CN",
    availableUsdc: 1000,
    isActive: true,
    permissionCodes: []
  },
  profile: createProfile("user-1", 1000),
  operatedHistory: [],
  positions: [createPosition("position-1", 3, "open", 1_700_000_000_100), createPosition("position-2", 5, "open", 1_700_000_000_200)],
  orders: [createOrder("order-1", 1_700_000_000_130)],
  orderLifecycles: [createLifecycle("lifecycle-1", 1_700_000_000_200)],
  logs: []
};

const tradePayload: UserTradePayload = {
  viewedUserId: "user-1",
  profile: createProfile("user-1", 999),
  positionsMode: "delta",
  positions: [createPosition("position-2", 7, "closed", 1_700_000_000_200), createPosition("position-3", 2, "open", 1_700_000_000_300)],
  orders: [createOrder("order-2", 1_700_000_000_330)],
  orderLifecycles: [createLifecycle("lifecycle-2", 1_700_000_000_400)]
};

useAppStore.setState({
  viewedUserId: undefined,
  viewedUser: undefined,
  profile: undefined,
  positions: [],
  orders: [],
  orderLifecycles: [],
  logs: [],
  operatedHistory: []
});

useAppStore.getState().setUserPayload(userPayload);
useAppStore.getState().setUserTradePayload(tradePayload);

const state = useAppStore.getState();

assert.equal(state.profile?.availableUsdc, 999);
assert.deepEqual(
  state.positions.map((position) => ({ id: position.id, qty: position.qty, status: position.status })),
  [
    { id: "position-3", qty: 2, status: "open" },
    { id: "position-2", qty: 7, status: "closed" },
    { id: "position-1", qty: 3, status: "open" }
  ]
);
assert.equal(state.orders[0]?.id, "order-2");
assert.equal(state.orderLifecycles[0]?.id, "lifecycle-2");

console.log("[user-trade-delta-merge-check] ok");

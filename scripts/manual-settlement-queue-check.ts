import { readFileSync } from "node:fs";
import assert from "node:assert/strict";
import { buildServerConfig } from "../apps/server/src/config";
import { ROLE_PERMISSIONS } from "../apps/server/src/auth/permissions";
import type { OrderRecord, PositionRecord, RoundRecord } from "../apps/server/src/domain/types";
import {
  buildManualSettlementCandidates,
  shouldRequireManualSettlement
} from "../apps/server/src/services/settlement/manual-queue";

const now = 1_700_000_000_000;
const sevenAndHalfMinutes = 450_000;

const config = buildServerConfig({});
assert.equal(config.manualSettlementTimeoutMs, sevenAndHalfMinutes);

assert.equal(ROLE_PERMISSIONS.Admin.includes("settlement:manual"), true);
assert.equal(ROLE_PERMISSIONS.Tester.includes("settlement:manual"), false);
assert.equal(ROLE_PERMISSIONS["Senior Tester"].includes("settlement:manual"), false);
assert.equal(ROLE_PERMISSIONS["Test Engineer"].includes("settlement:manual"), false);

const baseRound: RoundRecord = {
  id: "round_manual",
  marketId: "market_manual",
  symbol: "BTC",
  marketSlug: "btc-updown-5m-test",
  startAt: now - 15 * 60_000,
  endAt: now - 6 * 60_000,
  priceToBeat: 65000,
  status: "Polling",
  pollCount: 40,
  pollStartAt: now - 5 * 60_000,
  lastPollAt: now - 1_000,
  acceptingOrders: false,
  manualReason: "Gamma polling timed out."
};

assert.equal(shouldRequireManualSettlement(baseRound, now, sevenAndHalfMinutes, 0), false);
assert.equal(
  shouldRequireManualSettlement({ ...baseRound, id: "round_recent", endAt: now - 2 * 60_000 }, now, sevenAndHalfMinutes, 0),
  false
);
assert.equal(
  shouldRequireManualSettlement({ ...baseRound, id: "round_closed", status: "Closed" }, now, sevenAndHalfMinutes, 0),
  false
);
assert.equal(
  shouldRequireManualSettlement({ ...baseRound, id: "round_settled", settledSide: "UP" }, now, sevenAndHalfMinutes, 0),
  false
);
assert.equal(
  shouldRequireManualSettlement({ ...baseRound, id: "round_timeout", endAt: now - 8 * 60_000 }, now, sevenAndHalfMinutes, 0),
  true
);

const positions: PositionRecord[] = [
  {
    id: "pos_up_1",
    userId: "user_a",
    roundId: "round_manual",
    side: "UP",
    qty: 2,
    averageEntry: 0.4,
    notionalSpent: 0.8,
    currentMark: 0.5,
    unrealizedPnl: 0.2,
    realizedPnl: 0,
    status: "open",
    openedAt: now - 10_000
  },
  {
    id: "pos_down_1",
    userId: "user_b",
    roundId: "round_manual",
    side: "DOWN",
    qty: 3,
    averageEntry: 0.3,
    notionalSpent: 0.9,
    currentMark: 0.5,
    unrealizedPnl: 0.6,
    realizedPnl: 0,
    status: "open",
    openedAt: now - 9_000
  },
  {
    id: "pos_closed",
    userId: "user_c",
    roundId: "round_manual",
    side: "UP",
    qty: 10,
    averageEntry: 0.2,
    notionalSpent: 2,
    currentMark: 1,
    unrealizedPnl: 0,
    realizedPnl: 8,
    status: "closed",
    openedAt: now - 8_000,
    closedAt: now - 7_000,
    settlementResult: "win"
  }
];

const orders: OrderRecord[] = [
  {
    id: "order_pending",
    traceId: "trace_pending",
    userId: "user_a",
    roundId: "round_manual",
    symbol: "BTC",
    marketId: "market_manual",
    action: "buy",
    side: "UP",
    status: "pending",
    notionalUsdc: 10,
    expectedQty: 10,
    filledQty: 0,
    unfilledQty: 10,
    bestBid: 0.4,
    bestAsk: 0.5,
    midPrice: 0.45,
    bookSnapshotTs: now - 2_000,
    partialFilled: false,
    matchLatencyMs: 1,
    serverRecvTs: now - 3_000,
    serverPublishTs: now - 2_900,
    createdAt: now - 3_000
  },
  {
    id: "order_filled",
    traceId: "trace_filled",
    userId: "user_b",
    roundId: "round_manual",
    symbol: "BTC",
    marketId: "market_manual",
    action: "buy",
    side: "DOWN",
    status: "filled",
    notionalUsdc: 3,
    expectedQty: 3,
    filledQty: 3,
    unfilledQty: 0,
    bestBid: 0.3,
    bestAsk: 0.4,
    midPrice: 0.35,
    bookSnapshotTs: now - 2_000,
    partialFilled: false,
    matchLatencyMs: 1,
    serverRecvTs: now - 3_000,
    serverPublishTs: now - 2_900,
    createdAt: now - 3_000
  }
];

const candidates = buildManualSettlementCandidates(
  [
    { ...baseRound, status: "Manual" },
    { ...baseRound, id: "round_other", status: "Polling" }
  ],
  positions,
  orders,
  10
);

assert.equal(candidates.length, 1);
assert.equal(candidates[0]?.roundId, "round_manual");
assert.equal(candidates[0]?.participantCount, 2);
assert.equal(candidates[0]?.openPositionCount, 2);
assert.equal(candidates[0]?.pendingOrderCount, 1);
assert.equal(candidates[0]?.upOpenQty, 2);
assert.equal(candidates[0]?.downOpenQty, 3);

const appSource = readFileSync("apps/client/src/App.tsx", "utf8");
assert.doesNotMatch(appSource, /className="manual-settle"/);
assert.doesNotMatch(appSource, /isManualSettlementPermissionError/);

console.log("manual-settlement-queue-check ok");

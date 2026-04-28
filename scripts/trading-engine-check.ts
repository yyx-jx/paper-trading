import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { estimateClobExecution } from "../apps/server/src/services/clob-execution";
import type { OrderAction, OrderBookSnapshot } from "../apps/server/src/domain/types";

const book: OrderBookSnapshot = {
  snapshotId: "book-test",
  snapshotTs: Date.now(),
  bestBid: 0.48,
  bestAsk: 0.5,
  midPrice: 0.49,
  bids: [
    { price: 0.48, qty: 100 },
    { price: 0.47, qty: 200 }
  ],
  asks: [
    { price: 0.5, qty: 100 },
    { price: 0.52, qty: 100 }
  ]
};

function estimate(input: { action: OrderAction; notional?: number; qty?: number; limitPrice?: number }) {
  return estimateClobExecution({
    ...input,
    book,
    orderId: `test-${input.action}-${input.notional ?? input.qty ?? "limit"}`,
    executedAt: Date.now()
  });
}

const buyFilled = estimate({ action: "buy", notional: 75 });
assert.equal(buyFilled.fullyMatched, true);
assert.equal(buyFilled.fills.length, 2);
assert.equal(buyFilled.filledQty, 148.07692308);
assert.equal(buyFilled.worstPrice, 0.52);
assert.equal(buyFilled.fills[0]?.makerOwnerId, "external:polymarket");

const buyAcrossLevels = estimate({ action: "buy", notional: 80 });
assert.equal(buyAcrossLevels.fullyMatched, true);
assert.equal(buyAcrossLevels.fills.length, 2);
assert.equal(buyAcrossLevels.avgPrice, 0.50731707);

const buyDepthFail = estimate({ action: "buy", notional: 500 });
assert.equal(buyDepthFail.fullyMatched, false);
assert.equal(buyDepthFail.failureReason, "Polymarket CLOB depth was insufficient for full fill.");

const buyLimitPending = estimate({ action: "buy", notional: 80, limitPrice: 0.5 });
assert.equal(buyLimitPending.fullyMatched, false);
assert.equal(buyLimitPending.failureReason, "Polymarket CLOB depth did not fully cross the limit price.");

const sellFilled = estimate({ action: "sell", qty: 250 });
assert.equal(sellFilled.fullyMatched, true);
assert.equal(sellFilled.fills.length, 2);
assert.equal(sellFilled.matchedNotional, 118.5);
assert.equal(sellFilled.worstPrice, 0.47);

const sellLimitPending = estimate({ action: "sell", qty: 250, limitPrice: 0.48 });
assert.equal(sellLimitPending.fullyMatched, false);
assert.equal(sellLimitPending.filledQty, 100);

assert.throws(
  () => estimate({ action: "buy", notional: 0 }),
  /Buy orders require positive notional/
);
assert.throws(
  () => estimate({ action: "sell", qty: 0 }),
  /Sell orders require positive quantity/
);

const pending: Array<{ action: OrderAction; notional?: number; qty?: number; limitPrice: number }> = Array.from(
  { length: 500 },
  (_, index) => ({
    action: index % 2 === 0 ? "buy" : "sell",
    notional: index % 2 === 0 ? 75 : undefined,
    qty: index % 2 === 0 ? undefined : 100,
    limitPrice: index % 2 === 0 ? 0.53 : 0.47
  })
);

const startedAt = performance.now();
const matches = pending.filter((order, index) =>
  estimateClobExecution({
    ...order,
    book,
    orderId: `stress-${index}`,
    executedAt: Date.now()
  }).fullyMatched
);
const elapsed = performance.now() - startedAt;

assert.equal(matches.length, 500);
assert.ok(elapsed < 1000, `stress estimate took ${elapsed}ms`);

console.log(`trading-engine-check ok: ${matches.length} pending orders evaluated in ${elapsed.toFixed(2)}ms`);

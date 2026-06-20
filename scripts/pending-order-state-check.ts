import { strict as assert } from "node:assert";
import {
  addPendingOrderClientId,
  latestPendingOrderClientId,
  removePendingOrderClientId
} from "../apps/client/src/features/trade/pending-order-state";

const first = addPendingOrderClientId([], "order-1");
assert.deepEqual(first, ["order-1"]);
assert.equal(latestPendingOrderClientId(first), "order-1");

const second = addPendingOrderClientId(first, "order-2");
assert.deepEqual(second, ["order-2", "order-1"]);
assert.equal(latestPendingOrderClientId(second), "order-2");

const deduped = addPendingOrderClientId(second, "order-1");
assert.deepEqual(deduped, ["order-1", "order-2"]);

const removedLatest = removePendingOrderClientId(deduped, "order-1");
assert.deepEqual(removedLatest, ["order-2"]);
assert.equal(latestPendingOrderClientId(removedLatest), "order-2");

const removedMissing = removePendingOrderClientId(removedLatest, "order-404");
assert.deepEqual(removedMissing, ["order-2"]);

const cleared = removePendingOrderClientId(removedLatest, "order-2");
assert.deepEqual(cleared, []);
assert.equal(latestPendingOrderClientId(cleared), undefined);

console.log("[pending-order-state-check] ok");

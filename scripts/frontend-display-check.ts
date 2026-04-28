import assert from "node:assert/strict";
import {
  ORDER_BOOK_STALE_WARNING_MS,
  isOrderBookStale,
  orderBookAgeMs,
  sourceFreshnessAlertKey,
  sourceFreshnessLabelKey
} from "../apps/client/src/utils/displayMetrics";

const snapshotTs = 1_000_000;
const book = { snapshotTs };

assert.equal(ORDER_BOOK_STALE_WARNING_MS, 2_000);
assert.equal(orderBookAgeMs(book, snapshotTs + 1_999), 1_999);
assert.equal(isOrderBookStale(book, snapshotTs + 2_000), false);
assert.equal(isOrderBookStale(book, snapshotTs + 2_001), true);
assert.equal(isOrderBookStale(undefined, snapshotTs + 10_000), false);

assert.equal(sourceFreshnessLabelKey("Chainlink"), "chainlinkFeedAge");
assert.equal(sourceFreshnessAlertKey("Chainlink"), "chainlinkFeedStale");
assert.equal(sourceFreshnessLabelKey("Binance"), "endToEnd");
assert.equal(sourceFreshnessAlertKey("CLOB"), "latencyOver3s");

console.log("frontend-display-check ok");

import { strict as assert } from "node:assert";
import {
  USER_HISTORY_PAGE_SIZE,
  buildPagedResult,
  normalizeHistoryPageQuery
} from "../apps/server/src/http/history-pagination";

const defaultPage = normalizeHistoryPageQuery({});
assert.equal(defaultPage.limit, USER_HISTORY_PAGE_SIZE);
assert.equal(defaultPage.offset, 0);

const explicitPage = normalizeHistoryPageQuery({ limit: "5", offset: "10" });
assert.equal(explicitPage.limit, 5);
assert.equal(explicitPage.offset, 10);

const rows = Array.from({ length: USER_HISTORY_PAGE_SIZE + 1 }, (_, index) => ({ id: `row-${index}` }));
const page = buildPagedResult(rows, defaultPage);
assert.equal(page.rows.length, USER_HISTORY_PAGE_SIZE);
assert.equal(page.hasMore, true);
assert.equal(page.nextOffset, USER_HISTORY_PAGE_SIZE);

const lastPage = buildPagedResult(rows.slice(0, 3), { limit: 25, offset: 50 });
assert.equal(lastPage.rows.length, 3);
assert.equal(lastPage.hasMore, false);
assert.equal(lastPage.nextOffset, undefined);

assert.throws(() => normalizeHistoryPageQuery({ limit: "501" }));
assert.throws(() => normalizeHistoryPageQuery({ offset: "-1" }));

console.log("[user-history-pagination-check] ok");

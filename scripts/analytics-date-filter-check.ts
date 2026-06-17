import assert from "node:assert/strict";
import {
  filterRowsByAnalyticsDate,
  resolveAnalyticsDateFilter
} from "../apps/client/src/utils/analyticsDateFilter";

const rowAt = (iso: string) => ({ ts: Date.parse(iso), label: iso });
const rows = [
  rowAt("2026-05-20T00:00:00.000Z"),
  rowAt("2026-05-20T23:59:59.999Z"),
  rowAt("2026-05-21T00:00:00.000Z"),
  rowAt("2025-05-20T12:00:00.000Z")
];

assert.deepEqual(resolveAnalyticsDateFilter({ year: "2026", month: "", day: "" }), {
  filter: { kind: "year", value: "2026" }
});
assert.deepEqual(resolveAnalyticsDateFilter({ year: "2026", month: "2026-05", day: "" }), {
  filter: { kind: "month", value: "2026-05" }
});
assert.deepEqual(resolveAnalyticsDateFilter({ year: "2026", month: "2026-05", day: "2026-05-20" }), {
  filter: { kind: "day", value: "2026-05-20" }
});
assert.deepEqual(resolveAnalyticsDateFilter({ year: "", month: "", day: "" }), {
  filter: { kind: "none" }
});

assert.deepEqual(resolveAnalyticsDateFilter({ year: "", month: "2026-13", day: "" }), {
  error: "month"
});
assert.deepEqual(resolveAnalyticsDateFilter({ year: "", month: "", day: "2026-02-31" }), {
  error: "day"
});

assert.deepEqual(
  filterRowsByAnalyticsDate(rows, { kind: "year", value: "2026" }).map((row) => row.label),
  [
    "2026-05-20T00:00:00.000Z",
    "2026-05-20T23:59:59.999Z",
    "2026-05-21T00:00:00.000Z"
  ]
);
assert.deepEqual(
  filterRowsByAnalyticsDate(rows, { kind: "month", value: "2026-05" }).map((row) => row.label),
  [
    "2026-05-20T00:00:00.000Z",
    "2026-05-20T23:59:59.999Z",
    "2026-05-21T00:00:00.000Z"
  ]
);
assert.deepEqual(
  filterRowsByAnalyticsDate(rows, { kind: "day", value: "2026-05-20" }).map((row) => row.label),
  ["2026-05-20T00:00:00.000Z", "2026-05-20T23:59:59.999Z"]
);
assert.deepEqual(filterRowsByAnalyticsDate(rows, { kind: "none" }), rows);

console.log("analytics date filter checks passed");

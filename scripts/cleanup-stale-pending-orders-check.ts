import assert from "node:assert/strict";
import { buildUnlockPlan, resolveBeforeTs, runCleanup } from "./cleanup-stale-pending-orders";

function testResolveBeforeTs() {
  assert.equal(resolveBeforeTs("2026-06-01T00:00:00+08:00"), 1_780_243_200_000);
}

function testBuildUnlockPlan() {
  const plan = buildUnlockPlan(
    [
      { id: "p1", locked_qty: 1.2, opened_at: 10 },
      { id: "p2", locked_qty: 2.3, opened_at: 20 }
    ],
    2
  );
  assert.deepEqual(plan, [
    { id: "p1", nextLockedQty: 0, releasedQty: 1.2 },
    { id: "p2", nextLockedQty: 1.5, releasedQty: 0.8 }
  ]);
}

function testBuildUnlockPlanFailsWhenShort() {
  assert.throws(
    () => buildUnlockPlan([{ id: "p1", locked_qty: 0.3, opened_at: 10 }], 0.5),
    /Insufficient locked quantity/
  );
}

async function testRunCleanupDryRun() {
  const queries: Array<{ text: string; params: unknown[] }> = [];
  const pool = {
    async query(text: string, params: unknown[]) {
      queries.push({ text, params });
      if (text.includes("COUNT(*)")) {
        return { rows: [{ count: "3" }] };
      }
      if (text.includes("SELECT id")) {
        return { rows: [{ id: "o1" }, { id: "o2" }, { id: "o3" }] };
      }
      throw new Error(`Unexpected query: ${text}`);
    }
  };
  const summary = await runCleanup(pool as never, {
    beforeTs: resolveBeforeTs("2026-06-01T00:00:00+08:00"),
    mode: "dry-run",
    batchSize: 50,
    sleepMs: 200,
    failureReason: "x"
  });
  assert.equal(summary.candidateCount, 3);
  assert.equal(summary.processedCount, 0);
  assert.deepEqual(summary.sampleOrderIds, ["o1", "o2", "o3"]);
  assert.equal(queries.length, 2);
}

async function main() {
  testResolveBeforeTs();
  testBuildUnlockPlan();
  testBuildUnlockPlanFailsWhenShort();
  await testRunCleanupDryRun();
  console.log("cleanup-stale-pending-orders-check ok");
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

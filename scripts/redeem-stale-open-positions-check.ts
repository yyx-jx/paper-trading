import assert from "node:assert/strict";
import { resolveBeforeTs, runRedeem } from "./redeem-stale-open-positions";

function testResolveBeforeTs() {
  assert.equal(resolveBeforeTs("2026-06-01T00:00:00+08:00"), 1_780_243_200_000);
}

async function testRunRedeemDryRun() {
  const queries: Array<{ text: string; params: unknown[] }> = [];
  const pool = {
    async query(text: string, params: unknown[]) {
      queries.push({ text, params });
      if (text.includes("WITH target_positions")) {
        return {
          rows: [
            {
              settled_round_count: "2",
              unsettled_round_count: "1",
              candidate_open_position_count: "5"
            }
          ]
        };
      }
      if (text.includes("SELECT") && text.includes("FROM rounds r") && text.includes("LIMIT")) {
        return {
          rows: [
            { round_id: "round-1" },
            { round_id: "round-2" }
          ]
        };
      }
      throw new Error(`Unexpected query: ${text}`);
    }
  };

  const summary = await runRedeem(pool as never, {
    beforeTs: resolveBeforeTs("2026-06-01T00:00:00+08:00"),
    mode: "dry-run",
    batchSize: 20,
    sleepMs: 200
  });

  assert.equal(summary.candidateRoundCount, 2);
  assert.equal(summary.candidateOpenPositionCount, 5);
  assert.equal(summary.unsettledRoundCount, 1);
  assert.equal(summary.processedRoundCount, 0);
  assert.deepEqual(summary.sampleRoundIds, ["round-1", "round-2"]);
  assert.equal(queries.length, 2);
}

async function main() {
  testResolveBeforeTs();
  await testRunRedeemDryRun();
  console.log("redeem-stale-open-positions-check ok");
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

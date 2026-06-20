import "dotenv/config";
import { setTimeout as sleep } from "node:timers/promises";
import { Pool, type PoolClient } from "pg";

type Mode = "dry-run" | "apply";
type TradeSide = "UP" | "DOWN";

type PendingOrderRow = {
  id: string;
  user_id: string;
  round_id: string;
  side: TradeSide;
  status: string;
  frozen_usdc: number | null;
  frozen_qty: number | null;
  created_at: number;
};

type PositionLockRow = {
  id: string;
  locked_qty: number | null;
  opened_at: number;
};

type CleanupConfig = {
  beforeTs: number;
  mode: Mode;
  batchSize: number;
  sleepMs: number;
  maxOrders?: number;
  failureReason: string;
};

type CleanupSummary = {
  mode: Mode;
  beforeTs: number;
  batchSize: number;
  sleepMs: number;
  candidateCount: number;
  processedCount: number;
  successCount: number;
  skippedCount: number;
  failureCount: number;
  refundedUsdc: number;
  releasedQty: number;
  sampleOrderIds: string[];
  failures: Array<{ orderId: string; reason: string }>;
};

type UnlockPlanEntry = {
  id: string;
  nextLockedQty: number;
  releasedQty: number;
};

const DEFAULT_BEFORE = "2026-06-01T00:00:00+08:00";
const DEFAULT_BATCH_SIZE = 50;
const DEFAULT_SLEEP_MS = 200;
const DEFAULT_FAILURE_REASON = `Maintenance cleanup for stale pending orders before ${DEFAULT_BEFORE}`;
const QTY_EPSILON = 0.0001;

function argValue(name: string) {
  const prefix = `--${name}=`;
  const inline = process.argv.find((item) => item.startsWith(prefix));
  if (inline) {
    return inline.slice(prefix.length);
  }
  const index = process.argv.indexOf(`--${name}`);
  return index >= 0 ? process.argv[index + 1] : undefined;
}

function roundQty(value: number) {
  return Math.round(value * 10_000) / 10_000;
}

function roundCurrency(value: number) {
  return Math.round(value * 100) / 100;
}

export function resolveBeforeTs(input = DEFAULT_BEFORE) {
  const ts = Date.parse(input);
  if (!Number.isFinite(ts)) {
    throw new Error(`Invalid --before value: ${input}`);
  }
  return ts;
}

export function buildUnlockPlan(positions: PositionLockRow[], frozenQty: number): UnlockPlanEntry[] {
  let remaining = roundQty(frozenQty);
  const plan: UnlockPlanEntry[] = [];
  for (const position of positions) {
    if (remaining <= QTY_EPSILON) {
      break;
    }
    const lockedQty = roundQty(Math.max(position.locked_qty ?? 0, 0));
    if (lockedQty <= QTY_EPSILON) {
      continue;
    }
    const releasedQty = roundQty(Math.min(lockedQty, remaining));
    if (releasedQty <= QTY_EPSILON) {
      continue;
    }
    plan.push({
      id: position.id,
      nextLockedQty: roundQty(Math.max(lockedQty - releasedQty, 0)),
      releasedQty
    });
    remaining = roundQty(Math.max(remaining - releasedQty, 0));
  }
  if (remaining > QTY_EPSILON) {
    throw new Error(`Insufficient locked quantity. Missing ${remaining.toFixed(4)}.`);
  }
  return plan;
}

function readConfig(): CleanupConfig {
  const modeInput = (argValue("mode") ?? "dry-run").trim();
  if (modeInput !== "dry-run" && modeInput !== "apply") {
    throw new Error(`Invalid --mode value: ${modeInput}`);
  }
  const batchSize = Number(argValue("batch-size") ?? DEFAULT_BATCH_SIZE);
  const sleepMs = Number(argValue("sleep-ms") ?? DEFAULT_SLEEP_MS);
  const maxOrdersRaw = argValue("max-orders");
  const maxOrders = typeof maxOrdersRaw === "string" ? Number(maxOrdersRaw) : undefined;
  if (!Number.isInteger(batchSize) || batchSize <= 0) {
    throw new Error(`Invalid --batch-size value: ${batchSize}`);
  }
  if (!Number.isInteger(sleepMs) || sleepMs < 0) {
    throw new Error(`Invalid --sleep-ms value: ${sleepMs}`);
  }
  if (typeof maxOrders !== "undefined" && (!Number.isInteger(maxOrders) || maxOrders <= 0)) {
    throw new Error(`Invalid --max-orders value: ${maxOrdersRaw}`);
  }
  return {
    beforeTs: resolveBeforeTs(argValue("before") ?? DEFAULT_BEFORE),
    mode: modeInput,
    batchSize,
    sleepMs,
    maxOrders,
    failureReason: DEFAULT_FAILURE_REASON
  };
}

async function countCandidates(pool: Pool, beforeTs: number) {
  const result = await pool.query<{ count: string }>(
    "SELECT COUNT(*)::text AS count FROM orders WHERE status = 'pending' AND created_at < $1",
    [beforeTs]
  );
  return Number(result.rows[0]?.count ?? "0");
}

async function listCandidateIds(pool: Pool, beforeTs: number, limit: number) {
  const result = await pool.query<{ id: string }>(
    `
      SELECT id
      FROM orders
      WHERE status = 'pending' AND created_at < $1
      ORDER BY created_at ASC, id ASC
      LIMIT $2
    `,
    [beforeTs, limit]
  );
  return result.rows.map((row) => row.id);
}

async function listBatch(pool: Pool, config: CleanupConfig, remaining?: number) {
  const limit = typeof remaining === "number" ? Math.min(config.batchSize, remaining) : config.batchSize;
  const result = await pool.query<PendingOrderRow>(
    `
      SELECT id, user_id, round_id, side, status, frozen_usdc, frozen_qty, created_at
      FROM orders
      WHERE status = 'pending' AND created_at < $1
      ORDER BY created_at ASC, id ASC
      LIMIT $2
    `,
    [config.beforeTs, limit]
  );
  return result.rows;
}

async function cleanupOrder(client: PoolClient, orderId: string, config: CleanupConfig) {
  await client.query("BEGIN");
  try {
    const orderResult = await client.query<PendingOrderRow>(
      `
        SELECT id, user_id, round_id, side, status, frozen_usdc, frozen_qty, created_at
        FROM orders
        WHERE id = $1
        FOR UPDATE
      `,
      [orderId]
    );
    const order = orderResult.rows[0];
    if (!order) {
      await client.query("ROLLBACK");
      return { outcome: "skipped" as const, refundedUsdc: 0, releasedQty: 0, reason: "Order not found" };
    }
    if (order.status !== "pending") {
      await client.query("ROLLBACK");
      return { outcome: "skipped" as const, refundedUsdc: 0, releasedQty: 0, reason: `Order already ${order.status}` };
    }

    const refundedUsdc = roundCurrency(Math.max(order.frozen_usdc ?? 0, 0));
    const releasedQty = roundQty(Math.max(order.frozen_qty ?? 0, 0));

    const userResult = await client.query<{ available_usdc: number }>(
      "SELECT available_usdc FROM users WHERE id = $1 FOR UPDATE",
      [order.user_id]
    );
    const user = userResult.rows[0];
    if (!user) {
      throw new Error(`User ${order.user_id} not found.`);
    }

    if (refundedUsdc > 0) {
      await client.query("UPDATE users SET available_usdc = $2, updated_at = $3 WHERE id = $1", [
        order.user_id,
        roundCurrency(Number(user.available_usdc) + refundedUsdc),
        Date.now()
      ]);
    }

    if (releasedQty > QTY_EPSILON) {
      const positionResult = await client.query<PositionLockRow>(
        `
          SELECT id, locked_qty, opened_at
          FROM positions
          WHERE user_id = $1 AND round_id = $2 AND side = $3 AND status = 'open'
          ORDER BY opened_at ASC, id ASC
          FOR UPDATE
        `,
        [order.user_id, order.round_id, order.side]
      );
      const plan = buildUnlockPlan(positionResult.rows, releasedQty);
      for (const step of plan) {
        await client.query("UPDATE positions SET locked_qty = $2 WHERE id = $1", [step.id, step.nextLockedQty]);
      }
    }

    const now = Date.now();
    await client.query(
      `
        UPDATE orders
        SET status = 'failed',
            lifecycle_status = 'failed',
            result_type = 'all_failed',
            failure_reason = $2,
            frozen_usdc = 0,
            frozen_qty = 0,
            server_publish_ts = $3
        WHERE id = $1
      `,
      [order.id, config.failureReason, now]
    );

    await client.query("COMMIT");
    return { outcome: "success" as const, refundedUsdc, releasedQty };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}

export async function runCleanup(pool: Pool, config: CleanupConfig): Promise<CleanupSummary> {
  const candidateCount = await countCandidates(pool, config.beforeTs);
  const summary: CleanupSummary = {
    mode: config.mode,
    beforeTs: config.beforeTs,
    batchSize: config.batchSize,
    sleepMs: config.sleepMs,
    candidateCount,
    processedCount: 0,
    successCount: 0,
    skippedCount: 0,
    failureCount: 0,
    refundedUsdc: 0,
    releasedQty: 0,
    sampleOrderIds: await listCandidateIds(pool, config.beforeTs, Math.min(config.maxOrders ?? 10, 10)),
    failures: []
  };

  if (config.mode === "dry-run") {
    return summary;
  }

  let remaining = config.maxOrders;
  while (typeof remaining !== "number" || remaining > 0) {
    const batch = await listBatch(pool, config, remaining);
    if (batch.length === 0) {
      break;
    }
    for (const order of batch) {
      try {
        const client = await pool.connect();
        try {
          const result = await cleanupOrder(client, order.id, config);
          summary.processedCount += 1;
          if (result.outcome === "success") {
            summary.successCount += 1;
            summary.refundedUsdc = roundCurrency(summary.refundedUsdc + result.refundedUsdc);
            summary.releasedQty = roundQty(summary.releasedQty + result.releasedQty);
          } else {
            summary.skippedCount += 1;
            summary.failures.push({ orderId: order.id, reason: result.reason });
          }
        } finally {
          client.release();
        }
      } catch (error) {
        summary.processedCount += 1;
        summary.failureCount += 1;
        summary.failures.push({
          orderId: order.id,
          reason: error instanceof Error ? error.message : "Unknown cleanup failure"
        });
      }
      if (typeof remaining === "number") {
        remaining -= 1;
        if (remaining <= 0) {
          break;
        }
      }
    }
    if ((typeof remaining !== "number" || remaining > 0) && config.sleepMs > 0) {
      await sleep(config.sleepMs);
    }
  }

  return summary;
}

async function main() {
  const config = readConfig();
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required.");
  }
  const pool = new Pool({ connectionString: databaseUrl });
  try {
    const summary = await runCleanup(pool, config);
    console.log(JSON.stringify(summary, null, 2));
  } finally {
    await pool.end();
  }
}

if (process.argv[1]?.includes("cleanup-stale-pending-orders.ts")) {
  void main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}

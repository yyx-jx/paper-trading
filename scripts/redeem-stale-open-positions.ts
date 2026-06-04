import "dotenv/config";
import { createHash } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { Pool, type PoolClient } from "pg";

type Mode = "dry-run" | "apply";
type TradeSide = "UP" | "DOWN";
type SettlementResult = "win" | "loss";

type CandidateRoundRow = {
  round_id: string;
  market_id: string;
  market_slug: string | null;
  symbol: string;
  end_at: number;
  status: string;
  settled_side: TradeSide | null;
  settlement_ts: number | null;
  settlement_price: number | null;
  redeem_finish_ts: number | null;
  open_position_count: number;
  participant_count: number;
};

type PositionRow = {
  id: string;
  user_id: string;
  round_id: string;
  side: TradeSide;
  qty: number;
  locked_qty: number | null;
  notional_spent: number;
  realized_pnl: number;
  status: string;
};

type UserRow = {
  id: string;
  role: string;
  available_usdc: number;
};

type LifecycleRow = {
  id: string;
  remaining_token_qty: number;
  closed_token_qty: number;
  exit_notional: number;
  exit_token_price: number | null;
  exit_type: string | null;
  fee_currency: string | null;
};

type RedeemConfig = {
  beforeTs: number;
  mode: Mode;
  batchSize: number;
  sleepMs: number;
  maxRounds?: number;
};

type RedeemSummary = {
  mode: Mode;
  beforeTs: number;
  batchSize: number;
  sleepMs: number;
  candidateRoundCount: number;
  candidateOpenPositionCount: number;
  unsettledRoundCount: number;
  processedRoundCount: number;
  successRoundCount: number;
  failureRoundCount: number;
  closedPositionCount: number;
  creditedUsdc: number;
  sampleRoundIds: string[];
  failures: Array<{ roundId: string; reason: string }>;
};

const DEFAULT_BEFORE = "2026-06-01T00:00:00+08:00";
const DEFAULT_BATCH_SIZE = 20;
const DEFAULT_SLEEP_MS = 200;
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

function readConfig(): RedeemConfig {
  const modeInput = (argValue("mode") ?? "dry-run").trim();
  if (modeInput !== "dry-run" && modeInput !== "apply") {
    throw new Error(`Invalid --mode value: ${modeInput}`);
  }
  const batchSize = Number(argValue("batch-size") ?? DEFAULT_BATCH_SIZE);
  const sleepMs = Number(argValue("sleep-ms") ?? DEFAULT_SLEEP_MS);
  const maxRoundsRaw = argValue("max-rounds");
  const maxRounds = typeof maxRoundsRaw === "string" ? Number(maxRoundsRaw) : undefined;
  if (!Number.isInteger(batchSize) || batchSize <= 0) {
    throw new Error(`Invalid --batch-size value: ${batchSize}`);
  }
  if (!Number.isInteger(sleepMs) || sleepMs < 0) {
    throw new Error(`Invalid --sleep-ms value: ${sleepMs}`);
  }
  if (typeof maxRounds !== "undefined" && (!Number.isInteger(maxRounds) || maxRounds <= 0)) {
    throw new Error(`Invalid --max-rounds value: ${maxRoundsRaw}`);
  }
  return {
    beforeTs: resolveBeforeTs(argValue("before") ?? DEFAULT_BEFORE),
    mode: modeInput,
    batchSize,
    sleepMs,
    maxRounds
  };
}

async function fetchSummaryCounts(pool: Pool, beforeTs: number) {
  const result = await pool.query<{
    settled_round_count: string;
    unsettled_round_count: string;
    candidate_open_position_count: string;
  }>(
    `
      WITH target_positions AS (
        SELECT p.id, p.round_id
        FROM positions p
        JOIN rounds r ON r.id = p.round_id
        WHERE p.status = 'open' AND r.end_at < $1
      ),
      target_rounds AS (
        SELECT DISTINCT r.id, r.settled_side
        FROM rounds r
        JOIN target_positions p ON p.round_id = r.id
      )
      SELECT
        COUNT(*) FILTER (WHERE settled_side IS NOT NULL)::text AS settled_round_count,
        COUNT(*) FILTER (WHERE settled_side IS NULL)::text AS unsettled_round_count,
        (SELECT COUNT(*) FROM target_positions)::text AS candidate_open_position_count
      FROM target_rounds
    `,
    [beforeTs]
  );
  const row = result.rows[0] ?? {
    settled_round_count: "0",
    unsettled_round_count: "0",
    candidate_open_position_count: "0"
  };
  return {
    settledRoundCount: Number(row.settled_round_count),
    unsettledRoundCount: Number(row.unsettled_round_count),
    candidateOpenPositionCount: Number(row.candidate_open_position_count)
  };
}

async function listCandidateRounds(pool: Pool, beforeTs: number, limit: number) {
  const result = await pool.query<CandidateRoundRow>(
    `
      SELECT
        r.id AS round_id,
        r.market_id,
        r.market_slug,
        r.symbol,
        r.end_at,
        r.status,
        r.settled_side,
        r.settlement_ts,
        r.settlement_price,
        r.redeem_finish_ts,
        COUNT(p.id)::int AS open_position_count,
        COUNT(DISTINCT p.user_id)::int AS participant_count
      FROM rounds r
      JOIN positions p ON p.round_id = r.id
      WHERE p.status = 'open'
        AND r.end_at < $1
        AND r.settled_side IS NOT NULL
      GROUP BY r.id
      ORDER BY r.end_at ASC, r.id ASC
      LIMIT $2
    `,
    [beforeTs, limit]
  );
  return result.rows;
}

async function fetchRoundPositions(client: PoolClient, roundId: string) {
  const result = await client.query<PositionRow>(
    `
      SELECT id, user_id, round_id, side, qty, locked_qty, notional_spent, realized_pnl, status
      FROM positions
      WHERE round_id = $1 AND status = 'open'
      ORDER BY user_id ASC, id ASC
      FOR UPDATE
    `,
    [roundId]
  );
  return result.rows;
}

async function fetchUser(client: PoolClient, userId: string) {
  const result = await client.query<UserRow>(
    "SELECT id, role, available_usdc FROM users WHERE id = $1 FOR UPDATE",
    [userId]
  );
  return result.rows[0];
}

async function claimRedeemLedger(
  client: PoolClient,
  input: {
    roundId: string;
    userId: string;
    positionId: string;
    redeemAmountUsdc: number;
    realizedPnlUsdc: number;
    settlementResult: SettlementResult;
    createdAtMs: number;
    details: Record<string, unknown>;
  }
) {
  const key = `${input.roundId}:${input.userId}:${input.positionId}`;
  const result = await client.query(
    `
      INSERT INTO redeem_ledger (
        id, round_id, user_id, position_id, redeem_amount_usdc, realized_pnl_usdc,
        settlement_result, created_at_ms, details
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
      ON CONFLICT (round_id, user_id, position_id) DO NOTHING
    `,
    [
      `redeem_${createHash("sha256").update(key).digest("hex").slice(0, 24)}`,
      input.roundId,
      input.userId,
      input.positionId,
      input.redeemAmountUsdc,
      input.realizedPnlUsdc,
      input.settlementResult,
      input.createdAtMs,
      JSON.stringify(input.details)
    ]
  );
  return (result.rowCount ?? 0) > 0;
}

async function settleOpenOrderLifecycles(
  client: PoolClient,
  input: {
    userId: string;
    roundId: string;
    side: TradeSide;
    settlementResult: SettlementResult;
    settlementDirection: TradeSide;
    settlementTimeMs: number;
    exitTokenPrice: number;
  }
) {
  const result = await client.query<LifecycleRow>(
    `
      SELECT id, remaining_token_qty, closed_token_qty, exit_notional, exit_token_price, exit_type, fee_currency
      FROM order_lifecycle_logs
      WHERE user_id = $1
        AND round_id = $2
        AND direction = $3
        AND remaining_token_qty > $4
      ORDER BY order_timestamp_ms ASC, id ASC
      FOR UPDATE
    `,
    [input.userId, input.roundId, input.side, QTY_EPSILON]
  );

  for (const log of result.rows) {
    const remaining = roundQty(log.remaining_token_qty);
    const nextClosedTokenQty = roundQty(log.closed_token_qty + remaining);
    const nextExitNotional = roundQty(log.exit_notional + remaining * input.exitTokenPrice);
    const nextExitTokenPrice = roundQty(nextExitNotional / Math.max(nextClosedTokenQty, QTY_EPSILON));
    await client.query(
      `
        UPDATE order_lifecycle_logs
        SET closed_token_qty = $2,
            exit_notional = $3,
            exit_token_price = $4,
            remaining_token_qty = 0,
            exit_type = $5,
            settlement_result = $6,
            settlement_direction = $7,
            settlement_time_ms = $8,
            fee_currency = COALESCE(fee_currency, 'USD'),
            updated_at = $9
        WHERE id = $1
      `,
      [
        log.id,
        nextClosedTokenQty,
        nextExitNotional,
        nextExitTokenPrice,
        log.exit_type && log.exit_type !== "settlement" ? "mixed" : "settlement",
        input.settlementResult,
        input.settlementDirection,
        input.settlementTimeMs,
        Date.now()
      ]
    );
  }
}

async function redeemRound(client: PoolClient, round: CandidateRoundRow) {
  const closedAt = Date.now();
  const settledSide = round.settled_side as TradeSide;
  const settlementTimeMs = round.settlement_ts ?? closedAt;
  const positions = await fetchRoundPositions(client, round.round_id);
  let closedPositionCount = 0;
  let creditedUsdc = 0;

  for (const position of positions) {
    const user = await fetchUser(client, position.user_id);
    if (!user) {
      throw new Error(`User ${position.user_id} not found for position ${position.id}`);
    }
    const isWinner = position.side === settledSide;
    const redeemAmount = isWinner ? roundCurrency(position.qty) : 0;
    const realizedPnl = roundCurrency(redeemAmount - position.notional_spent);
    const settlementResult: SettlementResult = isWinner ? "win" : "loss";
    const claimed = await claimRedeemLedger(client, {
      roundId: round.round_id,
      userId: user.id,
      positionId: position.id,
      redeemAmountUsdc: redeemAmount,
      realizedPnlUsdc: realizedPnl,
      settlementResult,
      createdAtMs: closedAt,
      details: {
        side: position.side,
        settledSide,
        marketId: round.market_id,
        marketSlug: round.market_slug
      }
    });
    if (!claimed) {
      continue;
    }

    await client.query("UPDATE users SET available_usdc = $2, updated_at = $3 WHERE id = $1", [
      user.id,
      roundCurrency(user.available_usdc + redeemAmount),
      closedAt
    ]);
    await client.query(
      `
        UPDATE positions
        SET realized_pnl = $2,
            unrealized_pnl = 0,
            cost_basis_usdc = 0,
            mark_pnl_usdc = 0,
            executable_pnl_usdc = 0,
            status = 'closed',
            closed_at = $3,
            current_mark = $4,
            current_bid = NULL,
            current_ask = NULL,
            current_mid = NULL,
            source_latency_ms = NULL,
            locked_qty = 0,
            current_value = $5,
            settlement_result = $6
        WHERE id = $1
      `,
      [
        position.id,
        roundCurrency(position.realized_pnl + realizedPnl),
        closedAt,
        isWinner ? 1 : 0,
        redeemAmount,
        settlementResult
      ]
    );
    await settleOpenOrderLifecycles(client, {
      userId: user.id,
      roundId: round.round_id,
      side: position.side,
      settlementResult,
      settlementDirection: settledSide,
      settlementTimeMs,
      exitTokenPrice: isWinner ? 1 : 0
    });
    closedPositionCount += 1;
    creditedUsdc = roundCurrency(creditedUsdc + redeemAmount);
  }

  await client.query(
    `
      UPDATE rounds
      SET redeem_finish_ts = $2,
          redeem_scheduled_at = COALESCE(redeem_scheduled_at, $2),
          status = 'Closed'
      WHERE id = $1
    `,
    [round.round_id, closedAt]
  );

  return { closedPositionCount, creditedUsdc };
}

export async function runRedeem(pool: Pool, config: RedeemConfig): Promise<RedeemSummary> {
  const counts = await fetchSummaryCounts(pool, config.beforeTs);
  const summary: RedeemSummary = {
    mode: config.mode,
    beforeTs: config.beforeTs,
    batchSize: config.batchSize,
    sleepMs: config.sleepMs,
    candidateRoundCount: counts.settledRoundCount,
    candidateOpenPositionCount: counts.candidateOpenPositionCount,
    unsettledRoundCount: counts.unsettledRoundCount,
    processedRoundCount: 0,
    successRoundCount: 0,
    failureRoundCount: 0,
    closedPositionCount: 0,
    creditedUsdc: 0,
    sampleRoundIds: (await listCandidateRounds(pool, config.beforeTs, Math.min(config.maxRounds ?? 10, 10))).map(
      (row) => row.round_id
    ),
    failures: []
  };

  if (config.mode === "dry-run") {
    return summary;
  }

  let remaining = config.maxRounds;
  while (typeof remaining !== "number" || remaining > 0) {
    const batch = await listCandidateRounds(
      pool,
      config.beforeTs,
      typeof remaining === "number" ? Math.min(config.batchSize, remaining) : config.batchSize
    );
    if (batch.length === 0) {
      break;
    }

    for (const round of batch) {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        const result = await redeemRound(client, round);
        await client.query("COMMIT");
        summary.processedRoundCount += 1;
        summary.successRoundCount += 1;
        summary.closedPositionCount += result.closedPositionCount;
        summary.creditedUsdc = roundCurrency(summary.creditedUsdc + result.creditedUsdc);
      } catch (error) {
        await client.query("ROLLBACK").catch(() => undefined);
        summary.processedRoundCount += 1;
        summary.failureRoundCount += 1;
        summary.failures.push({
          roundId: round.round_id,
          reason: error instanceof Error ? error.message : "Unknown redeem failure"
        });
      } finally {
        client.release();
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
    const summary = await runRedeem(pool, config);
    console.log(JSON.stringify(summary, null, 2));
  } finally {
    await pool.end();
  }
}

if (process.argv[1]?.includes("redeem-stale-open-positions.ts")) {
  void main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}

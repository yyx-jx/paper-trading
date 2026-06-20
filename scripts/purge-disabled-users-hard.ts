import "dotenv/config";
import { createHash } from "node:crypto";
import { createReadStream, createWriteStream, existsSync, mkdirSync, readdirSync, renameSync, rmSync } from "node:fs";
import path from "node:path";
import readline from "node:readline";
import { Pool, type PoolClient } from "pg";

type TargetUser = {
  id: string;
  username: string;
};

type TableCounts = {
  users: number;
  orders: number;
  positions: number;
  orderLifecycleLogs: number;
  auditEvents: number;
  redeemLedger: number;
  exportAuditLogs: number;
  behaviorActionLogs: number;
};

type JsonlSummary = {
  filePath: string;
  exists: boolean;
  totalLines: number;
  removedLines: number;
};

type ApplyCounts = TableCounts;

type Mode = "dry-run" | "apply";

const LOG_DIR = path.resolve(process.cwd(), process.env.PURGE_JSONL_DIR ?? "data/logs");
const AUDIT_LOG_PREFIX = process.env.PURGE_AUDIT_JSONL_PREFIX ?? "audit-events";
const BEHAVIOR_LOG_PREFIX = process.env.PURGE_BEHAVIOR_JSONL_PREFIX ?? "behavior-action-logs";

function usage() {
  console.log(`purge-disabled-users-hard

Usage:
  npx tsx scripts/purge-disabled-users-hard.ts --dry-run
  npx tsx scripts/purge-disabled-users-hard.ts --apply

Options:
  --dry-run   Scan disabled users and print the planned database/JSONL deletions.
  --apply     Delete disabled users and associated records, then rewrite current JSONL logs.
  --help      Show this message.
`);
}

function parseMode(argv: string[]): Mode {
  if (argv.includes("--help") || argv.includes("-h")) {
    usage();
    process.exit(0);
  }
  if (argv.includes("--apply")) {
    return "apply";
  }
  return "dry-run";
}

function anonymizeUserId(userId: string) {
  return createHash("sha256").update(`paper-trading:${userId}`).digest("hex").slice(0, 16);
}

function rawObject(value: unknown) {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

async function loadTargets(client: PoolClient): Promise<TargetUser[]> {
  const result = await client.query<TargetUser>(
    `
    SELECT id, username
    FROM users
    WHERE is_active = false
    ORDER BY username ASC
    `
  );
  return result.rows.map((row) => ({ id: String(row.id), username: String(row.username) }));
}

async function countTable(client: PoolClient, query: string, params: unknown[]) {
  const result = await client.query<{ count: string }>(query, params);
  return Number(result.rows[0]?.count ?? 0);
}

async function collectTableCounts(client: PoolClient, userIds: string[], anonIds: string[]): Promise<TableCounts> {
  if (userIds.length === 0) {
    return {
      users: 0,
      orders: 0,
      positions: 0,
      orderLifecycleLogs: 0,
      auditEvents: 0,
      redeemLedger: 0,
      exportAuditLogs: 0,
      behaviorActionLogs: 0
    };
  }

  const params = [userIds, anonIds];
  const users = await countTable(client, `SELECT count(*) FROM users WHERE id = ANY($1::text[])`, [userIds]);
  const orders = await countTable(client, `SELECT count(*) FROM orders WHERE user_id = ANY($1::text[])`, [userIds]);
  const positions = await countTable(client, `SELECT count(*) FROM positions WHERE user_id = ANY($1::text[])`, [userIds]);
  const orderLifecycleLogs = await countTable(
    client,
    `
    SELECT count(*)
    FROM order_lifecycle_logs
    WHERE user_id = ANY($1::text[]) OR tester_id = ANY($1::text[])
    `,
    [userIds]
  );
  const auditEvents = await countTable(
    client,
    `
    SELECT count(*)
    FROM audit_events
    WHERE user_id = ANY($1::text[])
       OR details ->> 'userId' = ANY($1::text[])
       OR details ->> 'targetUserId' = ANY($1::text[])
    `,
    [userIds]
  );
  const redeemLedger = await countTable(client, `SELECT count(*) FROM redeem_ledger WHERE user_id = ANY($1::text[])`, [userIds]);
  const exportAuditLogs = await countTable(
    client,
    `SELECT count(*) FROM export_audit_logs WHERE actor_user_id = ANY($1::text[])`,
    [userIds]
  );
  const behaviorActionLogs = await countTable(
    client,
    `SELECT count(*) FROM behavior_action_logs WHERE tester_id_anon = ANY($1::text[])`,
    [anonIds]
  );

  return {
    users,
    orders,
    positions,
    orderLifecycleLogs,
    auditEvents,
    redeemLedger,
    exportAuditLogs,
    behaviorActionLogs
  };
}

async function deleteRows(client: PoolClient, userIds: string[], anonIds: string[]): Promise<ApplyCounts> {
  const behaviorActionLogs = (
    await client.query(`DELETE FROM behavior_action_logs WHERE tester_id_anon = ANY($1::text[])`, [anonIds])
  ).rowCount;
  const exportAuditLogs = (
    await client.query(`DELETE FROM export_audit_logs WHERE actor_user_id = ANY($1::text[])`, [userIds])
  ).rowCount;
  const redeemLedger = (await client.query(`DELETE FROM redeem_ledger WHERE user_id = ANY($1::text[])`, [userIds])).rowCount;
  const auditEvents = (
    await client.query(
      `
      DELETE FROM audit_events
      WHERE user_id = ANY($1::text[])
         OR details ->> 'userId' = ANY($1::text[])
         OR details ->> 'targetUserId' = ANY($1::text[])
      `,
      [userIds]
    )
  ).rowCount;
  const positions = (await client.query(`DELETE FROM positions WHERE user_id = ANY($1::text[])`, [userIds])).rowCount;
  const orderLifecycleLogs = (
    await client.query(
      `
      DELETE FROM order_lifecycle_logs
      WHERE user_id = ANY($1::text[]) OR tester_id = ANY($1::text[])
      `,
      [userIds]
    )
  ).rowCount;
  const orders = (await client.query(`DELETE FROM orders WHERE user_id = ANY($1::text[])`, [userIds])).rowCount;
  const users = (await client.query(`DELETE FROM users WHERE id = ANY($1::text[])`, [userIds])).rowCount;

  return {
    users: users ?? 0,
    orders: orders ?? 0,
    positions: positions ?? 0,
    orderLifecycleLogs: orderLifecycleLogs ?? 0,
    auditEvents: auditEvents ?? 0,
    redeemLedger: redeemLedger ?? 0,
    exportAuditLogs: exportAuditLogs ?? 0,
    behaviorActionLogs: behaviorActionLogs ?? 0
  };
}

async function scanOrRewriteJsonl(
  filePath: string,
  apply: boolean,
  shouldRemove: (value: unknown) => boolean
): Promise<JsonlSummary> {
  if (!existsSync(filePath)) {
    return {
      filePath,
      exists: false,
      totalLines: 0,
      removedLines: 0
    };
  }

  const tempPath = `${filePath}.purge-tmp`;
  const reader = readline.createInterface({
    input: createReadStream(filePath, { encoding: "utf8" }),
    crlfDelay: Infinity
  });
  let writer: ReturnType<typeof createWriteStream> | undefined;
  if (apply) {
    mkdirSync(path.dirname(filePath), { recursive: true });
    writer = createWriteStream(tempPath, { encoding: "utf8" });
  }

  let totalLines = 0;
  let removedLines = 0;
  try {
    for await (const line of reader) {
      totalLines += 1;
      let remove = false;
      if (line.trim().length > 0) {
        try {
          remove = shouldRemove(JSON.parse(line));
        } catch {
          remove = false;
        }
      }
      if (remove) {
        removedLines += 1;
        continue;
      }
      if (writer) {
        writer.write(`${line}\n`);
      }
    }
    if (writer) {
      await new Promise<void>((resolve, reject) => {
        writer!.end(() => resolve());
        writer!.on("error", reject);
      });
      renameSync(tempPath, filePath);
    }
  } catch (error) {
    writer?.destroy();
    if (apply) {
      rmSync(tempPath, { force: true });
    }
    throw error;
  }

  return {
    filePath,
    exists: true,
    totalLines,
    removedLines
  };
}

function collectJsonlFiles(prefix: string) {
  if (!existsSync(LOG_DIR)) {
    return [] as string[];
  }
  return readdirSync(LOG_DIR)
    .filter((name) => name.startsWith(prefix) && name.endsWith(".jsonl"))
    .sort((left, right) => left.localeCompare(right))
    .map((name) => path.join(LOG_DIR, name));
}

function printTargets(targets: TargetUser[]) {
  console.log(`disabled users: ${targets.length}`);
  for (const target of targets) {
    console.log(` - ${target.username}`);
  }
}

function printTableCounts(label: string, counts: TableCounts) {
  console.log(label);
  console.table({
    users: counts.users,
    orders: counts.orders,
    positions: counts.positions,
    order_lifecycle_logs: counts.orderLifecycleLogs,
    audit_events: counts.auditEvents,
    redeem_ledger: counts.redeemLedger,
    export_audit_logs: counts.exportAuditLogs,
    behavior_action_logs: counts.behaviorActionLogs
  });
}

function printJsonlSummary(label: string, summaries: JsonlSummary[]) {
  console.log(label);
  console.table(
    summaries.map((summary) => ({
      file: summary.filePath,
      exists: summary.exists,
      totalLines: summary.totalLines,
      removedLines: summary.removedLines
    }))
  );
  const totals = summaries.reduce(
    (acc, summary) => {
      acc.files += summary.exists ? 1 : 0;
      acc.totalLines += summary.totalLines;
      acc.removedLines += summary.removedLines;
      return acc;
    },
    { files: 0, totalLines: 0, removedLines: 0 }
  );
  console.log(`jsonl totals: files=${totals.files} totalLines=${totals.totalLines} removedLines=${totals.removedLines}`);
}

async function main() {
  const mode = parseMode(process.argv.slice(2));
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required.");
  }

  const pool = new Pool({ connectionString: databaseUrl });
  try {
    const client = await pool.connect();
    try {
      const targets = await loadTargets(client);
      const userIds = targets.map((target) => target.id);
      const userIdSet = new Set(userIds);
      const anonIds = userIds.map((userId) => anonymizeUserId(userId));
      const anonIdSet = new Set(anonIds);

      printTargets(targets);

      const plannedCounts = await collectTableCounts(client, userIds, anonIds);
      printTableCounts("planned database deletions", plannedCounts);

      const auditFiles = collectJsonlFiles(AUDIT_LOG_PREFIX);
      const behaviorFiles = collectJsonlFiles(BEHAVIOR_LOG_PREFIX);
      const auditPredicate = (value: unknown) => {
        const event = rawObject(value);
        const details = rawObject(event.details);
        const directUserId = typeof event.userId === "string" ? event.userId : undefined;
        const detailUserId = typeof details.userId === "string" ? details.userId : undefined;
        const targetUserId = typeof details.targetUserId === "string" ? details.targetUserId : undefined;
        return Boolean(
          (directUserId && userIdSet.has(directUserId)) ||
            (detailUserId && userIdSet.has(detailUserId)) ||
            (targetUserId && userIdSet.has(targetUserId))
        );
      };
      const behaviorPredicate = (value: unknown) => {
        const log = rawObject(value);
        const testerIdAnon = typeof log.testerIdAnon === "string" ? log.testerIdAnon : undefined;
        return Boolean(testerIdAnon && anonIdSet.has(testerIdAnon));
      };
      const auditJsonl = await Promise.all(auditFiles.map((filePath) => scanOrRewriteJsonl(filePath, false, auditPredicate)));
      const behaviorJsonl = await Promise.all(
        behaviorFiles.map((filePath) => scanOrRewriteJsonl(filePath, false, behaviorPredicate))
      );
      printJsonlSummary("planned JSONL rewrites (audit)", auditJsonl);
      printJsonlSummary("planned JSONL rewrites (behavior)", behaviorJsonl);

      if (mode !== "apply") {
        console.log("purge-disabled-users-hard dry-run ok");
        return;
      }

      await client.query("BEGIN");
      let deletedCounts: ApplyCounts;
      try {
        deletedCounts = await deleteRows(client, userIds, anonIds);
        await client.query("COMMIT");
      } catch (error) {
        await client.query("ROLLBACK").catch(() => undefined);
        throw error;
      }
      printTableCounts("applied database deletions", deletedCounts);

      const appliedAuditJsonl = await Promise.all(auditFiles.map((filePath) => scanOrRewriteJsonl(filePath, true, auditPredicate)));
      const appliedBehaviorJsonl = await Promise.all(
        behaviorFiles.map((filePath) => scanOrRewriteJsonl(filePath, true, behaviorPredicate))
      );
      printJsonlSummary("applied JSONL rewrites (audit)", appliedAuditJsonl);
      printJsonlSummary("applied JSONL rewrites (behavior)", appliedBehaviorJsonl);

      const remainingCounts = await collectTableCounts(client, userIds, anonIds);
      printTableCounts("remaining database records for deleted users", remainingCounts);
      console.log("purge-disabled-users-hard apply ok");
    } finally {
      client.release();
    }
  } finally {
    await pool.end();
  }
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

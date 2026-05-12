import "dotenv/config";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, rmSync } from "node:fs";
import { mkdtemp } from "node:fs/promises";
import { readdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { Pool } from "pg";

const MIGRATIONS_DIR = path.resolve(process.cwd(), "db/migrations");
const EXPECTED_MIGRATION_ID = "000005";

type Migration = {
  id: string;
  filename: string;
  sql: string;
};

function requiredEnv(name: string) {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required. Use a disposable PostgreSQL database; this smoke test resets its schema.`);
  }
  return value;
}

function assertSmokeUrlSafe(smokeUrl: string) {
  const productionUrl = process.env.DATABASE_URL?.trim();
  if (productionUrl && smokeUrl === productionUrl) {
    throw new Error("MIGRATION_SMOKE_DATABASE_URL must not equal DATABASE_URL.");
  }
  const parsed = new URL(smokeUrl);
  if (!["postgres:", "postgresql:"].includes(parsed.protocol)) {
    throw new Error("MIGRATION_SMOKE_DATABASE_URL must be a PostgreSQL connection string.");
  }
}

function readMigrations() {
  const migrations = readdirSync(MIGRATIONS_DIR)
    .filter((name) => /^\d{6}_.+\.sql$/.test(name))
    .sort((left, right) => left.localeCompare(right))
    .map<Migration>((filename) => ({
      id: filename.slice(0, 6),
      filename,
      sql: readFileSync(path.join(MIGRATIONS_DIR, filename), "utf8")
    }));
  assert.ok(migrations.some((migration) => migration.id === EXPECTED_MIGRATION_ID));
  return migrations;
}

async function resetDatabase(databaseUrl: string) {
  const pool = new Pool({ connectionString: databaseUrl });
  try {
    await pool.query("DROP SCHEMA IF EXISTS public CASCADE");
    await pool.query("CREATE SCHEMA public");
    await pool.query("GRANT ALL ON SCHEMA public TO public");
  } finally {
    await pool.end();
  }
}

async function ensureMigrationTable(pool: Pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      id TEXT PRIMARY KEY,
      filename TEXT NOT NULL,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);
}

async function appliedMigrationIds(pool: Pool) {
  const result = await pool.query<{ id: string }>("SELECT id FROM schema_migrations ORDER BY id ASC");
  return new Set(result.rows.map((row) => row.id));
}

async function applyMigrations(databaseUrl: string, migrations: Migration[], throughId?: string) {
  const pool = new Pool({ connectionString: databaseUrl });
  try {
    await ensureMigrationTable(pool);
    const applied = await appliedMigrationIds(pool);
    for (const migration of migrations) {
      if (throughId && migration.id > throughId) {
        break;
      }
      if (applied.has(migration.id)) {
        continue;
      }
      await pool.query("BEGIN");
      try {
        await pool.query(migration.sql);
        await pool.query("INSERT INTO schema_migrations (id, filename) VALUES ($1, $2)", [
          migration.id,
          migration.filename
        ]);
        await pool.query("COMMIT");
      } catch (error) {
        await pool.query("ROLLBACK").catch(() => undefined);
        throw error;
      }
    }
  } finally {
    await pool.end();
  }
}

async function assertRequiredMigration(databaseUrl: string, expectedId = EXPECTED_MIGRATION_ID) {
  const pool = new Pool({ connectionString: databaseUrl });
  try {
    const tableResult = await pool.query<{ exists: boolean }>(
      "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'schema_migrations') AS exists"
    );
    if (!tableResult.rows[0]?.exists) {
      throw new Error("schema_migrations is missing");
    }
    const migrationResult = await pool.query("SELECT 1 FROM schema_migrations WHERE id = $1", [expectedId]);
    if (migrationResult.rowCount === 0) {
      throw new Error(`Required migration ${expectedId} is not applied`);
    }
  } finally {
    await pool.end();
  }
}

async function assertSchema(databaseUrl: string, expectedId = EXPECTED_MIGRATION_ID) {
  const pool = new Pool({ connectionString: databaseUrl });
  try {
    const migrationResult = await pool.query<{ id: string }>(
      "SELECT id FROM schema_migrations ORDER BY id DESC LIMIT 1"
    );
    assert.equal(migrationResult.rows[0]?.id, expectedId);

    const redeemTable = await pool.query<{ exists: boolean }>(
      "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'redeem_ledger') AS exists"
    );
    assert.equal(redeemTable.rows[0]?.exists, true);

    const pnlColumn = await pool.query<{ exists: boolean }>(
      "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'positions' AND column_name = 'executable_pnl_usdc') AS exists"
    );
    assert.equal(pnlColumn.rows[0]?.exists, true);

    const clientOrderColumn = await pool.query<{ exists: boolean }>(
      "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'client_order_id') AS exists"
    );
    assert.equal(clientOrderColumn.rows[0]?.exists, true);

    const clientOrderIndex = await pool.query<{ exists: boolean }>(
      "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'idx_orders_user_client_order_id') AS exists"
    );
    assert.equal(clientOrderIndex.rows[0]?.exists, true);

    const positionBuyOrderColumn = await pool.query<{ exists: boolean }>(
      "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'positions' AND column_name = 'buy_order_id') AS exists"
    );
    assert.equal(positionBuyOrderColumn.rows[0]?.exists, true);

    const positionBuyOrderIndex = await pool.query<{ exists: boolean }>(
      "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'idx_positions_buy_order_id') AS exists"
    );
    assert.equal(positionBuyOrderIndex.rows[0]?.exists, true);
  } finally {
    await pool.end();
  }
}

async function expectFailFast(databaseUrl: string, expectedMessage: RegExp) {
  await assert.rejects(() => assertRequiredMigration(databaseUrl), expectedMessage);
}

function runCommand(command: string, args: string[]) {
  try {
    execFileSync(command, args, { stdio: "pipe" });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`${command} ${args.join(" ")} failed. Ensure PostgreSQL client tools are installed. ${message}`);
  }
}

async function smokeBackupRestore(sourceUrl: string, restoreUrl: string) {
  const tempDir = await mkdtemp(path.join(tmpdir(), "btc-paper-migration-smoke-"));
  const dumpPath = path.join(tempDir, "migration-smoke.dump");
  try {
    runCommand("pg_dump", ["-Fc", "--file", dumpPath, sourceUrl]);
    assert.equal(existsSync(dumpPath), true);
    await resetDatabase(restoreUrl);
    runCommand("pg_restore", ["--clean", "--if-exists", "--dbname", restoreUrl, dumpPath]);
    await assertSchema(restoreUrl);
  } finally {
    rmSync(tempDir, { force: true, recursive: true });
  }
}

async function main() {
  const smokeUrl = requiredEnv("MIGRATION_SMOKE_DATABASE_URL");
  assertSmokeUrlSafe(smokeUrl);
  const restoreUrl = process.env.MIGRATION_SMOKE_RESTORE_DATABASE_URL?.trim() || smokeUrl;
  if (restoreUrl !== smokeUrl) {
    assertSmokeUrlSafe(restoreUrl);
  }

  const migrations = readMigrations();

  await resetDatabase(smokeUrl);
  await applyMigrations(smokeUrl, migrations);
  await assertRequiredMigration(smokeUrl);
  await assertSchema(smokeUrl);

  await resetDatabase(smokeUrl);
  await expectFailFast(smokeUrl, /schema_migrations is missing/);
  await applyMigrations(smokeUrl, migrations, "000002");
  await expectFailFast(smokeUrl, /Required migration 000005 is not applied/);
  await applyMigrations(smokeUrl, migrations);
  await assertSchema(smokeUrl);

  await smokeBackupRestore(smokeUrl, restoreUrl);

  console.log("migration-smoke-check ok");
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

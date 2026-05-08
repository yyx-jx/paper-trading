import "dotenv/config";
import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { Pool } from "pg";

const MIGRATIONS_DIR = path.resolve(process.cwd(), "db/migrations");

type Migration = {
  id: string;
  filename: string;
  path: string;
  sql: string;
};

function readMigrations(): Migration[] {
  return readdirSync(MIGRATIONS_DIR)
    .filter((name) => /^\d{6}_.+\.sql$/.test(name))
    .sort((left, right) => left.localeCompare(right))
    .map((filename) => ({
      id: filename.slice(0, 6),
      filename,
      path: path.join(MIGRATIONS_DIR, filename),
      sql: readFileSync(path.join(MIGRATIONS_DIR, filename), "utf8")
    }));
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

function assertMigrationFiles(migrations: Migration[]) {
  const ids = new Set<string>();
  for (const migration of migrations) {
    if (ids.has(migration.id)) {
      throw new Error(`Duplicate migration id: ${migration.id}`);
    }
    ids.add(migration.id);
  }
}

async function main() {
  const command = process.argv[2] ?? "up";
  const migrations = readMigrations();
  assertMigrationFiles(migrations);

  if (command === "check") {
    console.log(`migration-check ok: ${migrations.length} file(s)`);
    return;
  }

  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required.");
  }

  const pool = new Pool({ connectionString: databaseUrl });
  try {
    await ensureMigrationTable(pool);
    const applied = await appliedMigrationIds(pool);
    if (command === "status") {
      for (const migration of migrations) {
        console.log(`${applied.has(migration.id) ? "applied" : "pending"} ${migration.filename}`);
      }
      return;
    }
    if (command !== "up") {
      throw new Error(`Unsupported command: ${command}`);
    }

    for (const migration of migrations) {
      if (applied.has(migration.id)) {
        continue;
      }
      console.log(`applying ${migration.filename}`);
      await pool.query("BEGIN");
      try {
        await pool.query(migration.sql);
        await pool.query("INSERT INTO schema_migrations (id, filename) VALUES ($1, $2)", [
          migration.id,
          migration.filename
        ]);
        await pool.query("COMMIT");
      } catch (error) {
        await pool.query("ROLLBACK");
        throw error;
      }
    }
    console.log("db:migrate ok");
  } finally {
    await pool.end();
  }
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

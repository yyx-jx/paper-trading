import assert from "node:assert/strict";
import { AppStore } from "../apps/server/src/services/store";
import { AppMetrics } from "../apps/server/src/services/metrics";
import type { SourceHealth } from "../apps/server/src/domain/types";
import { readText } from "./deployment-assertions";

function createStore(overrides: Partial<ConstructorParameters<typeof AppStore>[0]> = {}) {
  return new AppStore({
    initialBalance: 10000,
    logRetentionMs: 300000,
    snapshotRetentionSeconds: 300,
    symbol: "BTC",
    databaseUrl: "postgresql://postgres:postgres@127.0.0.1:1/unavailable",
    redisUrl: "redis://127.0.0.1:1",
    persistenceMode: "external",
    coinbaseEnabled: true,
    strictPersistence: true,
    requireSchemaMigrations: true,
    allowDevSchemaBootstrap: false,
    expectedSchemaMigrationId: "000008",
    pgConnectionTimeoutMs: 25,
    pgIdleTimeoutMs: 25,
    pgMaxConnections: 1,
    pgKeepAlive: false,
    pgReconnectIntervalMs: 60_000,
    pgReconnectMaxIntervalMs: 60_000,
    orderBookSnapshotsMemoryMax: 10,
    orderBookSnapshotsMemoryMaxAgeMs: 60_000,
    ordersMemoryMax: 10,
    positionsMemoryMax: 10,
    auditLogsMemoryMax: 10,
    behaviorLogsMemoryMax: 10,
    orderLifecycleMemoryMax: 10,
    roundsMemoryMax: 10,
    serverHeapWarnMb: 768,
    serverHeapProtectMb: 1024,
    ...overrides
  });
}

function sourceHealth(source: SourceHealth["source"], state: SourceHealth["state"], ageMs: number): SourceHealth {
  const now = Date.now();
  return {
    source,
    symbol: "BTC",
    state,
    reconnectCount: state === "healthy" ? 0 : 3,
    sourceEventTs: now - ageMs,
    serverRecvTs: now - ageMs,
    normalizedTs: now - ageMs,
    serverPublishTs: now,
    acquireLatencyMs: 0,
    publishLatencyMs: 0,
    frontendLatencyMs: 0,
    message: state === "healthy" ? undefined : `${source} injected ${state}`
  };
}

async function assertSchemaGuardFails() {
  const missingStore = createStore();
  const outdatedStore = createStore();
  try {
    await assert.rejects(
      () =>
        (missingStore as unknown as { assertSchemaMigrations(pool: unknown): Promise<void> }).assertSchemaMigrations({
          query: async () => ({ rows: [{ exists: false }], rowCount: 1 })
        }),
      /schema_migrations is missing/
    );
    await assert.rejects(
      () =>
        (outdatedStore as unknown as { assertSchemaMigrations(pool: unknown): Promise<void> }).assertSchemaMigrations({
          query: async (sql: string) =>
            sql.includes("information_schema.tables")
              ? { rows: [{ exists: true }], rowCount: 1 }
              : { rows: [], rowCount: 0 }
        }),
      /Required migration 000008 is not applied/
    );
  } finally {
    await Promise.all([missingStore.close(), outdatedStore.close()]);
  }
}

async function assertStrictPersistenceFailsClosed() {
  const store = createStore();
  try {
    (store as unknown as { closed: boolean }).closed = true;
    assert.throws(() => store.assertWritablePersistence("Order placement"), /persistent storage is unavailable/);
    await assert.rejects(() => store.withTransaction(async () => "should-not-run"), /persistent storage is unavailable/);
  } finally {
    await store.close();
  }
}

async function assertRedisDegradedIsObservable() {
  const store = createStore({ strictPersistence: false });
  try {
    const internals = store as unknown as {
      redisEnabled: boolean;
      persistenceHealth: { redis: { enabled: boolean; writable: boolean; state: string; lastError?: string } };
    };
    internals.redisEnabled = false;
    internals.persistenceHealth.redis.enabled = false;
    internals.persistenceHealth.redis.writable = false;
    internals.persistenceHealth.redis.state = "blocked";
    internals.persistenceHealth.redis.lastError = "injected redis outage";

    const status = store.getPersistenceStatus();
    assert.equal(status.redis, false);
    assert.equal(status.state.redis.state, "blocked");
    assert.equal(status.state.redis.lastError, "injected redis outage");
  } finally {
    await store.close();
  }
}

async function assertExternalSourcesAreObservable() {
  const metrics = new AppMetrics();
  metrics.setRuntime({
    persistence: { postgres: true, redis: false },
    sources: [
      sourceHealth("Binance", "degraded", 20_000),
      sourceHealth("Coinbase", "stale", 120_000),
      sourceHealth("CLOB", "reconnecting", 45_000)
    ]
  });
  const text = await metrics.text();
  assert.match(text, /persistence_state\{target="redis"\} -1/);
  assert.match(text, /source_status\{source="binance",state="degraded"\} 1/);
  assert.match(text, /source_status\{source="coinbase",state="stale"\} 1/);
  assert.match(text, /source_status\{source="clob",state="reconnecting"\} 1/);
  assert.match(text, /source_stale_age_seconds\{source="coinbase"\}/);
}

function assertNoRequestStormPatterns() {
  const connectorFiles = [
    "apps/server/src/services/connectors/binance.ts",
    "apps/server/src/services/connectors/coinbase.ts",
    "apps/server/src/services/connectors/polymarket.ts"
  ];
  for (const file of connectorFiles) {
    const source = readText(file);
    assert.match(source, /timeout|Timeout|AbortSignal|AbortController|requestTimeout/i);
    assert.doesNotMatch(source, /while\s*\(\s*true\s*\)\s*\{[\s\S]{0,200}(fetch|request)/);
  }
}

async function main() {
  await assertSchemaGuardFails();
  await assertStrictPersistenceFailsClosed();
  await assertRedisDegradedIsObservable();
  await assertExternalSourcesAreObservable();
  assertNoRequestStormPatterns();
  console.log("fault-tolerance-check ok");
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

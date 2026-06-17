import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { AppStore } from "../apps/server/src/services/store";
import type { MarketCandleRecord } from "../apps/server/src/domain/types";

const migrationPath = "db/migrations/000007_market_candles.sql";

assert.equal(existsSync(migrationPath), true, "000007 market_candles migration should exist");

const migrationSource = readFileSync(migrationPath, "utf8");
assert.match(migrationSource, /CREATE TABLE IF NOT EXISTS market_candles/);
assert.match(migrationSource, /PRIMARY KEY \(source, symbol, interval, open_ts\)/);
assert.match(migrationSource, /CHECK \(open_ts % 30000 = 0\)/);
assert.match(migrationSource, /CHECK \(close_ts = open_ts \+ 30000\)/);
assert.match(migrationSource, /idx_market_candles_lookup/);

const storeSource = readFileSync("apps/server/src/services/store.ts", "utf8");
assert.match(storeSource, /CREATE TABLE IF NOT EXISTS market_candles/);
assert.match(storeSource, /upsertMarketCandles/);
assert.match(storeSource, /getMarketCandles/);

const simulationSource = readFileSync("apps/server/src/services/simulation.ts", "utf8");
assert.match(simulationSource, /flushPendingCoinbaseMarketCandles/);
assert.match(simulationSource, /refreshCoinbaseAggregateBucketFromThirtySecondBar/);
assert.match(simulationSource, /recordCoinbaseSample/);
assert.match(simulationSource, /syncCoinbaseHistoryCandles/);
assert.doesNotMatch(simulationSource, /currentRoundChainlinkOpenReferences/);
assert.doesNotMatch(simulationSource, /flushPendingChainlinkMarketCandles/);

function createMemoryStore() {
  return new AppStore({
    initialBalance: 1000,
    logRetentionMs: 60_000,
    snapshotRetentionSeconds: 60,
    symbol: "BTC",
    databaseUrl: "",
    redisUrl: "",
    persistenceMode: "memory",
    coinbaseEnabled: true,
    strictPersistence: false,
    seedDefaultUsers: false,
    requireSchemaMigrations: false,
    allowDevSchemaBootstrap: false,
    expectedSchemaMigrationId: "000007",
    pgConnectionTimeoutMs: 1000,
    pgIdleTimeoutMs: 1000,
    pgMaxConnections: 1,
    pgKeepAlive: false,
    pgReconnectIntervalMs: 1000,
    pgReconnectMaxIntervalMs: 1000,
    orderBookSnapshotsMemoryMax: 10,
    orderBookSnapshotsMemoryMaxAgeMs: 60_000,
    ordersMemoryMax: 10,
    positionsMemoryMax: 10,
    auditLogsMemoryMax: 10,
    behaviorLogsMemoryMax: 10,
    orderLifecycleMemoryMax: 10,
    roundsMemoryMax: 10,
    serverHeapWarnMb: 1024,
    serverHeapProtectMb: 2048
  });
}

const openTs = Math.floor(Date.now() / 30_000) * 30_000;
const fallback: MarketCandleRecord = {
  source: "coinbase",
  symbol: "BTC",
  interval: "30s",
  openTs,
  closeTs: openTs + 30_000,
  open: 100,
  high: 102,
  low: 99,
  close: 101,
  volume: 0,
  origin: "history_1m_split",
  updatedAt: openTs + 1
};
const realtime: MarketCandleRecord = {
  ...fallback,
  open: 110,
  high: 112,
  low: 109,
  close: 111,
  volume: 3,
  origin: "rtds_30s",
  updatedAt: openTs + 2
};

async function main() {
  const store = createMemoryStore();
  await store.upsertMarketCandles([fallback]);
  await store.upsertMarketCandles([realtime]);
  await store.upsertMarketCandles([{ ...fallback, open: 90, updatedAt: openTs + 3 }]);

  const candles = store.getMarketCandles({
    source: "coinbase",
    symbol: "BTC",
    interval: "30s",
    fromOpenTs: openTs - 1,
    toOpenTs: openTs + 1
  });

  assert.equal(candles.length, 1);
  assert.equal(candles[0]?.open, 110);
  assert.equal(candles[0]?.origin, "rtds_30s");
  assert.equal(candles[0]?.openTs % 30_000, 0);
  assert.equal(candles[0]?.closeTs, candles[0]!.openTs + 30_000);
  assert.ok([0, 30].includes(new Date(candles[0]!.openTs).getUTCSeconds()));
  assert.ok([0, 30].includes(new Date(candles[0]!.closeTs).getUTCSeconds()));

  console.log("market-candles-check ok");
}

void main();

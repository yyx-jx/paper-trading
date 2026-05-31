import assert from "node:assert/strict";
import { buildServerConfig, DEFAULT_COINBASE_REST_URL, DEFAULT_COINBASE_WS_URL } from "../apps/server/src/config";

const blankCoinbaseConfig = buildServerConfig({
  COINBASE_ENABLED: "true",
  COINBASE_WS_URL: "   ",
  COINBASE_REST_URL: ""
});

assert.equal(blankCoinbaseConfig.coinbaseEnabled, true);
assert.equal(blankCoinbaseConfig.coinbaseWsUrl, DEFAULT_COINBASE_WS_URL);
assert.equal(blankCoinbaseConfig.coinbaseRestUrl, DEFAULT_COINBASE_REST_URL);
assert.equal(blankCoinbaseConfig.requireSchemaMigrations, false);
assert.equal(blankCoinbaseConfig.expectedSchemaMigrationId, "000008");
assert.equal(blankCoinbaseConfig.manualSettlementTimeoutMs, 300000);

const disabledConfig = buildServerConfig({
  COINBASE_ENABLED: "false",
  COINBASE_WS_URL: "   ",
  UPSTREAM_PROXY_URL: "   "
});

assert.equal(disabledConfig.coinbaseEnabled, false);
assert.equal(disabledConfig.coinbaseWsUrl, DEFAULT_COINBASE_WS_URL);
assert.equal(disabledConfig.coinbaseRestUrl, DEFAULT_COINBASE_REST_URL);
assert.equal(disabledConfig.upstreamProxyUrl, undefined);

const migrationGuardConfig = buildServerConfig({
  SERVER_REQUIRE_MIGRATIONS: "true",
  EXPECTED_SCHEMA_MIGRATION_ID: "000123"
});

assert.equal(migrationGuardConfig.requireSchemaMigrations, true);
assert.equal(migrationGuardConfig.expectedSchemaMigrationId, "000123");

const productionConfig = buildServerConfig({
  NODE_ENV: "production",
  JWT_SECRET: "production-jwt-secret",
  CORS_ORIGINS: "http://<PRODUCTION_HOST>:10001",
  EXPORT_ANONYMIZATION_SECRET: "production-export-secret"
});

assert.equal(productionConfig.seedDefaultUsers, false);

console.log("config-check ok");

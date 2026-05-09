import assert from "node:assert/strict";
import { buildServerConfig, DEFAULT_CHAINLINK_FALLBACK_RPC_URLS, DEFAULT_CHAINLINK_RPC_URL } from "../apps/server/src/config";

const blankRpcConfig = buildServerConfig({
  CHAINLINK_ENABLED: "true",
  CHAINLINK_RPC_URL: "",
  CHAINLINK_FALLBACK_RPC_URLS: " , https://rpc.example/a ,  ,https://rpc.example/b "
});

assert.equal(blankRpcConfig.chainlinkEnabled, true);
assert.equal(blankRpcConfig.chainlinkRpcUrl, DEFAULT_CHAINLINK_RPC_URL);
assert.deepEqual(blankRpcConfig.chainlinkFallbackRpcUrls, ["https://rpc.example/a", "https://rpc.example/b"]);
assert.equal(blankRpcConfig.requireSchemaMigrations, false);
assert.equal(blankRpcConfig.expectedSchemaMigrationId, "000004");

const disabledConfig = buildServerConfig({
  CHAINLINK_ENABLED: "false",
  CHAINLINK_RPC_URL: "   ",
  UPSTREAM_PROXY_URL: "   "
});

assert.equal(disabledConfig.chainlinkEnabled, false);
assert.equal(disabledConfig.chainlinkRpcUrl, DEFAULT_CHAINLINK_RPC_URL);
assert.deepEqual(disabledConfig.chainlinkFallbackRpcUrls, DEFAULT_CHAINLINK_FALLBACK_RPC_URLS);
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
  CORS_ORIGINS: "http://103.147.13.98:10001",
  EXPORT_ANONYMIZATION_SECRET: "production-export-secret"
});

assert.equal(productionConfig.seedDefaultUsers, false);

console.log("config-check ok");

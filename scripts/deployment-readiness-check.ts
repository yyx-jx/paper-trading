import assert from "node:assert/strict";
import { assertProductionDeploymentBaseline, readText } from "./deployment-assertions";
import { readStoreServiceSource } from "./source-contracts";

assertProductionDeploymentBaseline();

const packageJson = readText("package.json");
const indexSource = readText("apps/server/src/index.ts");
const metricsSource = readText("apps/server/src/services/metrics.ts");
const storeSource = readStoreServiceSource();
const deploymentDoc = readText("docs/deployment-production.md");
const envExample = readText(".env.example");

assert.match(packageJson, /"test:deployment-readiness": "tsx scripts\/deployment-readiness-check\.ts"/);
assert.match(packageJson, /"test:fault-tolerance": "tsx scripts\/fault-tolerance-check\.ts"/);

for (const required of [
  "PUBLIC_DOMAIN",
  "CORS_ORIGINS",
  "JWT_SECRET",
  "EXPORT_ANONYMIZATION_SECRET",
  "SERVER_REQUIRE_MIGRATIONS",
  "SERVER_ALLOW_DEV_SCHEMA_BOOTSTRAP",
  "EXPECTED_SCHEMA_MIGRATION_ID"
]) {
  assert.match(envExample, new RegExp(`${required}=`));
  assert.match(deploymentDoc, new RegExp(required));
}

assert.match(indexSource, /app\.get\("\/api\/health\/live"/);
assert.match(indexSource, /app\.get\("\/api\/health\/ready"/);
assert.match(indexSource, /persistence\.postgres/);
assert.match(indexSource, /persistence\.state\.postgres\.state === "healthy"/);
assert.match(indexSource, /schemaMigration: serverConfig\.expectedSchemaMigrationId/);
assert.match(indexSource, /sources: store\.getSourceStatus\(\)/);
assert.match(indexSource, /matchingService: matching/);
assert.match(indexSource, /reply\.code\(503\)/);

for (const guardedWrite of [
  "Order placement",
  "Order cancellation",
  "Position sell",
  "Close side",
  "Reverse side"
]) {
  assert.match(indexSource, new RegExp(`store\\.assertWritablePersistence\\("${guardedWrite}"\\)`));
}

for (const metricsField of [
  "uptimeSec",
  "memoryMb",
  "wsClients",
  "orderLatencyP95Ms",
  "sourceHealth",
  "eventLoopLagMs",
  "jsonl",
  "persistence",
  "externalSources",
  "exports"
]) {
  assert.match(indexSource, new RegExp(`${metricsField}:`));
}

assert.match(metricsSource, /persistence_state/);
assert.match(metricsSource, /source_status/);
assert.match(metricsSource, /source_stale_age_seconds/);
assert.match(metricsSource, /jsonl_queue_depth/);
assert.match(metricsSource, /jsonl_rotation_total/);
assert.match(metricsSource, /jsonl_dropped_records_total/);
assert.match(metricsSource, /jsonl_backlog_state/);
assert.match(storeSource, /SERVER_REQUIRE_MIGRATIONS=true but schema_migrations is missing/);
assert.match(storeSource, /Required migration \$\{this\.config\.expectedSchemaMigrationId\} is not applied/);
assert.match(storeSource, /persistent storage is unavailable/);
assert.match(deploymentDoc, /Migration Smoke Test/);
assert.match(deploymentDoc, /Readiness And Fault Gates/);

console.log("deployment-readiness-check ok");

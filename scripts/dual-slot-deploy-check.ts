import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const compose = readFileSync("docker-compose.deploy.yml", "utf8");
const packageJson = readFileSync("package.json", "utf8");
const prodEnv = readFileSync(".env.production.example", "utf8");
const greenEnv = readFileSync(".env.green.example", "utf8");
const runbook = readFileSync("docs/dual-slot-green-prod-runbook.md", "utf8");

assert.match(packageJson, /"test:dual-slot-deploy": "tsx scripts\/dual-slot-deploy-check\.ts"/);

assert.match(compose, /image: \$\{APP_SERVER_IMAGE:-p-t-app-server:latest\}/);
assert.match(compose, /image: \$\{MATCHING_SERVICE_IMAGE:-p-t-matching-service:latest\}/);
assert.match(compose, /\$\{PUBLIC_BIND_HOST:-0\.0\.0\.0\}:\$\{PUBLIC_PORT:-10001\}:10001/);

for (const expected of [
  "PUBLIC_BIND_HOST=0.0.0.0",
  "PUBLIC_PORT=10001",
  "APP_SERVER_IMAGE=p-t-app-server:latest",
  "MATCHING_SERVICE_IMAGE=p-t-matching-service:latest"
]) {
  assert.match(prodEnv, new RegExp(expected.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
}

for (const expected of [
  "PUBLIC_PORT=10002",
  "POSTGRES_DB=paper_trading_green",
  "POSTGRES_USER=paper_trading_green",
  "APP_SERVER_IMAGE=p-t-app-server:0.6.3-rc1",
  "MATCHING_SERVICE_IMAGE=p-t-matching-service:0.6.3-rc1",
  "HYPER_BRIDGE_ENABLED=false"
]) {
  assert.match(greenEnv, new RegExp(expected.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
}

for (const expected of [
  "docker compose -p app-green",
  "docker compose -p app-prod",
  "--no-build --force-recreate",
  "PUBLIC_PORT=10002",
  "APP_SERVER_IMAGE=p-t-app-server:0.6.3-rc1"
]) {
  assert.match(runbook, new RegExp(expected.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
}

console.log("dual-slot-deploy-check ok");

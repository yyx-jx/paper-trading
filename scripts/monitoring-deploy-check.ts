import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const compose = readFileSync("docker-compose.deploy.yml", "utf8");
const prometheus = readFileSync("deploy/prometheus/prometheus.yml", "utf8");
const alerts = readFileSync("deploy/prometheus/alerts.yml", "utf8");
const dashboard = JSON.parse(readFileSync("deploy/grafana/dashboards/btc-paper-trading.json", "utf8")) as {
  title?: string;
  panels?: unknown[];
};
const caddy = readFileSync("deploy/Caddyfile", "utf8");
const docs = readFileSync("docs/monitoring-production.md", "utf8");

assert.match(compose, /prometheus:/);
assert.match(compose, /grafana:/);
assert.match(compose, /profiles:\s*\r?\n\s*- monitoring/);
assert.match(compose, /prometheus_data:/);
assert.match(compose, /grafana_data:/);
assert.match(compose, /app-server:\s*\r?\n\s*condition: service_healthy/);
assert.doesNotMatch(compose, /"9090:9090"/);
assert.doesNotMatch(compose, /"3000:3000"/);

assert.match(prometheus, /job_name: app-server/);
assert.match(prometheus, /metrics_path: \/metrics/);
assert.match(prometheus, /app-server:8787/);
assert.match(prometheus, /\/etc\/prometheus\/alerts\.yml/);

for (const alert of [
  "AppDown",
  "ReadinessFailed",
  "HighEventLoopLag",
  "HighHeapUsage",
  "PostgresUnavailable",
  "RedisUnavailable",
  "ExternalSourceStale",
  "HighOrderFailureRate",
  "JsonlQueueBacklog",
  "JsonlDroppedRecords",
  "WsBackpressure",
  "ExportFailures"
]) {
  assert.match(alerts, new RegExp(`alert: ${alert}`));
}

assert.equal(dashboard.title, "BTC Paper Trading Production");
assert.ok(Array.isArray(dashboard.panels));
assert.ok(dashboard.panels.length >= 5);
assert.match(caddy, /@metrics path \/metrics/);
assert.match(caddy, /respond @metrics 404/);
assert.match(docs, /docker compose -f docker-compose\.deploy\.yml --profile monitoring up -d --build/);

console.log("monitoring-deploy-check ok");

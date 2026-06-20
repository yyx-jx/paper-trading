import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { AppMetrics } from "../apps/server/src/services/metrics";
import { AsyncJsonlWriter } from "../apps/server/src/services/log-writer";

const metrics = new AppMetrics();
metrics.recordHttp("GET", "/api/health/ready", 200, 12);
metrics.setWsConnections("market", 2);
metrics.setWsConnections("user", 1);
metrics.recordWsSend("market", 512, 3, true);
metrics.recordWsDisconnect("user", "close");
metrics.recordOrder("filled", 35);
metrics.recordOrder("failed", 50);
metrics.recordTradePersistSegment("persistOrder", 12);
metrics.recordTradePersistSegment("persistPosition", 25);
metrics.setPendingOrders(4);
metrics.recordPositionClose("success");
metrics.recordExport("customer_dataset", "success", 25);
metrics.recordExport("internal_logs", "failed");
metrics.recordBulkImport("csv", "success");
metrics.setRuntime({
  persistence: { postgres: true, redis: false },
  sources: [
    {
      source: "Binance",
      state: "healthy",
      sourceEventTs: Date.now() - 1000,
      serverRecvTs: Date.now() - 900,
      serverPublishTs: Date.now(),
      frontendRecvTs: Date.now(),
      sourceLatencyMs: 100,
      backendLatencyMs: 10,
      frontendLatencyMs: 0,
      totalLatencyMs: 110
    },
    {
      source: "CLOB",
      state: "stale",
      sourceEventTs: Date.now() - 65000,
      serverRecvTs: Date.now() - 65000,
      serverPublishTs: Date.now(),
      frontendRecvTs: Date.now(),
      sourceLatencyMs: 0,
      backendLatencyMs: 0,
      frontendLatencyMs: 0,
      totalLatencyMs: 0
    }
  ]
});

async function main() {
  const tmp = await mkdtemp(join(tmpdir(), "btc-metrics-"));
  try {
    const writer = new AsyncJsonlWriter(join(tmp, "audit.jsonl"), { batchSize: 2, flushIntervalMs: 1000 });
    writer.write({ event: "one" });
    assert.equal(writer.getStats().queueDepth, 1);
    writer.write({ event: "two" });
    await writer.flush();
    const stats = writer.getStats();
    assert.equal(stats.queueDepth, 0);
    assert.equal(stats.flushCount, 1);
    assert.match(stats.currentFilePath, /audit-\d{4}-\d{2}-\d{2}\.jsonl$/);
    metrics.setJsonlStats({ audit: stats });
    metrics.setJsonlStats({
      audit: {
        ...stats,
        rotationCount: 2,
        droppedRecordCount: 3,
        backlog: true,
        currentFileBytes: stats.currentFileBytes + 10
      }
    });

    const payload = await readFile(stats.currentFilePath, "utf8");
    assert.match(payload, /"event":"one"/);
  } finally {
    await rm(tmp, { recursive: true, force: true });
  }

  const text = await metrics.text();
  for (const metricName of [
    "http_requests_total",
    "http_request_duration_seconds",
    "ws_connections",
    "ws_payload_bytes",
    "ws_send_duration_seconds",
    "ws_disconnects_total",
    "order_status_total",
    "order_place_duration_seconds",
    "trade_persist_segment_duration_seconds",
    "pending_orders_total",
    "position_close_requests_total",
    "export_requests_total",
    "bulk_import_requests_total",
    "jsonl_queue_depth",
    "jsonl_last_flush_duration_ms",
    "jsonl_rotation_total",
    "jsonl_dropped_records_total",
    "jsonl_backlog_state",
    "jsonl_current_file_bytes",
    "persistence_state",
    "source_stale_age_seconds",
    "nodejs_event_loop_lag_seconds"
  ]) {
    assert.match(text, new RegExp(`# HELP ${metricName}`));
    assert.match(text, new RegExp(`# TYPE ${metricName}`));
  }

  assert.match(text, /http_requests_total\{method="GET",route="\/api\/health\/ready",status="200"\} 1/);
  assert.match(text, /ws_connections\{channel="market"\} 2/);
  assert.match(text, /trade_persist_segment_duration_seconds_count\{segment="persistOrder"\} 1/);
  assert.match(text, /trade_persist_segment_duration_seconds_count\{segment="persistPosition"\} 1/);
  assert.match(text, /jsonl_queue_depth\{writer="audit"\} 0/);
  assert.match(text, /jsonl_rotation_total\{writer="audit"\} 2/);
  assert.match(text, /jsonl_dropped_records_total\{writer="audit"\} 3/);
  assert.match(text, /jsonl_backlog_state\{writer="audit"\} 1/);
  assert.match(text, /export_rows_total\{type="customer_dataset"\} 25/);
  assert.doesNotMatch(text, /app_heap_used_bytes/);
  assert.deepEqual(metrics.getExportOverview(), {
    customer_dataset: { success: 1, failed: 0, rows: 25 },
    internal_logs: { success: 0, failed: 1, rows: 0 }
  });

  const indexSource = await readFile("apps/server/src/index.ts", "utf8");
  const heartbeatSource = await readFile("apps/server/src/ws/heartbeat.ts", "utf8");
  for (const field of [
    "eventLoopLagMs",
    "http",
    "ws",
    "orders",
    "jsonl",
    "persistence",
    "externalSources",
    "exports"
  ]) {
    assert.match(indexSource, new RegExp(`${field}:`));
  }
  assert.match(indexSource, /app\.get\("\/metrics"/);
  assert.match(indexSource, /app\.get\("\/api\/metrics"/);
  assert.match(indexSource, /exports: appMetrics\.getExportOverview\(\)/);
  assert.doesNotMatch(indexSource, /metricsExposePublic/);
  assert.doesNotMatch(indexSource, /see \/metrics counters/);
  assert.match(indexSource, /consumeHeartbeatTimeout\(socket\)/);
  assert.match(indexSource, /createHeartbeatController/);
  assert.match(heartbeatSource, /heartbeatTimeoutSockets\.add\(socket\)/);
  assert.match(heartbeatSource, /recordDisconnect\?\.\(channel, "heartbeat_timeout"\)/);

  console.log("metrics-check ok");
}

void main();

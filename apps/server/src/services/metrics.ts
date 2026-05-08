import { monitorEventLoopDelay } from "node:perf_hooks";
import {
  collectDefaultMetrics,
  Counter,
  Gauge,
  Histogram,
  Registry
} from "prom-client";
import type { SourceHealth } from "../domain/types";

export type JsonlWriterStats = {
  queueDepth: number;
  maxQueueDepth?: number;
  flushCount: number;
  flushFailureCount: number;
  rotationCount?: number;
  droppedRecordCount?: number;
  backlog?: boolean;
  backlogSinceAt?: number;
  currentFilePath?: string;
  currentFileBytes?: number;
  flushing?: boolean;
  lastFlushDurationMs: number;
  lastFlushAt?: number;
  lastError?: string;
};

const SOURCE_STATES = ["healthy", "reconnecting", "stale", "degraded", "disabled", "blocked"] as const;

function stateValue(state: string) {
  if (state === "healthy") return 1;
  if (state === "degraded") return 0.5;
  if (state === "stale" || state === "reconnecting") return 0;
  return -1;
}

export class AppMetrics {
  readonly registry = new Registry();
  private readonly eventLoop = monitorEventLoopDelay({ resolution: 20 });
  private readonly jsonlFailureCounts = new Map<string, number>();
  private readonly jsonlRotationCounts = new Map<string, number>();
  private readonly jsonlDroppedCounts = new Map<string, number>();

  private readonly httpRequests = new Counter({
    name: "http_requests_total",
    help: "Total HTTP requests.",
    labelNames: ["method", "route", "status"],
    registers: [this.registry]
  });

  private readonly httpDuration = new Histogram({
    name: "http_request_duration_seconds",
    help: "HTTP request duration in seconds.",
    labelNames: ["method", "route", "status"],
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5],
    registers: [this.registry]
  });

  private readonly wsConnections = new Gauge({
    name: "ws_connections",
    help: "Current WebSocket connections by channel.",
    labelNames: ["channel"],
    registers: [this.registry]
  });

  private readonly wsPayloadBytes = new Histogram({
    name: "ws_payload_bytes",
    help: "WebSocket payload size in bytes.",
    labelNames: ["channel"],
    buckets: [128, 512, 1024, 4096, 16384, 65536, 262144],
    registers: [this.registry]
  });

  private readonly wsSendDuration = new Histogram({
    name: "ws_send_duration_seconds",
    help: "WebSocket send callback duration in seconds.",
    labelNames: ["channel", "status"],
    buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1],
    registers: [this.registry]
  });

  private readonly wsDisconnects = new Counter({
    name: "ws_disconnects_total",
    help: "WebSocket disconnects by channel and reason.",
    labelNames: ["channel", "reason"],
    registers: [this.registry]
  });

  private readonly orderStatus = new Counter({
    name: "order_status_total",
    help: "Order outcomes by status.",
    labelNames: ["status"],
    registers: [this.registry]
  });

  private readonly orderDuration = new Histogram({
    name: "order_place_duration_seconds",
    help: "Order placement duration in seconds.",
    labelNames: ["status"],
    buckets: [0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1, 2, 5],
    registers: [this.registry]
  });

  private readonly pendingOrders = new Gauge({
    name: "pending_orders_total",
    help: "Current pending orders.",
    registers: [this.registry]
  });

  private readonly exportRequests = new Counter({
    name: "export_requests_total",
    help: "Export requests by type and status.",
    labelNames: ["type", "status"],
    registers: [this.registry]
  });

  private readonly exportRows = new Counter({
    name: "export_rows_total",
    help: "Rows exported by export type.",
    labelNames: ["type"],
    registers: [this.registry]
  });

  private readonly bulkImportRequests = new Counter({
    name: "bulk_import_requests_total",
    help: "Bulk import requests by type and status.",
    labelNames: ["type", "status"],
    registers: [this.registry]
  });

  private readonly jsonlQueueDepth = new Gauge({
    name: "jsonl_queue_depth",
    help: "JSONL writer queue depth.",
    labelNames: ["writer"],
    registers: [this.registry]
  });

  private readonly jsonlFlushFailures = new Counter({
    name: "jsonl_write_failures_total",
    help: "JSONL writer flush failures.",
    labelNames: ["writer"],
    registers: [this.registry]
  });

  private readonly jsonlFlushDuration = new Gauge({
    name: "jsonl_last_flush_duration_ms",
    help: "Last JSONL flush duration in milliseconds.",
    labelNames: ["writer"],
    registers: [this.registry]
  });

  private readonly jsonlRotations = new Counter({
    name: "jsonl_rotation_total",
    help: "JSONL writer file rotations.",
    labelNames: ["writer"],
    registers: [this.registry]
  });

  private readonly jsonlDroppedRecords = new Counter({
    name: "jsonl_dropped_records_total",
    help: "JSONL records dropped because the writer queue was saturated or closed.",
    labelNames: ["writer"],
    registers: [this.registry]
  });

  private readonly jsonlBacklogState = new Gauge({
    name: "jsonl_backlog_state",
    help: "JSONL backlog state. 1 means queue is at or over the configured limit or drops occurred.",
    labelNames: ["writer"],
    registers: [this.registry]
  });

  private readonly jsonlCurrentFileBytes = new Gauge({
    name: "jsonl_current_file_bytes",
    help: "Current JSONL writer file size in bytes.",
    labelNames: ["writer"],
    registers: [this.registry]
  });

  private readonly persistenceState = new Gauge({
    name: "persistence_state",
    help: "Persistence state by dependency. 1 healthy, 0 degraded, -1 unavailable.",
    labelNames: ["target"],
    registers: [this.registry]
  });

  private readonly sourceStatus = new Gauge({
    name: "source_status",
    help: "External source status. Current state label is set to 1, others to 0.",
    labelNames: ["source", "state"],
    registers: [this.registry]
  });

  private readonly sourceStateValue = new Gauge({
    name: "source_state_value",
    help: "External source numeric status. 1 healthy, 0.5 degraded, 0 stale/reconnecting, -1 disabled.",
    labelNames: ["source"],
    registers: [this.registry]
  });

  private readonly sourceStaleAge = new Gauge({
    name: "source_stale_age_seconds",
    help: "External source event age in seconds.",
    labelNames: ["source"],
    registers: [this.registry]
  });

  private readonly eventLoopLag = new Gauge({
    name: "nodejs_event_loop_lag_seconds",
    help: "Event loop lag p95 in seconds.",
    registers: [this.registry]
  });

  private readonly positionCloseRequests = new Counter({
    name: "position_close_requests_total",
    help: "Position close, sell, reverse, or redeem requests by status.",
    labelNames: ["status"],
    registers: [this.registry]
  });

  private readonly exportCounts = new Map<string, { success: number; failed: number; rows: number }>();

  constructor() {
    collectDefaultMetrics({ register: this.registry });
    this.eventLoop.enable();
  }

  recordHttp(method: string, route: string, status: number, durationMs: number) {
    const labels = { method, route, status: String(status) };
    this.httpRequests.inc(labels);
    this.httpDuration.observe(labels, durationMs / 1000);
  }

  setWsConnections(channel: "market" | "user", count: number) {
    this.wsConnections.set({ channel }, count);
  }

  recordWsSend(channel: "market" | "user", bytes: number, durationMs: number, ok: boolean) {
    this.wsPayloadBytes.observe({ channel }, bytes);
    this.wsSendDuration.observe({ channel, status: ok ? "success" : "failed" }, durationMs / 1000);
  }

  recordWsDisconnect(channel: "market" | "user", reason: string) {
    this.wsDisconnects.inc({ channel, reason });
  }

  recordOrder(status: string, durationMs: number) {
    this.orderStatus.inc({ status });
    this.orderDuration.observe({ status }, durationMs / 1000);
  }

  setPendingOrders(count: number) {
    this.pendingOrders.set(count);
  }

  recordExport(type: string, status: "success" | "failed", rows = 0) {
    this.exportRequests.inc({ type, status });
    if (rows > 0) {
      this.exportRows.inc({ type }, rows);
    }
    const current = this.exportCounts.get(type) ?? { success: 0, failed: 0, rows: 0 };
    current[status] += 1;
    current.rows += rows;
    this.exportCounts.set(type, current);
  }

  recordBulkImport(type: string, status: "success" | "failed") {
    this.bulkImportRequests.inc({ type, status });
  }

  recordPositionClose(status: "success" | "failed") {
    this.positionCloseRequests.inc({ status });
  }

  getExportOverview() {
    return Object.fromEntries(
      [...this.exportCounts.entries()].map(([type, counts]) => [
        type,
        {
          success: counts.success,
          failed: counts.failed,
          rows: counts.rows
        }
      ])
    );
  }

  setJsonlStats(stats: Record<string, JsonlWriterStats>) {
    for (const [writer, stat] of Object.entries(stats)) {
      this.jsonlQueueDepth.set({ writer }, stat.queueDepth);
      this.jsonlFlushDuration.set({ writer }, stat.lastFlushDurationMs);
      this.jsonlBacklogState.set({ writer }, stat.backlog ? 1 : 0);
      this.jsonlCurrentFileBytes.set({ writer }, stat.currentFileBytes ?? 0);
      const previousFailures = this.jsonlFailureCounts.get(writer) ?? 0;
      const delta = Math.max(stat.flushFailureCount - previousFailures, 0);
      if (delta > 0) {
        this.jsonlFlushFailures.inc({ writer }, delta);
        this.jsonlFailureCounts.set(writer, stat.flushFailureCount);
      }
      const previousRotations = this.jsonlRotationCounts.get(writer) ?? 0;
      const rotationDelta = Math.max((stat.rotationCount ?? 0) - previousRotations, 0);
      if (rotationDelta > 0) {
        this.jsonlRotations.inc({ writer }, rotationDelta);
        this.jsonlRotationCounts.set(writer, stat.rotationCount ?? 0);
      }
      const previousDrops = this.jsonlDroppedCounts.get(writer) ?? 0;
      const dropDelta = Math.max((stat.droppedRecordCount ?? 0) - previousDrops, 0);
      if (dropDelta > 0) {
        this.jsonlDroppedRecords.inc({ writer }, dropDelta);
        this.jsonlDroppedCounts.set(writer, stat.droppedRecordCount ?? 0);
      }
    }
  }

  setRuntime(input: {
    persistence: { postgres: boolean; redis: boolean };
    sources: SourceHealth[];
  }) {
    this.persistenceState.set({ target: "postgres" }, input.persistence.postgres ? 1 : -1);
    this.persistenceState.set({ target: "redis" }, input.persistence.redis ? 1 : -1);
    const now = Date.now();
    for (const source of input.sources) {
      for (const state of SOURCE_STATES) {
        this.sourceStatus.set({ source: source.source.toLowerCase(), state }, source.state === state ? 1 : 0);
      }
      this.sourceStateValue.set({ source: source.source.toLowerCase() }, stateValue(source.state));
      this.sourceStaleAge.set({ source: source.source.toLowerCase() }, Math.max((now - source.sourceEventTs) / 1000, 0));
    }
    this.eventLoopLag.set(this.eventLoop.percentile(95) / 1e9);
  }

  getEventLoopLagMs() {
    return this.eventLoop.percentile(95) / 1e6;
  }

  async text() {
    return this.registry.metrics();
  }

  contentType() {
    return this.registry.contentType;
  }
}

export const appMetrics = new AppMetrics();

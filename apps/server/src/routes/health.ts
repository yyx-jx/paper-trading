import type { FastifyInstance } from "fastify";
import { serverConfig } from "../config";
import type { SimulationEngine } from "../services/simulation";
import type { AppStore } from "../services/store";

type HealthMetrics = {
  contentType(): string;
  text(): string | Promise<string>;
  getEventLoopLagMs(): number;
  getExportOverview(): unknown;
};

export function registerHealthRoutes(
  app: FastifyInstance,
  context: {
    store: AppStore;
    engine: SimulationEngine;
    metrics: HealthMetrics;
    wsConnectionCounts: { market: number; user: number };
    isShuttingDown: () => boolean;
    updateRuntimeMetrics: () => void;
    metricsAuthorized: (authHeader: string | string[] | undefined) => boolean;
  }
) {
  app.get("/health", async () => {
    const matching = await context.engine.getMatchingHealth().catch(() => undefined);
    const sources = context.store.getSourceStatus();
    const currentRound = context.store.getCurrentRound();
    const memory = context.store.getMemoryStatus();
    return {
      ok: true,
      serverNow: Date.now(),
      symbol: serverConfig.symbol,
      persistence: context.store.getPersistenceStatus(),
      heapUsedMb: memory.heapUsedMb,
      heapLimitMb: memory.heapLimitMb,
      memoryProtectionState: memory.memoryProtectionState,
      sources,
      currentRoundPresent: Boolean(currentRound),
      currentMarketSlug: context.store.marketSnapshot.marketSlug ?? null,
      lastSuccessfulUpdateTs:
        sources
          .filter((source) => source.state === "healthy" || source.state === "degraded")
          .map((source) => source.sourceEventTs)
          .sort((left, right) => right - left)[0] ?? 0,
      matchingService: matching
        ? {
            reachable: matching.ok,
            persistence: matching.persistence
          }
        : {
            reachable: false
          }
    };
  });

  app.get("/api/health/live", async () => ({
    ok: true,
    shuttingDown: context.isShuttingDown(),
    serverNow: Date.now(),
    uptimeSec: Math.round(process.uptime())
  }));

  app.get("/api/health/ready", async (_request, reply) => {
    const persistence = context.store.getPersistenceStatus();
    const matching = await context.engine.getMatchingHealth().catch(() => undefined);
    const persistenceReady =
      persistence.postgres ||
      (!serverConfig.isProduction && serverConfig.persistenceMode === "memory" && !serverConfig.strictPersistence);
    const ready =
      !context.isShuttingDown() &&
      persistenceReady &&
      (!serverConfig.strictPersistence || persistence.state.postgres.state === "healthy") &&
      (!serverConfig.embeddedMatchingService || Boolean(matching?.ok));
    if (!ready) {
      reply.code(503);
    }
    return {
      ok: ready,
      shuttingDown: context.isShuttingDown(),
      persistence,
      matchingService: matching ?? { ok: false },
      schemaMigration: serverConfig.expectedSchemaMigrationId,
      sources: context.store.getSourceStatus(),
      serverNow: Date.now()
    };
  });

  app.get("/api/metrics", async () => {
    context.updateRuntimeMetrics();
    const memory = context.store.getMemoryStatus();
    const profileCount = context.store.listUsers().length;
    const jsonlStats = context.store.getJsonlStats();
    const persistence = context.store.getPersistenceStatus();
    const sources = context.store.getSourceStatus();
    const orderLatencies = context.store
      .getRecentLogs("")
      .filter((log) => log.actionType === "place_order" && typeof log.backendLatencyMs === "number")
      .map((log) => log.backendLatencyMs as number)
      .sort((a, b) => a - b);
    const p95Index =
      orderLatencies.length > 0 ? Math.min(orderLatencies.length - 1, Math.ceil(orderLatencies.length * 0.95) - 1) : -1;
    return {
      uptimeSec: Math.round(process.uptime()),
      memoryMb: memory.heapUsedMb,
      heapLimitMb: memory.heapLimitMb,
      memoryProtectionState: memory.memoryProtectionState,
      wsClients: context.wsConnectionCounts.market + context.wsConnectionCounts.user,
      wsClientsByChannel: { ...context.wsConnectionCounts },
      users: profileCount,
      orderLatencyP95Ms: p95Index >= 0 ? orderLatencies[p95Index] : 0,
      eventLoopLagMs: context.metrics.getEventLoopLagMs(),
      http: {
        metricsEnabled: serverConfig.metricsEnabled
      },
      ws: {
        connections: context.wsConnectionCounts.market + context.wsConnectionCounts.user,
        byChannel: { ...context.wsConnectionCounts }
      },
      orders: {
        latencyP95Ms: p95Index >= 0 ? orderLatencies[p95Index] : 0,
        sampleCount: orderLatencies.length
      },
      jsonl: jsonlStats,
      persistence,
      externalSources: sources.map((source) => ({
        source: source.source,
        state: source.state,
        sourceEventAgeMs: Math.max(Date.now() - source.sourceEventTs, 0)
      })),
      exports: context.metrics.getExportOverview(),
      sourceHealth: Object.fromEntries(sources.map((source) => [source.source.toLowerCase(), source.state]))
    };
  });

  app.get("/metrics", async (request, reply) => {
    if (!serverConfig.metricsEnabled) {
      reply.code(404);
      return "metrics disabled";
    }
    if (!context.metricsAuthorized(request.headers.authorization)) {
      reply.header("www-authenticate", 'Basic realm="metrics"');
      reply.code(401);
      return "unauthorized";
    }
    context.updateRuntimeMetrics();
    reply.header("content-type", context.metrics.contentType());
    return context.metrics.text();
  });
}

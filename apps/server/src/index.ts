import cors from "@fastify/cors";
import websocket from "@fastify/websocket";
import Fastify from "fastify";
import jwt from "jsonwebtoken";
import { z } from "zod";
import { serverConfig } from "./config";
import type { AuditLogQuery, BehaviorLogQuery, Language, MarketSnapshot, SourceHealth, TradeSide, UserRecord } from "./domain/types";
import { createMatchingServiceApp } from "./services/matching/app";
import { MatchingServiceClient } from "./services/matching/client";
import { SimulationEngine } from "./services/simulation";
import { AppStore } from "./services/store";

const app = Fastify({ logger: false });

const store = new AppStore({
  initialBalance: serverConfig.initialBalance,
  logRetentionMs: serverConfig.logRetentionMs,
  snapshotRetentionSeconds: serverConfig.snapshotRetentionSeconds,
  symbol: serverConfig.symbol,
  databaseUrl: serverConfig.databaseUrl,
  redisUrl: serverConfig.redisUrl,
  chainlinkEnabled: serverConfig.chainlinkEnabled
});

const matchingClient = new MatchingServiceClient({
  baseUrl: serverConfig.matchingServiceUrl,
  timeoutMs: serverConfig.matchingServiceTimeoutMs
});

const engine = new SimulationEngine(store, matchingClient, {
  symbol: serverConfig.symbol,
  marketId: serverConfig.marketId,
  freezeWindowMs: serverConfig.freezeWindowMs,
  pollDelayMs: serverConfig.pollDelayMs,
  gammaPollIntervalMs: serverConfig.gammaPollIntervalMs,
  gammaMaxPolls: serverConfig.gammaMaxPolls,
  binanceRestUrl: serverConfig.binanceRestUrl,
  binanceWsUrl: serverConfig.binanceWsUrl,
  binanceRequestTimeoutMs: serverConfig.binanceRequestTimeoutMs,
  binanceRestPollMs: serverConfig.binanceRestPollMs,
  binanceWsStaleMs: serverConfig.binanceWsStaleMs,
  upstreamProxyUrl: serverConfig.upstreamProxyUrl,
  chainlinkEnabled: serverConfig.chainlinkEnabled,
  chainlinkRpcUrl: serverConfig.chainlinkRpcUrl,
  chainlinkFallbackRpcUrls: serverConfig.chainlinkFallbackRpcUrls,
  chainlinkRequestTimeoutMs: serverConfig.chainlinkRequestTimeoutMs,
  chainlinkBtcUsdProxyAddress: serverConfig.chainlinkBtcUsdProxyAddress as `0x${string}`,
  chainlinkPollMs: serverConfig.chainlinkPollMs,
  gammaBaseUrl: serverConfig.gammaBaseUrl,
  clobBaseUrl: serverConfig.clobBaseUrl,
  dataApiBaseUrl: serverConfig.dataApiBaseUrl,
  polymarketMarketId: serverConfig.polymarketMarketId,
  polymarketMarketSlug: serverConfig.polymarketMarketSlug,
  polymarketSearchQuery: serverConfig.polymarketSearchQuery,
  polymarketSeriesSlug: serverConfig.polymarketSeriesSlug,
  polymarketDiscoveryTimeoutMs: serverConfig.polymarketDiscoveryTimeoutMs,
  polymarketDiscoveryKeywords: serverConfig.polymarketDiscoveryKeywords,
  marketDiscoveryIntervalMs: serverConfig.marketDiscoveryIntervalMs,
  polymarketBookPollMs: serverConfig.polymarketBookPollMs,
  polymarketTradesPollMs: serverConfig.polymarketTradesPollMs
});

const loginSchema = z.object({
  username: z.string().min(1),
  password: z.string().min(1)
});

const orderSchema = z.object({
  action: z.enum(["buy", "sell"]).optional(),
  side: z.enum(["UP", "DOWN"]),
  orderKind: z.enum(["market", "limit"]).optional(),
  amount: z.number().positive().optional(),
  qty: z.number().positive().optional(),
  limitPrice: z.number().positive().optional(),
  clientSendTs: z.number().optional()
});

const languageSchema = z.object({
  language: z.enum(["zh-CN", "en-US"])
});

const quickSideSchema = z.object({
  side: z.enum(["UP", "DOWN"]),
  clientSendTs: z.number().optional()
});

const trainingLogQuerySchema = z.object({
  from: z.coerce.number().optional(),
  to: z.coerce.number().optional(),
  userId: z.string().optional(),
  roundId: z.string().optional(),
  actionType: z.string().optional(),
  actionStatus: z.enum(["success", "failed", "timeout"]).optional(),
  traceId: z.string().optional(),
  orderId: z.string().optional(),
  marketId: z.string().optional(),
  marketSlug: z.string().optional()
});

const auditLogQuerySchema = z.object({
  from: z.coerce.number().optional(),
  to: z.coerce.number().optional(),
  userId: z.string().optional(),
  roundId: z.string().optional(),
  category: z.enum(["operation", "matching", "settlement", "latency"]).optional(),
  actionType: z.string().optional(),
  actionStatus: z.enum(["success", "failed", "timeout"]).optional(),
  traceId: z.string().optional(),
  orderId: z.string().optional(),
  positionId: z.string().optional(),
  resultCode: z.string().optional()
});

const tradeTimelineQuerySchema = z.object({
  orderId: z.string().min(1)
});

const matchingQuerySchema = z.object({
  side: z.enum(["UP", "DOWN"]).optional(),
  bookKey: z.string().optional(),
  roundId: z.string().optional(),
  marketId: z.string().optional(),
  fromSequence: z.coerce.number().optional(),
  toSequence: z.coerce.number().optional(),
  limit: z.coerce.number().optional()
});

function signToken(user: UserRecord) {
  return jwt.sign({ userId: user.id, role: user.role }, serverConfig.jwtSecret, {
    expiresIn: "12h"
  });
}

function readToken(raw?: string) {
  if (!raw) {
    return undefined;
  }
  if (raw.startsWith("Bearer ")) {
    return raw.slice("Bearer ".length);
  }
  return raw;
}

function getUserFromRequest(request: { headers: Record<string, string | string[] | undefined> }) {
  const token = readToken(
    typeof request.headers.authorization === "string" ? request.headers.authorization : undefined
  );
  if (!token) {
    throw new Error("Missing authorization token.");
  }
  const payload = jwt.verify(token, serverConfig.jwtSecret) as { userId: string };
  const user = store.getUserById(payload.userId);
  if (!user) {
    throw new Error("User session is invalid.");
  }
  return user;
}

function requirePermission(user: UserRecord, code: string) {
  if (!user.permissionCodes.includes(code as never)) {
    throw new Error(`Missing permission: ${code}`);
  }
}

function canViewAllLogs(user: UserRecord) {
  return user.role === "Admin" || user.role === "Test Engineer" || user.permissionCodes.includes("audit:view");
}

function stampSourceForTransport(source: SourceHealth, serverPublishTs: number): SourceHealth {
  return {
    ...source,
    serverPublishTs,
    frontendLatencyMs: 0
  };
}

function stampSnapshotForTransport(snapshot: MarketSnapshot, serverPublishTs = Date.now()): MarketSnapshot {
  return {
    ...snapshot,
    sources: {
      binance: stampSourceForTransport(snapshot.sources.binance, serverPublishTs),
      chainlink: stampSourceForTransport(snapshot.sources.chainlink, serverPublishTs),
      clob: stampSourceForTransport(snapshot.sources.clob, serverPublishTs)
    }
  };
}

async function recordLoginAudit(input: {
  username?: string;
  user?: UserRecord;
  success: boolean;
  serverRecvTs: number;
  resultMessage: string;
}) {
  const serverPublishTs = Date.now();
  await store.recordLog({
    eventId: store.newId("evt"),
    traceId: store.newTraceId(),
    category: "operation",
    actionType: "login",
    actionStatus: input.success ? "success" : "failed",
    userId: input.user?.id,
    role: input.user?.role,
    pageName: "auth.login",
    moduleName: "login.form",
    resultCode: input.success ? "LOGIN_SUCCESS" : "LOGIN_FAILED",
    resultMessage: input.resultMessage,
    serverRecvTs: input.serverRecvTs,
    serverPublishTs,
    backendLatencyMs: Math.max(serverPublishTs - input.serverRecvTs, 0),
    details: {
      username: input.user?.username ?? input.username,
      userId: input.user?.id,
      role: input.user?.role,
      serverRecvTs: input.serverRecvTs,
      resultCode: input.success ? "LOGIN_SUCCESS" : "LOGIN_FAILED",
      failureReason: input.success ? undefined : input.resultMessage
    }
  });
}

async function safeRoute<T>(handler: () => Promise<T>) {
  try {
    return await handler();
  } catch (error) {
    return {
      error: true,
      message: error instanceof Error ? error.message : "Unexpected server error."
    } as T;
  }
}

function warnForLocalMisconfiguration() {
  if (serverConfig.upstreamProxyUrl) {
    console.warn(`[startup] Using upstream proxy for Binance/Polymarket: ${serverConfig.upstreamProxyUrl}`);
  }
  if (!serverConfig.chainlinkEnabled) {
    console.warn("[startup] Testing mode: Chainlink disabled. Local success will depend on Binance and Polymarket only.");
    return;
  }

  const warnings: string[] = [];
  if (
    !serverConfig.chainlinkRpcUrl ||
    serverConfig.chainlinkRpcUrl.includes("YOUR_PRIMARY_KEY") ||
    serverConfig.chainlinkRpcUrl.includes("YOUR_API_KEY") ||
    serverConfig.chainlinkRpcUrl.includes("YOUR_ALCHEMY_KEY")
  ) {
    warnings.push("CHAINLINK_RPC_URL is still a placeholder.");
  }
  if (
    serverConfig.chainlinkFallbackRpcUrls.length === 0 ||
    serverConfig.chainlinkFallbackRpcUrls.some(
      (url) => url.includes("YOUR_FALLBACK_KEY") || url.includes("YOUR_API_KEY") || url.includes("YOUR_INFURA_KEY")
    )
  ) {
    warnings.push("CHAINLINK_FALLBACK_RPC_URLS is missing or still contains placeholders.");
  }
  if (warnings.length === 0) {
    return;
  }

  console.warn("[startup] Full real-source local success is not possible until the following items are fixed:");
  for (const warning of warnings) {
    console.warn(`[startup] - ${warning}`);
  }
}

let shuttingDown = false;
let matchingRuntime: Awaited<ReturnType<typeof createMatchingServiceApp>> | undefined;

const shutdown = async () => {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  await engine.stop();
  await store.close();
  await matchingRuntime?.close().catch(() => undefined);
  await app.close().catch(() => undefined);
};

process.on("SIGINT", () => {
  void shutdown();
});
process.on("SIGTERM", () => {
  void shutdown();
});

async function bootstrap() {
  warnForLocalMisconfiguration();
  if (serverConfig.embeddedMatchingService) {
    matchingRuntime = await createMatchingServiceApp({
      databaseUrl: serverConfig.databaseUrl,
      redisUrl: serverConfig.redisUrl,
      redisSnapshotSeconds: serverConfig.snapshotRetentionSeconds
    });
    await matchingRuntime.app.listen({
      host: "0.0.0.0",
      port: serverConfig.matchingServicePort
    });
  }

  await store.init();

  await app.register(cors, {
    origin: true,
    credentials: true
  });
  await app.register(websocket);

  app.get("/health", async () => {
    const matching = await engine.getMatchingHealth().catch(() => undefined);
    const sources = store.getSourceStatus();
    const currentRound = store.getCurrentRound();
    return {
      ok: true,
      serverNow: Date.now(),
      symbol: serverConfig.symbol,
      persistence: store.getPersistenceStatus(),
      sources,
      currentRoundPresent: Boolean(currentRound),
      currentMarketSlug: store.marketSnapshot.marketSlug ?? null,
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

  app.post("/api/auth/login", async (request, reply) => {
    const serverRecvTs = Date.now();
    const parsed = loginSchema.safeParse(request.body);
    if (!parsed.success) {
      const rawUsername = (request.body as { username?: unknown } | undefined)?.username;
      await recordLoginAudit({
        username: typeof rawUsername === "string" ? rawUsername : undefined,
        success: false,
        serverRecvTs,
        resultMessage: "Invalid login payload."
      });
      reply.code(400);
      return { message: "Invalid login payload." };
    }
    const user = store.findUserByCredentials(parsed.data.username, parsed.data.password);
    if (!user) {
      await recordLoginAudit({
        username: parsed.data.username,
        success: false,
        serverRecvTs,
        resultMessage: "Invalid username or password."
      });
      reply.code(401);
      return { message: "Invalid username or password.", code: "AUTH_FAILED" };
    }

    const token = signToken(user);
    await recordLoginAudit({
      user,
      success: true,
      serverRecvTs,
      resultMessage: "Login succeeded."
    });
    return {
      token,
      user_id: user.id,
      role: user.role,
      language: user.language,
      display_name: user.displayName,
      permission_codes: user.permissionCodes
    };
  });

  app.get("/api/me", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      return store.sanitizeUser(user);
    })
  );

  app.post("/api/me/language", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      const parsed = languageSchema.parse(request.body);
      await engine.updateLanguage(user, parsed.language as Language);
      store.emitUserPayload(user.id);
      return store.sanitizeUser(user);
    })
  );

  app.get("/api/rounds/current", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "trade:view");
      return {
        currentRound: store.getCurrentRound(),
        snapshot: stampSnapshotForTransport(store.marketSnapshot)
      };
    })
  );

  app.get("/api/rounds/history", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "trade:view");
      const limit = Number((request.query as { limit?: string }).limit ?? 10);
      return store.getHistory(limit, user.id);
    })
  );

  app.get("/api/profile/me", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "profile:view");
      return store.getProfile(user.id);
    })
  );

  app.get("/api/positions/me", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "profile:view");
      return store.getPositions(user.id);
    })
  );

  app.get("/api/orders/me", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "profile:view");
      return store.getOrders(user.id);
    })
  );

  app.post("/api/orders", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "trade:order");
      const parsed = orderSchema.parse(request.body);
      return await engine.placeOrder(
        user,
        parsed as {
          action?: "buy" | "sell";
          side: TradeSide;
          orderKind?: "market" | "limit";
          amount?: number;
          qty?: number;
          limitPrice?: number;
          clientSendTs?: number;
        }
      );
    })
  );

  app.post("/api/orders/:id/cancel", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "trade:cancel");
      const params = request.params as { id: string };
      return await engine.cancelOrder(user, params.id);
    })
  );

  app.post("/api/positions/:id/sell", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "trade:sell");
      const params = request.params as { id: string };
      return await engine.sellPosition(user, params.id);
    })
  );

  app.post("/api/positions/close-side", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "trade:sell");
      const parsed = quickSideSchema.parse(request.body);
      return await engine.closeSide(user, parsed as { side: TradeSide; clientSendTs?: number });
    })
  );

  app.post("/api/positions/reverse-side", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "trade:sell");
      requirePermission(user, "trade:order");
      const parsed = quickSideSchema.parse(request.body);
      return await engine.reverseSide(user, parsed as { side: TradeSide; clientSendTs?: number });
    })
  );

  app.get("/api/logs/me", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "profile:view");
      return store.getRecentLogs(user.id);
    })
  );

  app.get("/api/logs/training", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "system:status:view");
      const parsed = trainingLogQuerySchema.parse(request.query) as BehaviorLogQuery;
      return store.getBehaviorLogs(parsed);
    })
  );

  app.get("/api/logs/training/export", async (request, reply) => {
    try {
      const user = getUserFromRequest(request);
      requirePermission(user, "system:status:view");
      const parsed = trainingLogQuerySchema.parse(request.query) as BehaviorLogQuery;
      const logs = store.getBehaviorLogs(parsed);
      const body = logs.map((log) => JSON.stringify(log)).join("\n");
      reply
        .header("Content-Type", "application/x-ndjson; charset=utf-8")
        .header(
          "Content-Disposition",
          `attachment; filename="behavior-action-logs-${new Date().toISOString().slice(0, 10)}.jsonl"`
        );
      return reply.send(body ? `${body}\n` : "");
    } catch (error) {
      reply.code(400);
      return {
        error: true,
        message: error instanceof Error ? error.message : "Training log export failed."
      };
    }
  });

  app.get("/api/logs/audit", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      const parsed = auditLogQuerySchema.parse(request.query) as AuditLogQuery;
      const filters = canViewAllLogs(user) ? parsed : { ...parsed, userId: user.id };
      return store.getAuditLogs(filters);
    })
  );

  app.get("/api/logs/audit/export", async (request, reply) => {
    try {
      const user = getUserFromRequest(request);
      const parsed = auditLogQuerySchema.parse(request.query) as AuditLogQuery;
      const filters = canViewAllLogs(user) ? parsed : { ...parsed, userId: user.id };
      const logs = store.getAuditLogs(filters);
      const body = logs.map((log) => JSON.stringify(log)).join("\n");
      reply
        .header("Content-Type", "application/x-ndjson; charset=utf-8")
        .header(
          "Content-Disposition",
          `attachment; filename="audit-events-${new Date().toISOString().slice(0, 10)}.jsonl"`
        );
      return reply.send(body ? `${body}\n` : "");
    } catch (error) {
      reply.code(400);
      return {
        error: true,
        message: error instanceof Error ? error.message : "Audit log export failed."
      };
    }
  });

  app.get("/api/logs/trade-timeline", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      const parsed = tradeTimelineQuerySchema.parse(request.query);
      const timeline = store.getTradeTimeline(parsed.orderId);
      if (!timeline) {
        throw new Error("Order timeline was not found.");
      }
      if (!canViewAllLogs(user) && timeline.order.userId !== user.id) {
        throw new Error("Order timeline is not available for this user.");
      }
      const matchingReplay = timeline.order.bookKey
        ? await engine
            .getMatchingReplay({
              bookKey: timeline.order.bookKey,
              roundId: timeline.order.roundId,
              marketId: timeline.order.marketId,
              limit: 80
            })
            .catch(() => undefined)
        : undefined;
      return {
        ...timeline,
        matchingReplay
      };
    })
  );

  app.get("/api/system/sources/status", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "system:status:view");
      return store.getSourceStatus();
    })
  );

  app.get("/api/system/market/latency", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "system:status:view");
      return {
        serverNow: Date.now(),
        currentRoundPresent: Boolean(store.getCurrentRound()),
        currentMarketSlug: store.marketSnapshot.marketSlug ?? null,
        sources: store.getSourceStatus().map((source) => ({
          ...source,
          lastSuccessTs:
            source.state === "healthy" || source.state === "degraded" ? source.sourceEventTs : undefined
        }))
      };
    })
  );

  app.get("/api/matching/books/current", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "system:status:view");
      const parsed = matchingQuerySchema.parse(request.query);
      return await engine.getCurrentMatchingBookState({
        side: parsed.side as TradeSide | undefined,
        bookKey: parsed.bookKey,
        roundId: parsed.roundId,
        marketId: parsed.marketId
      });
    })
  );

  app.get("/api/matching/books/replay", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "system:status:view");
      const parsed = matchingQuerySchema.parse(request.query);
      return await engine.getMatchingReplay({
        side: parsed.side as TradeSide | undefined,
        bookKey: parsed.bookKey,
        roundId: parsed.roundId,
        marketId: parsed.marketId,
        fromSequence: parsed.fromSequence,
        toSequence: parsed.toSequence,
        limit: parsed.limit
      });
    })
  );

  app.get("/ws/market", { websocket: true }, (socket, request) => {
    try {
      const query = request.query as { token?: string };
      const token = readToken(query.token);
      if (!token) {
        socket.close();
        return;
      }
      const payload = jwt.verify(token, serverConfig.jwtSecret) as { userId: string };
      const user = store.getUserById(payload.userId);
      if (!user) {
        socket.close();
        return;
      }

      const sendPayload = () => {
        socket.send(
          JSON.stringify({
            type: "market",
            data: {
              snapshot: stampSnapshotForTransport(store.marketSnapshot),
              currentRound: store.getCurrentRound(),
              history: store.getHistory(10, user.id)
            }
          })
        );
      };

      const listener = () => sendPayload();
      sendPayload();
      store.emitter.on("market:update", listener);
      socket.on("close", () => {
        store.emitter.off("market:update", listener);
      });
    } catch {
      socket.close();
    }
  });

  app.get("/ws/user", { websocket: true }, (socket, request) => {
    try {
      const query = request.query as { token?: string };
      const token = readToken(query.token);
      if (!token) {
        socket.close();
        return;
      }
      const payload = jwt.verify(token, serverConfig.jwtSecret) as { userId: string };
      const user = store.getUserById(payload.userId);
      if (!user) {
        socket.close();
        return;
      }

      const eventName = `user:${user.id}`;
      const sendPayload = () => {
        socket.send(
          JSON.stringify({
            type: "user",
            data: {
              profile: store.getProfile(user.id),
              positions: store.getPositions(user.id),
              orders: store.getOrders(user.id),
              logs: store.getRecentLogs(user.id)
            }
          })
        );
      };

      const listener = () => sendPayload();
      sendPayload();
      store.emitter.on(eventName, listener);
      socket.on("close", () => {
        store.emitter.off(eventName, listener);
      });
    } catch {
      socket.close();
    }
  });

  await engine.start();
  await app.listen({
    host: "0.0.0.0",
    port: serverConfig.port
  });
}

bootstrap().catch(async (error) => {
  console.error(error);
  await shutdown();
  process.exit(1);
});

import cors from "@fastify/cors";
import { createHash } from "node:crypto";
import websocket from "@fastify/websocket";
import Fastify from "fastify";
import jwt from "jsonwebtoken";
import { nanoid } from "nanoid";
import { WebSocket as WsWebSocket } from "ws";
import { z } from "zod";
import { serverConfig } from "./config";
import { ApiError, sendApiError } from "./http-errors";
import { assertCan, assertCanManageUser } from "./auth/authz";
import { hasPermission } from "./auth/permissions";
import { canChangeUserGroupForActor, canCreateUserForActor, canExportUser, canViewUserRecords, getVisibleUserIdsForActor } from "./auth/scope";
import type {
  AuditLogQuery,
  BehaviorLogQuery,
  BookLevel,
  CandleBar,
  CandleInterval,
  Language,
  LogSearchQuery,
  LogSearchResult,
  LogSystem,
  MarketPayload,
  MarketRealtimeTick,
  MarketSnapshot,
  MarketTickPayload,
  MarketTransportMeta,
  MatchingEventRecord,
  PermissionLevel,
  Role,
  RoundRecord,
  SettlementPreview,
  SourceHealth,
  TradeSide,
  UnifiedLogRow,
  UserRecord,
  UserTradePayload,
  DatasetExportRequest
} from "./domain/types";
import { createMatchingServiceApp } from "./services/matching/app";
import { MatchingServiceClient } from "./services/matching/client";
import { SimulationEngine } from "./services/simulation";
import { AppStore, type UserPayloadScope } from "./services/store";
import {
  buildExportEntries,
  createZipArchive,
  filterOrdersForExport,
  filterPositionsForExport,
  filterRoundsForExport,
  resolveExportUsers,
  type ExportQuery,
  type ExportUser,
  type UserExportData
} from "./services/csv-zip-export";
import { CSV_BULK_USER_TEMPLATE, parseBulkUsersCsv, validateBulkCreateUsers } from "./services/bulk-users";
import { LOG_FACETS } from "./services/log-facets";
import { buildDatasetExport, previewDatasetExport } from "./services/dataset-export";
import { appMetrics } from "./services/metrics";

const app = Fastify({
  logger: false,
  trustProxy: serverConfig.trustProxy,
  requestTimeout: serverConfig.requestTimeoutMs
});
const rateBuckets = new Map<string, { count: number; resetAt: number }>();
let shuttingDown = false;
const wsConnectionCounts = {
  market: 0,
  user: 0
};
const wsTickets = new Map<string, { userId: string; viewedUserId: string; channel: "market" | "user"; expiresAt: number }>();
const WS_TICKET_TTL_MS = 60_000;
const WS_HEARTBEAT_MS = 25_000;
const httpStartTimes = new WeakMap<object, number>();
const heartbeatTimeoutSockets = new WeakSet<WsWebSocket>();

function logStartupStage(stage: string) {
  console.log(`[startup] ${new Date().toISOString()} ${stage}`);
}

function clientKey(request: { ip?: string; headers: Record<string, string | string[] | undefined> }, suffix: string) {
  const forwarded = typeof request.headers["x-forwarded-for"] === "string" ? request.headers["x-forwarded-for"].split(",")[0]?.trim() : undefined;
  return `${forwarded || request.ip || "unknown"}:${suffix}`;
}

function enforceRateLimit(key: string, max: number, windowMs: number) {
  const now = Date.now();
  const bucket = rateBuckets.get(key);
  if (!bucket || bucket.resetAt <= now) {
    rateBuckets.set(key, { count: 1, resetAt: now + windowMs });
    return;
  }
  bucket.count += 1;
  if (bucket.count > max) {
    throw new ApiError(429, "Too many requests. Please retry later.", "RATE_LIMITED");
  }
}

const store = new AppStore({
  initialBalance: serverConfig.initialBalance,
  logRetentionMs: serverConfig.logRetentionMs,
  snapshotRetentionSeconds: serverConfig.snapshotRetentionSeconds,
  symbol: serverConfig.symbol,
  databaseUrl: serverConfig.databaseUrl,
  redisUrl: serverConfig.redisUrl,
  persistenceMode: serverConfig.persistenceMode,
  coinbaseEnabled: serverConfig.coinbaseEnabled,
  strictPersistence: serverConfig.strictPersistence,
  seedDefaultUsers: serverConfig.seedDefaultUsers,
  requireSchemaMigrations: serverConfig.requireSchemaMigrations,
  allowDevSchemaBootstrap: serverConfig.allowDevSchemaBootstrap,
  expectedSchemaMigrationId: serverConfig.expectedSchemaMigrationId,
  pgConnectionTimeoutMs: serverConfig.pgConnectionTimeoutMs,
  pgIdleTimeoutMs: serverConfig.pgIdleTimeoutMs,
  pgMaxConnections: serverConfig.pgMaxConnections,
  pgKeepAlive: serverConfig.pgKeepAlive,
  pgReconnectIntervalMs: serverConfig.pgReconnectIntervalMs,
  pgReconnectMaxIntervalMs: serverConfig.pgReconnectMaxIntervalMs,
  orderBookSnapshotsMemoryMax: serverConfig.orderBookSnapshotsMemoryMax,
  orderBookSnapshotsMemoryMaxAgeMs: serverConfig.orderBookSnapshotsMemoryMaxAgeMs,
  ordersMemoryMax: serverConfig.ordersMemoryMax,
  positionsMemoryMax: serverConfig.positionsMemoryMax,
  auditLogsMemoryMax: serverConfig.auditLogsMemoryMax,
  behaviorLogsMemoryMax: serverConfig.behaviorLogsMemoryMax,
  orderLifecycleMemoryMax: serverConfig.orderLifecycleMemoryMax,
  roundsMemoryMax: serverConfig.roundsMemoryMax,
  serverHeapWarnMb: serverConfig.serverHeapWarnMb,
  serverHeapProtectMb: serverConfig.serverHeapProtectMb
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
  binanceFallbackRestUrl: serverConfig.binanceFallbackRestUrl,
  binanceFallbackRestPollMs: serverConfig.binanceFallbackRestPollMs,
  binanceWsUrl: serverConfig.binanceWsUrl,
  binanceRequestTimeoutMs: serverConfig.binanceRequestTimeoutMs,
  binanceRestPollMs: serverConfig.binanceRestPollMs,
  binanceWsStaleMs: serverConfig.binanceWsStaleMs,
  upstreamProxyUrl: serverConfig.upstreamProxyUrl,
  coinbaseEnabled: serverConfig.coinbaseEnabled,
  coinbaseWsUrl: serverConfig.coinbaseWsUrl,
  coinbaseRestUrl: serverConfig.coinbaseRestUrl,
  coinbaseRestPollMs: serverConfig.coinbaseRestPollMs,
  coinbaseRequestTimeoutMs: serverConfig.coinbaseRequestTimeoutMs,
  coinbaseWsStaleMs: serverConfig.coinbaseWsStaleMs,
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
  marketSnapshotIntervalMs: serverConfig.marketSnapshotIntervalMs,
  marketFullReconcileIntervalMs: serverConfig.marketFullReconcileIntervalMs,
  polymarketBookPollMs: serverConfig.polymarketBookPollMs,
  polymarketBookCalibrationMs: serverConfig.polymarketBookCalibrationMs,
  polymarketTradesPollMs: serverConfig.polymarketTradesPollMs
});

let marketPayloadSeq = 0;
const MARKET_WS_RETRY_MS = 25;
const MARKET_WS_MIN_INTERVAL_MS = Math.max(serverConfig.marketWsMinIntervalMs, 0);
const MARKET_WS_FULL_SNAPSHOT_INTERVAL_MS = 10_000;
const MARKET_WS_FULL_SNAPSHOT_STAGGER_MS = 40;
const MARKET_WS_INITIAL_FULL_SNAPSHOT_MAX_DELAY_MS = 2_000;
const MARKET_WS_INITIAL_FULL_SNAPSHOT_SLOTS = Math.max(
  1,
  Math.floor(MARKET_WS_INITIAL_FULL_SNAPSHOT_MAX_DELAY_MS / MARKET_WS_FULL_SNAPSHOT_STAGGER_MS)
);
const MARKET_WS_FULL_SNAPSHOT_RETRY_MS = 250;
const USER_WS_RETRY_MS = 50;
const USER_TRADE_ORDER_LIMIT = 50;
const USER_TRADE_LIFECYCLE_LIMIT = 80;
const MARKET_TRANSPORT_CANDLE_LIMIT = 120;
const MARKET_TRANSPORT_ORDER_BOOK_LEVEL_LIMIT = 10;
const MARKET_TRANSPORT_RECENT_TRADE_LIMIT = 30;
const MARKET_TRANSPORT_ODDS_POINT_LIMIT = 120;
const MARKET_HISTORY_CACHE_MAX_USERS = Math.max(serverConfig.marketHistoryCacheMaxUsers, 1);

type CachedMarketHistory = {
  revision: number;
  limit: number;
  rows: Array<RoundRecord & { userPnl: number }>;
};

const marketHistoryCache = new Map<string, CachedMarketHistory>();

const loginSchema = z.object({
  username: z.string().min(1),
  password: z.string().min(1)
});

const wsTicketSchema = z.object({
  channel: z.enum(["market", "user"]),
  viewUserId: z.string().optional()
});

const orderSchema = z.object({
  action: z.enum(["buy", "sell"]).optional(),
  side: z.enum(["UP", "DOWN"]),
  orderKind: z.enum(["market", "limit"]).optional(),
  amount: z.number().positive().optional(),
  qty: z.number().positive().optional(),
  limitPrice: z.number().positive().optional(),
  clientOrderId: z.string().trim().min(1).max(128).optional(),
  clientSendTs: z.number().optional()
});

const languageSchema = z.object({
  language: z.enum(["zh-CN", "en-US"])
});

const selfProfileSchema = z.object({
  displayName: z.string().trim().min(1).optional(),
  language: z.enum(["zh-CN", "en-US"]).optional()
});

const roleSchema = z.enum(["Tester", "Senior Tester", "Test Engineer", "Admin"]);
const DEFAULT_GROUP_MANAGER_USERNAME = "JDH1";

const createUserSchema = z.object({
  username: z.string().trim().min(1),
  password: z.string().min(1),
  displayName: z.string().trim().min(1),
  role: roleSchema,
  language: z.enum(["zh-CN", "en-US"]).optional(),
  seniorTesterId: z.string().optional(),
  managerUserId: z.string().optional(),
  permissionLevel: z.enum(["Initial", "Standard"]).optional(),
  availableUsdc: z.number().nonnegative().optional()
});

const updateUserSchema = z.object({
  displayName: z.string().trim().min(1).optional(),
  role: roleSchema.optional(),
  language: z.enum(["zh-CN", "en-US"]).optional(),
  seniorTesterId: z.string().nullable().optional(),
  managerUserId: z.string().nullable().optional(),
  permissionLevel: z.enum(["Initial", "Standard"]).optional(),
  availableUsdc: z.number().nonnegative().optional(),
  isActive: z.boolean().optional()
});

const changeUserGroupSchema = z.object({
  managerUserId: z.string().trim().min(1)
});

const bulkCreateUserItemSchema = z.object({
  username: z.string().trim().min(1),
  password: z.string().min(1),
  displayName: z.string().trim().optional(),
  role: roleSchema.optional(),
  language: z.enum(["zh-CN", "en-US"]).optional(),
  seniorTesterId: z.string().optional(),
  managerUserId: z.string().optional(),
  permissionLevel: z.enum(["Initial", "Standard"]).optional(),
  mustChangePassword: z.boolean().optional(),
  availableUsdc: z.number().nonnegative().optional()
});

const bulkCreateUsersSchema = z.object({
  users: z.array(bulkCreateUserItemSchema).min(1).max(500)
});

const bulkUsersCsvSchema = z.object({
  csv: z.string().min(1)
});

const datasetExportSchema = z.object({
  from: z.number().optional(),
  to: z.number().optional(),
  userIds: z.array(z.string()).optional(),
  includeDGrade: z.boolean().optional(),
  format: z.enum(["zip", "parquet"]).optional()
});

const resetPasswordSchema = z.object({
  currentPassword: z.string().min(1),
  password: z.string().min(1),
  confirmPassword: z.string().min(1)
});

const changePasswordSchema = resetPasswordSchema;

const userBalanceSchema = z.object({
  availableUsdc: z.number().nonnegative()
});

const quickSideSchema = z.object({
  side: z.enum(["UP", "DOWN"]),
  clientSendTs: z.number().optional()
});

const settlementActionSchema = z.object({
  side: z.enum(["UP", "DOWN"]),
  price: z.number().nonnegative().optional(),
  reason: z.string().trim().max(300).optional()
});

const trainingLogQuerySchema = z.object({
  from: z.coerce.number().optional(),
  to: z.coerce.number().optional(),
  viewUserId: z.string().optional(),
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
  viewUserId: z.string().optional(),
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

const logSearchQuerySchema = z.object({
  system: z.enum(["all", "audit", "training", "matching"]).optional(),
  from: z.coerce.number().optional(),
  to: z.coerce.number().optional(),
  viewUserId: z.string().optional(),
  userId: z.string().optional(),
  role: roleSchema.optional(),
  category: z.enum(["operation", "matching", "settlement", "latency"]).optional(),
  actionType: z.string().optional(),
  actionStatus: z.enum(["success", "failed", "timeout"]).optional(),
  moduleName: z.string().optional(),
  pageName: z.string().optional(),
  symbol: z.string().optional(),
  roundId: z.string().optional(),
  marketId: z.string().optional(),
  marketSlug: z.string().optional(),
  orderId: z.string().optional(),
  positionId: z.string().optional(),
  traceId: z.string().optional(),
  resultCode: z.string().optional(),
  direction: z.enum(["UP", "DOWN"]).optional(),
  roundStatus: z
    .enum(["Trading", "Frozen", "Settling", "Polling", "Settled", "Redeeming", "Closed", "Manual", "AdminReviewed"])
    .optional(),
  settlementResult: z.enum(["win", "loss", "sold"]).optional(),
  bookKey: z.string().optional(),
  bookSide: z.enum(["UP", "DOWN"]).optional(),
  eventType: z.enum(["external_book_synced", "order_executed", "order_cancelled"]).optional(),
  sequenceFrom: z.coerce.number().optional(),
  sequenceTo: z.coerce.number().optional(),
  logGroup: z.enum(["operation", "settlement", "market_latency", "system_latency", "matching_action"]).optional(),
  latencySource: z.enum(["binance", "coinbase", "clob", "system"]).optional(),
  connectionState: z.enum(["healthy", "reconnecting", "stale", "degraded", "disabled"]).optional(),
  latencyPhase: z.enum(["backend", "acquire", "publish", "frontend"]).optional(),
  latencyMinMs: z.coerce.number().optional(),
  latencyMaxMs: z.coerce.number().optional(),
  matchingLogKind: z.enum(["action", "engine"]).optional(),
  limit: z.coerce.number().optional(),
  cursor: z.string().optional()
});

const logExportBodySchema = logSearchQuerySchema.omit({
  limit: true,
  cursor: true
}).extend({
  systems: z.array(z.enum(["audit", "training", "matching"])).optional(),
  userIds: z.array(z.string()).optional()
});

const exportLogQuerySchema = logSearchQuerySchema.omit({
  limit: true,
  cursor: true
}).extend({
  marketId: z.string().optional(),
  marketSlug: z.string().optional()
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
  if (!user.isActive) {
    throw new Error("User account is disabled.");
  }
  return user;
}

function readViewUserId(query: unknown) {
  const raw = query && typeof query === "object" ? (query as { viewUserId?: unknown }).viewUserId : undefined;
  return typeof raw === "string" && raw.trim() ? raw.trim() : undefined;
}

function resolveViewedUser(actor: UserRecord, viewUserId?: string) {
  const target = viewUserId ? store.getUserById(viewUserId) : actor;
  if (!target) {
    throw new Error("Target user was not found.");
  }
  if (!canViewUserRecords(actor, target, store.listUserRecords())) {
    throw new Error("Records are not available for this user.");
  }
  return target;
}

function getViewedUserFromRequest(
  actor: UserRecord,
  request: { query?: unknown }
) {
  return resolveViewedUser(actor, readViewUserId(request.query));
}

type WsSession = {
  actor: UserRecord;
  viewedUser: UserRecord;
};

function createWsTicket(user: UserRecord, channel: "market" | "user", viewUserId?: string) {
  const viewedUser = resolveViewedUser(user, viewUserId);
  const ticket = `wst_${nanoid(32)}`;
  const expiresAt = Date.now() + WS_TICKET_TTL_MS;
  wsTickets.set(ticket, {
    userId: user.id,
    viewedUserId: viewedUser.id,
    channel,
    expiresAt
  });
  return {
    ticket,
    expiresAt
  };
}

function consumeWsTicket(rawTicket: string | undefined, channel: "market" | "user"): WsSession | undefined {
  if (!rawTicket) {
    return undefined;
  }
  const ticket = wsTickets.get(rawTicket);
  wsTickets.delete(rawTicket);
  if (!ticket || ticket.channel !== channel || ticket.expiresAt < Date.now()) {
    return undefined;
  }
  const actor = store.getUserById(ticket.userId);
  if (!actor) {
    return undefined;
  }
  return {
    actor,
    viewedUser: resolveViewedUser(actor, ticket.viewedUserId)
  };
}

function getWsSession(query: { token?: string; ticket?: string; viewUserId?: string }, channel: "market" | "user"): WsSession | undefined {
  const ticketSession = consumeWsTicket(query.ticket, channel);
  if (ticketSession) {
    return ticketSession;
  }
  const token = readToken(query.token);
  if (!token) {
    return undefined;
  }
  const payload = jwt.verify(token, serverConfig.jwtSecret) as { userId: string };
  const actor = store.getUserById(payload.userId);
  if (!actor) {
    return undefined;
  }
  return {
    actor,
    viewedUser: resolveViewedUser(actor, query.viewUserId)
  };
}

function attachHeartbeat(socket: WsWebSocket, channel: "market" | "user") {
  let alive = true;
  let missedPongs = 0;
  socket.on("pong", () => {
    alive = true;
    missedPongs = 0;
  });
  const timer = setInterval(() => {
    if (socket.readyState !== WsWebSocket.OPEN) {
      clearInterval(timer);
      return;
    }
    if (!alive) {
      missedPongs += 1;
      if (missedPongs >= 3) {
        heartbeatTimeoutSockets.add(socket);
        appMetrics.recordWsDisconnect(channel, "heartbeat_timeout");
        socket.close();
        clearInterval(timer);
        return;
      }
    }
    alive = false;
    socket.ping();
  }, WS_HEARTBEAT_MS);
  socket.on("close", () => clearInterval(timer));
}

function consumeHeartbeatTimeout(socket: WsWebSocket) {
  if (!heartbeatTimeoutSockets.has(socket)) {
    return false;
  }
  heartbeatTimeoutSockets.delete(socket);
  return true;
}

function updateRuntimeMetrics() {
  const persistence = store.getPersistenceStatus();
  const sources = store.getSourceStatus();
  appMetrics.setRuntime({
    persistence: {
      postgres: persistence.postgres,
      redis: persistence.redis
    },
    sources
  });
  appMetrics.setWsConnections("market", wsConnectionCounts.market);
  appMetrics.setWsConnections("user", wsConnectionCounts.user);
  appMetrics.setJsonlStats(store.getJsonlStats());
}

function metricsAuthorized(authHeader: string | string[] | undefined) {
  if (!serverConfig.metricsBasicAuthUser || !serverConfig.metricsBasicAuthPassword) {
    return true;
  }
  const header = typeof authHeader === "string" ? authHeader : undefined;
  if (!header?.startsWith("Basic ")) {
    return false;
  }
  const decoded = Buffer.from(header.slice("Basic ".length), "base64").toString("utf8");
  return decoded === `${serverConfig.metricsBasicAuthUser}:${serverConfig.metricsBasicAuthPassword}`;
}

function requirePermission(user: UserRecord, code: string) {
  assertCan(user, code as never);
}

function canViewAllLogs(user: UserRecord) {
  return user.role === "Admin";
}

function canViewTeamLogs(user: UserRecord) {
  return user.role === "Senior Tester" || user.role === "Test Engineer";
}

function teamVisibleUserIds(user: UserRecord) {
  return getVisibleUserIdsForActor(user, store.listUserRecords());
}

function resolveAuditLogFilters(user: UserRecord, parsed: AuditLogQuery): AuditLogQuery {
  const requestedUserId = parsed.userId ?? parsed.viewUserId;
  if (canViewAllLogs(user)) {
    return { ...parsed, userId: requestedUserId, viewUserId: undefined };
  }
  if (canViewTeamLogs(user)) {
    const userIds = teamVisibleUserIds(user);
    if (requestedUserId && !userIds.includes(requestedUserId)) {
      throw new Error("Logs are not available for this user.");
    }
    return requestedUserId ? { ...parsed, userId: requestedUserId, viewUserId: undefined } : { ...parsed, viewUserId: undefined, userIds };
  }
  if (requestedUserId && requestedUserId !== user.id) {
    throw new Error("Logs are not available for this user.");
  }
  return { ...parsed, userId: user.id, viewUserId: undefined };
}

function resolveBehaviorLogFilters(user: UserRecord, parsed: BehaviorLogQuery): BehaviorLogQuery {
  const requestedUserId = parsed.userId ?? parsed.viewUserId;
  if (canViewAllLogs(user)) {
    return { ...parsed, userId: requestedUserId, viewUserId: undefined };
  }
  if (canViewTeamLogs(user)) {
    const userIds = teamVisibleUserIds(user);
    if (requestedUserId && !userIds.includes(requestedUserId)) {
      throw new Error("Logs are not available for this user.");
    }
    return requestedUserId ? { ...parsed, userId: requestedUserId, viewUserId: undefined } : { ...parsed, viewUserId: undefined, userIds };
  }
  if (requestedUserId && requestedUserId !== user.id) {
    throw new Error("Logs are not available for this user.");
  }
  return { ...parsed, userId: user.id, viewUserId: undefined };
}

const LOG_SEARCH_DEFAULT_LIMIT = 100;
const LOG_SEARCH_MAX_LIMIT = 500;
const LOG_EXPORT_MAX_ROWS_PER_FILE = 50_000;

type LogCursor = Partial<Record<Exclude<LogSystem, "all">, number>>;

function normalizeLogLimit(limit?: number) {
  return Math.max(1, Math.min(Math.floor(limit ?? LOG_SEARCH_DEFAULT_LIMIT), LOG_SEARCH_MAX_LIMIT));
}

function decodeLogCursor(raw?: string): LogCursor {
  if (!raw) {
    return {};
  }
  try {
    const parsed = JSON.parse(Buffer.from(raw, "base64url").toString("utf8")) as LogCursor;
    return {
      audit: Math.max(0, Math.floor(parsed.audit ?? 0)),
      training: Math.max(0, Math.floor(parsed.training ?? 0)),
      matching: Math.max(0, Math.floor(parsed.matching ?? 0))
    };
  } catch {
    return {};
  }
}

function encodeLogCursor(cursor: LogCursor) {
  return Buffer.from(JSON.stringify(cursor), "utf8").toString("base64url");
}

function selectedLogSystems(system: LogSystem | undefined): Array<Exclude<LogSystem, "all">> {
  if (!system || system === "all") {
    return ["audit", "training", "matching"];
  }
  return [system];
}

function selectedExportSystems(query: Pick<LogSearchQuery, "system" | "systems">): Array<Exclude<LogSystem, "all">> {
  if (query.systems?.length) {
    return [...new Set(query.systems)];
  }
  return selectedLogSystems(query.system);
}

function scopedUserIdsForActor(actor: UserRecord) {
  if (canViewAllLogs(actor)) {
    return undefined;
  }
  if (canViewTeamLogs(actor)) {
    return teamVisibleUserIds(actor);
  }
  return [actor.id];
}

function resolveLogSearchFilters(actor: UserRecord, parsed: LogSearchQuery): LogSearchQuery {
  const scopedIds = scopedUserIdsForActor(actor);
  const allUsers = store.listUsers() as unknown as UserRecord[];
  const requestedUserId = parsed.userId ?? parsed.viewUserId;
  const requestedUser = requestedUserId ? store.getUserById(requestedUserId) : undefined;
  if (requestedUserId && !requestedUser) {
    throw new Error("Target user was not found.");
  }
  if (requestedUserId && scopedIds && !scopedIds.includes(requestedUserId)) {
    throw new Error("Logs are not available for this user.");
  }
  if (parsed.userIds?.length) {
    const missingUserId = parsed.userIds.find((userId) => !store.getUserById(userId));
    if (missingUserId) {
      throw new Error(`Target user was not found: ${missingUserId}`);
    }
    const forbiddenUserId = scopedIds ? parsed.userIds.find((userId) => !scopedIds.includes(userId)) : undefined;
    if (forbiddenUserId) {
      throw new Error("Logs are not available for this user.");
    }
  }

  let userIds = requestedUserId ? [requestedUserId] : parsed.userIds?.length ? [...new Set(parsed.userIds)] : scopedIds;
  if (parsed.role) {
    const roleIds = new Set(allUsers.filter((user) => user.role === parsed.role).map((user) => user.id));
    userIds = userIds ? userIds.filter((userId) => roleIds.has(userId)) : [...roleIds];
  }

  return {
    ...parsed,
    viewUserId: undefined,
    userId: requestedUserId,
    userIds: requestedUserId ? undefined : userIds,
    system: parsed.system ?? (parsed.systems?.length === 1 ? parsed.systems[0] : parsed.system)
  };
}

function userLookupMaps() {
  const users = store.listUsers();
  return {
    byId: new Map(users.map((user) => [user.id, user])),
    byAnonId: new Map(users.map((user) => [store.anonymizeUserId(user.id), user]))
  };
}

function rawObject(value: unknown) {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function detailNumber(details: Record<string, unknown>, key: string) {
  const value = details[key];
  return typeof value === "number" && Number.isFinite(value)
    ? value
    : typeof value === "string" && Number.isFinite(Number(value))
      ? Number(value)
      : undefined;
}

function deriveAuditLogGroup(log: Pick<Awaited<ReturnType<AppStore["searchAuditLogs"]>>[number], "category" | "moduleName">) {
  if (log.category === "matching") {
    return "matching_action" as const;
  }
  if (log.category === "settlement") {
    return "settlement" as const;
  }
  if (log.category === "latency") {
    return ["binance", "coinbase", "clob"].includes(log.moduleName) ? "market_latency" : "system_latency";
  }
  return "operation" as const;
}

function deriveLatencySource(moduleName?: string) {
  if (moduleName === "binance" || moduleName === "coinbase" || moduleName === "clob") {
    return moduleName;
  }
  return "system";
}

function latencyMetrics(log: Pick<Awaited<ReturnType<AppStore["searchAuditLogs"]>>[number], "backendLatencyMs" | "frontendLatencyMs" | "details">) {
  const details = rawObject(log.details);
  return {
    backend: log.backendLatencyMs,
    acquire: detailNumber(details, "acquireLatencyMs"),
    publish: detailNumber(details, "publishLatencyMs"),
    frontend: log.frontendLatencyMs ?? detailNumber(details, "frontendLatencyMs")
  };
}

function auditEventToUnifiedRow(log: Awaited<ReturnType<AppStore["searchAuditLogs"]>>[number]): UnifiedLogRow {
  const user = log.userId ? store.getUserById(log.userId) : undefined;
  const details = rawObject(log.details);
  const detailString = (key: string) => {
    const value = details[key];
    return typeof value === "string" || typeof value === "number" ? String(value) : undefined;
  };
  return {
    id: log.eventId,
    system: "audit",
    timestampMs: log.serverRecvTs,
    userId: log.userId,
    username: user?.username,
    displayName: user?.displayName,
    role: log.role ?? user?.role,
    category: log.category,
    actionType: log.actionType,
    actionStatus: log.actionStatus,
    moduleName: log.moduleName,
    pageName: log.pageName,
    symbol: log.symbol,
    roundId: log.roundId,
    marketId: detailString("marketId"),
    marketSlug: detailString("marketSlug"),
    orderId: detailString("orderId"),
    positionId: detailString("positionId"),
    traceId: log.traceId,
    resultCode: log.resultCode,
    resultMessage: log.resultMessage,
    direction: (detailString("direction") ?? detailString("side")) as TradeSide | undefined,
    roundStatus: detailString("roundStatus") as UnifiedLogRow["roundStatus"],
    settlementResult: detailString("settlementResult") as UnifiedLogRow["settlementResult"],
    logGroup: deriveAuditLogGroup(log),
    latencySource: log.category === "latency" ? deriveLatencySource(log.moduleName) : undefined,
    connectionState: detailString("connectionState") as UnifiedLogRow["connectionState"],
    latencyPhaseMetrics: log.category === "latency" ? latencyMetrics(log) : undefined,
    matchingLogKind: log.category === "matching" ? "action" : undefined,
    payload: details
  };
}

function behaviorLogToUnifiedRow(
  log: Awaited<ReturnType<AppStore["searchBehaviorLogs"]>>[number],
  byAnonId: ReturnType<typeof userLookupMaps>["byAnonId"]
): UnifiedLogRow {
  const user = byAnonId.get(log.testerIdAnon);
  return {
    id: log.logId,
    system: "training",
    timestampMs: log.timestampMs,
    userId: user?.id,
    username: user?.username,
    displayName: user?.displayName,
    role: user?.role,
    actionType: log.actionType,
    actionStatus: log.actionStatus,
    symbol: log.assetClass,
    roundId: log.roundId,
    marketId: log.marketId,
    marketSlug: log.marketSlug,
    orderId: log.orderId,
    traceId: log.traceId,
    resultMessage: log.actionStatus,
    direction: log.direction,
    roundStatus: log.roundStatus,
    settlementResult: log.settlementResult,
    payload: {
      ...log,
      testerIdAnon: log.testerIdAnon
    }
  };
}

function matchingEventUserId(event: MatchingEventRecord) {
  const request = event.payload.request as { userId?: string } | undefined;
  return request?.userId;
}

function matchingEventToUnifiedRow(event: MatchingEventRecord): UnifiedLogRow {
  const userId = matchingEventUserId(event);
  const user = userId ? store.getUserById(userId) : undefined;
  return {
    id: event.eventId,
    system: "matching",
    timestampMs: event.createdAt,
    userId,
    username: user?.username,
    displayName: user?.displayName,
    role: user?.role,
    actionType: event.eventType,
    actionStatus:
      event.eventType === "order_executed"
        ? event.payload.status === "failed"
          ? "failed"
          : "success"
        : "success",
    roundId: event.roundId,
    marketId: event.marketId,
    orderId: event.orderId,
    traceId: event.traceId,
    resultMessage: typeof event.payload.status === "string" ? event.payload.status : event.eventType,
    bookKey: event.bookKey,
    bookSide: event.bookSide,
    eventType: event.eventType,
    sequence: event.sequence,
    logGroup: "matching_action",
    matchingLogKind: "engine",
    payload: event.payload
  };
}

async function searchUnifiedLogs(actor: UserRecord, parsed: LogSearchQuery): Promise<LogSearchResult> {
  const scoped = resolveLogSearchFilters(actor, parsed);
  const limit = normalizeLogLimit(parsed.limit);
  const fetchLimit = limit + 1;
  const cursor = decodeLogCursor(parsed.cursor);
  const systems = selectedLogSystems(scoped.system);
  const includeAuditMatchingActions = scoped.system === "matching" && scoped.matchingLogKind !== "engine";
  const includeMatchingEngine =
    systems.includes("matching") &&
    scoped.matchingLogKind !== "action" &&
    !scoped.logGroup &&
    !scoped.latencySource &&
    !scoped.connectionState &&
    !scoped.latencyPhase &&
    typeof scoped.latencyMinMs !== "number" &&
    typeof scoped.latencyMaxMs !== "number";
  const mergeSystems = includeAuditMatchingActions ? [...new Set<Exclude<LogSystem, "all">>(["audit", ...systems])] : systems;
  const lookups = userLookupMaps();
  const fetched: Record<Exclude<LogSystem, "all">, UnifiedLogRow[]> = {
    audit: [],
    training: [],
    matching: []
  };

  if (systems.includes("audit") || includeAuditMatchingActions) {
    const auditQuery = includeAuditMatchingActions ? { ...scoped, category: "matching" as const } : scoped;
    fetched.audit = (await store.searchAuditLogs(auditQuery, { limit: fetchLimit, offset: cursor.audit ?? 0 })).map(
      auditEventToUnifiedRow
    );
  }
  if (systems.includes("training")) {
    fetched.training = (
      await store.searchBehaviorLogs(scoped, { limit: fetchLimit, offset: cursor.training ?? 0 })
    ).map((log) => behaviorLogToUnifiedRow(log, lookups.byAnonId));
  }
  if (includeMatchingEngine) {
    fetched.matching = await engine
      .searchMatchingEvents({
        from: scoped.from,
        to: scoped.to,
        userId: scoped.userId,
        userIds: scoped.userIds,
        roundId: scoped.roundId,
        marketId: scoped.marketId,
        bookKey: scoped.bookKey,
        bookSide: scoped.bookSide,
        eventType: scoped.eventType,
        traceId: scoped.traceId,
        orderId: scoped.orderId,
        sequenceFrom: scoped.sequenceFrom,
        sequenceTo: scoped.sequenceTo,
        limit: fetchLimit,
        offset: cursor.matching ?? 0
      })
      .then((result) => result.events.map(matchingEventToUnifiedRow))
      .catch(() => []);
  }

  const merged = mergeSystems
    .flatMap((system) => fetched[system].map((row) => ({ ...row, system })))
    .sort((left, right) => right.timestampMs - left.timestampMs || right.id.localeCompare(left.id))
    .slice(0, limit);

  const consumed: LogCursor = {
    audit: cursor.audit ?? 0,
    training: cursor.training ?? 0,
    matching: cursor.matching ?? 0
  };
  for (const row of merged) {
    consumed[row.system] = (consumed[row.system] ?? 0) + 1;
  }
  const hasMore = mergeSystems.some((system) => fetched[system].length > (consumed[system] ?? 0) - (cursor[system] ?? 0));

  return {
    rows: merged,
    nextCursor: hasMore ? encodeLogCursor(consumed) : undefined,
    limit,
    system: scoped.system ?? "all"
  };
}

function canListUsers(user: UserRecord) {
  return hasPermission(user, "users:list");
}

function listUsersForActor(actor: UserRecord) {
  const visibleIds = new Set(getVisibleUserIdsForActor(actor, store.listUserRecords()));
  return store.listUsers().filter((user) => visibleIds.has(user.id));
}

function isGroupManagerUser(user: UserRecord | undefined) {
  return Boolean(user && (user.role === "Senior Tester" || user.role === "Test Engineer"));
}

function getDefaultGroupManagerId() {
  const manager = store.findUserByUsername(DEFAULT_GROUP_MANAGER_USERNAME);
  return isGroupManagerUser(manager) ? manager?.id : undefined;
}

function getTargetUserForManagement(actor: UserRecord, targetUserId: string) {
  const target = store.getUserById(targetUserId);
  if (!target) {
    throw new Error("Target user was not found.");
  }
  assertCanManageUser(actor, target, store.listUserRecords());
  return target;
}

function getTargetUserForBalance(actor: UserRecord, targetUserId: string) {
  if (actor.role === "Senior Tester" && actor.id === targetUserId) {
    return actor;
  }
  return getTargetUserForManagement(actor, targetUserId);
}

function normalizeManagerUserId(role: Role, managerUserId?: string | null, options?: { requireActive?: boolean }) {
  if (role !== "Tester") {
    return undefined;
  }
  if (!managerUserId) {
    return undefined;
  }
  const senior = store.getUserById(managerUserId);
  if (!senior || (senior.role !== "Senior Tester" && senior.role !== "Test Engineer")) {
    throw new Error("managerUserId must point to a Senior Tester or Test Engineer.");
  }
  if (options?.requireActive && !senior.isActive) {
    throw new Error("managerUserId must point to an active Senior Tester or Test Engineer.");
  }
  return managerUserId;
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
    latencyBreakdown: {
      ...snapshot.latencyBreakdown,
      serverComputeLatency: Math.max(serverPublishTs - snapshot.serverNow, 0)
    },
    sources: {
      binance: stampSourceForTransport(snapshot.sources.binance, serverPublishTs),
      coinbase: stampSourceForTransport(snapshot.sources.coinbase, serverPublishTs),
      clob: stampSourceForTransport(snapshot.sources.clob, serverPublishTs)
    }
  };
}

function compactCandlesByInterval(candlesByInterval: Record<CandleInterval, CandleBar[]>) {
  return Object.fromEntries(
    Object.entries(candlesByInterval).map(([interval, bars]) => [
      interval,
      bars.slice(-MARKET_TRANSPORT_CANDLE_LIMIT)
    ])
  ) as Record<CandleInterval, CandleBar[]>;
}

function compactSnapshotForTransport(snapshot: MarketSnapshot): MarketSnapshot {
  const orderBooks = {
    UP: {
      ...snapshot.orderBooks.UP,
      bids: snapshot.orderBooks.UP.bids.slice(0, MARKET_TRANSPORT_ORDER_BOOK_LEVEL_LIMIT),
      asks: snapshot.orderBooks.UP.asks.slice(0, MARKET_TRANSPORT_ORDER_BOOK_LEVEL_LIMIT)
    },
    DOWN: {
      ...snapshot.orderBooks.DOWN,
      bids: snapshot.orderBooks.DOWN.bids.slice(0, MARKET_TRANSPORT_ORDER_BOOK_LEVEL_LIMIT),
      asks: snapshot.orderBooks.DOWN.asks.slice(0, MARKET_TRANSPORT_ORDER_BOOK_LEVEL_LIMIT)
    }
  };
  return {
    ...snapshot,
    orderBooks,
    recentTrades: snapshot.recentTrades.slice(0, MARKET_TRANSPORT_RECENT_TRADE_LIMIT),
    candles: snapshot.candles.slice(-MARKET_TRANSPORT_CANDLE_LIMIT),
    binance: {
      ...snapshot.binance,
      candlesByInterval: compactCandlesByInterval(snapshot.binance.candlesByInterval)
    },
    coinbase: {
      ...snapshot.coinbase,
      candles5s: snapshot.coinbase.candles5s.slice(-MARKET_TRANSPORT_CANDLE_LIMIT),
      candlesByInterval: compactCandlesByInterval(snapshot.coinbase.candlesByInterval)
    },
    clob: {
      ...snapshot.clob,
      upBook: orderBooks.UP,
      downBook: orderBooks.DOWN,
      recentTrades: snapshot.clob.recentTrades.slice(0, MARKET_TRANSPORT_RECENT_TRADE_LIMIT),
      currentRoundUpPriceSeries: snapshot.clob.currentRoundUpPriceSeries.slice(-MARKET_TRANSPORT_ODDS_POINT_LIMIT)
    }
  };
}

function nextMarketTransportMeta(coalescedCount = 0, snapshotBuildTs?: number): MarketTransportMeta {
  const serverPublishTs = Date.now();
  marketPayloadSeq += 1;
  return {
    serverPublishTs,
    payloadSeq: marketPayloadSeq,
    coalescedCount: coalescedCount > 0 ? coalescedCount : undefined,
    snapshotBuildTs
  };
}

function markTransportSendStart(transportMeta: MarketTransportMeta) {
  const sendStartedAt = Date.now();
  transportMeta.wsSendStartTs = sendStartedAt;
  transportMeta.serverQueueMs = Math.max(sendStartedAt - transportMeta.serverPublishTs, 0);
  return sendStartedAt;
}

function decorateRoundWithSettlementPreview<T extends RoundRecord & { userPnl?: number }>(
  round: T
): T & { settlementPreview?: SettlementPreview } {
  const settlementPreview = engine.getSettlementPreview(round);
  return settlementPreview ? { ...round, settlementPreview } : round;
}

function decorateCurrentRoundForTransport(round: RoundRecord | undefined) {
  const displayRound = engine.withCurrentRoundBinanceOpenReference(engine.withCurrentRoundCoinbaseOpenReference(round));
  return displayRound ? decorateRoundWithSettlementPreview(displayRound) : undefined;
}

function getCachedHistory(limit: number, userId?: string) {
  const revision = store.getHistoryRevision();
  const cacheKey = `${userId ?? "__public__"}:${limit}`;
  const cached = marketHistoryCache.get(cacheKey);
  if (cached && cached.revision === revision && cached.limit === limit) {
    return cached.rows;
  }
  const rows = store.getHistory(limit, userId);
  marketHistoryCache.set(cacheKey, { revision, limit, rows });
  if (marketHistoryCache.size > MARKET_HISTORY_CACHE_MAX_USERS) {
    const oldestKey = marketHistoryCache.keys().next().value;
    if (oldestKey) {
      marketHistoryCache.delete(oldestKey);
    }
  }
  return rows;
}

function getHistoryWithSettlementPreview(limit: number, userId?: string) {
  return getCachedHistory(limit, userId).map((round) => decorateRoundWithSettlementPreview(round));
}

function getOperatedHistoryWithSettlementPreview(limit: number, userId: string) {
  return store.getOperatedHistory(limit, userId).map((round) => decorateRoundWithSettlementPreview(round));
}

function createCurrentRoundPayload(coalescedCount = 0, viewedUserId?: string) {
  const transportMeta = nextMarketTransportMeta(coalescedCount, store.marketSnapshot.serverNow);
  const currentRound = store.getCurrentRound();
  const history = getHistoryWithSettlementPreview(10, viewedUserId);
  const settlementPreview =
    (currentRound ? engine.getSettlementPreview(currentRound) : undefined) ??
    engine.getLatestSettlementPreview(history);
  return {
    viewedUserId,
    currentRound: decorateCurrentRoundForTransport(currentRound),
    snapshot: compactSnapshotForTransport(stampSnapshotForTransport(store.marketSnapshot, transportMeta.serverPublishTs)),
    settlementPreview,
    transportMeta
  };
}

function createMarketPayload(viewedUserId: string, coalescedCount = 0): MarketPayload {
  return {
    ...createCurrentRoundPayload(coalescedCount, viewedUserId),
    viewedUserId,
    history: getHistoryWithSettlementPreview(10, viewedUserId)
  };
}

function countdownTargetTsFor(snapshot: MarketSnapshot) {
  const countdownMs = snapshot.uiMeta.countdownMs;
  return Number.isFinite(countdownMs) && countdownMs > 0 ? snapshot.serverNow + countdownMs : undefined;
}

function latestCandleUpdates(candlesByInterval: Record<CandleInterval, CandleBar[]>) {
  const updates: Partial<Record<CandleInterval, CandleBar>> = {};
  for (const [interval, bars] of Object.entries(candlesByInterval) as Array<[CandleInterval, CandleBar[]]>) {
    const latest = bars.at(-1);
    if (latest) {
      updates[interval] = latest;
    }
  }
  return updates;
}

function topLevelsForTick(snapshot: MarketSnapshot): Record<TradeSide, { bids: BookLevel[]; asks: BookLevel[] }> {
  return {
    UP: {
      bids: snapshot.orderBooks.UP.bids.slice(0, 5),
      asks: snapshot.orderBooks.UP.asks.slice(0, 5)
    },
    DOWN: {
      bids: snapshot.orderBooks.DOWN.bids.slice(0, 5),
      asks: snapshot.orderBooks.DOWN.asks.slice(0, 5)
    }
  };
}

function createMarketRealtimeTick(snapshot: MarketSnapshot, serverPublishTs: number): MarketRealtimeTick {
  const stamped = stampSnapshotForTransport(snapshot, serverPublishTs);
  const currentRoundUpPricePoint = stamped.clob.currentRoundUpPriceSeries.at(-1);
  return {
    symbol: stamped.symbol,
    marketId: stamped.marketId,
    marketSlug: stamped.marketSlug,
    serverNow: stamped.serverNow,
    currentPrice: stamped.currentPrice,
    binancePrice: stamped.binancePrice,
    coinbasePrice: stamped.coinbasePrice,
    priceToBeat: stamped.priceToBeat,
    displayPriceToBeat: stamped.displayPriceToBeat,
    displayPriceToBeatSource: stamped.displayPriceToBeatSource,
    upPrice: stamped.upPrice,
    downPrice: stamped.downPrice,
    displayPrices: stamped.displayPrices,
    displayPriceSource: stamped.displayPriceSource,
    displayPriceSpread: stamped.displayPriceSpread,
    latencyBreakdown: stamped.latencyBreakdown,
    sources: stamped.sources,
    binance: {
      spotPrice: stamped.binance.spotPrice,
      latestTick: stamped.binance.latestTick,
      candleUpdates: latestCandleUpdates(stamped.binance.candlesByInterval)
    },
    coinbase: {
      referencePrice: stamped.coinbase.referencePrice,
      settlementReference: stamped.coinbase.settlementReference,
      currentRoundOpenReference: stamped.coinbase.currentRoundOpenReference,
      candleUpdates: latestCandleUpdates(stamped.coinbase.candlesByInterval),
      latestTick:
        stamped.coinbase.referencePrice > 0
          ? { ts: stamped.sources.coinbase.normalizedTs || stamped.serverNow, price: stamped.coinbase.referencePrice }
          : undefined
    },
    clob: {
      delta: stamped.clob.delta,
      volume: stamped.clob.volume,
      currentRoundUpPricePoint,
      bestBidAskSummary: stamped.clob.bestBidAskSummary,
      topLevels: topLevelsForTick(stamped)
    },
    uiMeta: {
      countdownMs: stamped.uiMeta.countdownMs,
      countdownTargetTs: countdownTargetTsFor(stamped),
      acceptingOrders: stamped.uiMeta.acceptingOrders,
      marketSwitchState: stamped.uiMeta.marketSwitchState,
      sourceStatusSummary: stamped.uiMeta.sourceStatusSummary
    }
  };
}

function createMarketTickPayload(coalescedCount = 0, viewedUserId = ""): MarketTickPayload {
  const snapshot = store.marketSnapshot;
  const transportMeta = nextMarketTransportMeta(coalescedCount, snapshot.serverNow);
  const currentRound = store.getCurrentRound();
  const settlementPreview = currentRound ? engine.getSettlementPreview(currentRound) : undefined;
  return {
    viewedUserId,
    currentRound: decorateCurrentRoundForTransport(currentRound),
    tick: createMarketRealtimeTick(snapshot, transportMeta.serverPublishTs),
    settlementPreview,
    transportMeta
  };
}

type MarketBroadcastFrame = {
  data: MarketTickPayload;
  bytes: number;
  buildMs: number;
  serializeMs: number;
};

type MarketBroadcastClient = {
  actorUserId: string;
  viewedUserId: string;
  socket: WsWebSocket;
  closed: boolean;
  sendingTick: boolean;
  sendingFull: boolean;
  fullOrdinal: number;
  fullTimer?: NodeJS.Timeout;
  fullRetryTimer?: NodeJS.Timeout;
};

const marketBroadcastClients = new Set<MarketBroadcastClient>();
let marketBroadcastClientOrdinal = 0;
let marketBroadcastLastSentAt = 0;
let marketBroadcastCoalescedCount = 0;
let marketBroadcastRetryTimer: NodeJS.Timeout | undefined;
let marketBroadcastTickTimer: NodeJS.Timeout | undefined;
let marketBroadcastBackpressureDropped = false;
let marketBroadcastStarted = false;

function createMarketBroadcastFrame(coalescedCount = 0, droppedForBackpressure = false): MarketBroadcastFrame {
  const buildStartedAt = Date.now();
  const data = createMarketTickPayload(coalescedCount);
  const buildMs = Date.now() - buildStartedAt;
  data.transportMeta.broadcastBuildMs = buildMs;
  data.transportMeta.broadcastFanoutSize = marketBroadcastClients.size;
  data.transportMeta.droppedForBackpressure = droppedForBackpressure || undefined;
  markTransportSendStart(data.transportMeta);
  const serializeStartedAt = Date.now();
  const outbound = JSON.stringify({ type: "market:tick", data });
  const serializeMs = Date.now() - serializeStartedAt;
  return {
    data,
    bytes: Buffer.byteLength(outbound),
    buildMs,
    serializeMs
  };
}

function scheduleMarketBroadcastRetry(delayMs = MARKET_WS_RETRY_MS) {
  if (marketBroadcastRetryTimer) {
    return;
  }
  marketBroadcastRetryTimer = setTimeout(() => {
    marketBroadcastRetryTimer = undefined;
    flushMarketBroadcast();
  }, Math.max(delayMs, MARKET_WS_RETRY_MS));
}

function broadcastMarketTickFrame(frame: MarketBroadcastFrame) {
  const sendStartedAt = Date.now();
  let skippedForBackpressure = 0;
  let maxBufferedAmount = 0;
  for (const client of marketBroadcastClients) {
    if (client.closed || client.socket.readyState !== WsWebSocket.OPEN) {
      continue;
    }
    const actor = store.getUserById(client.actorUserId);
    if (!actor?.isActive) {
      client.socket.close();
      continue;
    }
    maxBufferedAmount = Math.max(maxBufferedAmount, client.socket.bufferedAmount);
    if (client.sendingTick || client.socket.bufferedAmount > 0) {
      skippedForBackpressure += 1;
      marketBroadcastBackpressureDropped = true;
      continue;
    }
    client.sendingTick = true;
    const outbound = JSON.stringify({
      type: "market:tick",
      data: {
        ...frame.data,
        viewedUserId: client.viewedUserId
      }
    });
    const bytes = Buffer.byteLength(outbound);
    client.socket.send(outbound, (error?: Error) => {
      client.sendingTick = false;
      appMetrics.recordWsSend("market", bytes, Date.now() - sendStartedAt, !error);
    });
  }
  appMetrics.recordMarketBroadcast({
    buildMs: frame.buildMs,
    serializeMs: frame.serializeMs,
    fanoutSize: marketBroadcastClients.size,
    skippedForBackpressure,
    maxBufferedAmount
  });
}

function flushMarketBroadcast() {
  if (marketBroadcastClients.size === 0) {
    marketBroadcastCoalescedCount = 0;
    return;
  }
  const now = Date.now();
  const elapsedSinceLastSend = marketBroadcastLastSentAt ? now - marketBroadcastLastSentAt : MARKET_WS_MIN_INTERVAL_MS;
  if (elapsedSinceLastSend < MARKET_WS_MIN_INTERVAL_MS) {
    scheduleMarketBroadcastRetry(MARKET_WS_MIN_INTERVAL_MS - elapsedSinceLastSend);
    return;
  }
  const frame = createMarketBroadcastFrame(marketBroadcastCoalescedCount, marketBroadcastBackpressureDropped);
  marketBroadcastCoalescedCount = 0;
  marketBroadcastBackpressureDropped = false;
  marketBroadcastLastSentAt = Date.now();
  broadcastMarketTickFrame(frame);
}

function requestMarketBroadcastTick(markCoalesced: boolean) {
  if (markCoalesced) {
    marketBroadcastCoalescedCount += 1;
  }
  if (marketBroadcastClients.size === 0) {
    return;
  }
  flushMarketBroadcast();
}

function handleMarketUpdate() {
  requestMarketBroadcastTick(true);
}

function scheduleFullSnapshotForClient(client: MarketBroadcastClient, delayMs = MARKET_WS_FULL_SNAPSHOT_INTERVAL_MS) {
  if (client.closed || client.fullTimer || client.fullRetryTimer) {
    return;
  }
  client.fullTimer = setTimeout(() => {
    client.fullTimer = undefined;
    sendFullSnapshotForClient(client);
  }, delayMs);
}

function retryFullSnapshotForClient(client: MarketBroadcastClient) {
  if (client.closed || client.fullRetryTimer) {
    return;
  }
  client.fullRetryTimer = setTimeout(() => {
    client.fullRetryTimer = undefined;
    sendFullSnapshotForClient(client);
  }, MARKET_WS_FULL_SNAPSHOT_RETRY_MS);
}

function sendFullSnapshotForClient(client: MarketBroadcastClient) {
  if (client.closed || client.socket.readyState !== WsWebSocket.OPEN) {
    return;
  }
  const actor = store.getUserById(client.actorUserId);
  if (!actor?.isActive) {
    client.socket.close();
    return;
  }
  const tickRecentlySent =
    marketBroadcastLastSentAt > 0 && Date.now() - marketBroadcastLastSentAt < MARKET_WS_MIN_INTERVAL_MS;
  if (
    client.sendingFull ||
    client.sendingTick ||
    marketBroadcastRetryTimer ||
    tickRecentlySent ||
    client.socket.bufferedAmount > 0
  ) {
    retryFullSnapshotForClient(client);
    return;
  }
  client.sendingFull = true;
  const data = createMarketPayload(client.viewedUserId);
  const sendStartedAt = markTransportSendStart(data.transportMeta);
  const outbound = JSON.stringify({ type: "market", data });
  const bytes = Buffer.byteLength(outbound);
  client.socket.send(outbound, (error?: Error) => {
    client.sendingFull = false;
    appMetrics.recordWsSend("market", bytes, Date.now() - sendStartedAt, !error);
    if (!client.closed) {
      scheduleFullSnapshotForClient(client);
    }
  });
}

function initialFullSnapshotDelayMs(client: MarketBroadcastClient) {
  if (marketBroadcastClients.size <= 1) {
    return 0;
  }
  return (client.fullOrdinal % MARKET_WS_INITIAL_FULL_SNAPSHOT_SLOTS) * MARKET_WS_FULL_SNAPSHOT_STAGGER_MS;
}

function registerMarketBroadcastClient(actorUserId: string, viewedUserId: string, socket: WsWebSocket) {
  const client: MarketBroadcastClient = {
    actorUserId,
    viewedUserId,
    socket,
    closed: false,
    sendingTick: false,
    sendingFull: false,
    fullOrdinal: marketBroadcastClientOrdinal++
  };
  marketBroadcastClients.add(client);
  requestMarketBroadcastTick(false);
  scheduleFullSnapshotForClient(
    client,
    Math.max(initialFullSnapshotDelayMs(client), MARKET_WS_FULL_SNAPSHOT_RETRY_MS)
  );
  return () => {
    client.closed = true;
    marketBroadcastClients.delete(client);
    if (client.fullTimer) {
      clearTimeout(client.fullTimer);
    }
    if (client.fullRetryTimer) {
      clearTimeout(client.fullRetryTimer);
    }
  };
}

function startMarketBroadcasting() {
  if (marketBroadcastStarted) {
    return;
  }
  marketBroadcastStarted = true;
  marketBroadcastTickTimer = setInterval(
    () => requestMarketBroadcastTick(false),
    Math.max(MARKET_WS_MIN_INTERVAL_MS, 50)
  );
  store.emitter.on("market:update", handleMarketUpdate);
}

function stopMarketBroadcasting() {
  if (!marketBroadcastStarted) {
    return;
  }
  marketBroadcastStarted = false;
  if (marketBroadcastTickTimer) {
    clearInterval(marketBroadcastTickTimer);
    marketBroadcastTickTimer = undefined;
  }
  if (marketBroadcastRetryTimer) {
    clearTimeout(marketBroadcastRetryTimer);
    marketBroadcastRetryTimer = undefined;
  }
  store.emitter.off("market:update", handleMarketUpdate);
}

function createBootstrapPayload(user: UserRecord, viewedUser: UserRecord = user) {
  const market = createCurrentRoundPayload(0, viewedUser.id);
  return {
    ...market,
    viewedUserId: viewedUser.id,
    viewedUser: store.sanitizeUser(viewedUser),
    history: getHistoryWithSettlementPreview(30, viewedUser.id),
    me: store.sanitizeUser(user),
    operatedHistory: getOperatedHistoryWithSettlementPreview(200, viewedUser.id),
    profile: store.getProfile(viewedUser.id),
    positions: store.getPositions(viewedUser.id),
    orders: store.getOrders(viewedUser.id),
    orderLifecycles: store.getOrderLifecycleLogs(viewedUser.id),
    logs: store.getRecentLogs(viewedUser.id),
    sourceStatus: user.permissionCodes.includes("system:status:view" as never) ? store.getSourceStatus() : []
  };
}

function createUserFullPayload(user: UserRecord) {
  return {
    viewedUserId: user.id,
    viewedUser: store.sanitizeUser(user),
    profile: store.getProfile(user.id),
    operatedHistory: getOperatedHistoryWithSettlementPreview(500, user.id),
    positions: store.getPositions(user.id),
    orders: store.getOrders(user.id),
    orderLifecycles: store.getOrderLifecycleLogs(user.id),
    logs: store.getRecentLogs(user.id)
  };
}

function createUserTradePayload(user: UserRecord) {
  return {
    viewedUserId: user.id,
    profile: store.getProfile(user.id),
    positions: store.getPositions(user.id),
    orders: store.getRecentTradeOrders(user.id, USER_TRADE_ORDER_LIMIT),
    orderLifecycles: store.getOrderLifecycleLogs(user.id).slice(0, USER_TRADE_LIFECYCLE_LIMIT)
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

async function recordUserManagementAudit(input: {
  actor: UserRecord;
  actionType:
    | "user.create"
    | "user.bulkCreate"
    | "user.update"
    | "user.disable"
    | "user.enable"
    | "user.resetPassword"
    | "user.changePassword"
    | "user.group.update"
    | "user.balance.set";
  success: boolean;
  serverRecvTs: number;
  targetUserId?: string;
  resultMessage: string;
  details?: Record<string, unknown>;
}) {
  const serverPublishTs = Date.now();
  await store.recordLog({
    eventId: store.newId("evt"),
    traceId: store.newTraceId(),
    category: "operation",
    actionType: input.actionType,
    actionStatus: input.success ? "success" : "failed",
    userId: input.actor.id,
    role: input.actor.role,
    pageName: "admin.users",
    moduleName: "user-management",
    resultCode: input.success ? "USER_MANAGEMENT_SUCCESS" : "USER_MANAGEMENT_FAILED",
    resultMessage: input.resultMessage,
    serverRecvTs: input.serverRecvTs,
    serverPublishTs,
    backendLatencyMs: Math.max(serverPublishTs - input.serverRecvTs, 0),
    details: {
      actorUserId: input.actor.id,
      actorRole: input.actor.role,
      targetUserId: input.targetUserId,
      ...input.details
    }
  });
}

function exportIncludesSystem(query: ExportQuery, system: Exclude<LogSystem, "all">) {
  return selectedExportSystems(query).includes(system);
}

async function buildLogsExportZip(actor: UserRecord, query: ExportQuery) {
  const allUsers: ExportUser[] = store.listUsers().map((user) => ({
    ...user,
    anonId: store.anonymizeUserId(user.id)
  }));
  const scopedQuery = resolveLogSearchFilters(actor, query);
  const visibleExportUsers = resolveExportUsers(actor, allUsers);
  const requestedUserIds = query.userIds?.length
    ? [...new Set(query.userIds)]
    : scopedQuery.userId
      ? [scopedQuery.userId]
      : undefined;
  if (requestedUserIds?.length) {
    const visibleIds = new Set(visibleExportUsers.map((user) => user.id));
    if (requestedUserIds.some((userId) => !visibleIds.has(userId))) {
      throw new Error("Logs are not available for this user.");
    }
  }
  const baseExportUsers = requestedUserIds?.length
    ? visibleExportUsers.filter((user) => requestedUserIds.includes(user.id))
    : visibleExportUsers;
  const exportUsers = baseExportUsers.filter((user) => {
    if (query.role && user.role !== query.role) {
      return false;
    }
    if (scopedQuery.userIds && !scopedQuery.userIds.includes(user.id)) {
      return false;
    }
    return true;
  });
  const users: UserExportData[] = await Promise.all(
    exportUsers.map(async (user) => {
      const userQuery = resolveLogSearchFilters(actor, {
        ...query,
        userId: user.id,
        limit: undefined,
        cursor: undefined
      });
      const auditLogs = exportIncludesSystem(query, "audit")
        ? await store.searchAuditLogs(userQuery, { limit: LOG_EXPORT_MAX_ROWS_PER_FILE })
        : [];
      const trainingLogs = exportIncludesSystem(query, "training")
        ? await store.searchBehaviorLogs(userQuery, { limit: LOG_EXPORT_MAX_ROWS_PER_FILE })
        : [];
      const matchingEvents = exportIncludesSystem(query, "matching")
        ? query.matchingLogKind === "action" || query.logGroup || query.latencySource || query.connectionState || query.latencyPhase
          ? []
          : await engine
            .searchMatchingEvents({
              from: userQuery.from,
              to: userQuery.to,
              userId: user.id,
              roundId: userQuery.roundId,
              marketId: userQuery.marketId,
              bookKey: userQuery.bookKey,
              bookSide: userQuery.bookSide,
              eventType: userQuery.eventType,
              traceId: userQuery.traceId,
              orderId: userQuery.orderId,
              sequenceFrom: userQuery.sequenceFrom,
              sequenceTo: userQuery.sequenceTo,
              limit: LOG_EXPORT_MAX_ROWS_PER_FILE
            })
            .then((result) => result.events)
            .catch(() => [])
        : [];
      const matchingActionLogs =
        exportIncludesSystem(query, "matching") && query.matchingLogKind !== "engine"
          ? await store.searchAuditLogs(
              { ...userQuery, category: "matching", matchingLogKind: "action" },
              { limit: LOG_EXPORT_MAX_ROWS_PER_FILE }
            )
          : [];
      return {
        user,
        auditLogs,
        trainingLogs,
        matchingEvents,
        matchingActionLogs,
        orders: filterOrdersForExport(store.getOrderLifecycleLogs(user.id, { includeSnapshots: true }), query),
        positions: filterPositionsForExport(store.getPositions(user.id), query),
        operatedRounds: filterRoundsForExport(store.getOperatedHistory(Number.MAX_SAFE_INTEGER, user.id), query),
        profile: store.getProfile(user.id)
      };
    })
  );
  const shouldIncludeSystemLatency =
    exportIncludesSystem(query, "audit") &&
    !query.userId &&
    !query.userIds?.length &&
    !query.role &&
    canViewAllLogs(actor) &&
    (!query.category || query.category === "latency");
  const systemLatencyLogs = shouldIncludeSystemLatency
    ? await store.searchAuditLogs(
        {
          from: query.from,
          to: query.to,
          category: "latency",
          actionType: query.actionType,
          actionStatus: query.actionStatus,
          moduleName: query.moduleName,
          pageName: query.pageName,
          symbol: query.symbol,
          traceId: query.traceId,
          roundId: query.roundId,
          orderId: query.orderId,
          positionId: query.positionId,
          marketId: query.marketId,
          marketSlug: query.marketSlug,
          resultCode: query.resultCode,
          logGroup: query.logGroup,
          latencySource: query.latencySource,
          connectionState: query.connectionState,
          latencyPhase: query.latencyPhase,
          latencyMinMs: query.latencyMinMs,
          latencyMaxMs: query.latencyMaxMs
        },
        { limit: LOG_EXPORT_MAX_ROWS_PER_FILE }
      ).then((logs) => logs.filter((log) => !log.userId))
    : [];
  const shouldIncludeSystemMatching =
    exportIncludesSystem(query, "matching") &&
    !query.userId &&
    !query.userIds?.length &&
    !query.role &&
    canViewAllLogs(actor);
  const exportedUserIds = new Set(exportUsers.map((user) => user.id));
  const systemMatchingEvents = shouldIncludeSystemMatching && query.matchingLogKind !== "action" && !query.logGroup && !query.latencySource && !query.connectionState && !query.latencyPhase
    ? await engine
        .searchMatchingEvents({
          from: query.from,
          to: query.to,
          roundId: query.roundId,
          marketId: query.marketId,
          bookKey: query.bookKey,
          bookSide: query.bookSide,
          eventType: query.eventType,
          traceId: query.traceId,
          orderId: query.orderId,
          sequenceFrom: query.sequenceFrom,
          sequenceTo: query.sequenceTo,
          limit: LOG_EXPORT_MAX_ROWS_PER_FILE
        })
        .then((result) =>
          result.events.filter((event) => {
            const eventUserId = matchingEventUserId(event);
            return !eventUserId || !exportedUserIds.has(eventUserId);
          })
        )
        .catch(() => [])
    : [];
  const systemMatchingActionLogs =
    shouldIncludeSystemMatching && query.matchingLogKind !== "engine"
      ? await store
          .searchAuditLogs(
            {
              ...query,
              category: "matching",
              matchingLogKind: "action",
              userIds: undefined,
              userId: undefined
            },
            { limit: LOG_EXPORT_MAX_ROWS_PER_FILE }
          )
          .then((logs) => logs.filter((log) => !log.userId || !exportedUserIds.has(log.userId)))
      : [];
  const dateLabel = new Date().toISOString().slice(0, 10);
  const actorExportUser: ExportUser = {
    ...store.sanitizeUser(actor),
    anonId: store.anonymizeUserId(actor.id)
  };
  const generatedAt = Date.now();
  return createZipArchive(
    buildExportEntries({
      actor: actorExportUser,
      users,
      systemMatchingEvents,
      systemMatchingActionLogs,
      systemLatencyLogs,
      query,
      generatedAt,
      dateLabel,
      singleUser: Boolean(query.userId) || users.length === 1
    })
  );
}

async function loadDatasetExportLogs(actor: UserRecord, request: DatasetExportRequest) {
  if (!hasPermission(actor, "data:export:all") && !hasPermission(actor, "data:export:managed")) {
    throw new ApiError(403, "Missing permission: data export.", "PERMISSION_DENIED");
  }
  if (request.includeDGrade && !hasPermission(actor, "data:export:include-d")) {
    throw new ApiError(403, "Only Admin can include D grade rows.", "PERMISSION_DENIED");
  }
  const allUsers = store.listUsers() as unknown as UserRecord[];
  const visibleUserIds = getVisibleUserIdsForActor(actor, allUsers);
  const requestedUserIds = request.userIds?.length ? request.userIds : visibleUserIds;
  const userIds = requestedUserIds.filter((userId) => {
    const target = store.getUserById(userId);
    return target ? canExportUser(actor, target.id, allUsers) : false;
  });
  if (userIds.length !== requestedUserIds.length) {
    throw new ApiError(403, "Dataset export includes users outside your scope.", "PERMISSION_DENIED");
  }
  const logs = await store.searchBehaviorLogs(
    {
      from: request.from,
      to: request.to,
      userIds,
      limit: LOG_EXPORT_MAX_ROWS_PER_FILE
    },
    { limit: LOG_EXPORT_MAX_ROWS_PER_FILE }
  );
  return { userIds, logs };
}

async function safeRoute<T>(handler: () => Promise<T>) {
  return handler();
}

function warnForLocalMisconfiguration() {
  if (serverConfig.upstreamProxyUrl) {
    console.warn(`[startup] Using upstream proxy for Binance/Coinbase/Polymarket: ${serverConfig.upstreamProxyUrl}`);
  }
  if (!serverConfig.coinbaseEnabled) {
    console.warn("[startup] Testing mode: Coinbase disabled. Local success will depend on Binance and Polymarket only.");
    return;
  }

  const warnings: string[] = [];
  if (!serverConfig.coinbaseWsUrl || !serverConfig.coinbaseWsUrl.startsWith("wss://")) {
    warnings.push("COINBASE_WS_URL must point to the Coinbase Advanced Trade WebSocket.");
  }
  if (warnings.length === 0) {
    return;
  }

  console.warn("[startup] Full real-source local success is not possible until the following items are fixed:");
  for (const warning of warnings) {
    console.warn(`[startup] - ${warning}`);
  }
}

let matchingRuntime: Awaited<ReturnType<typeof createMatchingServiceApp>> | undefined;

const shutdown = async () => {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  stopMarketBroadcasting();
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
  logStartupStage("bootstrap start");
  warnForLocalMisconfiguration();
  if (serverConfig.embeddedMatchingService) {
    logStartupStage("embedded matching bootstrap start");
    matchingRuntime = await createMatchingServiceApp({
      databaseUrl: serverConfig.databaseUrl,
      redisUrl: serverConfig.redisUrl,
      persistenceMode: serverConfig.persistenceMode,
      redisSnapshotSeconds: serverConfig.snapshotRetentionSeconds,
      strictPersistence: serverConfig.strictPersistence,
      pgConnectionTimeoutMs: serverConfig.pgConnectionTimeoutMs,
      pgIdleTimeoutMs: serverConfig.pgIdleTimeoutMs,
      pgMaxConnections: serverConfig.pgMaxConnections,
      pgKeepAlive: serverConfig.pgKeepAlive,
      pgReconnectIntervalMs: serverConfig.pgReconnectIntervalMs,
      pgReconnectMaxIntervalMs: serverConfig.pgReconnectMaxIntervalMs,
      eventsMemoryMax: serverConfig.matchingEventsMemoryMax,
      eventsMemoryMaxAgeMs: serverConfig.matchingEventsMemoryMaxAgeMs,
      booksMemoryMax: serverConfig.matchingBooksMemoryMax
    });
    await matchingRuntime.app.listen({
      host: "0.0.0.0",
      port: serverConfig.matchingServicePort
    });
    logStartupStage(`embedded matching bootstrap done port=${serverConfig.matchingServicePort}`);
  }

  logStartupStage("store.init start");
  await store.init();
  logStartupStage("store.init done");

  await app.register(cors, {
    origin: (origin, callback) => {
      if (!origin || serverConfig.corsOrigins.length === 0 || serverConfig.corsOrigins.includes(origin)) {
        callback(null, true);
        return;
      }
      callback(new Error("CORS origin is not allowed."), false);
    },
    credentials: true
  });
  await app.register(websocket);
  startMarketBroadcasting();
  app.addHook("onRequest", async (request, reply) => {
    httpStartTimes.set(request, Date.now());
    const requestId =
      typeof request.headers["x-request-id"] === "string" && request.headers["x-request-id"].trim()
        ? request.headers["x-request-id"].trim()
        : `req_${nanoid(12)}`;
    request.headers["x-request-id"] = requestId;
    reply.header("x-request-id", requestId);
    if (shuttingDown && request.url.startsWith("/api/") && !request.url.startsWith("/api/health/live")) {
      throw new ApiError(503, "Server is shutting down.", "SERVER_SHUTTING_DOWN");
    }
  });
  app.addHook("onResponse", async (request, reply) => {
    const startedAt = httpStartTimes.get(request) ?? Date.now();
    const route = request.routeOptions.url ?? request.url.split("?")[0] ?? "unknown";
    appMetrics.recordHttp(request.method, route, reply.statusCode, Math.max(Date.now() - startedAt, 0));
  });
  app.setErrorHandler((error, _request, reply) => sendApiError(reply, error));

  app.get("/health", async () => {
    const matching = await engine.getMatchingHealth().catch(() => undefined);
    const sources = store.getSourceStatus();
    const currentRound = store.getCurrentRound();
    const memory = store.getMemoryStatus();
    return {
      ok: true,
      serverNow: Date.now(),
      symbol: serverConfig.symbol,
      persistence: store.getPersistenceStatus(),
      heapUsedMb: memory.heapUsedMb,
      heapLimitMb: memory.heapLimitMb,
      memoryProtectionState: memory.memoryProtectionState,
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

  app.get("/api/health/live", async () => ({
    ok: true,
    shuttingDown,
    serverNow: Date.now(),
    uptimeSec: Math.round(process.uptime())
  }));

  app.get("/api/health/ready", async (request, reply) => {
    const persistence = store.getPersistenceStatus();
    const matching = await engine.getMatchingHealth().catch(() => undefined);
    const persistenceReady =
      persistence.postgres ||
      (!serverConfig.isProduction && serverConfig.persistenceMode === "memory" && !serverConfig.strictPersistence);
    const ready =
      !shuttingDown &&
      persistenceReady &&
      (!serverConfig.strictPersistence || persistence.state.postgres.state === "healthy") &&
      (!serverConfig.embeddedMatchingService || Boolean(matching?.ok));
    if (!ready) {
      reply.code(503);
    }
    return {
      ok: ready,
      shuttingDown,
      persistence,
      matchingService: matching ?? { ok: false },
      schemaMigration: serverConfig.expectedSchemaMigrationId,
      sources: store.getSourceStatus(),
      serverNow: Date.now()
    };
  });

  app.get("/api/metrics", async () => {
    updateRuntimeMetrics();
    const memory = store.getMemoryStatus();
    const profileCount = store.listUsers().length;
    const jsonlStats = store.getJsonlStats();
    const persistence = store.getPersistenceStatus();
    const sources = store.getSourceStatus();
    const orderLatencies = store
      .getRecentLogs("")
      .filter((log) => log.actionType === "place_order" && typeof log.backendLatencyMs === "number")
      .map((log) => log.backendLatencyMs as number)
      .sort((a, b) => a - b);
    const p95Index = orderLatencies.length > 0 ? Math.min(orderLatencies.length - 1, Math.ceil(orderLatencies.length * 0.95) - 1) : -1;
    return {
      uptimeSec: Math.round(process.uptime()),
      memoryMb: memory.heapUsedMb,
      heapLimitMb: memory.heapLimitMb,
      memoryProtectionState: memory.memoryProtectionState,
      wsClients: wsConnectionCounts.market + wsConnectionCounts.user,
      wsClientsByChannel: { ...wsConnectionCounts },
      users: profileCount,
      orderLatencyP95Ms: p95Index >= 0 ? orderLatencies[p95Index] : 0,
      eventLoopLagMs: appMetrics.getEventLoopLagMs(),
      http: {
        metricsEnabled: serverConfig.metricsEnabled
      },
      ws: {
        connections: wsConnectionCounts.market + wsConnectionCounts.user,
        byChannel: { ...wsConnectionCounts }
      },
      orders: {
        latencyP95Ms: p95Index >= 0 ? orderLatencies[p95Index] : 0,
        sampleCount: orderLatencies.length
      },
      jsonl: jsonlStats,
      orderBookSnapshots: store.getOrderBookSnapshotQueueStats(),
      persistence,
      externalSources: sources.map((source) => ({
        source: source.source,
        state: source.state,
        sourceEventAgeMs: Math.max(Date.now() - source.sourceEventTs, 0)
      })),
      exports: appMetrics.getExportOverview(),
      sourceHealth: Object.fromEntries(sources.map((source) => [source.source.toLowerCase(), source.state]))
    };
  });

  app.get("/metrics", async (request, reply) => {
    if (!serverConfig.metricsEnabled) {
      reply.code(404);
      return "metrics disabled";
    }
    if (!metricsAuthorized(request.headers.authorization)) {
      reply.header("www-authenticate", 'Basic realm="metrics"');
      reply.code(401);
      return "unauthorized";
    }
    updateRuntimeMetrics();
    reply.header("content-type", appMetrics.contentType());
    return appMetrics.text();
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
      return { message: "Invalid login payload.", code: "VALIDATION_FAILED" };
    }
    const candidate = store.findUserByUsername(parsed.data.username);
    if (candidate && store.isUserLocked(candidate)) {
      await recordLoginAudit({
        username: parsed.data.username,
        user: candidate,
        success: false,
        serverRecvTs,
        resultMessage: "User account is temporarily locked."
      });
      reply.code(423);
      return { message: "User account is temporarily locked.", code: "ACCOUNT_LOCKED" };
    }
    const user = store.findUserByCredentials(parsed.data.username, parsed.data.password);
    if (!user) {
      const disabledMatch = candidate && store.verifyUserPassword(candidate, parsed.data.password) && !candidate.isActive;
      if (candidate && !disabledMatch) {
        await store.recordFailedLogin(candidate);
      }
      await recordLoginAudit({
        username: parsed.data.username,
        user: disabledMatch ? candidate : undefined,
        success: false,
        serverRecvTs,
        resultMessage: disabledMatch ? "User account is disabled." : "Invalid username or password."
      });
      reply.code(disabledMatch ? 403 : 401);
      return {
        message: disabledMatch ? "User account is disabled." : "Invalid username or password.",
        code: disabledMatch ? "ACCOUNT_DISABLED" : "AUTH_FAILED"
      };
    }

    const token = signToken(user);
    await store.recordSuccessfulLogin(user);
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
      permission_codes: user.permissionCodes,
      username: user.username,
      available_usdc: user.availableUsdc,
      is_active: user.isActive,
      senior_tester_id: user.seniorTesterId,
      manager_user_id: user.managerUserId ?? user.seniorTesterId,
      permission_level: user.permissionLevel ?? "Standard",
      created_at: user.createdAt,
      updated_at: user.updatedAt
    };
  });

  app.post("/api/ws/tickets", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      const parsed = wsTicketSchema.parse(request.body);
      return createWsTicket(user, parsed.channel, parsed.viewUserId);
    })
  );

  app.get("/api/me", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      return store.sanitizeUser(user);
    })
  );

  app.get("/api/bootstrap/full", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "trade:view");
      requirePermission(user, "profile:view");
      const viewedUser = getViewedUserFromRequest(user, request);
      return createBootstrapPayload(user, viewedUser);
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

  app.patch("/api/me", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      const serverRecvTs = Date.now();
      const parsed = selfProfileSchema.parse(request.body);
      const updated = await store.updateUserProfile(user.id, {
        displayName: parsed.displayName,
        language: parsed.language as Language | undefined
      });
      await recordUserManagementAudit({
        actor: user,
        actionType: "user.update",
        success: true,
        serverRecvTs,
        targetUserId: updated.id,
        resultMessage: "User profile was updated.",
        details: {
          username: updated.username,
          role: updated.role,
          selfService: true
        }
      });
      return store.sanitizeUser(updated);
    })
  );

  app.post("/api/me/password", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      const serverRecvTs = Date.now();
      const parsed = changePasswordSchema.parse(request.body);
      if (parsed.password !== parsed.confirmPassword) {
        throw new Error("Password confirmation does not match.");
      }
      const verifiedUser = store.findUserByCredentials(user.username, parsed.currentPassword);
      if (verifiedUser?.id !== user.id) {
        throw new Error("Current password is invalid.");
      }
      const updated = await store.resetUserPassword(user.id, parsed.password);
      await recordUserManagementAudit({
        actor: user,
        actionType: "user.changePassword",
        success: true,
        serverRecvTs,
        targetUserId: updated.id,
        resultMessage: "User password was changed.",
        details: {
          username: updated.username,
          role: updated.role
        }
      });
      return store.sanitizeUser(updated);
    })
  );

  app.get("/api/users", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      if (!canListUsers(user)) {
        throw new Error("Missing permission: users:list");
      }
      return listUsersForActor(user);
    })
  );

  app.post("/api/users", async (request) =>
    safeRoute(async () => {
      const actor = getUserFromRequest(request);
      requirePermission(actor, "users:create");
      const serverRecvTs = Date.now();
      const parsed = createUserSchema.parse(request.body);
      const requestedRole = parsed.role as Role;
      if (!canCreateUserForActor(actor, requestedRole)) {
        throw new Error("You can only create Tester accounts in your own group.");
      }
      const finalRole = actor.role === "Admin" ? requestedRole : "Tester";
      const requestedManagerUserId = parsed.managerUserId ?? parsed.seniorTesterId;
      if (actor.role !== "Admin" && requestedManagerUserId && requestedManagerUserId !== actor.id) {
        throw new Error("You can only assign new Tester accounts to your own group.");
      }
      const managerUserId = normalizeManagerUserId(
        finalRole,
        actor.role === "Admin" ? requestedManagerUserId ?? getDefaultGroupManagerId() : actor.id,
        { requireActive: true }
      );
      const created = await store.createUser({
        username: parsed.username,
        password: parsed.password,
        displayName: parsed.displayName,
        role: finalRole,
        language: (parsed.language ?? "zh-CN") as Language,
        seniorTesterId: managerUserId,
        managerUserId,
        permissionLevel: (parsed.permissionLevel ?? "Standard") as PermissionLevel,
        availableUsdc: parsed.availableUsdc ?? serverConfig.initialBalance
      });
      await recordUserManagementAudit({
        actor,
        actionType: "user.create",
        success: true,
        serverRecvTs,
        targetUserId: created.id,
        resultMessage: "User account was created.",
        details: {
          username: created.username,
          role: created.role,
          managerUserId: created.managerUserId ?? created.seniorTesterId,
          availableUsdc: created.availableUsdc
        }
      });
      return store.sanitizeUser(created);
    })
  );

  app.patch("/api/users/:id", async (request) =>
    safeRoute(async () => {
      const actor = getUserFromRequest(request);
      requirePermission(actor, "users:update");
      const serverRecvTs = Date.now();
      const params = request.params as { id: string };
      if (params.id === actor.id) {
        throw new Error("Use the profile controls to update your own account.");
      }
      const target = getTargetUserForManagement(actor, params.id);
      const parsed = updateUserSchema.parse(request.body);
      const nextRole = (parsed.role ?? target.role) as Role;
      const hasManagerPatch =
        Object.prototype.hasOwnProperty.call(parsed, "managerUserId") ||
        Object.prototype.hasOwnProperty.call(parsed, "seniorTesterId");
      if (actor.role !== "Admin" && parsed.role && parsed.role !== target.role) {
        throw new Error("Only Admin can change user roles.");
      }
      if (actor.role !== "Admin" && typeof parsed.isActive === "boolean") {
        throw new Error("Use enable/disable actions for account status changes.");
      }
      const requestedManagerUserId = parsed.managerUserId ?? parsed.seniorTesterId;
      const clearingManager = parsed.managerUserId === null || parsed.seniorTesterId === null;
      if (actor.role !== "Admin" && (clearingManager || (requestedManagerUserId && requestedManagerUserId !== actor.id))) {
        throw new Error("You can only keep Tester accounts in your own group.");
      }
      const managerUserId = normalizeManagerUserId(
        nextRole,
        actor.role !== "Admin"
          ? actor.id
          : clearingManager
            ? undefined
            : requestedManagerUserId ?? target.managerUserId ?? target.seniorTesterId,
        { requireActive: hasManagerPatch && !clearingManager }
      );
      const updated = await store.updateUserProfile(target.id, {
        displayName: parsed.displayName,
        role: nextRole,
        language: parsed.language as Language | undefined,
        seniorTesterId: managerUserId,
        managerUserId,
        permissionLevel: parsed.permissionLevel as PermissionLevel | undefined,
        availableUsdc: parsed.availableUsdc,
        isActive: parsed.isActive,
        disabledBy: parsed.isActive === false ? actor.id : undefined
      });
      await recordUserManagementAudit({
        actor,
        actionType: "user.update",
        success: true,
        serverRecvTs,
        targetUserId: updated.id,
        resultMessage: "User profile was updated.",
        details: {
          username: updated.username,
          role: updated.role,
          managerUserId: updated.managerUserId ?? updated.seniorTesterId,
          permissionLevel: updated.permissionLevel ?? "Standard",
          availableUsdc: updated.availableUsdc,
          isActive: updated.isActive
        }
      });
      return store.sanitizeUser(updated);
    })
  );

  app.patch("/api/users/:id/group", async (request) =>
    safeRoute(async () => {
      const actor = getUserFromRequest(request);
      requirePermission(actor, "users:manager:update");
      if (actor.role !== "Admin") {
        throw new Error("Only Admin can change user groups.");
      }
      const serverRecvTs = Date.now();
      const params = request.params as { id: string };
      const target = store.getUserById(params.id);
      if (!target) {
        throw new Error("Target user was not found.");
      }
      if (!canChangeUserGroupForActor(actor, target)) {
        throw new Error("Only Tester accounts can be moved between groups.");
      }
      const parsed = changeUserGroupSchema.parse(request.body);
      const managerUserId = normalizeManagerUserId("Tester", parsed.managerUserId, { requireActive: true });
      const updated = await store.updateUserProfile(target.id, {
        seniorTesterId: managerUserId,
        managerUserId
      });
      await recordUserManagementAudit({
        actor,
        actionType: "user.group.update",
        success: true,
        serverRecvTs,
        targetUserId: updated.id,
        resultMessage: "User group was changed.",
        details: {
          username: updated.username,
          role: updated.role,
          managerUserId: updated.managerUserId ?? updated.seniorTesterId
        }
      });
      return store.sanitizeUser(updated);
    })
  );

  app.get("/api/users/bulk/template.csv", async (request, reply) =>
    safeRoute(async () => {
      const actor = getUserFromRequest(request);
      requirePermission(actor, "users:bulk-create");
      reply.header("content-type", "text/csv; charset=utf-8");
      reply.header(
        "content-disposition",
        "attachment; filename=\"bulk-users-template.csv\"; filename*=UTF-8''%E6%89%B9%E9%87%8F%E7%94%A8%E6%88%B7%E6%A8%A1%E6%9D%BF.csv"
      );
      return `\uFEFF${CSV_BULK_USER_TEMPLATE}`;
    })
  );

  app.post("/api/users/bulk/csv/preview", async (request) =>
    safeRoute(async () => {
      const actor = getUserFromRequest(request);
      requirePermission(actor, "users:bulk-create");
      enforceRateLimit(clientKey(request, "bulk-users"), serverConfig.bulkImportRateLimitMax, serverConfig.writeRateLimitWindowMs);
      const parsed = bulkUsersCsvSchema.parse(request.body);
      const csv = parseBulkUsersCsv(parsed.csv, {
        findManagerByUsername: (username) => {
          const user = store.findUserByUsername(username);
          return user ? store.sanitizeUser(user) : undefined;
        }
      });
      const validation = validateBulkCreateUsers(csv.users, {
        initialBalance: serverConfig.initialBalance,
        usernameExists: (username) => Boolean(store.findUserByUsername(username)),
        seniorTesterExists: (userId) => {
          const senior = store.getUserById(userId);
          return Boolean(senior && (senior.role === "Senior Tester" || senior.role === "Test Engineer"));
        }
      });
      const failed = [...csv.failed, ...validation.failed];
      appMetrics.recordBulkImport("csv_preview", failed.length > 0 ? "failed" : "success");
      return {
        total: csv.total,
        valid: failed.length === 0 ? validation.normalized : [],
        failed
      };
    })
  );

  app.post("/api/users/bulk/csv", async (request) =>
    safeRoute(async () => {
      const actor = getUserFromRequest(request);
      requirePermission(actor, "users:bulk-create");
      enforceRateLimit(clientKey(request, "bulk-users"), serverConfig.bulkImportRateLimitMax, serverConfig.writeRateLimitWindowMs);
      const serverRecvTs = Date.now();
      const parsed = bulkUsersCsvSchema.parse(request.body);
      const csv = parseBulkUsersCsv(parsed.csv, {
        findManagerByUsername: (username) => {
          const user = store.findUserByUsername(username);
          return user ? store.sanitizeUser(user) : undefined;
        }
      });
      const validation = validateBulkCreateUsers(csv.users, {
        initialBalance: serverConfig.initialBalance,
        usernameExists: (username) => Boolean(store.findUserByUsername(username)),
        seniorTesterExists: (userId) => {
          const senior = store.getUserById(userId);
          return Boolean(senior && (senior.role === "Senior Tester" || senior.role === "Test Engineer"));
        }
      });
      const failed = [...csv.failed, ...validation.failed];
      if (failed.length > 0) {
        appMetrics.recordBulkImport("csv", "failed");
        await recordUserManagementAudit({
          actor,
          actionType: "user.bulkCreate",
          success: false,
          serverRecvTs,
          resultMessage: "CSV user import validation failed.",
          details: {
            requestedCount: csv.total,
            source: "csv",
            failed
          }
        });
        return { created: [], failed, total: csv.total };
      }

      const created = [];
      for (const item of validation.normalized) {
        const user = await store.createUser({
          username: item.username,
          password: item.password,
          displayName: item.displayName,
          role: item.role,
          language: item.language,
          seniorTesterId: item.seniorTesterId,
          managerUserId: item.managerUserId,
          permissionLevel: item.permissionLevel,
          mustChangePassword: item.mustChangePassword,
          availableUsdc: item.availableUsdc
        });
        created.push({
          rowNumber: item.rowNumber,
          user: store.sanitizeUser(user)
        });
      }
      await recordUserManagementAudit({
        actor,
        actionType: "user.bulkCreate",
        success: true,
        serverRecvTs,
        resultMessage: "CSV user import completed.",
        details: {
          requestedCount: csv.total,
          source: "csv",
          createdCount: created.length,
          usernames: created.map((item) => item.user.username)
        }
      });
      appMetrics.recordBulkImport("csv", "success");
      return { created, failed: [], total: csv.total };
    })
  );

  app.post("/api/users/bulk", async (request) =>
    safeRoute(async () => {
      const actor = getUserFromRequest(request);
      requirePermission(actor, "users:bulk-create");
      const serverRecvTs = Date.now();
      const parsed = bulkCreateUsersSchema.parse(request.body);
      const validation = validateBulkCreateUsers(parsed.users, {
        initialBalance: serverConfig.initialBalance,
        usernameExists: (username) => Boolean(store.findUserByUsername(username)),
        seniorTesterExists: (userId) => {
          const senior = store.getUserById(userId);
          return Boolean(senior && (senior.role === "Senior Tester" || senior.role === "Test Engineer"));
        }
      });
      if (validation.failed.length > 0) {
        appMetrics.recordBulkImport("legacy", "failed");
        await recordUserManagementAudit({
          actor,
          actionType: "user.bulkCreate",
          success: false,
          serverRecvTs,
          resultMessage: "Bulk user import validation failed.",
          details: {
            requestedCount: parsed.users.length,
            failed: validation.failed
          }
        });
        return {
          created: [],
          failed: validation.failed,
          total: parsed.users.length
        };
      }

      const created = [];
      for (const item of validation.normalized) {
        const user = await store.createUser({
          username: item.username,
          password: item.password,
          displayName: item.displayName,
          role: item.role,
          language: item.language,
          seniorTesterId: item.seniorTesterId,
          managerUserId: item.managerUserId,
          permissionLevel: item.permissionLevel,
          mustChangePassword: item.mustChangePassword,
          availableUsdc: item.availableUsdc
        });
        created.push({
          rowNumber: item.rowNumber,
          user: store.sanitizeUser(user)
        });
      }
      await recordUserManagementAudit({
        actor,
        actionType: "user.bulkCreate",
        success: true,
        serverRecvTs,
        resultMessage: "Bulk user import completed.",
        details: {
          requestedCount: parsed.users.length,
          createdCount: created.length,
          usernames: created.map((item) => item.user.username)
        }
      });
      appMetrics.recordBulkImport("legacy", "success");
      return {
        created,
        failed: [],
        total: parsed.users.length
      };
    })
  );

  app.post("/api/users/:id/disable", async (request) =>
    safeRoute(async () => {
      const actor = getUserFromRequest(request);
      requirePermission(actor, "users:disable");
      const serverRecvTs = Date.now();
      const params = request.params as { id: string };
      if (params.id === actor.id) {
        throw new Error("You cannot disable your own account.");
      }
      const target = getTargetUserForManagement(actor, params.id);
      const disabled = await store.disableUser(target.id, actor.id);
      await recordUserManagementAudit({
        actor,
        actionType: "user.disable",
        success: true,
        serverRecvTs,
        targetUserId: disabled.id,
        resultMessage: "User account was disabled.",
        details: {
          username: disabled.username,
          role: disabled.role
        }
      });
      return store.sanitizeUser(disabled);
    })
  );

  app.post("/api/users/:id/enable", async (request) =>
    safeRoute(async () => {
      const actor = getUserFromRequest(request);
      requirePermission(actor, "users:enable");
      const serverRecvTs = Date.now();
      const params = request.params as { id: string };
      const target = getTargetUserForManagement(actor, params.id);
      const enabled = await store.enableUser(target.id);
      await recordUserManagementAudit({
        actor,
        actionType: "user.enable",
        success: true,
        serverRecvTs,
        targetUserId: enabled.id,
        resultMessage: "User account was restored.",
        details: {
          username: enabled.username,
          role: enabled.role
        }
      });
      return store.sanitizeUser(enabled);
    })
  );

  app.post("/api/users/:id/reset-password", async (request) =>
    safeRoute(async () => {
      const actor = getUserFromRequest(request);
      requirePermission(actor, "users:reset-password");
      const serverRecvTs = Date.now();
      const params = request.params as { id: string };
      const parsed = resetPasswordSchema.parse(request.body);
      if (parsed.password !== parsed.confirmPassword) {
        throw new Error("Password confirmation does not match.");
      }
      const verifiedActor = store.findUserByCredentials(actor.username, parsed.currentPassword);
      if (verifiedActor?.id !== actor.id) {
        throw new Error("Current operator password is invalid.");
      }
      const target = getTargetUserForManagement(actor, params.id);
      const updated = await store.resetUserPassword(target.id, parsed.password);
      await recordUserManagementAudit({
        actor,
        actionType: "user.resetPassword",
        success: true,
        serverRecvTs,
        targetUserId: updated.id,
        resultMessage: "User password was reset.",
        details: {
          username: updated.username,
          role: updated.role
        }
      });
      return store.sanitizeUser(updated);
    })
  );

  app.post("/api/users/:id/balance", async (request) =>
    safeRoute(async () => {
      const actor = getUserFromRequest(request);
      requirePermission(actor, "users:balance:set");
      const serverRecvTs = Date.now();
      const params = request.params as { id: string };
      const parsed = userBalanceSchema.parse(request.body);
      const target = getTargetUserForBalance(actor, params.id);
      const updated = await store.setUserBalance(target.id, parsed.availableUsdc);
      await recordUserManagementAudit({
        actor,
        actionType: "user.balance.set",
        success: true,
        serverRecvTs,
        targetUserId: updated.id,
        resultMessage: "User available simulation balance was updated.",
        details: {
          username: updated.username,
          role: updated.role,
          availableUsdc: updated.availableUsdc
        }
      });
      return store.sanitizeUser(updated);
    })
  );

  app.get("/api/rounds/current", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "trade:view");
      const viewedUser = getViewedUserFromRequest(user, request);
      return createCurrentRoundPayload(0, viewedUser.id);
    })
  );

  app.get("/api/rounds/history", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "trade:view");
      const viewedUser = getViewedUserFromRequest(user, request);
      const limit = Number((request.query as { limit?: string }).limit ?? 10);
      return getHistoryWithSettlementPreview(limit, viewedUser.id);
    })
  );

app.post("/api/rounds/:id/admin-review", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      if (user.role !== "Admin") {
        throw new Error("Only Admin can review rounds.");
      }
      const params = request.params as { id: string };
      const parsed = settlementActionSchema.parse(request.body);
      return decorateRoundWithSettlementPreview(
        await store.withTransaction(() =>
          engine.adminReviewRound(user, {
            roundId: params.id,
            side: parsed.side,
            reason: parsed.reason
          })
        )
      );
    })
  );

  app.post("/api/rounds/:id/settle-my-positions", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      const params = request.params as { id: string };
      return store.withTransaction(() =>
        engine.redeemUserPositions(user, { roundId: params.id })
      );
    })
  );

  app.get("/api/rounds/unsettled", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      const now = Date.now();
      // 从 store.rounds 中找 Manual/AdminReviewed 轮次
      const unsettledStatuses = new Set(["Manual", "AdminReviewed"]);
      const rounds = store.rounds.filter(
        (round) =>
          unsettledStatuses.has(round.status) &&
          now >= round.endAt + 10 * 60 * 1000
      );
      // 再从当前用户的开放持仓中找需要结算的轮次
      const openPositionRoundIds = new Set(
        store.positions
          .filter((p) => p.userId === user.id && p.status === "open" && p.roundId)
          .map((p) => p.roundId)
      );
      const existingRoundIds = new Set(rounds.map((r) => r.id));
      for (const roundId of openPositionRoundIds) {
        if (!existingRoundIds.has(roundId)) {
          const round = await store.findRoundOrFallback(roundId);
          if (round) {
            rounds.push(round);
            existingRoundIds.add(roundId);
          }
        }
      }
      // Manual 排最前，AdminReviewed 按最近结算时间倒序（刚审核的排最前），其余按结束时间
      rounds.sort((a, b) => {
        if (a.status === "Manual" && b.status !== "Manual") return -1;
        if (b.status === "Manual" && a.status !== "Manual") return 1;
        if (a.status === "AdminReviewed" && b.status === "AdminReviewed") {
          return (b.settlementTs ?? b.endAt) - (a.settlementTs ?? a.endAt);
        }
        return (b.endAt ?? 0) - (a.endAt ?? 0);
      });
      return rounds.map((round) => decorateRoundWithSettlementPreview(round));
    })
  );

  app.get("/api/profile/rounds/operated", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "profile:view");
      const viewedUser = getViewedUserFromRequest(user, request);
      const limit = Number((request.query as { limit?: string }).limit ?? 500);
      return getOperatedHistoryWithSettlementPreview(limit, viewedUser.id);
    })
  );

  app.get("/api/profile/me", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "profile:view");
      const viewedUser = getViewedUserFromRequest(user, request);
      return store.getProfile(viewedUser.id);
    })
  );

  app.get("/api/positions/me", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "profile:view");
      const viewedUser = getViewedUserFromRequest(user, request);
      return store.getPositions(viewedUser.id);
    })
  );

  app.get("/api/orders/me", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "profile:view");
      const viewedUser = getViewedUserFromRequest(user, request);
      return store.getOrders(viewedUser.id);
    })
  );

  app.get("/api/order-lifecycles/me", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "profile:view");
      const viewedUser = getViewedUserFromRequest(user, request);
      return store.getOrderLifecycleLogs(viewedUser.id);
    })
  );

  app.post("/api/orders", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "trade:order");
      store.assertWritablePersistence("Order placement");
      const parsed = orderSchema.parse(request.body);
      const startedAt = Date.now();
      try {
        const result = await engine.placeOrder(
          user,
          parsed as {
            action?: "buy" | "sell";
            side: TradeSide;
            orderKind?: "market" | "limit";
            amount?: number;
            qty?: number;
            limitPrice?: number;
            clientOrderId?: string;
            clientSendTs?: number;
          }
        );
        appMetrics.recordOrder(result.order.status, Date.now() - startedAt);
        return {
          order: store.sanitizeOrder(result.order),
          tradePatch: createUserTradePayload(user) satisfies UserTradePayload
        };
      } catch (error) {
        appMetrics.recordOrder("failed", Date.now() - startedAt);
        throw error;
      }
    })
  );

  app.post("/api/orders/:id/cancel", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "trade:cancel");
      store.assertWritablePersistence("Order cancellation");
      const params = request.params as { id: string };
      const cancelled = store.sanitizeOrder(await engine.cancelOrder(user, params.id));
      appMetrics.recordOrder("cancelled", 0);
      return {
        order: cancelled,
        tradePatch: createUserTradePayload(user) satisfies UserTradePayload
      };
    })
  );

  app.post("/api/positions/:id/sell", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "trade:sell");
      store.assertWritablePersistence("Position sell");
      const params = request.params as { id: string };
      try {
        const sold = store.sanitizeOrder(await store.withTransaction(() => engine.sellPosition(user, params.id)));
        appMetrics.recordPositionClose("success");
        return {
          order: sold,
          tradePatch: createUserTradePayload(user) satisfies UserTradePayload
        };
      } catch (error) {
        appMetrics.recordPositionClose("failed");
        throw error;
      }
    })
  );

  app.post("/api/positions/close-side", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "trade:sell");
      store.assertWritablePersistence("Close side");
      const parsed = quickSideSchema.parse(request.body);
      try {
        const result = await store.withTransaction(() =>
          engine.closeSide(user, parsed as { side: TradeSide; clientSendTs?: number })
        );
        appMetrics.recordPositionClose("success");
        return {
          ...result,
          tradePatch: createUserTradePayload(user) satisfies UserTradePayload
        };
      } catch (error) {
        appMetrics.recordPositionClose("failed");
        throw error;
      }
    })
  );

  app.post("/api/positions/reverse-side", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "trade:sell");
      requirePermission(user, "trade:order");
      store.assertWritablePersistence("Reverse side");
      const parsed = quickSideSchema.parse(request.body);
      try {
        const result = await store.withTransaction(() =>
          engine.reverseSide(user, parsed as { side: TradeSide; clientSendTs?: number })
        );
        appMetrics.recordPositionClose("success");
        return {
          ...result,
          reverseOrder: store.sanitizeOrder(result.reverseOrder),
          tradePatch: createUserTradePayload(user) satisfies UserTradePayload
        };
      } catch (error) {
        appMetrics.recordPositionClose("failed");
        throw error;
      }
    })
  );

  app.get("/api/logs/me", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "profile:view");
      const viewedUser = getViewedUserFromRequest(user, request);
      return store.getRecentLogs(viewedUser.id);
    })
  );

  app.get("/api/logs/training", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      const parsed = trainingLogQuerySchema.parse(request.query) as BehaviorLogQuery;
      return store.getBehaviorLogs(resolveBehaviorLogFilters(user, parsed));
    })
  );

  app.get("/api/logs/search", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      const parsed = logSearchQuerySchema.parse(request.query) as LogSearchQuery;
      return await searchUnifiedLogs(user, parsed);
    })
  );

  app.get("/api/logs/facets", async (request) =>
    safeRoute(async () => {
      getUserFromRequest(request);
      return LOG_FACETS;
    })
  );

  app.post("/api/datasets/export/preview", async (request) =>
    safeRoute(async () => {
      const actor = getUserFromRequest(request);
      enforceRateLimit(clientKey(request, "dataset-export"), serverConfig.exportRateLimitMax, serverConfig.writeRateLimitWindowMs);
      const parsed = datasetExportSchema.parse(request.body) as DatasetExportRequest;
      if (parsed.format === "parquet") {
        throw new ApiError(501, "Parquet dataset export is not available yet.", "NOT_IMPLEMENTED");
      }
      const { userIds, logs } = await loadDatasetExportLogs(actor, parsed);
      return {
        ...previewDatasetExport(logs, parsed.includeDGrade),
        userCount: userIds.length
      };
    })
  );

  app.post("/api/datasets/export", async (request, reply) => {
    try {
      const actor = getUserFromRequest(request);
      enforceRateLimit(clientKey(request, "dataset-export"), serverConfig.exportRateLimitMax, serverConfig.writeRateLimitWindowMs);
      const parsed = datasetExportSchema.parse(request.body) as DatasetExportRequest;
      if (parsed.format === "parquet") {
        throw new ApiError(501, "Parquet dataset export is not available yet.", "NOT_IMPLEMENTED");
      }
      const { userIds, logs } = await loadDatasetExportLogs(actor, parsed);
      const exportId = store.newId("dataset_export");
      const generated = buildDatasetExport({
        exportId,
        actor: { id: actor.id, role: actor.role },
        request: parsed,
        userIds,
        logs,
        anonymizationSecret: serverConfig.exportAnonymizationSecret
      });
      await store.recordExportAudit({
        exportId,
        actorUserId: actor.id,
        actorRole: actor.role,
        exportType: "customer_dataset",
        format: "zip",
        scope: generated.manifest.scope,
        recordCount: generated.preview.recordCount,
        filteredDGradeCount: generated.preview.filteredDGradeCount,
        missingQualityCount: generated.preview.missingQualityCount,
        fileSha256: generated.sha256,
        createdAtMs: generated.manifest.generatedAt,
        details: {
          formats: generated.manifest.formats,
          userCount: userIds.length
        }
      });
      appMetrics.recordExport("customer_dataset", "success", generated.preview.recordCount);
      reply.header("content-type", "application/zip");
      reply.header("content-disposition", `attachment; filename="customer-dataset-${exportId}.zip"`);
      reply.header("x-export-id", exportId);
      reply.header("x-export-sha256", generated.sha256);
      return reply.send(generated.archive);
    } catch (error) {
      appMetrics.recordExport("customer_dataset", "failed");
      return sendApiError(reply, error, "Dataset export failed.");
    }
  });

  app.get("/api/logs/export", async (request, reply) => {
    try {
      const user = getUserFromRequest(request);
      const parsed = exportLogQuerySchema.parse(request.query) as ExportQuery;
      const body = await buildLogsExportZip(user, parsed);
      appMetrics.recordExport("internal_logs", "success");
      reply
        .header("Content-Type", "application/zip")
        .header("Content-Length", body.length)
        .header(
          "Content-Disposition",
          `attachment; filename="paper-trading-export-${new Date().toISOString().slice(0, 10)}.zip"`
        );
      return reply.send(body);
    } catch (error) {
      appMetrics.recordExport("internal_logs", "failed");
      return sendApiError(reply, error, "Log export failed.");
    }
  });

  app.post("/api/logs/export", async (request, reply) => {
    try {
      const user = getUserFromRequest(request);
      const parsed = logExportBodySchema.parse(request.body) as ExportQuery;
      const body = await buildLogsExportZip(user, parsed);
      appMetrics.recordExport("internal_logs", "success");
      reply
        .header("Content-Type", "application/zip")
        .header("Content-Length", body.length)
        .header(
          "Content-Disposition",
          `attachment; filename="paper-trading-export-${new Date().toISOString().slice(0, 10)}.zip"`
        );
      return reply.send(body);
    } catch (error) {
      appMetrics.recordExport("internal_logs", "failed");
      return sendApiError(reply, error, "Log export failed.");
    }
  });

  app.get("/api/logs/training/export", async (request, reply) => {
    try {
      const user = getUserFromRequest(request);
      const parsed = trainingLogQuerySchema.parse(request.query) as BehaviorLogQuery;
      const logs = store.getBehaviorLogs(resolveBehaviorLogFilters(user, parsed));
      const body = logs.map((log) => JSON.stringify(log)).join("\n");
      reply
        .header("Content-Type", "application/x-ndjson; charset=utf-8")
        .header(
          "Content-Disposition",
          `attachment; filename="behavior-action-logs-${new Date().toISOString().slice(0, 10)}.jsonl"`
        );
      return reply.send(body ? `${body}\n` : "");
    } catch (error) {
      return sendApiError(reply, error, "Training log export failed.");
    }
  });

  app.get("/api/logs/audit", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      const parsed = auditLogQuerySchema.parse(request.query) as AuditLogQuery;
      return store.getAuditLogs(resolveAuditLogFilters(user, parsed));
    })
  );

  app.get("/api/logs/round-activity", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "profile:view");
      const viewedUser = getViewedUserFromRequest(user, request);
      const parsed = auditLogQuerySchema.pick({ roundId: true }).required().parse(request.query) as { roundId: string };
      return {
        auditLogs: store.getAuditLogs({ userId: viewedUser.id, roundId: parsed.roundId }),
        behaviorLogs: store.getBehaviorLogs({ userId: viewedUser.id, roundId: parsed.roundId })
      };
    })
  );

  app.get("/api/logs/audit/export", async (request, reply) => {
    try {
      const user = getUserFromRequest(request);
      const parsed = auditLogQuerySchema.parse(request.query) as AuditLogQuery;
      const logs = store.getAuditLogs(resolveAuditLogFilters(user, parsed));
      const body = logs.map((log) => JSON.stringify(log)).join("\n");
      reply
        .header("Content-Type", "application/x-ndjson; charset=utf-8")
        .header(
          "Content-Disposition",
          `attachment; filename="audit-events-${new Date().toISOString().slice(0, 10)}.jsonl"`
        );
      return reply.send(body ? `${body}\n` : "");
    } catch (error) {
      return sendApiError(reply, error, "Audit log export failed.");
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
      const timelineUser = store.getUserById(timeline.order.userId);
      if (!timelineUser || !canViewUserRecords(user, timelineUser, store.listUserRecords())) {
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
      const memory = store.getMemoryStatus();
      return {
        serverNow: Date.now(),
        heapUsedMb: memory.heapUsedMb,
        heapLimitMb: memory.heapLimitMb,
        memoryProtectionState: memory.memoryProtectionState,
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
      const query = request.query as { token?: string; ticket?: string; viewUserId?: string };
      const session = getWsSession(query, "market");
      if (!session?.actor.isActive) {
        socket.close();
        return;
      }
      attachHeartbeat(socket, "market");
      wsConnectionCounts.market += 1;
      appMetrics.setWsConnections("market", wsConnectionCounts.market);
      const unregisterMarketClient = registerMarketBroadcastClient(session.actor.id, session.viewedUser.id, socket);
      socket.on("close", () => {
        unregisterMarketClient();
        wsConnectionCounts.market = Math.max(0, wsConnectionCounts.market - 1);
        appMetrics.setWsConnections("market", wsConnectionCounts.market);
        if (!consumeHeartbeatTimeout(socket)) {
          appMetrics.recordWsDisconnect("market", "close");
        }
      });
    } catch {
      socket.close();
    }
  });

  app.get("/ws/user", { websocket: true }, (socket, request) => {
    try {
      const query = request.query as { token?: string; ticket?: string; viewUserId?: string };
      const session = getWsSession(query, "user");
      if (!session?.actor.isActive) {
        socket.close();
        return;
      }
      const { actor, viewedUser } = session;
      attachHeartbeat(socket, "user");
      wsConnectionCounts.user += 1;
      appMetrics.setWsConnections("user", wsConnectionCounts.user);

      const eventName = `user:${viewedUser.id}`;
      let userSendInFlight = false;
      let pendingUserPayloadScope: UserPayloadScope | undefined;
      let userRetryTimer: NodeJS.Timeout | undefined;

      function mergeUserPayloadScope(current: UserPayloadScope | undefined, next: UserPayloadScope): UserPayloadScope {
        if (!current) {
          return next;
        }
        return current === "trade" || next === "trade" ? "trade" : "full";
      }

      const sendPayloadNow = (scope: UserPayloadScope = "full") => {
        const currentActor = store.getUserById(actor.id);
        const currentViewedUser = store.getUserById(viewedUser.id);
        if (!currentActor?.isActive || !currentViewedUser || !canViewUserRecords(currentActor, currentViewedUser, store.listUserRecords())) {
          socket.close();
          return;
        }
        if (userSendInFlight || socket.bufferedAmount > 0) {
          pendingUserPayloadScope = mergeUserPayloadScope(pendingUserPayloadScope, scope);
          queueUserPayload();
          return;
        }
        const buildStartedAt = Date.now();
        const outbound = JSON.stringify({
          type: scope === "trade" ? "user:trade" : "user",
          data: scope === "trade" ? createUserTradePayload(currentViewedUser) : createUserFullPayload(currentViewedUser)
        });
        const buildLatencyMs = Date.now() - buildStartedAt;
        const outboundBytes = Buffer.byteLength(outbound);
        appMetrics.recordUserWsPayload(scope, outboundBytes, buildLatencyMs);
        if (buildLatencyMs > 50 || outboundBytes > 200_000) {
          console.warn(
            `[ws:user] payload scope=${scope} bytes=${outboundBytes} buildMs=${buildLatencyMs}`
          );
        }
        const sendStartedAt = Date.now();
        userSendInFlight = true;
        socket.send(outbound, (error?: Error) => {
          userSendInFlight = false;
          appMetrics.recordWsSend("user", outboundBytes, Date.now() - sendStartedAt, !error);
          if (pendingUserPayloadScope) {
            queueUserPayload();
          }
        });
      };

      function queueUserPayload(scope?: UserPayloadScope) {
        if (scope) {
          pendingUserPayloadScope = mergeUserPayloadScope(pendingUserPayloadScope, scope);
        }
        if (userSendInFlight || userRetryTimer) {
          return;
        }
        userRetryTimer = setTimeout(() => {
          userRetryTimer = undefined;
          const nextScope = pendingUserPayloadScope;
          pendingUserPayloadScope = undefined;
          if (nextScope) {
            sendPayloadNow(nextScope);
          }
        }, USER_WS_RETRY_MS);
      }

      const listener = (scope?: UserPayloadScope) => queueUserPayload(scope ?? "full");
      queueUserPayload("full");
      store.emitter.on(eventName, listener);
      socket.on("close", () => {
        if (userRetryTimer) {
          clearTimeout(userRetryTimer);
        }
        wsConnectionCounts.user = Math.max(0, wsConnectionCounts.user - 1);
        appMetrics.setWsConnections("user", wsConnectionCounts.user);
        if (!consumeHeartbeatTimeout(socket)) {
          appMetrics.recordWsDisconnect("user", "close");
        }
        store.emitter.off(eventName, listener);
      });
    } catch {
      socket.close();
    }
  });

  logStartupStage("app.listen start");
  await app.listen({
    host: "0.0.0.0",
    port: serverConfig.port
  });
  logStartupStage(`app.listen done port=${serverConfig.port}`);

  logStartupStage("engine.start start");
  await engine.start();
  logStartupStage("engine.start done");
}

bootstrap().catch(async (error) => {
  console.error(error);
  await shutdown();
  process.exit(1);
});

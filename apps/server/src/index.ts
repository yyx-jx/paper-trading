import cors from "@fastify/cors";
import websocket from "@fastify/websocket";
import Fastify from "fastify";
import jwt from "jsonwebtoken";
import { WebSocket as WsWebSocket } from "ws";
import { z } from "zod";
import { serverConfig } from "./config";
import type {
  AuditLogQuery,
  BehaviorLogQuery,
  Language,
  LogSearchQuery,
  LogSearchResult,
  LogSystem,
  MarketPayload,
  MarketSnapshot,
  MarketTransportMeta,
  MatchingEventRecord,
  Role,
  RoundRecord,
  SettlementPreview,
  SourceHealth,
  TradeSide,
  UnifiedLogRow,
  UserRecord
} from "./domain/types";
import { createMatchingServiceApp } from "./services/matching/app";
import { MatchingServiceClient } from "./services/matching/client";
import { SimulationEngine } from "./services/simulation";
import { AppStore } from "./services/store";
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
import { validateBulkCreateUsers } from "./services/bulk-users";
import { LOG_FACETS } from "./services/log-facets";

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

let marketPayloadSeq = 0;
const MARKET_WS_RETRY_MS = 25;

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

const roleSchema = z.enum(["Tester", "Senior Tester", "Test Engineer", "Admin"]);

const createUserSchema = z.object({
  username: z.string().trim().min(1),
  password: z.string().min(1),
  displayName: z.string().trim().min(1),
  role: roleSchema,
  language: z.enum(["zh-CN", "en-US"]).optional(),
  seniorTesterId: z.string().optional(),
  availableUsdc: z.number().nonnegative().optional()
});

const bulkCreateUserItemSchema = z.object({
  username: z.string().trim().min(1),
  password: z.string().min(1),
  displayName: z.string().trim().optional(),
  role: roleSchema.optional(),
  language: z.enum(["zh-CN", "en-US"]).optional(),
  seniorTesterId: z.string().optional(),
  availableUsdc: z.number().nonnegative().optional()
});

const bulkCreateUsersSchema = z.object({
  users: z.array(bulkCreateUserItemSchema).min(1).max(500)
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

const logSearchQuerySchema = z.object({
  system: z.enum(["all", "audit", "training", "matching"]).optional(),
  from: z.coerce.number().optional(),
  to: z.coerce.number().optional(),
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
    .enum(["Trading", "Frozen", "Settling", "Polling", "Settled", "Redeeming", "Closed", "Manual"])
    .optional(),
  settlementResult: z.enum(["win", "loss", "sold"]).optional(),
  bookKey: z.string().optional(),
  bookSide: z.enum(["UP", "DOWN"]).optional(),
  eventType: z.enum(["external_book_synced", "order_executed", "order_cancelled"]).optional(),
  sequenceFrom: z.coerce.number().optional(),
  sequenceTo: z.coerce.number().optional(),
  logGroup: z.enum(["operation", "settlement", "market_latency", "system_latency", "matching_action"]).optional(),
  latencySource: z.enum(["binance", "chainlink", "clob", "system"]).optional(),
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

function requirePermission(user: UserRecord, code: string) {
  if (!user.permissionCodes.includes(code as never)) {
    throw new Error(`Missing permission: ${code}`);
  }
}

function canViewAllLogs(user: UserRecord) {
  return (
    user.role === "Admin" ||
    user.role === "Test Engineer" ||
    user.permissionCodes.includes("logs:view:all" as never)
  );
}

function canViewTeamLogs(user: UserRecord) {
  return user.role === "Senior Tester" || user.permissionCodes.includes("logs:view:team" as never);
}

function teamVisibleUserIds(user: UserRecord) {
  return [
    user.id,
    ...store
      .listUsers()
      .filter((candidate) => candidate.role === "Tester" && candidate.seniorTesterId === user.id)
      .map((candidate) => candidate.id)
  ];
}

function resolveAuditLogFilters(user: UserRecord, parsed: AuditLogQuery): AuditLogQuery {
  if (canViewAllLogs(user)) {
    return parsed;
  }
  if (canViewTeamLogs(user)) {
    const userIds = teamVisibleUserIds(user);
    if (parsed.userId && !userIds.includes(parsed.userId)) {
      throw new Error("Logs are not available for this user.");
    }
    return { ...parsed, userIds };
  }
  if (parsed.userId && parsed.userId !== user.id) {
    throw new Error("Logs are not available for this user.");
  }
  return { ...parsed, userId: user.id };
}

function resolveBehaviorLogFilters(user: UserRecord, parsed: BehaviorLogQuery): BehaviorLogQuery {
  if (canViewAllLogs(user)) {
    return parsed;
  }
  if (canViewTeamLogs(user)) {
    const userIds = teamVisibleUserIds(user);
    if (parsed.userId && !userIds.includes(parsed.userId)) {
      throw new Error("Logs are not available for this user.");
    }
    return { ...parsed, userIds };
  }
  if (parsed.userId && parsed.userId !== user.id) {
    throw new Error("Logs are not available for this user.");
  }
  return { ...parsed, userId: user.id };
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
  const allUsers = store.listUsers();
  const requestedUser = parsed.userId ? store.getUserById(parsed.userId) : undefined;
  if (parsed.userId && !requestedUser) {
    throw new Error("Target user was not found.");
  }
  if (parsed.userId && scopedIds && !scopedIds.includes(parsed.userId)) {
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

  let userIds = parsed.userId ? [parsed.userId] : parsed.userIds?.length ? [...new Set(parsed.userIds)] : scopedIds;
  if (parsed.role) {
    const roleIds = new Set(allUsers.filter((user) => user.role === parsed.role).map((user) => user.id));
    userIds = userIds ? userIds.filter((userId) => roleIds.has(userId)) : [...roleIds];
  }

  return {
    ...parsed,
    userId: parsed.userId,
    userIds: parsed.userId ? undefined : userIds,
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
    return ["binance", "chainlink", "clob"].includes(log.moduleName) ? "market_latency" : "system_latency";
  }
  return "operation" as const;
}

function deriveLatencySource(moduleName?: string) {
  if (moduleName === "binance" || moduleName === "chainlink" || moduleName === "clob") {
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
  return user.permissionCodes.includes("users:list" as never);
}

function listUsersForActor(actor: UserRecord) {
  if (actor.role === "Admin" || actor.role === "Test Engineer") {
    return store.listUsers();
  }
  if (actor.role === "Senior Tester") {
    const visibleIds = new Set(teamVisibleUserIds(actor));
    return store.listUsers().filter((user) => visibleIds.has(user.id));
  }
  return [];
}

function getTargetUserForManagement(actor: UserRecord, targetUserId: string) {
  const target = store.getUserById(targetUserId);
  if (!target) {
    throw new Error("Target user was not found.");
  }
  if (actor.role === "Admin") {
    return target;
  }
  if (actor.role === "Senior Tester" && target.role === "Tester" && target.seniorTesterId === actor.id) {
    return target;
  }
  throw new Error("Target user is outside your management scope.");
}

function getTargetUserForBalance(actor: UserRecord, targetUserId: string) {
  if (actor.role === "Senior Tester" && actor.id === targetUserId) {
    return actor;
  }
  return getTargetUserForManagement(actor, targetUserId);
}

function normalizeSeniorTesterId(role: Role, seniorTesterId?: string) {
  if (role !== "Tester") {
    return undefined;
  }
  if (!seniorTesterId) {
    return undefined;
  }
  const senior = store.getUserById(seniorTesterId);
  if (!senior || senior.role !== "Senior Tester") {
    throw new Error("seniorTesterId must point to a Senior Tester.");
  }
  return seniorTesterId;
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

function nextMarketTransportMeta(): MarketTransportMeta {
  marketPayloadSeq += 1;
  return {
    serverPublishTs: Date.now(),
    payloadSeq: marketPayloadSeq
  };
}

function decorateRoundWithSettlementPreview<T extends RoundRecord & { userPnl?: number }>(
  round: T
): T & { settlementPreview?: SettlementPreview } {
  const settlementPreview = engine.getSettlementPreview(round);
  return settlementPreview ? { ...round, settlementPreview } : round;
}

function getHistoryWithSettlementPreview(limit: number, userId?: string) {
  return store.getHistory(limit, userId).map((round) => decorateRoundWithSettlementPreview(round));
}

function getOperatedHistoryWithSettlementPreview(limit: number, userId: string) {
  return store.getOperatedHistory(limit, userId).map((round) => decorateRoundWithSettlementPreview(round));
}

function createCurrentRoundPayload() {
  const transportMeta = nextMarketTransportMeta();
  const currentRound = store.getCurrentRound();
  const history = getHistoryWithSettlementPreview(10);
  const settlementPreview =
    (currentRound ? engine.getSettlementPreview(currentRound) : undefined) ??
    engine.getLatestSettlementPreview(history);
  return {
    currentRound: currentRound ? decorateRoundWithSettlementPreview(currentRound) : undefined,
    snapshot: stampSnapshotForTransport(store.marketSnapshot, transportMeta.serverPublishTs),
    settlementPreview,
    transportMeta
  };
}

function createMarketPayload(userId: string): MarketPayload {
  return {
    ...createCurrentRoundPayload(),
    history: getHistoryWithSettlementPreview(10, userId)
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
    | "user.disable"
    | "user.enable"
    | "user.resetPassword"
    | "user.changePassword"
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
  const baseExportUsers = query.userIds?.length
    ? resolveExportUsers(actor, allUsers).filter((user) => query.userIds?.includes(user.id))
    : resolveExportUsers(actor, allUsers, query.userId);
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
    const candidate = store.findUserByUsername(parsed.data.username);
    const user = store.findUserByCredentials(parsed.data.username, parsed.data.password);
    if (!user) {
      const disabledMatch = candidate && candidate.password === parsed.data.password && !candidate.isActive;
      await recordLoginAudit({
        username: parsed.data.username,
        user: disabledMatch ? candidate : undefined,
        success: false,
        serverRecvTs,
        resultMessage: disabledMatch ? "User account is disabled." : "Invalid username or password."
      });
      reply.code(401);
      return {
        message: disabledMatch ? "User account is disabled." : "Invalid username or password.",
        code: disabledMatch ? "ACCOUNT_DISABLED" : "AUTH_FAILED"
      };
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
      permission_codes: user.permissionCodes,
      username: user.username,
      available_usdc: user.availableUsdc,
      is_active: user.isActive,
      senior_tester_id: user.seniorTesterId,
      created_at: user.createdAt,
      updated_at: user.updatedAt
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
      const seniorTesterId = normalizeSeniorTesterId(parsed.role as Role, parsed.seniorTesterId);
      const created = await store.createUser({
        username: parsed.username,
        password: parsed.password,
        displayName: parsed.displayName,
        role: parsed.role as Role,
        language: (parsed.language ?? "zh-CN") as Language,
        seniorTesterId,
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
          seniorTesterId: created.seniorTesterId,
          availableUsdc: created.availableUsdc
        }
      });
      return store.sanitizeUser(created);
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
          return Boolean(senior && senior.role === "Senior Tester");
        }
      });
      if (validation.failed.length > 0) {
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
      requirePermission(actor, "users:disable");
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
      return createCurrentRoundPayload();
    })
  );

  app.get("/api/rounds/history", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "trade:view");
      const limit = Number((request.query as { limit?: string }).limit ?? 10);
      return getHistoryWithSettlementPreview(limit, user.id);
    })
  );

  app.get("/api/profile/rounds/operated", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "profile:view");
      const limit = Number((request.query as { limit?: string }).limit ?? 500);
      return getOperatedHistoryWithSettlementPreview(limit, user.id);
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
      const result = await engine.placeOrder(
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
      return { order: store.sanitizeOrder(result.order) };
    })
  );

  app.post("/api/orders/:id/cancel", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "trade:cancel");
      const params = request.params as { id: string };
      return store.sanitizeOrder(await engine.cancelOrder(user, params.id));
    })
  );

  app.post("/api/positions/:id/sell", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "trade:sell");
      const params = request.params as { id: string };
      return store.sanitizeOrder(await engine.sellPosition(user, params.id));
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
      const result = await engine.reverseSide(user, parsed as { side: TradeSide; clientSendTs?: number });
      return {
        ...result,
        reverseOrder: store.sanitizeOrder(result.reverseOrder)
      };
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

  app.get("/api/logs/export", async (request, reply) => {
    try {
      const user = getUserFromRequest(request);
      const parsed = exportLogQuerySchema.parse(request.query) as ExportQuery;
      const body = await buildLogsExportZip(user, parsed);
      reply
        .header("Content-Type", "application/zip")
        .header("Content-Length", body.length)
        .header(
          "Content-Disposition",
          `attachment; filename="paper-trading-export-${new Date().toISOString().slice(0, 10)}.zip"`
        );
      return reply.send(body);
    } catch (error) {
      reply.code(400);
      return {
        error: true,
        message: error instanceof Error ? error.message : "Log export failed."
      };
    }
  });

  app.post("/api/logs/export", async (request, reply) => {
    try {
      const user = getUserFromRequest(request);
      const parsed = logExportBodySchema.parse(request.body) as ExportQuery;
      const body = await buildLogsExportZip(user, parsed);
      reply
        .header("Content-Type", "application/zip")
        .header("Content-Length", body.length)
        .header(
          "Content-Disposition",
          `attachment; filename="paper-trading-export-${new Date().toISOString().slice(0, 10)}.zip"`
        );
      return reply.send(body);
    } catch (error) {
      reply.code(400);
      return {
        error: true,
        message: error instanceof Error ? error.message : "Log export failed."
      };
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
      return store.getAuditLogs(resolveAuditLogFilters(user, parsed));
    })
  );

  app.get("/api/logs/round-activity", async (request) =>
    safeRoute(async () => {
      const user = getUserFromRequest(request);
      requirePermission(user, "profile:view");
      const parsed = auditLogQuerySchema.pick({ roundId: true }).required().parse(request.query) as { roundId: string };
      return {
        auditLogs: store.getAuditLogs({ userId: user.id, roundId: parsed.roundId }),
        behaviorLogs: store.getBehaviorLogs({ userId: user.id, roundId: parsed.roundId })
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
      const teamUserIds = canViewTeamLogs(user) ? teamVisibleUserIds(user) : [user.id];
      if (!canViewAllLogs(user) && !teamUserIds.includes(timeline.order.userId)) {
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
      if (!user || !user.isActive) {
        socket.close();
        return;
      }

      let lastSentSeq = 0;
      let sending = false;
      let pendingLatest = false;
      let retryTimer: NodeJS.Timeout | undefined;
      let closed = false;

      const isSocketOpen = () => !closed && socket.readyState === WsWebSocket.OPEN;
      const scheduleRetry = () => {
        if (closed || retryTimer) {
          return;
        }
        retryTimer = setTimeout(() => {
          retryTimer = undefined;
          if (pendingLatest) {
            pendingLatest = false;
            sendPayload();
          }
        }, MARKET_WS_RETRY_MS);
      };

      const deferLatest = () => {
        pendingLatest = true;
        scheduleRetry();
      };

      const sendPayload = () => {
        if (!isSocketOpen()) {
          return;
        }
        const currentUser = store.getUserById(user.id);
        if (!currentUser?.isActive) {
          socket.close();
          return;
        }
        if (sending || socket.bufferedAmount > 0) {
          deferLatest();
          return;
        }
        const data = createMarketPayload(user.id);
        const seq = data.transportMeta.payloadSeq;
        if (seq <= lastSentSeq) {
          return;
        }
        sending = true;
        socket.send(JSON.stringify({ type: "market", data }), (error?: Error) => {
          sending = false;
          if (!error) {
            lastSentSeq = Math.max(lastSentSeq, seq);
          }
          if (pendingLatest) {
            scheduleRetry();
          }
        });
      };

      const listener = () => {
        if (sending || socket.bufferedAmount > 0) {
          deferLatest();
          return;
        }
        sendPayload();
      };
      sendPayload();
      store.emitter.on("market:update", listener);
      socket.on("close", () => {
        closed = true;
        if (retryTimer) {
          clearTimeout(retryTimer);
        }
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
      if (!user || !user.isActive) {
        socket.close();
        return;
      }

      const eventName = `user:${user.id}`;
      const sendPayload = () => {
        const currentUser = store.getUserById(user.id);
        if (!currentUser?.isActive) {
          socket.close();
          return;
        }
        socket.send(
          JSON.stringify({
            type: "user",
            data: {
              profile: store.getProfile(user.id),
              operatedHistory: getOperatedHistoryWithSettlementPreview(500, user.id),
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

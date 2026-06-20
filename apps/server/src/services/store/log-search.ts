import type { AuditEvent, BehaviorActionLog, LogSearchQuery, UserRecord } from "../../domain/types";

export function detailString(details: Record<string, unknown> | undefined, key: string) {
  const value = details?.[key];
  return typeof value === "string" || typeof value === "number" ? String(value) : undefined;
}

export function normalizeSearchLimit(limit?: number) {
  return Math.max(1, Math.min(Math.floor(limit ?? 100), 501));
}

export function auditLogGroup(log: AuditEvent) {
  if (log.category === "matching") {
    return "matching_action";
  }
  if (log.category === "settlement") {
    return "settlement";
  }
  if (log.category === "latency") {
    return ["binance", "coinbase", "clob"].includes(log.moduleName) ? "market_latency" : "system_latency";
  }
  return "operation";
}

export function auditLatencySource(log: AuditEvent) {
  if (log.category !== "latency") {
    return undefined;
  }
  return ["binance", "coinbase", "clob"].includes(log.moduleName) ? log.moduleName : "system";
}

export function detailNumber(details: Record<string, unknown> | undefined, key: string) {
  const value = details?.[key];
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && Number.isFinite(Number(value))) {
    return Number(value);
  }
  return undefined;
}

export function auditLatencyValue(log: AuditEvent, phase?: LogSearchQuery["latencyPhase"]) {
  if (phase === "acquire") {
    return detailNumber(log.details, "acquireLatencyMs");
  }
  if (phase === "publish") {
    return detailNumber(log.details, "publishLatencyMs");
  }
  if (phase === "frontend") {
    return log.frontendLatencyMs ?? detailNumber(log.details, "frontendLatencyMs");
  }
  return log.backendLatencyMs;
}

export function matchesAuditSearch(log: AuditEvent, filters?: LogSearchQuery) {
  if (typeof filters?.from === "number" && log.serverRecvTs < filters.from) {
    return false;
  }
  if (typeof filters?.to === "number" && log.serverRecvTs > filters.to) {
    return false;
  }
  if (filters?.userId && log.userId !== filters.userId) {
    return false;
  }
  if (filters?.userIds && !filters.userIds.includes(log.userId ?? "")) {
    return false;
  }
  if (filters?.role && log.role !== filters.role) {
    return false;
  }
  if (filters?.category && log.category !== filters.category) {
    return false;
  }
  if (filters?.actionType && log.actionType !== filters.actionType) {
    return false;
  }
  if (filters?.actionStatus && log.actionStatus !== filters.actionStatus) {
    return false;
  }
  if (filters?.moduleName && log.moduleName !== filters.moduleName) {
    return false;
  }
  if (filters?.pageName && log.pageName !== filters.pageName) {
    return false;
  }
  if (filters?.symbol && log.symbol !== filters.symbol) {
    return false;
  }
  if (filters?.roundId && log.roundId !== filters.roundId) {
    return false;
  }
  if (filters?.traceId && log.traceId !== filters.traceId) {
    return false;
  }
  if (filters?.resultCode && log.resultCode !== filters.resultCode) {
    return false;
  }
  if (filters?.orderId && detailString(log.details, "orderId") !== filters.orderId) {
    return false;
  }
  if (filters?.positionId && detailString(log.details, "positionId") !== filters.positionId) {
    return false;
  }
  if (filters?.marketId && detailString(log.details, "marketId") !== filters.marketId) {
    return false;
  }
  if (filters?.marketSlug && detailString(log.details, "marketSlug") !== filters.marketSlug) {
    return false;
  }
  const direction = detailString(log.details, "direction") ?? detailString(log.details, "side");
  if (filters?.direction && direction !== filters.direction) {
    return false;
  }
  if (filters?.roundStatus && detailString(log.details, "roundStatus") !== filters.roundStatus) {
    return false;
  }
  if (
    filters?.settlementResult &&
    detailString(log.details, "settlementResult") !== filters.settlementResult
  ) {
    return false;
  }
  if (filters?.matchingLogKind === "engine") {
    return false;
  }
  if (filters?.bookKey || filters?.bookSide || filters?.eventType || filters?.sequenceFrom || filters?.sequenceTo) {
    return false;
  }
  if (filters?.logGroup && auditLogGroup(log) !== filters.logGroup) {
    return false;
  }
  if (filters?.matchingLogKind === "action" && log.category !== "matching") {
    return false;
  }
  if (filters?.latencySource && auditLatencySource(log) !== filters.latencySource) {
    return false;
  }
  if (filters?.connectionState && detailString(log.details, "connectionState") !== filters.connectionState) {
    return false;
  }
  if (typeof filters?.latencyMinMs === "number" || typeof filters?.latencyMaxMs === "number") {
    const latency = auditLatencyValue(log, filters.latencyPhase);
    if (typeof latency !== "number") {
      return false;
    }
    if (typeof filters.latencyMinMs === "number" && latency < filters.latencyMinMs) {
      return false;
    }
    if (typeof filters.latencyMaxMs === "number" && latency > filters.latencyMaxMs) {
      return false;
    }
  }
  return true;
}

export function resolveBehaviorSearchUserIds(filters: LogSearchQuery | undefined, users: Iterable<UserRecord>) {
  if (filters?.userId) {
    const user = [...users].find((item) => item.id === filters.userId);
    if (filters.role && user?.role !== filters.role) {
      return [] as string[];
    }
    return [filters.userId];
  }
  const baseIds = filters?.userIds;
  if (!filters?.role) {
    return baseIds;
  }
  const roleIds = new Set([...users].filter((user) => user.role === filters.role).map((user) => user.id));
  return baseIds ? baseIds.filter((userId) => roleIds.has(userId)) : [...roleIds];
}

export function hasUnsupportedBehaviorSearchFilter(filters?: LogSearchQuery) {
  return Boolean(
    filters?.category ||
      filters?.moduleName ||
      filters?.pageName ||
      filters?.symbol ||
      filters?.positionId ||
      filters?.resultCode ||
      filters?.bookKey ||
      filters?.bookSide ||
      filters?.eventType ||
      filters?.logGroup ||
      filters?.latencySource ||
      filters?.connectionState ||
      filters?.latencyPhase ||
      typeof filters?.latencyMinMs === "number" ||
      typeof filters?.latencyMaxMs === "number" ||
      filters?.matchingLogKind ||
      typeof filters?.sequenceFrom === "number" ||
      typeof filters?.sequenceTo === "number"
  );
}

export function matchesBehaviorSearch(
  log: BehaviorActionLog,
  filters: LogSearchQuery | undefined,
  userIds: string[] | undefined,
  anonymizeUserId: (userId: string) => string
) {
  if (typeof filters?.from === "number" && log.timestampMs < filters.from) {
    return false;
  }
  if (typeof filters?.to === "number" && log.timestampMs > filters.to) {
    return false;
  }
  if (userIds?.length) {
    const allowedAnonIds = new Set(userIds.map((userId) => anonymizeUserId(userId)));
    if (!allowedAnonIds.has(log.testerIdAnon)) {
      return false;
    }
  }
  if (filters?.roundId && log.roundId !== filters.roundId) {
    return false;
  }
  if (filters?.actionType && log.actionType !== filters.actionType) {
    return false;
  }
  if (filters?.actionStatus && log.actionStatus !== filters.actionStatus) {
    return false;
  }
  if (filters?.traceId && log.traceId !== filters.traceId) {
    return false;
  }
  if (filters?.orderId && log.orderId !== filters.orderId) {
    return false;
  }
  if (filters?.marketId && log.marketId !== filters.marketId) {
    return false;
  }
  if (filters?.marketSlug && log.marketSlug !== filters.marketSlug) {
    return false;
  }
  if (filters?.direction && log.direction !== filters.direction) {
    return false;
  }
  if (filters?.roundStatus && log.roundStatus !== filters.roundStatus) {
    return false;
  }
  if (filters?.settlementResult && log.settlementResult !== filters.settlementResult) {
    return false;
  }
  return true;
}

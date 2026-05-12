import { useEffect, useMemo, useState } from "react";
import { FieldChip } from "../../components/FieldChip";
import {
  api,
  type AuditEvent,
  type HistoryRound,
  type Language,
  type LogFacets,
  type LogSearchQuery,
  type LogSystem,
  type PublicUser,
  type Role,
  type TradeSide,
  type UnifiedLogRow
} from "../../utils/api";
import { dateTimeText, localLabel } from "../../utils/format";
import { redactNetworkAddresses } from "../../utils/redaction";

const jsonPreview = (value: unknown) => redactNetworkAddresses(JSON.stringify(value ?? {}, null, 2));
const exportFileName = () => `paper-trading-export-${new Date().toISOString().slice(0, 10)}.zip`;
const LOG_EXPORT_SYSTEMS: Array<Exclude<LogSystem, "all">> = ["audit", "training", "matching"];
const ROLE_OPTIONS: Role[] = ["Tester", "Senior Tester", "Test Engineer", "Admin"];
const ACTION_STATUS_OPTIONS: Array<NonNullable<LogSearchQuery["actionStatus"]>> = ["success", "failed", "timeout"];
const LOG_GROUP_OPTIONS: Array<NonNullable<LogSearchQuery["logGroup"]>> = [
  "operation",
  "settlement",
  "market_latency",
  "system_latency",
  "matching_action"
];
const LATENCY_SOURCE_OPTIONS: Array<NonNullable<LogSearchQuery["latencySource"]>> = ["binance", "chainlink", "clob", "system"];
const CONNECTION_STATE_OPTIONS: Array<NonNullable<LogSearchQuery["connectionState"]>> = [
  "healthy",
  "reconnecting",
  "stale",
  "degraded",
  "disabled"
];
const LATENCY_PHASE_OPTIONS: Array<NonNullable<LogSearchQuery["latencyPhase"]>> = ["backend", "acquire", "publish", "frontend"];
const MATCHING_KIND_OPTIONS: Array<NonNullable<LogSearchQuery["matchingLogKind"]>> = ["action", "engine"];
const MATCHING_EVENT_OPTIONS: Array<NonNullable<LogSearchQuery["eventType"]>> = [
  "external_book_synced",
  "order_executed",
  "order_cancelled"
];
const DEFAULT_LOG_FACETS: LogFacets = {
  audit: {
    categories: ["operation", "matching", "settlement", "latency"],
    actionTypes: [
      "login",
      "switch_language",
      "place_order",
      "cancel_order",
      "sell_position",
      "close_side",
      "reverse_side",
      "limit_order_triggered",
      "limit_order_failed",
      "capture_price_to_beat",
      "poll_settlement",
      "settlement_confirmed",
      "redeem_position",
      "round_closed",
      "market_latency",
      "user.create",
      "user.bulkCreate",
      "user.disable",
      "user.enable",
      "user.resetPassword",
      "user.balance.set",
      "user.changePassword"
    ],
    fields: [
      "eventId",
      "traceId",
      "category",
      "actionType",
      "actionStatus",
      "userId",
      "role",
      "pageName",
      "moduleName",
      "roundId",
      "resultCode",
      "details"
    ],
    logGroups: LOG_GROUP_OPTIONS,
    latencySources: LATENCY_SOURCE_OPTIONS,
    connectionStates: CONNECTION_STATE_OPTIONS,
    latencyPhases: LATENCY_PHASE_OPTIONS
  },
  training: {
    actionTypes: [
      "place_order",
      "cancel_order",
      "sell_position",
      "close_side",
      "reverse_side",
      "limit_order_triggered",
      "limit_order_failed",
      "redeem_position"
    ],
    fields: [
      "logId",
      "timestampMs",
      "actionType",
      "actionStatus",
      "testerIdAnon",
      "roundId",
      "direction",
      "orderId",
      "marketId",
      "bookSnapshotEntry",
      "sourceStates",
      "contextJson"
    ]
  },
  matching: {
    eventTypes: MATCHING_EVENT_OPTIONS,
    fields: ["eventId", "bookKey", "roundId", "marketId", "bookSide", "sequence", "eventType", "orderId", "traceId", "payload"],
    kinds: MATCHING_KIND_OPTIONS
  }
};

async function saveBlobWithDesktopFallback(blob: Blob, defaultFileName: string) {
  const desktopSave = window.paperTradingDesktop?.saveFile;
  if (desktopSave) {
    const result = await desktopSave({
      defaultFileName,
      bytes: await blob.arrayBuffer()
    });
    return result;
  }
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = defaultFileName;
  link.click();
  URL.revokeObjectURL(url);
  return {
    canceled: false,
    filePath: undefined,
    browserDownload: true
  };
}

function dateTimeLocalValue(value?: number) {
  if (!value) {
    return "";
  }
  const date = new Date(value);
  const localMs = value - date.getTimezoneOffset() * 60_000;
  return new Date(localMs).toISOString().slice(0, 16);
}

function numberOrUndefined(value: string) {
  if (!value.trim()) {
    return undefined;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function auditStatusTone(status: AuditEvent["actionStatus"]) {
  if (status === "success") {
    return "positive";
  }
  if (status === "timeout") {
    return "warning";
  }
  return "negative";
}

const AUDIT_ACTION_LABELS: Record<string, string> = {
  login: "登录",
  switch_language: "切换语言",
  place_order: "提交订单",
  cancel_order: "撤销订单",
  sell_position: "卖出持仓",
  close_side: "平仓方向",
  reverse_side: "一键反手",
  limit_order_triggered: "限价单触发",
  limit_order_failed: "限价单失败",
  capture_price_to_beat: "记录 PTB",
  poll_settlement: "轮询结算",
  settlement_confirmed: "确认结算",
  manual_settlement: "手动结算",
  redeem_position: "持仓兑付",
  round_closed: "轮次关闭",
  market_latency: "行情延迟",
  user_create: "创建用户",
  user_disable: "停用用户",
  user_enable: "启用用户",
  user_reset_password: "重置密码",
  user_changePassword: "修改密码",
  "user.changePassword": "修改密码",
  "user.resetPassword": "重置密码",
  "user.balance.set": "设置余额"
};

const AUDIT_CATEGORY_LABELS: Record<string, string> = {
  operation: "操作",
  matching: "撮合",
  settlement: "结算",
  latency: "延迟"
};

function auditActionLabel(actionType: string | undefined, _language: Language) {
  if (!actionType) return "--";
  const label = AUDIT_ACTION_LABELS[actionType];
  return label ? label : actionType;
}

function auditCategoryLabel(category: string | undefined, _language: Language) {
  if (!category) return "--";
  const label = AUDIT_CATEGORY_LABELS[category];
  return label ? label : category;
}

export function LogSearchPage(props: { t: (key: string, options?: Record<string, unknown>) => string; token: string; me: PublicUser; canExport: boolean }) {
  const { t, token, me } = props;
  const [filters, setFilters] = useState<Record<string, string>>({ system: "all", limit: "100" });
  const [logs, setLogs] = useState<UnifiedLogRow[]>([]);
  const [nextCursor, setNextCursor] = useState<string>();
  const [users, setUsers] = useState<PublicUser[]>([]);
  const [roundOptions, setRoundOptions] = useState<HistoryRound[]>([]);
  const [facets, setFacets] = useState<LogFacets>(DEFAULT_LOG_FACETS);
  const [busy, setBusy] = useState(false);
  const [exportBusy, setExportBusy] = useState(false);
  const [exportDialogOpen, setExportDialogOpen] = useState(false);
  const [showMoreFilters, setShowMoreFilters] = useState(false);
  const [showLogInfo, setShowLogInfo] = useState(false);
  const [expandedId, setExpandedId] = useState<string>();
  const [error, setError] = useState<string>();
  const language = me.language;
  const canFilterUsers =
    me.role === "Admin" ||
    me.role === "Test Engineer" ||
    me.role === "Senior Tester" ||
    me.permissionCodes.includes("logs:view:all") ||
    me.permissionCodes.includes("logs:view:team");

  const numberFilter = (key: string) => {
    const value = filters[key];
    if (!value) {
      return undefined;
    }
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  };

  const selectedSystem = (filters.system as LogSystem) || "all";
  const visibleUsers = users.filter((user) => !filters.role || user.role === filters.role);
  const selectedActionOptions =
    selectedSystem === "matching"
      ? facets.matching.eventTypes
      : selectedSystem === "training"
        ? facets.training.actionTypes
        : selectedSystem === "audit"
          ? facets.audit.actionTypes
          : [...new Set([...facets.audit.actionTypes, ...facets.training.actionTypes, ...facets.matching.eventTypes])];

  const numberFilterFrom = (source: Record<string, string>, key: string) => {
    const value = source[key];
    if (!value) {
      return undefined;
    }
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  };

  const sanitizeFiltersForSystem = (nextFilters: Record<string, string>, system: LogSystem) => {
    const cleaned: Record<string, string> = { ...nextFilters, system };
    if (system === "training") {
      for (const key of ["category", "moduleName", "pageName", "symbol", "positionId", "resultCode", "bookKey", "bookSide", "eventType", "sequenceFrom", "sequenceTo", "logGroup", "latencySource", "connectionState", "latencyPhase", "latencyMinMs", "latencyMaxMs", "matchingLogKind"]) {
        delete cleaned[key];
      }
    }
    if (system === "audit") {
      for (const key of ["bookKey", "bookSide", "eventType", "sequenceFrom", "sequenceTo", "matchingLogKind"]) {
        delete cleaned[key];
      }
    }
    if (system === "matching") {
      for (const key of ["category", "moduleName", "pageName", "symbol", "positionId", "resultCode", "roundStatus", "settlementResult", "latencySource", "connectionState", "latencyPhase", "latencyMinMs", "latencyMaxMs"]) {
        delete cleaned[key];
      }
    }
    return cleaned;
  };

  const toQueryFromFilters = (source: Record<string, string>, cursor?: string): LogSearchQuery => ({
    system: (source.system as LogSystem) || "all",
    from: source.from ? Date.parse(source.from) : undefined,
    to: source.to ? Date.parse(source.to) : undefined,
    userId: source.userId || undefined,
    role: (source.role as LogSearchQuery["role"]) || undefined,
    category: (source.category as LogSearchQuery["category"]) || undefined,
    actionType: source.actionType || undefined,
    actionStatus: (source.actionStatus as LogSearchQuery["actionStatus"]) || undefined,
    moduleName: source.moduleName || undefined,
    pageName: source.pageName || undefined,
    symbol: source.symbol || undefined,
    roundId: source.roundId || undefined,
    marketId: source.marketId || undefined,
    marketSlug: source.marketSlug || undefined,
    orderId: source.orderId || undefined,
    positionId: source.positionId || undefined,
    traceId: source.traceId || undefined,
    resultCode: source.resultCode || undefined,
    direction: (source.direction as TradeSide) || undefined,
    roundStatus: (source.roundStatus as LogSearchQuery["roundStatus"]) || undefined,
    settlementResult: (source.settlementResult as LogSearchQuery["settlementResult"]) || undefined,
    bookKey: source.bookKey || undefined,
    bookSide: (source.bookSide as TradeSide) || undefined,
    eventType: (source.eventType as LogSearchQuery["eventType"]) || undefined,
    sequenceFrom: numberFilterFrom(source, "sequenceFrom"),
    sequenceTo: numberFilterFrom(source, "sequenceTo"),
    logGroup: (source.logGroup as LogSearchQuery["logGroup"]) || undefined,
    latencySource: (source.latencySource as LogSearchQuery["latencySource"]) || undefined,
    connectionState: (source.connectionState as LogSearchQuery["connectionState"]) || undefined,
    latencyPhase: (source.latencyPhase as LogSearchQuery["latencyPhase"]) || undefined,
    latencyMinMs: numberFilterFrom(source, "latencyMinMs"),
    latencyMaxMs: numberFilterFrom(source, "latencyMaxMs"),
    matchingLogKind: (source.matchingLogKind as LogSearchQuery["matchingLogKind"]) || undefined,
    limit: numberFilterFrom(source, "limit") ?? 100,
    cursor
  });

  const toQuery = (cursor?: string): LogSearchQuery => toQueryFromFilters(filters, cursor);

  const search = async (mode: "replace" | "append" = "replace", nextFilters = filters) => {
    try {
      setBusy(true);
      setError(undefined);
      const result = await api.searchLogs(token, toQueryFromFilters(nextFilters, mode === "append" ? nextCursor : undefined));
      setLogs((current) => (mode === "append" ? [...current, ...result.rows] : result.rows));
      setNextCursor(result.nextCursor);
      if (mode === "replace") {
        setExpandedId(undefined);
      }
    } catch (searchError) {
      setError(searchError instanceof Error ? searchError.message : "Search failed.");
    } finally {
      setBusy(false);
    }
  };

  useEffect(() => {
    void search();
    if (canFilterUsers) {
      api.getUsers(token).then(setUsers).catch(() => setUsers([]));
    }
    api.getHistory(token, 200).then(setRoundOptions).catch(() => setRoundOptions([]));
    api.getLogFacets(token).then(setFacets).catch(() => setFacets(DEFAULT_LOG_FACETS));
  }, []);

  useEffect(() => {
    if (!filters.userId || !filters.role || users.length === 0) {
      return;
    }
    const selectedUser = users.find((user) => user.id === filters.userId);
    if (selectedUser && selectedUser.role !== filters.role) {
      setFilters((current) => ({ ...current, userId: "" }));
    }
  }, [filters.role, filters.userId, users]);

  const updateFilters = (patch: Record<string, string | undefined>) => {
    setFilters((current) => {
      const next = { ...current };
      for (const [key, value] of Object.entries(patch)) {
        if (value) {
          next[key] = value;
        } else {
          delete next[key];
        }
      }
      if (next.userId && next.role) {
        const selectedUser = users.find((user) => user.id === next.userId);
        if (selectedUser && selectedUser.role !== next.role) {
          delete next.userId;
        }
      }
      return next;
    });
  };

  const handleSystemChange = (system: LogSystem) => {
    const nextFilters = sanitizeFiltersForSystem(filters, system);
    setFilters(nextFilters);
    setLogs([]);
    setNextCursor(undefined);
    setExpandedId(undefined);
    void search("replace", nextFilters);
  };

  const applyFilterAndSearch = (patch: Record<string, string | undefined>) => {
    const patchedFilters = { ...filters };
    for (const [key, value] of Object.entries(patch)) {
      if (value) {
        patchedFilters[key] = value;
      } else {
        delete patchedFilters[key];
      }
    }
    const nextFilters = sanitizeFiltersForSystem(patchedFilters, selectedSystem);
    for (const [key, value] of Object.entries(patch)) {
      if (!value) {
        delete nextFilters[key];
      }
    }
    setFilters(nextFilters);
    setLogs([]);
    setNextCursor(undefined);
    setExpandedId(undefined);
    void search("replace", nextFilters);
  };

  const disabledUserIds = new Set(users.filter((user) => !user.isActive).map((user) => user.id));
  const logSystemLabel = (system: UnifiedLogRow["system"]) =>
    system === "audit"
      ? t("audit")
      : system === "training"
        ? t("trading")
        : t("matching");
  const logGroupLabel = (value?: string) => {
    if (value === "operation") return t("operationAudit");
    if (value === "settlement") return t("settlementAudit");
    if (value === "market_latency") return t("marketDataLatency");
    if (value === "system_latency") return t("systemLinkLatency");
    if (value === "matching_action") return t("matchingActions");
    return value ?? "--";
  };
  const latencySourceLabel = (value?: string) => {
    if (value === "binance") return "Binance";
    if (value === "chainlink") return "Chainlink";
    if (value === "clob") return t("polymarketBookClob");
    if (value === "system") return t("system");
    return value ?? "--";
  };
  const matchingKindLabel = (value?: string) =>
    value === "action"
      ? t("matchingActions")
      : value === "engine"
        ? t("matchingEngineEvents")
        : value ?? "--";
  const latencySummary = (log: UnifiedLogRow) => {
    if (!log.latencyPhaseMetrics) {
      return redactNetworkAddresses(log.resultMessage ?? log.resultCode ?? "--");
    }
    const metrics = log.latencyPhaseMetrics;
    const parts = [
      `${latencySourceLabel(log.latencySource)} / ${log.connectionState ?? "--"}`,
      `backend ${metrics.backend ?? "--"}ms`,
      `acquire ${metrics.acquire ?? "--"}ms`,
      `publish ${metrics.publish ?? "--"}ms`
    ];
    const reconnectCount = typeof log.payload?.reconnectCount === "number" ? `reconnect ${log.payload.reconnectCount}` : undefined;
    return reconnectCount ? `${parts.join(" / ")} / ${reconnectCount}` : parts.join(" / ");
  };
  const fieldSummary =
    selectedSystem === "training"
      ? facets.training.fields
      : selectedSystem === "matching"
        ? facets.matching.fields
        : selectedSystem === "audit"
          ? facets.audit.fields
          : [...new Set([...facets.audit.fields, ...facets.training.fields, ...facets.matching.fields])];
  const typeSummary =
    selectedSystem === "training"
      ? facets.training.actionTypes
      : selectedSystem === "matching"
        ? facets.matching.eventTypes
        : selectedSystem === "audit"
          ? facets.audit.actionTypes
          : [...new Set([...facets.audit.actionTypes, ...facets.training.actionTypes, ...facets.matching.eventTypes])];
  const logInfoText =
    selectedSystem === "training"
      ? localLabel(
          language,
          "交易日志保存交易行为样本：匿名用户、轮次、方向、订单、成交、滑点、盘口快照、价格源状态和上下文 JSON。",
          "Trading logs store trading behavior samples: anonymized user, round, direction, order, fills, slippage, book snapshots, source states, and context JSON."
        )
      : selectedSystem === "matching"
        ? localLabel(
            language,
            "撮合日志保存盘口同步、订单成交和撤单事件，按 bookKey/sequence/round/market 追踪撮合过程。",
            "Matching logs store book sync, execution, and cancellation events, tracked by bookKey, sequence, round, and market."
          )
        : localLabel(
            language,
            "审计日志保存用户操作、撮合结果、结算流程和系统延迟事件；顶层分类是 operation、matching、settlement、latency。",
            "Audit logs store user operations, matching outcomes, settlement flow, and latency events; categories are operation, matching, settlement, and latency."
          );

  return (
    <>
    <section className="panel log-search-panel">
      <div className="section-header">
        <div>
          <p className="eyebrow">{t("auditSearch")}</p>
          <h2>
            {selectedSystem === "all" ? t("all") : logSystemLabel(selectedSystem as UnifiedLogRow["system"])} · {logs.length}
          </h2>
        </div>
        <div className="button-row fit-actions">
          <button className="secondary-button" onClick={() => search()} disabled={busy}>
            {busy ? t("loading") : t("search")}
          </button>
          {props.canExport ? (
            <button className="ghost-button" onClick={() => setExportDialogOpen(true)} disabled={exportBusy}>
              {exportBusy ? t("loading") : t("export")}
            </button>
          ) : null}
        </div>
      </div>
      {error ? <div className="inline-error-banner">{redactNetworkAddresses(error)}</div> : null}
      <div className="log-system-tabs">
        {(["all", "audit", "training", "matching"] as LogSystem[]).map((system) => (
          <button
            key={system}
            className={filters.system === system ? "active" : ""}
            onClick={() => handleSystemChange(system)}
          >
            {system === "all" ? t("all") : logSystemLabel(system)}
          </button>
        ))}
      </div>
      {selectedSystem === "audit" ? (
        <div className="log-system-tabs log-sub-tabs">
          <button className={!filters.logGroup ? "active" : ""} onClick={() => applyFilterAndSearch({ logGroup: undefined })}>
            {t("all")}
          </button>
          {LOG_GROUP_OPTIONS.filter((group) => group !== "matching_action").map((group) => (
            <button key={group} className={filters.logGroup === group ? "active" : ""} onClick={() => applyFilterAndSearch({ logGroup: group })}>
              {logGroupLabel(group)}
            </button>
          ))}
        </div>
      ) : null}
      {selectedSystem === "matching" ? (
        <div className="log-system-tabs log-sub-tabs">
          <button className={!filters.matchingLogKind ? "active" : ""} onClick={() => applyFilterAndSearch({ matchingLogKind: undefined, logGroup: undefined })}>
            {t("all")}
          </button>
          {MATCHING_KIND_OPTIONS.map((kind) => (
            <button
              key={kind}
              className={filters.matchingLogKind === kind ? "active" : ""}
              onClick={() => applyFilterAndSearch({ matchingLogKind: kind, logGroup: kind === "action" ? "matching_action" : undefined })}
            >
              {matchingKindLabel(kind)}
            </button>
          ))}
        </div>
      ) : null}
      <div className="filter-grid">
        <label>
          {t("from")}
          <input type="datetime-local" value={filters.from ?? ""} onChange={(event) => updateFilters({ from: event.target.value })} />
        </label>
        <label>
          {t("to")}
          <input type="datetime-local" value={filters.to ?? ""} onChange={(event) => updateFilters({ to: event.target.value })} />
        </label>
        <label>
          {t("role")}
          <select value={filters.role ?? ""} onChange={(event) => updateFilters({ role: event.target.value })}>
            <option value="">{t("all")}</option>
            {ROLE_OPTIONS.map((role) => (
              <option key={role} value={role}>
                {role}
              </option>
            ))}
          </select>
        </label>
        {canFilterUsers ? (
          <label>
            {t("user")}
            <select value={filters.userId ?? ""} onChange={(event) => updateFilters({ userId: event.target.value })}>
              <option value="">{t("all")}</option>
              {visibleUsers.map((user) => (
                <option key={user.id} value={user.id}>
                  {user.username} / {user.role}
                </option>
              ))}
            </select>
          </label>
        ) : null}
        <label>
          {t("round")}
          <select value={filters.roundId ?? ""} onChange={(event) => updateFilters({ roundId: event.target.value })}>
            <option value="">{t("all")}</option>
            {roundOptions.map((round) => (
              <option key={round.id} value={round.id}>
                {(round.marketSlug ?? round.id).slice(0, 42)}
              </option>
            ))}
          </select>
        </label>
        <label>
          <span title={t("orderIdUniqueIdForAUserSystemOrder")}>
            {t("orderId")}
          </span>
          <input value={filters.orderId ?? ""} onChange={(event) => updateFilters({ orderId: event.target.value })} />
        </label>
        <label>
          <span title={t("traceIdInternalRequestProcessTraceForDebugging")}>
            {t("traceId")}
          </span>
          <input value={filters.traceId ?? ""} onChange={(event) => updateFilters({ traceId: event.target.value })} />
        </label>
        <label>
          {t("actionType")}
          <select value={filters.actionType ?? ""} onChange={(event) => updateFilters({ actionType: event.target.value })}>
            <option value="">{t("all")}</option>
            {selectedActionOptions.map((actionType) => (
              <option key={actionType} value={actionType}>
                {auditActionLabel(actionType, language)}
              </option>
            ))}
          </select>
        </label>
        <label>
          {t("status")}
          <select value={filters.actionStatus ?? ""} onChange={(event) => updateFilters({ actionStatus: event.target.value })}>
            <option value="">{t("all")}</option>
            <option value="success">success</option>
            <option value="failed">failed</option>
            <option value="timeout">timeout</option>
          </select>
        </label>
      </div>
      {showMoreFilters ? (
        <div className="filter-grid filter-grid-secondary">
          <label>
            category
            <select value={filters.category ?? ""} onChange={(event) => updateFilters({ category: event.target.value })} disabled={selectedSystem === "training" || selectedSystem === "matching"}>
            <option value="">{t("all")}</option>
            {facets.audit.categories.map((category) => (
              <option key={category} value={category}>
                  {auditCategoryLabel(category, language)}
              </option>
            ))}
          </select>
          </label>
          <label>
            {t("logGroup")}
            <select value={filters.logGroup ?? ""} onChange={(event) => updateFilters({ logGroup: event.target.value })} disabled={selectedSystem === "training"}>
              <option value="">{t("all")}</option>
              {(facets.audit.logGroups ?? LOG_GROUP_OPTIONS).map((group) => (
                <option key={group} value={group}>
                  {logGroupLabel(group)}
                </option>
              ))}
            </select>
          </label>
          <label>
            {t("latencySource")}
            <select value={filters.latencySource ?? ""} onChange={(event) => updateFilters({ latencySource: event.target.value })} disabled={selectedSystem === "training" || selectedSystem === "matching"}>
              <option value="">{t("all")}</option>
              {(facets.audit.latencySources ?? LATENCY_SOURCE_OPTIONS).map((source) => (
                <option key={source} value={source}>
                  {latencySourceLabel(source)}
                </option>
              ))}
            </select>
          </label>
          <label>
            {t("connectionState")}
            <select value={filters.connectionState ?? ""} onChange={(event) => updateFilters({ connectionState: event.target.value })} disabled={selectedSystem === "training" || selectedSystem === "matching"}>
              <option value="">{t("all")}</option>
              {(facets.audit.connectionStates ?? CONNECTION_STATE_OPTIONS).map((state) => (
                <option key={state} value={state}>
                  {state}
                </option>
              ))}
            </select>
          </label>
          <label>
            {t("latencyPhase")}
            <select value={filters.latencyPhase ?? ""} onChange={(event) => updateFilters({ latencyPhase: event.target.value })} disabled={selectedSystem === "training" || selectedSystem === "matching"}>
              <option value="">{t("all")}</option>
              {(facets.audit.latencyPhases ?? LATENCY_PHASE_OPTIONS).map((phase) => (
                <option key={phase} value={phase}>
                  {phase}
                </option>
              ))}
            </select>
          </label>
          <label>
            {t("minLatencyMs")}
            <input type="number" value={filters.latencyMinMs ?? ""} onChange={(event) => updateFilters({ latencyMinMs: event.target.value })} disabled={selectedSystem === "training" || selectedSystem === "matching"} />
          </label>
          <label>
            {t("maxLatencyMs")}
            <input type="number" value={filters.latencyMaxMs ?? ""} onChange={(event) => updateFilters({ latencyMaxMs: event.target.value })} disabled={selectedSystem === "training" || selectedSystem === "matching"} />
          </label>
          <label>
            {t("matchingKind")}
            <select value={filters.matchingLogKind ?? ""} onChange={(event) => updateFilters({ matchingLogKind: event.target.value })} disabled={selectedSystem === "audit" || selectedSystem === "training"}>
              <option value="">{t("all")}</option>
              {(facets.matching.kinds ?? MATCHING_KIND_OPTIONS).map((kind) => (
                <option key={kind} value={kind}>
                  {matchingKindLabel(kind)}
                </option>
              ))}
            </select>
          </label>
          <label>
            marketId
            <input value={filters.marketId ?? ""} onChange={(event) => updateFilters({ marketId: event.target.value })} />
          </label>
          <label>
            marketSlug
            <input value={filters.marketSlug ?? ""} onChange={(event) => updateFilters({ marketSlug: event.target.value })} />
          </label>
          <label>
            positionId
            <input value={filters.positionId ?? ""} onChange={(event) => updateFilters({ positionId: event.target.value })} disabled={selectedSystem === "training" || selectedSystem === "matching"} />
          </label>
          <label>
            resultCode
            <input value={filters.resultCode ?? ""} onChange={(event) => updateFilters({ resultCode: event.target.value })} disabled={selectedSystem === "training" || selectedSystem === "matching"} />
          </label>
          <label>
            direction
            <select value={filters.direction ?? ""} onChange={(event) => updateFilters({ direction: event.target.value })}>
              <option value="">{t("all")}</option>
              <option value="UP">UP</option>
              <option value="DOWN">DOWN</option>
            </select>
          </label>
          <label>
            roundStatus
            <input value={filters.roundStatus ?? ""} onChange={(event) => updateFilters({ roundStatus: event.target.value })} disabled={selectedSystem === "matching"} />
          </label>
          <label>
            settlementResult
            <select value={filters.settlementResult ?? ""} onChange={(event) => updateFilters({ settlementResult: event.target.value })} disabled={selectedSystem === "matching"}>
              <option value="">{t("all")}</option>
              <option value="win">win</option>
              <option value="loss">loss</option>
              <option value="sold">sold</option>
            </select>
          </label>
          <label>
            moduleName
            <input value={filters.moduleName ?? ""} onChange={(event) => updateFilters({ moduleName: event.target.value })} disabled={selectedSystem === "training" || selectedSystem === "matching"} />
          </label>
          <label>
            pageName
            <input value={filters.pageName ?? ""} onChange={(event) => updateFilters({ pageName: event.target.value })} disabled={selectedSystem === "training" || selectedSystem === "matching"} />
          </label>
          <label>
            {t("exactActiontype")}
            <input value={filters.actionType ?? ""} onChange={(event) => updateFilters({ actionType: event.target.value })} />
          </label>
          <label>
            bookKey
            <input value={filters.bookKey ?? ""} onChange={(event) => updateFilters({ bookKey: event.target.value })} disabled={selectedSystem === "audit" || selectedSystem === "training"} />
          </label>
          <label>
            bookSide
            <select value={filters.bookSide ?? ""} onChange={(event) => updateFilters({ bookSide: event.target.value })} disabled={selectedSystem === "audit" || selectedSystem === "training"}>
              <option value="">{t("all")}</option>
              <option value="UP">UP</option>
              <option value="DOWN">DOWN</option>
            </select>
          </label>
          <label>
            eventType
            <select value={filters.eventType ?? ""} onChange={(event) => updateFilters({ eventType: event.target.value })} disabled={selectedSystem === "audit" || selectedSystem === "training"}>
              <option value="">{t("all")}</option>
              {facets.matching.eventTypes.map((eventType) => (
                <option key={eventType} value={eventType}>
                  {eventType}
                </option>
              ))}
            </select>
          </label>
          <label>
            sequenceFrom
            <input type="number" value={filters.sequenceFrom ?? ""} onChange={(event) => updateFilters({ sequenceFrom: event.target.value })} disabled={selectedSystem === "audit" || selectedSystem === "training"} />
          </label>
          <label>
            sequenceTo
            <input type="number" value={filters.sequenceTo ?? ""} onChange={(event) => updateFilters({ sequenceTo: event.target.value })} disabled={selectedSystem === "audit" || selectedSystem === "training"} />
          </label>
          <label>
            limit
            <input type="number" min={1} max={500} value={filters.limit ?? "100"} onChange={(event) => updateFilters({ limit: event.target.value })} />
          </label>
        </div>
      ) : null}
      <div className="more-filter-actions">
        <button className="ghost-button compact-button" onClick={() => setShowLogInfo((value) => !value)}>
          {showLogInfo ? t("hideLogInfo") : t("logInfoFields")}
        </button>
        <button className="ghost-button compact-button" onClick={() => setShowMoreFilters((value) => !value)}>
          {showMoreFilters ? t("fewerFilters") : t("moreFilters")}
        </button>
      </div>
      {showLogInfo ? (
        <div className="log-info-panel">
          <div>
            <strong>{selectedSystem === "all" ? t("allLogs") : logSystemLabel(selectedSystem as UnifiedLogRow["system"])}</strong>
            <p>{logInfoText}</p>
          </div>
          <div className="log-info-grid">
            <div>
              <span>{selectedSystem === "matching" ? "eventType" : "actionType"}</span>
              <div className="field-chip-row">
                {typeSummary.map((item) => (
                  <FieldChip key={item} label={item} tone="info" />
                ))}
              </div>
            </div>
            <div>
              <span>{localLabel(language, "主字段 / Main Fields", "Main Fields")}</span>
              <div className="field-chip-row">
                {fieldSummary.map((field) => (
                  <FieldChip key={field} label={field} tone="neutral" />
                ))}
              </div>
            </div>
          </div>
        </div>
      ) : null}
      <table>
        <thead>
          <tr>
            <th>{t("time")}</th>
            <th>{t("system")}</th>
            <th>{t("userRole")}</th>
            <th>{t("round")}</th>
            <th>{t("actionType")}</th>
            <th>{t("status")}</th>
            <th>
              <span title={t("traceIdOrderId")}>
                {t("traceOrder")}
              </span>
            </th>
            <th>{t("message")}</th>
          </tr>
        </thead>
        <tbody>
          {logs.length === 0 ? (
            <tr>
              <td colSpan={8}>{t("noData")}</td>
            </tr>
          ) : (
            logs.map((log) => {
              const rowId = `${log.system}:${log.id}`;
              const isDisabledUserLog = Boolean(log.userId && disabledUserIds.has(log.userId));
              return (
                <tr
                  key={rowId}
                  className={isDisabledUserLog ? "disabled-user-log-row" : undefined}
                  onClick={() => setExpandedId(expandedId === rowId ? undefined : rowId)}
                >
                  <td>{dateTimeText(log.timestampMs)}</td>
                  <td>
                    <div className="field-stack">
                      <FieldChip label={logSystemLabel(log.system)} tone={log.system === "matching" ? "warning" : log.system === "training" ? "positive" : "info"} />
                      <small>{log.matchingLogKind ? matchingKindLabel(log.matchingLogKind) : (log.logGroup ? logGroupLabel(log.logGroup) : log.category ? auditCategoryLabel(log.category, language) : log.eventType ?? "--")}</small>
                    </div>
                  </td>
                  <td>
                    <div className="log-user-cell">
                      <span>{log.username ?? log.userId ?? "--"} / {log.role ?? "--"}</span>
                      {isDisabledUserLog ? <FieldChip label={t("disabled")} tone="negative" /> : null}
                    </div>
                  </td>
                  <td>
                    <div className="field-stack">
                      <span>{log.roundId ?? "--"}</span>
                      <small>{log.marketId ?? log.bookKey ?? "--"}</small>
                    </div>
                  </td>
                  <td>{auditActionLabel(log.actionType, language)}</td>
                  <td>{log.actionStatus ?? "--"}</td>
                  <td>
                    <div className="field-stack">
                      <span title={t("traceIdForInternalDiagnostics")}>{log.traceId ?? "--"}</span>
                      <small title={t("orderIdPositionId")}>{log.orderId ?? log.positionId ?? "--"}</small>
                    </div>
                  </td>
                  <td>
                    {log.logGroup === "market_latency" || log.logGroup === "system_latency" ? latencySummary(log) : redactNetworkAddresses(log.resultMessage ?? log.resultCode ?? "--")}
                    {expandedId === rowId ? <pre className="json-block">{jsonPreview(log.payload ?? log)}</pre> : null}
                  </td>
                </tr>
              );
            })
          )}
        </tbody>
      </table>
      {nextCursor ? (
        <div className="load-more-row">
          <button className="ghost-button compact-button" disabled={busy} onClick={() => search("append")}>
            {busy ? t("loading") : t("loadMore")}
          </button>
        </div>
      ) : null}
    </section>
    {exportDialogOpen ? (
      <LogExportDialog
        t={t}
        token={token}
        me={me}
        users={users}
        baseQuery={toQuery()}
        busy={exportBusy}
        setBusy={setExportBusy}
        onError={setError}
        onClose={() => setExportDialogOpen(false)}
      />
    ) : null}
    </>
  );
}

function LogExportDialog(props: {
  t: (key: string, options?: Record<string, unknown>) => string;
  token: string;
  me: PublicUser;
  users: PublicUser[];
  baseQuery: LogSearchQuery;
  busy: boolean;
  setBusy: (value: boolean) => void;
  onError: (message?: string) => void;
  onClose: () => void;
}) {
  const { t, token, me, baseQuery } = props;
  const language = me.language;
  const hasNativeSaveDialog = Boolean(window.paperTradingDesktop?.saveFile);
  const availableUsers = useMemo(() => {
    const byId = new Map<string, PublicUser>();
    for (const user of props.users) {
      byId.set(user.id, user);
    }
    byId.set(me.id, me);
    return [...byId.values()].sort((left, right) => left.username.localeCompare(right.username));
  }, [props.users, me]);
  const [systems, setSystems] = useState<Array<Exclude<LogSystem, "all">>>(
    baseQuery.system && baseQuery.system !== "all" ? [baseQuery.system] : LOG_EXPORT_SYSTEMS
  );
  const [selectedUserIds, setSelectedUserIds] = useState<string[]>(
    baseQuery.userIds?.length ? baseQuery.userIds : baseQuery.userId ? [baseQuery.userId] : []
  );
  const [form, setForm] = useState({
    from: dateTimeLocalValue(baseQuery.from),
    to: dateTimeLocalValue(baseQuery.to),
    role: baseQuery.role ?? "",
    category: baseQuery.category ?? "",
    actionType: baseQuery.actionType ?? "",
    actionStatus: baseQuery.actionStatus ?? "",
    moduleName: baseQuery.moduleName ?? "",
    pageName: baseQuery.pageName ?? "",
    symbol: baseQuery.symbol ?? "",
    roundId: baseQuery.roundId ?? "",
    marketId: baseQuery.marketId ?? "",
    marketSlug: baseQuery.marketSlug ?? "",
    orderId: baseQuery.orderId ?? "",
    positionId: baseQuery.positionId ?? "",
    traceId: baseQuery.traceId ?? "",
    resultCode: baseQuery.resultCode ?? "",
    direction: baseQuery.direction ?? "",
    roundStatus: baseQuery.roundStatus ?? "",
    settlementResult: baseQuery.settlementResult ?? "",
    bookKey: baseQuery.bookKey ?? "",
    bookSide: baseQuery.bookSide ?? "",
    eventType: baseQuery.eventType ?? "",
    sequenceFrom: typeof baseQuery.sequenceFrom === "number" ? String(baseQuery.sequenceFrom) : "",
    sequenceTo: typeof baseQuery.sequenceTo === "number" ? String(baseQuery.sequenceTo) : "",
    logGroup: baseQuery.logGroup ?? "",
    latencySource: baseQuery.latencySource ?? "",
    connectionState: baseQuery.connectionState ?? "",
    latencyPhase: baseQuery.latencyPhase ?? "",
    latencyMinMs: typeof baseQuery.latencyMinMs === "number" ? String(baseQuery.latencyMinMs) : "",
    latencyMaxMs: typeof baseQuery.latencyMaxMs === "number" ? String(baseQuery.latencyMaxMs) : "",
    matchingLogKind: baseQuery.matchingLogKind ?? ""
  });
  const [message, setMessage] = useState<string>();

  const systemLabel = (system: Exclude<LogSystem, "all">) =>
    system === "audit"
      ? t("auditLogs")
      : system === "training"
        ? t("tradingLogs")
        : t("matchingEvents");

  const updateForm = (key: keyof typeof form, value: string) => {
    setForm((current) => ({ ...current, [key]: value }));
  };

  const toggleSystem = (system: Exclude<LogSystem, "all">) => {
    setSystems((current) => {
      if (current.includes(system)) {
        return current.length === 1 ? current : current.filter((item) => item !== system);
      }
      return [...current, system];
    });
  };

  const toggleUser = (userId: string) => {
    const visibleIds = availableUsers.map((user) => user.id);
    setSelectedUserIds((current) => {
      const next = new Set(current.length === 0 ? visibleIds : current);
      if (next.has(userId)) {
        next.delete(userId);
      } else {
        next.add(userId);
      }
      if (next.size === 0 || next.size === visibleIds.length) {
        return [];
      }
      return [...next];
    });
  };

  const buildExportQuery = (): LogSearchQuery => ({
    system: systems.length === 1 ? systems[0] : "all",
    systems,
    userIds: selectedUserIds.length ? selectedUserIds : undefined,
    from: form.from ? Date.parse(form.from) : undefined,
    to: form.to ? Date.parse(form.to) : undefined,
    role: (form.role as Role) || undefined,
    category: (form.category as LogSearchQuery["category"]) || undefined,
    actionType: form.actionType || undefined,
    actionStatus: (form.actionStatus as LogSearchQuery["actionStatus"]) || undefined,
    moduleName: form.moduleName || undefined,
    pageName: form.pageName || undefined,
    symbol: form.symbol || undefined,
    roundId: form.roundId || undefined,
    marketId: form.marketId || undefined,
    marketSlug: form.marketSlug || undefined,
    orderId: form.orderId || undefined,
    positionId: form.positionId || undefined,
    traceId: form.traceId || undefined,
    resultCode: form.resultCode || undefined,
    direction: (form.direction as TradeSide) || undefined,
    roundStatus: (form.roundStatus as LogSearchQuery["roundStatus"]) || undefined,
    settlementResult: (form.settlementResult as LogSearchQuery["settlementResult"]) || undefined,
    bookKey: form.bookKey || undefined,
    bookSide: (form.bookSide as TradeSide) || undefined,
    eventType: (form.eventType as LogSearchQuery["eventType"]) || undefined,
    sequenceFrom: numberOrUndefined(form.sequenceFrom),
    sequenceTo: numberOrUndefined(form.sequenceTo),
    logGroup: (form.logGroup as LogSearchQuery["logGroup"]) || undefined,
    latencySource: (form.latencySource as LogSearchQuery["latencySource"]) || undefined,
    connectionState: (form.connectionState as LogSearchQuery["connectionState"]) || undefined,
    latencyPhase: (form.latencyPhase as LogSearchQuery["latencyPhase"]) || undefined,
    latencyMinMs: numberOrUndefined(form.latencyMinMs),
    latencyMaxMs: numberOrUndefined(form.latencyMaxMs),
    matchingLogKind: (form.matchingLogKind as LogSearchQuery["matchingLogKind"]) || undefined
  });

  const submitExport = async () => {
    try {
      props.setBusy(true);
      props.onError(undefined);
      setMessage(undefined);
      const blob = await api.exportLogsZipPost(token, buildExportQuery());
      const result = await saveBlobWithDesktopFallback(blob, exportFileName());
      if (result.canceled) {
        setMessage(t("saveWasCanceled"));
        return;
      }
      setMessage(
        result.filePath
          ? t("savedTo", { resultfilePath: result.filePath })
          : localLabel(
              language,
              "导出已开始下载。当前是浏览器模式，浏览器无法直接选择任意本地保存路径，请在浏览器下载设置中选择位置。",
              "Export download started. Browser mode cannot choose an arbitrary local save path; use your browser download settings to choose the location."
            )
      );
    } catch (exportError) {
      const errorMessage = exportError instanceof Error ? exportError.message : "Export failed.";
      props.onError(errorMessage);
      setMessage(errorMessage);
    } finally {
      props.setBusy(false);
    }
  };

  return (
    <div className="modal-backdrop">
      <section className="panel export-dialog" onClick={(event) => event.stopPropagation()}>
        <div className="section-header">
          <div>
            <p className="eyebrow">{t("logExport")}</p>
            <h2>{t("exportWizard")}</h2>
          </div>
          <button className="ghost-button compact-button" onClick={props.onClose}>
            {localLabel(language, "关闭 Close", "Close")}
          </button>
        </div>
        {message ? <div className="inline-info-banner">{message}</div> : null}
        <div className="inline-info-banner">
          {hasNativeSaveDialog
            ? localLabel(
                language,
                "桌面端会在生成 ZIP 后打开系统保存对话框，请选择保存目录和文件名。",
                "The desktop app opens the native save dialog after the ZIP is generated so you can choose the folder and file name."
              )
            : localLabel(
                language,
                "当前是浏览器模式：网页不能直接写入用户指定的任意本地路径，将使用浏览器下载兜底。",
                "Browser mode: the page cannot write to an arbitrary local path, so it will fall back to the browser download flow."
              )}
        </div>
        <div className="dialog-section">
          <strong>{t("logSystems")}</strong>
          <div className="choice-grid">
            {LOG_EXPORT_SYSTEMS.map((system) => (
              <label key={system} className="check-choice">
                <input type="checkbox" checked={systems.includes(system)} onChange={() => toggleSystem(system)} />
                <span>{systemLabel(system)}</span>
              </label>
            ))}
          </div>
        </div>
        <div className="dialog-section">
          <div className="section-header compact-header">
            <strong>{t("userScope")}</strong>
            <button className="ghost-button compact-button" onClick={() => setSelectedUserIds([])}>
              {t("allInScope")}
            </button>
          </div>
          <div className="choice-grid user-choice-grid">
            {availableUsers.map((user) => (
              <label key={user.id} className="check-choice">
                <input
                  type="checkbox"
                  checked={selectedUserIds.length === 0 || selectedUserIds.includes(user.id)}
                  onChange={() => toggleUser(user.id)}
                />
                <span>
                  {user.username} / {user.role}
                </span>
              </label>
            ))}
          </div>
        </div>
        <div className="filter-grid">
          <label>
            {t("from")}
            <input type="datetime-local" value={form.from} onChange={(event) => updateForm("from", event.target.value)} />
          </label>
          <label>
            {t("to")}
            <input type="datetime-local" value={form.to} onChange={(event) => updateForm("to", event.target.value)} />
          </label>
          <label>
            {t("role")}
            <select value={form.role} onChange={(event) => updateForm("role", event.target.value)}>
              <option value="">{t("all")}</option>
              {ROLE_OPTIONS.map((role) => (
                <option key={role} value={role}>
                  {role}
                </option>
              ))}
            </select>
          </label>
          <label>
            category
            <select value={form.category} onChange={(event) => updateForm("category", event.target.value)}>
              <option value="">{t("all")}</option>
              <option value="operation">operation</option>
              <option value="matching">matching</option>
              <option value="settlement">settlement</option>
              <option value="latency">latency</option>
            </select>
          </label>
          <label>
            {t("logGroup")}
            <select value={form.logGroup} onChange={(event) => updateForm("logGroup", event.target.value)}>
              <option value="">{t("all")}</option>
              {LOG_GROUP_OPTIONS.map((group) => (
                <option key={group} value={group}>
                  {group}
                </option>
              ))}
            </select>
          </label>
          <label>
            {t("latencySource")}
            <select value={form.latencySource} onChange={(event) => updateForm("latencySource", event.target.value)}>
              <option value="">{t("all")}</option>
              {LATENCY_SOURCE_OPTIONS.map((source) => (
                <option key={source} value={source}>
                  {source}
                </option>
              ))}
            </select>
          </label>
          <label>
            {t("connectionState")}
            <select value={form.connectionState} onChange={(event) => updateForm("connectionState", event.target.value)}>
              <option value="">{t("all")}</option>
              {CONNECTION_STATE_OPTIONS.map((state) => (
                <option key={state} value={state}>
                  {state}
                </option>
              ))}
            </select>
          </label>
          <label>
            {t("latencyPhase")}
            <select value={form.latencyPhase} onChange={(event) => updateForm("latencyPhase", event.target.value)}>
              <option value="">{t("all")}</option>
              {LATENCY_PHASE_OPTIONS.map((phase) => (
                <option key={phase} value={phase}>
                  {phase}
                </option>
              ))}
            </select>
          </label>
          <label>
            {t("minLatencyMs")}
            <input type="number" value={form.latencyMinMs} onChange={(event) => updateForm("latencyMinMs", event.target.value)} />
          </label>
          <label>
            {t("maxLatencyMs")}
            <input type="number" value={form.latencyMaxMs} onChange={(event) => updateForm("latencyMaxMs", event.target.value)} />
          </label>
          <label>
            {t("matchingKind")}
            <select value={form.matchingLogKind} onChange={(event) => updateForm("matchingLogKind", event.target.value)}>
              <option value="">{t("all")}</option>
              {MATCHING_KIND_OPTIONS.map((kind) => (
                <option key={kind} value={kind}>
                  {kind}
                </option>
              ))}
            </select>
          </label>
          <label>
            {t("round")}
            <input value={form.roundId} onChange={(event) => updateForm("roundId", event.target.value)} />
          </label>
          <label>
            marketId
            <input value={form.marketId} onChange={(event) => updateForm("marketId", event.target.value)} />
          </label>
          <label>
            marketSlug
            <input value={form.marketSlug} onChange={(event) => updateForm("marketSlug", event.target.value)} />
          </label>
          <label>
            <span title={t("orderIdUniqueIdForAUserSystemOrder")}>
              {t("orderId")}
            </span>
            <input value={form.orderId} onChange={(event) => updateForm("orderId", event.target.value)} />
          </label>
          <label>
            positionId
            <input value={form.positionId} onChange={(event) => updateForm("positionId", event.target.value)} />
          </label>
          <label>
            <span title={t("traceIdInternalRequestProcessTraceForDebugging")}>
              {t("traceId")}
            </span>
            <input value={form.traceId} onChange={(event) => updateForm("traceId", event.target.value)} />
          </label>
          <label>
            {t("actionType")}
            <input value={form.actionType} onChange={(event) => updateForm("actionType", event.target.value)} />
          </label>
          <label>
            {t("status")}
            <select value={form.actionStatus} onChange={(event) => updateForm("actionStatus", event.target.value)}>
              <option value="">{t("all")}</option>
              {ACTION_STATUS_OPTIONS.map((status) => (
                <option key={status} value={status}>
                  {status}
                </option>
              ))}
            </select>
          </label>
          <label>
            direction
            <select value={form.direction} onChange={(event) => updateForm("direction", event.target.value)}>
              <option value="">{t("all")}</option>
              <option value="UP">UP</option>
              <option value="DOWN">DOWN</option>
            </select>
          </label>
          <label>
            eventType
            <select value={form.eventType} onChange={(event) => updateForm("eventType", event.target.value)}>
              <option value="">{t("all")}</option>
              {MATCHING_EVENT_OPTIONS.map((eventType) => (
                <option key={eventType} value={eventType}>
                  {eventType}
                </option>
              ))}
            </select>
          </label>
          <label>
            bookKey
            <input value={form.bookKey} onChange={(event) => updateForm("bookKey", event.target.value)} />
          </label>
          <label>
            sequenceFrom
            <input type="number" value={form.sequenceFrom} onChange={(event) => updateForm("sequenceFrom", event.target.value)} />
          </label>
          <label>
            sequenceTo
            <input type="number" value={form.sequenceTo} onChange={(event) => updateForm("sequenceTo", event.target.value)} />
          </label>
        </div>
        <div className="button-row dialog-actions">
          <button className="secondary-button" disabled={props.busy || systems.length === 0} onClick={submitExport}>
            {props.busy
              ? t("loading")
              : hasNativeSaveDialog
                ? t("chooseSaveLocationAndExport")
                : t("downloadExport")}
          </button>
          <button className="ghost-button" disabled={props.busy} onClick={props.onClose}>
            {t("cancel")}
          </button>
        </div>
      </section>
    </div>
  );
}


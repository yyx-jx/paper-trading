import { useEffect, useState, type MouseEvent as ReactMouseEvent } from "react";
import { useTranslation } from "react-i18next";
import { useRef } from "react";
import { useMemo } from "react";
import type { ChangeEvent } from "react";
import {
  api,
  type AuditEvent,
  type BehaviorActionLog,
  type BulkCreateUserInput,
  type BulkCreateUsersResult,
  type CandleBar,
  type CandleInterval,
  type HistoryRound,
  type Language,
  type LogFacets,
  type LogSearchQuery,
  type LogSystem,
  type MarketPayload,
  type MarketSnapshot,
  type OrderAction,
  type OrderRecord,
  type PaperOrderKind,
  type PositionRecord,
  type ProfileOverview,
  type PublicUser,
  type RoundRecord,
  type Role,
  type SettlementPreview,
  type SourceHealth,
  type TradeSide,
  type TradeTimeline,
  type UnifiedLogRow,
  type UserPayload
} from "./utils/api";
import {
  isOrderBookStale,
  orderBookAgeMs,
  sourceFreshnessAlertKey,
  sourceFreshnessLabelKey
} from "./utils/displayMetrics";
import { useAppStore } from "./store/useAppStore";

declare global {
  interface Window {
    paperTradingDesktop?: {
      platform: string;
      saveFile?: (input: {
        defaultFileName: string;
        bytes: ArrayBuffer;
      }) => Promise<{ canceled: boolean; filePath?: string }>;
    };
  }
}

const money = (value = 0, digits = 2) =>
  new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  }).format(value);

const decimal = (value = 0, digits = 2) => value.toFixed(digits);
const signedMoney = (value = 0) => `${value >= 0 ? "+" : "-"}${money(Math.abs(value))}`;

function pad2(value: number) {
  return String(value).padStart(2, "0");
}

function utcParts(value: number) {
  const date = new Date(value);
  return {
    year: date.getUTCFullYear(),
    month: pad2(date.getUTCMonth() + 1),
    day: pad2(date.getUTCDate()),
    hour: pad2(date.getUTCHours()),
    minute: pad2(date.getUTCMinutes()),
    second: pad2(date.getUTCSeconds())
  };
}

const timeText = (value?: number) => {
  if (!value) {
    return "--";
  }
  const { hour, minute, second } = utcParts(value);
  return `${hour}:${minute}:${second} UTC`;
};

const dateTimeText = (value?: number) => {
  if (!value) {
    return "--";
  }
  const { year, month, day, hour, minute, second } = utcParts(value);
  return `${year}-${month}-${day} ${hour}:${minute}:${second} UTC`;
};

const compactPercent = (value = 0) => `${(value * 100).toFixed(1)}%`;
const jsonPreview = (value: unknown) => JSON.stringify(value ?? {}, null, 2);
const exportFileName = () => `paper-trading-export-${new Date().toISOString().slice(0, 10)}.zip`;
const LOG_EXPORT_SYSTEMS: Array<Exclude<LogSystem, "all">> = ["audit", "training", "matching"];
const ROLE_OPTIONS: Role[] = ["Tester", "Senior Tester", "Test Engineer", "Admin"];
const LANGUAGE_OPTIONS: Language[] = ["zh-CN", "en-US"];
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

function splitDelimitedLine(line: string, delimiter: "," | "\t") {
  const cells: string[] = [];
  let current = "";
  let quoted = false;
  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    const next = line[index + 1];
    if (char === '"' && quoted && next === '"') {
      current += '"';
      index += 1;
      continue;
    }
    if (char === '"') {
      quoted = !quoted;
      continue;
    }
    if (char === delimiter && !quoted) {
      cells.push(current.trim());
      current = "";
      continue;
    }
    current += char;
  }
  cells.push(current.trim());
  return cells;
}

interface ParsedBulkUserRow extends BulkCreateUserInput {
  rowNumber: number;
  errors: string[];
}

function parseBulkUserText(text: string, existingUsers: PublicUser[], language: Language) {
  const errors: string[] = [];
  const normalized = text.replace(/^\uFEFF/, "").trim();
  if (!normalized) {
    return {
      rows: [] as ParsedBulkUserRow[],
      validUsers: [] as BulkCreateUserInput[],
      errors: [localLabel(language, "请先导入或粘贴 CSV/TSV 内容。", "Import or paste CSV/TSV content first.")]
    };
  }

  const lines = normalized.split(/\r?\n/).filter((line) => line.trim());
  const delimiter = (lines[0].split("\t").length > lines[0].split(",").length ? "\t" : ",") as "," | "\t";
  const headers = splitDelimitedLine(lines[0], delimiter).map((header) => header.trim());
  const headerMap = new Map(headers.map((header, index) => [header.toLowerCase(), index]));
  for (const header of ["username", "password"]) {
    if (!headerMap.has(header)) {
      errors.push(localLabel(language, `缺少必填表头 ${header}。`, `Missing required header ${header}.`));
    }
  }

  const existingNames = new Set(existingUsers.map((user) => user.username));
  const seniorLookup = new Map(
    existingUsers
      .filter((user) => user.role === "Senior Tester" && user.isActive)
      .flatMap((user) => [
        [user.id, user.id],
        [user.username, user.id]
      ])
  );
  const seen = new Set<string>();
  const rows: ParsedBulkUserRow[] = [];

  const readValue = (values: string[], key: string) => {
    const index = headerMap.get(key.toLowerCase());
    return typeof index === "number" ? values[index]?.trim() ?? "" : "";
  };

  for (let lineIndex = 1; lineIndex < lines.length; lineIndex += 1) {
    const values = splitDelimitedLine(lines[lineIndex], delimiter);
    const rowNumber = lineIndex + 1;
    const rowErrors: string[] = [];
    const username = readValue(values, "username");
    const password = readValue(values, "password");
    const displayName = readValue(values, "displayName") || username;
    const roleText = readValue(values, "role") || "Tester";
    const languageText = readValue(values, "language") || "zh-CN";
    const seniorInput = readValue(values, "seniorTesterId");
    const availableUsdcText = readValue(values, "availableUsdc");

    if (!username) {
      rowErrors.push(localLabel(language, "username 必填。", "username is required."));
    }
    if (!password) {
      rowErrors.push(localLabel(language, "password 必填。", "password is required."));
    }
    if (username && seen.has(username)) {
      rowErrors.push(localLabel(language, "批次内 username 重复。", "Duplicate username in this batch."));
    }
    if (username) {
      seen.add(username);
    }
    if (username && existingNames.has(username)) {
      rowErrors.push(localLabel(language, "username 已存在。", "username already exists."));
    }
    if (!ROLE_OPTIONS.includes(roleText as Role)) {
      rowErrors.push(localLabel(language, "role 不合法。", "role is invalid."));
    }
    if (!LANGUAGE_OPTIONS.includes(languageText as Language)) {
      rowErrors.push(localLabel(language, "language 不合法。", "language is invalid."));
    }

    const role = ROLE_OPTIONS.includes(roleText as Role) ? (roleText as Role) : "Tester";
    const rowLanguage = LANGUAGE_OPTIONS.includes(languageText as Language) ? (languageText as Language) : "zh-CN";
    const seniorTesterId = seniorInput ? seniorLookup.get(seniorInput) : undefined;
    if (seniorInput && role !== "Tester") {
      rowErrors.push(localLabel(language, "seniorTesterId 仅适用于 Tester。", "seniorTesterId only applies to Tester."));
    }
    if (seniorInput && role === "Tester" && !seniorTesterId) {
      rowErrors.push(
        localLabel(
          language,
          "seniorTesterId 必须是有效的 Senior Tester ID 或用户名。",
          "seniorTesterId must be a valid Senior Tester ID or username."
        )
      );
    }

    const availableUsdc = availableUsdcText ? Number(availableUsdcText) : undefined;
    if (availableUsdcText && (!Number.isFinite(availableUsdc) || Number(availableUsdc) < 0)) {
      rowErrors.push(localLabel(language, "availableUsdc 必须是非负数字。", "availableUsdc must be a non-negative number."));
    }

    rows.push({
      rowNumber,
      username,
      password,
      displayName,
      role,
      language: rowLanguage,
      seniorTesterId: role === "Tester" ? seniorTesterId : undefined,
      availableUsdc,
      errors: rowErrors
    });
  }

  const validUsers =
    errors.length === 0 && rows.every((row) => row.errors.length === 0)
      ? rows.map(({ rowNumber: _rowNumber, errors: _errors, ...row }) => row)
      : [];

  return {
    rows,
    validUsers,
    errors
  };
}

const CHART_WINDOW_MS = 30 * 60_000;
const chartTimeText = (value?: number) => {
  if (!value) {
    return "--";
  }
  const { hour, minute } = utcParts(value);
  return `${hour}:${minute}`;
};

function roundTimeRangeText(round: Pick<RoundRecord, "startAt" | "endAt">) {
  return `${chartTimeText(round.startAt)}-${chartTimeText(round.endAt)} UTC`;
}

function datedRoundTimeRangeText(round: Pick<RoundRecord, "startAt" | "endAt">) {
  const start = utcParts(round.startAt);
  const end = utcParts(round.endAt);
  const startDate = `${start.year}-${start.month}-${start.day}`;
  const endDate = `${end.year}-${end.month}-${end.day}`;
  if (startDate === endDate) {
    return `${startDate} ${start.hour}:${start.minute}-${end.hour}:${end.minute} UTC`;
  }
  return `${startDate} ${start.hour}:${start.minute} - ${endDate} ${end.hour}:${end.minute} UTC`;
}

function roundTitleText(
  round: Pick<RoundRecord, "symbol" | "startAt" | "endAt"> | undefined,
  language: Language,
  fallback?: string
) {
  if (!round) {
    return fallback ?? "--";
  }
  return language === "zh-CN"
    ? `${round.symbol} 5 分钟轮次 ${roundTimeRangeText(round)}`
    : `${round.symbol} 5-Min Round ${roundTimeRangeText(round)}`;
}

function normalizeChartBars(bars: CandleBar[]) {
  const deduped = new Map<number, CandleBar>();
  for (const bar of bars) {
    deduped.set(bar.startTs, bar);
  }

  return [...deduped.values()].sort((left, right) => left.startTs - right.startTs);
}

function filterBarsToRecentWindow(bars: CandleBar[], windowMs = CHART_WINDOW_MS) {
  const visibleBars = normalizeChartBars(bars).filter((bar) => bar.high > 0 || bar.low > 0 || bar.close > 0);
  const latestEndTs = visibleBars.at(-1)?.endTs;
  if (!latestEndTs) {
    return [];
  }
  const windowStartTs = latestEndTs - windowMs + 1;
  return visibleBars.filter((bar) => bar.endTs >= windowStartTs);
}

function buildAxisLabelIndices(length: number, targetCount: number) {
  if (length <= 0) {
    return [];
  }
  if (length <= targetCount) {
    return Array.from({ length }, (_, index) => index);
  }

  const lastIndex = length - 1;
  const indices = new Set<number>();
  for (let step = 0; step < targetCount; step += 1) {
    indices.add(Math.round((step * lastIndex) / Math.max(targetCount - 1, 1)));
  }
  indices.add(lastIndex);
  return [...indices].sort((left, right) => left - right);
}

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(value, max));
}

function formatCountdown(value?: number, now = Date.now(), mode: "endAt" | "remainingMs" = "endAt") {
  if (typeof value !== "number") {
    return "--:--";
  }
  const remaining = Math.max(mode === "remainingMs" ? value : value - now, 0);
  const minutes = Math.floor(remaining / 60000)
    .toString()
    .padStart(2, "0");
  const seconds = Math.floor((remaining % 60000) / 1000)
    .toString()
    .padStart(2, "0");
  return `${minutes}:${seconds}`;
}

function positionStatusLabel(position: PositionRecord, language: Language) {
  const labels =
    language === "zh-CN"
      ? {
          open: "持仓中",
          pending_settlement: "待结算",
          settled: "已结算",
          sold: "已卖出"
        }
      : {
          open: "Open",
          pending_settlement: "Pending Settlement",
          settled: "Settled",
          sold: "Sold"
        };
  return labels[position.displayStatus ?? (position.status === "closed" ? "settled" : "open")];
}

function positionDisplayedPnl(position: PositionRecord) {
  return position.displayStatus === "open" ? position.unrealizedPnl : position.realizedPnl;
}

function isCurrentRoundOrder(order: OrderRecord, currentRound?: RoundRecord) {
  if (!currentRound) {
    return false;
  }
  return order.roundId === currentRound.id || Boolean(order.marketSlug && order.marketSlug === currentRound.marketSlug);
}

function orderStatusLabel(order: OrderRecord, language: Language) {
  if (order.status === "pending") {
    return localLabel(language, "待成交", "Pending");
  }
  if (order.status === "filled") {
    return localLabel(language, "已成交", "Filled");
  }
  if (order.status === "cancelled") {
    return localLabel(language, "已撤单", "Cancelled");
  }
  if (order.status === "failed") {
    return localLabel(language, "失败", "Failed");
  }
  return order.status;
}

function orderResultLabel(order: OrderRecord, language: Language) {
  const kind = order.orderKind === "limit" ? localLabel(language, "限价", "Limit") : localLabel(language, "市价", "Market");
  return `${kind} / ${orderStatusLabel(order, language)}`;
}

const PANEL_PAGE_SIZE = 10;

function pageCountFor(total: number, pageSize = PANEL_PAGE_SIZE) {
  return Math.max(Math.ceil(total / pageSize), 1);
}

function clampPage(page: number, totalItems: number, pageSize = PANEL_PAGE_SIZE) {
  return Math.min(Math.max(page, 0), pageCountFor(totalItems, pageSize) - 1);
}

function paginateRows<T>(items: T[], page: number, pageSize = PANEL_PAGE_SIZE) {
  const safePage = clampPage(page, items.length, pageSize);
  const start = safePage * pageSize;
  return items.slice(start, start + pageSize);
}

function orderStatusTone(status: OrderRecord["status"]) {
  if (status === "filled") {
    return "positive";
  }
  if (status === "pending" || status === "partial") {
    return "warning";
  }
  if (status === "failed") {
    return "negative";
  }
  return "neutral";
}

function orderBookExecutionPrice(order: OrderRecord) {
  const price = order.action === "buy" ? order.bestAsk : order.bestBid;
  return price > 0 ? price : order.midPrice;
}

function OrderExecutionCell({ order, language }: { order: OrderRecord; language: Language }) {
  const bookPrice = orderBookExecutionPrice(order);
  return (
    <div className="field-stack compact-order-metrics">
      <strong>{order.avgFillPrice ? decimal(order.avgFillPrice, 4) : "--"}</strong>
      <small className="cell-note">
        {localLabel(language, "盘口价", "Book")}: {bookPrice > 0 ? decimal(bookPrice, 4) : "--"}
      </small>
      <small className="cell-note">
        {localLabel(language, "滑点", "Slippage")}: {typeof order.slippageBps === "number" ? `${decimal(order.slippageBps, 2)} bps` : "--"}
      </small>
    </div>
  );
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

function actionTone(action: string) {
  if (action === "buy") {
    return "positive";
  }
  if (action === "sell") {
    return "negative";
  }
  return "info";
}

function sideTone(side: TradeSide) {
  return side === "UP" ? "positive" : "negative";
}

function orderKindLabel(order: OrderRecord, language: Language) {
  return order.orderKind === "limit" ? localLabel(language, "限价", "Limit") : localLabel(language, "市价", "Market");
}

function pageSummaryText(language: Language, page: number, totalItems: number, pageSize = PANEL_PAGE_SIZE) {
  return localLabel(
    language,
    `第 ${Math.min(page + 1, pageCountFor(totalItems, pageSize))} / ${pageCountFor(totalItems, pageSize)} 页`,
    `Page ${Math.min(page + 1, pageCountFor(totalItems, pageSize))} of ${pageCountFor(totalItems, pageSize)}`
  );
}

function sortOrdersForTradingPage(left: OrderRecord, right: OrderRecord, currentRound?: RoundRecord) {
  const leftCurrent = isCurrentRoundOrder(left, currentRound) ? 1 : 0;
  const rightCurrent = isCurrentRoundOrder(right, currentRound) ? 1 : 0;
  if (leftCurrent !== rightCurrent) {
    return rightCurrent - leftCurrent;
  }
  const leftPending = left.status === "pending" ? 1 : 0;
  const rightPending = right.status === "pending" ? 1 : 0;
  if (leftPending !== rightPending) {
    return rightPending - leftPending;
  }
  return right.createdAt - left.createdAt;
}

function latencyFor(source?: SourceHealth, now = Date.now(), clientRecvTs?: number) {
  if (!source || source.state === "disabled") {
    return {
      sourceToBackendLatencyMs: 0,
      backendToFrontendLatencyMs: undefined,
      endToEndLatencyMs: undefined,
      dataAgeMs: 0,
      marketUpdateAgeMs: 0,
      disabled: true
    };
  }
  const backendToFrontendLatencyMs =
    typeof source.clientRecvTs === "number"
      ? Math.max(source.clientRecvTs - source.serverPublishTs, 0)
      : typeof clientRecvTs === "number"
        ? Math.max(clientRecvTs - source.serverPublishTs, 0)
        : typeof source.frontendLatencyMs === "number"
          ? Math.max(source.frontendLatencyMs, 0)
          : undefined;
  return {
    sourceToBackendLatencyMs: Math.max(source.acquireLatencyMs, 0),
    backendToFrontendLatencyMs,
    endToEndLatencyMs:
      typeof backendToFrontendLatencyMs === "number"
        ? Math.max(source.serverPublishTs - source.sourceEventTs + backendToFrontendLatencyMs, 0)
        : undefined,
    dataAgeMs: Math.max(now - source.normalizedTs, 0),
    marketUpdateAgeMs:
      typeof source.clientRecvTs === "number"
        ? Math.max(now - source.clientRecvTs, 0)
        : typeof clientRecvTs === "number"
          ? Math.max(now - clientRecvTs, 0)
          : 0,
    disabled: false
  };
}

function localLabel(language: Language, zh: string, en: string) {
  return language === "zh-CN" ? zh : en;
}

function getSellBlockedReason(input: {
  language: Language;
  position: PositionRecord;
  currentRound?: RoundRecord;
  nowMs: number;
  acceptingOrders: boolean;
}) {
  const { language, position, currentRound, nowMs, acceptingOrders } = input;
  if (position.displayStatus !== "open" || position.status !== "open") {
    return localLabel(language, "该持仓已关闭，不能继续卖出。", "This position is already closed.");
  }
  if (!currentRound || position.roundId !== currentRound.id) {
    return localLabel(language, "该持仓不属于当前可交易轮次。", "This position does not belong to the current tradable round.");
  }
  const availableQty = Math.max(position.qty - (position.lockedQty ?? 0), 0);
  if (availableQty <= 0.0001) {
    return localLabel(language, "该持仓没有可卖出的可用数量。", "This position has no unlocked quantity available to sell.");
  }
  if (currentRound.status !== "Trading") {
    return localLabel(language, "当前轮次已冻结，不能再卖出持仓。", "Current round is frozen and can no longer sell positions.");
  }
  if (!acceptingOrders) {
    if (currentRound.endAt - nowMs <= 10_000) {
      return localLabel(language, "当前轮次已进入最后 10 秒禁卖窗口。", "Current round entered the final 10-second sell freeze window.");
    }
    return localLabel(language, "当前轮次暂不接受卖出订单。", "Current round is not accepting sell orders.");
  }
  return undefined;
}

function roundMoveLabel(round: HistoryRound, language: Language) {
  if (!isBtcReferencePrice(round.polymarketOpenPrice) || !isBtcReferencePrice(round.polymarketClosePrice)) {
    return "--";
  }
  const delta = round.polymarketClosePrice - round.polymarketOpenPrice;
  if (Math.abs(delta) < 0.0001) {
    return localLabel(language, "持平", "Flat");
  }
  return delta > 0 ? localLabel(language, "上涨", "Up") : localLabel(language, "下跌", "Down");
}

function roundMoveTone(round: HistoryRound) {
  if (!isBtcReferencePrice(round.polymarketOpenPrice) || !isBtcReferencePrice(round.polymarketClosePrice)) {
    return "tone-neutral";
  }
  const delta = round.polymarketClosePrice - round.polymarketOpenPrice;
  if (Math.abs(delta) < 0.0001) {
    return "tone-neutral";
  }
  return delta > 0 ? "tone-positive" : "tone-negative";
}

function settlementPreviewLabel(preview: SettlementPreview, language: Language) {
  if (preview.state === "preliminary") {
    return localLabel(language, "Preliminary 初步", "Preliminary");
  }
  if (preview.state === "manual") {
    return localLabel(language, "Manual Review 人工复核", "Manual Review");
  }
  return localLabel(language, "Confirmed 已确认", "Confirmed");
}

function settlementPreviewText(preview: SettlementPreview, language: Language) {
  const side = preview.side ?? "--";
  const price = typeof preview.price === "number" ? decimal(preview.price, 3) : "--";
  const source = preview.source;
  if (preview.state === "manual") {
    return preview.message ?? localLabel(language, "Gamma polling timed out", "Gamma polling timed out");
  }
  return localLabel(
    language,
    `${side} @ ${price} · 来源 ${source}`,
    `${side} @ ${price} · ${source}`
  );
}

function settlementPreviewHelpText(preview: SettlementPreview, language: Language) {
  if (preview.state === "preliminary") {
    return localLabel(
      language,
      "初步结果，仅用于即时感知，不参与正式结算",
      "Preliminary only; not used for final settlement"
    );
  }
  if (preview.state === "manual") {
    return localLabel(language, "需要人工复核后才能确认结算", "Manual review is required before settlement is confirmed");
  }
  return localLabel(language, "正式确认结果", "Official confirmed result");
}

function settlementPreviewTone(preview?: SettlementPreview) {
  if (!preview) {
    return "neutral";
  }
  if (preview.state === "manual") {
    return "negative";
  }
  if (preview.state === "confirmed") {
    return "positive";
  }
  return "warning";
}

interface EquityCurvePoint {
  roundId: string;
  marketSlug?: string;
  status: RoundRecord["status"];
  startAt: number;
  endAt: number;
  roundPnl: number;
  orderCount: number;
  cumulativeEquity: number;
  label: string;
  datedLabel: string;
}

type OperatedEquityWindow = 10 | 30 | 60 | "all";

interface RoundDisplayMeta {
  roundId: string;
  marketSlug?: string;
  status?: RoundRecord["status"];
  startAt?: number;
  endAt?: number;
  userPnl?: number;
}

interface RoundGroupedPositionView extends RoundDisplayMeta {
  positions: PositionRecord[];
  totalQty: number;
  openCount: number;
  positionValue: number;
  floatingPnl: number;
}

interface RoundGroupedOrderView extends RoundDisplayMeta {
  orders: OrderRecord[];
  pendingCount: number;
  notionalUsdc: number;
  filledQty: number;
}

interface RoundCalendarItem {
  roundId: string;
  marketSlug?: string;
  status: RoundRecord["status"];
  startAt: number;
  endAt: number;
  roundPnl: number;
  orderCount: number;
  sequence: number;
  label: string;
  datedLabel: string;
}

interface RoundLogDialogState {
  item: RoundCalendarItem;
  logs: AuditEvent[];
  behaviorLogs: BehaviorActionLog[];
}

function parseBtcFiveMinuteSlugStart(value?: string) {
  const match = value?.toLowerCase().match(/^btc-updown-5m-(\d+)$/);
  if (!match?.[1]) {
    return undefined;
  }
  const startAt = Number(match[1]) * 1000;
  return Number.isSafeInteger(startAt) && startAt > 0 ? startAt : undefined;
}

function inferRoundMeta(roundId: string, historyByRoundId: Map<string, HistoryRound>, marketSlug?: string): RoundDisplayMeta {
  const historyRound = historyByRoundId.get(roundId);
  if (historyRound) {
    return {
      roundId,
      marketSlug: historyRound.marketSlug ?? marketSlug,
      status: historyRound.status,
      startAt: historyRound.startAt,
      endAt: historyRound.endAt,
      userPnl: historyRound.userPnl
    };
  }
  const inferredSlug = marketSlug ?? roundId;
  const slugStartAt = parseBtcFiveMinuteSlugStart(inferredSlug);
  return {
    roundId,
    marketSlug,
    startAt: slugStartAt,
    endAt: typeof slugStartAt === "number" ? slugStartAt + 5 * 60_000 : undefined
  };
}

function roundDisplayTitle(meta: RoundDisplayMeta, language: Language) {
  if (typeof meta.startAt === "number" && typeof meta.endAt === "number") {
    return roundTimeRangeText({ startAt: meta.startAt, endAt: meta.endAt });
  }
  return localLabel(language, "盘口时间待同步", "Round Time Pending");
}

function roundSecondaryText(meta: RoundDisplayMeta, language: Language) {
  const slug = meta.marketSlug ?? meta.roundId;
  return `${localLabel(language, "盘口", "Market")}: ${slug}`;
}

function buildOperatedHistory(history: HistoryRound[], orders: OrderRecord[]) {
  const orderCountByRoundId = new Map<string, number>();
  for (const order of orders) {
    orderCountByRoundId.set(order.roundId, (orderCountByRoundId.get(order.roundId) ?? 0) + 1);
  }
  return [...history]
    .filter((round) => (orderCountByRoundId.get(round.id) ?? 0) > 0)
    .sort((left, right) => left.startAt - right.startAt)
    .map((round) => ({ round, orderCount: orderCountByRoundId.get(round.id) ?? 0 }));
}

function applyEquityWindow<T>(items: T[], window: OperatedEquityWindow) {
  return window === "all" ? items : items.slice(Math.max(items.length - window, 0));
}

function FastEquityCurve(props: { points: EquityCurvePoint[]; minValue: number; maxValue: number }) {
  const [hoverIndex, setHoverIndex] = useState<number>();
  const width = 1080;
  const height = 420;
  const padLeft = 58;
  const padRight = 14;
  const padTop = 10;
  const padBottom = 34;
  const chartWidth = width - padLeft - padRight;
  const chartHeight = height - padTop - padBottom;
  const domainMin = props.minValue;
  const domainMax = props.maxValue > props.minValue ? props.maxValue : props.minValue + 1;
  const valueRange = domainMax - domainMin;
  const xFor = (index: number) =>
    props.points.length === 1 ? padLeft + chartWidth / 2 : padLeft + (index / (props.points.length - 1)) * chartWidth;
  const yFor = (value: number) => padTop + chartHeight - ((value - domainMin) / valueRange) * chartHeight;
  const linePoints = props.points
    .map((point, index) => `${xFor(index).toFixed(1)},${yFor(point.cumulativeEquity).toFixed(1)}`)
    .join(" ");
  const ticks = [domainMax, domainMin + valueRange / 2, domainMin];
  const first = props.points[0];
  const last = props.points[props.points.length - 1];
  const hoverPoint = typeof hoverIndex === "number" ? props.points[hoverIndex] : undefined;
  const hoverX = typeof hoverIndex === "number" ? xFor(hoverIndex) : undefined;
  const hoverY = hoverPoint ? yFor(hoverPoint.cumulativeEquity) : undefined;
  const tooltipX = typeof hoverX === "number" ? Math.min(Math.max(hoverX + 18, padLeft), width - 330) : 0;
  const tooltipY = typeof hoverY === "number" ? Math.min(Math.max(hoverY - 76, padTop + 8), height - padBottom - 106) : 0;

  const handlePointerMove = (event: ReactMouseEvent<SVGSVGElement>) => {
    const rect = event.currentTarget.getBoundingClientRect();
    const pointerX = ((event.clientX - rect.left) / rect.width) * width;
    const nearestIndex = props.points.reduce(
      (nearest, _point, index) =>
        Math.abs(xFor(index) - pointerX) < Math.abs(xFor(nearest) - pointerX) ? index : nearest,
      0
    );
    setHoverIndex(nearestIndex);
  };

  return (
    <svg
      className="profile-fast-curve"
      viewBox={`0 0 ${width} ${height}`}
      role="img"
      aria-label="Equity curve"
      onMouseMove={handlePointerMove}
      onMouseLeave={() => setHoverIndex(undefined)}
    >
      {ticks.map((tick) => {
        const y = yFor(tick);
        return (
          <g key={tick.toFixed(2)}>
            <line x1={padLeft} x2={width - padRight} y1={y} y2={y} />
            <text x={padLeft - 8} y={y + 4} textAnchor="end">
              {money(tick, 0)}
            </text>
          </g>
        );
      })}
      <polyline points={linePoints} />
      {props.points.map((point, index) => (
        <circle key={point.roundId} cx={xFor(index)} cy={yFor(point.cumulativeEquity)} r="2.8">
          <title>
            {`${point.datedLabel} · ${point.marketSlug ?? point.roundId} · ${signedMoney(point.roundPnl)} · ${signedMoney(point.cumulativeEquity)}`}
          </title>
        </circle>
      ))}
      {hoverPoint && typeof hoverX === "number" && typeof hoverY === "number" ? (
        <g className="profile-fast-curve-hover">
          <line className="hover-line" x1={hoverX} x2={hoverX} y1={padTop} y2={height - padBottom} />
          <circle className="hover-dot" cx={hoverX} cy={hoverY} r="6" />
          <g className="hover-tooltip" transform={`translate(${tooltipX} ${tooltipY})`}>
            <rect width="310" height="96" rx="12" />
            <text x="12" y="22">{hoverPoint.datedLabel}</text>
            <text x="12" y="42">{hoverPoint.marketSlug ?? hoverPoint.roundId}</text>
            <text x="12" y="64">{`单轮盈亏: ${signedMoney(hoverPoint.roundPnl)}`}</text>
            <text x="12" y="84">{`累计收益: ${signedMoney(hoverPoint.cumulativeEquity)}`}</text>
          </g>
        </g>
      ) : null}
      {first ? (
        <text className="x-label" x={padLeft} y={height - 9}>
          {first.datedLabel}
        </text>
      ) : null}
      {last && last !== first ? (
        <text className="x-label" x={width - padRight} y={height - 9} textAnchor="end">
          {last.datedLabel}
        </text>
      ) : null}
    </svg>
  );
}

function buildEquityCurve(history: HistoryRound[], orders: OrderRecord[], window: OperatedEquityWindow): EquityCurvePoint[] {
  const operated = buildOperatedHistory(history, orders);
  const visible = applyEquityWindow(operated, window);
  if (visible.length === 0) {
    return [];
  }

  let runningProfit = 0;

  return visible.map(({ round, orderCount }) => {
    runningProfit += round.userPnl;
    return {
      roundId: round.id,
      marketSlug: round.marketSlug,
      status: round.status,
      startAt: round.startAt,
      endAt: round.endAt,
      roundPnl: round.userPnl,
      orderCount,
      cumulativeEquity: Number(runningProfit.toFixed(2)),
      label: roundTimeRangeText(round),
      datedLabel: datedRoundTimeRangeText(round)
    };
  });
}

function buildRoundCalendarItems(history: HistoryRound[], orders: OrderRecord[]): RoundCalendarItem[] {
  return buildOperatedHistory(history, orders).map(({ round, orderCount }, index) => ({
    roundId: round.id,
    marketSlug: round.marketSlug,
    status: round.status,
    startAt: round.startAt,
    endAt: round.endAt,
    roundPnl: round.userPnl,
    orderCount,
    sequence: index + 1,
    label: roundTimeRangeText(round),
    datedLabel: datedRoundTimeRangeText(round)
  }));
}

function buildGroupedPositions(history: HistoryRound[], positions: PositionRecord[], orders: OrderRecord[]): RoundGroupedPositionView[] {
  const historyByRoundId = new Map(history.map((round) => [round.id, round]));
  const slugByRoundId = new Map<string, string>();
  for (const order of orders) {
    if (order.marketSlug && !slugByRoundId.has(order.roundId)) {
      slugByRoundId.set(order.roundId, order.marketSlug);
    }
  }
  const grouped = new Map<string, PositionRecord[]>();
  for (const position of positions) {
    grouped.set(position.roundId, [...(grouped.get(position.roundId) ?? []), position]);
  }
  return [...grouped.entries()]
    .map(([roundId, roundPositions]) => {
      const meta = inferRoundMeta(roundId, historyByRoundId, slugByRoundId.get(roundId));
      return {
        ...meta,
        positions: roundPositions,
        totalQty: roundPositions.reduce((sum, position) => sum + position.qty, 0),
        openCount: roundPositions.filter((position) => position.displayStatus === "open").length,
        positionValue: roundPositions.reduce((sum, position) => sum + (position.currentValue ?? position.qty * position.currentMark), 0),
        floatingPnl: roundPositions.reduce((sum, position) => sum + positionDisplayedPnl(position), 0)
      };
    })
    .sort((left, right) => (right.startAt ?? 0) - (left.startAt ?? 0));
}

function buildGroupedOrders(history: HistoryRound[], orders: OrderRecord[]): RoundGroupedOrderView[] {
  const historyByRoundId = new Map(history.map((round) => [round.id, round]));
  const grouped = new Map<string, OrderRecord[]>();
  for (const order of orders) {
    grouped.set(order.roundId, [...(grouped.get(order.roundId) ?? []), order]);
  }
  return [...grouped.entries()]
    .map(([roundId, roundOrders]) => {
      const marketSlug = roundOrders.find((order) => order.marketSlug)?.marketSlug;
      const meta = inferRoundMeta(roundId, historyByRoundId, marketSlug);
      return {
        ...meta,
        orders: roundOrders.sort((left, right) => right.createdAt - left.createdAt),
        pendingCount: roundOrders.filter((order) => order.status === "pending").length,
        notionalUsdc: roundOrders.reduce((sum, order) => sum + (order.requestedAmountUsdc ?? order.notionalUsdc), 0),
        filledQty: roundOrders.reduce((sum, order) => sum + order.filledQty, 0)
      };
    })
    .sort((left, right) => (right.startAt ?? 0) - (left.startAt ?? 0));
}

function extractMarketPayloadPublishTs(payload?: Pick<MarketPayload, "snapshot" | "transportMeta">) {
  if (!payload?.snapshot) {
    return 0;
  }
  if (payload.transportMeta?.serverPublishTs) {
    return payload.transportMeta.serverPublishTs;
  }
  const snapshot = payload.snapshot;
  return Math.max(
    snapshot.sources.binance.serverPublishTs,
    snapshot.sources.chainlink.serverPublishTs,
    snapshot.sources.clob.serverPublishTs
  );
}

function getMarketStreamState(lastMarketRecvTs?: number, now = Date.now()) {
  if (typeof lastMarketRecvTs !== "number") {
    return "reconnecting" as const;
  }
  const idleMs = Math.max(now - lastMarketRecvTs, 0);
  if (idleMs > 45_000) {
    return "reconnecting" as const;
  }
  if (idleMs > 15_000) {
    return "stale" as const;
  }
  return "live" as const;
}

function marketStreamStateLabel(language: Language, state: "live" | "stale" | "reconnecting") {
  if (state === "live") {
    return localLabel(language, "live", "live");
  }
  if (state === "stale") {
    return localLabel(language, "stale", "stale");
  }
  return localLabel(language, "reconnecting", "reconnecting");
}

function sourceTone(state?: SourceHealth["state"]) {
  if (state === "healthy") {
    return "positive";
  }
  if (state === "disabled") {
    return "neutral";
  }
  if (state === "reconnecting" || state === "stale") {
    return "warning";
  }
  return "negative";
}

function isBtcReferencePrice(value?: number): value is number {
  return typeof value === "number" && Number.isFinite(value) && value > 1000;
}

function metricValueForSource(source: SourceHealth | undefined, value: number, digits = 2) {
  if (source?.state === "disabled" || !isBtcReferencePrice(value)) {
    return "--";
  }
  return money(value, digits);
}

function sourceStateLabel(language: Language, state?: SourceHealth["state"]) {
  if (!state) {
    return "--";
  }
  const labels: Record<SourceHealth["state"], { zh: string; en: string }> = {
    healthy: { zh: "live", en: "live" },
    reconnecting: { zh: "重连中", en: "reconnecting" },
    stale: { zh: "延迟", en: "stale" },
    degraded: { zh: "降级", en: "degraded" },
    disabled: { zh: "停用", en: "disabled" }
  };
  const label = labels[state];
  return localLabel(language, label.zh, label.en);
}

function AppMetric(props: {
  label: string;
  value: string;
  tone?: "positive" | "negative" | "neutral" | "warning";
  caption?: string;
}) {
  return (
    <div className={`app-metric tone-${props.tone ?? "neutral"}`}>
      <span>{props.label}</span>
      <strong>{props.value}</strong>
      {props.caption ? <small>{props.caption}</small> : null}
    </div>
  );
}

function SourceBadge(props: {
  label: string;
  source?: SourceHealth;
  nowMs: number;
  clientRecvTs?: number;
  language: Language;
  t: (key: string) => string;
  marketStreamState?: "live" | "stale" | "reconnecting";
}) {
  const source = props.source;
  const latency = latencyFor(source, props.nowMs, source?.clientRecvTs ?? props.clientRecvTs);
  const sourceName = source?.source ?? props.label;
  const isChainlinkSource = sourceName.toLowerCase() === "chainlink";
  const freshnessLabelKey = sourceFreshnessLabelKey(sourceName);
  const freshnessAlertKey = sourceFreshnessAlertKey(sourceName);
  const endToEndAlert =
    !latency.disabled && typeof latency.endToEndLatencyMs === "number" && latency.endToEndLatencyMs > 3000;
  return (
    <div className={`source-badge tone-${endToEndAlert ? "negative" : sourceTone(source?.state)}`}>
      <div className="source-badge-head">
        <strong>{props.label}</strong>
        <span>{sourceStateLabel(props.language, source?.state)}</span>
      </div>
      <small>
        {props.t("sourceToBackend")}: {latency.disabled ? "--" : `${Math.round(latency.sourceToBackendLatencyMs)} ms`}
      </small>
      <small>
        {props.t("backendToFrontend")}: {latency.disabled || typeof latency.backendToFrontendLatencyMs !== "number" ? "--" : `${Math.round(latency.backendToFrontendLatencyMs)} ms`}
      </small>
      <small>
        {props.t(freshnessLabelKey)}: {latency.disabled || typeof latency.endToEndLatencyMs !== "number" ? "--" : `${Math.round(latency.endToEndLatencyMs)} ms`}
      </small>
      <small>
        {localLabel(props.language, "市场更新年龄", "Market Update Age")}: {latency.disabled ? "--" : `${Math.round(latency.marketUpdateAgeMs)} ms`}
      </small>
      {endToEndAlert ? <small className="source-latency-alert">{props.t(freshnessAlertKey)}</small> : null}
      {isChainlinkSource ? (
        <small className="source-message">
          {localLabel(
            props.language,
            "当前使用 Chainlink AggregatorV3 链上 Feed；Chainlink Data Streams 尚未接入，接入需申请付费 API。",
            "Using Chainlink AggregatorV3 on-chain feed; Chainlink Data Streams is not connected and requires paid API credentials."
          )}
        </small>
      ) : null}
      {source?.message ? <small className="source-message">{source.message}</small> : null}
    </div>
  );
}

function FieldChip(props: { label: string; tone?: "positive" | "negative" | "neutral" | "warning" | "info" }) {
  return <span className={`field-chip tone-${props.tone ?? "neutral"}`}>{props.label}</span>;
}

function CandlestickChart(props: {
  bars: CandleBar[];
  upColor: string;
  downColor: string;
  emptyText: string;
  priceToBeat?: number;
  latestPrice?: number;
  round?: RoundRecord;
}) {
  const bars = filterBarsToRecentWindow(props.bars);
  const svgRef = useRef<SVGSVGElement | null>(null);
  const [chartSize, setChartSize] = useState({ width: 900, height: 460 });
  const [hoveredBar, setHoveredBar] = useState<{ index: number; mouseX: number; mouseY: number } | undefined>();

  useEffect(() => {
    const svg = svgRef.current;
    const container = svg?.parentElement;
    if (!container) {
      return;
    }

    const applySize = (rect: Pick<DOMRectReadOnly, "width" | "height">) => {
      const nextSize = {
        width: Math.max(Math.round(rect.width), 320),
        height: Math.max(Math.round(rect.height), 280)
      };
      setChartSize((currentSize) =>
        currentSize.width === nextSize.width && currentSize.height === nextSize.height ? currentSize : nextSize
      );
    };

    applySize(container.getBoundingClientRect());

    if (typeof ResizeObserver === "undefined") {
      return;
    }

    const observer = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (entry) {
        applySize(entry.contentRect);
      }
    });
    observer.observe(container);
    return () => observer.disconnect();
  }, []);

  if (bars.length === 0) {
    return <div className="chart-empty">{props.emptyText}</div>;
  }

  const width = chartSize.width;
  const height = chartSize.height;
  const padding = { top: 20, right: 92, bottom: 36, left: 14 };
  const highs = bars.map((bar) => bar.high);
  const lows = bars.map((bar) => bar.low);
  const overlayPrices = [props.priceToBeat, props.latestPrice].filter((value): value is number => Boolean(value && value > 0));
  const max = Math.max(...highs, ...overlayPrices);
  const min = Math.min(...lows, ...overlayPrices);
  const rawRange = Math.max(max - min, 1);
  const maxWithPadding = max + rawRange * 0.08;
  const minWithPadding = min - rawRange * 0.08;
  const range = Math.max(maxWithPadding - minWithPadding, 1);
  const innerWidth = width - padding.left - padding.right;
  const innerHeight = height - padding.top - padding.bottom;
  const slotWidth = innerWidth / Math.max(bars.length, 1);
  const candleWidth = Math.max(Math.min(slotWidth * 0.58, 16), 3);

  const yForPrice = (value: number) => padding.top + ((maxWithPadding - value) / range) * innerHeight;
  const xForIndex = (index: number) => padding.left + slotWidth * index + slotWidth / 2;
  const axisLabelIndices = buildAxisLabelIndices(bars.length, Math.min(bars.length <= 6 ? bars.length : 6, bars.length));
  const priceTicks = [0, 1, 2, 3, 4].map((step) => maxWithPadding - (range * step) / 4);
  const tooltipWidth = 126;
  const tooltipHeight = 106;
  const hoveredIndex = hoveredBar?.index;
  const hoveredCandle = typeof hoveredIndex === "number" ? bars[hoveredIndex] : undefined;
  const hoveredX = typeof hoveredIndex === "number" ? xForIndex(hoveredIndex) : undefined;
  const latestY = typeof props.latestPrice === "number" && props.latestPrice > 0 ? yForPrice(props.latestPrice) : undefined;
  const targetY = typeof props.priceToBeat === "number" && props.priceToBeat > 0 ? yForPrice(props.priceToBeat) : undefined;
  const targetLabelY =
    typeof targetY === "number" && typeof latestY === "number" && Math.abs(targetY - latestY) < 18
      ? targetY - 14
      : typeof targetY === "number"
        ? targetY - 6
        : undefined;
  const latestLabelY =
    typeof latestY === "number" && typeof targetY === "number" && Math.abs(targetY - latestY) < 18
      ? latestY + 18
      : typeof latestY === "number"
        ? latestY + 14
        : undefined;
  const roundStartX =
    props.round && props.round.startAt >= bars[0].startTs && props.round.startAt <= bars.at(-1)!.endTs
      ? padding.left + ((props.round.startAt - bars[0].startTs) / Math.max(bars.at(-1)!.endTs - bars[0].startTs, 1)) * innerWidth
      : undefined;
  const roundEndX =
    props.round && props.round.endAt >= bars[0].startTs && props.round.endAt <= bars.at(-1)!.endTs
      ? padding.left + ((props.round.endAt - bars[0].startTs) / Math.max(bars.at(-1)!.endTs - bars[0].startTs, 1)) * innerWidth
      : undefined;
  const tooltipX =
    hoveredBar && hoveredCandle
      ? clamp(
          hoveredBar.mouseX + 14 + tooltipWidth > width - padding.right
            ? hoveredBar.mouseX - tooltipWidth - 14
            : hoveredBar.mouseX + 14,
          padding.left,
          width - padding.right - tooltipWidth
        )
      : undefined;
  const tooltipY =
    hoveredBar && hoveredCandle
      ? clamp(
          hoveredBar.mouseY - tooltipHeight - 12 < padding.top
            ? hoveredBar.mouseY + 12
            : hoveredBar.mouseY - tooltipHeight - 12,
          padding.top,
          height - padding.bottom - tooltipHeight
        )
      : undefined;

  const setHoveredBarFromEvent = (event: ReactMouseEvent<SVGRectElement>) => {
    const svgRect = event.currentTarget.ownerSVGElement?.getBoundingClientRect();
    const fallbackIndex = Math.max(0, bars.length - 1);
    if (!svgRect) {
      setHoveredBar({
        index: fallbackIndex,
        mouseX: xForIndex(fallbackIndex),
        mouseY: padding.top + innerHeight / 2
      });
      return;
    }
    const mouseX = ((event.clientX - svgRect.left) / svgRect.width) * width;
    const mouseY = ((event.clientY - svgRect.top) / svgRect.height) * height;
    const index = clamp(Math.floor((mouseX - padding.left) / Math.max(slotWidth, 1)), 0, bars.length - 1);
    setHoveredBar({
      index: Math.round(index),
      mouseX,
      mouseY
    });
  };

  return (
    <svg
      ref={svgRef}
      viewBox={`0 0 ${width} ${height}`}
      className="candle-chart"
      role="img"
      aria-label="candlestick chart"
      onMouseLeave={() => setHoveredBar(undefined)}
    >
      <rect x="0" y="0" width={width} height={height} rx="20" fill="transparent" />
      {priceTicks.map((tick) => {
        const y = yForPrice(tick);
        return (
          <g key={tick}>
            <line x1={padding.left} y1={y} x2={width - padding.right} y2={y} className="chart-grid-line" />
            <text x={width - padding.right + 12} y={y + 4} className="chart-axis-label chart-price-axis-label">
              {decimal(tick, 2)}
            </text>
          </g>
        );
      })}
      {bars.map((bar, index) => {
        const x = xForIndex(index);
        const openY = yForPrice(bar.open);
        const closeY = yForPrice(bar.close);
        const highY = yForPrice(bar.high);
        const lowY = yForPrice(bar.low);
        const color = bar.close >= bar.open ? props.upColor : props.downColor;
        const bodyTop = Math.min(openY, closeY);
        const bodyHeight = Math.max(Math.abs(closeY - openY), 2);
        const isHovered = hoveredIndex === index;
        return (
          <g key={`${bar.interval}-${bar.startTs}`}>
            <line
              x1={x}
              y1={highY}
              x2={x}
              y2={lowY}
              stroke={color}
              strokeWidth={isHovered ? "2.4" : "1.8"}
              className={isHovered ? "chart-candle-wick is-hovered" : "chart-candle-wick"}
            />
            <rect
              x={x - candleWidth / 2}
              y={bodyTop}
              width={candleWidth}
              height={bodyHeight}
              rx="3"
              fill={color}
              fillOpacity={isHovered ? "1" : "0.95"}
              className={isHovered ? "chart-candle-body is-hovered" : "chart-candle-body"}
            />
          </g>
        );
      })}
      {typeof hoveredX === "number" ? (
        <line
          x1={hoveredX}
          y1={padding.top}
          x2={hoveredX}
          y2={height - padding.bottom}
          className="chart-hover-line"
        />
      ) : null}
      {typeof props.priceToBeat === "number" && props.priceToBeat > 0 ? (
        <g>
          <line x1={padding.left} y1={yForPrice(props.priceToBeat)} x2={width - padding.right} y2={yForPrice(props.priceToBeat)} className="chart-target-line" />
          <rect x={width - padding.right + 8} y={(targetLabelY ?? yForPrice(props.priceToBeat)) - 13} width="76" height="19" rx="6" className="chart-target-label-box" />
          <text x={width - padding.right + 14} y={targetLabelY ?? yForPrice(props.priceToBeat)} className="chart-target-label">
            PTB {decimal(props.priceToBeat, 2)}
          </text>
        </g>
      ) : null}
      {typeof props.latestPrice === "number" && props.latestPrice > 0 ? (
        <g>
          <line x1={padding.left} y1={yForPrice(props.latestPrice)} x2={width - padding.right} y2={yForPrice(props.latestPrice)} className="chart-current-line" />
          <rect x={width - padding.right + 8} y={(latestLabelY ?? yForPrice(props.latestPrice)) - 13} width="76" height="19" rx="6" className="chart-current-label-box" />
          <text x={width - padding.right + 14} y={latestLabelY ?? yForPrice(props.latestPrice)} className="chart-current-label">
            BTC {decimal(props.latestPrice, 2)}
          </text>
        </g>
      ) : null}
      {typeof roundStartX === "number" ? <line x1={roundStartX} y1={padding.top} x2={roundStartX} y2={height - padding.bottom} className="chart-round-line" /> : null}
      {typeof roundEndX === "number" ? <line x1={roundEndX} y1={padding.top} x2={roundEndX} y2={height - padding.bottom} className="chart-round-line" /> : null}
      {axisLabelIndices.map((barIndex) => {
        const bar = bars[barIndex];
        return (
          <text
            key={`${bar.startTs}-${barIndex}`}
            x={xForIndex(barIndex)}
            y={height - 8}
            textAnchor="middle"
            className="chart-axis-label"
          >
            {chartTimeText(bar.startTs)}
          </text>
        );
      })}
      {hoveredCandle && typeof tooltipX === "number" && typeof tooltipY === "number" ? (
        <g className="chart-tooltip" pointerEvents="none">
          <rect x={tooltipX} y={tooltipY} width={tooltipWidth} height={tooltipHeight} rx="12" className="chart-tooltip-box" />
          <text x={tooltipX + 12} y={tooltipY + 20} className="chart-tooltip-label">
            UTC
          </text>
          <text x={tooltipX + 12} y={tooltipY + 38} className="chart-tooltip-value">
            {chartTimeText(hoveredCandle.startTs)}
          </text>
          <text x={tooltipX + 12} y={tooltipY + 56} className="chart-tooltip-label">
            O
          </text>
          <text x={tooltipX + 34} y={tooltipY + 56} className="chart-tooltip-value">
            {decimal(hoveredCandle.open, 2)}
          </text>
          <text x={tooltipX + 72} y={tooltipY + 56} className="chart-tooltip-label">
            H
          </text>
          <text x={tooltipX + 92} y={tooltipY + 56} className="chart-tooltip-value">
            {decimal(hoveredCandle.high, 2)}
          </text>
          <text x={tooltipX + 12} y={tooltipY + 80} className="chart-tooltip-label">
            L
          </text>
          <text x={tooltipX + 34} y={tooltipY + 80} className="chart-tooltip-value">
            {decimal(hoveredCandle.low, 2)}
          </text>
          <text x={tooltipX + 72} y={tooltipY + 80} className="chart-tooltip-label">
            C
          </text>
          <text x={tooltipX + 92} y={tooltipY + 80} className="chart-tooltip-value">
            {decimal(hoveredCandle.close, 2)}
          </text>
        </g>
      ) : null}
      <rect
        x={padding.left}
        y={padding.top}
        width={innerWidth}
        height={innerHeight}
        fill="rgba(255,255,255,0.001)"
        pointerEvents="all"
        className="chart-hover-hitbox"
        onMouseEnter={setHoveredBarFromEvent}
        onMouseMove={setHoveredBarFromEvent}
      />
    </svg>
  );
}

function LoginScreen(props: {
  language: Language;
  error?: string;
  onLanguageChange: (language: Language) => void;
  onLogin: (username: string, password: string) => Promise<void>;
}) {
  const { t } = useTranslation();
  const [username, setUsername] = useState("tester");
  const [password, setPassword] = useState("tester123");
  const [busy, setBusy] = useState(false);

  return (
    <div className="login-shell">
      <div className="login-card">
        <div className="login-hero">
          <p className="eyebrow">{t("subtitle")}</p>
          <h1>{t("appTitle")}</h1>
          <span>{t("loginHint")}</span>
        </div>
        <label>
          <span>{t("username")}</span>
          <input value={username} onChange={(event) => setUsername(event.target.value)} />
        </label>
        <label>
          <span>{t("password")}</span>
          <input type="password" value={password} onChange={(event) => setPassword(event.target.value)} />
        </label>
        <label>
          <span>{t("language")}</span>
          <select value={props.language} onChange={(event) => props.onLanguageChange(event.target.value as Language)}>
            <option value="zh-CN">简体中文</option>
            <option value="en-US">English</option>
          </select>
        </label>
        <button
          className="primary-button"
          disabled={busy}
          onClick={async () => {
            setBusy(true);
            await props.onLogin(username, password);
            setBusy(false);
          }}
        >
          {t("login")}
        </button>
        {props.error ? <div className="error-banner">{props.error}</div> : null}
        <div className="account-hints">
          <strong>{t("testerHints")}</strong>
          <code>tester / tester123</code>
          <code>senior / senior123</code>
          <code>engineer / engineer123</code>
          <code>admin / admin123</code>
        </div>
      </div>
    </div>
  );
}

function App() {
  const { t, i18n } = useTranslation();
  const language = (i18n.language as Language) ?? "zh-CN";
  const {
    token,
    me,
    currentPage,
    currentRound,
    history,
    operatedHistory,
    snapshot,
    profile,
    positions,
    orders,
    logs,
    lastOrderLatencyMs,
    lastMarketRecvTs,
    setAuth,
    setUser,
    clearAuth,
    setCurrentPage,
    setShellData,
    setMarketPayload,
    setUserPayload,
    setSourceStatus,
    setLastOrderLatencyMs
  } = useAppStore();
  const [bootstrapping, setBootstrapping] = useState(false);
  const [error, setError] = useState<string>();
  const [orderAmount, setOrderAmount] = useState("150");
  const [orderQty, setOrderQty] = useState("1");
  const [limitPrice, setLimitPrice] = useState("0.5");
  const [orderAction, setOrderAction] = useState<OrderAction>("buy");
  const [orderKind, setOrderKind] = useState<PaperOrderKind>("market");
  const [selectedSide, setSelectedSide] = useState<TradeSide>("UP");
  const [selectedInterval, setSelectedInterval] = useState<CandleInterval>("1m");
  const [nowMs, setNowMs] = useState(Date.now());
  const [tradeBusy, setTradeBusy] = useState(false);
  const [quickBusy, setQuickBusy] = useState(false);
  const [cancelBusyOrderId, setCancelBusyOrderId] = useState<string>();
  const [sellBusyPositionId, setSellBusyPositionId] = useState<string>();
  const [sellFeedback, setSellFeedback] = useState<{ positionId?: string; message: string }>();
  const [timeline, setTimeline] = useState<TradeTimeline>();
  const [timelineBusyOrderId, setTimelineBusyOrderId] = useState<string>();
  const [roundLogDialog, setRoundLogDialog] = useState<RoundLogDialogState>();
  const [roundLogBusyRoundId, setRoundLogBusyRoundId] = useState<string>();
  const cancellingOrderIdsRef = useRef(new Set<string>());
  const countdownTargetMs =
    snapshot && typeof snapshot.uiMeta.countdownMs === "number"
      ? snapshot.serverNow + snapshot.uiMeta.countdownMs
      : currentRound?.endAt;
  const countdownText = formatCountdown(countdownTargetMs, nowMs);
  const headerTitle = roundTitleText(currentRound, language, snapshot?.uiMeta.marketTitle ?? t("refreshHint"));
  const canOpenUserManagement = me?.role === "Admin" || me?.role === "Senior Tester";

  useEffect(() => {
    const timer = window.setInterval(() => setNowMs(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    if (!token) {
      return;
    }

    let cancelled = false;
    const bootstrap = async () => {
      setBootstrapping(true);
      try {
        const [
          nextMe,
          roundData,
          nextHistory,
          nextOperatedHistory,
          nextProfile,
          nextPositions,
          nextOrders,
          nextLogs
        ] = await Promise.all([
          api.getMe(token),
          api.getCurrentRound(token),
          api.getHistory(token),
          api.getOperatedHistory(token),
          api.getProfile(token),
          api.getPositions(token),
          api.getOrders(token),
          api.getLogs(token)
        ]);

        if (cancelled) {
          return;
        }

        setUser(nextMe);
        i18n.changeLanguage(nextMe.language);
        setShellData({
          currentRound: roundData.currentRound,
          history: nextHistory,
          operatedHistory: nextOperatedHistory,
          snapshot: roundData.snapshot,
          profile: nextProfile,
          positions: nextPositions,
          orders: nextOrders,
          logs: nextLogs,
          settlementPreview: roundData.settlementPreview,
          transportMeta: roundData.transportMeta
        });

        if (nextMe.permissionCodes.includes("system:status:view")) {
          const status = await api.getSourceStatus(token);
          if (!cancelled) {
            setSourceStatus(status);
          }
        }
      } catch (bootstrapError) {
        clearAuth();
        setError(bootstrapError instanceof Error ? bootstrapError.message : "Bootstrap failed.");
      } finally {
        if (!cancelled) {
          setBootstrapping(false);
        }
      }
    };

    bootstrap();
    return () => {
      cancelled = true;
    };
  }, [token, clearAuth, i18n, setShellData, setSourceStatus, setUser]);

  useEffect(() => {
    if (!token) {
      return;
    }

    let disposed = false;
    let marketSocket: WebSocket | undefined;
    let userSocket: WebSocket | undefined;
    let marketReconnectTimer: number | undefined;
    let userReconnectTimer: number | undefined;
    let marketWatchdogTimer: number | undefined;
    let lastMarketMessageAt = Date.now();
    let refreshingMarket = false;
    const reconnectDelayMs = 1000;
    const marketPayloadRejectMs = 5000;
    const marketStaleMs = 15000;
    const marketReconnectStaleMs = 45000;

    const markMarketActivity = (receivedAt = Date.now()) => {
      lastMarketMessageAt = receivedAt;
    };

    const refreshMarketSnapshot = async () => {
      if (disposed || refreshingMarket) {
        return;
      }
      refreshingMarket = true;
      try {
        const [roundData, nextHistory] = await Promise.all([api.getCurrentRound(token), api.getHistory(token)]);
        if (!disposed) {
          const receivedAt = Date.now();
          const payload = {
            currentRound: roundData.currentRound,
            history: nextHistory,
            snapshot: roundData.snapshot,
            settlementPreview: roundData.settlementPreview,
            transportMeta: roundData.transportMeta
          };
          const publishTs = extractMarketPayloadPublishTs(payload);
          if (publishTs > 0 && receivedAt - publishTs > marketPayloadRejectMs) {
            return;
          }
          if (setMarketPayload(payload, receivedAt)) {
            markMarketActivity(receivedAt);
          }
        }
      } catch {
        // Socket reconnect remains the primary recovery path; polling is only a stale-data fallback.
      } finally {
        refreshingMarket = false;
      }
    };

    const scheduleMarketReconnect = () => {
      if (disposed || typeof marketReconnectTimer === "number") {
        return;
      }
      marketReconnectTimer = window.setTimeout(() => {
        marketReconnectTimer = undefined;
        connectMarketSocket();
      }, reconnectDelayMs);
    };

    const scheduleUserReconnect = () => {
      if (disposed || typeof userReconnectTimer === "number") {
        return;
      }
      userReconnectTimer = window.setTimeout(() => {
        userReconnectTimer = undefined;
        connectUserSocket();
      }, reconnectDelayMs);
    };

    const connectMarketSocket = () => {
      if (disposed) {
        return;
      }
      marketSocket?.close();
      const socket = new WebSocket(api.createWsUrl("/ws/market", token));
      marketSocket = socket;
      socket.onopen = () => {
        markMarketActivity();
      };
      socket.onmessage = (event) => {
        const receivedAt = Date.now();
        const parsed = JSON.parse(event.data) as {
          type: "market";
          data: MarketPayload;
        };
        if (parsed.type === "market") {
          const publishTs = extractMarketPayloadPublishTs(parsed.data);
          if (publishTs > 0 && receivedAt - publishTs > marketPayloadRejectMs) {
            void refreshMarketSnapshot();
            if (receivedAt - publishTs > marketReconnectStaleMs && socket.readyState === WebSocket.OPEN) {
              socket.close();
            }
            return;
          }
          if (setMarketPayload(parsed.data, receivedAt)) {
            markMarketActivity(receivedAt);
          }
        }
      };
      socket.onerror = () => {
        socket.close();
      };
      socket.onclose = () => {
        if (marketSocket === socket) {
          marketSocket = undefined;
        }
        scheduleMarketReconnect();
      };
    };

    const connectUserSocket = () => {
      if (disposed) {
        return;
      }
      userSocket?.close();
      const socket = new WebSocket(api.createWsUrl("/ws/user", token));
      userSocket = socket;
      socket.onmessage = (event) => {
        const parsed = JSON.parse(event.data) as {
          type: "user";
          data: UserPayload;
        };
        if (parsed.type === "user") {
          setUserPayload(parsed.data);
        }
      };
      socket.onerror = () => {
        socket.close();
      };
      socket.onclose = () => {
        if (userSocket === socket) {
          userSocket = undefined;
        }
        scheduleUserReconnect();
      };
    };

    const handleForegroundRecovery = () => {
      if (disposed) {
        return;
      }
      const now = Date.now();
      const idleMs = now - lastMarketMessageAt;
      if (!marketSocket || marketSocket.readyState !== WebSocket.OPEN) {
        void refreshMarketSnapshot();
        scheduleMarketReconnect();
        return;
      }
      if (idleMs > marketStaleMs) {
        void refreshMarketSnapshot();
      }
    };

    const handleVisibilityChange = () => {
      if (document.visibilityState === "visible") {
        handleForegroundRecovery();
      }
    };

    connectMarketSocket();
    connectUserSocket();
    document.addEventListener("visibilitychange", handleVisibilityChange);
    window.addEventListener("focus", handleForegroundRecovery);
    window.addEventListener("pageshow", handleForegroundRecovery);
    marketWatchdogTimer = window.setInterval(() => {
      if (disposed) {
        return;
      }
      const socket = marketSocket;
      if (!socket || socket.readyState === WebSocket.CLOSED) {
        void refreshMarketSnapshot();
        scheduleMarketReconnect();
        return;
      }
      const idleMs = Date.now() - lastMarketMessageAt;
      if (socket.readyState === WebSocket.OPEN && idleMs > marketStaleMs) {
        void refreshMarketSnapshot();
      }
      if (socket.readyState === WebSocket.OPEN && idleMs > marketReconnectStaleMs) {
        socket.close();
      }
    }, 1000);

    return () => {
      disposed = true;
      if (typeof marketReconnectTimer === "number") {
        window.clearTimeout(marketReconnectTimer);
      }
      if (typeof userReconnectTimer === "number") {
        window.clearTimeout(userReconnectTimer);
      }
      if (typeof marketWatchdogTimer === "number") {
        window.clearInterval(marketWatchdogTimer);
      }
      document.removeEventListener("visibilitychange", handleVisibilityChange);
      window.removeEventListener("focus", handleForegroundRecovery);
      window.removeEventListener("pageshow", handleForegroundRecovery);
      marketSocket?.close();
      userSocket?.close();
    };
  }, [token, setMarketPayload, setUserPayload]);

  const handleLogin = async (username: string, password: string) => {
    setError(undefined);
    try {
      const result = await api.login(username, password);
      setAuth(result.token);
    } catch (loginError) {
      setError(loginError instanceof Error ? loginError.message : "Login failed.");
    }
  };

  const handleLanguageChange = async (language: Language) => {
    i18n.changeLanguage(language);
    if (!token) {
      return;
    }
    try {
      const updated = await api.setLanguage(token, language);
      setUser(updated);
    } catch (languageError) {
      setError(languageError instanceof Error ? languageError.message : "Language update failed.");
    }
  };

  const handlePlaceOrder = async () => {
    if (!token) {
      return;
    }
    try {
      setTradeBusy(true);
      setError(undefined);
      const result = await api.placeOrder(token, {
        action: orderAction,
        side: selectedSide,
        orderKind,
        amount: orderAction === "buy" ? Number(orderAmount) : undefined,
        qty: orderAction === "sell" ? Number(orderQty) : undefined,
        limitPrice: orderKind === "limit" ? Number(limitPrice) : undefined
      });
      setLastOrderLatencyMs(result.order.matchLatencyMs);
    } catch (placeOrderError) {
      const message = placeOrderError instanceof Error ? placeOrderError.message : "Order failed.";
      if (message.includes("Insufficient virtual balance")) {
        setError(
          localLabel(
            language,
            `可用余额不足：本单需冻结 ${money(Number(orderAmount || 0))}，当前可用 ${money(profile?.availableUsdc ?? 0)}。`,
            `Insufficient available balance: this order would freeze ${money(Number(orderAmount || 0))}, current available is ${money(profile?.availableUsdc ?? 0)}.`
          )
        );
      } else {
        setError(message);
      }
    } finally {
      setTradeBusy(false);
    }
  };

  const handleCloseSide = async () => {
    if (!token) {
      return;
    }
    try {
      setQuickBusy(true);
      setError(undefined);
      const result = await api.closeSide(token, selectedSide);
      setLastOrderLatencyMs(result.matchLatencyMs);
    } catch (closeError) {
      setError(closeError instanceof Error ? closeError.message : "Close side failed.");
    } finally {
      setQuickBusy(false);
    }
  };

  const handleReverseSide = async () => {
    if (!token) {
      return;
    }
    try {
      setQuickBusy(true);
      setError(undefined);
      const result = await api.reverseSide(token, selectedSide);
      setSelectedSide(result.reverseSide);
      setLastOrderLatencyMs(result.reverseOrder.matchLatencyMs);
    } catch (reverseError) {
      setError(reverseError instanceof Error ? reverseError.message : "Reverse side failed.");
    } finally {
      setQuickBusy(false);
    }
  };

  const handleCancelOrder = async (orderId: string) => {
    if (!token) {
      return;
    }
    if (cancellingOrderIdsRef.current.has(orderId)) {
      return;
    }
    cancellingOrderIdsRef.current.add(orderId);
    setCancelBusyOrderId(orderId);
    try {
      setError(undefined);
      await api.cancelOrder(token, orderId);
      const [nextProfile, nextOperatedHistory, nextPositions, nextOrders, nextLogs] = await Promise.all([
        api.getProfile(token),
        api.getOperatedHistory(token),
        api.getPositions(token),
        api.getOrders(token),
        api.getLogs(token)
      ]);
      setUserPayload({
        profile: nextProfile,
        operatedHistory: nextOperatedHistory,
        positions: nextPositions,
        orders: nextOrders,
        logs: nextLogs
      });
    } catch (cancelError) {
      setError(cancelError instanceof Error ? cancelError.message : "Cancel failed.");
    } finally {
      cancellingOrderIdsRef.current.delete(orderId);
      setCancelBusyOrderId(undefined);
    }
  };

  const handleOpenTimeline = async (orderId: string) => {
    if (!token) {
      return;
    }
    try {
      setError(undefined);
      setTimelineBusyOrderId(orderId);
      setTimeline(await api.getTradeTimeline(token, orderId));
    } catch (timelineError) {
      setError(timelineError instanceof Error ? timelineError.message : "Timeline failed.");
    } finally {
      setTimelineBusyOrderId(undefined);
    }
  };

  const handleOpenRoundLogs = async (item: RoundCalendarItem) => {
    if (!token) {
      return;
    }
    try {
      setError(undefined);
      setRoundLogBusyRoundId(item.roundId);
      const activity = await api.getRoundActivity(token, item.roundId);
      setRoundLogDialog({
        item,
        logs: [...activity.auditLogs].sort((left, right) => left.serverRecvTs - right.serverRecvTs),
        behaviorLogs: [...activity.behaviorLogs].sort((left, right) => left.timestampMs - right.timestampMs)
      });
    } catch (roundLogError) {
      setError(roundLogError instanceof Error ? roundLogError.message : "Round logs failed.");
    } finally {
      setRoundLogBusyRoundId(undefined);
    }
  };

  const handleSell = async (positionId: string) => {
    if (!token) {
      return;
    }
    try {
      setSellBusyPositionId(positionId);
      setSellFeedback(undefined);
      setError(undefined);
      const result = await api.sellPosition(token, positionId);
      setLastOrderLatencyMs(result.matchLatencyMs);
      const [nextProfile, nextOperatedHistory, nextPositions, nextOrders, nextLogs] = await Promise.all([
        api.getProfile(token),
        api.getOperatedHistory(token),
        api.getPositions(token),
        api.getOrders(token),
        api.getLogs(token)
      ]);
      setUserPayload({
        profile: nextProfile,
        operatedHistory: nextOperatedHistory,
        positions: nextPositions,
        orders: nextOrders,
        logs: nextLogs
      });
    } catch (sellError) {
      const message = sellError instanceof Error ? sellError.message : "Sell failed.";
      setError(message);
      setSellFeedback({ positionId, message });
    } finally {
      setSellBusyPositionId(undefined);
    }
  };

  if (!token || !me) {
    return (
      <LoginScreen
        error={error}
        language={language}
        onLanguageChange={handleLanguageChange}
        onLogin={handleLogin}
      />
    );
  }

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand-block">
          <p className="eyebrow">{t("subtitle")}</p>
          <h1>{t("appTitle")}</h1>
          <span>{headerTitle}</span>
        </div>

        <div className="topbar-status">
          <div className="status-pill">
            <span>{t("symbol")}</span>
            <strong>{snapshot?.symbol ?? "BTC"}</strong>
          </div>
          <div className="status-pill">
            <span>{t("roundStatus")}</span>
            <strong>{currentRound?.status ?? "--"}</strong>
          </div>
          <div className="status-pill">
            <span>{t("countdown")}</span>
            <strong>{countdownText}</strong>
          </div>
        </div>

        <div className="topbar-actions">
          <nav className="page-tabs">
            <button className={currentPage === "trade" ? "active" : ""} onClick={() => setCurrentPage("trade")}>
              {t("trade")}
            </button>
            <button className={currentPage === "profile" ? "active" : ""} onClick={() => setCurrentPage("profile")}>
              {t("profile")}
            </button>
            <button className={currentPage === "logs" ? "active" : ""} onClick={() => setCurrentPage("logs")}>
              {t("auditSearch")}
            </button>
            {canOpenUserManagement ? (
              <button className={currentPage === "users" ? "active" : ""} onClick={() => setCurrentPage("users")}>
                {localLabel((i18n.language as Language) ?? "zh-CN", "用户管理", "Users")}
              </button>
            ) : null}
          </nav>
          <select value={i18n.language} onChange={(event) => handleLanguageChange(event.target.value as Language)}>
            <option value="zh-CN">简体中文</option>
            <option value="en-US">English</option>
          </select>
          <div className="user-pill">
            <strong>{me.displayName}</strong>
            <span>
              {t("role")}: {me.role}
            </span>
          </div>
          <button className="ghost-button" onClick={clearAuth}>
            {t("logout")}
          </button>
        </div>
      </header>

      {error ? <div className="error-banner">{error}</div> : null}
      {bootstrapping ? <div className="loading-banner">{t("bootstrapping")}</div> : null}

      <main className="page-grid">
        {currentPage === "trade" ? (
          <TradePage
            t={t}
            nowMs={nowMs}
            currentRound={currentRound}
            history={history}
            snapshot={snapshot}
            profile={profile}
            positions={positions}
            orders={orders}
            logs={logs}
            language={(i18n.language as Language) ?? "zh-CN"}
            selectedSide={selectedSide}
            selectedInterval={selectedInterval}
            lastOrderLatencyMs={lastOrderLatencyMs}
            lastMarketRecvTs={lastMarketRecvTs}
            orderAmount={orderAmount}
            orderQty={orderQty}
            limitPrice={limitPrice}
            orderAction={orderAction}
            orderKind={orderKind}
            tradeBusy={tradeBusy}
            quickBusy={quickBusy}
            sellBusyPositionId={sellBusyPositionId}
            sellFeedback={sellFeedback}
            canPlaceOrder={me.permissionCodes.includes("trade:order")}
            canSell={me.permissionCodes.includes("trade:sell")}
            onAmountChange={setOrderAmount}
            onQtyChange={setOrderQty}
            onLimitPriceChange={setLimitPrice}
            onOrderActionChange={setOrderAction}
            onOrderKindChange={setOrderKind}
            onIntervalChange={setSelectedInterval}
            onSelectSide={setSelectedSide}
            onPlaceOrder={handlePlaceOrder}
            onCloseSide={handleCloseSide}
            onReverseSide={handleReverseSide}
            onSell={handleSell}
            onCancel={handleCancelOrder}
            onTimeline={handleOpenTimeline}
            timelineBusyOrderId={timelineBusyOrderId}
            cancelBusyOrderId={cancelBusyOrderId}
          />
        ) : currentPage === "profile" ? (
          <ProfilePage
            t={t}
            language={(i18n.language as Language) ?? "zh-CN"}
            profile={profile}
            history={history}
            operatedHistory={operatedHistory}
            positions={positions}
            orders={orders}
            logs={logs}
            onSell={handleSell}
            onCancel={handleCancelOrder}
            onTimeline={handleOpenTimeline}
            onOpenRoundLogs={handleOpenRoundLogs}
            timelineBusyOrderId={timelineBusyOrderId}
            cancelBusyOrderId={cancelBusyOrderId}
            selectedRoundLogId={roundLogDialog?.item.roundId}
            roundLogBusyRoundId={roundLogBusyRoundId}
          />
        ) : currentPage === "logs" ? (
          <LogSearchPage
            t={t}
            token={token}
            me={me}
            canExport={me.permissionCodes.includes("profile:view") || me.role === "Admin"}
          />
        ) : (
          <UserManagementPage
            t={t}
            token={token}
            me={me}
            language={(i18n.language as Language) ?? "zh-CN"}
            onProfileRefresh={async () => {
              const nextMe = await api.getMe(token);
              setUser(nextMe);
            }}
          />
        )}
      </main>
      {timeline ? <TimelineDialog t={t} timeline={timeline} onClose={() => setTimeline(undefined)} /> : null}
      {roundLogDialog ? (
        <RoundLogDialog t={t} state={roundLogDialog} onClose={() => setRoundLogDialog(undefined)} />
      ) : null}
    </div>
  );
}

function TradePage(props: {
  t: (key: string) => string;
  language: Language;
  nowMs: number;
  currentRound?: RoundRecord;
  history: HistoryRound[];
  snapshot?: MarketSnapshot;
  profile?: ProfileOverview;
  positions: PositionRecord[];
  orders: OrderRecord[];
  logs: AuditEvent[];
  selectedSide: TradeSide;
  selectedInterval: CandleInterval;
  lastOrderLatencyMs?: number;
  lastMarketRecvTs?: number;
  orderAmount: string;
  orderQty: string;
  limitPrice: string;
  orderAction: OrderAction;
  orderKind: PaperOrderKind;
  tradeBusy: boolean;
  quickBusy: boolean;
  sellBusyPositionId?: string;
  sellFeedback?: { positionId?: string; message: string };
  canPlaceOrder: boolean;
  canSell: boolean;
  onAmountChange: (value: string) => void;
  onQtyChange: (value: string) => void;
  onLimitPriceChange: (value: string) => void;
  onOrderActionChange: (value: OrderAction) => void;
  onOrderKindChange: (value: PaperOrderKind) => void;
  onIntervalChange: (value: CandleInterval) => void;
  onSelectSide: (side: TradeSide) => void;
  onPlaceOrder: () => Promise<void>;
  onCloseSide: () => Promise<void>;
  onReverseSide: () => Promise<void>;
  onSell: (positionId: string) => Promise<void>;
  onCancel: (orderId: string) => Promise<void>;
  onTimeline: (orderId: string) => Promise<void>;
  timelineBusyOrderId?: string;
  cancelBusyOrderId?: string;
}) {
  const { t, snapshot, profile, positions, orders, selectedSide, selectedInterval, nowMs, language } = props;
  const [orderBookExpanded, setOrderBookExpanded] = useState(false);
  const [historyExpanded, setHistoryExpanded] = useState(false);
  const [tradePositionsExpanded, setTradePositionsExpanded] = useState(true);
  const [tradeOrdersExpanded, setTradeOrdersExpanded] = useState(true);
  const [tradePositionsPage, setTradePositionsPage] = useState(0);
  const [tradeOrdersPage, setTradeOrdersPage] = useState(0);
  const sourceBinance = snapshot?.sources.binance;
  const sourceChainlink = snapshot?.sources.chainlink;
  const sourceClob = snapshot?.sources.clob;
  const marketStreamState = getMarketStreamState(props.lastMarketRecvTs, nowMs);
  const pageRound = props.currentRound;
  const marketTitle = roundTitleText(pageRound, language, snapshot?.uiMeta.marketTitle ?? `${snapshot?.symbol ?? "BTC"} 5m`);
  const marketSubtitle = pageRound ? roundTimeRangeText(pageRound) : snapshot?.uiMeta.marketSubtitle ?? "--";
  const currentRoundPositions = positions.filter((position) => position.roundId === props.currentRound?.id);
  const selectedOpenPositions = currentRoundPositions.filter(
    (position) => position.status === "open" && position.side === selectedSide
  );
  const selectedExposureQty = selectedOpenPositions.reduce((sum, position) => sum + position.qty, 0);
  const selectedLockedQty = selectedOpenPositions.reduce((sum, position) => sum + (position.lockedQty ?? 0), 0);
  const selectedAvailableQty = Math.max(selectedExposureQty - selectedLockedQty, 0);
  const selectedExposureValue = selectedOpenPositions.reduce(
    (sum, position) => sum + position.qty * position.currentMark,
    0
  );
  const chartBars = filterBarsToRecentWindow(snapshot?.binance.candlesByInterval[selectedInterval] ?? []);
  const tradePrice = selectedSide === "UP" ? snapshot?.upPrice ?? 0 : snapshot?.downPrice ?? 0;
  const parsedAmount = Number(props.orderAmount || 0);
  const parsedLimitPrice = Number(props.limitPrice || 0);
  const estimatedPrice = props.orderKind === "limit" && parsedLimitPrice > 0 ? parsedLimitPrice : tradePrice;
  const estimatedQty =
    props.orderAction === "buy"
      ? estimatedPrice > 0
        ? parsedAmount / estimatedPrice
        : 0
      : Number(props.orderQty || 0);
  const payoutIfWin = props.orderAction === "buy" ? estimatedQty : undefined;
  const estimatedNotional =
    props.orderAction === "buy" ? parsedAmount : estimatedQty * estimatedPrice;
  const orderBook = selectedSide === "UP" ? snapshot?.clob.upBook : snapshot?.clob.downBook;
  const selectedOrderBookAgeMs = orderBookAgeMs(orderBook, nowMs);
  const selectedOrderBookStale = isOrderBookStale(orderBook, nowMs);
  const acceptingOrders = Boolean(snapshot?.uiMeta.acceptingOrders && props.currentRound?.status === "Trading");
  const balanceWarning =
    props.orderAction === "buy" && parsedAmount > (profile?.availableUsdc ?? 0) + 0.0001
      ? localLabel(
          language,
          `可用余额不足：本单需冻结 ${money(parsedAmount)}，当前可用 ${money(profile?.availableUsdc ?? 0)}。`,
          `Insufficient available balance: this order would freeze ${money(parsedAmount)}, current available is ${money(profile?.availableUsdc ?? 0)}.`
        )
      : undefined;
  const canTrade = (props.orderAction === "buy" ? props.canPlaceOrder : props.canSell) && acceptingOrders && !balanceWarning;
  const canQuickAction = props.canSell && acceptingOrders;
  const sellFeedbackMessage = props.sellFeedback?.message;
  const clobTransportLatency = latencyFor(sourceClob, nowMs, sourceClob?.clientRecvTs ?? props.lastMarketRecvTs);
  const chainlinkLatency = latencyFor(sourceChainlink, nowMs, sourceChainlink?.clientRecvTs ?? props.lastMarketRecvTs);
  const chainlinkMetricCaption =
    chainlinkLatency.disabled || typeof chainlinkLatency.endToEndLatencyMs !== "number"
      ? undefined
      : `${t("chainlinkFeedAge")}: ${Math.round(chainlinkLatency.endToEndLatencyMs)} ms`;
  const currentRoundOrders = orders.filter((order) => isCurrentRoundOrder(order, props.currentRound));
  const pendingOrders = currentRoundOrders.filter((order) => order.status === "pending");
  const sortedTradeOrders = [...currentRoundOrders].sort((left, right) => sortOrdersForTradingPage(left, right, props.currentRound));
  const tradePositionsTotalPages = pageCountFor(currentRoundPositions.length);
  const tradeOrdersTotalPages = pageCountFor(sortedTradeOrders.length);
  const tradePositionsPageSafe = clampPage(tradePositionsPage, currentRoundPositions.length);
  const tradeOrdersPageSafe = clampPage(tradeOrdersPage, sortedTradeOrders.length);
  const displayPositions = paginateRows(currentRoundPositions, tradePositionsPageSafe);
  const displayOrders = paginateRows(sortedTradeOrders, tradeOrdersPageSafe);
  const polymarketUrl = snapshot?.marketSlug
    ? `https://polymarket.com/event/${snapshot.marketSlug}`
    : props.currentRound?.marketSlug
      ? `https://polymarket.com/event/${props.currentRound.marketSlug}`
      : "https://polymarket.com";
  const binanceUrl = "https://www.binance.com/en/trade/BTC_USDT?type=spot";

  useEffect(() => {
    setTradePositionsPage((page) => clampPage(page, currentRoundPositions.length));
  }, [currentRoundPositions.length]);

  useEffect(() => {
    setTradeOrdersPage((page) => clampPage(page, sortedTradeOrders.length));
  }, [sortedTradeOrders.length]);

  return (
    <>
      <section className="market-header">
        <div className="market-title-block">
          <p className="eyebrow">{t("market")}</p>
          <h2>{marketTitle}</h2>
          <span>{marketSubtitle}</span>
          <div className="market-link-row">
            <a className="market-link" href={polymarketUrl} target="_blank" rel="noreferrer">
              <strong>Polymarket</strong>
              <span>CLOB</span>
            </a>
            <a className="market-link" href={binanceUrl} target="_blank" rel="noreferrer">
              <strong>Binance</strong>
              <span>Spot</span>
            </a>
          </div>
        </div>
        <div className="portfolio-strip">
          <AppMetric label={t("available")} value={money(profile?.availableUsdc ?? 0)} />
          <AppMetric
            label={localLabel(language, "总盈亏", "Total PnL")}
            value={signedMoney(profile?.realizedPnlToday ?? 0)}
            tone={(profile?.realizedPnlToday ?? 0) >= 0 ? "positive" : "negative"}
          />
          <AppMetric
            label={t("floatingPnl")}
            value={signedMoney(profile?.unrealizedPnl ?? 0)}
            tone={(profile?.unrealizedPnl ?? 0) >= 0 ? "positive" : "negative"}
          />
          <AppMetric label={t("positionValue")} value={money(profile?.positionValue ?? 0)} />
          <AppMetric label={t("lastOrderLatency")} value={props.lastOrderLatencyMs ? `${props.lastOrderLatencyMs} ms` : "--"} />
        </div>
      </section>

      <section className="trade-layout">
        <div className="panel chart-panel">
          <div className="section-header">
            <div>
              <p className="eyebrow">{t("chart")}</p>
              <h2>{t("binanceKline")}</h2>
            </div>
            <div className="interval-tabs">
              {(["1m", "5m"] as CandleInterval[]).map((interval) => (
                <button
                  key={interval}
                  className={selectedInterval === interval ? "active" : ""}
                  onClick={() => props.onIntervalChange(interval)}
                >
                  {interval}
                </button>
              ))}
            </div>
          </div>

          <div className="headline-metrics">
            <AppMetric label={t("currentPrice")} value={money(snapshot?.binance.spotPrice ?? 0)} />
            <AppMetric label={t("priceToBeat")} value={money(snapshot?.priceToBeat ?? 0)} />
            <AppMetric
              label={t("chainlinkPrice")}
              value={metricValueForSource(sourceChainlink, snapshot?.chainlink.referencePrice ?? 0)}
              caption={chainlinkMetricCaption}
            />
          </div>

          <div className="chart-shell">
            <CandlestickChart
              bars={chartBars}
              upColor="#3fd07d"
              downColor="#ff7d6a"
              emptyText={t("noData")}
              priceToBeat={snapshot?.priceToBeat}
              latestPrice={snapshot?.binance.spotPrice}
              round={props.currentRound}
            />
          </div>

          <div className="source-row">
            <SourceBadge label="Binance" source={sourceBinance} nowMs={nowMs} clientRecvTs={props.lastMarketRecvTs} language={language} t={t} marketStreamState={marketStreamState} />
            <SourceBadge label="Chainlink" source={sourceChainlink} nowMs={nowMs} clientRecvTs={props.lastMarketRecvTs} language={language} t={t} marketStreamState={marketStreamState} />
          </div>
        </div>

        <aside className="panel trade-sidebar">
          <div className="section-header">
            <div>
              <p className="eyebrow">{t("orderPanel")}</p>
              <h2>{t("quickTrade")}</h2>
            </div>
            <span className={`status-chip tone-${sourceTone(sourceClob?.state)}`}>
              CLOB 路 {sourceClob?.state ?? "--"}
            </span>
          </div>

          <div className="side-card-grid">
            {(["UP", "DOWN"] as TradeSide[]).map((side) => (
              <button
                key={side}
                className={`side-card side-${side.toLowerCase()} ${selectedSide === side ? "active" : ""}`}
                onClick={() => props.onSelectSide(side)}
              >
                <span>{side}</span>
                <strong>{money(side === "UP" ? snapshot?.upPrice ?? 0 : snapshot?.downPrice ?? 0, 3)}</strong>
                <small>
                  {t("bestBid")}: {decimal(snapshot?.clob.bestBidAskSummary[side].bestBid ?? 0, 3)}
                </small>
              </button>
            ))}
          </div>

          <div className="segmented-control">
            {(["buy", "sell"] as OrderAction[]).map((action) => (
              <button
                key={action}
                className={props.orderAction === action ? "active" : ""}
                onClick={() => props.onOrderActionChange(action)}
              >
                {action === "buy" ? t("buy") : t("sell")}
              </button>
            ))}
          </div>

          <div className="segmented-control">
            {(["market", "limit"] as PaperOrderKind[]).map((kind) => (
              <button
                key={kind}
                className={props.orderKind === kind ? "active" : ""}
                onClick={() => props.onOrderKindChange(kind)}
              >
                {kind === "market" ? t("marketOrder") : t("limitOrder")}
              </button>
            ))}
          </div>

          <div className="mini-portfolio">
            <div>
              <span>{t("currentSideExposure")}</span>
              <strong>{decimal(selectedExposureQty, 4)}</strong>
            </div>
            <div>
              <span>{t("availableQty")}</span>
              <strong>{decimal(selectedAvailableQty, 4)}</strong>
            </div>
            <div>
              <span>{t("currentSideValue")}</span>
              <strong>{money(selectedExposureValue)}</strong>
            </div>
          </div>

          {props.orderAction === "buy" ? (
            <label className="order-input">
              <span>{t("amount")}</span>
              <input value={props.orderAmount} onChange={(event) => props.onAmountChange(event.target.value)} />
            </label>
          ) : (
            <label className="order-input">
              <span>{t("qty")}</span>
              <input value={props.orderQty} onChange={(event) => props.onQtyChange(event.target.value)} />
            </label>
          )}

          {props.orderKind === "limit" ? (
            <label className="order-input">
              <span>{t("limitPrice")}</span>
              <input value={props.limitPrice} onChange={(event) => props.onLimitPriceChange(event.target.value)} />
            </label>
          ) : null}

          <div className="trade-summary">
            <div>
              <span>{t("referenceOdds")}</span>
              <strong>{decimal(tradePrice, 3)}</strong>
            </div>
            <div>
              <span>{t("estimatedQty")}</span>
              <strong>{decimal(estimatedQty, 4)}</strong>
            </div>
            <div>
              <span>{props.orderKind === "limit" && props.orderAction === "buy" ? t("frozenAmount") : t("amount")}</span>
              <strong>{money(estimatedNotional)}</strong>
            </div>
            <div>
              <span>{t("payoutIfWin")}</span>
              <strong>{typeof payoutIfWin === "number" ? money(payoutIfWin) : "--"}</strong>
            </div>
            <div>
              <span>{t("slippageHint")}</span>
              <strong>{orderBook?.bestAsk && orderBook.bestBid ? decimal(orderBook.bestAsk - orderBook.bestBid, 3) : "--"}</strong>
            </div>
            <div>
              <span>{t("backendToFrontend")}</span>
              <strong>{clobTransportLatency.disabled || typeof clobTransportLatency.backendToFrontendLatencyMs !== "number" ? "--" : `${Math.round(clobTransportLatency.backendToFrontendLatencyMs)} ms`}</strong>
            </div>
            <div>
              <span>{localLabel(language, "市场更新年龄", "Market Update Age")}</span>
              <strong>{clobTransportLatency.disabled ? "--" : `${Math.round(clobTransportLatency.marketUpdateAgeMs)} ms`}</strong>
            </div>
            <div className={selectedOrderBookStale ? "tone-warning" : undefined}>
              <span>{t("orderBookAge")}</span>
              <strong>{typeof selectedOrderBookAgeMs === "number" ? `${Math.round(selectedOrderBookAgeMs)} ms` : "--"}</strong>
            </div>
          </div>

          {selectedOrderBookStale ? (
            <div className="inline-warning-banner compact-feedback" role="status">
              <strong>{t("orderBookStaleTitle")}</strong>
              <span>{t("orderBookStaleWarning")}</span>
            </div>
          ) : null}

          <div className="button-stack">
            {balanceWarning ? <div className="inline-error-banner compact-feedback">{balanceWarning}</div> : null}
            <button
              className={`primary-button ${selectedSide === "DOWN" ? "danger-shift" : ""}`}
              disabled={!canTrade || props.tradeBusy}
              title={balanceWarning}
              onClick={props.onPlaceOrder}
            >
              {props.orderAction === "buy"
                ? selectedSide === "UP"
                  ? t("buyUp")
                  : t("buyDown")
                : `${t("sell")} ${selectedSide}`}
            </button>
            <div className="button-row">
              <button className="ghost-button" disabled={!canQuickAction || props.quickBusy || selectedOpenPositions.length === 0} onClick={props.onCloseSide}>
                {t("closeSide")}
              </button>
              <button className="secondary-button" disabled={!canQuickAction || props.quickBusy || selectedOpenPositions.length === 0} onClick={props.onReverseSide}>
                {t("reverseSide")}
              </button>
            </div>
          </div>

          <SourceBadge label="CLOB" source={sourceClob} nowMs={nowMs} clientRecvTs={props.lastMarketRecvTs} language={language} t={t} marketStreamState={marketStreamState} />
        </aside>
      </section>

      <section className="info-grid">
        <div className="panel">
          <div className="section-header">
            <div>
              <p className="eyebrow">{t("orderBook")}</p>
              <h2>{t("depthAndTrades")}</h2>
            </div>
            <div className="section-actions">
              <div className="metric-inline">
                <span>{t("clobDelta")}: {decimal(snapshot?.clob.delta ?? 0, 4)}</span>
                <span>{t("clobVolume")}: {decimal(snapshot?.clob.volume ?? 0, 4)}</span>
              </div>
              <button className="ghost-button compact-button" onClick={() => setOrderBookExpanded((value) => !value)}>
                {orderBookExpanded ? t("collapse") : t("expand")}
              </button>
            </div>
          </div>
          {orderBookExpanded ? (
            <div className="orderbook-columns">
              {(["UP", "DOWN"] as TradeSide[]).map((side) => {
                const book = snapshot?.orderBooks[side];
                return (
                  <div className="book-column" key={side}>
                    <div className="book-column-head">
                      <strong>{side}</strong>
                      <span>
                        {t("buyOneSellOne")}: {decimal(book?.bestBid ?? 0, 3)} / {decimal(book?.bestAsk ?? 0, 3)}
                      </span>
                    </div>
                    <table>
                      <thead>
                        <tr>
                          <th>{t("bid")}</th>
                          <th>{t("qty")}</th>
                          <th>{t("ask")}</th>
                          <th>{t("qty")}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {Array.from({ length: 5 }).map((_, index) => {
                          const bid = book?.bids[index];
                          const ask = book?.asks[index];
                          return (
                            <tr key={`${side}-${index}`}>
                              <td>{bid ? decimal(bid.price, 3) : "--"}</td>
                              <td>{bid ? decimal(bid.qty, 3) : "--"}</td>
                              <td>{ask ? decimal(ask.price, 3) : "--"}</td>
                              <td>{ask ? decimal(ask.qty, 3) : "--"}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                );
              })}
            </div>
          ) : null}
        </div>

        <div className="panel">
          <div className="section-header">
            <div>
              <p className="eyebrow">{localLabel(language, "盘口结果", "Market Results")}</p>
              <h2>{t("recentRounds")}</h2>
            </div>
            <button className="ghost-button compact-button" onClick={() => setHistoryExpanded((value) => !value)}>
              {historyExpanded ? t("collapse") : t("expand")}
            </button>
          </div>
          {historyExpanded ? <div className="stack-panels">
            <div className="history-grid">
              {props.history.map((round) => {
                const openDiff =
                  isBtcReferencePrice(round.polymarketOpenPrice) && typeof round.binanceOpenPrice === "number"
                    ? round.polymarketOpenPrice - round.binanceOpenPrice
                    : undefined;
                const closeDiff =
                  isBtcReferencePrice(round.polymarketClosePrice) && typeof round.binanceClosePrice === "number"
                    ? round.polymarketClosePrice - round.binanceClosePrice
                    : undefined;
                const preview = round.settlementPreview;
                return (
                  <div className="history-card" key={round.id}>
                    <strong>{round.id}</strong>
                    <span className={roundMoveTone(round)}>{roundMoveLabel(round, language)}</span>
                    <small>{dateTimeText(round.startAt)}</small>
                    <small>
                      {localLabel(language, "Polymarket BTC 开/收", "Polymarket BTC O/C")}:{" "}
                      {isBtcReferencePrice(round.polymarketOpenPrice) ? money(round.polymarketOpenPrice) : "--"} /{" "}
                      {isBtcReferencePrice(round.polymarketClosePrice) ? money(round.polymarketClosePrice) : "--"}
                    </small>
                    <small>
                      {localLabel(language, "Binance BTC 开/收", "Binance BTC O/C")}:{" "}
                      {typeof round.binanceOpenPrice === "number" ? money(round.binanceOpenPrice) : "--"} /{" "}
                      {typeof round.binanceClosePrice === "number" ? money(round.binanceClosePrice) : "--"}
                    </small>
                    <small>
                      Δ Open / Δ Close: {typeof openDiff === "number" ? signedMoney(openDiff) : "--"} /{" "}
                      {typeof closeDiff === "number" ? signedMoney(closeDiff) : "--"}
                    </small>
                    <small>
                      {localLabel(language, "状态", "Status")}: {round.status} 路 {round.settlementSource ?? "Gamma"}
                    </small>
                    {preview ? (
                      <div className={`settlement-preview-note tone-${settlementPreviewTone(preview)}`}>
                        <strong>
                          {settlementPreviewLabel(preview, language)}: {settlementPreviewText(preview, language)}
                        </strong>
                        <small>{settlementPreviewHelpText(preview, language)}</small>
                      </div>
                    ) : null}
                    <small>
                      {t("result")}: {round.settledSide ?? "--"}
                    </small>
                    <small className={round.userPnl >= 0 ? "tone-positive" : "tone-negative"}>
                      {t("historyPnl")}: {signedMoney(round.userPnl)}
                    </small>
                  </div>
                );
              })}
            </div>
          </div> : null}
        </div>
      </section>

      <section className="bottom-grid trade-workspace trade-panels-grid">
        <div className="panel">
          <div className="section-header">
            <div>
              <p className="eyebrow">{t("positions")}</p>
              <h2>{currentRoundPositions.length}</h2>
              <span className="section-subtitle">{pageSummaryText(language, tradePositionsPageSafe, currentRoundPositions.length)}</span>
            </div>
            <div className="section-actions">
              <button className="ghost-button compact-button" disabled={tradePositionsPageSafe === 0} onClick={() => setTradePositionsPage((page) => Math.max(page - 1, 0))}>
                {localLabel(language, "上一页", "Previous")}
              </button>
              <button className="ghost-button compact-button" disabled={tradePositionsPageSafe >= tradePositionsTotalPages - 1} onClick={() => setTradePositionsPage((page) => Math.min(page + 1, tradePositionsTotalPages - 1))}>
                {localLabel(language, "下一页", "Next")}
              </button>
              <button className="ghost-button compact-button" onClick={() => setTradePositionsExpanded((value) => !value)}>
                {tradePositionsExpanded ? t("collapse") : t("expand")}
              </button>
            </div>
          </div>
          {tradePositionsExpanded ? (
            <>
              {sellFeedbackMessage ? <div className="inline-error-banner">{sellFeedbackMessage}</div> : null}
              <table>
                <thead>
                  <tr>
                    <th>{t("market")}</th>
                    <th>{t("side")}</th>
                    <th>{t("qty")}</th>
                    <th>{t("lockedQty")}</th>
                    <th>{t("avgPrice")}</th>
                    <th>{t("currentBook")}</th>
                    <th>{t("positionValue")}</th>
                    <th>{t("floatingPnl")}</th>
                    <th>{t("status")}</th>
                    <th>{t("action")}</th>
                  </tr>
                </thead>
                <tbody>
                  {displayPositions.length === 0 ? (
                    <tr>
                      <td colSpan={10}>{t("noData")}</td>
                    </tr>
                  ) : (
                    displayPositions.map((position) => {
                      const sellBlockedReason = getSellBlockedReason({
                        language,
                        position,
                        currentRound: props.currentRound,
                        nowMs,
                        acceptingOrders
                      });
                      const canSellPosition = props.canSell && !sellBlockedReason;
                      const isSelling = props.sellBusyPositionId === position.id;
                      return (
                        <tr key={position.id}>
                          <td>
                            <div className="field-stack">
                              <strong>{position.roundId}</strong>
                            </div>
                          </td>
                          <td><FieldChip label={position.side} tone={sideTone(position.side)} /></td>
                          <td>{decimal(position.qty, 4)}</td>
                          <td>{decimal(position.lockedQty ?? 0, 4)}</td>
                          <td>{decimal(position.averageEntry, 4)}</td>
                          <td>
                            {position.displayStatus === "open"
                              ? `${decimal(position.currentBid ?? 0, 3)} / ${decimal(position.currentAsk ?? 0, 3)}`
                              : "--"}
                            <small className="cell-note">
                              CLOB {position.displayStatus === "open" && position.sourceLatencyMs ? `${Math.round(position.sourceLatencyMs)} ms` : "--"}
                            </small>
                          </td>
                          <td>{money(position.currentValue ?? position.qty * position.currentMark)}</td>
                          <td className={positionDisplayedPnl(position) >= 0 ? "tone-positive" : "tone-negative"}>
                            {signedMoney(positionDisplayedPnl(position))}
                          </td>
                          <td><FieldChip label={positionStatusLabel(position, language)} tone={position.displayStatus === "open" ? "positive" : "neutral"} /></td>
                          <td>
                            {position.displayStatus === "open" ? (
                              <div className="table-action-cell">
                                <button
                                  className="ghost-button compact-button"
                                  disabled={!canSellPosition || isSelling}
                                  title={sellBlockedReason}
                                  onClick={() => props.onSell(position.id)}
                                >
                                  {isSelling ? "Selling..." : t("sell")}
                                </button>
                                {sellBlockedReason ? <small className="cell-note">{sellBlockedReason}</small> : null}
                              </div>
                            ) : (
                              <FieldChip label={positionStatusLabel(position, language)} tone="neutral" />
                            )}
                          </td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </>
          ) : null}
        </div>

        <div className="panel">
          <div className="section-header">
            <div>
              <p className="eyebrow">{localLabel(language, "操作日志", "Operation Log")}</p>
              <h2>{currentRoundOrders.length}</h2>
              <span className="section-subtitle">
                {t("pendingOrders")} {pendingOrders.length} · {pageSummaryText(language, tradeOrdersPageSafe, sortedTradeOrders.length)}
              </span>
            </div>
            <div className="section-actions">
              <button className="ghost-button compact-button" disabled={tradeOrdersPageSafe === 0} onClick={() => setTradeOrdersPage((page) => Math.max(page - 1, 0))}>
                {localLabel(language, "上一页", "Previous")}
              </button>
              <button className="ghost-button compact-button" disabled={tradeOrdersPageSafe >= tradeOrdersTotalPages - 1} onClick={() => setTradeOrdersPage((page) => Math.min(page + 1, tradeOrdersTotalPages - 1))}>
                {localLabel(language, "下一页", "Next")}
              </button>
              <button className="ghost-button compact-button" onClick={() => setTradeOrdersExpanded((value) => !value)}>
                {tradeOrdersExpanded ? t("collapse") : t("expand")}
              </button>
            </div>
          </div>
          {tradeOrdersExpanded ? (
            <table>
              <thead>
                <tr>
                  <th>{t("operationTimeUtc")}</th>
                  <th>{t("type")}</th>
                  <th>{t("market")}</th>
                  <th>{t("action")}</th>
                  <th>{t("side")}</th>
                  <th>{t("amount")}</th>
                  <th>{t("qty")}</th>
                  <th>{t("avgPrice")}</th>
                  <th>{t("status")}</th>
                  <th>{t("action")}</th>
                </tr>
              </thead>
              <tbody>
                {displayOrders.length === 0 ? (
                  <tr>
                    <td colSpan={10}>{t("noData")}</td>
                  </tr>
                ) : (
                  displayOrders.map((order) => (
                    <tr key={order.id} className={order.status === "pending" ? "pending-order-row" : ""}>
                      <td>{dateTimeText(order.createdAt)}</td>
                      <td>
                        <div className="field-stack">
                          <div className="field-chip-row">
                            <FieldChip label={orderKindLabel(order, language)} tone="info" />
                            <FieldChip label={order.resultType ?? orderStatusLabel(order, language)} tone={orderStatusTone(order.status)} />
                          </div>
                        </div>
                      </td>
                      <td>
                        <div className="field-stack">
                          <strong>{order.marketSlug ?? order.roundId}</strong>
                          <small>{order.roundId}</small>
                        </div>
                      </td>
                      <td><FieldChip label={order.action} tone={actionTone(order.action)} /></td>
                      <td><FieldChip label={order.side} tone={sideTone(order.side)} /></td>
                      <td>
                        {money(order.requestedAmountUsdc ?? order.notionalUsdc)}
                        {order.frozenUsdc && order.frozenUsdc > 0 ? <small className="cell-note">{localLabel(language, "冻结", "Frozen")}: {money(order.frozenUsdc)}</small> : null}
                      </td>
                      <td>
                        {decimal(order.filledQty, 4)}
                        {order.status === "pending" ? <small className="cell-note">{t("remainingQty")}: {decimal(order.unfilledQty, 4)}</small> : null}
                        {order.frozenQty && order.frozenQty > 0 ? <small className="cell-note">{t("frozenQty")}: {decimal(order.frozenQty, 4)}</small> : null}
                      </td>
                      <td><OrderExecutionCell order={order} language={language} /></td>
                      <td><FieldChip label={orderStatusLabel(order, language)} tone={orderStatusTone(order.status)} /></td>
                      <td>
                        <div className="table-action-cell">
                          {order.status === "pending" ? (
                            <button
                              className="ghost-button compact-button"
                              disabled={props.cancelBusyOrderId === order.id}
                              onMouseDown={(event) => {
                                if (event.button === 0) {
                                  event.preventDefault();
                                  void props.onCancel(order.id);
                                }
                              }}
                              onClick={() => props.onCancel(order.id)}
                            >
                              {props.cancelBusyOrderId === order.id ? t("loading") : t("cancel")}
                            </button>
                          ) : (
                            <small className="cell-note">{localLabel(language, "撮合", "Match")}: {order.matchLatencyMs} ms</small>
                          )}
                          <button className="ghost-button compact-button" onClick={() => props.onTimeline(order.id)}>
                            {props.timelineBusyOrderId === order.id ? t("loading") : t("timeline")}
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          ) : null}
        </div>
      </section>
    </>
  );
}

function ProfilePage(props: {
  t: (key: string) => string;
  language: Language;
  profile?: ProfileOverview;
  history: HistoryRound[];
  operatedHistory: HistoryRound[];
  positions: PositionRecord[];
  orders: OrderRecord[];
  logs: AuditEvent[];
  onSell: (positionId: string) => Promise<void>;
  onCancel: (orderId: string) => Promise<void>;
  onTimeline: (orderId: string) => Promise<void>;
  onOpenRoundLogs: (item: RoundCalendarItem) => Promise<void>;
  timelineBusyOrderId?: string;
  cancelBusyOrderId?: string;
  selectedRoundLogId?: string;
  roundLogBusyRoundId?: string;
}) {
  const { t, language } = props;
  const [equityWindow, setEquityWindow] = useState<OperatedEquityWindow>(30);
  const equityCurve = buildEquityCurve(props.operatedHistory, props.orders, equityWindow);
  const roundCalendarItems = buildRoundCalendarItems(props.operatedHistory, props.orders);
  const curveMin = equityCurve.reduce((min, point) => Math.min(min, point.cumulativeEquity), Number.POSITIVE_INFINITY);
  const curveMax = equityCurve.reduce((max, point) => Math.max(max, point.cumulativeEquity), Number.NEGATIVE_INFINITY);
  const padding = Number.isFinite(curveMin) && Number.isFinite(curveMax) ? Math.max((curveMax - curveMin) * 0.12, 20) : 20;
  const curveDomainMin = Number.isFinite(curveMin) ? curveMin - padding : 0;
  const curveDomainMax = Number.isFinite(curveMax) ? curveMax + padding : 1;
  const roundsParticipatedTotal = props.profile?.roundsParticipatedTotal ?? props.operatedHistory.length;
  const roundsPerPage = 84;
  const totalCalendarPages = Math.max(Math.ceil(roundCalendarItems.length / roundsPerPage), 1);
  const [calendarPage, setCalendarPage] = useState(totalCalendarPages - 1);
  const [profilePositionsExpanded, setProfilePositionsExpanded] = useState(true);
  const [profileOrdersExpanded, setProfileOrdersExpanded] = useState(true);
  const [profileLogsExpanded, setProfileLogsExpanded] = useState(true);
  const [profilePositionsPage, setProfilePositionsPage] = useState(0);
  const [profileOrdersPage, setProfileOrdersPage] = useState(0);
  const [profileLogsPage, setProfileLogsPage] = useState(0);
  const [expandedPositionRounds, setExpandedPositionRounds] = useState<string[]>([]);
  const [expandedOrderRounds, setExpandedOrderRounds] = useState<string[]>([]);
  const groupedPositions = buildGroupedPositions(props.history, props.positions, props.orders);
  const groupedOrders = buildGroupedOrders(props.history, props.orders);
  const sortedProfileLogs = [...props.logs].sort((left, right) => right.serverRecvTs - left.serverRecvTs);
  const profilePositionsTotalPages = pageCountFor(groupedPositions.length);
  const profileOrdersTotalPages = pageCountFor(groupedOrders.length);
  const profileLogsTotalPages = pageCountFor(sortedProfileLogs.length);
  const profilePositionsPageSafe = clampPage(profilePositionsPage, groupedPositions.length);
  const profileOrdersPageSafe = clampPage(profileOrdersPage, groupedOrders.length);
  const profileLogsPageSafe = clampPage(profileLogsPage, sortedProfileLogs.length);
  const displayProfilePositionGroups = paginateRows(groupedPositions, profilePositionsPageSafe);
  const displayProfileOrderGroups = paginateRows(groupedOrders, profileOrdersPageSafe);
  const displayProfileLogs = paginateRows(sortedProfileLogs, profileLogsPageSafe);

  useEffect(() => {
    setCalendarPage(Math.max(totalCalendarPages - 1, 0));
  }, [totalCalendarPages]);

  useEffect(() => {
    setProfilePositionsPage((page) => clampPage(page, groupedPositions.length));
  }, [groupedPositions.length]);

  useEffect(() => {
    setProfileOrdersPage((page) => clampPage(page, groupedOrders.length));
  }, [groupedOrders.length]);

  useEffect(() => {
    setProfileLogsPage((page) => clampPage(page, sortedProfileLogs.length));
  }, [sortedProfileLogs.length]);

  const pageStartIndex = calendarPage * roundsPerPage;
  const visibleRoundCalendarItems = roundCalendarItems.slice(pageStartIndex, pageStartIndex + roundsPerPage);
  const togglePositionRound = (roundId: string) => {
    setExpandedPositionRounds((roundIds) =>
      roundIds.includes(roundId) ? roundIds.filter((value) => value !== roundId) : [...roundIds, roundId]
    );
  };
  const toggleOrderRound = (roundId: string) => {
    setExpandedOrderRounds((roundIds) =>
      roundIds.includes(roundId) ? roundIds.filter((value) => value !== roundId) : [...roundIds, roundId]
    );
  };
  const equityWindowOptions: OperatedEquityWindow[] = [10, 30, 60, "all"];

  return (
    <>
      <section className="profile-stats">
        <AppMetric label={t("totalEquity")} value={money(props.profile?.totalEquity ?? 0)} />
        <AppMetric label={t("available")} value={money(props.profile?.availableUsdc ?? 0)} />
        <AppMetric label={t("positionValue")} value={money(props.profile?.positionValue ?? 0)} />
        <AppMetric
          label={t("floatingPnl")}
          value={signedMoney(props.profile?.unrealizedPnl ?? 0)}
          tone={(props.profile?.unrealizedPnl ?? 0) >= 0 ? "positive" : "negative"}
        />
        <AppMetric
          label={t("realizedPnl")}
          value={signedMoney(props.profile?.realizedPnlToday ?? 0)}
          tone={(props.profile?.realizedPnlToday ?? 0) >= 0 ? "positive" : "negative"}
        />
        <AppMetric label={t("winRate")} value={compactPercent(props.profile?.winRate ?? 0)} />
        <AppMetric label={t("roundsParticipated")} value={String(roundsParticipatedTotal)} />
      </section>

      <section className="profile-insights">
        <div className="panel profile-curve-panel">
          <div className="section-header">
            <div>
              <p className="eyebrow">{t("equityCurve")}</p>
              <h2>{t("cumulativeEquity")}</h2>
            </div>
            <div className="section-actions">
              <div className="segmented-control compact-segmented">
                {equityWindowOptions.map((option) => (
                  <button
                    key={String(option)}
                    className={equityWindow === option ? "active" : ""}
                    onClick={() => setEquityWindow(option)}
                  >
                    {option === "all" ? localLabel(language, "全部", "All") : localLabel(language, `最近 ${option}`, `Last ${option}`)}
                  </button>
                ))}
              </div>
            </div>
          </div>
          {equityCurve.length === 0 ? (
            <div className="empty-chart-state">{t("noCurveData")}</div>
          ) : (
            <div className="profile-curve-shell">
              <FastEquityCurve points={equityCurve} minValue={curveDomainMin} maxValue={curveDomainMax} />
            </div>
          )}
        </div>
        <div className="panel round-calendar-panel">
          <div className="section-header">
            <div>
              <p className="eyebrow">{t("roundCalendar")}</p>
              <h2>{t("operatedRounds")}</h2>
              <span className="section-subtitle">
                {roundCalendarItems.length} {t("roundsParticipated")}
              </span>
            </div>
            <div className="section-actions">
              <button
                className="ghost-button compact-button"
                disabled={calendarPage === 0}
                onClick={() => setCalendarPage((value) => Math.max(value - 1, 0))}
              >
                {t("previousPage")}
              </button>
              <button
                className="ghost-button compact-button"
                disabled={calendarPage >= totalCalendarPages - 1}
                onClick={() => setCalendarPage((value) => Math.min(value + 1, totalCalendarPages - 1))}
              >
                {t("nextPage")}
              </button>
            </div>
          </div>
          {visibleRoundCalendarItems.length === 0 ? (
            <div className="empty-chart-state">{t("noOperatedRounds")}</div>
          ) : (
            <div className="round-calendar-grid">
              {visibleRoundCalendarItems.map((item) => (
                <button
                  key={item.roundId}
                  className={`round-calendar-tile round-calendar-${item.roundPnl > 0 ? "positive" : item.roundPnl < 0 ? "negative" : "neutral"}${props.selectedRoundLogId === item.roundId ? " active" : ""}`}
                  onClick={() => void props.onOpenRoundLogs(item)}
                  disabled={props.roundLogBusyRoundId === item.roundId}
                >
                  <span className="round-calendar-sequence">
                    {t("roundSequence")} #{item.sequence}
                  </span>
                  <strong>{item.datedLabel}</strong>
                  <small>{item.marketSlug ?? item.roundId}</small>
                  <em>{signedMoney(item.roundPnl)}</em>
                </button>
              ))}
            </div>
          )}
        </div>

      </section>

      <section className="bottom-grid profile-panels-grid">
        <div className="panel">
          <div className="section-header">
            <div>
              <p className="eyebrow">{t("positions")}</p>
              <h2>{groupedPositions.length}</h2>
              <span className="section-subtitle">
                {props.positions.length} {t("positions")} · {pageSummaryText(language, profilePositionsPageSafe, groupedPositions.length)}
              </span>
            </div>
            <div className="section-actions">
              <button className="ghost-button compact-button" disabled={profilePositionsPageSafe === 0} onClick={() => setProfilePositionsPage((page) => Math.max(page - 1, 0))}>
                {t("previousPage")}
              </button>
              <button className="ghost-button compact-button" disabled={profilePositionsPageSafe >= profilePositionsTotalPages - 1} onClick={() => setProfilePositionsPage((page) => Math.min(page + 1, profilePositionsTotalPages - 1))}>
                {t("nextPage")}
              </button>
              <button className="ghost-button compact-button" onClick={() => setProfilePositionsExpanded((value) => !value)}>
                {profilePositionsExpanded ? t("collapse") : t("expand")}
              </button>
            </div>
          </div>
          {profilePositionsExpanded ? (
            <div className="round-group-list">
              {displayProfilePositionGroups.length === 0 ? (
                <div className="empty-round-log-state">{t("noData")}</div>
              ) : (
                displayProfilePositionGroups.map((group) => {
                  const expanded = expandedPositionRounds.includes(group.roundId);
                  return (
                    <div className="round-group-card" key={group.roundId}>
                      <button className="round-group-summary" onClick={() => togglePositionRound(group.roundId)}>
                        <div className="field-stack">
                          <strong>{roundDisplayTitle(group, language)}</strong>
                          <small>{roundSecondaryText(group, language)}</small>
                        </div>
                        <div className="round-group-metrics">
                          <span>{group.positions.length} {t("positions")}</span>
                          <span>{localLabel(language, "持仓价值", "Value")}: {money(group.positionValue)}</span>
                          <span className={group.floatingPnl >= 0 ? "tone-positive" : "tone-negative"}>{signedMoney(group.floatingPnl)}</span>
                          {typeof group.userPnl === "number" ? <span className={group.userPnl >= 0 ? "tone-positive" : "tone-negative"}>{t("historyPnl")}: {signedMoney(group.userPnl)}</span> : null}
                        </div>
                        <FieldChip label={expanded ? t("collapse") : t("expand")} tone="info" />
                      </button>
                      {expanded ? (
                        <table>
                          <thead>
                            <tr>
                              <th>{t("side")}</th>
                              <th>{t("qty")}</th>
                              <th>{t("lockedQty")}</th>
                              <th>{t("avgPrice")}</th>
                              <th>{t("currentBook")}</th>
                              <th>{t("positionValue")}</th>
                              <th>{t("floatingPnl")}</th>
                              <th>{t("status")}</th>
                              <th>{t("action")}</th>
                            </tr>
                          </thead>
                          <tbody>
                            {group.positions.map((position) => (
                              <tr key={position.id}>
                                <td><FieldChip label={position.side} tone={sideTone(position.side)} /></td>
                                <td>{decimal(position.qty, 4)}</td>
                                <td>{decimal(position.lockedQty ?? 0, 4)}</td>
                                <td>{decimal(position.averageEntry, 4)}</td>
                                <td>
                                  {position.displayStatus === "open"
                                    ? `${decimal(position.currentBid ?? 0, 3)} / ${decimal(position.currentAsk ?? 0, 3)}`
                                    : "--"}
                                  <small className="cell-note">CLOB {position.displayStatus === "open" && position.sourceLatencyMs ? `${Math.round(position.sourceLatencyMs)} ms` : "--"}</small>
                                </td>
                                <td>{money(position.currentValue ?? position.qty * position.currentMark)}</td>
                                <td className={positionDisplayedPnl(position) >= 0 ? "tone-positive" : "tone-negative"}>
                                  {signedMoney(positionDisplayedPnl(position))}
                                </td>
                                <td><FieldChip label={positionStatusLabel(position, language)} tone={position.displayStatus === "open" ? "positive" : "neutral"} /></td>
                                <td>
                                  {position.displayStatus === "open" ? (
                                    <button className="ghost-button compact-button" onClick={() => props.onSell(position.id)}>
                                      {t("sell")}
                                    </button>
                                  ) : (
                                    <FieldChip label={positionStatusLabel(position, language)} tone="neutral" />
                                  )}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      ) : null}
                    </div>
                  );
                })
              )}
            </div>
          ) : null}
        </div>

        <div className="panel">
          <div className="section-header">
            <div>
              <p className="eyebrow">{t("orders")}</p>
              <h2>{groupedOrders.length}</h2>
              <span className="section-subtitle">
                {props.orders.length} {t("orders")} · {pageSummaryText(language, profileOrdersPageSafe, groupedOrders.length)}
              </span>
            </div>
            <div className="section-actions">
              <button className="ghost-button compact-button" disabled={profileOrdersPageSafe === 0} onClick={() => setProfileOrdersPage((page) => Math.max(page - 1, 0))}>
                {t("previousPage")}
              </button>
              <button className="ghost-button compact-button" disabled={profileOrdersPageSafe >= profileOrdersTotalPages - 1} onClick={() => setProfileOrdersPage((page) => Math.min(page + 1, profileOrdersTotalPages - 1))}>
                {t("nextPage")}
              </button>
              <button className="ghost-button compact-button" onClick={() => setProfileOrdersExpanded((value) => !value)}>
                {profileOrdersExpanded ? t("collapse") : t("expand")}
              </button>
            </div>
          </div>
          {profileOrdersExpanded ? (
            <div className="round-group-list">
              {displayProfileOrderGroups.length === 0 ? (
                <div className="empty-round-log-state">{t("noData")}</div>
              ) : (
                displayProfileOrderGroups.map((group) => {
                  const expanded = expandedOrderRounds.includes(group.roundId);
                  return (
                    <div className="round-group-card" key={group.roundId}>
                      <button className="round-group-summary" onClick={() => toggleOrderRound(group.roundId)}>
                        <div className="field-stack">
                          <strong>{roundDisplayTitle(group, language)}</strong>
                          <small>{roundSecondaryText(group, language)}</small>
                        </div>
                        <div className="round-group-metrics">
                          <span>{group.orders.length} {t("orders")}</span>
                          <span>{t("pendingOrders")} {group.pendingCount}</span>
                          <span>{t("amount")}: {money(group.notionalUsdc)}</span>
                          {typeof group.userPnl === "number" ? <span className={group.userPnl >= 0 ? "tone-positive" : "tone-negative"}>{t("historyPnl")}: {signedMoney(group.userPnl)}</span> : null}
                        </div>
                        <FieldChip label={expanded ? t("collapse") : t("expand")} tone="info" />
                      </button>
                      {expanded ? (
                        <table>
                          <thead>
                            <tr>
                              <th>{t("operationTimeUtc")}</th>
                              <th>{t("type")}</th>
                              <th>{t("action")}</th>
                              <th>{t("side")}</th>
                              <th>{t("amount")}</th>
                              <th>{t("qty")}</th>
                              <th>{t("avgPrice")}</th>
                              <th>{t("status")}</th>
                              <th>{t("action")}</th>
                            </tr>
                          </thead>
                          <tbody>
                            {group.orders.map((order) => (
                              <tr key={order.id} className={order.status === "pending" ? "pending-order-row" : ""}>
                                <td>{dateTimeText(order.createdAt)}</td>
                                <td>
                                  <div className="field-chip-row">
                                    <FieldChip label={orderKindLabel(order, language)} tone="info" />
                                    <FieldChip label={order.resultType ?? orderStatusLabel(order, language)} tone={orderStatusTone(order.status)} />
                                  </div>
                                </td>
                                <td><FieldChip label={order.action} tone={actionTone(order.action)} /></td>
                                <td><FieldChip label={order.side} tone={sideTone(order.side)} /></td>
                                <td>
                                  {money(order.requestedAmountUsdc ?? order.notionalUsdc)}
                                  {order.frozenUsdc && order.frozenUsdc > 0 ? <small className="cell-note">{localLabel(language, "冻结", "Frozen")}: {money(order.frozenUsdc)}</small> : null}
                                </td>
                                <td>
                                  {decimal(order.filledQty, 4)}
                                  {order.status === "pending" ? <small className="cell-note">{t("remainingQty")}: {decimal(order.unfilledQty, 4)}</small> : null}
                                  {order.frozenQty && order.frozenQty > 0 ? <small className="cell-note">{t("frozenQty")}: {decimal(order.frozenQty, 4)}</small> : null}
                                </td>
                                <td><OrderExecutionCell order={order} language={language} /></td>
                                <td><FieldChip label={orderStatusLabel(order, language)} tone={orderStatusTone(order.status)} /></td>
                                <td>
                                  <div className="table-action-cell">
                                    {order.status === "pending" ? (
                                      <button
                                        className="ghost-button compact-button"
                                        disabled={props.cancelBusyOrderId === order.id}
                                        onMouseDown={(event) => {
                                          if (event.button === 0) {
                                            event.preventDefault();
                                            void props.onCancel(order.id);
                                          }
                                        }}
                                        onClick={() => props.onCancel(order.id)}
                                      >
                                        {props.cancelBusyOrderId === order.id ? t("loading") : t("cancel")}
                                      </button>
                                    ) : (
                                      <small className="cell-note">{localLabel(language, "撮合", "Match")}: {order.matchLatencyMs} ms</small>
                                    )}
                                    <button className="ghost-button compact-button" onClick={() => props.onTimeline(order.id)}>
                                      {props.timelineBusyOrderId === order.id ? t("loading") : t("timeline")}
                                    </button>
                                  </div>
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      ) : null}
                    </div>
                  );
                })
              )}
            </div>
          ) : null}
        </div>

        <div className="panel profile-panel-full">
          <div className="section-header">
            <div>
              <p className="eyebrow">{t("logs")}</p>
              <h2>{props.logs.length}</h2>
              <span className="section-subtitle">{pageSummaryText(language, profileLogsPageSafe, sortedProfileLogs.length)}</span>
            </div>
            <div className="section-actions">
              <button className="ghost-button compact-button" disabled={profileLogsPageSafe === 0} onClick={() => setProfileLogsPage((page) => Math.max(page - 1, 0))}>
                {t("previousPage")}
              </button>
              <button className="ghost-button compact-button" disabled={profileLogsPageSafe >= profileLogsTotalPages - 1} onClick={() => setProfileLogsPage((page) => Math.min(page + 1, profileLogsTotalPages - 1))}>
                {t("nextPage")}
              </button>
              <button className="ghost-button compact-button" onClick={() => setProfileLogsExpanded((value) => !value)}>
                {profileLogsExpanded ? t("collapse") : t("expand")}
              </button>
            </div>
          </div>
          {profileLogsExpanded ? (
            <table>
              <thead>
                <tr>
                  <th>{t("time")}</th>
                  <th>{t("module")}</th>
                  <th>{t("actionType")}</th>
                  <th>{t("status")}</th>
                  <th>{t("message")}</th>
                </tr>
              </thead>
              <tbody>
                {displayProfileLogs.length === 0 ? (
                  <tr>
                    <td colSpan={5}>{t("noData")}</td>
                  </tr>
                ) : (
                  displayProfileLogs.map((log) => (
                    <tr key={log.eventId}>
                      <td>{dateTimeText(log.serverRecvTs)}</td>
                      <td>
                        <div className="field-stack">
                          <FieldChip label={log.moduleName} tone="info" />
                          <small>{log.category}</small>
                        </div>
                      </td>
                      <td><FieldChip label={log.actionType} tone={actionTone(log.actionType)} /></td>
                      <td><FieldChip label={log.actionStatus} tone={auditStatusTone(log.actionStatus)} /></td>
                      <td>
                        <div className="field-stack">
                          <strong>{log.resultMessage}</strong>
                          <small>{log.traceId}</small>
                        </div>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          ) : null}
        </div>
      </section>
    </>
  );
}

function LogSearchPage(props: { t: (key: string) => string; token: string; me: PublicUser; canExport: boolean }) {
  const { t, token, me } = props;
  const [filters, setFilters] = useState<Record<string, string>>({ system: "all", limit: "100" });
  const [logs, setLogs] = useState<UnifiedLogRow[]>([]);
  const [nextCursor, setNextCursor] = useState<string>();
  const [users, setUsers] = useState<PublicUser[]>([]);
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
      ? localLabel(language, "审计", "Audit")
      : system === "training"
        ? localLabel(language, "交易", "Trading")
        : localLabel(language, "撮合", "Matching");
  const logGroupLabel = (value?: string) => {
    if (value === "operation") return localLabel(language, "操作审计", "Operation Audit");
    if (value === "settlement") return localLabel(language, "结算审计", "Settlement Audit");
    if (value === "market_latency") return localLabel(language, "市场数据延迟", "Market Data Latency");
    if (value === "system_latency") return localLabel(language, "系统/链路延迟", "System/Link Latency");
    if (value === "matching_action") return localLabel(language, "交易撮合动作", "Matching Actions");
    return value ?? "--";
  };
  const latencySourceLabel = (value?: string) => {
    if (value === "binance") return "Binance";
    if (value === "chainlink") return "Chainlink";
    if (value === "clob") return localLabel(language, "Polymarket盘口(CLOB)", "Polymarket Book (CLOB)");
    if (value === "system") return localLabel(language, "系统", "System");
    return value ?? "--";
  };
  const matchingKindLabel = (value?: string) =>
    value === "action"
      ? localLabel(language, "交易撮合动作", "Matching Actions")
      : value === "engine"
        ? localLabel(language, "撮合引擎事件", "Matching Engine Events")
        : value ?? "--";
  const latencySummary = (log: UnifiedLogRow) => {
    if (!log.latencyPhaseMetrics) {
      return log.resultMessage ?? log.resultCode ?? "--";
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
      {error ? <div className="inline-error-banner">{error}</div> : null}
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
            {localLabel(language, "用户", "User")}
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
          <input value={filters.roundId ?? ""} onChange={(event) => updateFilters({ roundId: event.target.value })} />
        </label>
        <label>
          {t("orderId")}
          <input value={filters.orderId ?? ""} onChange={(event) => updateFilters({ orderId: event.target.value })} />
        </label>
        <label>
          traceId
          <input value={filters.traceId ?? ""} onChange={(event) => updateFilters({ traceId: event.target.value })} />
        </label>
        <label>
          {t("actionType")}
          <select value={filters.actionType ?? ""} onChange={(event) => updateFilters({ actionType: event.target.value })}>
            <option value="">{t("all")}</option>
            {selectedActionOptions.map((actionType) => (
              <option key={actionType} value={actionType}>
                {actionType}
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
                  {category}
                </option>
              ))}
            </select>
          </label>
          <label>
            {localLabel(language, "日志分组", "Log Group")}
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
            {localLabel(language, "延迟来源", "Latency Source")}
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
            {localLabel(language, "链路状态", "Connection State")}
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
            {localLabel(language, "延迟阶段", "Latency Phase")}
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
            {localLabel(language, "最小延迟(ms)", "Min Latency (ms)")}
            <input type="number" value={filters.latencyMinMs ?? ""} onChange={(event) => updateFilters({ latencyMinMs: event.target.value })} disabled={selectedSystem === "training" || selectedSystem === "matching"} />
          </label>
          <label>
            {localLabel(language, "最大延迟(ms)", "Max Latency (ms)")}
            <input type="number" value={filters.latencyMaxMs ?? ""} onChange={(event) => updateFilters({ latencyMaxMs: event.target.value })} disabled={selectedSystem === "training" || selectedSystem === "matching"} />
          </label>
          <label>
            {localLabel(language, "撮合子类型", "Matching Kind")}
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
            {localLabel(language, "精确 actionType", "Exact actionType")}
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
          {showLogInfo ? localLabel(language, "收起日志说明", "Hide Log Info") : localLabel(language, "日志说明/字段说明", "Log Info / Fields")}
        </button>
        <button className="ghost-button compact-button" onClick={() => setShowMoreFilters((value) => !value)}>
          {showMoreFilters ? localLabel(language, "收起筛选", "Fewer Filters") : localLabel(language, "更多筛选", "More Filters")}
        </button>
      </div>
      {showLogInfo ? (
        <div className="log-info-panel">
          <div>
            <strong>{selectedSystem === "all" ? localLabel(language, "全部日志", "All Logs") : logSystemLabel(selectedSystem as UnifiedLogRow["system"])}</strong>
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
              <span>{localLabel(language, "主要字段", "Main Fields")}</span>
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
            <th>{localLabel(language, "系统", "System")}</th>
            <th>{t("userRole")}</th>
            <th>{t("round")}</th>
            <th>{t("actionType")}</th>
            <th>{t("status")}</th>
            <th>trace / order</th>
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
                      <small>{log.matchingLogKind ? matchingKindLabel(log.matchingLogKind) : (log.logGroup ? logGroupLabel(log.logGroup) : log.category ?? log.eventType ?? "--")}</small>
                    </div>
                  </td>
                  <td>
                    <div className="log-user-cell">
                      <span>{log.username ?? log.userId ?? "--"} / {log.role ?? "--"}</span>
                      {isDisabledUserLog ? <FieldChip label={localLabel(language, "停用", "Disabled")} tone="negative" /> : null}
                    </div>
                  </td>
                  <td>
                    <div className="field-stack">
                      <span>{log.roundId ?? "--"}</span>
                      <small>{log.marketId ?? log.bookKey ?? "--"}</small>
                    </div>
                  </td>
                  <td>{log.actionType}</td>
                  <td>{log.actionStatus ?? "--"}</td>
                  <td>
                    <div className="field-stack">
                      <span>{log.traceId ?? "--"}</span>
                      <small>{log.orderId ?? log.positionId ?? "--"}</small>
                    </div>
                  </td>
                  <td>
                    {log.logGroup === "market_latency" || log.logGroup === "system_latency" ? latencySummary(log) : log.resultMessage ?? log.resultCode ?? "--"}
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
            {busy ? t("loading") : localLabel(language, "加载更多", "Load More")}
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
  t: (key: string) => string;
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
      ? localLabel(language, "审计日志", "Audit Logs")
      : system === "training"
        ? localLabel(language, "交易日志", "Trading Logs")
        : localLabel(language, "撮合事件", "Matching Events");

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
        setMessage(localLabel(language, "已取消保存。", "Save was canceled."));
        return;
      }
      setMessage(
        result.filePath
          ? localLabel(language, `已保存到 ${result.filePath}`, `Saved to ${result.filePath}`)
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
            <p className="eyebrow">{localLabel(language, "日志导出", "Log Export")}</p>
            <h2>{localLabel(language, "导出向导", "Export Wizard")}</h2>
          </div>
          <button className="ghost-button compact-button" onClick={props.onClose}>
            {t("close")}
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
          <strong>{localLabel(language, "日志类型", "Log Systems")}</strong>
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
            <strong>{localLabel(language, "人员范围", "User Scope")}</strong>
            <button className="ghost-button compact-button" onClick={() => setSelectedUserIds([])}>
              {localLabel(language, "当前权限全部", "All In Scope")}
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
            {localLabel(language, "日志分组", "Log Group")}
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
            {localLabel(language, "延迟来源", "Latency Source")}
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
            {localLabel(language, "链路状态", "Connection State")}
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
            {localLabel(language, "延迟阶段", "Latency Phase")}
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
            {localLabel(language, "最小延迟(ms)", "Min Latency (ms)")}
            <input type="number" value={form.latencyMinMs} onChange={(event) => updateForm("latencyMinMs", event.target.value)} />
          </label>
          <label>
            {localLabel(language, "最大延迟(ms)", "Max Latency (ms)")}
            <input type="number" value={form.latencyMaxMs} onChange={(event) => updateForm("latencyMaxMs", event.target.value)} />
          </label>
          <label>
            {localLabel(language, "撮合子类型", "Matching Kind")}
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
            {t("orderId")}
            <input value={form.orderId} onChange={(event) => updateForm("orderId", event.target.value)} />
          </label>
          <label>
            positionId
            <input value={form.positionId} onChange={(event) => updateForm("positionId", event.target.value)} />
          </label>
          <label>
            traceId
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
                ? localLabel(language, "选择保存位置并导出", "Choose Save Location and Export")
                : localLabel(language, "浏览器下载导出", "Download Export")}
          </button>
          <button className="ghost-button" disabled={props.busy} onClick={props.onClose}>
            {t("cancel")}
          </button>
        </div>
      </section>
    </div>
  );
}

function BulkUserDialog(props: {
  t: (key: string) => string;
  token: string;
  language: Language;
  users: PublicUser[];
  busy: boolean;
  setBusy: (value: boolean) => void;
  onError: (message?: string) => void;
  onCreated: () => Promise<void>;
  onClose: () => void;
}) {
  const { t, token, language } = props;
  const template =
    "username,password,displayName,role,language,seniorTesterId,availableUsdc\n" +
    "tester_new_01,ChangeMe123,Tester New 01,Tester,zh-CN,,10000";
  const [sourceText, setSourceText] = useState(template);
  const [localError, setLocalError] = useState<string>();
  const [result, setResult] = useState<BulkCreateUsersResult>();
  const parsed = useMemo(() => parseBulkUserText(sourceText, props.users, language), [sourceText, props.users, language]);
  const hasErrors = parsed.errors.length > 0 || parsed.rows.some((row) => row.errors.length > 0);

  const handleFile = (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.currentTarget.files?.[0];
    if (!file) {
      return;
    }
    file
      .text()
      .then((text) => {
        setSourceText(text);
        setResult(undefined);
        setLocalError(undefined);
      })
      .catch(() => {
        setLocalError(localLabel(language, "读取文件失败。", "Failed to read the file."));
      });
  };

  const submit = async () => {
    if (hasErrors || parsed.validUsers.length === 0) {
      setLocalError(localLabel(language, "请先修正导入内容中的错误。", "Fix import errors before creating users."));
      return;
    }
    try {
      props.setBusy(true);
      props.onError(undefined);
      setLocalError(undefined);
      const nextResult = await api.bulkCreateUsers(token, parsed.validUsers);
      setResult(nextResult);
      if (nextResult.failed.length === 0) {
        await props.onCreated();
      }
    } catch (bulkError) {
      const message = bulkError instanceof Error ? bulkError.message : "Bulk create failed.";
      setLocalError(message);
      props.onError(message);
    } finally {
      props.setBusy(false);
    }
  };

  return (
    <div className="modal-backdrop">
      <section className="panel bulk-user-dialog" onClick={(event) => event.stopPropagation()}>
        <div className="section-header">
          <div>
            <p className="eyebrow">{localLabel(language, "批量注册", "Bulk Registration")}</p>
            <h2>{localLabel(language, "CSV / TSV 导入账号", "CSV / TSV User Import")}</h2>
          </div>
          <button className="ghost-button compact-button" onClick={props.onClose}>
            {t("close")}
          </button>
        </div>
        <div className="dialog-section">
          <div className="button-row fit-actions">
            <label className="file-import-button">
              {localLabel(language, "选择 CSV/TSV 文件", "Choose CSV/TSV File")}
              <input type="file" accept=".csv,.tsv,text/csv,text/tab-separated-values,text/plain" onChange={handleFile} />
            </label>
            <button
              className="ghost-button compact-button"
              onClick={() => {
                setSourceText(template);
                setResult(undefined);
                setLocalError(undefined);
              }}
            >
              {localLabel(language, "填入模板", "Use Template")}
            </button>
          </div>
          <small className="muted-line">
            {localLabel(
              language,
              "必填表头：username,password；可选：displayName,role,language,seniorTesterId,availableUsdc。",
              "Required headers: username,password; optional: displayName,role,language,seniorTesterId,availableUsdc."
            )}
          </small>
        </div>
        <div className="dialog-form">
          <label>
            {localLabel(language, "粘贴 CSV/TSV 文本", "Paste CSV/TSV Text")}
            <textarea
              value={sourceText}
              onChange={(event) => {
                setSourceText(event.target.value);
                setResult(undefined);
                setLocalError(undefined);
              }}
            />
          </label>
        </div>
        {localError ? <div className="inline-error-banner">{localError}</div> : null}
        {parsed.errors.length > 0 ? (
          <div className="inline-error-banner">{parsed.errors.join(" ")}</div>
        ) : null}
        {result ? (
          <div className={result.failed.length ? "inline-error-banner" : "inline-info-banner"}>
            {localLabel(
              language,
              `创建 ${result.created.length} 个，失败 ${result.failed.length} 个。`,
              `Created ${result.created.length}, failed ${result.failed.length}.`
            )}
            {result.failed.length ? ` ${result.failed.map((item) => `#${item.rowNumber}: ${item.error}`).join("; ")}` : ""}
          </div>
        ) : null}
        <div className="dialog-table-shell">
          <table>
            <thead>
              <tr>
                <th>#</th>
                <th>{t("username")}</th>
                <th>{localLabel(language, "姓名", "Display Name")}</th>
                <th>{t("role")}</th>
                <th>{t("language")}</th>
                <th>{localLabel(language, "直属组长", "Senior Tester")}</th>
                <th>{t("available")}</th>
                <th>{localLabel(language, "校验", "Validation")}</th>
              </tr>
            </thead>
            <tbody>
              {parsed.rows.length === 0 ? (
                <tr>
                  <td colSpan={8}>{t("noData")}</td>
                </tr>
              ) : (
                parsed.rows.map((row) => (
                  <tr key={row.rowNumber}>
                    <td>{row.rowNumber}</td>
                    <td>{row.username || "--"}</td>
                    <td>{row.displayName || "--"}</td>
                    <td>{row.role ?? "Tester"}</td>
                    <td>{row.language ?? "zh-CN"}</td>
                    <td>{row.seniorTesterId ?? "--"}</td>
                    <td>{typeof row.availableUsdc === "number" ? money(row.availableUsdc) : localLabel(language, "默认", "Default")}</td>
                    <td>
                      {row.errors.length ? (
                        <span className="tone-negative">{row.errors.join(" ")}</span>
                      ) : (
                        <FieldChip label={localLabel(language, "可创建", "Ready")} tone="positive" />
                      )}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
        <div className="button-row dialog-actions">
          <button className="secondary-button" disabled={props.busy || hasErrors || parsed.validUsers.length === 0} onClick={submit}>
            {props.busy ? t("loading") : localLabel(language, `创建 ${parsed.validUsers.length} 个账号`, `Create ${parsed.validUsers.length} Users`)}
          </button>
          <button className="ghost-button" disabled={props.busy} onClick={props.onClose}>
            {t("cancel")}
          </button>
        </div>
      </section>
    </div>
  );
}

function UserManagementPage(props: {
  t: (key: string) => string;
  token: string;
  me: PublicUser;
  language: Language;
  onProfileRefresh: () => Promise<void>;
}) {
  const { token, me, language } = props;
  const [users, setUsers] = useState<PublicUser[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const [balanceDialog, setBalanceDialog] = useState<{ user: PublicUser; amount: string }>();
  const [passwordDialog, setPasswordDialog] = useState<{
    user: PublicUser;
    mode: "self" | "reset";
    currentPassword: string;
    password: string;
    confirmPassword: string;
  }>();
  const [bulkDialogOpen, setBulkDialogOpen] = useState(false);
  const [form, setForm] = useState({
    username: "",
    password: "",
    displayName: "",
    role: "Tester" as Role,
    language: "zh-CN" as Language,
    seniorTesterId: "",
    availableUsdc: "10000"
  });
  const isAdmin = me.role === "Admin";
  const canBulkCreate = me.permissionCodes.includes("users:bulk-create");

  const loadUsers = async () => {
    try {
      setBusy(true);
      setError(undefined);
      setUsers(await api.getUsers(token));
    } catch (loadError) {
      setError(loadError instanceof Error ? loadError.message : "Load users failed.");
    } finally {
      setBusy(false);
    }
  };

  useEffect(() => {
    void loadUsers();
  }, []);

  const seniorOptions = users.filter((user) => user.role === "Senior Tester" && user.isActive);
  const canManageTarget = (user: PublicUser) =>
    isAdmin || (me.role === "Senior Tester" && user.role === "Tester" && user.seniorTesterId === me.id);
  const canSetBalance = (user: PublicUser) => canManageTarget(user) || (me.role === "Senior Tester" && user.id === me.id);

  const createUser = async () => {
    try {
      setBusy(true);
      setError(undefined);
      await api.createUser(token, {
        username: form.username,
        password: form.password,
        displayName: form.displayName,
        role: form.role,
        language: form.language,
        seniorTesterId: form.role === "Tester" ? form.seniorTesterId || undefined : undefined,
        availableUsdc: Number(form.availableUsdc || 0)
      });
      setForm({
        username: "",
        password: "",
        displayName: "",
        role: "Tester",
        language: "zh-CN",
        seniorTesterId: "",
        availableUsdc: "10000"
      });
      await loadUsers();
    } catch (createError) {
      setError(createError instanceof Error ? createError.message : "Create user failed.");
    } finally {
      setBusy(false);
    }
  };

  const disableUser = async (user: PublicUser) => {
    const ok = window.confirm(localLabel(language, `确认停用账号 ${user.username}？`, `Disable account ${user.username}?`));
    if (!ok) {
      return;
    }
    try {
      setBusy(true);
      setError(undefined);
      await api.disableUser(token, user.id);
      await loadUsers();
    } catch (disableError) {
      setError(disableError instanceof Error ? disableError.message : "Disable user failed.");
    } finally {
      setBusy(false);
    }
  };

  const enableUser = async (user: PublicUser) => {
    const ok = window.confirm(localLabel(language, `确认恢复账号 ${user.username}？`, `Restore account ${user.username}?`));
    if (!ok) {
      return;
    }
    try {
      setBusy(true);
      setError(undefined);
      await api.enableUser(token, user.id);
      await loadUsers();
    } catch (enableError) {
      setError(enableError instanceof Error ? enableError.message : "Restore user failed.");
    } finally {
      setBusy(false);
    }
  };

  const submitPasswordReset = async () => {
    if (!passwordDialog) {
      return;
    }
    if (passwordDialog.password !== passwordDialog.confirmPassword) {
      setError(localLabel(language, "两次输入的新密码不一致。", "The new password confirmation does not match."));
      return;
    }
    try {
      setBusy(true);
      setError(undefined);
      const payload = {
        currentPassword: passwordDialog.currentPassword,
        password: passwordDialog.password,
        confirmPassword: passwordDialog.confirmPassword
      };
      if (passwordDialog.mode === "self") {
        await api.changeMyPassword(token, payload);
        await props.onProfileRefresh();
      } else {
        await api.resetUserPassword(token, passwordDialog.user.id, payload);
      }
      setPasswordDialog(undefined);
      await loadUsers();
    } catch (resetError) {
      setError(resetError instanceof Error ? resetError.message : "Reset password failed.");
    } finally {
      setBusy(false);
    }
  };

  const submitBalance = async () => {
    if (!balanceDialog) {
      return;
    }
    const amount = Number(balanceDialog.amount);
    if (!Number.isFinite(amount) || amount < 0) {
      setError(localLabel(language, "请输入有效金额。", "Enter a valid amount."));
      return;
    }
    try {
      setBusy(true);
      setError(undefined);
      const targetUserId = balanceDialog.user.id;
      await api.setUserBalance(token, targetUserId, amount);
      setBalanceDialog(undefined);
      await loadUsers();
      if (targetUserId === me.id) {
        await props.onProfileRefresh();
      }
    } catch (balanceError) {
      setError(balanceError instanceof Error ? balanceError.message : "Set balance failed.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="panel log-search-panel">
      <div className="section-header">
        <div>
          <p className="eyebrow">{localLabel(language, "用户管理", "Users")}</p>
          <h2>{users.length}</h2>
        </div>
        <div className="button-row fit-actions">
          {canBulkCreate ? (
            <button
              className="secondary-button"
              disabled={busy}
              onClick={() => {
                setError(undefined);
                setBulkDialogOpen(true);
              }}
            >
              {localLabel(language, "批量注册", "Bulk Register")}
            </button>
          ) : null}
          <button
            className="secondary-button"
            disabled={busy}
            onClick={() => {
              setError(undefined);
              setPasswordDialog({ user: me, mode: "self", currentPassword: "", password: "", confirmPassword: "" });
            }}
          >
            {localLabel(language, "修改个人密码", "Change My Password")}
          </button>
          <button className="secondary-button" onClick={loadUsers} disabled={busy}>
            {busy ? props.t("loading") : props.t("search")}
          </button>
        </div>
      </div>
      {error ? <div className="inline-error-banner">{error}</div> : null}

      {isAdmin ? (
        <div className="filter-grid user-create-grid">
          <label>
            {props.t("username")}
            <input value={form.username} onChange={(event) => setForm({ ...form, username: event.target.value })} />
          </label>
          <label>
            {props.t("password")}
            <input value={form.password} onChange={(event) => setForm({ ...form, password: event.target.value })} />
          </label>
          <label>
            {localLabel(language, "姓名", "Display Name")}
            <input value={form.displayName} onChange={(event) => setForm({ ...form, displayName: event.target.value })} />
          </label>
          <label>
            {props.t("role")}
            <select value={form.role} onChange={(event) => setForm({ ...form, role: event.target.value as Role })}>
              {(["Tester", "Senior Tester", "Test Engineer", "Admin"] as Role[]).map((role) => (
                <option key={role} value={role}>
                  {role}
                </option>
              ))}
            </select>
          </label>
          <label>
            {props.t("language")}
            <select value={form.language} onChange={(event) => setForm({ ...form, language: event.target.value as Language })}>
              <option value="zh-CN">简体中文</option>
              <option value="en-US">English</option>
            </select>
          </label>
          <label>
            {localLabel(language, "直属组长", "Senior Tester")}
            <select value={form.seniorTesterId} onChange={(event) => setForm({ ...form, seniorTesterId: event.target.value })} disabled={form.role !== "Tester"}>
              <option value="">{props.t("all")}</option>
              {seniorOptions.map((user) => (
                <option key={user.id} value={user.id}>
                  {user.username}
                </option>
              ))}
            </select>
          </label>
          <label>
            {props.t("available")}
            <input value={form.availableUsdc} onChange={(event) => setForm({ ...form, availableUsdc: event.target.value })} />
          </label>
          <button className="primary-button user-create-button" disabled={busy} onClick={createUser}>
            {localLabel(language, "创建账号", "Create")}
          </button>
        </div>
      ) : null}

      <table>
        <thead>
          <tr>
            <th>{props.t("username")}</th>
            <th>{localLabel(language, "姓名", "Display Name")}</th>
            <th>{props.t("role")}</th>
            <th>{props.t("status")}</th>
            <th>{localLabel(language, "直属组长", "Senior Tester")}</th>
            <th>{props.t("available")}</th>
            <th>{props.t("action")}</th>
          </tr>
        </thead>
        <tbody>
          {users.length === 0 ? (
            <tr>
              <td colSpan={7}>{props.t("noData")}</td>
            </tr>
          ) : (
            users.map((user) => {
              const senior = users.find((candidate) => candidate.id === user.seniorTesterId);
              return (
                <tr key={user.id}>
                  <td>{user.username}</td>
                  <td>{user.displayName}</td>
                  <td>{user.role}</td>
                  <td>
                    <FieldChip
                      label={user.isActive ? localLabel(language, "启用", "Active") : localLabel(language, "停用", "Disabled")}
                      tone={user.isActive ? "positive" : "negative"}
                    />
                  </td>
                  <td>{senior?.username ?? "--"}</td>
                  <td>{money(user.availableUsdc)}</td>
                  <td>
                    <div className="table-action-cell">
                      {canSetBalance(user) ? (
                        <button
                          className="ghost-button compact-button"
                          disabled={busy}
                          onClick={() => {
                            setError(undefined);
                            setBalanceDialog({ user, amount: String(user.availableUsdc) });
                          }}
                        >
                          {localLabel(language, "重置余额", "Balance")}
                        </button>
                      ) : null}
                      {canManageTarget(user) ? (
                        <button
                          className="ghost-button compact-button"
                          disabled={busy}
                          onClick={() => {
                            setError(undefined);
                            setPasswordDialog({ user, mode: "reset", currentPassword: "", password: "", confirmPassword: "" });
                          }}
                        >
                          {localLabel(language, "重置密码", "Password")}
                        </button>
                      ) : null}
                      {canManageTarget(user) && user.id !== me.id ? (
                        user.isActive ? (
                          <button className="ghost-button compact-button" disabled={busy} onClick={() => disableUser(user)}>
                            {localLabel(language, "停用", "Disable")}
                          </button>
                        ) : (
                          <button className="ghost-button compact-button" disabled={busy} onClick={() => enableUser(user)}>
                            {localLabel(language, "恢复", "Restore")}
                          </button>
                        )
                      ) : null}
                    </div>
                  </td>
                </tr>
              );
            })
          )}
        </tbody>
      </table>
      {balanceDialog ? (
        <div className="modal-backdrop">
          <div className="panel user-action-dialog">
            <div className="section-header">
              <div>
                <p className="eyebrow">{localLabel(language, "重置余额", "Balance")}</p>
                <h2>{balanceDialog.user.username}</h2>
              </div>
              <button className="ghost-button compact-button" onClick={() => setBalanceDialog(undefined)}>
                {props.t("close")}
              </button>
            </div>
            {error ? <div className="inline-error-banner">{error}</div> : null}
            <div className="dialog-form">
              <label>
                {props.t("available")}
                <input
                  value={balanceDialog.amount}
                  onChange={(event) => setBalanceDialog({ ...balanceDialog, amount: event.target.value })}
                />
              </label>
              <div className="button-row">
                <button className="secondary-button" disabled={busy} onClick={submitBalance}>
                  {localLabel(language, "确认重置", "Confirm")}
                </button>
                <button className="ghost-button" disabled={busy} onClick={() => setBalanceDialog(undefined)}>
                  {props.t("cancel")}
                </button>
              </div>
            </div>
          </div>
        </div>
      ) : null}
      {passwordDialog ? (
        <div className="modal-backdrop">
          <div className="panel user-action-dialog">
            <div className="section-header">
              <div>
                <p className="eyebrow">{localLabel(language, "身份验证", "Identity Check")}</p>
                <h2>
                  {passwordDialog.mode === "self"
                    ? localLabel(language, "修改个人密码", "Change My Password")
                    : localLabel(language, "重置密码", "Reset Password")}{" "}
                  / {passwordDialog.user.username}
                </h2>
              </div>
              <button className="ghost-button compact-button" onClick={() => setPasswordDialog(undefined)}>
                {props.t("close")}
              </button>
            </div>
            {error ? <div className="inline-error-banner">{error}</div> : null}
            <div className="dialog-form">
              <label>
                {localLabel(language, "当前操作人密码", "Current Operator Password")}
                <input
                  type="password"
                  value={passwordDialog.currentPassword}
                  onChange={(event) => setPasswordDialog({ ...passwordDialog, currentPassword: event.target.value })}
                />
              </label>
              <label>
                {localLabel(language, "新密码", "New Password")}
                <input
                  type="password"
                  value={passwordDialog.password}
                  onChange={(event) => setPasswordDialog({ ...passwordDialog, password: event.target.value })}
                />
              </label>
              <label>
                {localLabel(language, "确认新密码", "Confirm New Password")}
                <input
                  type="password"
                  value={passwordDialog.confirmPassword}
                  onChange={(event) => setPasswordDialog({ ...passwordDialog, confirmPassword: event.target.value })}
                />
              </label>
              <div className="button-row">
                <button className="secondary-button" disabled={busy} onClick={submitPasswordReset}>
                  {localLabel(language, "确认重置", "Confirm")}
                </button>
                <button className="ghost-button" disabled={busy} onClick={() => setPasswordDialog(undefined)}>
                  {props.t("cancel")}
                </button>
              </div>
            </div>
          </div>
        </div>
      ) : null}
      {bulkDialogOpen ? (
        <BulkUserDialog
          t={props.t}
          token={token}
          language={language}
          users={users}
          busy={busy}
          setBusy={setBusy}
          onError={setError}
          onCreated={loadUsers}
          onClose={() => setBulkDialogOpen(false)}
        />
      ) : null}
    </section>
  );
}

function TimelineDialog(props: { t: (key: string) => string; timeline: TradeTimeline; onClose: () => void }) {
  const { t, timeline } = props;
  const rows = [
    ...timeline.auditEvents.map((event) => ({
      id: event.eventId,
      ts: event.serverRecvTs,
      kind: "audit",
      action: event.actionType,
      status: event.actionStatus,
      message: event.resultMessage,
      detail: event.details
    })),
    ...timeline.behaviorLogs.map((log) => ({
      id: log.logId,
      ts: log.timestampMs,
      kind: "behavior",
      action: log.actionType,
      status: log.actionStatus,
      message: log.orderId ?? log.traceId ?? "",
      detail: log.contextJson
    }))
  ].sort((left, right) => left.ts - right.ts);

  return (
    <div className="modal-backdrop" onClick={props.onClose}>
      <section className="panel timeline-dialog" onClick={(event) => event.stopPropagation()}>
        <div className="section-header">
          <div>
            <p className="eyebrow">{t("timeline")}</p>
            <h2>{timeline.order.id}</h2>
          </div>
          <button className="ghost-button" onClick={props.onClose}>
            {t("close")}
          </button>
        </div>
        <div className="timeline-summary">
          <AppMetric label={t("status")} value={timeline.order.status} />
          <AppMetric label={t("market")} value={timeline.order.marketSlug ?? timeline.order.roundId} />
          <AppMetric label={t("avgPrice")} value={timeline.order.avgFillPrice ? decimal(timeline.order.avgFillPrice, 4) : "--"} />
          <AppMetric label={t("matchingReplay")} value={String(timeline.matchingReplay?.steps.length ?? 0)} />
        </div>
        <div className="timeline-list">
          {rows.map((row) => (
            <details key={`${row.kind}-${row.id}`} className="timeline-item">
              <summary>
                <span>{dateTimeText(row.ts)}</span>
                <strong>{row.kind} / {row.action}</strong>
                <em>{row.status}</em>
              </summary>
              <p>{row.message}</p>
              <pre className="json-block">{jsonPreview(row.detail)}</pre>
            </details>
          ))}
        </div>
      </section>
    </div>
  );
}

function RoundLogDialog(props: {
  t: (key: string) => string;
  state: RoundLogDialogState;
  onClose: () => void;
}) {
  const { t, state } = props;
  const activityRows = [
    ...state.logs.map((log) => ({
      id: log.eventId,
      ts: log.serverRecvTs,
      source: "Audit",
      title: `${log.moduleName} / ${log.actionType}`,
      status: log.actionStatus,
      message: log.resultMessage,
      details: log.details
    })),
    ...state.behaviorLogs.map((log) => ({
      id: log.logId,
      ts: log.timestampMs,
      source: "Behavior",
      title: log.actionType,
      status: log.actionStatus,
      message: [
        log.direction ? `direction=${log.direction}` : undefined,
        typeof log.entryOdds === "number" ? `entryOdds=${decimal(log.entryOdds, 3)}` : undefined,
        typeof log.actualFillPrice === "number" ? `fill=${decimal(log.actualFillPrice, 3)}` : undefined,
        log.orderId ? `order=${log.orderId}` : undefined
      ].filter(Boolean).join(" · "),
      details: log.contextJson ?? log
    }))
  ].sort((left, right) => left.ts - right.ts);

  return (
    <div className="modal-backdrop" onClick={props.onClose}>
      <section className="panel timeline-dialog" onClick={(event) => event.stopPropagation()}>
        <div className="section-header">
          <div>
            <p className="eyebrow">{t("roundLogs")}</p>
            <h2>{state.item.marketSlug ?? state.item.roundId}</h2>
            <span className="section-subtitle">{dateTimeText(state.item.startAt)} - {timeText(state.item.endAt)}</span>
          </div>
          <button className="ghost-button" onClick={props.onClose}>
            {t("close")}
          </button>
        </div>
        <div className="timeline-summary">
          <AppMetric label={t("roundPnl")} value={signedMoney(state.item.roundPnl)} tone={state.item.roundPnl >= 0 ? "positive" : "negative"} />
          <AppMetric label={t("status")} value={state.item.status} />
          <AppMetric label={t("orders")} value={String(state.item.orderCount)} />
          <AppMetric label={t("roundSequence")} value={`#${state.item.sequence}`} />
        </div>
        {activityRows.length === 0 ? (
          <div className="empty-round-log-state">{t("noRoundLogs")}</div>
        ) : (
          <div className="timeline-list">
            {activityRows.map((log) => (
              <details key={`${log.source}-${log.id}`} className="timeline-item">
                <summary>
                  <span>{dateTimeText(log.ts)} · {log.source}</span>
                  <strong>{log.title}</strong>
                  <em>{log.status}</em>
                </summary>
                <p>{log.message || "--"}</p>
                <pre className="json-block">{jsonPreview(log.details)}</pre>
              </details>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}

export default App;





import { useEffect, useState, type MouseEvent as ReactMouseEvent } from "react";
import { useTranslation } from "react-i18next";
import i18n from "./i18n";


import type { ReactNode } from "react";
import { useRef } from "react";
import { useMemo } from "react";
import { useCallback } from "react";
import type { ChangeEvent, Dispatch, KeyboardEvent as ReactKeyboardEvent, SetStateAction, WheelEvent as ReactWheelEvent } from "react";
import {
  api,
  type AuditEvent,
  type BehaviorActionLog,
  type CandlePoint,
  type BulkCreateUsersResult,
  type BulkCreateUsersPreviewResult,
  type CandleBar,
  type CandleInterval,
  type HistoryRound,
  type Language,
  type LogFacets,
  type LogSearchQuery,
  type LogSystem,
  type MarketTrade,
  type MarketSnapshot,
  type OrderAction,
  type OrderLifecycleRecord,
  type OrderRecord,
  type PaperOrderKind,
  type PermissionLevel,
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
  type UpdateUserInput
} from "./utils/api";
import {
  isOrderBookBackendStale,
  orderBookBackendLatencyMs
} from "./utils/displayMetrics";
import { layoutChartPriceLabels, nextChartVisibleCount, nextChartYZoom } from "./utils/chartWheel";
import { FieldChip } from "./components/FieldChip";
import { PersonalHomePage } from "./features/profile/PersonalHomePage";
import { PositionPnlBreakdown } from "./features/trade/PositionPnlBreakdown";
import { positionDisplayedPnl, summarizePositionPnl } from "./features/trade/pnl";
import { useOrderActions } from "./features/trade/useOrderActions";
import { useMarketSocket } from "./features/market/useMarketSocket";
import { useUserSocket } from "./features/user/useUserSocket";
import { ManualSettlementQueue } from "./features/settlement/ManualSettlementQueue";
import {
  ACTION_STATUS_OPTIONS,
  CONNECTION_STATE_OPTIONS,
  DEFAULT_LOG_FACETS,
  LANGUAGE_OPTIONS,
  LATENCY_PHASE_OPTIONS,
  LATENCY_SOURCE_OPTIONS,
  LOG_EXPORT_SYSTEMS,
  LOG_GROUP_OPTIONS,
  MATCHING_EVENT_OPTIONS,
  MATCHING_KIND_OPTIONS,
  ROLE_OPTIONS
} from "./features/logs/logConfig";
import {
  initialRealtimeStatus,
  realtimeStatusDetail,
  realtimeStatusLabel,
  realtimeStatusTone,
  transitionRealtimeChannel,
  type RealtimeChannel,
  type RealtimeChannelStatus,
  type RealtimeStatus
} from "./features/realtime/status";
import { useAppStore } from "./store/useAppStore";
import {
  filterRowsByAnalyticsDate,
  resolveAnalyticsDateFilter,
  type AnalyticsDateFilter,
  type AnalyticsDateQueryError
} from "./utils/analyticsDateFilter";
import { dateTimeText, decimal, money, signedMoney, timeText, tokenPriceText, tradeDisplayPriceText, utcParts } from "./utils/format";
import { redactNetworkAddresses } from "./utils/redaction";

const t = (key: string, options?: Record<string, unknown>) => i18n.t(key, options);

declare const __APP_VERSION__: string;
declare const __APP_DISPLAY_TITLE__: string;

const APP_VERSION = __APP_VERSION__;
const APP_VERSION_LABEL = `v${APP_VERSION}`;

if (typeof document !== "undefined") {
  document.title = __APP_DISPLAY_TITLE__;
}

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

const compactPercent = (value = 0) => `${(value * 100).toFixed(1)}%`;
const jsonPreview = (value: unknown) => redactNetworkAddresses(JSON.stringify(value ?? {}, null, 2));
const exportFileName = () => `paper-trading-export-${new Date().toISOString().slice(0, 10)}.zip`;

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

const CHART_COUNT_OPTIONS = [10, 20, 30, 50, 100];
const TRADE_INTERVAL_OPTIONS = ["30s", "1m", "5m", "15m", "1h"] as const satisfies readonly CandleInterval[];
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
  _language: Language,
  fallback?: string
) {
  if (!round) {
    return fallback ?? "--";
  }
  return t("roundFiveMinuteTitle", { symbol: round.symbol, timeRange: roundTimeRangeText(round) });
}

function normalizeChartBars(bars: CandleBar[]) {
  const deduped = new Map<number, CandleBar>();
  for (const bar of bars) {
    deduped.set(bar.startTs, bar);
  }

  return [...deduped.values()].sort((left, right) => left.startTs - right.startTs);
}

function filterBarsToRecentWindow(bars: CandleBar[]) {
  return normalizeChartBars(bars).filter((bar) => bar.high > 0 || bar.low > 0 || bar.close > 0);
}

function intervalDurationMs(interval: CandleBar["interval"]) {
  if (interval === "5s") {
    return 5_000;
  }
  if (interval === "30s") {
    return 30_000;
  }
  if (interval === "1h") {
    return 60 * 60_000;
  }
  if (interval === "15m") {
    return 15 * 60_000;
  }
  if (interval === "5m") {
    return 5 * 60_000;
  }
  if (interval === "1d") {
    return 24 * 60 * 60_000;
  }
  return 60_000;
}

function defaultVisibleCountForInterval(interval?: CandleBar["interval"]) {
  if (interval === "1h" || interval === "15m") {
    return 24;
  }
  if (interval === "5m") {
    return 30;
  }
  if (interval === "5s" || interval === "30s") {
    return 50;
  }
  return 60;
}

function parseBarCountInput(value: string) {
  const normalized = value.trim();
  if (!/^\d+$/.test(normalized)) {
    return undefined;
  }
  const parsed = Number(normalized);
  if (!Number.isInteger(parsed) || parsed < 10 || parsed > 200) {
    return undefined;
  }
  return parsed;
}

function parseLimitPriceCentsInput(value: string) {
  const normalized = value.trim();
  if (!/^\d+$/.test(normalized)) {
    return undefined;
  }
  const parsed = Number(normalized);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 99) {
    return undefined;
  }
  return parsed;
}

function shortChartHint(language: Language) {
  return t("wheelZoomShiftYDblReset");
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

function chartPriceAxisText(value: number) {
  return Math.round(value).toLocaleString("en-US");
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

function formatCountdownSeconds(value?: number, now = Date.now(), mode: "endAt" | "remainingMs" = "endAt") {
  if (typeof value !== "number") {
    return "--";
  }
  const remaining = Math.max(mode === "remainingMs" ? value : value - now, 0);
  return `${Math.floor(remaining / 1000)}s`;
}

function countdownTone(countdownMs: number) {
  if (countdownMs <= 30_000) {
    return "danger";
  }
  if (countdownMs <= 120_000) {
    return "warn";
  }
  return "live";
}

function activeRoundTradeBlockReason(round: RoundRecord | undefined, nowMs: number, language: Language) {
  if (!round || nowMs < round.startAt || nowMs >= round.endAt) {
    return t("uiNoActiveTradableRound94c14d76");
  }
  if (round.status !== "Trading") {
    return t("uiCurrentRoundIsValue133dcca9", { p0: round.status });
  }
  if (round.endAt - nowMs <= 10_000) {
    return t("uiTheRoundEnteredTheFinal102ed81789");
  }
  return undefined;
}

function buildTradeAvailability(input: {
  language: Language;
  currentRound?: RoundRecord;
  nowMs: number;
  selectedSide: TradeSide;
  orderAction: OrderAction;
  canPlaceOrder: boolean;
  canSell: boolean;
  acceptingOrders?: boolean;
  tradeBlockReason?: string;
  openSidePositions: PositionRecord[];
  orderBook?: MarketSnapshot["orderBooks"][TradeSide];
  oppositeOrderBook?: MarketSnapshot["orderBooks"][TradeSide];
  parsedQty: number;
}) {
  const roundBlockReason = activeRoundTradeBlockReason(input.currentRound, input.nowMs, input.language);
  const selectedAskAvailable = (input.orderBook?.bestAsk ?? 0) > 0 && (input.orderBook?.asks.length ?? 0) > 0;
  const selectedBidAvailable = (input.orderBook?.bestBid ?? 0) > 0 && (input.orderBook?.bids.length ?? 0) > 0;
  const oppositeAskAvailable = (input.oppositeOrderBook?.bestAsk ?? 0) > 0 && (input.oppositeOrderBook?.asks.length ?? 0) > 0;
  const hasOpenSidePositions = input.openSidePositions.some((position) => position.qty > 0);
  const buyReason =
    roundBlockReason ??
    (!input.canPlaceOrder ? t("uiCurrentUserCannotPlaceOrders400362fe") : undefined) ??
    (input.acceptingOrders === false ? t("uiTheMarketIsNotAcceptingNew6192c8ae") : undefined) ??
    (!selectedAskAvailable ? t("uiNoAskDepthIsAvailableFor0dab8698") : undefined) ??
    input.tradeBlockReason;
  const sellReason =
    roundBlockReason ??
    (!input.canSell ? t("uiCurrentUserCannotSelldc4068e3") : undefined) ??
    (!hasOpenSidePositions ? t("uiNoOpenPositionOnThisSide9c5685d4") : undefined) ??
    (!selectedBidAvailable ? t("uiNoBidDepthIsAvailableFor43981024") : undefined) ??
    (input.orderAction === "sell" && input.parsedQty <= 0
      ? t("uiEnterAValidSellQuantity91bdc477")
      : undefined) ??
    input.tradeBlockReason;
  const reverseReason =
    sellReason ??
    (!input.canPlaceOrder ? t("uiCurrentUserCannotPlaceTheReverseb15aabb6") : undefined) ??
    (input.acceptingOrders === false ? t("uiTheMarketIsNotAcceptingThebb3b03fd") : undefined) ??
    (!oppositeAskAvailable ? t("uiNoAskDepthIsAvailableFor0d45282f") : undefined);
  return {
    canBuy: !buyReason,
    canSell: !sellReason,
    canCloseSide: !sellReason,
    canReverseSide: !reverseReason,
    buyReason,
    sellReason,
    closeSideReason: sellReason,
    reverseReason
  };
}

function isCurrentRoundOrder(order: OrderRecord, currentRound?: RoundRecord) {
  if (!currentRound) {
    return false;
  }
  return order.roundId === currentRound.id || Boolean(order.marketSlug && order.marketSlug === currentRound.marketSlug);
}

function orderStatusLabel(order: OrderRecord, language: Language) {
  if (order.status === "pending") {
    return t("pending");
  }
  if (order.status === "filled") {
    return t("filled");
  }
  if (order.status === "cancelled") {
    return t("cancelled");
  }
  if (order.status === "failed") {
    return t("failed");
  }
  return order.status;
}

function orderResultLabel(order: OrderRecord, language: Language) {
  const kind = order.orderKind === "limit" ? t("limit") : t("market");
  return `${kind} / ${orderStatusLabel(order, language)}`;
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

function displayPriceForSide(snapshot: MarketSnapshot | undefined, side: TradeSide) {
  const displayPrice = snapshot?.displayPrices?.[side];
  if (typeof displayPrice === "number" && Number.isFinite(displayPrice)) {
    return displayPrice;
  }
  return side === "UP" ? snapshot?.upPrice ?? 0 : snapshot?.downPrice ?? 0;
}

function filledOrderReferencePrice(order: OrderRecord) {
  if (typeof order.avgFillPrice === "number" && order.avgFillPrice > 0) {
    return order.avgFillPrice;
  }
  if (typeof order.limitPrice === "number" && order.limitPrice > 0) {
    return order.limitPrice;
  }
  return orderBookExecutionPrice(order) ?? 0;
}

function orderReferencePrice(order: OrderRecord, snapshot?: MarketSnapshot) {
  if (order.status === "filled") {
    return filledOrderReferencePrice(order);
  }
  const currentDisplayPrice = displayPriceForSide(snapshot, order.side);
  return currentDisplayPrice > 0 ? currentDisplayPrice : orderBookExecutionPrice(order) ?? 0;
}

function orderReferencePriceText(order: OrderRecord, snapshot?: MarketSnapshot) {
  const referencePrice = orderReferencePrice(order, snapshot);
  if (order.status === "filled") {
    return tokenPriceText(referencePrice);
  }
  if (order.orderKind === "limit" && order.status === "pending" && typeof order.limitPrice === "number" && order.limitPrice > 0) {
    return tokenPriceText(order.limitPrice);
  }
  const currentDisplayPrice = displayPriceForSide(snapshot, order.side);
  if (currentDisplayPrice > 0) {
    return tradeDisplayPriceText(currentDisplayPrice);
  }
  return tokenPriceText(referencePrice);
}

function orderTradeLabel(order: OrderRecord, language: Language) {
  const actionLabel = (order.action === "buy" ? t("buy") : t("sell"));
  const sideLabel = order.side === "UP" ? "UP" : "DOWN";
  return `${actionLabel} ${sideLabel}`;
}

function orderPriceQualifier(order: OrderRecord, language: Language) {
  if (order.status === "filled") {
    return t("uiFillExclFee8feb86d7");
  }
  if (order.orderKind === "limit" && order.status === "pending") {
    return t("uiLimitPrice795eca5e");
  }
  if (typeof order.limitPrice === "number" && order.limitPrice > 0) {
    return t("uiLimit0295355c");
  }
  return t("uiLatestTradeDisplayf79d82d5");
}

function OrderExecutionCell({ order, language }: { order: OrderRecord; language: Language }) {
  const bookPrice = orderBookExecutionPrice(order);
  return (
    <div className="field-stack compact-order-metrics">
      <strong>{order.avgFillPrice ? decimal(order.avgFillPrice, 4) : "--"}</strong>
      <small className="cell-note">
        {t("book")}: {bookPrice > 0 ? decimal(bookPrice, 4) : "--"}
      </small>
      <small className="cell-note">
        {t("slippage")}: {typeof order.slippageBps === "number" ? `${decimal(order.slippageBps, 2)} bps` : "--"}
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

const AUDIT_ACTION_LABEL_KEYS: Record<string, string> = {
  login: "auditActionLogin",
  switch_language: "auditActionSwitchLanguage",
  place_order: "auditActionPlaceOrder",
  cancel_order: "auditActionCancelOrder",
  sell_position: "auditActionSellPosition",
  close_side: "auditActionCloseSide",
  reverse_side: "auditActionReverseSide",
  limit_order_triggered: "auditActionLimitOrderTriggered",
  limit_order_failed: "auditActionLimitOrderFailed",
  capture_price_to_beat: "auditActionCapturePriceToBeat",
  poll_settlement: "auditActionPollSettlement",
  settlement_confirmed: "auditActionSettlementConfirmed",
  manual_settlement: "auditActionManualSettlement",
  redeem_position: "auditActionRedeemPosition",
  round_closed: "auditActionRoundClosed",
  market_latency: "auditActionMarketLatency",
  user_create: "auditActionCreateUser",
  user_disable: "auditActionDisableUser",
  user_enable: "auditActionEnableUser",
  user_reset_password: "auditActionResetPassword",
  user_changePassword: "auditActionChangePassword",
  "user.changePassword": "auditActionChangePassword",
  "user.resetPassword": "auditActionResetPassword",
  "user.balance.set": "auditActionSetBalance"
};

const AUDIT_CATEGORY_LABEL_KEYS: Record<string, string> = {
  operation: "auditCategoryOperation",
  matching: "auditCategoryMatching",
  settlement: "auditCategorySettlement",
  latency: "auditCategoryLatency"
};

function auditActionLabel(actionType: string | undefined, _language: Language) {
  if (!actionType) return "--";
  const key = AUDIT_ACTION_LABEL_KEYS[actionType];
  return key ? t(key) : actionType;
}

function auditCategoryLabel(category: string | undefined, _language: Language) {
  if (!category) return "--";
  const key = AUDIT_CATEGORY_LABEL_KEYS[category];
  return key ? t(key) : category;
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
  return order.orderKind === "limit" ? t("limit") : t("market");
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

function transportAgeMs(receivedAt: number, publishTs: number, clientClockOffsetMs = 0) {
  return Math.max(receivedAt - publishTs - clientClockOffsetMs, 0);
}

function latencyFor(source?: SourceHealth, now = Date.now(), clientRecvTs?: number, clientClockOffsetMs = 0) {
  if (!source || source.state === "disabled") {
    return {
      sourceToBackendLatencyMs: 0,
      backendToFrontendLatencyMs: undefined,
      endToEndLatencyMs: undefined,
      dataAgeMs: 0,
      sourceDataAgeMs: 0,
      marketUpdateAgeMs: 0,
      disabled: true
    };
  }
  const backendToFrontendLatencyMs =
    typeof source.clientRecvTs === "number"
      ? transportAgeMs(source.clientRecvTs, source.serverPublishTs, clientClockOffsetMs)
      : typeof clientRecvTs === "number"
        ? Math.max(clientRecvTs - source.serverPublishTs - clientClockOffsetMs, 0)
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
    sourceDataAgeMs: Math.max(now - source.normalizedTs, 0),
    marketUpdateAgeMs:
      typeof source.clientRecvTs === "number"
        ? Math.max(now - source.clientRecvTs, 0)
        : typeof clientRecvTs === "number"
          ? Math.max(now - clientRecvTs, 0)
          : 0,
    disabled: false
  };
}

function sourceComponent(source: SourceHealth | undefined, key: string) {
  return source?.components?.[key];
}

function componentStateLabel(state: SourceHealth["state"] | undefined, language: Language) {
  if (!state) return t("uiUnknown30b090da");
  const keys: Record<SourceHealth["state"], string> = {
    healthy: "sourceStateHealthy",
    degraded: "sourceStateDegraded",
    reconnecting: "sourceStateReconnecting",
    stale: "sourceStateStale",
    disabled: "sourceStateDisabled"
  };
  return t(keys[state]);
}

function clobComponentSummary(source: SourceHealth | undefined, language: Language) {
  const orderBook = sourceComponent(source, "orderBook");
  const marketWs = sourceComponent(source, "marketWs");
  const trades = sourceComponent(source, "trades");
  const orderBookText = t("uiBookValue5849f7a2", { p0: componentStateLabel(orderBook?.state, language) });
  const secondaryIssues = [
    marketWs && marketWs.state !== "healthy"
      ? t("uiWSValuebd6fad9d", { p0: componentStateLabel(marketWs.state, language) })
      : undefined,
    trades && trades.state !== "healthy"
      ? t("uiTradesValuee89ae62d", { p0: componentStateLabel(trades.state, language) })
      : undefined
  ].filter(Boolean);
  return secondaryIssues.length > 0 ? `${orderBookText} / ${secondaryIssues.join(" / ")}` : orderBookText;
}

function TerminalSection(props: { title: string; meta?: string; children: ReactNode }) {
  return (
    <section className="terminal-section">
      <div className="terminal-section-head">
        <span>{props.title}</span>
        {props.meta ? <em>{props.meta}</em> : null}
      </div>
      {props.children}
    </section>
  );
}

function buildRiskAlerts(input: {
  language: Language;
  countdownMs: number;
  upPrice: number;
  downPrice: number;
  oddsChange: number;
  sources: Array<SourceHealth | undefined>;
  clobLatencyMs: number;
  nowMs: number;
}) {
  // ACK state is intentionally retained in the alert model even when the current UI does not expose a separate button.
  const alerts: Array<{
    kind: string;
    group: "market" | "trading" | "settlement" | "system";
    level: "info" | "warn" | "danger";
    text: string;
    detail?: string;
  }> = [];
  if (input.countdownMs > 0 && input.countdownMs < 10_000) {
    alerts.push({
      kind: "frozen",
      group: "trading",
      level: "danger",
      text: t("uiTradingFrozenOrderButtonsDisabled44a1885e"),
      detail: t("uiTheRoundEnteredTheFinal102ed81789")
    });
  } else if (input.countdownMs > 0 && input.countdownMs < 30_000) {
    alerts.push({
      kind: "freeze_warning",
      group: "trading",
      level: "warn",
      text: t("freezeWarningUnder30s"),
      detail: t("uiWatchLiquidityAndCancellationWindowsIna8d652f4")
    });
  }
  if (Math.abs(input.oddsChange) > 0.05) {
    alerts.push({
      kind: "odds_jump",
      group: "market",
      level: "warn",
      text: t("uiPriceJumpedValueValue674015a5", { p0: input.oddsChange >= 0 ? "+" : "", p1: decimal(input.oddsChange, 4) })
    });
  }
  if (input.upPrice > 0.97 || input.downPrice > 0.97) {
    alerts.push({
      kind: "pre_settle",
      group: "settlement",
      level: "info",
      text: t("uiPreSettleSignalValue3930617a", { p0: input.upPrice > input.downPrice ? "UP" : "DOWN" }),
      detail: t("uiDisplayOnlyBalancesAndPositionsStayffdf6b7d")
    });
  }
  for (const source of input.sources) {
    if (source && source.state !== "healthy" && input.nowMs - source.sourceEventTs > 5000) {
      alerts.push({
        kind: `source_${source.source}`,
        group: "market",
        level: "danger",
        text: t("uiValueDataInterruptedd1f85f00", { p0: source.source }),
        detail: redactNetworkAddresses(source.message)
      });
    }
  }
  if (input.clobLatencyMs > 1000) {
    alerts.push({
      kind: "high_lag",
      group: "system",
      level: "warn",
      text: t("uiCLOBMarketStaleValueMs9441175b", { p0: Math.round(input.clobLatencyMs) })
    });
  }
  return alerts;
}

function buildStrategyHints(input: {
  language: Language;
  upPrice: number;
  downPrice: number;
  oddsChange: number;
  doubleSideCost: number;
}) {
  const momentum =
    Math.abs(input.oddsChange) > 0.03
      ? (input.oddsChange > 0 ? t("uiUPMomentumStrong9ebcb4ae") : t("uiDOWNMomentumStrongf1f1e59a"))
      : t("momentumNeutral");
  return [
    { label: t("uiMomentum8206fa0a"), value: `${momentum} (${input.oddsChange >= 0 ? "+" : ""}${tokenPriceText(Math.abs(input.oddsChange), 1)})` },
    {
      label: t("uiTwoSideAskb5478767"),
      value: `${tokenPriceText(input.doubleSideCost, 1)} (${decimal(Math.max(input.doubleSideCost - 1, 0) * 100, 1)}%)`
    }
  ];
}

function summarizeMiniSeries(series: CandlePoint[]) {
  if (series.length === 0) {
    return undefined;
  }
  let high = series[0].price;
  let low = series[0].price;
  for (const point of series) {
    high = Math.max(high, point.price);
    low = Math.min(low, point.price);
  }
  return {
    high,
    low,
    latest: series.at(-1)?.price ?? series[0].price
  };
}

function OddsMiniChart(props: { series: CandlePoint[]; language: Language }) {
  const points = props.series.slice(-80);
  const width = 720;
  const height = 44;
  const summary = summarizeMiniSeries(points);
  if (!summary) {
    return (
      <div className="terminal-odds-strip empty">
        <span>{t("uiHTUPThisRounddbdace32")}</span>
        <em>{t("uiWaitingForThisRoundPricePointsd7620794")}</em>
      </div>
    );
  }
  const syntheticPoints =
    points.length === 1
      ? [
          { ts: points[0].ts - 1, price: points[0].price },
          points[0]
        ]
      : points;
  const min = Math.min(...syntheticPoints.map((point) => point.price));
  const max = Math.max(...syntheticPoints.map((point) => point.price));
  const range = Math.max(max - min, 0.01);
  const xForIndex = (index: number, count: number) => (index / Math.max(count - 1, 1)) * (width - 112);
  const yForPrice = (price: number) => height - 4 - ((price - min) / range) * (height - 10);
  const d = syntheticPoints
    .map((point, index) => {
      const x = xForIndex(index, syntheticPoints.length);
      const y = yForPrice(point.price);
      return `${index === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  const area = `${d} L${xForIndex(syntheticPoints.length - 1, syntheticPoints.length).toFixed(1)},${height} L0,${height} Z`;
  const lastX = xForIndex(syntheticPoints.length - 1, syntheticPoints.length);
  const lastY = yForPrice(summary.latest);
  return (
    <div className="terminal-odds-strip">
      <span>{t("uiHTUPThisRounddbdace32")}</span>
      <b>{tokenPriceText(summary.latest, 1)}</b>
      <svg viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
        <path className="mini-grid" d={`M0,11.5 H${width} M0,22 H${width} M0,32.5 H${width}`} />
        <path className="mini-area" d={area} />
        <path className="mini-line" d={d} />
        <circle className="mini-last" cx={lastX} cy={lastY} r="2.7" />
      </svg>
      <div className="mini-values">
        <span className="mini-value-row high"><i>{t("uiHighf320ca9d")}</i><b>{tokenPriceText(summary.high, 1)}</b></span>
        <span className="mini-value-row low"><i>{t("uiLow85296bfa")}</i><b>{tokenPriceText(summary.low, 1)}</b></span>
      </div>
    </div>
  );
}

function CoinbaseComparisonChart(props: {
  bars: CandleBar[];
  referencePrice?: number;
  binancePrice?: number;
  emptyText: string;
  visibleCount?: number;
  onVisibleCountChange?: (value: number) => void;
  timeDomainStartTs?: number;
  timeDomainEndTs?: number;
  yZoom?: number;
  onYZoomChange?: Dispatch<SetStateAction<number>>;
}) {
  const latestReferencePrice = props.referencePrice ?? props.binancePrice ?? props.bars.at(-1)?.close ?? 0;
  return (
    <CandlestickChart
      bars={props.bars}
      upColor="#00f0c0"
      downColor="#f03060"
      emptyText={props.emptyText}
      latestPrice={latestReferencePrice}
      visibleCount={props.visibleCount}
      onVisibleCountChange={props.onVisibleCountChange}
      timeDomainStartTs={props.timeDomainStartTs}
      timeDomainEndTs={props.timeDomainEndTs}
      yZoom={props.yZoom}
      onYZoomChange={props.onYZoomChange}
    />
  );
}

function roundMoveLabel(round: HistoryRound, language: Language) {
  if (!isBtcReferencePrice(round.polymarketOpenPrice) || !isBtcReferencePrice(round.polymarketClosePrice)) {
    return "--";
  }
  const delta = round.polymarketClosePrice - round.polymarketOpenPrice;
  if (Math.abs(delta) < 0.0001) {
    return t("flat");
  }
  return delta > 0 ? t("up") : t("down");
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

function roundHasEnded(round: Pick<RoundRecord, "endAt">, nowMs: number) {
  return nowMs >= round.endAt;
}

function preliminarySideFromRound(round: RoundRecord) {
  const price = round.closingSpotPrice ?? round.binanceClosePrice ?? round.polymarketClosePrice;
  if (
    typeof price !== "number" ||
    !Number.isFinite(price) ||
    !isBtcReferencePrice(round.priceToBeat) ||
    !isOfficialPtbSource(round.priceToBeatSource)
  ) {
    return undefined;
  }
  return price >= round.priceToBeat ? "UP" : "DOWN";
}

function recentRoundOutcome(input: {
  round: RoundRecord & { settlementPreview?: SettlementPreview };
  nowMs: number;
  language: Language;
}) {
  const { round, nowMs, language } = input;
  const preview = round.settlementPreview;
  if (!roundHasEnded(round, nowMs)) {
    return {
      className: "live",
      label: t("uiLIVE9e10d574")
    };
  }
  const confirmedSide =
    round.settledSide ??
    (preview?.state === "confirmed" && preview.side ? preview.side : undefined);
  if (confirmedSide) {
    return {
      className: confirmedSide === "UP" ? "up" : "down",
      label: confirmedSide === "UP" ? "UP" : "DN"
    };
  }
  if (round.status === "Manual" || preview?.state === "manual") {
    return {
      className: "manual",
      label: t("uiREVa7f3ebf2")
    };
  }
  const preliminarySide = preview?.state === "preliminary" && preview.side ? preview.side : preliminarySideFromRound(round);
  if (preliminarySide) {
    return {
      className: preliminarySide === "UP" ? "pre-up" : "pre-down",
      label: preliminarySide === "UP" ? "PRE UP" : "PRE DN"
    };
  }
  if (preview?.confidence === "conflict") {
    return {
      className: "conflict",
      label: t("uiCON5b6e4814")
    };
  }
  return {
    className: "pending",
    label: t("uiWAIT60658fa1")
  };
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

type OperatedCurveWindow = 10 | 30 | 60 | "all";

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
  return t("roundTimePending");
}

function roundSecondaryText(meta: RoundDisplayMeta, language: Language) {
  const slug = meta.marketSlug ?? meta.roundId;
  return `${t("market")}: ${slug}`;
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

function applyCurveWindow<T>(items: T[], window: OperatedCurveWindow) {
  return window === "all" ? items : items.slice(Math.max(items.length - window, 0));
}

function CompactEquityCurve(props: { points: EquityCurvePoint[]; minValue: number; maxValue: number }) {
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
            <text x="12" y="64">{t("singleRoundPnlLabel", { value: signedMoney(hoverPoint.roundPnl) })}</text>
            <text x="12" y="84">{t("cumulativeReturnLabel", { value: signedMoney(hoverPoint.cumulativeEquity) })}</text>
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

function buildCurveSeries(history: HistoryRound[], orders: OrderRecord[], window: OperatedCurveWindow): EquityCurvePoint[] {
  const operated = buildOperatedHistory(history, orders);
  const visible = applyCurveWindow(operated, window);
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

function isOfficialPtbSource(source?: string) {
  const normalized = source?.toLowerCase() ?? "";
  return normalized.includes("coinbase");
}

function btcMoneyOrDash(value?: number) {
  return isBtcReferencePrice(value) ? money(value) : "--";
}

function referenceSpread(current?: number, reference?: number) {
  return isBtcReferencePrice(current) && isBtcReferencePrice(reference)
    ? current - reference
    : undefined;
}

function spreadToneClass(spread?: number) {
  if (typeof spread !== "number" || Math.abs(spread) < 0.005) {
    return "terminal-neutral";
  }
  return spread > 0 ? "terminal-green" : "terminal-red";
}

function spreadDisplayText(spread?: number) {
  if (typeof spread !== "number" || Math.abs(spread) < 0.005) {
    return "--";
  }
  return signedMoney(spread);
}

function ptbDisplayLabel(language: Language, source?: MarketSnapshot["displayPriceToBeatSource"]) {
  if (source === "binance_open_fallback") {
    return t("uiPTBBinanceOpenf6c19fc9");
  }
  return "PTB";
}

function metricValueForSource(source: SourceHealth | undefined, value: number, digits = 2) {
  if (source?.state === "disabled" || !isBtcReferencePrice(value)) {
    return "--";
  }
  return money(value, digits);
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

function CandlestickChart(props: {
  bars: CandleBar[];
  upColor: string;
  downColor: string;
  emptyText: string;
  priceToBeat?: number;
  latestPrice?: number;
  round?: RoundRecord;
  visibleCount?: number;
  onVisibleCountChange?: (value: number) => void;
  timeDomainStartTs?: number;
  timeDomainEndTs?: number;
  onTimeDomainChange?: (domain: { startTs: number; endTs: number }) => void;
  yZoom?: number;
  onYZoomChange?: Dispatch<SetStateAction<number>>;
}) {
  const normalizedBars = filterBarsToRecentWindow(props.bars);
  const svgRef = useRef<SVGSVGElement | null>(null);
  const [chartSize, setChartSize] = useState({ width: 900, height: 460 });
  const [hoveredBar, setHoveredBar] = useState<{ index: number; mouseX: number; mouseY: number } | undefined>();
  const [uncontrolledVisibleCount, setUncontrolledVisibleCount] = useState(60);
  const [panOffset, setPanOffset] = useState(0);
  const [uncontrolledYZoom, setUncontrolledYZoom] = useState(1);
  const lastIntervalRef = useRef<CandleBar["interval"] | undefined>(undefined);
  const visibleCount = props.visibleCount ?? uncontrolledVisibleCount;
  const setVisibleCount = props.onVisibleCountChange ?? setUncontrolledVisibleCount;
  const yZoom = props.yZoom ?? uncontrolledYZoom;
  const setYZoom = props.onYZoomChange ?? setUncontrolledYZoom;

  useEffect(() => {
    const lastInterval = normalizedBars.at(-1)?.interval;
    if (lastInterval && lastInterval !== lastIntervalRef.current) {
      setVisibleCount(clamp(defaultVisibleCountForInterval(lastInterval), 10, 200));
      setPanOffset(0);
    }
    lastIntervalRef.current = lastInterval;
  }, [normalizedBars, setVisibleCount]);

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

  useEffect(() => {
    if (!props.onTimeDomainChange || normalizedBars.length === 0) {
      return;
    }
    const safeVisibleCount = clamp(visibleCount, 10, 200);
    const end = Math.max(normalizedBars.length - panOffset, 0);
    const start = Math.max(end - safeVisibleCount, 0);
    const scopedBars = normalizedBars.slice(start, end);
    if (scopedBars.length === 0) {
      return;
    }
    props.onTimeDomainChange({
      startTs: scopedBars[0].startTs,
      endTs: scopedBars.at(-1)!.endTs
    });
  }, [normalizedBars, panOffset, props.onTimeDomainChange, visibleCount]);

  if (normalizedBars.length === 0) {
    return <div className="chart-empty">{props.emptyText}</div>;
  }

  const safeVisibleCount = clamp(visibleCount, 10, 200);
  const end = Math.max(normalizedBars.length - panOffset, 0);
  const start = Math.max(end - safeVisibleCount, 0);
  const scopedBars = normalizedBars.slice(start, end);
  const domainStartTs = props.timeDomainStartTs ?? scopedBars[0].startTs;
  const domainEndTs = props.timeDomainEndTs ?? scopedBars.at(-1)!.endTs;
  const bars = scopedBars.filter((bar) => bar.endTs >= domainStartTs && bar.startTs <= domainEndTs);
  if (bars.length === 0) {
    return <div className="chart-empty">{props.emptyText}</div>;
  }
  const resetChartView = () => {
    setVisibleCount(clamp(defaultVisibleCountForInterval(normalizedBars.at(-1)?.interval), 10, 200));
    setPanOffset(0);
    setYZoom(1);
  };

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
  const mid = (maxWithPadding + minWithPadding) / 2;
  const maxZoomThatKeepsVisibleRange = Math.max((maxWithPadding - minWithPadding) / rawRange, 1);
  const effectiveYZoom = Math.min(yZoom, maxZoomThatKeepsVisibleRange);
  const zoomedRange = Math.max((maxWithPadding - minWithPadding) / effectiveYZoom, 1);
  const range = zoomedRange;
  const zoomMax = mid + zoomedRange / 2;
  const zoomMin = mid - zoomedRange / 2;
  const innerWidth = width - padding.left - padding.right;
  const innerHeight = height - padding.top - padding.bottom;
  const domainSpanMs = Math.max(domainEndTs - domainStartTs, 1);
  const approximateSlotWidth = innerWidth / Math.max(safeVisibleCount, bars.length, 1);
  const candleWidth = Math.max(Math.min(approximateSlotWidth * 0.58, 16), 3);

  const yForPrice = (value: number) => padding.top + ((zoomMax - value) / range) * innerHeight;
  const xForTs = (ts: number) => padding.left + ((ts - domainStartTs) / domainSpanMs) * innerWidth;
  const barCenterTs = (bar: CandleBar) => bar.startTs + intervalDurationMs(bar.interval) / 2;
  const axisTicks = Array.from({ length: 6 }, (_, index) => domainStartTs + Math.round((domainSpanMs * index) / 5));
  const priceTicks = [0, 1, 2, 3, 4].map((step) => zoomMax - (range * step) / 4);
  const tooltipWidth = 126;
  const tooltipHeight = 106;
  const hoveredIndex = hoveredBar?.index;
  const hoveredCandle = typeof hoveredIndex === "number" ? bars[hoveredIndex] : undefined;
  const hoveredX = hoveredCandle ? xForTs(barCenterTs(hoveredCandle)) : undefined;
  const latestY = typeof props.latestPrice === "number" && props.latestPrice > 0 ? yForPrice(props.latestPrice) : undefined;
  const latestLabelY = layoutChartPriceLabels({
    latestY,
    plotTop: padding.top,
    plotBottom: height - padding.bottom
  }).latestLabelY;
  const roundStartX =
    props.round && props.round.startAt >= domainStartTs && props.round.startAt <= domainEndTs
      ? xForTs(props.round.startAt)
      : undefined;
  const roundEndX =
    props.round && props.round.endAt >= domainStartTs && props.round.endAt <= domainEndTs
      ? xForTs(props.round.endAt)
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
        mouseX: xForTs(barCenterTs(bars[fallbackIndex])),
        mouseY: padding.top + innerHeight / 2
      });
      return;
    }
    const mouseX = ((event.clientX - svgRect.left) / svgRect.width) * width;
    const mouseY = ((event.clientY - svgRect.top) / svgRect.height) * height;
    const hoveredTs = domainStartTs + ((mouseX - padding.left) / Math.max(innerWidth, 1)) * domainSpanMs;
    let index = 0;
    let minDistance = Number.POSITIVE_INFINITY;
    for (let barIndex = 0; barIndex < bars.length; barIndex += 1) {
      const distance = Math.abs(barCenterTs(bars[barIndex]) - hoveredTs);
      if (distance < minDistance) {
        minDistance = distance;
        index = barIndex;
      }
    }
    setHoveredBar({
      index: Math.round(index),
      mouseX,
      mouseY
    });
  };

  const handleChartWheel = (event: ReactWheelEvent<SVGSVGElement>) => {
    event.preventDefault();
    if (event.shiftKey) {
      setYZoom((value) => nextChartYZoom(value, event.deltaY));
      return;
    }
    setVisibleCount(nextChartVisibleCount(visibleCount, event.deltaY));
    setPanOffset((value) => clamp(value, 0, Math.max(normalizedBars.length - 10, 0)));
  };

  return (
    <svg
      ref={svgRef}
      viewBox={`0 0 ${width} ${height}`}
      className="candle-chart"
      data-y-zoom={decimal(effectiveYZoom, 3)}
      role="img"
      aria-label="candlestick chart"
      onWheel={handleChartWheel}
      onDoubleClick={resetChartView}
      onMouseLeave={() => setHoveredBar(undefined)}
    >
      <rect x="0" y="0" width={width} height={height} rx="20" fill="transparent" />
      {priceTicks.map((tick) => {
        const y = yForPrice(tick);
        return (
          <g key={tick}>
            <line x1={padding.left} y1={y} x2={width - padding.right} y2={y} className="chart-grid-line" />
            <text x={width - padding.right + 12} y={y + 4} className="chart-axis-label chart-price-axis-label">
              {chartPriceAxisText(tick)}
            </text>
          </g>
        );
      })}
      {bars.map((bar, index) => {
        const x = xForTs(barCenterTs(bar));
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
          <rect data-overlay-label="ptb-box" x={padding.left + 8} y={yForPrice(props.priceToBeat) - 13} width="76" height="19" rx="6" className="chart-target-label-box" />
          <text data-overlay-label="ptb" x={padding.left + 14} y={yForPrice(props.priceToBeat)} className="chart-target-label">
            PTB {chartPriceAxisText(props.priceToBeat)}
          </text>
        </g>
      ) : null}
      {typeof props.latestPrice === "number" && props.latestPrice > 0 ? (
        <g>
          <line x1={padding.left} y1={yForPrice(props.latestPrice)} x2={width - padding.right} y2={yForPrice(props.latestPrice)} className="chart-current-line" />
          <rect data-overlay-label="btc-box" x={width - padding.right + 8} y={(latestLabelY ?? yForPrice(props.latestPrice)) - 13} width="76" height="19" rx="6" className="chart-current-label-box" />
          <text data-overlay-label="btc" x={width - padding.right + 14} y={latestLabelY ?? yForPrice(props.latestPrice)} className="chart-current-label">
            BTC {chartPriceAxisText(props.latestPrice)}
          </text>
        </g>
      ) : null}
      {typeof roundStartX === "number" ? <line x1={roundStartX} y1={padding.top} x2={roundStartX} y2={height - padding.bottom} className="chart-round-line" /> : null}
      {typeof roundEndX === "number" ? <line x1={roundEndX} y1={padding.top} x2={roundEndX} y2={height - padding.bottom} className="chart-round-line" /> : null}
      {axisTicks.map((tickTs) => {
        return (
          <text
            key={tickTs}
            x={xForTs(tickTs)}
            y={height - 8}
            textAnchor="middle"
            className="chart-axis-label"
          >
            {chartTimeText(tickTs)}
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
  useTranslation();
  const [username, setUsername] = useState("tester");
  const [password, setPassword] = useState("tester123");
  const [busy, setBusy] = useState(false);
  const [passwordVisible, setPasswordVisible] = useState(false);
  const [savedUsers, setSavedUsers] = useState<string[]>([]);
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const [serverOnline, setServerOnline] = useState(false);
  const [clock, setClock] = useState(() => new Date().toLocaleTimeString("en-GB", { hour12: false }));

  useEffect(() => {
    try {
      const parsed = JSON.parse(localStorage.getItem("ht_saved_users") ?? "[]") as string[];
      setSavedUsers(parsed.filter((item) => typeof item === "string").slice(0, 8));
    } catch {
      setSavedUsers([]);
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    const ping = async () => {
      try {
        const healthy = await api.checkHealth();
        if (!cancelled) setServerOnline(healthy);
      } catch {
        if (!cancelled) setServerOnline(false);
      }
    };
    const clockTimer = setInterval(() => {
      setClock(new Date().toLocaleTimeString("en-GB", { hour12: false }));
    }, 1000);
    const pingTimer = setInterval(ping, 5000);
    void ping();
    return () => {
      cancelled = true;
      clearInterval(clockTimer);
      clearInterval(pingTimer);
    };
  }, []);

  const submit = async () => {
    setBusy(true);
    try {
      await props.onLogin(username, password);
      const nextSaved = [username, ...savedUsers.filter((item) => item !== username)].filter(Boolean).slice(0, 8);
      setSavedUsers(nextSaved);
      localStorage.setItem("ht_saved_users", JSON.stringify(nextSaved));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="terminal-login-page">
      <div className="terminal-login-center">
        <div className="terminal-login-logo">
          <span>Hyper</span>
          <em>liquid</em>
        </div>

        <div className="terminal-login-tabs">
          <button className="active">PAPER</button>
          <button className="locked" disabled>
            LIVE <span aria-hidden="true">/</span>
          </button>
        </div>

        <div className="terminal-login-card">
          {props.error ? <div className="terminal-login-error">{redactNetworkAddresses(props.error)}</div> : null}
          <label>
            <span>{t("username")}</span>
            <div className="terminal-user-wrap">
              <input
                value={username}
                autoComplete="username"
                onFocus={() => setDropdownOpen(true)}
                onBlur={() => window.setTimeout(() => setDropdownOpen(false), 120)}
                onChange={(event) => setUsername(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter") void submit();
                }}
              />
              {dropdownOpen && savedUsers.length > 0 ? (
                <div className="terminal-user-dropdown">
                  {savedUsers.map((item) => (
                    <button key={item} type="button" onMouseDown={() => setUsername(item)}>
                      {item}
                    </button>
                  ))}
                </div>
              ) : null}
            </div>
          </label>
          <label>
            <span>{t("password")}</span>
            <div className="terminal-password-wrap">
              <input
                type={passwordVisible ? "text" : "password"}
                value={password}
                autoComplete="current-password"
                onChange={(event) => setPassword(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter") void submit();
                }}
              />
              <button type="button" onClick={() => setPasswordVisible((value) => !value)}>
                {passwordVisible ? "hide" : "show"}
              </button>
            </div>
          </label>
          <label>
            <span>{t("language")}</span>
            <select value={props.language} onChange={(event) => props.onLanguageChange(event.target.value as Language)}>
              <option value="zh-CN">简体中文</option>
              <option value="en-US">English</option>
            </select>
          </label>
          <button className="terminal-sign-button" disabled={busy} onClick={submit}>
            {busy ? t("loading") : t("signIn")}
          </button>
        </div>
        <div className="terminal-login-version">{APP_VERSION_LABEL} · Hyper Terminal</div>
      </div>
      <div className="terminal-login-status">
        <span className={serverOnline ? "login-status-dot" : "login-status-dot off"} />
        <span>{serverOnline ? t("serverConnected") : t("backendOffline")}</span>
        <span className="login-clock">{clock}</span>
      </div>
    </div>
  );
}

function App() {
  const { i18n } = useTranslation();
  const language = (i18n.language as Language) ?? "zh-CN";
  const {
    token,
    me,
    viewedUserId,
    viewedUser,
    currentPage,
    currentRound,
    history,
    operatedHistory,
    settlementPreview,
    snapshot,
    profile,
    positions,
    orders,
    orderLifecycles,
    logs,
    lastOrderLatencyMs,
    lastMarketRecvTs,
    setAuth,
    setUser,
    setViewedUserTarget,
    clearAuth,
    setCurrentPage,
    setBootstrap,
    setMarketPayload,
    setMarketTickPayload,
    setMarketHistoryPatch,
    markMarketRenderCommit,
    setUserPayload,
    setUserTradePayload,
    setLastOrderLatencyMs
  } = useAppStore();
  const [bootstrapping, setBootstrapping] = useState(false);
  const [error, setError] = useState<string>();
  const [orderAmount, setOrderAmount] = useState("150");
  const [orderQty, setOrderQty] = useState("1");
  const [limitPrice, setLimitPrice] = useState("50");
  const [orderAction, setOrderAction] = useState<OrderAction>("buy");
  const [orderKind, setOrderKind] = useState<PaperOrderKind>("market");
  const [selectedSide, setSelectedSide] = useState<TradeSide>("UP");
  const [selectedInterval, setSelectedInterval] = useState<CandleInterval>("1m");
  const [chartVisibleCount, setChartVisibleCount] = useState(60);
  const [nowMs, setNowMs] = useState(Date.now());
  const [realtimeStatus, setRealtimeStatus] = useState<RealtimeStatus>(() => initialRealtimeStatus());
  const [timeline, setTimeline] = useState<TradeTimeline>();
  const [timelineBusyOrderId, setTimelineBusyOrderId] = useState<string>();
  const [roundLogDialog, setRoundLogDialog] = useState<RoundLogDialogState>();
  const [roundLogBusyRoundId, setRoundLogBusyRoundId] = useState<string>();
  const [viewUserId, setViewUserId] = useState<string>();
  const [visibleViewUsers, setVisibleViewUsers] = useState<PublicUser[]>([]);
  const clientClockOffsetMsRef = useRef(0);
  const countdownTargetMs =
    typeof snapshot?.uiMeta.countdownTargetTs === "number"
      ? snapshot.uiMeta.countdownTargetTs + clientClockOffsetMsRef.current
      : currentRound?.endAt;
  const countdownText = formatCountdown(countdownTargetMs, nowMs);
  const headerTitle = roundTitleText(currentRound, language, snapshot?.uiMeta.marketTitle ?? t("refreshHint"));
  const canOpenUserManagement = Boolean(me?.permissionCodes.includes("users:list"));
  const canSelectViewUser = Boolean(me && (me.role === "Admin" || me.role === "Senior Tester" || me.role === "Test Engineer"));
  const effectiveViewUserId = viewUserId ?? me?.id;
  const currentViewedUser =
    (viewedUserId === effectiveViewUserId ? viewedUser : undefined) ??
    visibleViewUsers.find((user) => user.id === effectiveViewUserId) ??
    me;
  const isViewingSelf = !effectiveViewUserId || effectiveViewUserId === me?.id;
  const {
    tradeBusy,
    quickBusy,
    cancelBusyOrderId,
    sellBusyPositionId,
    sellFeedback,
    pendingOrderClientId,
    ensureViewingSelfForMutation,
    handlePlaceOrder,
    handleCloseSide,
    handleReverseSide,
    handleCancelOrder,
    handleSell
  } = useOrderActions({
    token,
    me,
    isViewingSelf,
    language,
    profile,
    orderAmount,
    orderQty,
    limitPrice,
    orderAction,
    selectedSide,
    orderKind,
    setSelectedSide,
    setError,
    setUserTradePayload,
    setLastOrderLatencyMs
  });

  useEffect(() => {
    setChartVisibleCount(defaultVisibleCountForInterval(selectedInterval));
  }, [selectedInterval]);

  useEffect(() => {
    if (!me) {
      setViewUserId(undefined);
      setVisibleViewUsers([]);
      return;
    }
    setViewUserId((current) => current ?? me.id);
  }, [me?.id]);

  const refreshVisibleViewUsers = useCallback(async () => {
    if (!token || !me) {
      setVisibleViewUsers([]);
      return;
    }
    if (!canSelectViewUser) {
      setVisibleViewUsers([me]);
      setViewUserId(me.id);
      return;
    }
    const users = await api.getUsers(token);
    const nextUsers = users.length ? users : [me];
    setVisibleViewUsers(nextUsers);
    setViewUserId((current) => {
      const nextId = current ?? me.id;
      return nextUsers.some((user) => user.id === nextId) ? nextId : me.id;
    });
  }, [token, me, canSelectViewUser]);

  const handleViewUserChange = (nextViewUserId: string) => {
    const nextViewedUser = visibleViewUsers.find((user) => user.id === nextViewUserId) ?? me;
    setViewUserId(nextViewUserId);
    setViewedUserTarget(nextViewUserId, nextViewedUser);
    setError(undefined);
  };

  useEffect(() => {
    if (!token || !me) {
      setVisibleViewUsers([]);
      return;
    }
    let cancelled = false;
    refreshVisibleViewUsers()
      .catch(() => {
        if (!cancelled) {
          setVisibleViewUsers([me]);
          setViewUserId(me.id);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [token, me, refreshVisibleViewUsers]);

  useEffect(() => {
    const timer = window.setInterval(() => setNowMs(Date.now()), 250);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    if (!token) {
      clientClockOffsetMsRef.current = 0;
      return;
    }
    let cancelled = false;
    const updateClockOffset = async () => {
      const offset = await api.sampleClockOffset().catch(() => undefined);
      if (!cancelled && typeof offset === "number") {
        clientClockOffsetMsRef.current = offset;
      }
    };
    void updateClockOffset();
    const timer = window.setInterval(() => void updateClockOffset(), 30_000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [token]);

  const updateRealtimeChannel = useCallback((
    channel: RealtimeChannel,
    patch: Partial<RealtimeChannelStatus>,
    options?: { now?: number; force?: boolean; failure?: boolean; recoverPayloads?: number }
  ) => {
    setRealtimeStatus((current) => ({
      ...current,
      [channel]: transitionRealtimeChannel(current[channel], patch, options)
    }));
  }, []);

  useEffect(() => {
    if (!token || !me) {
      setRealtimeStatus(initialRealtimeStatus());
      return;
    }

    let cancelled = false;
    const bootstrap = async () => {
      setBootstrapping(true);
      try {
        const bootstrapData = await api.getBootstrap(token, effectiveViewUserId);

        if (cancelled) {
          return;
        }

        i18n.changeLanguage(bootstrapData.me.language);
        setBootstrap(bootstrapData);
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
  }, [token, effectiveViewUserId, clearAuth, i18n, setBootstrap]);

  useMarketSocket({
    token,
    meId: me?.id,
    activeViewUserId: effectiveViewUserId,
    clientClockOffsetMsRef,
    setRealtimeStatus,
    updateRealtimeChannel,
    setMarketPayload,
    setMarketTickPayload,
    setMarketHistoryPatch,
    markMarketRenderCommit
  });

  useUserSocket({
    token,
    me,
    activeViewUserId: effectiveViewUserId,
    activeViewedUser: currentViewedUser,
    setRealtimeStatus,
    updateRealtimeChannel,
    setUserPayload,
    setUserTradePayload
  });

  const handleLogin = async (username: string, password: string) => {
    setError(undefined);
    try {
      const result = await api.login(username, password);
      setAuth(result.token, result);
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
      const activity = await api.getRoundActivity(token, item.roundId, effectiveViewUserId);
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

  const refreshAfterManualSettlement = async () => {
    if (!token || !me) {
      return;
    }
    const [roundData, nextHistory, nextProfile, nextPositions, nextOrders, nextOrderLifecycles, nextLogs] = await Promise.all([
      api.getCurrentRound(token, effectiveViewUserId),
      api.getHistory(token, 60, effectiveViewUserId),
      api.getProfile(token, effectiveViewUserId),
      api.getPositions(token, effectiveViewUserId),
      api.getOrders(token, effectiveViewUserId),
      api.getOrderLifecycles(token, effectiveViewUserId),
      api.getLogs(token, effectiveViewUserId)
    ]);
    setMarketPayload({
      viewedUserId: roundData.viewedUserId ?? effectiveViewUserId ?? me.id,
      currentRound: roundData.currentRound,
      history: nextHistory,
      snapshot: roundData.snapshot,
      settlementPreview: roundData.settlementPreview,
      transportMeta: roundData.transportMeta
    }, Date.now(), clientClockOffsetMsRef.current);
    setUserPayload({
      viewedUserId: effectiveViewUserId ?? me.id,
      viewedUser: currentViewedUser ?? me,
      profile: nextProfile,
      positions: nextPositions,
      orders: nextOrders,
      orderLifecycles: nextOrderLifecycles,
      logs: nextLogs
    });
  };

  const realtimeLabel = realtimeStatusLabel(realtimeStatus, language);
  const realtimeTone = realtimeStatusTone(realtimeStatus);
  const realtimeDetail = realtimeStatusDetail(realtimeStatus, nowMs, language);

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
    <div className={`app-shell page-${currentPage}`}>
      <header className="topbar">
        <div className="brand-block">
          <p className="eyebrow">{t("subtitle")}</p>
          <h1>{t("appTitle")} <small className="app-version-badge">{APP_VERSION_LABEL}</small></h1>
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
          <div className={`status-pill realtime-pill ${realtimeTone}`}>
            <span>{t("uiRealtimed6257476")}</span>
            <strong>{realtimeLabel}</strong>
          </div>
        </div>

        <div className="topbar-actions">
          {canSelectViewUser ? (
            <label className="view-user-control">
              <span>{t("uiViewb481c5fe")}</span>
              <select value={effectiveViewUserId ?? me.id} onChange={(event) => handleViewUserChange(event.target.value)}>
                {visibleViewUsers.map((user) => (
                  <option key={user.id} value={user.id}>
                    {user.username} / {user.role}
                  </option>
                ))}
              </select>
            </label>
          ) : null}
          {!isViewingSelf && currentViewedUser ? (
            <div className="view-user-chip">
              <strong>{currentViewedUser.displayName || currentViewedUser.username}</strong>
              <span>{t("uiReadOnly3f760b7f")}</span>
            </div>
          ) : null}
          <nav className="page-tabs">
            <button className={currentPage === "trade" ? "active" : ""} onClick={() => setCurrentPage("trade")}>
              {t("trade")}
            </button>
            <button className={currentPage === "home" ? "active" : ""} onClick={() => setCurrentPage("home")}>
              {t("home")}
            </button>
            <button className={currentPage === "profile" ? "active" : ""} onClick={() => setCurrentPage("profile")}>
              {t("profile")}
            </button>
            <button className={currentPage === "logs" ? "active" : ""} onClick={() => setCurrentPage("logs")}>
              {t("auditSearch")}
            </button>
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

      {error ? <div className="error-banner">{redactNetworkAddresses(error)}</div> : null}
      {bootstrapping ? <div className="loading-banner">{t("bootstrapping")}</div> : null}

      <main className="page-grid">
        {currentPage === "trade" ? (
          <TradePageRestored
            t={t}
            nowMs={nowMs}
            me={me}
            viewedUser={currentViewedUser}
            isViewingSelf={isViewingSelf}
            currentRound={currentRound}
            settlementPreview={settlementPreview}
            currentPage={currentPage}
            history={history}
            snapshot={snapshot}
            profile={profile}
            positions={positions}
            orders={orders}
            logs={logs}
            language={(i18n.language as Language) ?? "zh-CN"}
            selectedSide={selectedSide}
            selectedInterval={selectedInterval}
            chartVisibleCount={chartVisibleCount}
            lastOrderLatencyMs={lastOrderLatencyMs}
            lastMarketRecvTs={lastMarketRecvTs}
            clientClockOffsetMs={clientClockOffsetMsRef.current}
            countdownTargetMs={countdownTargetMs}
            realtimeLabel={realtimeLabel}
            realtimeTone={realtimeTone}
            realtimeDetail={realtimeDetail}
            orderAmount={orderAmount}
            orderQty={orderQty}
            limitPrice={limitPrice}
            orderAction={orderAction}
            orderKind={orderKind}
            tradeBusy={tradeBusy}
            quickBusy={quickBusy}
            pendingOrderClientId={pendingOrderClientId}
            sellBusyPositionId={sellBusyPositionId}
            sellFeedback={sellFeedback}
            canPlaceOrder={isViewingSelf && me.permissionCodes.includes("trade:order")}
            canSell={isViewingSelf && me.permissionCodes.includes("trade:sell")}
            onAmountChange={setOrderAmount}
            onQtyChange={setOrderQty}
            onLimitPriceChange={setLimitPrice}
            onOrderActionChange={setOrderAction}
            onOrderKindChange={setOrderKind}
            onIntervalChange={setSelectedInterval}
            onChartVisibleCountChange={setChartVisibleCount}
            onSelectSide={setSelectedSide}
            onPlaceOrder={handlePlaceOrder}
            onCloseSide={handleCloseSide}
            onReverseSide={handleReverseSide}
            onSell={handleSell}
            onCancel={handleCancelOrder}
            onTimeline={handleOpenTimeline}
            onNavigate={setCurrentPage}
            onLanguageChange={handleLanguageChange}
            onLogout={clearAuth}
            timelineBusyOrderId={timelineBusyOrderId}
            cancelBusyOrderId={cancelBusyOrderId}
          />
        ) : currentPage === "home" ? (
          <PersonalHomePage
            t={t}
            token={token}
            me={me}
            language={(i18n.language as Language) ?? "zh-CN"}
            profile={profile}
            positions={positions}
            logs={logs}
            canOpenUserManagement={canOpenUserManagement}
            userManagementSlot={
              canOpenUserManagement ? (
                <UserManagementPage
                  t={t}
                  token={token}
                  me={me}
                  language={(i18n.language as Language) ?? "zh-CN"}
                  embedded
                  onProfileRefresh={async () => {
                    const nextMe = await api.getMe(token);
                    setUser(nextMe);
                  }}
                  onUsersChanged={refreshVisibleViewUsers}
                />
              ) : undefined
            }
            onProfileRefresh={async () => {
              const nextMe = await api.getMe(token);
              setUser(nextMe);
            }}
            onUserUpdated={setUser}
          />
        ) : currentPage === "profile" ? (
          <AnalyticsPage
            t={t}
            token={token}
            language={(i18n.language as Language) ?? "zh-CN"}
            profile={profile}
            history={history}
            operatedHistory={operatedHistory}
            positions={positions}
            orders={orders}
            orderLifecycles={orderLifecycles}
            logs={logs}
            viewUsers={visibleViewUsers}
            viewedUserId={effectiveViewUserId}
            viewedUser={currentViewedUser}
            isViewingSelf={isViewingSelf}
            onViewUserChange={handleViewUserChange}
            onSell={handleSell}
            onTimeline={handleOpenTimeline}
            onOpenRoundLogs={handleOpenRoundLogs}
            timelineBusyOrderId={timelineBusyOrderId}
            selectedRoundLogId={roundLogDialog?.item.roundId}
            roundLogBusyRoundId={roundLogBusyRoundId}
            canManualSettle={me.role === "Admin" && me.permissionCodes.includes("settlement:manual")}
            onManualSettlementComplete={refreshAfterManualSettlement}
          />
        ) : (
          <LogSearchPage
            t={t}
            token={token}
            me={me}
            viewedUserId={effectiveViewUserId}
            canExport={me.permissionCodes.includes("profile:view") || me.role === "Admin"}
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

type AnalyticsPeriod = "all" | "year" | "month" | "week" | "day" | "trades";
type AnalyticsResult = "WIN" | "LOSE" | "SOLD" | "OPEN" | "UNFILLED";
type AnalyticsTone = "positive" | "negative" | "neutral" | "warning";
type AnalyticsSettlementState = "SETTLED" | "UNSETTLED";
const ANALYTICS_INITIAL_TRADE_LIMIT = 200;
const ANALYTICS_TRADE_LIMIT_STEP = 200;
const ANALYTICS_QTY_EPSILON = 0.0001;
interface AnalyticsTradeRow {
  id: string;
  ts: number;
  exitTs?: number;
  result: AnalyticsResult;
  roundId: string;
  roundStartAt?: number;
  roundLabel: string;
  side: TradeSide;
  invested: number;
  entryPrice: number;
  settlementPrice?: number;
  shares: number;
  fees: number;
  pnl: number;
  analysisText: string;
  analysisTone: AnalyticsTone;
  settlementState: AnalyticsSettlementState;
}

function roundAnalyticsMoney(value: number) {
  return Math.round(value * 100) / 100;
}

function lifecycleEntryPrice(log: OrderLifecycleRecord, order?: OrderRecord) {
  if (typeof log.actualFillPrice === "number") {
    return log.actualFillPrice;
  }
  if (typeof log.entryTokenPrice === "number") {
    return log.entryTokenPrice;
  }
  if (typeof order?.avgFillPrice === "number") {
    return order.avgFillPrice;
  }
  const entryFee = log.entryFee ?? 0;
  return Math.max(log.positionNotional - entryFee, 0) / Math.max(log.volumeTokenQty, ANALYTICS_QTY_EPSILON);
}

function lifecycleResult(log: OrderLifecycleRecord): AnalyticsResult {
  if (log.remainingTokenQty > ANALYTICS_QTY_EPSILON) {
    return "OPEN";
  }
  if (log.settlementResult === "win") {
    return "WIN";
  }
  if (log.settlementResult === "loss") {
    return "LOSE";
  }
  return log.closedTokenQty > ANALYTICS_QTY_EPSILON ? "SOLD" : "OPEN";
}

function lifecyclePnl(log: OrderLifecycleRecord, position?: PositionRecord) {
  const volumeQty = Math.max(log.volumeTokenQty, ANALYTICS_QTY_EPSILON);
  const closedQty = Math.min(Math.max(log.closedTokenQty, 0), volumeQty);
  const closedCost = (log.positionNotional * closedQty) / volumeQty;
  const realizedPnl = (log.exitNotional ?? 0) - (log.exitFee ?? 0) - closedCost;
  const openPnl = log.remainingTokenQty > ANALYTICS_QTY_EPSILON && position ? positionDisplayedPnl(position) : 0;
  return roundAnalyticsMoney(realizedPnl + openPnl);
}

function inferAnalyticsRoundStartAt(input: { round?: HistoryRound; roundId: string; marketSlug?: string; fallbackTs?: number }) {
  if (typeof input.round?.startAt === "number") {
    return input.round.startAt;
  }
  const slugStartAt = parseBtcFiveMinuteSlugStart(input.marketSlug) ?? parseBtcFiveMinuteSlugStart(input.roundId);
  if (typeof slugStartAt === "number") {
    return slugStartAt;
  }
  const fallbackTs = input.fallbackTs;
  if (typeof fallbackTs === "number" && Number.isFinite(fallbackTs)) {
    return Math.floor(fallbackTs / (5 * 60_000)) * (5 * 60_000);
  }
  return undefined;
}

function buildAnalyticsRows(
  history: HistoryRound[],
  positions: PositionRecord[],
  orders: OrderRecord[],
  orderLifecycles: OrderLifecycleRecord[],
  language: Language
) {
  const rows: AnalyticsTradeRow[] = [];
  const roundsById = new Map(history.map((round) => [round.id, round]));
  const ordersById = new Map(orders.map((order) => [order.id, order]));
  const positionsByBuyOrderId = new Map(
    positions
      .filter((position) => position.buyOrderId)
      .map((position) => [position.buyOrderId!, position])
  );
  const lifecycleBuyOrderIds = new Set<string>();
  for (const log of orderLifecycles) {
    lifecycleBuyOrderIds.add(log.buyOrderId);
    const round = roundsById.get(log.roundId);
    const position = positionsByBuyOrderId.get(log.buyOrderId);
    const order = ordersById.get(log.buyOrderId);
    const result = lifecycleResult(log);
    const settlementState = result === "OPEN" ? "UNSETTLED" : "SETTLED";
    const analysis = analyticsRowAnalysis(result, language);
    const exitTs =
      log.closedTokenQty > ANALYTICS_QTY_EPSILON || result !== "OPEN"
        ? log.settlementTimeMs ?? log.updatedAt
        : undefined;
    const roundStartAt = inferAnalyticsRoundStartAt({
      round,
      roundId: log.roundId,
      marketSlug: log.marketSlug ?? order?.marketSlug,
      fallbackTs: log.orderTimestampMs
    });
    rows.push({
      id: `lifecycle:${log.id}`,
      ts: log.orderTimestampMs,
      exitTs,
      result,
      roundId: log.roundId,
      roundStartAt,
      roundLabel: analyticsRoundLabel(roundStartAt),
      side: log.direction,
      invested: log.positionNotional,
      entryPrice: lifecycleEntryPrice(log, order),
      settlementPrice: typeof log.exitTokenPrice === "number" ? log.exitTokenPrice : undefined,
      shares: log.volumeTokenQty,
      fees: (log.entryFee ?? 0) + (log.exitFee ?? 0),
      pnl: lifecyclePnl(log, position),
      analysisText: analysis.text,
      analysisTone: analysis.tone,
      settlementState
    });
  }

  const ordersByRoundSide = new Map<string, OrderRecord[]>();
  for (const order of orders) {
    const key = `${order.roundId}:${order.side}`;
    const next = ordersByRoundSide.get(key) ?? [];
    next.push(order);
    ordersByRoundSide.set(key, next);
  }
  for (const position of positions) {
    if (position.buyOrderId && lifecycleBuyOrderIds.has(position.buyOrderId)) {
      continue;
    }
    const relatedOrders = ordersByRoundSide.get(`${position.roundId}:${position.side}`) ?? [];
    const fees = relatedOrders.reduce((sum, order) => sum + (order.actualFee ?? order.estimatedFee ?? 0), 0);
    const result: AnalyticsResult =
      position.status === "open"
        ? "OPEN"
        : position.settlementResult === "win"
          ? "WIN"
          : position.settlementResult === "sold"
            ? "SOLD"
            : "LOSE";
    const round = roundsById.get(position.roundId);
    const settlementState = result === "OPEN" ? "UNSETTLED" : "SETTLED";
    const analysis = analyticsRowAnalysis(result, language);
    const relatedBuyOrder = position.buyOrderId ? ordersById.get(position.buyOrderId) : undefined;
    const roundStartAt = inferAnalyticsRoundStartAt({
      round,
      roundId: position.roundId,
      marketSlug: relatedBuyOrder?.marketSlug ?? relatedOrders.find((order) => order.marketSlug)?.marketSlug,
      fallbackTs: position.openedAt
    });
    rows.push({
      id: `position:${position.id}`,
      ts: position.closedAt ?? position.openedAt,
      result,
      roundId: position.roundId,
      roundStartAt,
      roundLabel: analyticsRoundLabel(roundStartAt),
      side: position.side,
      invested: position.notionalSpent,
      entryPrice: position.averageEntry,
      settlementPrice: settlementState === "SETTLED" ? position.currentMark : undefined,
      shares: position.qty,
      fees,
      pnl: positionDisplayedPnl(position),
      analysisText: analysis.text,
      analysisTone: analysis.tone,
      settlementState
    });
  }
  for (const order of orders) {
    if (order.status !== "failed" || !isClobDepthFailure(order)) {
      continue;
    }
    const round = roundsById.get(order.roundId);
    const analysis = analyticsRowAnalysis("UNFILLED", language);
    const roundStartAt = inferAnalyticsRoundStartAt({
      round,
      roundId: order.roundId,
      marketSlug: order.marketSlug,
      fallbackTs: order.createdAt
    });
    rows.push({
      id: `order:${order.id}`,
      ts: order.createdAt,
      result: "UNFILLED",
      roundId: order.roundId,
      roundStartAt,
      roundLabel: analyticsRoundLabel(roundStartAt),
      side: order.side,
      invested: order.notionalUsdc,
      entryPrice: order.limitPrice ?? order.bestAsk ?? order.midPrice ?? 0,
      settlementPrice: undefined,
      shares: order.expectedQty,
      fees: order.actualFee ?? order.estimatedFee ?? 0,
      pnl: 0,
      analysisText: analysis.text,
      analysisTone: analysis.tone,
      settlementState: "UNSETTLED"
    });
  }
  return rows.sort((left, right) => right.ts - left.ts);
}

function filterAnalyticsPeriod(rows: AnalyticsTradeRow[], period: AnalyticsPeriod) {
  const now = Date.now();
  const cutoff =
    period === "day"
      ? now - 24 * 60 * 60_000
      : period === "week"
        ? now - 7 * 24 * 60 * 60_000
        : period === "month"
          ? now - 30 * 24 * 60 * 60_000
          : period === "year"
            ? now - 365 * 24 * 60 * 60_000
            : undefined;
  if (period === "trades") {
    return rows;
  }
  if (typeof cutoff !== "number") {
    return rows;
  }
  return rows.filter((row) => row.ts >= cutoff);
}

function analyticsSummary(rows: AnalyticsTradeRow[]) {
  const closedRows = rows.filter((row) => row.result !== "OPEN" && row.result !== "UNFILLED");
  const wins = closedRows.filter((row) => row.result === "WIN").length;
  const losses = closedRows.filter((row) => row.result === "LOSE").length;
  const totalPnl = closedRows.reduce((sum, row) => sum + row.pnl, 0);
  const totalFees = rows.reduce((sum, row) => sum + row.fees, 0);
  const bestTrade = closedRows.reduce((best, row) => Math.max(best, row.pnl), Number.NEGATIVE_INFINITY);
  const worstTrade = closedRows.reduce((worst, row) => Math.min(worst, row.pnl), Number.POSITIVE_INFINITY);
  return {
    totalPnl,
    totalFees,
    wins,
    losses,
    trades: closedRows.length,
    winRate: closedRows.length > 0 ? wins / closedRows.length : 0,
    bestTrade: Number.isFinite(bestTrade) ? bestTrade : 0,
    worstTrade: Number.isFinite(worstTrade) ? worstTrade : 0
  };
}

function analyticsRoundLabel(roundStartAt?: number) {
  return roundStartAt ? `HT-${dateTimeText(roundStartAt)}` : "HT--";
}

function analyticsPeriodLabel(period: AnalyticsPeriod, _language: Language) {
  const labels: Record<AnalyticsPeriod, string> = {
    all: "all",
    year: "uiYearf0aa55a9",
    month: "uiMonth63bb45a9",
    week: "uiWeek401ffc0f",
    day: "uiDay26f5a9c1",
    trades: "uiTradese772f691"
  };
  return t(labels[period]);
}

function analyticsDateQueryErrorLabel(error: AnalyticsDateQueryError, _language: Language) {
  const labels: Record<AnalyticsDateQueryError, string> = {
    year: "uiYearQueryMustUseYYYY1c737c52",
    month: "uiMonthQueryMustUseYYYYMM329dbe3f",
    day: "uiDayQueryMustUseYYYYMMDDc9ef9c54"
  };
  return t(labels[error]);
}

function analyticsResultLabel(result: AnalyticsResult, _language: Language) {
  const labels: Record<AnalyticsResult, string> = {
    WIN: "uiWin91a0fd19",
    LOSE: "uiLoss75805320",
    SOLD: "uiSolda25a1efa",
    OPEN: "uiOpen1f841e78",
    UNFILLED: "uiUnfilled431c6a2e"
  };
  return t(labels[result]);
}

function analyticsSettlementLabel(state: AnalyticsSettlementState, language: Language) {
  return state === "SETTLED"
    ? t("uiSettled3f248bb9")
    : t("uiUnsettledec860135");
}

function analyticsRowAnalysis(result: AnalyticsResult, language: Language): { text: string; tone: AnalyticsTone } {
  if (result === "WIN") {
    return {
      tone: "positive",
      text: t("uiProfitWasRealizedEntryPriceAlignedadf7fffc")
    };
  }
  if (result === "LOSE") {
    return {
      tone: "negative",
      text: t("uiTheSideDidNotResolveReview471c0db2")
    };
  }
  if (result === "SOLD") {
    return {
      tone: "warning",
      text: t("uiExitedBeforeSettlementCheckExitDiscipline0b506909")
    };
  }
  if (result === "UNFILLED") {
    return {
      tone: "warning",
      text: t("uiBookDepthWasInsufficientTheOrderdf938091")
    };
  }
  return {
    tone: "neutral",
    text: t("uiStillInItsLifecycleWaitForae544a7c")
  };
}

function isClobDepthFailure(order: OrderRecord) {
  return typeof order.failureReason === "string" && /insufficient CLOB depth/i.test(order.failureReason);
}

function TradePageRestored(props: {
  t: (key: string, options?: Record<string, unknown>) => string;
  language: Language;
  me?: PublicUser;
  viewedUser?: PublicUser;
  isViewingSelf: boolean;
  nowMs: number;
  currentRound?: RoundRecord;
  settlementPreview?: SettlementPreview;
  currentPage: "trade" | "home" | "profile" | "logs";
  history: HistoryRound[];
  snapshot?: MarketSnapshot;
  profile?: ProfileOverview;
  positions: PositionRecord[];
  orders: OrderRecord[];
  logs: AuditEvent[];
  selectedSide: TradeSide;
  selectedInterval: CandleInterval;
  chartVisibleCount: number;
  lastOrderLatencyMs?: number;
  lastMarketRecvTs?: number;
  clientClockOffsetMs: number;
  countdownTargetMs?: number;
  realtimeLabel: string;
  realtimeTone: string;
  realtimeDetail: string;
  orderAmount: string;
  orderQty: string;
  limitPrice: string;
  orderAction: OrderAction;
  orderKind: PaperOrderKind;
  tradeBusy: boolean;
  quickBusy: boolean;
  pendingOrderClientId?: string;
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
  onChartVisibleCountChange: (value: number) => void;
  onSelectSide: (side: TradeSide) => void;
  onPlaceOrder: () => Promise<void>;
  onCloseSide: (side?: TradeSide) => Promise<void>;
  onReverseSide: () => Promise<void>;
  onSell: (positionId: string) => Promise<void>;
  onCancel: (orderId: string) => Promise<void>;
  onTimeline: (orderId: string) => Promise<void>;
  onNavigate: (page: "trade" | "home" | "profile" | "logs") => void;
  onLanguageChange: (language: Language) => Promise<void>;
  onLogout: () => void;
  timelineBusyOrderId?: string;
  cancelBusyOrderId?: string;
}) {
  const { t, snapshot, profile, positions, orders, selectedSide, selectedInterval, nowMs, language } = props;
  const [orderBookExpanded, setOrderBookExpanded] = useState(false);
  const [sharedChartDomain, setSharedChartDomain] = useState<{ startTs: number; endTs: number }>();
  const [sharedChartYZoom, setSharedChartYZoom] = useState(1);
  const [chartVisibleDraft, setChartVisibleDraft] = useState(String(props.chartVisibleCount));
  const [chartVisibleError, setChartVisibleError] = useState<string>();
  useEffect(() => {
    setChartVisibleDraft(String(props.chartVisibleCount));
    setChartVisibleError(undefined);
  }, [props.chartVisibleCount]);
  const handleChartTimeDomainChange = useCallback((domain: { startTs: number; endTs: number }) => {
    setSharedChartDomain((current) =>
      current?.startTs === domain.startTs && current?.endTs === domain.endTs ? current : domain
    );
  }, []);
  const currentRound = props.currentRound;
  const sourceBinance = snapshot?.sources.binance;
  const sourceCoinbase = snapshot?.sources.coinbase;
  const sourceClob = snapshot?.sources.clob;
  const currentRoundPositions = positions.filter((position) => position.roundId === currentRound?.id);
  const openSidePositions = currentRoundPositions.filter((position) => position.status === "open" && position.side === selectedSide);
  const selectedBinanceBars = snapshot?.binance.candlesByInterval[selectedInterval] ?? [];
  const selectedCoinbaseBars = snapshot?.coinbase.candlesByInterval[selectedInterval] ?? [];
  const chartBars = useMemo(() => filterBarsToRecentWindow(selectedBinanceBars), [selectedBinanceBars]);
  const coinbaseBars = useMemo(() => filterBarsToRecentWindow(selectedCoinbaseBars), [selectedCoinbaseBars]);
  const displayPrice = displayPriceForSide(snapshot, selectedSide);
  const upDisplayPrice = displayPriceForSide(snapshot, "UP");
  const downDisplayPrice = displayPriceForSide(snapshot, "DOWN");
  const parsedAmount = Number(props.orderAmount || 0);
  const parsedQty = Number(props.orderQty || 0);
  const parsedLimitPriceCents = parseLimitPriceCentsInput(props.limitPrice);
  const limitPriceError =
    props.orderKind === "limit" && typeof parsedLimitPriceCents !== "number"
      ? t("uiLimitPriceMustBeAWholea2031356")
      : undefined;
  const limitTokenPrice = typeof parsedLimitPriceCents === "number" ? parsedLimitPriceCents / 100 : undefined;
  const estimatedPrice = props.orderKind === "limit" ? limitTokenPrice ?? 0 : displayPrice;
  const estimatedQty = props.orderAction === "buy" ? (estimatedPrice > 0 ? parsedAmount / estimatedPrice : 0) : parsedQty;
  const feeRate = snapshot?.clob.marketInfo.platformFeeRate;
  const estimatedFee =
    typeof feeRate === "number" && snapshot?.clob.marketInfo.feeRateAvailable !== false && estimatedPrice > 0
      ? props.orderAction === "buy"
        ? (parsedAmount * feeRate * estimatedPrice * (1 - estimatedPrice)) / estimatedPrice
        : estimatedQty * feeRate * estimatedPrice * (1 - estimatedPrice)
      : undefined;
  const clobLatency = latencyFor(sourceClob, nowMs, props.lastMarketRecvTs, props.clientClockOffsetMs);
  const orderBook = selectedSide === "UP" ? snapshot?.orderBooks.UP : snapshot?.orderBooks.DOWN;
  const orderBookComponent = sourceComponent(sourceClob, "orderBook");
  const orderBookStale = isOrderBookBackendStale(orderBookComponent);
  const orderBookBackendLatency = orderBookBackendLatencyMs(orderBookComponent);
  const btcLatency = latencyFor(sourceBinance, nowMs, props.lastMarketRecvTs, props.clientClockOffsetMs);
  const coinbaseLatency = latencyFor(sourceCoinbase, nowMs, props.lastMarketRecvTs, props.clientClockOffsetMs);
  const countdownMs =
    typeof props.countdownTargetMs === "number"
      ? Math.max(props.countdownTargetMs - nowMs, 0)
      : snapshot?.uiMeta.countdownMs ?? 0;
  const countdownClass = countdownTone(countdownMs);
  const acceptingOrders = Boolean(snapshot?.uiMeta.acceptingOrders);
  const balanceWarning =
    props.orderAction === "buy" && parsedAmount + (estimatedFee ?? 0) > (profile?.availableUsdc ?? 0) + 0.0001
      ? t("uiInsufficientAvailableBalanceThisOrderWould6c2e619d", { p0: money(parsedAmount + (estimatedFee ?? 0)), p1: money(profile?.availableUsdc ?? 0) })
      : undefined;
  const tradeBlockReason = balanceWarning ?? limitPriceError;
  const openPositionsBySide = (["UP", "DOWN"] as TradeSide[]).reduce<Record<TradeSide, PositionRecord[]>>(
    (accumulator, side) => {
      accumulator[side] = currentRoundPositions.filter(
        (position) =>
          position.side === side &&
          position.status === "open" &&
          position.displayStatus !== "settled" &&
          position.displayStatus !== "sold"
      );
      return accumulator;
    },
    { UP: [], DOWN: [] }
  );
  const oppositeSide = selectedSide === "UP" ? "DOWN" : "UP";
  const tradeAvailability = buildTradeAvailability({
    language,
    currentRound,
    nowMs,
    selectedSide,
    orderAction: props.orderAction,
    canPlaceOrder: props.canPlaceOrder,
    canSell: props.canSell,
    acceptingOrders,
    tradeBlockReason,
    openSidePositions,
    orderBook,
    oppositeOrderBook: snapshot?.orderBooks[oppositeSide],
    parsedQty
  });
  const canTrade = props.orderAction === "buy" ? tradeAvailability.canBuy : tradeAvailability.canSell;
  const executeBlockReason = props.orderAction === "buy" ? tradeAvailability.buyReason : tradeAvailability.sellReason;
  const recentOrders = [...orders.filter((order) => isCurrentRoundOrder(order, currentRound))]
    .sort((left, right) => sortOrdersForTradingPage(left, right, currentRound))
    .slice(0, 16);
  const sellablePositionByBuyOrderId = new Map(
    currentRoundPositions
      .filter(
        (position) =>
          position.buyOrderId &&
          position.status === "open" &&
          position.displayStatus !== "settled" &&
          position.displayStatus !== "sold" &&
          position.qty > 0
      )
      .map((position) => [position.buyOrderId!, position])
  );
  const positionCards = (["UP", "DOWN"] as TradeSide[]).map((side) => {
    const sidePositions = openPositionsBySide[side];
    const qty = sidePositions.reduce((sum, position) => sum + position.qty, 0);
    const value = sidePositions.reduce(
      (sum, position) => sum + (position.currentValue ?? position.qty * position.currentMark),
      0
    );
    const entryNotional = sidePositions.reduce((sum, position) => sum + position.notionalSpent, 0);
    const entryQty = sidePositions.reduce((sum, position) => sum + position.qty, 0);
    const pnlSummary = summarizePositionPnl(sidePositions);
    const pnl = pnlSummary.markPnlUsdc;
    return {
      side,
      qty,
      value,
      averageEntry: entryQty > 0 ? entryNotional / entryQty : 0,
      pnl,
      pnlSummary,
      hasOpenPositions: sidePositions.some((position) => position.displayStatus === "open")
    };
  });
  const recentRounds: Array<RoundRecord & { settlementPreview?: SettlementPreview; userPnl?: number }> = [
    ...(currentRound ? [{ ...currentRound, userPnl: 0 }] : []),
    ...props.history.filter((round) => round.id !== currentRound?.id)
  ]
    .map((round) => ({
      ...round,
      settlementPreview:
        round.settlementPreview ??
        (props.settlementPreview?.roundId === round.id ? props.settlementPreview : undefined)
    }))
    .slice(0, 10);
  const closedRounds = props.history.filter((round) => Boolean(round.settledSide || round.redeemFinishTs || round.status === "Closed"));
  const wins = closedRounds.filter((round) => round.userPnl > 0).length;
  const losses = closedRounds.filter((round) => round.userPnl < 0).length;
  const recentOneHourPnl = closedRounds
    .filter((round) => nowMs - (round.redeemFinishTs ?? round.settlementTs ?? round.endAt) <= 60 * 60_000)
    .reduce((sum, round) => sum + round.userPnl, 0);
  const oddsChange = (() => {
    const series = snapshot?.clob.currentRoundUpPriceSeries ?? [];
    if (series.length < 2) return 0;
    return (series.at(-1)?.price ?? 0) - series[0].price;
  })();
  const doubleSideCost = (snapshot?.clob.bestBidAskSummary.UP.bestAsk ?? 0) + (snapshot?.clob.bestBidAskSummary.DOWN.bestAsk ?? 0);
  const binancePtbReference = isBtcReferencePrice(snapshot?.displayPriceToBeat)
    ? snapshot.displayPriceToBeat
    : currentRound?.binanceOpenPrice;
  const binancePtbSpread = referenceSpread(snapshot?.binance.spotPrice, binancePtbReference);
  const coinbasePtbReference = isBtcReferencePrice(snapshot?.coinbase.currentRoundOpenReference)
    ? snapshot.coinbase.currentRoundOpenReference
    : currentRound?.coinbaseOpenPrice;
  const coinbasePtbSpread = referenceSpread(snapshot?.coinbase.referencePrice, coinbasePtbReference);
  const commitChartVisibleDraft = () => {
    const nextValue = parseBarCountInput(chartVisibleDraft);
    if (typeof nextValue !== "number") {
      setChartVisibleError(t("uiEnterAWholeNumberFrom107463c881"));
      return;
    }
    setChartVisibleError(undefined);
    props.onChartVisibleCountChange(nextValue);
  };
  const strategy = buildStrategyHints({
    language,
    upPrice: upDisplayPrice,
    downPrice: downDisplayPrice,
    oddsChange,
    doubleSideCost
  });
  const riskAlerts = buildRiskAlerts({
    language,
    countdownMs,
    upPrice: upDisplayPrice,
    downPrice: downDisplayPrice,
    oddsChange,
    sources: [sourceBinance, sourceCoinbase, sourceClob],
    clobLatencyMs: clobLatency.marketUpdateAgeMs,
    nowMs
  });
  const marketUpdateAge = Math.max(
    clobLatency.marketUpdateAgeMs,
    btcLatency.marketUpdateAgeMs,
    coinbaseLatency.marketUpdateAgeMs
  );
  const sourceAgeMax = Math.max(
    clobLatency.sourceDataAgeMs,
    btcLatency.sourceDataAgeMs,
    coinbaseLatency.sourceDataAgeMs
  );
  const groupedAlerts = [
    { key: "market", label: t("uiMarketDatab5219cab") },
    { key: "trading", label: t("uiTradingRiskc096896a") },
    { key: "settlement", label: t("uiSettlementRiskfd276a2b") },
    { key: "system", label: t("uiSystemDelayeeffe398") }
  ].map((group) => ({ ...group, items: riskAlerts.filter((alert) => alert.group === group.key) })).filter((group) => group.items.length > 0);
  const latencyRows = [
    { label: t("uiMarketUpdateAgebf535c1f"), value: marketUpdateAge },
    { label: t("uiOldestSourceAge44acef60"), value: sourceAgeMax },
    { label: t("uiBackendCompute74f83d9a"), value: snapshot?.latencyBreakdown.serverComputeLatency },
    { label: t("uiFrontendTransport3df4869f"), value: snapshot?.latencyBreakdown.clientTransportLatency }
  ];
  const topLatency = [...latencyRows].sort((left, right) => (right.value ?? -1) - (left.value ?? -1))[0];
  const selectedSummary = snapshot?.clob.bestBidAskSummary[selectedSide];
  const spreadText =
    selectedSummary && selectedSummary.bestAsk > 0 && selectedSummary.bestBid > 0
      ? tokenPriceText(selectedSummary.bestAsk - selectedSummary.bestBid)
      : "--";
  const estimatedOrderFee = typeof estimatedFee === "number" ? money(estimatedFee, 4) : t("uiUnavailable250f247d");
  const healthRows = [
    { label: "CLOB", primary: t("uiFrontendStoreValueMs1faeec6b", { p0: Math.round(clobLatency.marketUpdateAgeMs) }), secondary: t("uiStateUpstreamTransportMs3c89b45e", { p0: clobComponentSummary(sourceClob, language), p1: Math.round(clobLatency.sourceDataAgeMs), p2: Math.round(clobLatency.backendToFrontendLatencyMs ?? 0) }), tone: sourceClob?.state ?? "stale" },
    { label: "BTC", primary: t("uiFrontendStoreValueMs1faeec6b", { p0: Math.round(btcLatency.marketUpdateAgeMs) }), secondary: t("uiStateUpstreamTransportMs3c89b45e", { p0: `${t("uiBinanceFeed2936ce51")} ${componentStateLabel(sourceBinance?.state, language)}`, p1: Math.round(btcLatency.sourceDataAgeMs), p2: Math.round(btcLatency.backendToFrontendLatencyMs ?? 0) }), tone: sourceBinance?.state ?? "stale" },
    { label: "CB", primary: t("uiFrontendStoreValueMs1faeec6b", { p0: Math.round(coinbaseLatency.marketUpdateAgeMs) }), secondary: t("uiStateUpstreamTransportMs3c89b45e", { p0: `${t("uiCoinbaseFeedb41e66a7")} ${componentStateLabel(sourceCoinbase?.state, language)}`, p1: Math.round(coinbaseLatency.sourceDataAgeMs), p2: Math.round(coinbaseLatency.backendToFrontendLatencyMs ?? 0) }), tone: sourceCoinbase?.state ?? "stale" },
    { label: "Gamma", primary: currentRound?.lastPollAt ? `${Math.round((nowMs - currentRound.lastPollAt) / 1000)}s` : "--", secondary: `${t("uiSettlementPollf8a550bd")} · ${t("uiFinalSettlement004ceaa1")}`, tone: currentRound?.status === "Manual" ? "manual" : "healthy" }
  ];
  const bookStatsFor = (side: TradeSide) => {
    const book = snapshot?.orderBooks[side];
    const totalBidQty = book?.bids.reduce((sum, level) => sum + level.qty, 0) ?? 0;
    const totalAskQty = book?.asks.reduce((sum, level) => sum + level.qty, 0) ?? 0;
    const denom = totalBidQty + totalAskQty;
    const obi = denom > 0 ? (totalBidQty - totalAskQty) / denom : 0;
    return { book, totalBidQty, totalAskQty, obi };
  };
  const orderBookTotals = { UP: bookStatsFor("UP"), DOWN: bookStatsFor("DOWN") };
  const displayPriceToBeat = isBtcReferencePrice(snapshot?.displayPriceToBeat) ? snapshot.displayPriceToBeat : undefined;
  const btcUsdMeta = [
    `CB ${money(snapshot?.coinbase.referencePrice ?? 0)}`,
    displayPriceToBeat
      ? `${ptbDisplayLabel(language, snapshot?.displayPriceToBeatSource)} ${btcMoneyOrDash(displayPriceToBeat)}`
      : undefined
  ].filter(Boolean).join(" · ");

  return (
    <section className="terminal-page">
      <div className="terminal-top">
        <div className="terminal-logo">
          <span>Hyper</span><strong>Terminal</strong><em>PAPER</em>
          <small className="terminal-version">{APP_VERSION_LABEL}</small>
        </div>
        <div className="terminal-top-nav">
          <button className={props.currentPage === "trade" ? "active" : ""} onClick={() => props.onNavigate("trade")}>{t("trade")}</button>
          <button className={props.currentPage === "home" ? "active" : ""} onClick={() => props.onNavigate("home")}>{t("home")}</button>
          <button className={props.currentPage === "profile" ? "active" : ""} onClick={() => props.onNavigate("profile")}>{t("profile")}</button>
          <button className={props.currentPage === "logs" ? "active" : ""} onClick={() => props.onNavigate("logs")}>{t("auditSearch")}</button>
        </div>
        <div className="terminal-top-mid">
          <span>BTC @{money(snapshot?.binance.spotPrice ?? 0, 2)}</span>
          <span>UP {tradeDisplayPriceText(upDisplayPrice)}</span>
          <span>DN {tradeDisplayPriceText(downDisplayPrice)}</span>
          <span className={`terminal-realtime-state ${props.realtimeTone}`} title={props.realtimeDetail}>
            {props.realtimeLabel}
          </span>
          <span>{currentRound ? roundTimeRangeText(currentRound) : "--"}</span>
        </div>
        <div className="terminal-top-right">
          <span>{t("uiEQ2583d76f")} {money(profile?.totalEquity ?? 0)}</span>
          <span className="terminal-green">{t("uiAVLc9c82dcd")} {money(profile?.availableUsdc ?? 0)}</span>
          {!props.isViewingSelf && props.viewedUser ? (
            <span className="terminal-readonly-badge">
              {props.viewedUser.displayName || props.viewedUser.username} · {t("uiReadOnly3f760b7f")}
            </span>
          ) : null}
          <span className="terminal-user-badge">
            <strong>{props.me?.displayName ?? "--"}</strong>
            <em>{props.me?.role ?? "--"}</em>
          </span>
          <select value={language} onChange={(event) => void props.onLanguageChange(event.target.value as Language)}>
            <option value="zh-CN">简体中文</option>
            <option value="en-US">English</option>
          </select>
          <button type="button" onClick={props.onLogout}>{t("logout")}</button>
        </div>
      </div>

      <div className="terminal-monitor">
        <div className="monitor-cell hot monitor-analytics">
          <small>{t("uiHTMove27a027f4")} <b>{oddsChange >= 0 ? "↑" : "↓"} {tradeDisplayPriceText(Math.abs(oddsChange))}</b></small>
          <strong>{tradeDisplayPriceText(upDisplayPrice)}</strong>
          <span>DN {tradeDisplayPriceText(downDisplayPrice)} · {t("uiTwoSideAskb5478767")} {tradeDisplayPriceText(doubleSideCost)}</span>
        </div>
        <div className="monitor-cell monitor-latency-breakdown">
          <small>{t("uiLatencySplit08e91cc3")}</small>
          <strong>{topLatency?.label ?? "--"} {typeof topLatency?.value === "number" ? `${Math.round(topLatency.value)}ms` : "--"}</strong>
          <div className="latency-mini-list">
            {latencyRows.map((row) => <span key={row.label}>{row.label}: {typeof row.value === "number" ? `${Math.round(row.value)}ms` : "--"}</span>)}
          </div>
        </div>
        <div className="monitor-cell compact monitor-balance">
          <small>{t("uiAssets42b9f464")}</small>
          <strong>
            <span><i>{t("uiTotalda091ab8")}</i>{money(profile?.totalEquity ?? 0)}</span>
            <span><i>{t("uiAvailablee21a3cf6")}</i>{money(profile?.availableUsdc ?? 0)}</span>
          </strong>
          <span>{t("uiUnreala70318be")} {signedMoney(profile?.unrealizedPnl ?? 0)}</span>
        </div>
        <div className="monitor-cell monitor-reference-spreads">
          <small>{t("uiReferenceSpread7f406f4d")}</small>
          <strong>
            <span>
              <i>BINANCE VS PTB</i>
              <b className={spreadToneClass(binancePtbSpread)}>{spreadDisplayText(binancePtbSpread)}</b>
            </span>
            <span>
              <i>Coinbase VS PTB</i>
              <b className={spreadToneClass(coinbasePtbSpread)}>{spreadDisplayText(coinbasePtbSpread)}</b>
            </span>
          </strong>
          <span>B PTB {btcMoneyOrDash(binancePtbReference)} · CB PTB {btcMoneyOrDash(coinbasePtbReference)}</span>
        </div>
        <div className={`monitor-timer monitor-round-state ${countdownClass}`}>
          <small>{currentRound?.status ?? "--"}</small>
          <strong>{formatCountdownSeconds(countdownMs, nowMs, "remainingMs")}</strong>
          <span>{snapshot?.uiMeta.marketSwitchState ?? "--"}</span>
        </div>
      </div>

      <div className="terminal-body">
        <aside className="terminal-left">
          <TerminalSection title={t("positions")} meta={String(currentRoundPositions.length)}>
            <div className="terminal-position-cards">
              {positionCards.map((card) => (
                <div className={`position-token-card ${card.side === "UP" ? "up" : "down"}`} key={card.side}>
                  <div>
                    <span>{card.side === "UP" ? "▲ UP" : "▼ DOWN"}</span>
                    <b>{decimal(card.qty, card.qty >= 100 ? 2 : 4)}</b>
                  </div>
                  <strong>{money(card.value)}</strong>
                  <em className="position-token-meta">
                    <span>{t("uiAvgPositionCostInclEntryFee30e6c149")} {tokenPriceText(card.averageEntry)}</span>
                    <span className={card.pnl >= 0 ? "terminal-green" : "terminal-red"}>
                      {(card.pnl >= 0 ? t("uiPnL7f1596bc") : t("uiPnL5f0b0267"))} {signedMoney(card.pnl)}
                    </span>
                  </em>
                  <PositionPnlBreakdown
                    language={language}
                    markPnlUsdc={card.pnlSummary.markPnlUsdc}
                    executablePnlUsdc={card.pnlSummary.executablePnlUsdc}
                    entryFeeUsdc={card.pnlSummary.entryFeeUsdc}
                    exitFeeUsdc={card.pnlSummary.exitFeeUsdc}
                    totalFeeUsdc={card.pnlSummary.totalFeeUsdc}
                  />
                  {card.hasOpenPositions ? (
                    <button
                      type="button"
                      onClick={() => {
                        props.onSelectSide(card.side);
                        void props.onCloseSide(card.side);
                      }}
                      disabled={!props.canSell || props.quickBusy}
                    >
                      {props.quickBusy ? t("loading") : t("uiCloseSide6ff81aa0")}
                    </button>
                  ) : null}
                </div>
              ))}
            </div>
          </TerminalSection>

          <TerminalSection title={t("thisRound")} meta={String(recentOrders.length)}>
            <div className="terminal-current-orders">
              {recentOrders.length === 0 ? (
                <div className="terminal-empty">{t("noData")}</div>
              ) : (
                recentOrders.map((order) => {
                  const canCancelOrder = order.orderKind === "limit" && order.status === "pending";
                  const sellablePosition = sellablePositionByBuyOrderId.get(order.id);
                  const canSellPosition = order.action === "buy" && order.status === "filled" && Boolean(sellablePosition);
                  const cancelBusy = props.cancelBusyOrderId === order.id;
                  const sellBusy = Boolean(sellablePosition && props.sellBusyPositionId === sellablePosition.id);
                  const statusLabel = order.status === "filled" ? "OK" : order.status.toUpperCase();
                  return (
                    <article className="terminal-current-order-card" key={order.id}>
                      <div className="terminal-current-order-main">
                        <span className="terminal-current-order-time">{timeText(order.createdAt).replace(" UTC", "")}</span>
                        <span className={`terminal-current-order-side terminal-current-order-side-${order.action}`}>
                          <b>{orderTradeLabel(order, language)}</b>
                          <small>{order.side === "UP" ? "▲UP" : "▼DN"}</small>
                        </span>
                        <span className="terminal-current-order-amount">{money(order.requestedAmountUsdc ?? order.notionalUsdc, 0)}</span>
                        <span className={`terminal-current-order-status terminal-current-order-status-${order.status}`}>
                          {statusLabel}
                        </span>
                      </div>
                      <div className="terminal-current-order-meta">
                        <div className="terminal-current-order-reference">
                          <strong>@{orderReferencePriceText(order, snapshot)}</strong>
                          <small>{orderPriceQualifier(order, language)}</small>
                        </div>
                        <div className="terminal-current-order-action">
                          {canCancelOrder ? (
                            <button
                              type="button"
                              className="terminal-order-action-button terminal-cancel-order-button"
                              disabled={cancelBusy}
                              onClick={() => props.onCancel(order.id)}
                              title={t("cancel")}
                            >
                              {cancelBusy ? t("loading") : t("cancel")}
                            </button>
                          ) : canSellPosition && sellablePosition ? (
                            <button
                              type="button"
                              className="terminal-order-action-button terminal-sell-position-button"
                              disabled={sellBusy}
                              onClick={() => props.onSell(sellablePosition.id)}
                              title={t("uiSellThisOrderLot5c8b2937")}
                            >
                              {sellBusy ? t("loading") : t("uiSellPosition72403b28")}
                            </button>
                          ) : (
                            <span className="terminal-current-order-action-placeholder">--</span>
                          )}
                        </div>
                      </div>
                    </article>
                  );
                })
              )}
            </div>
          </TerminalSection>

          <TerminalSection title={t("today")} meta={t("stats")}>
            <div className="terminal-stat-grid">
              <div><small>PnL</small><b>{signedMoney(profile?.realizedPnlToday ?? 0)}</b></div>
              <div><small>{t("winRate")}</small><b>{wins + losses > 0 ? compactPercent(wins / (wins + losses)) : "—"}</b></div>
              <div><small>{t("uiTrades938dd9be")}</small><b>{`${wins}W/${losses}L`}</b></div>
              <div><small>{t("uiLast1H97a93afe")}</small><b>{signedMoney(recentOneHourPnl)}</b></div>
            </div>
            <div className="terminal-round-dots">
              {recentRounds.map((round) => {
                const preview = round.settlementPreview;
                const outcome = recentRoundOutcome({ round, nowMs, language });
                const title = [
                  round.id,
                  `${t("status")}: ${round.status}`,
                  `${t("result")}: ${round.settledSide ?? (preview?.state === "preliminary" && preview.side ? `PRE-${preview.side}` : preview?.side) ?? "--"}`,
                  `${t("uiCloseTime13605431")}: ${dateTimeText(round.endAt)}`,
                  isOfficialPtbSource(round.priceToBeatSource) ? `PTB: ${btcMoneyOrDash(round.priceToBeat)}` : undefined,
                  `Gamma: ${round.settlementSource ?? (preview?.state === "preliminary" ? "pending" : "Gamma")}`,
                  preview?.tokenSide ? `Token: ${preview.tokenSide} (${tokenPriceText(preview.tokenSide === "UP" ? preview.upPrice : preview.downPrice)})` : undefined,
                  preview?.binanceSide ? `Binance pre: ${preview.binanceSide}` : undefined,
                  `Binance: ${typeof round.binanceClosePrice === "number" ? money(round.binanceClosePrice) : "--"}`,
                  `PnL: ${signedMoney(round.userPnl)}`
                ].filter(Boolean).join("\n");
                return <span key={round.id} className={outcome.className} title={title}>{outcome.label}</span>;
              })}
            </div>
          </TerminalSection>
        </aside>

        <main className={`terminal-center ${orderBookExpanded ? "book-expanded" : ""}`}>
          <div className="terminal-chart-block">
            <div className="chart-toolbar compact">
              <b>BTC/USD</b>
              {TRADE_INTERVAL_OPTIONS.map((interval) => <button key={interval} className={selectedInterval === interval ? "on" : ""} onClick={() => props.onIntervalChange(interval)}>{interval}</button>)}
              <div className="chart-count-group">
                {CHART_COUNT_OPTIONS.map((count) => (
                  <button
                    key={count}
                    className={props.chartVisibleCount === count ? "on" : ""}
                    onClick={() => {
                      setChartVisibleError(undefined);
                      props.onChartVisibleCountChange(count);
                    }}
                  >
                    {count}
                  </button>
                ))}
                <input
                  aria-label="visible candles"
                  aria-invalid={Boolean(chartVisibleError)}
                  type="number"
                  min={10}
                  max={200}
                  step={1}
                  value={chartVisibleDraft}
                  onChange={(event) => setChartVisibleDraft(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") {
                      commitChartVisibleDraft();
                    }
                  }}
                />
                {chartVisibleError ? <em>{chartVisibleError}</em> : null}
              </div>
              <span>{shortChartHint(language)}</span>
            </div>
            <CandlestickChart
              bars={chartBars}
              upColor="#00f0c0"
              downColor="#f03060"
              emptyText={t("noData")}
              priceToBeat={displayPriceToBeat}
              latestPrice={snapshot?.binance.spotPrice}
              round={currentRound}
              visibleCount={props.chartVisibleCount}
              onVisibleCountChange={props.onChartVisibleCountChange}
              onTimeDomainChange={handleChartTimeDomainChange}
              yZoom={sharedChartYZoom}
              onYZoomChange={setSharedChartYZoom}
            />
          </div>
          <OddsMiniChart series={snapshot?.clob.currentRoundUpPriceSeries ?? []} language={language} />
          {orderBookExpanded ? (
            <div className="terminal-orderbook-expanded">
              <div className="chart-toolbar compact">
                <b>{t("uiFullOrderBookb29712bb")}</b>
                <span>{t("uiLiveFullDepthInTheBTC99243afb")}</span>
              </div>
              <div className="orderbook-expanded-grid">
                {(["UP", "DOWN"] as TradeSide[]).map((side) => {
                  const { book, totalBidQty, totalAskQty, obi } = orderBookTotals[side];
                  return (
                    <section className="expanded-book" key={side}>
                      <header>
                        <strong>{side}</strong>
                        <span>{t("uiBidAsk3e52dad0")} {tokenPriceText(book?.bestBid ?? 0)} / {tokenPriceText(book?.bestAsk ?? 0)}</span>
                        <span className={`obi-pill ${obi >= 0 ? "up" : "down"}`}>OBI {decimal(obi, 3)}</span>
                      </header>
                      <div className="book-table-pair">
                        <div>
                          <b>{t("uiBidsf24a2fa4")}</b>
                          {(book?.bids ?? []).map((level, index) => <span key={`${side}-bid-${index}`}><em>{tokenPriceText(level.price)}</em><strong>{decimal(level.qty, 3)}</strong></span>)}
                        </div>
                        <div>
                          <b>{t("uiAsks08cdf28f")}</b>
                          {(book?.asks ?? []).map((level, index) => <span key={`${side}-ask-${index}`}><em>{tokenPriceText(level.price)}</em><strong>{decimal(level.qty, 3)}</strong></span>)}
                        </div>
                      </div>
                      <footer>
                        <span>{t("uiBidQty8f0f3e82")} {decimal(totalBidQty, 3)}</span>
                        <span>{t("uiAskQty00a5837a")} {decimal(totalAskQty, 3)}</span>
                        <span>{t("uiUpdated8505907f")} {timeText(book?.snapshotTs)}</span>
                      </footer>
                    </section>
                  );
                })}
              </div>
            </div>
          ) : (
            <div className="terminal-chart-block coinbase">
              <div className="chart-toolbar compact">
                <b>BTC / CB</b>
                <span>CB-Binance {signedMoney((snapshot?.coinbase.referencePrice ?? 0) - (snapshot?.binance.spotPrice ?? 0))}</span>
              </div>
              <CoinbaseComparisonChart
                bars={coinbaseBars}
                referencePrice={snapshot?.coinbase.referencePrice ?? 0}
                emptyText={t("noData")}
                visibleCount={props.chartVisibleCount}
                onVisibleCountChange={props.onChartVisibleCountChange}
                timeDomainStartTs={sharedChartDomain?.startTs}
                timeDomainEndTs={sharedChartDomain?.endTs}
                yZoom={sharedChartYZoom}
                onYZoomChange={setSharedChartYZoom}
              />
            </div>
          )}
          <div className="terminal-depth">
            {(["UP", "DOWN"] as TradeSide[]).map((side) => {
              const book = snapshot?.orderBooks[side];
              const bid = book?.bestBid ?? 0;
              const ask = book?.bestAsk ?? 0;
              const width = Math.max(Math.min((bid + ask) * 50, 100), 8);
              return (
                <div key={side}>
                  <small>
                    BTC {side} Book
                    <button type="button" onClick={() => setOrderBookExpanded((value) => !value)}>
                      {orderBookExpanded ? t("uiHide0be8b81a") : t("uiOpen11c00628")}
                    </button>
                  </small>
                  <div className="terminal-depth-bar"><span style={{ width: `${width}%` }} /></div>
                  <b>{tokenPriceText(bid)} / {tokenPriceText(ask)}</b>
                </div>
              );
            })}
          </div>
        </main>

        <aside className="terminal-right">
          <TerminalSection title={t("systemHealthAlerts")} meta="LIVE">
            <div className="health-grid">
              {healthRows.map((item) => (
                <div className={`health-dot ${item.tone}`} key={item.label}>
                  <div className="health-dot-head"><b>{item.label}</b><strong>{item.primary}</strong></div>
                  <span>{item.secondary}</span>
                </div>
              ))}
            </div>
            <div className="stability-meter">{[1, 2, 3, 4, 5].map((step) => <span key={step} className={step <= healthRows.filter((row) => row.tone === "healthy").length + 1 ? "on" : ""} />)}</div>
            <div className="risk-alerts">
              {groupedAlerts.length === 0 ? <span>{t("noActiveAlerts")}</span> : groupedAlerts.map((group) => (
                <div className="risk-alert-group" key={group.key}>
                  <header><b>{group.label}</b><em>{group.items.length}</em></header>
                  {group.items.map((alert) => (
                    <button key={alert.kind} className={`risk-alert ${alert.level}`}>
                      <span>{alert.text}</span>
                      {alert.detail ? <em>{alert.detail}</em> : null}
                    </button>
                  ))}
                </div>
              ))}
            </div>
          </TerminalSection>

          <TerminalSection title="BTC/USD" meta={btcUsdMeta}>
            <div className="terminal-order">
              <div className="order-odds">
                <button className={selectedSide === "UP" ? "active up" : "up"} onClick={() => props.onSelectSide("UP")}><span>▲ UP</span><b>{tradeDisplayPriceText(upDisplayPrice)}</b><em>{t("uiLatestTradeDisplayf79d82d5")}</em></button>
                <button className={selectedSide === "DOWN" ? "active down" : "down"} onClick={() => props.onSelectSide("DOWN")}><span>▼ DOWN</span><b>{tradeDisplayPriceText(downDisplayPrice)}</b><em>{t("uiLatestTradeDisplayf79d82d5")}</em></button>
              </div>
              <div className="terminal-segment action-segment">
                {(["buy", "sell"] as OrderAction[]).map((action) => <button key={action} className={props.orderAction === action ? "on" : ""} onClick={() => props.onOrderActionChange(action)}>{action === "buy" ? "BUY / ENTER" : "SELL / EXIT"}</button>)}
              </div>
              <div className="terminal-segment order-kind-segment">
                <button className={props.orderKind === "market" ? "on kind-market" : "kind-market"} onClick={() => props.onOrderKindChange("market")}><b>MARKET</b>{" "}<small>FOK</small></button>
                <button className={props.orderKind === "limit" ? "on kind-limit" : "kind-limit"} onClick={() => props.onOrderKindChange("limit")}><b>LIMIT</b>{" "}<small>GTC</small></button>
              </div>
              <div className="amount-grid">
                {[1, 5, 10, 20, 50].map((amount) => <button key={amount} onClick={() => props.onAmountChange(String(amount))}>{amount} USD</button>)}
                <button onClick={() => props.onAmountChange(String(Math.max((profile?.availableUsdc ?? 0) / 2, 0).toFixed(0)))}>1/2</button>
                <button onClick={() => props.onAmountChange(String(Math.max(profile?.availableUsdc ?? 0, 0).toFixed(0)))}>MAX</button>
              </div>
              <label className="terminal-input">
                <span>{props.orderAction === "buy" ? t("amount") : t("qty")}</span>
                <input value={props.orderAction === "buy" ? props.orderAmount : props.orderQty} onChange={(event) => props.orderAction === "buy" ? props.onAmountChange(event.target.value) : props.onQtyChange(event.target.value)} />
              </label>
              {props.orderKind === "limit" ? (
                <label className={`terminal-input ${limitPriceError ? "error" : ""}`}>
                  <span>{t("uiLimitb69c76f3")}</span>
                  <input
                    aria-invalid={Boolean(limitPriceError)}
                    type="number"
                    min={1}
                    max={99}
                    step={1}
                    value={props.limitPrice}
                    onChange={(event) => props.onLimitPriceChange(event.target.value)}
                  />
                  {limitPriceError ? <em className="terminal-input-error">{limitPriceError}</em> : null}
                </label>
              ) : null}
              <div className="terminal-price-note">
                <span>{t("uiDisplay86019eb3")}</span>
                <b>{tradeDisplayPriceText(displayPrice)}</b>
                <span>{t("slippageHint")} {spreadText}</span>
              </div>
              <div className="order-meta">
                <span>{t("uiFee11903579")} {estimatedOrderFee}</span>
                <span>{t("available")}: {money(profile?.availableUsdc ?? 0)}</span>
                <span>{t("estimatedQty")}: {decimal(estimatedQty, 4)}</span>
              </div>
              <button className={`execute ${selectedSide === "DOWN" ? "down" : "up"}`} disabled={!canTrade || props.tradeBusy} title={executeBlockReason} onClick={props.onPlaceOrder}>
                {props.tradeBusy ? t("loading") : props.orderAction === "buy" ? `BUY ${selectedSide}` : `SELL ${selectedSide}`}
              </button>
              {props.pendingOrderClientId ? (
                <div className="inline-info-banner compact-feedback" role="status">
                  <strong>{t("uiOrderSubmitteda80277a1")}</strong>
                  <span>{props.pendingOrderClientId.slice(0, 8)}</span>
                </div>
              ) : null}
              <div className="quick-row">
                <button disabled={!tradeAvailability.canCloseSide || props.quickBusy} title={tradeAvailability.closeSideReason} onClick={() => props.onCloseSide()}>{t("uiExit082fe47a")} {selectedSide}</button>
                <button disabled={!tradeAvailability.canReverseSide || props.quickBusy} title={tradeAvailability.reverseReason} onClick={props.onReverseSide}>{t("reverseSide")}</button>
              </div>
              {balanceWarning ? <div className="inline-error-banner compact-feedback">{balanceWarning}</div> : null}
              {orderBookStale ? (
                <div className="inline-warning-banner compact-feedback" role="status">
                  <strong>{t("orderBookStaleTitle")}</strong>
                  <span>{t("orderBookStaleWarning")} {typeof orderBookBackendLatency === "number" ? `${Math.round(orderBookBackendLatency)}ms` : ""}</span>
                </div>
              ) : null}
            </div>
          </TerminalSection>

          <TerminalSection title={t("strategyHints")} meta="RULES">
            <div className="strategy-list">
              {strategy.map((item) => <div key={item.label}><span>{item.label}</span><b>{item.value}</b></div>)}
            </div>
          </TerminalSection>
        </aside>
      </div>
    </section>
  );
}

function AnalyticsPage(props: {
  t: (key: string, options?: Record<string, unknown>) => string;
  token: string;
  language: Language;
  profile?: ProfileOverview;
  history: HistoryRound[];
  operatedHistory: HistoryRound[];
  positions: PositionRecord[];
  orders: OrderRecord[];
  orderLifecycles: OrderLifecycleRecord[];
  logs: AuditEvent[];
  viewUsers: PublicUser[];
  viewedUserId?: string;
  viewedUser?: PublicUser;
  isViewingSelf: boolean;
  onViewUserChange: (viewUserId: string) => void;
  onSell: (positionId: string) => Promise<void>;
  onTimeline: (orderId: string) => Promise<void>;
  onOpenRoundLogs: (item: RoundCalendarItem) => Promise<void>;
  timelineBusyOrderId?: string;
  selectedRoundLogId?: string;
  roundLogBusyRoundId?: string;
  canManualSettle: boolean;
  onManualSettlementComplete: () => Promise<void>;
}) {
  const { t, language } = props;
  type AnalyticsResultFilter = "ALL" | AnalyticsResult;
  const [period, setPeriod] = useState<AnalyticsPeriod>("all");
  const [direction, setDirection] = useState<"ALL" | TradeSide>("ALL");
  const [resultFilter, setResultFilter] = useState<AnalyticsResultFilter>("ALL");
  const [yearQuery, setYearQuery] = useState("");
  const [monthQuery, setMonthQuery] = useState("");
  const [dayQuery, setDayQuery] = useState("");
  const [dateFilter, setDateFilter] = useState<AnalyticsDateFilter>({ kind: "none" });
  const [dateQueryError, setDateQueryError] = useState<AnalyticsDateQueryError>();
  const [visibleTradeLimit, setVisibleTradeLimit] = useState(ANALYTICS_INITIAL_TRADE_LIMIT);
  useEffect(() => {
    setVisibleTradeLimit(ANALYTICS_INITIAL_TRADE_LIMIT);
  }, [dateFilter, direction, period, resultFilter]);
  const rows = useMemo(
    () => buildAnalyticsRows(props.history, props.positions, props.orders, props.orderLifecycles, language),
    [props.history, props.positions, props.orders, props.orderLifecycles, language]
  );
  const periodRows = useMemo(() => filterAnalyticsPeriod(rows, period), [rows, period]);
  const dateRows = useMemo(() => filterRowsByAnalyticsDate(periodRows, dateFilter), [dateFilter, periodRows]);
  const filteredRows = useMemo(
    () =>
      dateRows.filter((row) => {
        if (direction !== "ALL" && row.side !== direction) {
          return false;
        }
        if (resultFilter !== "ALL" && row.result !== resultFilter) {
          return false;
        }
        return true;
      }),
    [dateRows, direction, resultFilter]
  );
  const summary = useMemo(() => analyticsSummary(filteredRows), [filteredRows]);
  const displayedRows = useMemo(
    () => (period === "trades" ? filteredRows.slice(0, visibleTradeLimit) : filteredRows),
    [filteredRows, period, visibleTradeLimit]
  );
  const openPnlSummary = useMemo(
    () => summarizePositionPnl(props.positions.filter((position) => position.status === "open")),
    [props.positions]
  );
  const analyticsViewUsers = props.viewUsers.length ? props.viewUsers : props.viewedUser ? [props.viewedUser] : [];
  const periodOptions = [
    { id: "all", label: analyticsPeriodLabel("all", language) },
    { id: "year", label: analyticsPeriodLabel("year", language) },
    { id: "month", label: analyticsPeriodLabel("month", language) },
    { id: "week", label: analyticsPeriodLabel("week", language) },
    { id: "day", label: analyticsPeriodLabel("day", language) },
    { id: "trades", label: analyticsPeriodLabel("trades", language) }
  ] as const;
  const clearDateQuery = () => {
    setYearQuery("");
    setMonthQuery("");
    setDayQuery("");
    setDateFilter({ kind: "none" });
    setDateQueryError(undefined);
  };
  const handleDateSearch = () => {
    const result = resolveAnalyticsDateFilter({ year: yearQuery, month: monthQuery, day: dayQuery });
    if ("error" in result) {
      setDateQueryError(result.error);
      return;
    }
    setDateQueryError(undefined);
    setDateFilter(result.filter);
  };
  const handleDateQueryKeyDown = (event: ReactKeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Enter") {
      event.preventDefault();
      handleDateSearch();
    }
  };

  return (
    <section className="analytics-terminal-page">
      <div className="analytics-headband">
        <div className="analytics-head-copy">
          <b>{t("uiAnalytics2f2530c0")}</b>
          <span>{t("uiBTCPaperTradingLifecycleShownIn86c22f48")}</span>
        </div>
        <div className="analytics-head-meta">
          <span>{filteredRows.length} {t("uiRows308a8de9")}</span>
          <span>{summary.trades} {t("uiSettled3226da2e")}</span>
          <span>{summary.wins}W / {summary.losses}L</span>
        </div>
      </div>

      <div className="analytics-summary">
        <div className="analytics-card">
          <span>{t("totalPnl")}</span>
          <strong>{signedMoney(summary.totalPnl)}</strong>
          <small>
            {t("uiTotalEquity7ef16a8b")} {money(props.profile?.totalEquity ?? 0)}
            {" · "}
            {t("uiAvailablee21a3cf6")} {money(props.profile?.availableUsdc ?? 0)}
          </small>
        </div>
        <div className="analytics-card">
          <span>{t("winRate")}</span>
          <strong>{summary.trades > 0 ? compactPercent(summary.winRate) : "—"}</strong>
          <small>{summary.wins}W / {summary.losses}L</small>
        </div>
        <div className="analytics-card">
          <span>{t("uiTrades41bfdf39")}</span>
          <strong>{summary.trades}</strong>
          <small>{filteredRows.length} {t("uiRows308a8de9")}</small>
        </div>
        <div className="analytics-card">
          <span>{t("uiTotalFees3b9e0b7d")}</span>
          <strong>{money(summary.totalFees, 4)}</strong>
          <small>
            {t("uiPositionFees184502da")} {money(openPnlSummary.totalFeeUsdc, 4)}
          </small>
        </div>
        <div className="analytics-card">
          <span>{t("uiMarkPnL6430b836")}</span>
          <strong>{signedMoney(openPnlSummary.markPnlUsdc)}</strong>
          <small>{t("uiMidMarkFeeAdjustedWhenAvailablede0f2a8d")}</small>
        </div>
        <div className="analytics-card">
          <span>{t("uiExecutablePnL89182743")}</span>
          <strong>{signedMoney(openPnlSummary.executablePnlUsdc)}</strong>
          <small>{t("uiBestBidExecutableView47023d16")}</small>
        </div>
        <div className="analytics-card">
          <span>{t("uiBestTrade14641cdd")}</span>
          <strong>{summary.trades > 0 ? signedMoney(summary.bestTrade) : "—"}</strong>
          <small>{t("uiBestSettledResulta2334c3e")}</small>
        </div>
        <div className="analytics-card">
          <span>{t("uiWorstTradef56901d7")}</span>
          <strong>{summary.trades > 0 ? signedMoney(summary.worstTrade) : "—"}</strong>
          <small>{t("uiWorstSettledResult16a0556d")}</small>
        </div>
      </div>

      <ManualSettlementQueue
        token={props.token}
        t={t}
        canManualSettle={props.canManualSettle}
        onManualSettlementComplete={props.onManualSettlementComplete}
      />

      <div className="analytics-controls">
        <div className="analytics-period-tabs">
          {periodOptions.map((option) => (
            <button
              key={option.id}
              className={period === option.id ? "active" : ""}
              onClick={() => {
                if (option.id === "all") {
                  clearDateQuery();
                }
                setPeriod(option.id);
              }}
            >
              {option.label}
            </button>
          ))}
        </div>
        <div className="analytics-date-query">
          <label>
            <span>{t("uiYear3d2f1630")}</span>
            <input
              value={yearQuery}
              onChange={(event) => setYearQuery(event.target.value)}
              onKeyDown={handleDateQueryKeyDown}
              placeholder="YYYY"
              inputMode="numeric"
            />
          </label>
          <label>
            <span>{t("uiMonthf01bab2e")}</span>
            <input
              value={monthQuery}
              onChange={(event) => setMonthQuery(event.target.value)}
              onKeyDown={handleDateQueryKeyDown}
              placeholder="YYYY-MM"
            />
          </label>
          <label>
            <span>{t("uiDaycbfdd519")}</span>
            <input
              value={dayQuery}
              onChange={(event) => setDayQuery(event.target.value)}
              onKeyDown={handleDateQueryKeyDown}
              placeholder="YYYY-MM-DD"
            />
          </label>
          <button type="button" className="secondary-button analytics-search-button" onClick={handleDateSearch}>
            {t("search")}
          </button>
        </div>
        <label>
          <span>{t("uiViewUserd33b758b")}</span>
          <select
            className="analytics-view-user-control"
            value={props.viewedUserId ?? props.viewedUser?.id ?? ""}
            onChange={(event) => props.onViewUserChange(event.target.value)}
          >
            {analyticsViewUsers.map((user) => (
              <option key={user.id} value={user.id}>
                {user.username} / {user.role}
              </option>
            ))}
          </select>
        </label>
        {!props.isViewingSelf && props.viewedUser ? (
          <span className="analytics-readonly-badge">
            {props.viewedUser.displayName || props.viewedUser.username} · {t("uiReadOnly3f760b7f")}
          </span>
        ) : null}
        <label>
          <span>{t("uiSymbol0c2a0a16")}</span>
          <select value="BTC" disabled>
            <option value="BTC">BTC</option>
          </select>
        </label>
        <label>
          <span>{t("uiDirection06ace5db")}</span>
          <select value={direction} onChange={(event) => setDirection(event.target.value as "ALL" | TradeSide)}>
            <option value="ALL">{t("all")}</option>
            <option value="UP">UP</option>
            <option value="DOWN">DOWN</option>
          </select>
        </label>
        <label>
          <span>{t("result")}</span>
          <select value={resultFilter} onChange={(event) => setResultFilter(event.target.value as AnalyticsResultFilter)}>
            <option value="ALL">{t("all")}</option>
            <option value="WIN">{analyticsResultLabel("WIN", language)}</option>
            <option value="LOSE">{analyticsResultLabel("LOSE", language)}</option>
            <option value="SOLD">{analyticsResultLabel("SOLD", language)}</option>
            <option value="OPEN">{analyticsResultLabel("OPEN", language)}</option>
            <option value="UNFILLED">{analyticsResultLabel("UNFILLED", language)}</option>
          </select>
        </label>
        {dateQueryError ? <span className="analytics-query-error">{analyticsDateQueryErrorLabel(dateQueryError, language)}</span> : null}
        <span>{t("uiAllTimesShownInUTCea3eff53")}</span>
      </div>

      <div className="analytics-table-panel">
        {displayedRows.length === 0 ? (
          <div className="analytics-empty">
            <b>{t("uiEmptya6461b8d")}</b>
            <div>{t("uiNoAnalyticsRowsMatchTheCurrent392ea37c")}</div>
          </div>
        ) : (
          <div className="analytics-table-wrap">
            <table>
              <thead>
                <tr>
                  <th>{t("uiEntryTime342d3edf")}</th>
                  <th>{t("uiExitSettleTime0e60f35d")}</th>
                  <th>{t("uiRound86342c68")}</th>
                  <th>{t("uiDirection06ace5db")}</th>
                  <th>{t("uiEntryCost65a0f840")}</th>
                  <th>{t("uiEntryPricecb585d00")}</th>
                  <th>{t("uiSettleExitbb76723b")}</th>
                  <th>{t("uiShares680c0dfe")}</th>
                  <th>{t("uiFees5ef20e69")}</th>
                  <th>PnL</th>
                  <th>{t("uiState4fbe6edf")}</th>
                  <th>{t("result")}</th>
                  <th>{t("uiAnalysiseaf5bdb2")}</th>
                </tr>
              </thead>
              <tbody>
                {displayedRows.map((row) => (
                  <tr key={row.id}>
                    <td>{dateTimeText(row.ts)}</td>
                    <td>{row.exitTs ? dateTimeText(row.exitTs) : "—"}</td>
                    <td>{row.roundLabel}</td>
                    <td><span className={`analytics-tag ${row.side === "UP" ? "up" : "down"}`}>{row.side}</span></td>
                    <td>{money(row.invested)}</td>
                    <td>{tokenPriceText(row.entryPrice)}</td>
                    <td>{typeof row.settlementPrice === "number" ? tokenPriceText(row.settlementPrice) : "—"}</td>
                    <td>{decimal(row.shares, 4)}</td>
                    <td>{money(row.fees, 4)}</td>
                    <td>{signedMoney(row.pnl)}</td>
                    <td>
                      <span className={`analytics-tag state-${row.settlementState.toLowerCase()}`}>
                        {analyticsSettlementLabel(row.settlementState, language)}
                      </span>
                    </td>
                    <td><span className={`analytics-tag result-${row.result.toLowerCase()}`}>{analyticsResultLabel(row.result, language)}</span></td>
                    <td><span className={`analytics-row-analysis ${row.analysisTone}`}>{row.analysisText}</span></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {period === "trades" && visibleTradeLimit < filteredRows.length ? (
          <button
            type="button"
            className="analytics-load-more global"
            onClick={() => setVisibleTradeLimit((limit) => limit + ANALYTICS_TRADE_LIMIT_STEP)}
          >
            {t("uiLoadValueMoreTrades26ad558e", { p0: Math.min(filteredRows.length - visibleTradeLimit, ANALYTICS_TRADE_LIMIT_STEP) })}
          </button>
        ) : null}
      </div>
    </section>
  );
}

function LogSearchPage(props: { t: (key: string, options?: Record<string, unknown>) => string; token: string; me: PublicUser; viewedUserId?: string; canExport: boolean }) {
  const { t, token, me } = props;
  const [filters, setFilters] = useState<Record<string, string>>({ system: "all", limit: "100", userId: props.viewedUserId ?? "" });
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
    me.role === "Senior Tester";

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
    viewUserId: props.viewedUserId,
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
    const nextFilters = { ...filters, userId: props.viewedUserId ?? "" };
    setFilters(nextFilters);
    void search("replace", nextFilters);
    if (canFilterUsers) {
      api.getUsers(token).then(setUsers).catch(() => setUsers([]));
    }
    api.getHistory(token, 200, props.viewedUserId).then(setRoundOptions).catch(() => setRoundOptions([]));
    api.getLogFacets(token).then(setFacets).catch(() => setFacets(DEFAULT_LOG_FACETS));
  }, [token, props.viewedUserId]);

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
    if (value === "coinbase") return "Coinbase";
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
      ? t("uiTradingLogsStoreTradingBehaviorSamples55427303")
      : selectedSystem === "matching"
        ? t("uiMatchingLogsStoreBookSyncExecution54ad6232")
        : t("uiAuditLogsStoreUserOperationsMatching102cca32");

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
              <span>{t("uiMainFields9332cc68")}</span>
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
    viewUserId: baseQuery.viewUserId,
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
          ? t("savedTo", { value: result.filePath })
          : t("uiExportDownloadStartedBrowserModeCannot545c30e7")
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
            {t("uiClose829762f5")}
          </button>
        </div>
        {message ? <div className="inline-info-banner">{message}</div> : null}
        <div className="inline-info-banner">
          {hasNativeSaveDialog
            ? t("uiTheDesktopAppOpensTheNative7ae7f7b5")
            : t("uiBrowserModeThePageCannotWrite6a6b6e7b")}
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

function BulkUserDialog(props: {
  t: (key: string, options?: Record<string, unknown>) => string;
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
    "username,password,displayName,role,language,managerUsername,availableUsdc,permissionLevel,mustChangePassword\n" +
    "tester_new_01,ChangeMe123,Tester New 01,Tester,zh-CN,,10000,Standard,true";
  const [sourceText, setSourceText] = useState("");
  const [localError, setLocalError] = useState<string>();
  const [preview, setPreview] = useState<BulkCreateUsersPreviewResult>();
  const [result, setResult] = useState<BulkCreateUsersResult>();
  const previewRows = preview?.valid ?? [];
  const previewFailed = preview?.failed ?? [];

  const downloadCsv = (text: string) => {
    const blob = new Blob([text], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = t("uiBulkUsersTemplateCsv42db64fc");
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
  };

  const previewCsv = async (text = sourceText) => {
    if (!text.trim()) {
      setLocalError(t("importOrPasteCsvTsvContentFirst"));
      setPreview(undefined);
      return;
    }
    try {
      props.setBusy(true);
      props.onError(undefined);
      setLocalError(undefined);
      setResult(undefined);
      setPreview(await api.previewBulkUsersCsv(token, text));
    } catch (previewError) {
      const message = previewError instanceof Error ? previewError.message : "CSV preview failed.";
      setPreview(undefined);
      setLocalError(message);
      props.onError(message);
    } finally {
      props.setBusy(false);
    }
  };

  const downloadTemplate = async () => {
    try {
      props.setBusy(true);
      props.onError(undefined);
      setLocalError(undefined);
      const text = await api.downloadBulkUsersTemplate(token);
      downloadCsv(text);
      setSourceText(text.replace(/^\uFEFF/, ""));
      setPreview(undefined);
      setResult(undefined);
    } catch (downloadError) {
      const message = downloadError instanceof Error ? downloadError.message : "Template download failed.";
      setLocalError(message);
      props.onError(message);
    } finally {
      props.setBusy(false);
    }
  };

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
        void previewCsv(text);
      })
      .catch(() => {
        setLocalError(t("failedToReadTheFile"));
      });
  };

  const submit = async () => {
    if (!sourceText.trim()) {
      setLocalError(t("importOrPasteCsvTsvContentFirst"));
      return;
    }
    if (!preview) {
      await previewCsv();
      return;
    }
    if (previewFailed.length > 0 || previewRows.length === 0) {
      setLocalError(t("fixImportErrorsBeforeCreatingUsers"));
      return;
    }
    try {
      props.setBusy(true);
      props.onError(undefined);
      setLocalError(undefined);
      const nextResult = await api.bulkCreateUsersCsv(token, sourceText);
      setResult(nextResult);
      if (nextResult.failed.length === 0) {
        await props.onCreated();
        setPreview(undefined);
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
            <p className="eyebrow">{t("bulkRegistration")}</p>
            <h2>{t("csvTsvUserImport")}</h2>
          </div>
          <button className="ghost-button compact-button" onClick={props.onClose}>
            {t("close")}
          </button>
        </div>
        <div className="dialog-section">
          <div className="button-row fit-actions">
            <button className="secondary-button compact-button" disabled={props.busy} onClick={downloadTemplate}>
              {t("uiDownloadTemplatede514b86")}
            </button>
            <label className="file-import-button">
              {t("chooseCsvTsvFile")}
              <input type="file" accept=".csv,.tsv,text/csv,text/tab-separated-values,text/plain" onChange={handleFile} />
            </label>
            <button
              className="ghost-button compact-button"
              disabled={props.busy}
              onClick={() => void previewCsv()}
            >
              {t("uiPreview2f1dbf47")}
            </button>
            <button
              className="ghost-button compact-button"
              disabled={props.busy}
              onClick={() => {
                setSourceText(template);
                setPreview(undefined);
                setResult(undefined);
                setLocalError(undefined);
              }}
            >
              {t("useTemplate")}
            </button>
          </div>
          <small className="muted-line">
            {t("uiDownloadTheTemplateFirstUploadOr21c933d6")}
          </small>
        </div>
        <div className="dialog-form">
          <label>
            {t("pasteCsvTsvText")}
            <textarea
              value={sourceText}
              onChange={(event) => {
                setSourceText(event.target.value);
                setPreview(undefined);
                setResult(undefined);
                setLocalError(undefined);
              }}
            />
          </label>
        </div>
        {localError ? <div className="inline-error-banner">{redactNetworkAddresses(localError)}</div> : null}
        {preview ? (
          <div className={previewFailed.length ? "inline-error-banner" : "inline-info-banner"}>
            {t("uiPreviewedValueRowsValueCreatableValuea14a203d", { p0: preview.total, p1: previewRows.length, p2: previewFailed.length })}
            {previewFailed.length
              ? ` ${previewFailed.map((item) => `#${item.rowNumber}: ${redactNetworkAddresses(item.error)}`).join("; ")}`
              : ""}
          </div>
        ) : null}
        {result ? (
          <div className={result.failed.length ? "inline-error-banner" : "inline-info-banner"}>
            {t("uiCreatedValueFailedValue3599531e", { p0: result.created.length, p1: result.failed.length })}
            {result.failed.length
              ? ` ${result.failed.map((item) => `#${item.rowNumber}: ${redactNetworkAddresses(item.error)}`).join("; ")}`
              : ""}
          </div>
        ) : null}
        <div className="dialog-table-shell">
          <table>
            <thead>
              <tr>
                <th>#</th>
                <th>{t("username")}</th>
                <th>{t("displayName")}</th>
                <th>{t("role")}</th>
                <th>{t("language")}</th>
                <th>{t("uiGroupManager8e43c165")}</th>
                <th>{t("available")}</th>
                <th>{t("validation")}</th>
              </tr>
            </thead>
            <tbody>
              {!preview || (previewRows.length === 0 && previewFailed.length === 0) ? (
                <tr>
                  <td colSpan={8}>{t("noData")}</td>
                </tr>
              ) : (
                [
                  ...previewRows.map((row) => ({
                    ...row,
                    status: "ready" as const,
                    error: ""
                  })),
                  ...previewFailed.map((row) => ({
                    rowNumber: row.rowNumber,
                    username: row.username ?? "",
                    displayName: "",
                    role: "Tester" as Role,
                    language: "zh-CN" as Language,
                    seniorTesterId: undefined,
                    managerUserId: undefined,
                    availableUsdc: undefined,
                    status: "failed" as const,
                    error: row.error
                  }))
                ].map((row) => (
                  <tr key={row.rowNumber}>
                    <td>{row.rowNumber}</td>
                    <td>{row.username || "--"}</td>
                    <td>{row.displayName || "--"}</td>
                    <td>{row.role ?? "Tester"}</td>
                    <td>{row.language ?? "zh-CN"}</td>
                    <td>{row.managerUserId ?? row.seniorTesterId ?? "--"}</td>
                    <td>{typeof row.availableUsdc === "number" ? money(row.availableUsdc) : t("default")}</td>
                    <td>
                      {row.status === "failed" ? (
                        <span className="tone-negative">{redactNetworkAddresses(row.error)}</span>
                      ) : (
                        <FieldChip label={t("ready")} tone="positive" />
                      )}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
        <div className="button-row dialog-actions">
          <button className="secondary-button" disabled={props.busy || previewFailed.length > 0 || previewRows.length === 0} onClick={submit}>
            {props.busy ? t("loading") : t("createUsers", { value: previewRows.length })}
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
  t: (key: string, options?: Record<string, unknown>) => string;
  token: string;
  me: PublicUser;
  language: Language;
  embedded?: boolean;
  onProfileRefresh: () => Promise<void>;
  onUsersChanged: () => Promise<void>;
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
  const [searchText, setSearchText] = useState("");
  const [roleFilter, setRoleFilter] = useState<"ALL" | Role>("ALL");
  const [editDialog, setEditDialog] = useState<{
    user: PublicUser;
    displayName: string;
    role: Role;
    language: Language;
    permissionLevel: PermissionLevel;
    availableUsdc: string;
    isActive: boolean;
  }>();
  const [groupDialog, setGroupDialog] = useState<{ user: PublicUser; managerUserId: string }>();
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
  const isGroupManager = me.role === "Senior Tester" || me.role === "Test Engineer";
  const canBulkCreate = me.permissionCodes.includes("users:bulk-create");
  const canUpdateUsers = me.permissionCodes.includes("users:update");
  const canCreateSingleUser = me.permissionCodes.includes("users:create") && (isAdmin || isGroupManager);

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

  const managerOptions = users.filter((user) => (user.role === "Senior Tester" || user.role === "Test Engineer") && user.isActive);
  const selectableManagerOptions =
    isGroupManager && !managerOptions.some((user) => user.id === me.id) ? [me, ...managerOptions] : managerOptions;
  const defaultManagerUserId = managerOptions.find((user) => user.username === "JDH1")?.id ?? "";
  const createRole = isAdmin ? form.role : "Tester";
  const createManagerUserId = createRole === "Tester" ? (isAdmin ? form.seniorTesterId || defaultManagerUserId : me.id) : undefined;
  const canManageTarget = (user: PublicUser) =>
    isAdmin || ((me.role === "Senior Tester" || me.role === "Test Engineer") && user.role === "Tester" && (user.managerUserId ?? user.seniorTesterId) === me.id);
  const canChangeGroup = (user: PublicUser) => isAdmin && user.role === "Tester" && user.id !== me.id;
  const canSetBalance = (user: PublicUser) => canManageTarget(user) || (me.role === "Senior Tester" && user.id === me.id);
  const groupOptionLabel = (user: PublicUser) => `${user.username} / ${user.role} ${t("uiGroup5fc62521")}`;
  const groupLabelForUser = (user: PublicUser) => {
    if (user.role === "Admin") {
      return t("uiSystemAdmin59af7656");
    }
    if (user.role === "Senior Tester" || user.role === "Test Engineer") {
      return t("uiGroupManager51c384af");
    }
    const manager = users.find((candidate) => candidate.id === (user.managerUserId ?? user.seniorTesterId));
    return manager ? groupOptionLabel(manager) : t("uiUnassigned5675780c");
  };
  const visibleUsers = users.filter((user) => {
    const query = searchText.trim().toLowerCase();
    if (roleFilter !== "ALL" && user.role !== roleFilter) return false;
    if (!query) return true;
    return [user.username, user.displayName, user.role, user.permissionLevel ?? "Standard"].some((value) =>
      value.toLowerCase().includes(query)
    );
  });
  const activeCount = users.filter((user) => user.isActive).length;
  const managedCount = users.filter((user) => canManageTarget(user)).length;
  const roleCounts = users.reduce<Record<Role, number>>(
    (counts, user) => ({ ...counts, [user.role]: counts[user.role] + 1 }),
    { Tester: 0, "Senior Tester": 0, "Test Engineer": 0, Admin: 0 }
  );

  useEffect(() => {
    if (isAdmin && form.role === "Tester" && !form.seniorTesterId && defaultManagerUserId) {
      setForm((current) => ({ ...current, seniorTesterId: defaultManagerUserId }));
    } else if (!isAdmin && isGroupManager && form.seniorTesterId !== me.id) {
      setForm((current) => ({ ...current, role: "Tester", seniorTesterId: me.id }));
    }
  }, [defaultManagerUserId, form.role, form.seniorTesterId, isAdmin, isGroupManager, me.id]);

  const createUser = async () => {
    try {
      setBusy(true);
      setError(undefined);
      await api.createUser(token, {
        username: form.username,
        password: form.password,
        displayName: form.displayName,
        role: createRole,
        language: form.language,
        seniorTesterId: createManagerUserId,
        managerUserId: createManagerUserId,
        availableUsdc: Number(form.availableUsdc || 0)
      });
      setForm({
        username: "",
        password: "",
        displayName: "",
        role: "Tester",
        language: "zh-CN",
        seniorTesterId: isAdmin ? defaultManagerUserId : me.id,
        availableUsdc: "10000"
      });
      await loadUsers();
      await props.onUsersChanged();
    } catch (createError) {
      setError(createError instanceof Error ? createError.message : "Create user failed.");
    } finally {
      setBusy(false);
    }
  };

  const disableUser = async (user: PublicUser) => {
    const ok = window.confirm(t("disableAccount", { value: user.username }));
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
    const ok = window.confirm(t("restoreAccount", { value: user.username }));
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
      setError(t("theNewPasswordConfirmationDoesNotMatch"));
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
      setError(t("enterAValidAmount"));
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

  const openEditDialog = (user: PublicUser) => {
    setError(undefined);
    setEditDialog({
      user,
      displayName: user.displayName,
      role: user.role,
      language: user.language,
      permissionLevel: user.permissionLevel ?? "Standard",
      availableUsdc: String(user.availableUsdc),
      isActive: user.isActive
    });
  };

  const openGroupDialog = (user: PublicUser) => {
    setError(undefined);
    setGroupDialog({ user, managerUserId: user.managerUserId ?? user.seniorTesterId ?? defaultManagerUserId });
  };

  const submitEdit = async () => {
    if (!editDialog) return;
    const amount = Number(editDialog.availableUsdc);
    if (!Number.isFinite(amount) || amount < 0) {
      setError(t("enterAValidAmount"));
      return;
    }
    const payload: UpdateUserInput = {
      displayName: editDialog.displayName,
      role: editDialog.role,
      language: editDialog.language,
      permissionLevel: editDialog.permissionLevel,
      availableUsdc: amount,
      isActive: editDialog.isActive
    };
    if (editDialog.role !== "Tester") {
      payload.managerUserId = null;
      payload.seniorTesterId = null;
    }
    try {
      setBusy(true);
      setError(undefined);
      await api.updateUser(token, editDialog.user.id, payload);
      setEditDialog(undefined);
      await loadUsers();
      await props.onUsersChanged();
    } catch (editError) {
      setError(editError instanceof Error ? editError.message : "Update user failed.");
    } finally {
      setBusy(false);
    }
  };

  const submitGroupChange = async () => {
    if (!groupDialog) {
      return;
    }
    if (!groupDialog.managerUserId) {
      setError(t("uiSelectAGroupa92fb730"));
      return;
    }
    try {
      setBusy(true);
      setError(undefined);
      await api.changeUserGroup(token, groupDialog.user.id, groupDialog.managerUserId);
      setGroupDialog(undefined);
      await loadUsers();
      await props.onUsersChanged();
    } catch (groupError) {
      setError(groupError instanceof Error ? groupError.message : "Change group failed.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className={props.embedded ? "user-home-panel" : "panel log-search-panel"}>
      <div className="section-header">
        <div>
          <p className="eyebrow">{t("userScope")}</p>
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
              {t("bulkRegister")}
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
            {t("changeMyPassword")}
          </button>
          <button className="secondary-button" onClick={loadUsers} disabled={busy}>
            {busy ? props.t("loading") : props.t("search")}
          </button>
        </div>
      </div>
      {error ? <div className="inline-error-banner">{redactNetworkAddresses(error)}</div> : null}

      <div className="user-overview-grid">
        <div className="analytics-card"><span>{t("uiVisibleUsersa57e8dbb")}</span><strong>{users.length}</strong><small>{managedCount} {t("uiManageablec5ebe68f")}</small></div>
        <div className="analytics-card"><span>{t("uiActivecb6b213c")}</span><strong>{activeCount}</strong><small>{users.length - activeCount} {t("uiDisabled38d29ccb")}</small></div>
        <div className="analytics-card"><span>{t("uiTester2bdfc143")}</span><strong>{roleCounts.Tester}</strong><small>{roleCounts["Senior Tester"]} Senior</small></div>
        <div className="analytics-card"><span>{t("uiEngineerAdminb600f02c")}</span><strong>{roleCounts["Test Engineer"] + roleCounts.Admin}</strong><small>{roleCounts.Admin} Admin</small></div>
      </div>

      {canCreateSingleUser ? (
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
            {t("displayName")}
            <input value={form.displayName} onChange={(event) => setForm({ ...form, displayName: event.target.value })} />
          </label>
          <label>
            {props.t("role")}
            <select
              value={createRole}
              disabled={!isAdmin}
              onChange={(event) => setForm({ ...form, role: event.target.value as Role })}
            >
              {((isAdmin ? ["Tester", "Senior Tester", "Test Engineer", "Admin"] : ["Tester"]) as Role[]).map((role) => (
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
            {t("uiGroup5b2b11e5")}
            <select
              value={createManagerUserId ?? ""}
              onChange={(event) => setForm({ ...form, seniorTesterId: event.target.value })}
              disabled={!isAdmin || createRole !== "Tester"}
            >
              <option value="">{t("uiUnassignedSelect204b7154")}</option>
              {selectableManagerOptions.map((user) => (
                <option key={user.id} value={user.id}>
                  {groupOptionLabel(user)}
                </option>
              ))}
            </select>
          </label>
          <label>
            {props.t("available")}
            <input value={form.availableUsdc} onChange={(event) => setForm({ ...form, availableUsdc: event.target.value })} />
          </label>
          <button className="primary-button user-create-button" disabled={busy} onClick={createUser}>
            {t("create")}
          </button>
        </div>
      ) : null}

      <div className="user-scope-toolbar">
        <label>
          {t("search")}
          <input
            value={searchText}
            placeholder={t("uiUsernameDisplayNameRoleb230e989")}
            onChange={(event) => setSearchText(event.target.value)}
          />
        </label>
        <label>
          {props.t("role")}
          <select value={roleFilter} onChange={(event) => setRoleFilter(event.target.value as "ALL" | Role)}>
            <option value="ALL">{props.t("all")}</option>
            {(["Tester", "Senior Tester", "Test Engineer", "Admin"] as Role[]).map((role) => (
              <option key={role} value={role}>{role}</option>
            ))}
          </select>
        </label>
      </div>

      <table>
        <thead>
          <tr>
            <th>{props.t("username")}</th>
            <th>{t("displayName")}</th>
            <th>{props.t("role")}</th>
            <th>{props.t("status")}</th>
            <th>{t("uiGroup5b2b11e5")}</th>
            <th>{t("uiPermission410e3761")}</th>
            <th>{props.t("available")}</th>
            <th>{props.t("action")}</th>
          </tr>
        </thead>
        <tbody>
          {visibleUsers.length === 0 ? (
            <tr>
              <td colSpan={8}>{props.t("noData")}</td>
            </tr>
          ) : (
            visibleUsers.map((user) => {
              return (
                <tr key={user.id}>
                  <td>{user.username}</td>
                  <td>{user.displayName}</td>
                  <td>{user.role}</td>
                  <td>
                    <FieldChip
                      label={user.isActive ? t("active") : t("disabled")}
                      tone={user.isActive ? "positive" : "negative"}
                    />
                  </td>
                  <td>{groupLabelForUser(user)}</td>
                  <td>{user.permissionLevel ?? "Standard"}</td>
                  <td>{money(user.availableUsdc)}</td>
                  <td>
                    <div className="table-action-cell">
                      {canUpdateUsers && canManageTarget(user) && user.id !== me.id ? (
                        <button className="ghost-button compact-button" disabled={busy} onClick={() => openEditDialog(user)}>
                          {t("uiEditca7ff73d")}
                        </button>
                      ) : null}
                      {canChangeGroup(user) ? (
                        <button className="ghost-button compact-button" disabled={busy} onClick={() => openGroupDialog(user)}>
                          {t("uiMoveGroup188e294c")}
                        </button>
                      ) : null}
                      {canSetBalance(user) ? (
                        <button
                          className="ghost-button compact-button"
                          disabled={busy}
                          onClick={() => {
                            setError(undefined);
                            setBalanceDialog({ user, amount: String(user.availableUsdc) });
                          }}
                        >
                          {t("balance")}
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
                          {t("password")}
                        </button>
                      ) : null}
                      {canManageTarget(user) && user.id !== me.id ? (
                        user.isActive ? (
                          <button className="ghost-button compact-button" disabled={busy} onClick={() => disableUser(user)}>
                            {t("disable")}
                          </button>
                        ) : (
                          <button className="ghost-button compact-button" disabled={busy} onClick={() => enableUser(user)}>
                            {t("restore")}
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
      {editDialog ? (
        <div className="modal-backdrop">
          <div className="panel user-action-dialog user-profile-dialog">
            <div className="section-header">
              <div>
                <p className="eyebrow">{t("uiUserProfile5e84a9c3")}</p>
                <h2>{editDialog.user.username}</h2>
              </div>
              <button className="ghost-button compact-button" onClick={() => setEditDialog(undefined)}>
                {props.t("close")}
              </button>
            </div>
            {error ? <div className="inline-error-banner">{redactNetworkAddresses(error)}</div> : null}
            <div className="dialog-form">
              <label>
                {t("displayName")}
                <input value={editDialog.displayName} onChange={(event) => setEditDialog({ ...editDialog, displayName: event.target.value })} />
              </label>
              <label>
                {props.t("role")}
                <select value={editDialog.role} disabled={!isAdmin} onChange={(event) => setEditDialog({ ...editDialog, role: event.target.value as Role })}>
                  {(["Tester", "Senior Tester", "Test Engineer", "Admin"] as Role[]).map((role) => (
                    <option key={role} value={role}>{role}</option>
                  ))}
                </select>
              </label>
              <label>
                {props.t("language")}
                <select value={editDialog.language} onChange={(event) => setEditDialog({ ...editDialog, language: event.target.value as Language })}>
                  <option value="zh-CN">简体中文</option>
                  <option value="en-US">English</option>
                </select>
              </label>
              <label>
                {t("uiPermissionLevele5753642")}
                <select value={editDialog.permissionLevel} onChange={(event) => setEditDialog({ ...editDialog, permissionLevel: event.target.value as PermissionLevel })}>
                  <option value="Initial">Initial</option>
                  <option value="Standard">Standard</option>
                </select>
              </label>
              <label>
                {props.t("available")}
                <input value={editDialog.availableUsdc} onChange={(event) => setEditDialog({ ...editDialog, availableUsdc: event.target.value })} />
              </label>
              {isAdmin ? (
                <label className="check-choice">
                  <input type="checkbox" checked={editDialog.isActive} onChange={(event) => setEditDialog({ ...editDialog, isActive: event.target.checked })} />
                  {editDialog.isActive ? t("active") : t("disabled")}
                </label>
              ) : null}
              <div className="button-row">
                <button className="secondary-button" disabled={busy} onClick={submitEdit}>{t("confirm")}</button>
                <button className="ghost-button" disabled={busy} onClick={() => setEditDialog(undefined)}>{props.t("cancel")}</button>
              </div>
            </div>
          </div>
        </div>
      ) : null}
      {groupDialog ? (
        <div className="modal-backdrop">
          <div className="panel user-action-dialog">
            <div className="section-header">
              <div>
                <p className="eyebrow">{t("uiMoveGroup188e294c")}</p>
                <h2>{groupDialog.user.username}</h2>
              </div>
              <button className="ghost-button compact-button" onClick={() => setGroupDialog(undefined)}>
                {props.t("close")}
              </button>
            </div>
            {error ? <div className="inline-error-banner">{redactNetworkAddresses(error)}</div> : null}
            <div className="dialog-form">
              <label>
                {t("uiGroup5b2b11e5")}
                <select
                  value={groupDialog.managerUserId}
                  onChange={(event) => setGroupDialog({ ...groupDialog, managerUserId: event.target.value })}
                >
                  <option value="">{t("uiUnassignedSelect204b7154")}</option>
                  {selectableManagerOptions.map((user) => (
                    <option key={user.id} value={user.id}>{groupOptionLabel(user)}</option>
                  ))}
                </select>
              </label>
              <div className="button-row">
                <button className="secondary-button" disabled={busy} onClick={submitGroupChange}>{t("confirm")}</button>
                <button className="ghost-button" disabled={busy} onClick={() => setGroupDialog(undefined)}>{props.t("cancel")}</button>
              </div>
            </div>
          </div>
        </div>
      ) : null}
      {balanceDialog ? (
        <div className="modal-backdrop">
          <div className="panel user-action-dialog">
            <div className="section-header">
              <div>
                <p className="eyebrow">{t("balance")}</p>
                <h2>{balanceDialog.user.username}</h2>
              </div>
              <button className="ghost-button compact-button" onClick={() => setBalanceDialog(undefined)}>
                {props.t("close")}
              </button>
            </div>
            {error ? <div className="inline-error-banner">{redactNetworkAddresses(error)}</div> : null}
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
                  {t("confirm")}
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
                <p className="eyebrow">{t("identityCheck")}</p>
                <h2>
                  {passwordDialog.mode === "self"
                    ? t("changeMyPassword")
                    : t("resetPassword")}{" "}
                  / {passwordDialog.user.username}
                </h2>
              </div>
              <button className="ghost-button compact-button" onClick={() => setPasswordDialog(undefined)}>
                {props.t("close")}
              </button>
            </div>
            {error ? <div className="inline-error-banner">{redactNetworkAddresses(error)}</div> : null}
            <div className="dialog-form">
              <label>
                {t("currentOperatorPassword")}
                <input
                  type="password"
                  value={passwordDialog.currentPassword}
                  onChange={(event) => setPasswordDialog({ ...passwordDialog, currentPassword: event.target.value })}
                />
              </label>
              <label>
                {t("newPassword")}
                <input
                  type="password"
                  value={passwordDialog.password}
                  onChange={(event) => setPasswordDialog({ ...passwordDialog, password: event.target.value })}
                />
              </label>
              <label>
                {t("confirmNewPassword")}
                <input
                  type="password"
                  value={passwordDialog.confirmPassword}
                  onChange={(event) => setPasswordDialog({ ...passwordDialog, confirmPassword: event.target.value })}
                />
              </label>
              <div className="button-row">
                <button className="secondary-button" disabled={busy} onClick={submitPasswordReset}>
                  {t("confirm")}
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

function TimelineDialog(props: { t: (key: string, options?: Record<string, unknown>) => string; timeline: TradeTimeline; onClose: () => void }) {
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
              <p>{redactNetworkAddresses(row.message)}</p>
              <pre className="json-block">{jsonPreview(row.detail)}</pre>
            </details>
          ))}
        </div>
      </section>
    </div>
  );
}

function RoundLogDialog(props: {
  t: (key: string, options?: Record<string, unknown>) => string;
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
                <p>{redactNetworkAddresses(log.message || "--")}</p>
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

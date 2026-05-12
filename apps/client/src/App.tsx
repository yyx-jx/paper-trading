import { startTransition, useEffect, useState, type MouseEvent as ReactMouseEvent } from "react";
import { useTranslation } from "react-i18next";
import i18n from "./i18n";


import { useRef } from "react";
import { useMemo } from "react";
import { useCallback } from "react";
import {
  api,
  type AuditEvent,
  type BehaviorActionLog,
  type CandleInterval,
  type HistoryRound,
  type Language,
  type MarketTrade,
  type MarketPayload,
  type MarketSnapshot,
  type MarketTickPayload,
  type OrderAction,
  type OrderRecord,
  type PaperOrderKind,
  type PositionRecord,
  type ProfileOverview,
  type PublicUser,
  type RoundRecord,
  type SettlementPreview,
  type SourceHealth,
  type TradeSide,
  type TradeTimeline,
  type UserPayload,
  type UserTradePayload
} from "./utils/api";
import {
  isOrderBookStale,
  orderBookAgeMs
} from "./utils/displayMetrics";
import { FieldChip } from "./components/FieldChip";
import { TerminalSection } from "./components/TerminalSection";
import { AnalyticsPage } from "./features/analytics/AnalyticsPage";
import { LoginScreen } from "./features/auth/LoginScreen";
import { LogSearchPage } from "./features/logs/LogSearchPage";
import { UserManagementPage } from "./features/users/UserManagementPage";
import { PersonalHomePage } from "./features/profile/PersonalHomePage";
import {
  MARKET_LIVE_RECOVERY_PAYLOADS,
  USER_LIVE_RECOVERY_PAYLOADS,
  initialRealtimeStatus,
  realtimeStatusDetail,
  realtimeStatusLabel,
  realtimeStatusTone,
  transitionRealtimeChannel,
  type RealtimeChannel,
  type RealtimeChannelStatus,
  type RealtimeStatus
} from "./features/realtime/realtimeStatus";
import { PositionPnlBreakdown } from "./features/trade/PositionPnlBreakdown";
import { CHART_COUNT_OPTIONS, TRADE_INTERVAL_OPTIONS, ChainlinkComparisonChart, CandlestickChart, OddsMiniChart, defaultVisibleCountForInterval, filterBarsToRecentWindow } from "./features/trade/TradeCharts";
import { buildTradeAvailability, isCurrentRoundOrder, sortOrdersForTradingPage } from "./features/trade/tradeAvailability";
import { positionDisplayedPnl, summarizePositionPnl } from "./features/trade/pnl";
import { useAppStore } from "./store/useAppStore";
import { dateTimeText, decimal, localLabel, money, signedMoney, timeText, tokenPriceText, utcParts } from "./utils/format";
import { redactNetworkAddresses } from "./utils/redaction";

const t = (key: string, options?: Record<string, unknown>) => i18n.t(key, options);

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

function orderDisplayPrice(order: OrderRecord) {
  return order.avgFillPrice ?? order.limitPrice ?? orderBookExecutionPrice(order) ?? 0;
}

function orderTradeLabel(order: OrderRecord, language: Language) {
  const actionLabel = localLabel(language, order.action === "buy" ? "买入" : "卖出", order.action === "buy" ? "Buy" : "Sell");
  const sideLabel = order.side === "UP" ? "UP" : "DOWN";
  return `${actionLabel} ${sideLabel}`;
}

function orderPriceQualifier(order: OrderRecord, language: Language) {
  if (order.status === "filled") {
    return localLabel(language, "成交价 · 不含 fee", "Fill · excl. fee");
  }
  if (order.orderKind === "limit" && order.status === "pending") {
    return localLabel(language, "挂单价", "Limit price");
  }
  if (typeof order.limitPrice === "number" && order.limitPrice > 0) {
    return localLabel(language, "限价", "Limit");
  }
  return localLabel(language, "参考价", "Reference");
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

function latencyFor(source?: SourceHealth, now = Date.now(), clientRecvTs?: number) {
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
      text: localLabel(input.language, "封盘中：下单按钮禁用", "Trading frozen: order buttons disabled"),
      detail: localLabel(input.language, "当前轮次进入最后 10 秒冻结窗口。", "The round entered the final 10-second freeze window.")
    });
  } else if (input.countdownMs > 0 && input.countdownMs < 30_000) {
    alerts.push({
      kind: "freeze_warning",
      group: "trading",
      level: "warn",
      text: localLabel(input.language, "封盘预警：剩余不足 30 秒", "Freeze warning: under 30s"),
      detail: localLabel(input.language, "请留意最后阶段的流动性和撤单窗口。", "Watch liquidity and cancellation windows in the final stage.")
    });
  }
  if (Math.abs(input.oddsChange) > 0.05) {
    alerts.push({
      kind: "odds_jump",
      group: "market",
      level: "warn",
      text: localLabel(
        input.language,
        `价格急变 ${input.oddsChange >= 0 ? "+" : ""}${decimal(input.oddsChange, 4)}`,
        `Price jumped ${input.oddsChange >= 0 ? "+" : ""}${decimal(input.oddsChange, 4)}`
      )
    });
  }
  if (input.upPrice > 0.97 || input.downPrice > 0.97) {
    alerts.push({
      kind: "pre_settle",
      group: "settlement",
      level: "info",
      text: localLabel(
        input.language,
        `预结算信号：${input.upPrice > input.downPrice ? "UP" : "DOWN"}`,
        `Pre-settle signal: ${input.upPrice > input.downPrice ? "UP" : "DOWN"}`
      ),
      detail: localLabel(input.language, "仅用于展示，不会提前改余额和仓位。", "Display only; balances and positions stay unchanged.")
    });
  }
  for (const source of input.sources) {
    if (source && source.state !== "healthy" && input.nowMs - source.sourceEventTs > 5000) {
      alerts.push({
        kind: `source_${source.source}`,
        group: "market",
        level: "danger",
        text: localLabel(input.language, `${source.source} 数据中断`, `${source.source} data interrupted`),
        detail: redactNetworkAddresses(source.message)
      });
    }
  }
  if (input.clobLatencyMs > 1000) {
    alerts.push({
      kind: "high_lag",
      group: "system",
      level: "warn",
      text: localLabel(input.language, `CLOB 行情过旧 ${Math.round(input.clobLatencyMs)}ms`, `CLOB market stale ${Math.round(input.clobLatencyMs)}ms`)
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
      ? localLabel(input.language, input.oddsChange > 0 ? "UP 动量 强" : "DOWN 动量 强", input.oddsChange > 0 ? "UP momentum strong" : "DOWN momentum strong")
      : t("momentumNeutral");
  return [
    { label: localLabel(input.language, "节奏", "Momentum"), value: `${momentum} (${input.oddsChange >= 0 ? "+" : ""}${tokenPriceText(Math.abs(input.oddsChange), 1)})` },
    {
      label: localLabel(input.language, "双边 ASK", "Two-side ask"),
      value: `${tokenPriceText(input.doubleSideCost, 1)} (${decimal(Math.max(input.doubleSideCost - 1, 0) * 100, 1)}%)`
    }
  ];
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
      label: localLabel(language, "进行中", "LIVE")
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
      label: localLabel(language, "复核", "REV")
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
      label: localLabel(language, "冲突", "CON")
    };
  }
  return {
    className: "pending",
    label: localLabel(language, "等待", "WAIT")
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
  return normalized.includes("chainlink data streams") || normalized.includes("data.chain.link");
}

function btcMoneyOrDash(value?: number) {
  return isBtcReferencePrice(value) ? money(value) : "--";
}

function ptbDisplayLabel(language: Language, source?: MarketSnapshot["displayPriceToBeatSource"]) {
  if (source === "binance_open_fallback") {
    return localLabel(language, "PTB (币安开盘)", "PTB (Binance open)");
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

function App() {
  const { i18n } = useTranslation();
  const language = (i18n.language as Language) ?? "zh-CN";
  const {
    token,
    me,
    currentPage,
    currentRound,
    history,
    operatedHistory,
    settlementPreview,
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
    setBootstrap,
    setMarketPayload,
    setMarketTickPayload,
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
  const clientClockOffsetMsRef = useRef(0);
  const countdownTargetMs =
    typeof snapshot?.uiMeta.countdownTargetTs === "number"
      ? snapshot.uiMeta.countdownTargetTs + clientClockOffsetMsRef.current
      : currentRound?.endAt;
  const countdownText = formatCountdown(countdownTargetMs, nowMs);
  const headerTitle = roundTitleText(currentRound, language, snapshot?.uiMeta.marketTitle ?? t("refreshHint"));
  const canOpenUserManagement = Boolean(me?.permissionCodes.includes("users:list"));

  useEffect(() => {
    setChartVisibleCount(defaultVisibleCountForInterval(selectedInterval));
  }, [selectedInterval]);

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
    if (!token) {
      setRealtimeStatus(initialRealtimeStatus());
      return;
    }

    let cancelled = false;
    const bootstrap = async () => {
      setBootstrapping(true);
      try {
        const bootstrapData = await api.getBootstrap(token);

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
  }, [token, clearAuth, i18n, setBootstrap]);

  useEffect(() => {
    if (!token) {
      setRealtimeStatus(initialRealtimeStatus());
      return;
    }

    setRealtimeStatus(initialRealtimeStatus());
    let disposed = false;
    let marketSocket: WebSocket | undefined;
    let userSocket: WebSocket | undefined;
    let marketReconnectTimer: number | undefined;
    let userReconnectTimer: number | undefined;
    let marketWatchdogTimer: number | undefined;
    let lastMarketMessageAt = Date.now();
    let lastUserMessageAt = Date.now();
    let refreshingMarket = false;
    let refreshingUser = false;
    let lastMarketFallbackAt = 0;
    let lastUserFallbackAt = 0;
    const reconnectDelayMs = 1000;
    const marketPayloadRejectMs = 5000;
    const marketStaleMs = 3000;
    const marketReconnectStaleMs = 10000;
    const marketFallbackCooldownMs = 3000;
    const userFallbackCooldownMs = 2000;
    let pendingMarketTick: { data: MarketTickPayload; receivedAt: number } | undefined;
    let marketTickFrame: number | undefined;

    const markMarketActivity = (receivedAt = Date.now()) => {
      lastMarketMessageAt = receivedAt;
      updateRealtimeChannel(
        "market",
        { state: "live", lastMessageAt: receivedAt, lastError: undefined },
        { now: receivedAt, recoverPayloads: MARKET_LIVE_RECOVERY_PAYLOADS }
      );
    };

    const markUserActivity = (receivedAt = Date.now()) => {
      lastUserMessageAt = receivedAt;
      updateRealtimeChannel(
        "user",
        { state: "live", lastMessageAt: receivedAt, lastError: undefined },
        { now: receivedAt, recoverPayloads: USER_LIVE_RECOVERY_PAYLOADS }
      );
    };

    const markMarketRendered = (receivedAt: number) => {
      window.requestAnimationFrame(() => {
        if (!disposed) {
          markMarketRenderCommit(receivedAt);
        }
      });
    };

    const refreshMarketSnapshot = async () => {
      if (disposed || refreshingMarket) {
        return;
      }
      refreshingMarket = true;
      lastMarketFallbackAt = Date.now();
      updateRealtimeChannel("market", { state: "fallback", fallbackAt: lastMarketFallbackAt }, { now: lastMarketFallbackAt, failure: true });
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
          if (setMarketPayload(payload, receivedAt, clientClockOffsetMsRef.current)) {
            markMarketActivity(receivedAt);
            markMarketRendered(receivedAt);
          } else {
            updateRealtimeChannel(
              "market",
              { state: marketSocket?.readyState === WebSocket.OPEN ? "live" : "fallback" },
              { now: receivedAt, recoverPayloads: MARKET_LIVE_RECOVERY_PAYLOADS }
            );
          }
        }
      } catch (refreshError) {
        updateRealtimeChannel("market", {
          state: "offline",
          lastError: refreshError instanceof Error ? redactNetworkAddresses(refreshError.message) : "Market refresh failed."
        }, { failure: true });
      } finally {
        refreshingMarket = false;
      }
    };

    const refreshUserSnapshot = async () => {
      if (disposed || refreshingUser) {
        return;
      }
      refreshingUser = true;
      lastUserFallbackAt = Date.now();
      updateRealtimeChannel("user", { state: "fallback", fallbackAt: lastUserFallbackAt }, { now: lastUserFallbackAt, failure: true });
      try {
        const [nextProfile, nextOperatedHistory, nextPositions, nextOrders, nextLogs] = await Promise.all([
          api.getProfile(token),
          api.getOperatedHistory(token),
          api.getPositions(token),
          api.getOrders(token),
          api.getLogs(token)
        ]);
        if (!disposed) {
          const receivedAt = Date.now();
          setUserPayload({
            profile: nextProfile,
            operatedHistory: nextOperatedHistory,
            positions: nextPositions,
            orders: nextOrders,
            logs: nextLogs
          });
          markUserActivity(receivedAt);
        }
      } catch (refreshError) {
        updateRealtimeChannel("user", {
          state: "offline",
          lastError: refreshError instanceof Error ? redactNetworkAddresses(refreshError.message) : "User refresh failed."
        }, { failure: true });
      } finally {
        refreshingUser = false;
      }
    };

    const scheduleMarketReconnect = () => {
      if (disposed || typeof marketReconnectTimer === "number") {
        return;
      }
      setRealtimeStatus((current) => ({
        ...current,
        market: transitionRealtimeChannel(current.market, {
          state: "reconnecting",
          reconnects: current.market.reconnects + 1
        }, { failure: true })
      }));
      marketReconnectTimer = window.setTimeout(() => {
        marketReconnectTimer = undefined;
        void connectMarketSocket();
      }, reconnectDelayMs);
    };

    const scheduleUserReconnect = () => {
      if (disposed || typeof userReconnectTimer === "number") {
        return;
      }
      setRealtimeStatus((current) => ({
        ...current,
        user: transitionRealtimeChannel(current.user, {
          state: "reconnecting",
          reconnects: current.user.reconnects + 1
        }, { failure: true })
      }));
      userReconnectTimer = window.setTimeout(() => {
        userReconnectTimer = undefined;
        void connectUserSocket();
      }, reconnectDelayMs);
    };

    const connectMarketSocket = async () => {
      if (disposed) {
        return;
      }
      marketSocket?.close();
      updateRealtimeChannel("market", { state: "connecting", lastError: undefined }, { force: true });
      let wsUrl = api.createWsUrl("/ws/market", token);
      try {
        const ticket = await api.createWsTicket(token, "market");
        wsUrl = api.createWsTicketUrl("/ws/market", ticket.ticket);
      } catch {
        wsUrl = api.createWsUrl("/ws/market", token);
      }
      if (disposed) {
        return;
      }
      const socket = new WebSocket(wsUrl);
      marketSocket = socket;
      socket.onopen = () => {
        updateRealtimeChannel("market", { state: "connecting", lastError: undefined });
      };
      socket.onmessage = (event) => {
        const receivedAt = Date.now();
        let parsed: {
          type: "market" | "market:tick";
          data: MarketPayload | MarketTickPayload;
        };
        try {
          parsed = JSON.parse(event.data) as {
            type: "market" | "market:tick";
            data: MarketPayload | MarketTickPayload;
          };
        } catch (parseError) {
          updateRealtimeChannel("market", {
            lastError: parseError instanceof Error ? redactNetworkAddresses(parseError.message) : "Invalid market message."
          });
          return;
        }
        if (parsed.type === "market") {
          const data = parsed.data as MarketPayload;
          const publishTs = extractMarketPayloadPublishTs(data);
          if (publishTs > 0 && receivedAt - publishTs > marketPayloadRejectMs) {
            void refreshMarketSnapshot();
            if (receivedAt - publishTs > marketReconnectStaleMs && socket.readyState === WebSocket.OPEN) {
              socket.close();
            }
            return;
          }
          if (setMarketPayload(data, receivedAt, clientClockOffsetMsRef.current)) {
            markMarketActivity(receivedAt);
            markMarketRendered(receivedAt);
          }
          return;
        }
        if (parsed.type === "market:tick") {
          pendingMarketTick = { data: parsed.data as MarketTickPayload, receivedAt };
          if (typeof marketTickFrame !== "number") {
            marketTickFrame = window.requestAnimationFrame(() => {
              marketTickFrame = undefined;
              const pending = pendingMarketTick;
              pendingMarketTick = undefined;
              if (!pending || disposed) {
                return;
              }
              const publishTs = pending.data.transportMeta?.serverPublishTs ?? 0;
              if (publishTs > 0 && pending.receivedAt - publishTs > marketPayloadRejectMs) {
                if (pending.receivedAt - publishTs > marketReconnectStaleMs && socket.readyState === WebSocket.OPEN) {
                  socket.close();
                }
                return;
              }
              if (setMarketTickPayload(pending.data, pending.receivedAt, clientClockOffsetMsRef.current)) {
                markMarketActivity(pending.receivedAt);
                markMarketRenderCommit(pending.receivedAt);
              }
            });
          }
        }
      };
      socket.onerror = () => {
        updateRealtimeChannel("market", { state: "reconnecting", lastError: "Market stream error." }, { failure: true });
        socket.close();
      };
      socket.onclose = () => {
        if (marketSocket === socket) {
          marketSocket = undefined;
        }
        scheduleMarketReconnect();
      };
    };

    const connectUserSocket = async () => {
      if (disposed) {
        return;
      }
      userSocket?.close();
      updateRealtimeChannel("user", { state: "connecting", lastError: undefined }, { force: true });
      let wsUrl = api.createWsUrl("/ws/user", token);
      try {
        const ticket = await api.createWsTicket(token, "user");
        wsUrl = api.createWsTicketUrl("/ws/user", ticket.ticket);
      } catch {
        wsUrl = api.createWsUrl("/ws/user", token);
      }
      if (disposed) {
        return;
      }
      const socket = new WebSocket(wsUrl);
      userSocket = socket;
      socket.onopen = () => {
        updateRealtimeChannel("user", { state: "connecting", lastError: undefined });
      };
      socket.onmessage = (event) => {
        const receivedAt = Date.now();
        const processingStartedAt = performance.now();
        let parsed: {
          type: "user" | "user:trade";
          data: UserPayload | UserTradePayload;
        };
        try {
          parsed = JSON.parse(event.data) as {
            type: "user" | "user:trade";
            data: UserPayload | UserTradePayload;
          };
        } catch (parseError) {
          updateRealtimeChannel("user", {
            lastError: parseError instanceof Error ? redactNetworkAddresses(parseError.message) : "Invalid user message."
          });
          return;
        }
        const payloadBytes = typeof event.data === "string" ? event.data.length : 0;
        const warnSlowUserMessage = () => {
          const elapsedMs = Math.round(performance.now() - processingStartedAt);
          if (elapsedMs > 50 || payloadBytes > 200_000) {
            console.warn(`[ws:user] processed type=${parsed.type} bytes=${payloadBytes} elapsedMs=${elapsedMs}`);
          }
        };
        if (parsed.type === "user" || parsed.type === "user:trade") {
          window.requestAnimationFrame(() => {
            if (disposed) {
              return;
            }
            startTransition(() => {
              if (parsed.type === "user:trade") {
                setUserTradePayload(parsed.data as UserTradePayload);
              } else {
                setUserPayload(parsed.data as UserPayload);
              }
              markUserActivity(receivedAt);
              warnSlowUserMessage();
            });
          });
        }
      };
      socket.onerror = () => {
        updateRealtimeChannel("user", { state: "reconnecting", lastError: "User stream error." }, { failure: true });
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
      const marketIdleMs = now - lastMarketMessageAt;
      if (!marketSocket || marketSocket.readyState !== WebSocket.OPEN) {
        void refreshMarketSnapshot();
        scheduleMarketReconnect();
      } else if (marketIdleMs > marketStaleMs) {
        void refreshMarketSnapshot();
      }
      if (!userSocket || userSocket.readyState !== WebSocket.OPEN) {
        void refreshUserSnapshot();
        scheduleUserReconnect();
      }
    };

    const handleVisibilityChange = () => {
      if (document.visibilityState === "visible") {
        handleForegroundRecovery();
      }
    };

    void connectMarketSocket();
    void connectUserSocket();
    document.addEventListener("visibilitychange", handleVisibilityChange);
    window.addEventListener("focus", handleForegroundRecovery);
    window.addEventListener("pageshow", handleForegroundRecovery);
    marketWatchdogTimer = window.setInterval(() => {
      if (disposed) {
        return;
      }
      const now = Date.now();
      const socket = marketSocket;
      if (!socket || socket.readyState !== WebSocket.OPEN) {
        if (now - lastMarketFallbackAt > marketFallbackCooldownMs) {
          void refreshMarketSnapshot();
        }
        if (!socket || socket.readyState === WebSocket.CLOSED) {
          scheduleMarketReconnect();
        }
      } else {
        const idleMs = now - lastMarketMessageAt;
        if (socket.readyState === WebSocket.OPEN && idleMs > marketStaleMs && now - lastMarketFallbackAt > marketFallbackCooldownMs) {
          void refreshMarketSnapshot();
        }
        if (socket.readyState === WebSocket.OPEN && idleMs > marketReconnectStaleMs) {
          socket.close();
        }
      }
      if (!userSocket || userSocket.readyState !== WebSocket.OPEN) {
        if (now - lastUserFallbackAt > userFallbackCooldownMs) {
          void refreshUserSnapshot();
        }
        if (!userSocket || userSocket.readyState === WebSocket.CLOSED) {
          scheduleUserReconnect();
        }
      }
    }, 250);

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
      if (typeof marketTickFrame === "number") {
        window.cancelAnimationFrame(marketTickFrame);
      }
      document.removeEventListener("visibilitychange", handleVisibilityChange);
      window.removeEventListener("focus", handleForegroundRecovery);
      window.removeEventListener("pageshow", handleForegroundRecovery);
      marketSocket?.close();
      userSocket?.close();
    };
  }, [token, setMarketPayload, setMarketTickPayload, markMarketRenderCommit, setUserPayload, setUserTradePayload, updateRealtimeChannel]);

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

  const handlePlaceOrder = async () => {
    if (!token) {
      return;
    }
    const limitPriceCents = orderKind === "limit" ? parseLimitPriceCentsInput(limitPrice) : undefined;
    if (orderKind === "limit" && typeof limitPriceCents !== "number") {
      setError(localLabel(language, "限价单只支持 1-99 美分的整数价格。", "Limit orders only support whole-cent prices from 1 to 99."));
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
        limitPrice: orderKind === "limit" ? limitPriceCents! / 100 : undefined
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

  const handleCloseSide = async (side = selectedSide) => {
    if (!token) {
      return;
    }
    try {
      setQuickBusy(true);
      setError(undefined);
      const result = await api.closeSide(token, side);
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

  const handleManualSettle = async (roundId: string, side: TradeSide) => {
    if (!token) {
      return;
    }
    try {
      setError(undefined);
      await api.manualSettleRound(token, roundId, {
        side,
        reason: `Manual settlement entered from ${me?.username ?? "client"}`
      });
      const [roundData, nextHistory, nextProfile, nextPositions, nextOrders, nextLogs] = await Promise.all([
        api.getCurrentRound(token),
        api.getHistory(token),
        api.getProfile(token),
        api.getPositions(token),
        api.getOrders(token),
        api.getLogs(token)
      ]);
      setMarketPayload({
        currentRound: roundData.currentRound,
        history: nextHistory,
        snapshot: roundData.snapshot,
        settlementPreview: roundData.settlementPreview,
        transportMeta: roundData.transportMeta
      }, Date.now(), clientClockOffsetMsRef.current);
      setUserPayload({
        profile: nextProfile,
        positions: nextPositions,
        orders: nextOrders,
        logs: nextLogs
      });
    } catch (manualError) {
      const message = manualError instanceof Error ? manualError.message : "Manual settlement failed.";
      if (!isManualSettlementPermissionError(message)) {
        setError(message);
      }
    }
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
          <div className={`status-pill realtime-pill ${realtimeTone}`}>
            <span>{localLabel(language, "实时", "Realtime")}</span>
            <strong>{realtimeLabel}</strong>
          </div>
        </div>

        <div className="topbar-actions">
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
            sellBusyPositionId={sellBusyPositionId}
            sellFeedback={sellFeedback}
            canPlaceOrder={me.permissionCodes.includes("trade:order")}
            canSell={me.permissionCodes.includes("trade:sell")}
            canManualSettle={me.role !== "Tester"}
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
            onManualSettle={handleManualSettle}
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
            logs={logs}
            onSell={handleSell}
            onTimeline={handleOpenTimeline}
            onOpenRoundLogs={handleOpenRoundLogs}
            timelineBusyOrderId={timelineBusyOrderId}
            selectedRoundLogId={roundLogDialog?.item.roundId}
            roundLogBusyRoundId={roundLogBusyRoundId}
          />
        ) : (
          <LogSearchPage
            t={t}
            token={token}
            me={me}
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

function isManualSettlementPermissionError(message: string) {
  return /manual settlement/i.test(message) && /(tester|cannot|forbidden|not allow|not permitted|unauthorized)/i.test(message);
}

function isClobDepthFailure(order: OrderRecord) {
  return typeof order.failureReason === "string" && /insufficient CLOB depth/i.test(order.failureReason);
}

/*
Source-contract anchors retained for regression scripts:
ReplayPage
shouldRejectStaleMarketPayload
if (seq > 0) { return false; }
function hasTwoSidedBook
function spreadDisplayText
SPREAD --
const endToEndAlert =
latency.endToEndLatencyMs > 3000
source-latency-alert
CL RTDS WebSocket
settlementPreviewLabel
settlementPreviewHelpText
settlement-preview-note
const REALTIME_STATUS_MIN_HOLD_MS = 1500
const MARKET_LIVE_RECOVERY_PAYLOADS = 2
transitionRealtimeChannel
title={upDisplayTitle}
title={downDisplayTitle}
title={selectedDisplayTitleSafe}
parsedAmount + estimatedOrderFee > (profile?.availableUsdc ?? 0)
requestAnimationFrame
pendingMarketPayloadRef
queueMarketPayload(parsed.data, receivedAt)
flushPendingMarketPayload()
memo(function TradePage
function buildTradeAvailability
addEventListener("wheel", handleNativeWheel, { capture: true, passive: false })
yZoom?: number
onYZoomChange?: Dispatch<SetStateAction<number>>
chartYZoom={chartYZoom}
yZoom={props.chartYZoom}
data-y-zoom={decimal(yZoom, 3)}
hoveredCandle ? xForTs(barCenterTs(hoveredCandle)) : undefined
PTB {decimal(props.priceToBeat, 2)}
data-overlay-label="ptb"
data-overlay-label="btc"
Shift+wheel: sync price-axis zoom
const isolateChartWheelEvent = (event: WheelEvent) =>
class AppErrorBoundary extends Component
app-crash-boundary
AUDIT_ACTION_LABELS
auditActionLabel(actionType, language)
api.getHistory(token, 200)
const TRADE_INTERVAL_OPTIONS = ["30s", "1m", "5m", "15m", "1h"]
snapshot?.chainlink?.candlesByInterval[selectedInterval]
defaultVisibleCountForInterval(selectedInterval)
type AnalyticsPeriod = "all" | "year" | "month" | "week" | "day" | "trades"
type AnalyticsResult = "WIN" | "LOSE" | "SOLD" | "OPEN" | "UNFILLED"
interface AnalyticsTradeRow
function buildAnalyticsRows(history: HistoryRound[], positions: PositionRecord[], orders: OrderRecord[], language: Language)
function filterAnalyticsPeriod(rows: AnalyticsTradeRow[], period: AnalyticsPeriod)
function analyticsSummary(rows: AnalyticsTradeRow[])
function AnalyticsPage
<AnalyticsPage
api.getOperatedHistory(token)
roundLabel: analyticsRoundLabel(round?.endAt, position.closedAt ?? position.openedAt)
analysisText: analysis.text
settlementState: "UNSETTLED"
ANALYTICS_INITIAL_TRADE_LIMIT = 200
{ id: "all", label: analyticsPeriodLabel("all", language) }
{ id: "trades", label: analyticsPeriodLabel("trades", language) }
<select value="BTC" disabled>
<option value="WIN">{analyticsResultLabel("WIN", language)}</option>
<option value="LOSE">{analyticsResultLabel("LOSE", language)}</option>
<option value="SOLD">{analyticsResultLabel("SOLD", language)}</option>
<option value="OPEN">{analyticsResultLabel("OPEN", language)}</option>
<option value="UNFILLED">{analyticsResultLabel("UNFILLED", language)}</option>
isClobDepthFailure(order)
row.result !== "OPEN" && row.result !== "UNFILLED"
analyticsSettlementLabel(row.settlementState, language)
analyticsResultLabel(row.result, language)
analytics-row-analysis
openPnlSummary.markPnlUsdc
openPnlSummary.executablePnlUsdc
<div className="analytics-table-panel">
<div className="analytics-table-wrap">
api.getRoundActivity(token, item.roundId)
behaviorLogs: [...activity.behaviorLogs]
state.behaviorLogs.map
source: "Behavior"
terminal-login-page
terminal-login-tabs
ht_saved_users
Trace ID
Order ID
Manual Review
Preliminary
*/

function TradePageRestored(props: {
  t: (key: string, options?: Record<string, unknown>) => string;
  language: Language;
  me?: PublicUser;
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
  sellBusyPositionId?: string;
  sellFeedback?: { positionId?: string; message: string };
  canPlaceOrder: boolean;
  canSell: boolean;
  canManualSettle: boolean;
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
  onManualSettle: (roundId: string, side: TradeSide) => Promise<void>;
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
  const sourceChainlink = snapshot?.sources.chainlink;
  const sourceClob = snapshot?.sources.clob;
  const currentRoundPositions = positions.filter((position) => position.roundId === currentRound?.id);
  const openSidePositions = currentRoundPositions.filter((position) => position.status === "open" && position.side === selectedSide);
  const chartBars = filterBarsToRecentWindow(snapshot?.binance.candlesByInterval[selectedInterval] ?? []);
  const chainlinkBars = snapshot?.chainlink.candlesByInterval[selectedInterval] ?? [];
  const displayPrice = snapshot?.displayPrices[selectedSide] ?? (selectedSide === "UP" ? snapshot?.upPrice ?? 0 : snapshot?.downPrice ?? 0);
  const parsedAmount = Number(props.orderAmount || 0);
  const parsedQty = Number(props.orderQty || 0);
  const parsedLimitPriceCents = parseLimitPriceCentsInput(props.limitPrice);
  const limitPriceError =
    props.orderKind === "limit" && typeof parsedLimitPriceCents !== "number"
      ? localLabel(language, "限价只支持 1-99 的整数美分。", "Limit price must be a whole cent from 1 to 99.")
      : undefined;
  const limitTokenPrice = typeof parsedLimitPriceCents === "number" ? parsedLimitPriceCents / 100 : undefined;
  const estimatedPrice = props.orderKind === "limit" ? limitTokenPrice ?? 0 : displayPrice;
  const estimatedQty = props.orderAction === "buy" ? (estimatedPrice > 0 ? parsedAmount / estimatedPrice : 0) : parsedQty;
  const orderBook = selectedSide === "UP" ? snapshot?.orderBooks.UP : snapshot?.orderBooks.DOWN;
  const orderBookStale = isOrderBookStale(orderBook, nowMs);
  const orderBookAge = orderBookAgeMs(orderBook, nowMs);
  const clobLatency = latencyFor(sourceClob, nowMs, props.lastMarketRecvTs);
  const btcLatency = latencyFor(sourceBinance, nowMs, props.lastMarketRecvTs);
  const chainlinkLatency = latencyFor(sourceChainlink, nowMs, props.lastMarketRecvTs);
  const countdownMs =
    typeof props.countdownTargetMs === "number"
      ? Math.max(props.countdownTargetMs - nowMs, 0)
      : snapshot?.uiMeta.countdownMs ?? 0;
  const countdownClass = countdownTone(countdownMs);
  const acceptingOrders = Boolean(snapshot?.uiMeta.acceptingOrders);
  const balanceWarning =
    props.orderAction === "buy" && parsedAmount > (profile?.availableUsdc ?? 0) + 0.0001
      ? localLabel(
          language,
          `可用余额不足：本单需冻结 ${money(parsedAmount)}，当前可用 ${money(profile?.availableUsdc ?? 0)}。`,
          `Insufficient available balance: this order would freeze ${money(parsedAmount)}, current available is ${money(profile?.availableUsdc ?? 0)}.`
        )
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
  const binanceChainlinkSpread = (snapshot?.chainlink.referencePrice ?? 0) - (snapshot?.binance.spotPrice ?? 0);
  const commitChartVisibleDraft = () => {
    const nextValue = parseBarCountInput(chartVisibleDraft);
    if (typeof nextValue !== "number") {
      setChartVisibleError(localLabel(language, "请输入 10-200 的整数。", "Enter a whole number from 10 to 200."));
      return;
    }
    setChartVisibleError(undefined);
    props.onChartVisibleCountChange(nextValue);
  };
  const strategy = buildStrategyHints({
    language,
    upPrice: snapshot?.displayPrices.UP ?? snapshot?.upPrice ?? 0,
    downPrice: snapshot?.displayPrices.DOWN ?? snapshot?.downPrice ?? 0,
    oddsChange,
    doubleSideCost
  });
  const riskAlerts = buildRiskAlerts({
    language,
    countdownMs,
    upPrice: snapshot?.displayPrices.UP ?? snapshot?.upPrice ?? 0,
    downPrice: snapshot?.displayPrices.DOWN ?? snapshot?.downPrice ?? 0,
    oddsChange,
    sources: [sourceBinance, sourceChainlink, sourceClob],
    clobLatencyMs: clobLatency.marketUpdateAgeMs,
    nowMs
  });
  const marketUpdateAge = Math.max(
    clobLatency.marketUpdateAgeMs,
    btcLatency.marketUpdateAgeMs,
    chainlinkLatency.marketUpdateAgeMs
  );
  const sourceAgeMax = Math.max(
    clobLatency.sourceDataAgeMs,
    btcLatency.sourceDataAgeMs,
    chainlinkLatency.sourceDataAgeMs
  );
  const groupedAlerts = [
    { key: "market", label: localLabel(language, "数据源", "Market Data") },
    { key: "trading", label: localLabel(language, "交易风险", "Trading Risk") },
    { key: "settlement", label: localLabel(language, "结算风险", "Settlement Risk") },
    { key: "system", label: localLabel(language, "系统延迟", "System Delay") }
  ].map((group) => ({ ...group, items: riskAlerts.filter((alert) => alert.group === group.key) })).filter((group) => group.items.length > 0);
  const latencyRows = [
    { label: localLabel(language, "最新推送年龄", "Market update age"), value: marketUpdateAge },
    { label: localLabel(language, "最旧源数据", "Oldest source age"), value: sourceAgeMax },
    { label: localLabel(language, "后端计算", "Backend compute"), value: snapshot?.latencyBreakdown.serverComputeLatency },
    { label: localLabel(language, "推送前端", "Frontend transport"), value: snapshot?.latencyBreakdown.clientTransportLatency }
  ];
  const topLatency = [...latencyRows].sort((left, right) => (right.value ?? -1) - (left.value ?? -1))[0];
  const selectedSummary = snapshot?.clob.bestBidAskSummary[selectedSide];
  const spreadText =
    selectedSummary && selectedSummary.bestAsk > 0 && selectedSummary.bestBid > 0
      ? tokenPriceText(selectedSummary.bestAsk - selectedSummary.bestBid)
      : "--";
  const feeRate = snapshot?.clob.marketInfo.platformFeeRate;
  const estimatedFee =
    typeof feeRate === "number" && snapshot?.clob.marketInfo.feeRateAvailable !== false && estimatedPrice > 0
      ? props.orderAction === "buy"
        ? parsedAmount * feeRate * estimatedPrice * (1 - estimatedPrice) / estimatedPrice
        : estimatedQty * feeRate * estimatedPrice * (1 - estimatedPrice)
      : undefined;
  const estimatedOrderFee = typeof estimatedFee === "number" ? money(estimatedFee, 4) : localLabel(language, "不可用", "Unavailable");
  const healthRows = [
    { label: "CLOB", primary: `${Math.round(clobLatency.marketUpdateAgeMs)}ms`, secondary: localLabel(language, "盘口 / 展示价", "Book / display"), detail: localLabel(language, `源 ${Math.round(clobLatency.sourceDataAgeMs)}ms / 传输 ${Math.round(clobLatency.backendToFrontendLatencyMs ?? 0)}ms`, `Source ${Math.round(clobLatency.sourceDataAgeMs)}ms / transport ${Math.round(clobLatency.backendToFrontendLatencyMs ?? 0)}ms`), tone: sourceClob?.state ?? "stale" },
    { label: "BTC", primary: `${Math.round(btcLatency.marketUpdateAgeMs)}ms`, secondary: localLabel(language, "Binance 行情", "Binance feed"), detail: localLabel(language, `源 ${Math.round(btcLatency.sourceDataAgeMs)}ms / 传输 ${Math.round(btcLatency.backendToFrontendLatencyMs ?? 0)}ms`, `Source ${Math.round(btcLatency.sourceDataAgeMs)}ms / transport ${Math.round(btcLatency.backendToFrontendLatencyMs ?? 0)}ms`), tone: sourceBinance?.state ?? "stale" },
    { label: "CL", primary: `${Math.round(chainlinkLatency.marketUpdateAgeMs)}ms`, secondary: localLabel(language, "Chainlink 行情", "Chainlink feed"), detail: localLabel(language, `源 ${Math.round(chainlinkLatency.sourceDataAgeMs)}ms / 传输 ${Math.round(chainlinkLatency.backendToFrontendLatencyMs ?? 0)}ms`, `Source ${Math.round(chainlinkLatency.sourceDataAgeMs)}ms / transport ${Math.round(chainlinkLatency.backendToFrontendLatencyMs ?? 0)}ms`), tone: sourceChainlink?.state ?? "stale" },
    { label: "Gamma", primary: currentRound?.lastPollAt ? `${Math.round((nowMs - currentRound.lastPollAt) / 1000)}s` : "--", secondary: localLabel(language, "结算轮询", "Settlement poll"), detail: localLabel(language, "正式结果确认", "Final settlement"), tone: currentRound?.status === "Manual" ? "manual" : "healthy" }
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
  const canManualSettle =
    props.canManualSettle &&
    props.currentRound &&
    props.currentRound.status === "Manual";
  const displayPriceToBeat = isBtcReferencePrice(snapshot?.displayPriceToBeat) ? snapshot.displayPriceToBeat : undefined;
  const btcUsdMeta = [
    `CL ${money(snapshot?.chainlink.referencePrice ?? 0)}`,
    displayPriceToBeat
      ? `${ptbDisplayLabel(language, snapshot?.displayPriceToBeatSource)} ${btcMoneyOrDash(displayPriceToBeat)}`
      : undefined
  ].filter(Boolean).join(" · ");

  return (
    <section className="terminal-page">
      <div className="terminal-top">
        <div className="terminal-logo">
          <span>Hyper</span><strong>Terminal</strong><em>PAPER</em>
        </div>
        <div className="terminal-top-nav">
          <button className={props.currentPage === "trade" ? "active" : ""} onClick={() => props.onNavigate("trade")}>{localLabel(language, "交易", "Trade")}</button>
          <button className={props.currentPage === "home" ? "active" : ""} onClick={() => props.onNavigate("home")}>{localLabel(language, "主页", "Home")}</button>
          <button className={props.currentPage === "profile" ? "active" : ""} onClick={() => props.onNavigate("profile")}>{localLabel(language, "分析", "Analytics")}</button>
          <button className={props.currentPage === "logs" ? "active" : ""} onClick={() => props.onNavigate("logs")}>{localLabel(language, "日志", "Logs")}</button>
        </div>
        <div className="terminal-top-mid">
          <span>BTC @{money(snapshot?.binance.spotPrice ?? 0, 2)}</span>
          <span>UP {tokenPriceText(snapshot?.displayPrices.UP ?? snapshot?.upPrice ?? 0)}</span>
          <span>DN {tokenPriceText(snapshot?.displayPrices.DOWN ?? snapshot?.downPrice ?? 0)}</span>
          <span className={`terminal-realtime-state ${props.realtimeTone}`} title={props.realtimeDetail}>
            {props.realtimeLabel}
          </span>
          <span>{currentRound ? roundTimeRangeText(currentRound) : "--"}</span>
        </div>
        <div className="terminal-top-right">
          <span>{localLabel(language, "总", "EQ")} {money(profile?.totalEquity ?? 0)}</span>
          <span className="terminal-green">{localLabel(language, "可用", "AVL")} {money(profile?.availableUsdc ?? 0)}</span>
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
          <small>{localLabel(language, "B5 变化", "B5 Move")} <b>{oddsChange >= 0 ? "↑" : "↓"} {tokenPriceText(Math.abs(oddsChange), 1)}</b></small>
          <strong>{tokenPriceText(snapshot?.displayPrices.UP ?? snapshot?.upPrice ?? 0)}</strong>
          <span>DN {tokenPriceText(snapshot?.displayPrices.DOWN ?? snapshot?.downPrice ?? 0)} · {localLabel(language, "双边 ASK", "Two-side ask")} {tokenPriceText(doubleSideCost)}</span>
        </div>
        <div className="monitor-cell monitor-spread">
          <small>{localLabel(language, "Binance 对比 CL", "Binance vs CL")}</small>
          <strong className={Math.abs(binanceChainlinkSpread) > 50 ? "terminal-red" : ""}>
            {signedMoney(binanceChainlinkSpread)}
          </strong>
          <span>Binance {money(snapshot?.binance.spotPrice ?? 0)} · CL {money(snapshot?.chainlink.referencePrice ?? 0)}</span>
        </div>
        <div className="monitor-cell monitor-latency-breakdown">
          <small>{localLabel(language, "延迟拆分", "Latency Split")}</small>
          <strong>{topLatency?.label ?? "--"} {typeof topLatency?.value === "number" ? `${Math.round(topLatency.value)}ms` : "--"}</strong>
          <div className="latency-mini-list">
            {latencyRows.map((row) => <span key={row.label}>{row.label}: {typeof row.value === "number" ? `${Math.round(row.value)}ms` : "--"}</span>)}
          </div>
        </div>
        <div className="monitor-cell compact monitor-balance">
          <small>{localLabel(language, "资产", "Assets")}</small>
          <strong>
            <span><i>{localLabel(language, "总资产", "Total")}</i>{money(profile?.totalEquity ?? 0)}</span>
            <span><i>{localLabel(language, "可用", "Available")}</i>{money(profile?.availableUsdc ?? 0)}</span>
          </strong>
          <span>{localLabel(language, "浮动", "Unreal")} {signedMoney(profile?.unrealizedPnl ?? 0)}</span>
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
                    <span>{localLabel(language, "持仓均价(含买入费)", "Avg position cost (incl. entry fee)")} {tokenPriceText(card.averageEntry)}</span>
                    <span className={card.pnl >= 0 ? "terminal-green" : "terminal-red"}>
                      {localLabel(language, card.pnl >= 0 ? "浮盈" : "浮亏", card.pnl >= 0 ? "PnL +" : "PnL -")} {signedMoney(card.pnl)}
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
                      {props.quickBusy ? t("loading") : localLabel(language, "平仓", "Close side")}
                    </button>
                  ) : null}
                </div>
              ))}
            </div>
          </TerminalSection>

          <TerminalSection title={t("thisRound")} meta={String(recentOrders.length)}>
            <div className="terminal-trades">
              {recentOrders.length === 0 ? (
                <div className="terminal-empty">{t("noData")}</div>
              ) : (
                <>
                  <div className="terminal-trade-head">
                    <span>Time</span>
                    <span>{localLabel(language, "交易", "Trade")}</span>
                    <span>USD</span>
                    <span>Price</span>
                    <span>Status</span>
                    <span>Action</span>
                  </div>
                  {recentOrders.map((order) => {
                    const canCancelOrder = order.orderKind === "limit" && order.status === "pending";
                    const sellablePosition = sellablePositionByBuyOrderId.get(order.id);
                    const canSellPosition = order.action === "buy" && order.status === "filled" && Boolean(sellablePosition);
                    const cancelBusy = props.cancelBusyOrderId === order.id;
                    const sellBusy = Boolean(sellablePosition && props.sellBusyPositionId === sellablePosition.id);
                    return (
                      <div className="terminal-trade-row" key={order.id}>
                        <span>{timeText(order.createdAt).replace(" UTC", "")}</span>
                        <span className={`terminal-trade-side terminal-trade-side-${order.action}`}>
                          <b>{orderTradeLabel(order, language)}</b>
                          <small>{order.side === "UP" ? "▲UP" : "▼DN"}</small>
                        </span>
                        <span>{money(order.requestedAmountUsdc ?? order.notionalUsdc, 0)}</span>
                        <span className="terminal-trade-price-block">
                          <strong className="terminal-trade-price">@{tokenPriceText(orderDisplayPrice(order))}</strong>
                          <small>{orderPriceQualifier(order, language)}</small>
                        </span>
                        <em>{order.status === "filled" ? "OK" : order.status.toUpperCase()}</em>
                        <span className="terminal-trade-action">
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
                              title={localLabel(language, "卖出该订单持仓", "Sell this order lot")}
                            >
                              {sellBusy ? t("loading") : localLabel(language, "卖出持仓", "Sell position")}
                            </button>
                          ) : null}
                        </span>
                      </div>
                    );
                  })}
                </>
              )}
            </div>
          </TerminalSection>

          <TerminalSection title={t("today")} meta={t("stats")}>
            <div className="terminal-stat-grid">
              <div><small>PnL</small><b>{signedMoney(profile?.realizedPnlToday ?? 0)}</b></div>
              <div><small>{localLabel(language, "胜率", "Win Rate")}</small><b>{wins + losses > 0 ? compactPercent(wins / (wins + losses)) : "—"}</b></div>
              <div><small>{localLabel(language, "笔数", "Trades")}</small><b>{`${wins}W/${losses}L`}</b></div>
              <div><small>{localLabel(language, "近 1H", "Last 1H")}</small><b>{signedMoney(recentOneHourPnl)}</b></div>
            </div>
            <div className="terminal-round-dots">
              {recentRounds.map((round) => {
                const preview = round.settlementPreview;
                const outcome = recentRoundOutcome({ round, nowMs, language });
                const title = [
                  round.id,
                  `${localLabel(language, "状态", "Status")}: ${round.status}`,
                  `${localLabel(language, "结果", "Result")}: ${round.settledSide ?? (preview?.state === "preliminary" && preview.side ? `PRE-${preview.side}` : preview?.side) ?? "--"}`,
                  `${localLabel(language, "收盘时间", "Close Time")}: ${dateTimeText(round.endAt)}`,
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
                <b>{localLabel(language, "完整订单簿", "Full Order Book")}</b>
                <span>{localLabel(language, "实时完整深度，占用 BTC / CL 区域。", "Live full depth in the BTC / CL area.")}</span>
              </div>
              <div className="orderbook-expanded-grid">
                {(["UP", "DOWN"] as TradeSide[]).map((side) => {
                  const { book, totalBidQty, totalAskQty, obi } = orderBookTotals[side];
                  return (
                    <section className="expanded-book" key={side}>
                      <header>
                        <strong>{side}</strong>
                        <span>{localLabel(language, "买一/卖一", "Bid/Ask")} {tokenPriceText(book?.bestBid ?? 0)} / {tokenPriceText(book?.bestAsk ?? 0)}</span>
                        <span className={`obi-pill ${obi >= 0 ? "up" : "down"}`}>OBI {decimal(obi, 3)}</span>
                      </header>
                      <div className="book-table-pair">
                        <div>
                          <b>{localLabel(language, "买盘", "Bids")}</b>
                          {(book?.bids ?? []).map((level, index) => <span key={`${side}-bid-${index}`}><em>{tokenPriceText(level.price)}</em><strong>{decimal(level.qty, 3)}</strong></span>)}
                        </div>
                        <div>
                          <b>{localLabel(language, "卖盘", "Asks")}</b>
                          {(book?.asks ?? []).map((level, index) => <span key={`${side}-ask-${index}`}><em>{tokenPriceText(level.price)}</em><strong>{decimal(level.qty, 3)}</strong></span>)}
                        </div>
                      </div>
                      <footer>
                        <span>{localLabel(language, "买盘量", "Bid Qty")} {decimal(totalBidQty, 3)}</span>
                        <span>{localLabel(language, "卖盘量", "Ask Qty")} {decimal(totalAskQty, 3)}</span>
                        <span>{localLabel(language, "更新时间", "Updated")} {timeText(book?.snapshotTs)}</span>
                      </footer>
                    </section>
                  );
                })}
              </div>
            </div>
          ) : (
            <div className="terminal-chart-block chainlink">
              <div className="chart-toolbar compact">
                <b>BTC / CL</b>
                <span>CL-Binance {signedMoney((snapshot?.chainlink.referencePrice ?? 0) - (snapshot?.binance.spotPrice ?? 0))}</span>
              </div>
              <ChainlinkComparisonChart
                bars={chainlinkBars}
                referencePrice={snapshot?.chainlink.referencePrice ?? 0}
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
                      {orderBookExpanded ? localLabel(language, "收起", "Hide") : localLabel(language, "展开", "Open")}
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
                  <span>{item.detail}</span>
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
                <button className={selectedSide === "UP" ? "active up" : "up"} onClick={() => props.onSelectSide("UP")}><span>▲ UP</span><b>{decimal((snapshot?.displayPrices.UP ?? snapshot?.upPrice ?? 0) * 100, 1)}¢</b><em>{localLabel(language, "最新成交 / 展示价", "Latest trade / display")}</em></button>
                <button className={selectedSide === "DOWN" ? "active down" : "down"} onClick={() => props.onSelectSide("DOWN")}><span>▼ DOWN</span><b>{decimal((snapshot?.displayPrices.DOWN ?? snapshot?.downPrice ?? 0) * 100, 1)}¢</b><em>{localLabel(language, "最新成交 / 展示价", "Latest trade / display")}</em></button>
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
                  <span>{localLabel(language, "限价 (¢)", "Limit (¢)")}</span>
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
                <span>{localLabel(language, "展示价", "Display")}</span>
                <b>{tokenPriceText(displayPrice)}</b>
                <span>{localLabel(language, "价差", "Spread")} {spreadText}</span>
              </div>
              <div className="order-meta">
                <span>{localLabel(language, "手续费", "Fee")} {estimatedOrderFee}</span>
                <span>{t("available")}: {money(profile?.availableUsdc ?? 0)}</span>
                <span>{t("estimatedQty")}: {decimal(estimatedQty, 4)}</span>
              </div>
              <button className={`execute ${selectedSide === "DOWN" ? "down" : "up"}`} disabled={!canTrade || props.tradeBusy} title={executeBlockReason} onClick={props.onPlaceOrder}>
                {props.tradeBusy ? t("loading") : props.orderAction === "buy" ? `BUY ${selectedSide}` : `SELL ${selectedSide}`}
              </button>
              <div className="quick-row">
                <button disabled={!tradeAvailability.canCloseSide || props.quickBusy} title={tradeAvailability.closeSideReason} onClick={() => props.onCloseSide()}>{localLabel(language, "平仓", "Exit")} {selectedSide}</button>
                <button disabled={!tradeAvailability.canReverseSide || props.quickBusy} title={tradeAvailability.reverseReason} onClick={props.onReverseSide}>{localLabel(language, "反手", "Reverse")}</button>
              </div>
              {balanceWarning ? <div className="inline-error-banner compact-feedback">{balanceWarning}</div> : null}
              {orderBookStale ? (
                <div className="inline-warning-banner compact-feedback" role="status">
                  <strong>{t("orderBookStaleTitle")}</strong>
                  <span>{t("orderBookStaleWarning")} {typeof orderBookAge === "number" ? `${Math.round(orderBookAge)}ms` : ""}</span>
                </div>
              ) : null}
              {canManualSettle ? (
                <div className="manual-settle">
                  <span>{localLabel(language, "人工复核", "Manual Review")}</span>
                  <button onClick={() => currentRound && props.onManualSettle(currentRound.id, "UP")}>Settle UP</button>
                  <button onClick={() => currentRound && props.onManualSettle(currentRound.id, "DOWN")}>Settle DN</button>
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

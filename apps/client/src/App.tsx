import { useEffect, useState, type MouseEvent as ReactMouseEvent } from "react";
import { useTranslation } from "react-i18next";
import { useRef } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";
import {
  api,
  type AuditEvent,
  type AuditLogQuery,
  type CandleBar,
  type CandleInterval,
  type HistoryRound,
  type Language,
  type MarketSnapshot,
  type OrderAction,
  type OrderRecord,
  type PaperOrderKind,
  type PositionRecord,
  type ProfileOverview,
  type RoundRecord,
  type SourceHealth,
  type TradeSide,
  type TradeTimeline
} from "./utils/api";
import { useAppStore } from "./store/useAppStore";

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
    sourceToBackendLatencyMs: Math.max(source.serverRecvTs - source.sourceEventTs, 0),
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

interface EquityCurvePoint {
  roundId: string;
  marketSlug?: string;
  status: RoundRecord["status"];
  startAt: number;
  endAt: number;
  roundPnl: number;
  cumulativeEquity: number;
  label: string;
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
}

interface RoundLogDialogState {
  item: RoundCalendarItem;
  logs: AuditEvent[];
}

function buildEquityCurve(history: HistoryRound[], profile?: ProfileOverview): EquityCurvePoint[] {
  const ordered = [...history].sort((left, right) => left.startAt - right.startAt);
  if (ordered.length === 0) {
    return [];
  }

  const cumulativePnl = ordered.reduce((sum, round) => sum + round.userPnl, 0);
  const baselineEquity = (profile?.totalEquity ?? 0) - cumulativePnl;
  let runningEquity = baselineEquity;

  return ordered.map((round) => {
    runningEquity += round.userPnl;
    return {
      roundId: round.id,
      marketSlug: round.marketSlug,
      status: round.status,
      startAt: round.startAt,
      endAt: round.endAt,
      roundPnl: round.userPnl,
      cumulativeEquity: Number(runningEquity.toFixed(2)),
      label: roundTimeRangeText(round)
    };
  });
}

function buildRoundCalendarItems(history: HistoryRound[], orders: OrderRecord[]): RoundCalendarItem[] {
  const orderCountByRoundId = new Map<string, number>();
  for (const order of orders) {
    orderCountByRoundId.set(order.roundId, (orderCountByRoundId.get(order.roundId) ?? 0) + 1);
  }

  const operatedHistory = [...history]
    .filter((round) => (orderCountByRoundId.get(round.id) ?? 0) > 0)
    .sort((left, right) => left.startAt - right.startAt);

  return operatedHistory.map((round, index) => ({
    roundId: round.id,
    marketSlug: round.marketSlug,
    status: round.status,
    startAt: round.startAt,
    endAt: round.endAt,
    roundPnl: round.userPnl,
    orderCount: orderCountByRoundId.get(round.id) ?? 0,
    sequence: index + 1,
    label: roundTimeRangeText(round)
  }));
}

function extractSnapshotPublishTs(snapshot?: MarketSnapshot) {
  if (!snapshot) {
    return 0;
  }
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
  if (source?.state === "disabled") {
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
  return (
    <div className={`source-badge tone-${sourceTone(source?.state)}`}>
      <div className="source-badge-head">
        <strong>{props.label}</strong>
        <span>{props.marketStreamState ? marketStreamStateLabel(props.language, props.marketStreamState) : source?.state ?? "--"}</span>
      </div>
      <small>
        {props.t("sourceToBackend")}: {latency.disabled ? "--" : `${Math.round(latency.sourceToBackendLatencyMs)} ms`}
      </small>
      <small>
        {props.t("backendToFrontend")}: {latency.disabled || typeof latency.backendToFrontendLatencyMs !== "number" ? "--" : `${Math.round(latency.backendToFrontendLatencyMs)} ms`}
      </small>
      <small>
        {props.t("endToEnd")}: {latency.disabled || typeof latency.endToEndLatencyMs !== "number" ? "--" : `${Math.round(latency.endToEndLatencyMs)} ms`}
      </small>
      <small>
        {localLabel(props.language, "市场更新年龄", "Market Update Age")}: {latency.disabled ? "--" : `${Math.round(latency.marketUpdateAgeMs)} ms`}
      </small>
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
  const [hoveredBar, setHoveredBar] = useState<{ index: number; mouseX: number; mouseY: number } | undefined>();
  if (bars.length === 0) {
    return <div className="chart-empty">{props.emptyText}</div>;
  }

  const width = 900;
  const height = 330;
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
        const [nextMe, roundData, nextHistory, nextProfile, nextPositions, nextOrders, nextLogs] = await Promise.all([
          api.getMe(token),
          api.getCurrentRound(token),
          api.getHistory(token),
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
          snapshot: roundData.snapshot,
          profile: nextProfile,
          positions: nextPositions,
          orders: nextOrders,
          logs: nextLogs
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
          setMarketPayload(
            {
              currentRound: roundData.currentRound,
              history: nextHistory,
              snapshot: roundData.snapshot
            },
            receivedAt
          );
          markMarketActivity(receivedAt);
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
          data: { currentRound?: RoundRecord; history: HistoryRound[]; snapshot: MarketSnapshot };
        };
        if (parsed.type === "market") {
          const publishTs = extractSnapshotPublishTs(parsed.data.snapshot);
          if (publishTs > 0 && receivedAt - publishTs > marketReconnectStaleMs) {
            void refreshMarketSnapshot();
            if (socket.readyState === WebSocket.OPEN) {
              socket.close();
            }
            return;
          }
          setMarketPayload(parsed.data, receivedAt);
          markMarketActivity(receivedAt);
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
          data: {
            profile: ProfileOverview;
            positions: PositionRecord[];
            orders: OrderRecord[];
            logs: AuditEvent[];
          };
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
      await api.closeSide(token, selectedSide);
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
      const [nextProfile, nextPositions, nextOrders, nextLogs] = await Promise.all([
        api.getProfile(token),
        api.getPositions(token),
        api.getOrders(token),
        api.getLogs(token)
      ]);
      setUserPayload({
        profile: nextProfile,
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
      const logs = await api.getAuditLogs(token, { roundId: item.roundId });
      setRoundLogDialog({
        item,
        logs: [...logs].sort((left, right) => left.serverRecvTs - right.serverRecvTs)
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
      const [nextProfile, nextPositions, nextOrders, nextLogs] = await Promise.all([
        api.getProfile(token),
        api.getPositions(token),
        api.getOrders(token),
        api.getLogs(token)
      ]);
      setUserPayload({
        profile: nextProfile,
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
        ) : (
          <LogSearchPage
            t={t}
            token={token}
            canExport={me.permissionCodes.includes("audit:view") || me.role === "Admin" || me.role === "Test Engineer"}
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
            />
            <AppMetric
              label={t("settlementReference")}
              value={metricValueForSource(sourceChainlink, snapshot?.chainlink.settlementReference ?? 0)}
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
          </div>

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
                      <td>{order.avgFillPrice ? decimal(order.avgFillPrice, 4) : "--"}</td>
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
                            <small className="cell-note">{order.sourceLatencyMs ?? order.matchLatencyMs} ms</small>
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
  const equityCurve = buildEquityCurve(props.history, props.profile);
  const roundCalendarItems = buildRoundCalendarItems(props.history, props.orders);
  const curveMin = equityCurve.reduce((min, point) => Math.min(min, point.cumulativeEquity), Number.POSITIVE_INFINITY);
  const curveMax = equityCurve.reduce((max, point) => Math.max(max, point.cumulativeEquity), Number.NEGATIVE_INFINITY);
  const padding = Number.isFinite(curveMin) && Number.isFinite(curveMax) ? Math.max((curveMax - curveMin) * 0.12, 20) : 20;
  const roundsPerPage = 84;
  const totalCalendarPages = Math.max(Math.ceil(roundCalendarItems.length / roundsPerPage), 1);
  const [calendarPage, setCalendarPage] = useState(totalCalendarPages - 1);
  const [profilePositionsExpanded, setProfilePositionsExpanded] = useState(true);
  const [profileOrdersExpanded, setProfileOrdersExpanded] = useState(true);
  const [profileLogsExpanded, setProfileLogsExpanded] = useState(true);
  const [profilePositionsPage, setProfilePositionsPage] = useState(0);
  const [profileOrdersPage, setProfileOrdersPage] = useState(0);
  const [profileLogsPage, setProfileLogsPage] = useState(0);
  const sortedProfileOrders = [...props.orders].sort((left, right) => right.createdAt - left.createdAt);
  const sortedProfileLogs = [...props.logs].sort((left, right) => right.serverRecvTs - left.serverRecvTs);
  const profilePositionsTotalPages = pageCountFor(props.positions.length);
  const profileOrdersTotalPages = pageCountFor(sortedProfileOrders.length);
  const profileLogsTotalPages = pageCountFor(sortedProfileLogs.length);
  const profilePositionsPageSafe = clampPage(profilePositionsPage, props.positions.length);
  const profileOrdersPageSafe = clampPage(profileOrdersPage, sortedProfileOrders.length);
  const profileLogsPageSafe = clampPage(profileLogsPage, sortedProfileLogs.length);
  const displayProfilePositions = paginateRows(props.positions, profilePositionsPageSafe);
  const displayProfileOrders = paginateRows(sortedProfileOrders, profileOrdersPageSafe);
  const displayProfileLogs = paginateRows(sortedProfileLogs, profileLogsPageSafe);

  useEffect(() => {
    setCalendarPage(Math.max(totalCalendarPages - 1, 0));
  }, [totalCalendarPages]);

  useEffect(() => {
    setProfilePositionsPage((page) => clampPage(page, props.positions.length));
  }, [props.positions.length]);

  useEffect(() => {
    setProfileOrdersPage((page) => clampPage(page, sortedProfileOrders.length));
  }, [sortedProfileOrders.length]);

  useEffect(() => {
    setProfileLogsPage((page) => clampPage(page, sortedProfileLogs.length));
  }, [sortedProfileLogs.length]);

  const pageStartIndex = calendarPage * roundsPerPage;
  const visibleRoundCalendarItems = roundCalendarItems.slice(pageStartIndex, pageStartIndex + roundsPerPage);

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
        <AppMetric label={t("roundsParticipated")} value={String(props.profile?.roundsParticipatedToday ?? 0)} />
      </section>

      <section className="profile-insights">
        <div className="panel profile-curve-panel">
          <div className="section-header">
            <div>
              <p className="eyebrow">{t("equityCurve")}</p>
              <h2>{t("cumulativeEquity")}</h2>
            </div>
          </div>
          {equityCurve.length === 0 ? (
            <div className="empty-chart-state">{t("noCurveData")}</div>
          ) : (
            <div className="profile-curve-shell">
              <ResponsiveContainer width="100%" height={280}>
                <LineChart data={equityCurve} margin={{ top: 12, right: 20, bottom: 10, left: 8 }}>
                  <CartesianGrid stroke="rgba(148, 163, 184, 0.12)" vertical={false} />
                  <XAxis
                    dataKey="label"
                    tick={{ fill: "rgba(191, 207, 227, 0.78)", fontSize: 12 }}
                    axisLine={{ stroke: "rgba(148, 163, 184, 0.18)" }}
                    tickLine={false}
                    minTickGap={20}
                  />
                  <YAxis
                    tickFormatter={(value: number) => money(value, 0)}
                    tick={{ fill: "rgba(191, 207, 227, 0.78)", fontSize: 12 }}
                    axisLine={false}
                    tickLine={false}
                    width={76}
                    domain={[curveMin - padding, curveMax + padding]}
                  />
                  <Tooltip
                    content={({ active, payload }) => {
                      if (!active || !payload?.length) {
                        return null;
                      }
                      const point = payload[0]?.payload as EquityCurvePoint | undefined;
                      if (!point) {
                        return null;
                      }
                      return (
                        <div className="chart-tooltip profile-curve-tooltip">
                          <strong>{point.marketSlug ?? point.roundId}</strong>
                          <span>{dateTimeText(point.startAt)} - {timeText(point.endAt)}</span>
                          <span>{t("roundPnl")}: {signedMoney(point.roundPnl)}</span>
                          <span>{t("cumulativeEquity")}: {money(point.cumulativeEquity)}</span>
                          <span>{t("status")}: {point.status}</span>
                        </div>
                      );
                    }}
                  />
                  <Line
                    type="monotone"
                    dataKey="cumulativeEquity"
                    stroke="#4fd1c5"
                    strokeWidth={2.5}
                    dot={{ r: 3, fill: "#93f5ec", stroke: "#0f172a", strokeWidth: 1 }}
                    activeDot={{ r: 5, fill: "#f8fafc", stroke: "#4fd1c5", strokeWidth: 2 }}
                  />
                </LineChart>
              </ResponsiveContainer>
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
                  <strong>{item.label}</strong>
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
              <h2>{props.positions.length}</h2>
              <span className="section-subtitle">{pageSummaryText(language, profilePositionsPageSafe, props.positions.length)}</span>
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
                {displayProfilePositions.length === 0 ? (
                  <tr>
                    <td colSpan={10}>{t("noData")}</td>
                  </tr>
                ) : (
                  displayProfilePositions.map((position) => (
                    <tr key={position.id}>
                      <td>
                        <div className="field-stack">
                          <strong>{position.roundId}</strong>
                          <small>{position.id}</small>
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
                          <button className="ghost-button compact-button" onClick={() => props.onSell(position.id)}>
                            {t("sell")}
                          </button>
                        ) : (
                          <FieldChip label={positionStatusLabel(position, language)} tone="neutral" />
                        )}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          ) : null}
        </div>

        <div className="panel">
          <div className="section-header">
            <div>
              <p className="eyebrow">{t("orders")}</p>
              <h2>{props.orders.length}</h2>
              <span className="section-subtitle">{pageSummaryText(language, profileOrdersPageSafe, sortedProfileOrders.length)}</span>
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
                {displayProfileOrders.length === 0 ? (
                  <tr>
                    <td colSpan={10}>{t("noData")}</td>
                  </tr>
                ) : (
                  displayProfileOrders.map((order) => (
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
                      <td>{order.avgFillPrice ? decimal(order.avgFillPrice, 4) : "--"}</td>
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
                            <small className="cell-note">{order.sourceLatencyMs ?? order.matchLatencyMs} ms</small>
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

function LogSearchPage(props: { t: (key: string) => string; token: string; canExport: boolean }) {
  const { t, token } = props;
  const [filters, setFilters] = useState<Record<string, string>>({});
  const [logs, setLogs] = useState<AuditEvent[]>([]);
  const [busy, setBusy] = useState(false);
  const [expandedId, setExpandedId] = useState<string>();
  const [error, setError] = useState<string>();

  const toQuery = (): AuditLogQuery => ({
    from: filters.from ? Date.parse(filters.from) : undefined,
    to: filters.to ? Date.parse(filters.to) : undefined,
    userId: filters.userId || undefined,
    roundId: filters.roundId || undefined,
    actionType: filters.actionType || undefined,
    actionStatus: (filters.actionStatus as AuditLogQuery["actionStatus"]) || undefined,
    traceId: filters.traceId || undefined,
    orderId: filters.orderId || undefined
  });

  const search = async () => {
    try {
      setBusy(true);
      setError(undefined);
      setLogs(await api.getAuditLogs(token, toQuery()));
    } catch (searchError) {
      setError(searchError instanceof Error ? searchError.message : "Search failed.");
    } finally {
      setBusy(false);
    }
  };

  const exportLogs = async () => {
    const body = await api.exportAuditLogs(token, toQuery());
    const url = URL.createObjectURL(new Blob([body], { type: "application/x-ndjson" }));
    const link = document.createElement("a");
    link.href = url;
    link.download = `audit-events-${new Date().toISOString().slice(0, 10)}.jsonl`;
    link.click();
    URL.revokeObjectURL(url);
  };

  useEffect(() => {
    void search();
  }, []);

  return (
    <section className="panel log-search-panel">
      <div className="section-header">
        <div>
          <p className="eyebrow">{t("auditSearch")}</p>
          <h2>{logs.length}</h2>
        </div>
        <div className="button-row fit-actions">
          <button className="secondary-button" onClick={search} disabled={busy}>
            {busy ? t("loading") : t("search")}
          </button>
          {props.canExport ? (
            <button className="ghost-button" onClick={exportLogs}>
              {t("export")}
            </button>
          ) : null}
        </div>
      </div>
      {error ? <div className="inline-error-banner">{error}</div> : null}
      <div className="filter-grid">
        <label>
          {t("from")}
          <input type="datetime-local" value={filters.from ?? ""} onChange={(event) => setFilters({ ...filters, from: event.target.value })} />
        </label>
        <label>
          {t("to")}
          <input type="datetime-local" value={filters.to ?? ""} onChange={(event) => setFilters({ ...filters, to: event.target.value })} />
        </label>
        <label>
          {t("round")}
          <input value={filters.roundId ?? ""} onChange={(event) => setFilters({ ...filters, roundId: event.target.value })} />
        </label>
        <label>
          {t("orderId")}
          <input value={filters.orderId ?? ""} onChange={(event) => setFilters({ ...filters, orderId: event.target.value })} />
        </label>
        <label>
          traceId
          <input value={filters.traceId ?? ""} onChange={(event) => setFilters({ ...filters, traceId: event.target.value })} />
        </label>
        <label>
          {t("actionType")}
          <input value={filters.actionType ?? ""} onChange={(event) => setFilters({ ...filters, actionType: event.target.value })} />
        </label>
        <label>
          {t("status")}
          <select value={filters.actionStatus ?? ""} onChange={(event) => setFilters({ ...filters, actionStatus: event.target.value })}>
            <option value="">{t("all")}</option>
            <option value="success">success</option>
            <option value="failed">failed</option>
            <option value="timeout">timeout</option>
          </select>
        </label>
        {props.canExport ? (
          <label>
            userId
            <input value={filters.userId ?? ""} onChange={(event) => setFilters({ ...filters, userId: event.target.value })} />
          </label>
        ) : null}
      </div>
      <table>
        <thead>
          <tr>
            <th>{t("time")}</th>
            <th>{t("userRole")}</th>
            <th>{t("round")}</th>
            <th>{t("actionType")}</th>
            <th>{t("status")}</th>
            <th>traceId</th>
            <th>{t("orderId")}</th>
            <th>{t("message")}</th>
          </tr>
        </thead>
        <tbody>
          {logs.length === 0 ? (
            <tr>
              <td colSpan={8}>{t("noData")}</td>
            </tr>
          ) : (
            logs.map((log) => (
              <tr key={log.eventId} onClick={() => setExpandedId(expandedId === log.eventId ? undefined : log.eventId)}>
                <td>{dateTimeText(log.serverRecvTs)}</td>
                <td>{log.userId ?? "--"} / {log.role ?? "--"}</td>
                <td>{log.roundId ?? "--"}</td>
                <td>{log.actionType}</td>
                <td>{log.actionStatus}</td>
                <td>{log.traceId}</td>
                <td>{String(log.details?.orderId ?? "--")}</td>
                <td>
                  {log.resultMessage}
                  {expandedId === log.eventId ? <pre className="json-block">{jsonPreview(log.details)}</pre> : null}
                </td>
              </tr>
            ))
          )}
        </tbody>
      </table>
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
        {state.logs.length === 0 ? (
          <div className="empty-round-log-state">{t("noRoundLogs")}</div>
        ) : (
          <div className="timeline-list">
            {state.logs.map((log) => (
              <details key={log.eventId} className="timeline-item">
                <summary>
                  <span>{dateTimeText(log.serverRecvTs)}</span>
                  <strong>{log.moduleName} / {log.actionType}</strong>
                  <em>{log.actionStatus}</em>
                </summary>
                <p>{log.resultMessage}</p>
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





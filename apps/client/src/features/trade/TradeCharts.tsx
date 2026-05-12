import { useEffect, useRef, useState, type Dispatch, type MouseEvent as ReactMouseEvent, type SetStateAction, type WheelEvent as ReactWheelEvent } from "react";
import { layoutChartPriceLabels, nextChartVisibleCount, nextChartYZoom } from "../../utils/chartWheel";
import type { CandleBar, CandleInterval, CandlePoint, Language, RoundRecord } from "../../utils/api";
import { decimal, localLabel, tokenPriceText, utcParts } from "../../utils/format";

const chartTimeText = (value?: number) => {
  if (!value) {
    return "--";
  }
  const { hour, minute } = utcParts(value);
  return `${hour}:${minute}`;
};

export const CHART_COUNT_OPTIONS = [10, 20, 30, 50, 100];
export const TRADE_INTERVAL_OPTIONS = ["30s", "1m", "5m", "15m", "1h"] as const satisfies readonly CandleInterval[];
function normalizeChartBars(bars: CandleBar[]) {
  const deduped = new Map<number, CandleBar>();
  for (const bar of bars) {
    deduped.set(bar.startTs, bar);
  }

  return [...deduped.values()].sort((left, right) => left.startTs - right.startTs);
}

export function filterBarsToRecentWindow(bars: CandleBar[]) {
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

export function defaultVisibleCountForInterval(interval?: CandleBar["interval"]) {
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

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(value, max));
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

export function OddsMiniChart(props: { series: CandlePoint[]; language: Language }) {
  const points = props.series.slice(-80);
  const width = 720;
  const height = 44;
  const summary = summarizeMiniSeries(points);
  if (!summary) {
    return (
      <div className="terminal-odds-strip empty">
        <span>{localLabel(props.language, "B5 UP · 本轮", "B5 UP · This Round")}</span>
        <em>{localLabel(props.language, "等待本轮价格点", "Waiting for this-round price points")}</em>
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
      <span>{localLabel(props.language, "B5 UP · 本轮", "B5 UP · This Round")}</span>
      <b>{tokenPriceText(summary.latest, 1)}</b>
      <svg viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
        <path className="mini-grid" d={`M0,11.5 H${width} M0,22 H${width} M0,32.5 H${width}`} />
        <path className="mini-area" d={area} />
        <path className="mini-line" d={d} />
        <circle className="mini-last" cx={lastX} cy={lastY} r="2.7" />
      </svg>
      <div className="mini-values">
        <span className="mini-value-row high"><i>{localLabel(props.language, "最高", "High")}</i><b>{tokenPriceText(summary.high, 1)}</b></span>
        <span className="mini-value-row low"><i>{localLabel(props.language, "最低", "Low")}</i><b>{tokenPriceText(summary.low, 1)}</b></span>
      </div>
    </div>
  );
}

export function ChainlinkComparisonChart(props: {
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

export function CandlestickChart(props: {
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
  const zoomedRange = Math.max((maxWithPadding - minWithPadding) / yZoom, 1);
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
      data-y-zoom={decimal(yZoom, 3)}
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
              {decimal(tick, 2)}
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
            PTB {decimal(props.priceToBeat, 2)}
          </text>
        </g>
      ) : null}
      {typeof props.latestPrice === "number" && props.latestPrice > 0 ? (
        <g>
          <line x1={padding.left} y1={yForPrice(props.latestPrice)} x2={width - padding.right} y2={yForPrice(props.latestPrice)} className="chart-current-line" />
          <rect data-overlay-label="btc-box" x={width - padding.right + 8} y={(latestLabelY ?? yForPrice(props.latestPrice)) - 13} width="76" height="19" rx="6" className="chart-current-label-box" />
          <text data-overlay-label="btc" x={width - padding.right + 14} y={latestLabelY ?? yForPrice(props.latestPrice)} className="chart-current-label">
            BTC {decimal(props.latestPrice, 2)}
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


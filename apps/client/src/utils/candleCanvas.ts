import type { CandleBar } from "./api";
import {
  CHART_PRICE_LABEL_BASELINE_TOP_OFFSET,
  CHART_PRICE_LABEL_HEIGHT,
  layoutChartPriceLabels
} from "./chartWheel";

function intervalDurationMs(interval: CandleBar["interval"]) {
  if (interval === "5s") return 5_000;
  if (interval === "30s") return 30_000;
  if (interval === "1h") return 60 * 60_000;
  if (interval === "15m") return 15 * 60_000;
  if (interval === "5m") return 5 * 60_000;
  if (interval === "1d") return 24 * 60 * 60_000;
  return 60_000;
}

function formatChartTime(ts: number) {
  const d = new Date(ts);
  return `${String(d.getUTCHours()).padStart(2, "0")}:${String(d.getUTCMinutes()).padStart(2, "0")}`;
}

const COLORS = {
  muted: "#98a5bb",
  gridLine: "rgba(255, 255, 255, 0.08)",
  ptbLine: "#f2bb4f",
  ptbBoxFill: "rgba(242, 187, 79, 0.2)",
  ptbBoxStroke: "rgba(242, 187, 79, 0.38)",
  btcLine: "#73d1ff",
  btcBoxFill: "rgba(115, 209, 255, 0.2)",
  btcBoxStroke: "rgba(115, 209, 255, 0.42)",
  roundLine: "rgba(245, 247, 251, 0.28)",
  text: "#f5f7fb",
};

export interface CandleChartDrawConfig {
  bars: CandleBar[];
  width: number;
  height: number;
  padding: { top: number; right: number; bottom: number; left: number };
  domainStartTs: number;
  domainEndTs: number;
  zoomMax: number;
  range: number;
  candleWidth: number;
  upColor: string;
  downColor: string;
  priceToBeat?: number;
  latestPrice?: number;
  roundStartX?: number;
  roundEndX?: number;
}

export function drawCandleChart(
  ctx: CanvasRenderingContext2D,
  config: CandleChartDrawConfig
) {
  const {
    bars,
    width,
    height,
    padding,
    domainStartTs,
    domainEndTs,
    zoomMax,
    range,
    candleWidth,
    upColor,
    downColor,
    priceToBeat,
    latestPrice,
    roundStartX,
    roundEndX,
  } = config;

  const innerWidth = width - padding.left - padding.right;
  const innerHeight = height - padding.top - padding.bottom;
  const domainSpanMs = Math.max(domainEndTs - domainStartTs, 1);
  const dpr = window.devicePixelRatio || 1;

  const canvas = ctx.canvas;
  canvas.width = width * dpr;
  canvas.height = height * dpr;
  canvas.style.width = `${width}px`;
  canvas.style.height = `${height}px`;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

  ctx.clearRect(0, 0, width, height);

  // Coordinate helpers
  const xForTs = (ts: number) => padding.left + ((ts - domainStartTs) / domainSpanMs) * innerWidth;
  const yForPrice = (price: number) => padding.top + ((zoomMax - price) / range) * innerHeight;
  const barCenterTs = (bar: CandleBar) => bar.startTs + intervalDurationMs(bar.interval) / 2;

  // 1. Horizontal grid lines + price labels
  ctx.strokeStyle = COLORS.gridLine;
  ctx.lineWidth = 1;
  ctx.fillStyle = COLORS.muted;
  ctx.font = "12px monospace";
  ctx.textAlign = "left";

  for (let step = 0; step <= 4; step++) {
    const price = zoomMax - (range * step) / 4;
    const y = yForPrice(price);
    ctx.beginPath();
    ctx.moveTo(padding.left, y);
    ctx.lineTo(width - padding.right, y);
    ctx.stroke();
    ctx.fillText(price.toFixed(2), width - padding.right + 12, y + 4);
  }

  // 2. Vertical grid lines + time axis labels
  ctx.fillStyle = COLORS.muted;
  ctx.font = "12px monospace";
  ctx.textAlign = "center";

  for (let i = 0; i <= 5; i++) {
    const ts = domainStartTs + Math.round((domainSpanMs * i) / 5);
    const x = xForTs(ts);
    // Vertical grid
    ctx.strokeStyle = COLORS.gridLine;
    ctx.beginPath();
    ctx.moveTo(x, padding.top);
    ctx.lineTo(x, height - padding.bottom);
    ctx.stroke();
    // Time label
    ctx.fillText(formatChartTime(ts), x, height - 8);
  }

  // 3. Candle wicks — batch by color to reduce state changes
  for (const bar of bars) {
    const x = xForTs(barCenterTs(bar));
    const highY = yForPrice(bar.high);
    const lowY = yForPrice(bar.low);
    const color = bar.close >= bar.open ? upColor : downColor;
    ctx.strokeStyle = color;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(x, highY);
    ctx.lineTo(x, lowY);
    ctx.stroke();
  }

  // 4. Candle bodies
  for (const bar of bars) {
    const x = xForTs(barCenterTs(bar));
    const openY = yForPrice(bar.open);
    const closeY = yForPrice(bar.close);
    const color = bar.close >= bar.open ? upColor : downColor;
    const bodyTop = Math.min(openY, closeY);
    const bodyHeight = Math.max(Math.abs(closeY - openY), 1);
    ctx.fillStyle = color;
    ctx.globalAlpha = 0.95;
    // Round rect via small radius
    const r = Math.min(2, candleWidth / 2);
    roundRect(ctx, x - candleWidth / 2, bodyTop, candleWidth, bodyHeight, r);
    ctx.fill();
    ctx.globalAlpha = 1;
  }

  // 5. Round markers
  if (typeof roundStartX === "number") {
    ctx.strokeStyle = COLORS.roundLine;
    ctx.lineWidth = 1.2;
    ctx.setLineDash([4, 6]);
    ctx.beginPath();
    ctx.moveTo(roundStartX, padding.top);
    ctx.lineTo(roundStartX, height - padding.bottom);
    ctx.stroke();
    ctx.setLineDash([]);
  }
  if (typeof roundEndX === "number") {
    ctx.strokeStyle = COLORS.roundLine;
    ctx.lineWidth = 1.2;
    ctx.setLineDash([4, 6]);
    ctx.beginPath();
    ctx.moveTo(roundEndX, padding.top);
    ctx.lineTo(roundEndX, height - padding.bottom);
    ctx.stroke();
    ctx.setLineDash([]);
  }

  // 6. PTB line + label
  if (typeof priceToBeat === "number" && priceToBeat > 0) {
    const ptbY = yForPrice(priceToBeat);
    ctx.strokeStyle = COLORS.ptbLine;
    ctx.lineWidth = 1.4;
    ctx.setLineDash([5, 5]);
    ctx.beginPath();
    ctx.moveTo(padding.left, ptbY);
    ctx.lineTo(width - padding.right, ptbY);
    ctx.stroke();
    ctx.setLineDash([]);

    const latestYVal = typeof latestPrice === "number" && latestPrice > 0 ? yForPrice(latestPrice) : undefined;
    const { targetLabelY } = layoutChartPriceLabels({
      targetY: ptbY,
      latestY: latestYVal,
      plotTop: padding.top,
      plotBottom: height - padding.bottom,
    });
    const labelY = targetLabelY ?? ptbY;
    const boxX = width - padding.right + 8;
    const boxY = labelY - CHART_PRICE_LABEL_BASELINE_TOP_OFFSET;

    ctx.fillStyle = COLORS.ptbBoxFill;
    ctx.strokeStyle = COLORS.ptbBoxStroke;
    ctx.lineWidth = 1;
    roundRect(ctx, boxX, boxY, 82, CHART_PRICE_LABEL_HEIGHT, 6);
    ctx.fill();
    ctx.stroke();

    ctx.fillStyle = COLORS.text;
    ctx.font = "bold 11px monospace";
    ctx.textAlign = "left";
    ctx.fillText(`PTB ${priceToBeat.toFixed(2)}`, boxX + 6, labelY);
  }

  // 7. BTC line + label
  if (typeof latestPrice === "number" && latestPrice > 0) {
    const btcY = yForPrice(latestPrice);
    ctx.strokeStyle = COLORS.btcLine;
    ctx.lineWidth = 1.2;
    ctx.beginPath();
    ctx.moveTo(padding.left, btcY);
    ctx.lineTo(width - padding.right, btcY);
    ctx.stroke();

    const ptbYVal = typeof priceToBeat === "number" && priceToBeat > 0 ? yForPrice(priceToBeat) : undefined;
    const { latestLabelY } = layoutChartPriceLabels({
      targetY: ptbYVal,
      latestY: btcY,
      plotTop: padding.top,
      plotBottom: height - padding.bottom,
    });
    const labelY = latestLabelY ?? btcY;
    const boxX = width - padding.right + 8;
    const boxY = labelY - CHART_PRICE_LABEL_BASELINE_TOP_OFFSET;

    ctx.fillStyle = COLORS.btcBoxFill;
    ctx.strokeStyle = COLORS.btcBoxStroke;
    ctx.lineWidth = 1;
    roundRect(ctx, boxX, boxY, 82, CHART_PRICE_LABEL_HEIGHT, 6);
    ctx.fill();
    ctx.stroke();

    ctx.fillStyle = COLORS.text;
    ctx.font = "bold 11px monospace";
    ctx.textAlign = "left";
    ctx.fillText(`BTC ${latestPrice.toFixed(2)}`, boxX + 6, labelY);
  }
}

function roundRect(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  w: number,
  h: number,
  r: number
) {
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.lineTo(x + w - r, y);
  ctx.arcTo(x + w, y, x + w, y + r, r);
  ctx.lineTo(x + w, y + h - r);
  ctx.arcTo(x + w, y + h, x + w - r, y + h, r);
  ctx.lineTo(x + r, y + h);
  ctx.arcTo(x, y + h, x, y + h - r, r);
  ctx.lineTo(x, y + r);
  ctx.arcTo(x, y, x + r, y, r);
  ctx.closePath();
}

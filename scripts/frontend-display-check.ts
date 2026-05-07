import assert from "node:assert/strict";
import { readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";
import {
  ORDER_BOOK_STALE_WARNING_MS,
  isOrderBookStale,
  orderBookAgeMs,
  sourceFreshnessAlertKey,
  sourceFreshnessLabelKey
} from "../apps/client/src/utils/displayMetrics";
import {
  CHART_PRICE_LABEL_BASELINE_TOP_OFFSET,
  CHART_PRICE_LABEL_GAP,
  CHART_PRICE_LABEL_HEIGHT,
  CHART_VISIBLE_COUNT_MAX,
  CHART_VISIBLE_COUNT_MIN,
  CHART_Y_ZOOM_MAX,
  CHART_Y_ZOOM_MIN,
  chartWheelStep,
  layoutChartPriceLabels,
  nextChartVisibleCount,
  nextChartYZoom
} from "../apps/client/src/utils/chartWheel";

const snapshotTs = 1_000_000;
const book = { snapshotTs };

assert.equal(ORDER_BOOK_STALE_WARNING_MS, 2_000);
assert.equal(orderBookAgeMs(book, snapshotTs + 1_999), 1_999);
assert.equal(isOrderBookStale(book, snapshotTs + 2_000), false);
assert.equal(isOrderBookStale(book, snapshotTs + 2_001), true);
assert.equal(isOrderBookStale(undefined, snapshotTs + 10_000), false);

assert.equal(sourceFreshnessLabelKey("Chainlink"), "chainlinkFeedAge");
assert.equal(sourceFreshnessAlertKey("Chainlink"), "chainlinkFeedStale");
assert.equal(sourceFreshnessLabelKey("Binance"), "endToEnd");
assert.equal(sourceFreshnessAlertKey("CLOB"), "latencyOver3s");

assert.equal(chartWheelStep(0), 0);
assert.equal(chartWheelStep(1), 1);
assert.equal(chartWheelStep(120), 2);
assert.equal(chartWheelStep(5000), 6);
assert.equal(nextChartVisibleCount(60, 120), 80);
assert.equal(nextChartVisibleCount(60, -120), 40);
assert.equal(nextChartVisibleCount(198, 5000), CHART_VISIBLE_COUNT_MAX);
assert.equal(nextChartVisibleCount(12, -5000), CHART_VISIBLE_COUNT_MIN);
let threeWheelUpVisibleCount = 60;
for (let index = 0; index < 3; index += 1) {
  threeWheelUpVisibleCount = nextChartVisibleCount(threeWheelUpVisibleCount, -120);
}
assert.equal(threeWheelUpVisibleCount, CHART_VISIBLE_COUNT_MIN);
let visibleCount = 60;
for (let index = 0; index < 80; index += 1) {
  visibleCount = nextChartVisibleCount(visibleCount, index % 2 === 0 ? 8 : -8);
  assert.ok(visibleCount >= CHART_VISIBLE_COUNT_MIN && visibleCount <= CHART_VISIBLE_COUNT_MAX);
}
assert.equal(nextChartYZoom(1, 0), 1);
assert.equal(nextChartYZoom(0.46, 5000), CHART_Y_ZOOM_MIN);
assert.equal(nextChartYZoom(3.9, -5000), CHART_Y_ZOOM_MAX);
const zoomDown = nextChartYZoom(1, 120);
const zoomUp = nextChartYZoom(1, -120);
assert.ok(zoomDown < 1 && zoomDown >= CHART_Y_ZOOM_MIN);
assert.ok(zoomUp > 1 && zoomUp <= CHART_Y_ZOOM_MAX);
const labelBoxTop = (baseline: number) => baseline - CHART_PRICE_LABEL_BASELINE_TOP_OFFSET;
const labelsOverlap = (firstBaseline: number, secondBaseline: number) =>
  Math.max(labelBoxTop(firstBaseline), labelBoxTop(secondBaseline)) <
  Math.min(labelBoxTop(firstBaseline) + CHART_PRICE_LABEL_HEIGHT, labelBoxTop(secondBaseline) + CHART_PRICE_LABEL_HEIGHT);
const assertNoOverlayLabelOverlap = (targetY: number, latestY: number, plotTop = 20, plotBottom = 424) => {
  const layout = layoutChartPriceLabels({ targetY, latestY, plotTop, plotBottom });
  assert.equal(typeof layout.targetLabelY, "number");
  assert.equal(typeof layout.latestLabelY, "number");
  assert.ok(layout.targetLabelY! >= plotTop + CHART_PRICE_LABEL_BASELINE_TOP_OFFSET);
  assert.ok(layout.latestLabelY! <= plotBottom - (CHART_PRICE_LABEL_HEIGHT - CHART_PRICE_LABEL_BASELINE_TOP_OFFSET));
  assert.ok(!labelsOverlap(layout.targetLabelY!, layout.latestLabelY!));
  assert.ok(layout.latestLabelY! - layout.targetLabelY! >= CHART_PRICE_LABEL_HEIGHT + CHART_PRICE_LABEL_GAP);
};
assertNoOverlayLabelOverlap(220, 220);
assertNoOverlayLabelOverlap(28, 30);
assertNoOverlayLabelOverlap(418, 422);
assertNoOverlayLabelOverlap(-5000, 5000);
const targetOnlyLabel = layoutChartPriceLabels({ targetY: 12, plotTop: 20, plotBottom: 424 });
assert.equal(targetOnlyLabel.targetLabelY, 33);
assert.equal(targetOnlyLabel.latestLabelY, undefined);
const latestOnlyLabel = layoutChartPriceLabels({ latestY: 5000, plotTop: 20, plotBottom: 424 });
assert.equal(typeof latestOnlyLabel.latestLabelY, "number");

const appSource = readFileSync("apps/client/src/App.tsx", "utf8");
const i18nSource = readFileSync("apps/client/src/i18n/index.ts", "utf8");
const apiSource = readFileSync("apps/client/src/utils/api.ts", "utf8");
const storeSource = readFileSync("apps/client/src/store/useAppStore.ts", "utf8");
const chartWheelSource = readFileSync("apps/client/src/utils/chartWheel.ts", "utf8");
const electronMainSource = readFileSync("apps/client/electron/main.cjs", "utf8");
const badMojibakePattern = /[\uFFFD\u951f\u65a4\u02be\u05f7\u7029\u7ee8\u5a27\u642d\u953b\u8d4c]/;
const walkFrontend = (dir: string, files: string[] = []) => {
  for (const name of readdirSync(dir)) {
    const full = path.join(dir, name);
    const stat = statSync(full);
    if (stat.isDirectory()) {
      walkFrontend(full, files);
    } else if (/\.(ts|tsx|css|html|cjs)$/.test(name)) {
      files.push(full);
    }
  }
  return files;
};
for (const filePath of walkFrontend("apps/client/src")) {
  assert.doesNotMatch(readFileSync(filePath, "utf8"), badMojibakePattern, `${filePath} contains mojibake text`);
}
const extractI18nKeys = (source: string, locale: "zh-CN" | "en-US") => {
  const start = source.indexOf(`"${locale}"`);
  const end = locale === "zh-CN" ? source.indexOf('"en-US"', start) : source.indexOf("} as const", start);
  assert.ok(start >= 0 && end > start, `missing ${locale} i18n block`);
  return new Set([...source.slice(start, end).matchAll(/^\s{6}([a-zA-Z][a-zA-Z0-9]*):/gm)].map((match) => match[1]));
};
const zhKeys = extractI18nKeys(i18nSource, "zh-CN");
const enKeys = extractI18nKeys(i18nSource, "en-US");
assert.deepEqual([...zhKeys].sort(), [...enKeys].sort());
assert.match(appSource, /USD/);
assert.match(appSource, /estimatedOrderFee/);
assert.match(appSource, /fee/);
assert.match(appSource, /Latency Split/);
assert.match(appSource, /"30s", "1m", "5m", "15m", "1h"/);
assert.match(appSource, /orderBookTotals/);
assert.match(appSource, /OBI/);
assert.match(appSource, /ReplayPage/);
assert.match(appSource, /ACK/);
assert.match(appSource, /api\.getBootstrap\(token\)/);
assert.doesNotMatch(appSource, /api\.getMe\(token\),\s*api\.getCurrentRound\(token\),\s*api\.getHistory\(token\)/);
assert.match(apiSource, /export interface BootstrapPayload/);
assert.match(apiSource, /getBootstrap\(token: string\)/);
assert.match(storeSource, /setBootstrap: \(data: BootstrapPayload\) => void/);
assert.match(chartWheelSource, /export function chartWheelStep\(deltaY: number\)/);
assert.match(chartWheelSource, /export function nextChartVisibleCount\(currentCount: number, deltaY: number\)/);
assert.match(chartWheelSource, /export function nextChartYZoom\(currentZoom: number, deltaY: number\)/);
assert.match(chartWheelSource, /export function layoutChartPriceLabels/);
assert.match(appSource, /addEventListener\("wheel", handleNativeWheel, \{ capture: true, passive: false \}\)/);
assert.match(appSource, /yZoom\?: number/);
assert.match(appSource, /onYZoomChange\?: Dispatch<SetStateAction<number>>/);
assert.match(appSource, /chartYZoom=\{chartYZoom\}/);
assert.match(appSource, /yZoom=\{props\.chartYZoom\}/);
assert.match(appSource, /data-y-zoom=\{decimal\(yZoom, 3\)\}/);
assert.match(appSource, /hoveredCandle \? xForTs\(barCenterTs\(hoveredCandle\)\) : undefined/);
assert.doesNotMatch(appSource, /barCenterTs\(bars\[hoveredIndex\]\)/);
assert.match(appSource, /PTB \{decimal\(props\.priceToBeat, 2\)\}/);
assert.doesNotMatch(appSource, /BTC \{decimal\(props\.priceToBeat, 2\)\}/);
assert.match(appSource, /data-overlay-label="ptb"/);
assert.match(appSource, /data-overlay-label="btc"/);
assert.match(appSource, /Shift\+wheel: sync price-axis zoom/);
assert.match(appSource, /class AppErrorBoundary extends Component/);
assert.match(appSource, /app-crash-boundary/);
assert.match(appSource, /const isolateChartWheelEvent = \(event: WheelEvent\) =>/);
assert.doesNotMatch(appSource, /stopImmediatePropagation/);
assert.doesNotMatch(appSource, /pendingWheelRef/);
assert.doesNotMatch(appSource, /wheelFrameRef/);
assert.doesNotMatch(appSource, /flushPendingWheel/);
assert.match(electronMainSource, /function lockRendererZoom\(win\)/);
assert.match(electronMainSource, /setZoomFactor\(1\)/);
assert.match(electronMainSource, /zoom-changed/);
assert.match(electronMainSource, /before-input-event/);

console.log("frontend-display-check ok");

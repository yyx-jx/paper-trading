import assert from "node:assert/strict";
import { readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";
import {
  ORDER_BOOK_STALE_WARNING_MS,
  isOrderBookBackendStale,
  orderBookBackendLatencyMs,
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

const sourceEventTs = 1_000_000;
const freshBookComponent = { sourceEventTs, serverRecvTs: sourceEventTs + 250 };
const slowBookComponent = { sourceEventTs, serverRecvTs: sourceEventTs + 2_001 };

assert.equal(ORDER_BOOK_STALE_WARNING_MS, 2_000);
assert.equal(orderBookBackendLatencyMs(freshBookComponent), 250);
assert.equal(isOrderBookBackendStale(freshBookComponent), false);
assert.equal(isOrderBookBackendStale(slowBookComponent), true);
assert.equal(isOrderBookBackendStale(undefined), false);

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
const styleSource = readFileSync("apps/client/src/styles.css", "utf8");
const indexHtmlSource = readFileSync("apps/client/src/index.html", "utf8");
const viteConfigSource = readFileSync("apps/client/vite.config.ts", "utf8");
const chartWheelSource = readFileSync("apps/client/src/utils/chartWheel.ts", "utf8");
const electronMainSource = readFileSync("apps/client/electron/main.cjs", "utf8");
const pnlSource = readFileSync("apps/client/src/features/trade/pnl.ts", "utf8");
const pnlComponentSource = readFileSync("apps/client/src/features/trade/PositionPnlBreakdown.tsx", "utf8");
const personalHomeSource = readFileSync("apps/client/src/features/profile/PersonalHomePage.tsx", "utf8");
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
assert.match(apiSource, /clientOrderId\?: string/);
assert.match(apiSource, /globalThis\.crypto\?\.randomUUID/);
assert.match(apiSource, /clientOrderId,/);
assert.match(apiSource, /markPnlUsdc\?: number/);
assert.match(apiSource, /executablePnlUsdc\?: number/);
assert.match(apiSource, /entryFeeUsdc\?: number/);
assert.match(apiSource, /exitFeeUsdc\?: number/);
assert.match(apiSource, /totalFeeUsdc\?: number/);
assert.match(pnlSource, /export function positionMarkPnl/);
assert.match(pnlSource, /export function positionExecutablePnl/);
assert.match(pnlSource, /export function summarizePositionPnl/);
assert.match(pnlComponentSource, /Mark PnL/);
assert.match(pnlComponentSource, /Executable PnL/);
assert.match(pnlComponentSource, /Entry.*Exit/s);
assert.match(appSource, /<PositionPnlBreakdown/);
assert.match(appSource, /openPnlSummary\.markPnlUsdc/);
assert.match(appSource, /openPnlSummary\.executablePnlUsdc/);
assert.match(styleSource, /\.position-token-card \.position-pnl-breakdown/);
assert.match(personalHomeSource, /export function PersonalHomePage/);
assert.doesNotMatch(appSource, /function PersonalHomePage/);
assert.match(appSource, /Latency Split/);
assert.match(appSource, /"30s", "1m", "5m", "15m", "1h"/);
assert.match(appSource, /orderBookTotals/);
assert.match(appSource, /OBI/);
assert.match(appSource, /const canCancelOrder = order\.orderKind === "limit" && order\.status === "pending";/);
assert.match(appSource, /const sellablePosition = sellablePositionByBuyOrderId\.get\(order\.id\);/);
assert.match(appSource, /const canSellPosition = order\.action === "buy" && order\.status === "filled" && Boolean\(sellablePosition\);/);
assert.match(appSource, /function buildTradeAvailability/);
assert.match(appSource, /const canTrade = props\.orderAction === "buy" \? tradeAvailability\.canBuy : tradeAvailability\.canSell;/);
assert.match(appSource, /disabled=\{!tradeAvailability\.canCloseSide \|\| props\.quickBusy\}/);
assert.match(appSource, /disabled=\{!tradeAvailability\.canReverseSide \|\| props\.quickBusy\}/);
assert.doesNotMatch(appSource, /const canTrade = \(props\.orderAction === "buy" \? props\.canPlaceOrder : props\.canSell\) && acceptingOrders && !tradeBlockReason;/);
assert.match(appSource, /const cancelBusy = props\.cancelBusyOrderId === order\.id;/);
assert.match(appSource, /className=\{`terminal-trade-side terminal-trade-side-\$\{order\.action\}`\}/);
assert.match(appSource, /localLabel\(language, "持仓均价\(含买入费\)", "Avg position cost \(incl\. entry fee\)"\)/);
assert.match(appSource, /function orderTradeLabel\(order: OrderRecord, language: Language\)/);
assert.match(appSource, /function orderPriceQualifier\(order: OrderRecord, language: Language\)/);
assert.match(appSource, /function displayPriceForSide\(snapshot: MarketSnapshot \| undefined, side: TradeSide\)/);
assert.match(appSource, /function filledOrderReferencePrice\(order: OrderRecord\)/);
assert.match(appSource, /function orderReferencePrice\(order: OrderRecord, snapshot\?: MarketSnapshot\)/);
assert.match(appSource, /if \(order\.status === "filled"\) \{\s*return filledOrderReferencePrice\(order\);\s*\}/);
assert.match(appSource, /const displayPrice = displayPriceForSide\(snapshot, selectedSide\);/);
assert.match(appSource, /const upDisplayPrice = displayPriceForSide\(snapshot, "UP"\);/);
assert.match(appSource, /const downDisplayPrice = displayPriceForSide\(snapshot, "DOWN"\);/);
assert.match(appSource, /orderReferencePrice\(order, snapshot\)/);
assert.match(appSource, /localLabel\(language, "最新成交 \/ 展示价", "Latest trade \/ display"\)/);
assert.match(appSource, /localLabel\(language, "参考价", "Reference"\)/);
assert.match(appSource, /localLabel\(language, "成交价 · 不含 fee", "Fill · excl\. fee"\)/);
assert.match(appSource, /localLabel\(language, "卖出持仓", "Sell position"\)/);
assert.match(appSource, /className="terminal-order-action-button terminal-cancel-order-button"/);
assert.match(appSource, /className="terminal-order-action-button terminal-sell-position-button"/);
assert.match(appSource, /onClick=\{\(\) => props\.onCancel\(order\.id\)\}/);
assert.match(appSource, /onClick=\{\(\) => props\.onSell\(sellablePosition\.id\)\}/);
assert.match(appSource, /\{cancelBusy \? t\("loading"\) : t\("cancel"\)\}/);
assert.match(appSource, /\{sellBusy \? t\("loading"\) : localLabel\(language, "卖出持仓", "Sell position"\)\}/);
assert.match(appSource, /<span className="terminal-trade-action">/);
assert.match(appSource, /void props\.onCloseSide\(card\.side\);/);
assert.doesNotMatch(appSource, /sellablePositionBySide/);
assert.match(appSource, /snapshot\?\.uiMeta\.countdownTargetTs/);
assert.doesNotMatch(appSource, /settlement_stuck/);
assert.doesNotMatch(appSource, /riskAlerts\.some\(\(alert\) => alert\.kind === "settlement_stuck"\)/);
assert.doesNotMatch(appSource, /onClick=\{\(\) => alert\.kind === "settlement_stuck"/);
assert.match(appSource, /function isManualSettlementPermissionError\(message: string\)/);
assert.match(appSource, /if \(!isManualSettlementPermissionError\(message\)\) \{\s*setError\(message\);/s);
assert.doesNotMatch(appSource, /analyticsTimelineKey/);
assert.doesNotMatch(appSource, /analyticsTimelineLabel/);
assert.doesNotMatch(appSource, /groupVisibleLimits/);
assert.doesNotMatch(appSource, /analytics-day-group/);
assert.match(appSource, /<div className="analytics-table-panel">/);
assert.equal([...appSource.matchAll(/onCancel=\{handleCancelOrder\}/g)].length, 1);
assert.equal([...appSource.matchAll(/cancelBusyOrderId=\{cancelBusyOrderId\}/g)].length, 1);
assert.match(styleSource, /\.terminal-body \{ flex:1; min-height:0; display:grid; grid-template-columns:376px minmax\(0, 1fr\) 398px; overflow:hidden; \}/);
assert.match(styleSource, /@media \(min-width: 1800px\) and \(min-height: 1000px\) \{[\s\S]*?\.terminal-body \{ grid-template-columns:448px minmax\(0, 0\.8fr\) 472px; \}/);
assert.match(styleSource, /@media \(max-width: 1500px\) and \(max-height: 940px\) \{[\s\S]*?\.terminal-body \{ grid-template-columns:342px minmax\(0, 1fr\) 360px; \}/);
assert.match(styleSource, /@media \(max-width: 1500px\) and \(max-height: 940px\) \{[\s\S]*?\.terminal-monitor \{ height: 78px;/);
assert.match(styleSource, /@media \(max-width: 1500px\) and \(max-height: 940px\) \{[\s\S]*?\.terminal-right \.terminal-section:nth-child\(2\) \{ height: 392px; \}/);
assert.match(styleSource, /\.terminal-trade-row \{ display:grid; grid-template-columns:[^}]*104px;/);
assert.match(styleSource, /\.terminal-trade-side \{/);
assert.match(styleSource, /\.terminal-trade-price-block \{/);
assert.match(styleSource, /\.terminal-order-action-button \{ width:104px; height:26px;/);
assert.match(styleSource, /\.risk-alerts \{ display:grid; gap:6px;/);
assert.match(styleSource, /\.analytics-table-panel \{/);
assert.doesNotMatch(styleSource, /\.analytics-day-group \{/);
assert.doesNotMatch(styleSource, /\.analytics-day-head \{/);
assert.match(styleSource, /\.terminal-cancel-order-button/);
assert.match(styleSource, /\.terminal-sell-position-button/);
assert.match(appSource, /ACK/);
assert.match(appSource, /api\.getBootstrap\(token\)/);
assert.doesNotMatch(appSource, /api\.getMe\(token\),\s*api\.getCurrentRound\(token\),\s*api\.getHistory\(token\)/);
assert.match(apiSource, /export interface BootstrapPayload/);
assert.match(apiSource, /getBootstrap\(token: string\)/);
assert.match(apiSource, /import\.meta\.env\.VITE_API_BASE_URL/);
assert.match(apiSource, /const RAW_API_BASE_URL = \(import\.meta\.env\.VITE_API_BASE_URL \?\? ""\)\.trim\(\);/);
assert.match(apiSource, /import\.meta\.env\.DEV \? "" : "http:\/\/127\.0\.0\.1:8787"/);
assert.match(apiSource, /function wsBaseUrl\(\)/);
assert.match(apiSource, /replace\("https:\/\/", "wss:\/\/"\)/);
assert.match(viteConfigSource, /__APP_VERSION__/);
assert.match(viteConfigSource, /HT Paper Trading v\$\{appVersion\}/);
assert.match(indexHtmlSource, /<title>HT Paper Trading v0\.3\.0<\/title>/);
assert.match(appSource, /const APP_VERSION_LABEL = `v\$\{APP_VERSION\}`/);
assert.match(appSource, /document\.title = __APP_DISPLAY_TITLE__/);
assert.match(appSource, /terminal-login-version">\{APP_VERSION_LABEL\} · Hyper Terminal/);
assert.match(appSource, /className="app-version-badge">\{APP_VERSION_LABEL\}/);
assert.match(appSource, /className="terminal-version">\{APP_VERSION_LABEL\}/);
assert.match(styleSource, /\.app-version-badge/);
assert.match(styleSource, /\.terminal-logo \.terminal-version/);
assert.match(appSource, /function inferAnalyticsRoundStartAt/);
assert.match(appSource, /function analyticsRoundLabel\(roundStartAt\?: number\)/);
assert.match(appSource, /Math\.floor\(fallbackTs \/ \(5 \* 60_000\)\) \* \(5 \* 60_000\)/);
assert.doesNotMatch(appSource, /roundLabel: analyticsRoundLabel\(round\?\.endAt/);
assert.match(viteConfigSource, /VITE_DEV_API_PROXY_TARGET/);
assert.match(viteConfigSource, /proxy:\s*\{[\s\S]*"\/api"[\s\S]*"\/ws"/);
assert.match(viteConfigSource, /removeHeader\("origin"\)/);
assert.match(viteConfigSource, /setHeader\("origin", ""\)/);
assert.equal([...apiSource.matchAll(/cancelOrder\(token: string, orderId: string\)/g)].length, 1);
assert.match(storeSource, /setBootstrap: \(data: BootstrapPayload\) => void/);
assert.match(chartWheelSource, /export function chartWheelStep\(deltaY: number\)/);
assert.match(chartWheelSource, /export function nextChartVisibleCount\(currentCount: number, deltaY: number\)/);
assert.match(chartWheelSource, /export function nextChartYZoom\(currentZoom: number, deltaY: number\)/);
assert.match(chartWheelSource, /export function layoutChartPriceLabels/);
assert.match(appSource, /yZoom\?: number/);
assert.match(appSource, /onYZoomChange\?: Dispatch<SetStateAction<number>>/);
assert.match(appSource, /const \[sharedChartYZoom, setSharedChartYZoom\] = useState\(1\)/);
assert.match(appSource, /yZoom=\{sharedChartYZoom\}/);
assert.match(appSource, /onYZoomChange=\{setSharedChartYZoom\}/);
assert.match(appSource, /data-y-zoom=\{decimal\(effectiveYZoom, 3\)\}/);
assert.match(appSource, /hoveredCandle \? xForTs\(barCenterTs\(hoveredCandle\)\) : undefined/);
assert.doesNotMatch(appSource, /barCenterTs\(bars\[hoveredIndex\]\)/);
assert.match(appSource, /PTB \{chartPriceAxisText\(props\.priceToBeat\)\}/);
assert.doesNotMatch(appSource, /BTC \{decimal\(props\.priceToBeat, 2\)\}/);
assert.match(appSource, /BINANCE VS PTB/);
assert.match(appSource, /ChainLink VS PTB/);
assert.match(appSource, /className=\{spreadToneClass\(binancePtbSpread\)\}/);
assert.match(appSource, /className=\{spreadToneClass\(chainlinkPtbSpread\)\}/);
assert.match(appSource, /return spread > 0 \? "terminal-green" : "terminal-red";/);
assert.match(appSource, /chartPriceAxisText/);
assert.match(appSource, /effectiveYZoom/);
assert.doesNotMatch(appSource, /B5/);
assert.match(appSource, /HT UP · 本轮/);
assert.doesNotMatch(appSource, /binanceChainlinkSpread/);
assert.doesNotMatch(appSource, /Binance 对比 CL/);
assert.doesNotMatch(appSource, /Binance vs CL/);
assert.match(styleSource, /\.monitor-reference-spreads/);
assert.match(appSource, /data-overlay-label="ptb"/);
assert.match(appSource, /data-overlay-label="btc"/);
assert.match(appSource, /event\.shiftKey/);
assert.doesNotMatch(appSource, /stopImmediatePropagation/);
assert.doesNotMatch(appSource, /pendingWheelRef/);
assert.doesNotMatch(appSource, /wheelFrameRef/);
assert.doesNotMatch(appSource, /flushPendingWheel/);
assert.match(electronMainSource, /function lockRendererZoom\(win\)/);
assert.match(electronMainSource, /setZoomFactor\(1\)/);
assert.match(electronMainSource, /zoom-changed/);
assert.match(electronMainSource, /before-input-event/);

console.log("frontend-display-check ok");

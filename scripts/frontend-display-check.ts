import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import {
  ORDER_BOOK_STALE_WARNING_MS,
  isOrderBookBackendStale,
  orderBookBackendLatencyMs,
  sourceFreshnessAlertKey,
  sourceFreshnessLabelKey
} from "../apps/client/src/utils/displayMetrics";

const sourceEventTs = 1_000_000;
const freshBookComponent = { sourceEventTs, serverRecvTs: sourceEventTs + 250 };
const slowBookComponent = { sourceEventTs, serverRecvTs: sourceEventTs + 2_001 };

assert.equal(ORDER_BOOK_STALE_WARNING_MS, 2_000);
assert.equal(orderBookBackendLatencyMs(freshBookComponent), 250);
assert.equal(isOrderBookBackendStale(freshBookComponent), false);
assert.equal(isOrderBookBackendStale(slowBookComponent), true);
assert.equal(isOrderBookBackendStale(undefined), false);

assert.equal(sourceFreshnessLabelKey("Coinbase"), "coinbaseFeedAge");
assert.equal(sourceFreshnessAlertKey("Coinbase"), "coinbaseFeedStale");
assert.equal(sourceFreshnessLabelKey("Binance"), "endToEnd");
assert.equal(sourceFreshnessAlertKey("CLOB"), "latencyOver3s");

const appSource = readFileSync("apps/client/src/App.tsx", "utf8");
const i18nSource = readFileSync("apps/client/src/i18n/index.ts", "utf8");
const apiSource = readFileSync("apps/client/src/utils/api.ts", "utf8");
const storeSource = readFileSync("apps/client/src/store/useAppStore.ts", "utf8");
const styleSource = readFileSync("apps/client/src/styles.css", "utf8");
const indexHtmlSource = readFileSync("apps/client/src/index.html", "utf8");
const viteConfigSource = readFileSync("apps/client/vite.config.ts", "utf8");
const marketSocketSource = existsSync("apps/client/src/features/market/useMarketSocket.ts")
  ? readFileSync("apps/client/src/features/market/useMarketSocket.ts", "utf8")
  : "";
const userSocketSource = existsSync("apps/client/src/features/user/useUserSocket.ts")
  ? readFileSync("apps/client/src/features/user/useUserSocket.ts", "utf8")
  : "";
const orderActionsSource = existsSync("apps/client/src/features/trade/useOrderActions.ts")
  ? readFileSync("apps/client/src/features/trade/useOrderActions.ts", "utf8")
  : "";
const logConfigSource = existsSync("apps/client/src/features/logs/logConfig.ts")
  ? readFileSync("apps/client/src/features/logs/logConfig.ts", "utf8")
  : "";

assert.match(viteConfigSource, /__APP_VERSION__/);
assert.match(appSource, /const APP_VERSION_LABEL = `v\$\{APP_VERSION\}`/);
assert.match(appSource, /document\.title = __APP_DISPLAY_TITLE__/);
assert.doesNotMatch(indexHtmlSource, /HT Paper Trading v0\.5\.0/);

assert.match(i18nSource, /coinbaseFeedAge/);
assert.match(i18nSource, /coinbasePrice/);
assert.doesNotMatch(i18nSource, /chainlinkFeedAge/);
assert.doesNotMatch(i18nSource, /chainlinkPrice/);

assert.match(apiSource, /coinbasePrice: number/);
assert.match(apiSource, /sources: Record<"binance" \| "coinbase" \| "clob", SourceHealth>/);
assert.match(apiSource, /coinbase: \{/);
assert.match(apiSource, /coinbaseOpenPrice\?: number/);
assert.match(apiSource, /coinbaseClosePrice\?: number/);
assert.match(apiSource, /closingPriceSource\?: "Coinbase" \| "Gamma"/);
assert.doesNotMatch(apiSource, /\bchainlinkPrice\b/);

assert.match(storeSource, /snapshot\.sources\.coinbase/);
assert.match(storeSource, /tick\.coinbasePrice/);
assert.match(storeSource, /tick\.coinbase\.candleUpdates/);
assert.doesNotMatch(storeSource, /tick\.chainlink/);
assert.match(appSource, /function CoinbaseComparisonChart/);
assert.match(appSource, /snapshot\?\.coinbase\.candlesByInterval\[selectedInterval\]/);
assert.match(appSource, /uiCoinbaseFeedb41e66a7/);
assert.match(i18nSource, /Coinbase 行情/);
assert.match(i18nSource, /Coinbase feed/);

assert.match(appSource, /CB PTB/);
assert.match(appSource, /CB-Binance/);
assert.doesNotMatch(appSource, /ChainLink VS PTB/);
assert.doesNotMatch(appSource, /snapshot\?\.chainlink/);
assert.match(i18nSource, /Latency Split/);
assert.match(i18nSource, /Market update age/);
assert.match(i18nSource, /Oldest source age/);
assert.match(i18nSource, /Frontend transport/);
assert.match(i18nSource, /CLOB market stale/);
assert.match(marketSocketSource, /marketPayloadRejectMs = 4000/);
assert.match(marketSocketSource, /marketStaleMs = 1500/);
assert.match(marketSocketSource, /marketFallbackCooldownMs = 1500/);
assert.equal(existsSync("apps/client/src/features/market/useMarketSocket.ts"), true);
assert.equal(existsSync("apps/client/src/features/user/useUserSocket.ts"), true);
assert.equal(existsSync("apps/client/src/features/trade/useOrderActions.ts"), true);
assert.equal(existsSync("apps/client/src/features/logs/logConfig.ts"), true);
assert.match(marketSocketSource, /export function useMarketSocket/);
assert.match(userSocketSource, /export function useUserSocket/);
assert.match(orderActionsSource, /export function useOrderActions/);
assert.match(logConfigSource, /export const DEFAULT_LOG_FACETS/);
assert.doesNotMatch(appSource, /const DEFAULT_LOG_FACETS: LogFacets/);
assert.match(orderActionsSource, /pendingOrderClientId/);
assert.match(appSource, /pendingOrderClientId=\{pendingOrderClientId\}/);
assert.match(appSource, /uiOrderSubmitteda80277a1/);
assert.match(i18nSource, /Order submitted/);
assert.doesNotMatch(appSource, /parseBulkUserText/);
assert.doesNotMatch(appSource, /splitDelimitedLine/);
assert.doesNotMatch(appSource, /const connectMarketSocket = async/);
assert.doesNotMatch(appSource, /const connectUserSocket = async/);
assert.doesNotMatch(appSource, /const handlePlaceOrder = async/);

assert.match(styleSource, /\.terminal-body \{ flex:1; min-height:0; display:grid; grid-template-columns:430px minmax\(0, 1fr\) 398px; overflow:hidden; \}/);
assert.match(styleSource, /@media \(max-width: 1500px\) and \(max-height: 940px\) \{[\s\S]*?\.terminal-body \{ grid-template-columns:390px minmax\(0, 1fr\) 360px; \}/);
assert.match(styleSource, /@media \(min-width: 1800px\) and \(min-height: 1000px\) \{[\s\S]*?\.terminal-body \{ grid-template-columns:500px minmax\(0, 0\.8fr\) 472px; \}/);
assert.match(styleSource, /\.terminal-current-orders/);
assert.match(styleSource, /\.terminal-current-order-card/);
assert.match(styleSource, /\.terminal-current-order-main/);
assert.match(styleSource, /\.terminal-current-order-meta/);
assert.doesNotMatch(styleSource, /\.terminal-chart-block\.chainlink/);

console.log("frontend-display-check ok");

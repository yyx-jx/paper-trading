import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";

const simulationSource = readFileSync("apps/server/src/services/simulation.ts", "utf8");
const indexSource = readFileSync("apps/server/src/index.ts", "utf8");
const configSource = readFileSync("apps/server/src/config.ts", "utf8");
const storeSource = readFileSync("apps/server/src/services/store.ts", "utf8");
const appSource = readFileSync("apps/client/src/App.tsx", "utf8");
const appStoreSource = readFileSync("apps/client/src/store/useAppStore.ts", "utf8");
const typesSource = readFileSync("apps/server/src/domain/types.ts", "utf8");

assert.equal(existsSync("apps/server/src/services/connectors/coinbase.ts"), true);
assert.equal(existsSync("apps/server/src/services/connectors/chainlink.ts"), false);
assert.equal(existsSync("apps/server/src/services/connectors/polymarket-reference.ts"), false);

assert.match(simulationSource, /import \{ CoinbaseConnector \} from "\.\/connectors\/coinbase";/);
assert.doesNotMatch(simulationSource, /ChainlinkConnector/);
assert.doesNotMatch(simulationSource, /PolymarketReferenceResolver/);
assert.doesNotMatch(simulationSource, /ChainlinkReferenceRateLimitError/);
assert.doesNotMatch(simulationSource, /data\.chain\.link/);
assert.doesNotMatch(simulationSource, /chainlinkReference/);
assert.doesNotMatch(simulationSource, /queueRoundPolymarketReferenceResolution/);
assert.match(simulationSource, /private readonly coinbaseConnector: CoinbaseConnector;/);
assert.match(simulationSource, /private coinbaseState: CoinbaseConnectorState;/);
assert.match(simulationSource, /private coinbaseCandlesByInterval = createEmptyCoinbaseIntervalBars\(\);/);
assert.match(simulationSource, /private pendingCoinbaseMarketCandles = new Map<number, MarketCandleRecord>\(\);/);
assert.match(simulationSource, /this\.scheduleSnapshotOnlyRefresh\("coinbase"\)/);

assert.match(configSource, /coinbaseEnabled: env\.COINBASE_ENABLED !== "false"/);
assert.match(configSource, /coinbaseWsUrl:/);
assert.match(configSource, /coinbaseRestUrl:/);
assert.match(configSource, /coinbaseWsStaleMs:/);
assert.doesNotMatch(configSource, /CHAINLINK_REFERENCE/);
assert.doesNotMatch(configSource, /DEFAULT_CHAINLINK_HISTORY_URL/);

assert.match(typesSource, /export type LatencySource = "binance" \| "coinbase" \| "clob" \| "system";/);
assert.match(typesSource, /source: "Binance" \| "Coinbase" \| "CLOB";/);
assert.match(typesSource, /export type MarketCandleSource = "coinbase";/);
assert.match(typesSource, /export interface CoinbaseConnectorState/);
assert.doesNotMatch(typesSource, /export interface ChainlinkConnectorState/);

assert.match(storeSource, /coinbaseEnabled: boolean/);
assert.match(storeSource, /source === "Coinbase" && !coinbaseEnabled/);
assert.match(storeSource, /coinbasePrice:/);
assert.match(storeSource, /coinbase: \{/);
assert.match(storeSource, /coinbaseOpenPrice:/);
assert.match(storeSource, /coinbaseClosePrice:/);
assert.match(storeSource, /chainlink_open_price DOUBLE PRECISION/);
assert.match(storeSource, /chainlink_close_price DOUBLE PRECISION/);

const createMarketTickBody = indexSource.match(/function createMarketTickPayload[\s\S]*?^}/m)?.[0] ?? "";
const createMarketRealtimeTickBody = indexSource.match(/function createMarketRealtimeTick[\s\S]*?^}/m)?.[0] ?? "";
const placeOrderBody = simulationSource.match(/async placeOrder\([\s\S]*?\n  async cancelOrder/)?.[0] ?? "";

assert.doesNotMatch(createMarketTickBody, /getMarketCandles|upsertMarketCandles/);
assert.doesNotMatch(createMarketRealtimeTickBody, /getMarketCandles|upsertMarketCandles/);
assert.doesNotMatch(placeOrderBody, /getMarketCandles|upsertMarketCandles/);
assert.match(storeSource, /prepareOrderBookSnapshotForOrder\(order: OrderRecord\)/);
assert.match(storeSource, /private async flushOrderBookSnapshotQueue\(\)/);
assert.match(simulationSource, /tradePersistSegments/);
assert.match(placeOrderBody, /persistOrderBookSnapshot/);
assert.match(placeOrderBody, /persistOrder/);
assert.match(simulationSource, /persistPosition/);
assert.match(simulationSource, /persistUser/);
assert.match(simulationSource, /persistOrderLifecycle/);
assert.match(indexSource, /engine\.withCurrentRoundCoinbaseOpenReference\(round\)/);
assert.match(createMarketRealtimeTickBody, /latestCandleUpdates\(stamped\.coinbase\.candlesByInterval\)/);

assert.match(appSource, /snapshot\?\.coinbase\.candlesByInterval\[selectedInterval\]/);
assert.match(appSource, /CoinbaseComparisonChart/);
assert.match(appSource, /CB PTB/);
assert.match(appSource, /CB-Binance/);
assert.doesNotMatch(appSource, /snapshot\?\.chainlink/);
assert.match(appStoreSource, /tick\.coinbase\.candleUpdates/);
assert.doesNotMatch(appStoreSource, /tick\.chainlink/);

console.log("flow-regression-check ok");

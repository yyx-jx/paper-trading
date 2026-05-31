import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { readSimulationServiceSource, readStoreServiceSource } from "./source-contracts";

const simulationSource = readSimulationServiceSource();
const indexSource = readFileSync("apps/server/src/index.ts", "utf8");
const configSource = readFileSync("apps/server/src/config.ts", "utf8");
const storeSource = readStoreServiceSource();
const appSource = readFileSync("apps/client/src/App.tsx", "utf8");
const appStoreSource = readFileSync("apps/client/src/store/useAppStore.ts", "utf8");
const typesSource = readFileSync("apps/server/src/domain/types.ts", "utf8");

assert.equal(existsSync("apps/server/src/services/connectors/coinbase.ts"), true);
assert.equal(existsSync("apps/server/src/services/connectors/chainlink.ts"), false);
assert.equal(existsSync("apps/server/src/services/connectors/polymarket-reference.ts"), false);
assert.equal(existsSync("apps/server/src/ws/session.ts"), true);
assert.equal(existsSync("apps/server/src/ws/heartbeat.ts"), true);
assert.equal(existsSync("apps/server/src/payloads/market.ts"), true);
assert.equal(existsSync("apps/server/src/payloads/user.ts"), true);
assert.equal(existsSync("apps/server/src/services/log-export-stream.ts"), true);

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

const placeOrderBody = simulationSource.match(/async placeOrder\([\s\S]*?\n  async cancelOrder/)?.[0] ?? "";
const wsSessionSource = readFileSync("apps/server/src/ws/session.ts", "utf8");
const wsHeartbeatSource = readFileSync("apps/server/src/ws/heartbeat.ts", "utf8");
const marketPayloadSource = readFileSync("apps/server/src/payloads/market.ts", "utf8");
const userPayloadSource = readFileSync("apps/server/src/payloads/user.ts", "utf8");
const logExportStreamSource = readFileSync("apps/server/src/services/log-export-stream.ts", "utf8");
const createMarketTickBody = marketPayloadSource.match(/createMarketTickPayload[\s\S]*?=> \{/m)?.[0] ?? "";
const createMarketRealtimeTickBody = marketPayloadSource.match(/function createMarketRealtimeTick[\s\S]*?^}/m)?.[0] ?? "";

assert.doesNotMatch(createMarketTickBody, /getMarketCandles|upsertMarketCandles/);
assert.doesNotMatch(createMarketRealtimeTickBody, /getMarketCandles|upsertMarketCandles/);
assert.doesNotMatch(placeOrderBody, /getMarketCandles|upsertMarketCandles/);
assert.match(storeSource, /prepareOrderBookSnapshotForOrder\(order: OrderRecord\)/);
assert.match(storeSource, /private async flushOrderBookSnapshotQueue\(\)/);
assert.match(storeSource, /private ordersByUserId = new Map<string, OrderRecord\[\]>\(\)/);
assert.match(storeSource, /private positionsByUserId = new Map<string, PositionRecord\[\]>\(\)/);
assert.match(storeSource, /private operatedRoundIdsByUserId = new Map<string, Set<string>>\(\)/);
assert.match(storeSource, /this\.cleanupRetentionIfDue\(event\.serverRecvTs\);/);
assert.doesNotMatch(storeSource, /await this\.cleanupRetentionIfDue\(event\.serverRecvTs\)/);
assert.match(simulationSource, /tradePersistSegments/);
assert.match(placeOrderBody, /persistOrderBookSnapshot/);
assert.match(placeOrderBody, /persistOrder/);
assert.match(simulationSource, /persistPosition/);
assert.match(simulationSource, /persistUser/);
assert.match(simulationSource, /persistOrderLifecycle/);
assert.match(marketPayloadSource, /withCurrentRoundCoinbaseOpenReference\(round\)/);
assert.match(marketPayloadSource, /latestCandleUpdates\(stamped\.coinbase\.candlesByInterval\)/);
assert.match(wsSessionSource, /createWsTicket/);
assert.match(wsSessionSource, /consumeWsTicket/);
assert.match(wsHeartbeatSource, /attachHeartbeat/);
assert.match(marketPayloadSource, /createMarketTickPayload/);
assert.match(userPayloadSource, /createUserTradePayload/);
assert.doesNotMatch(indexSource, /function createWsTicket/);
assert.doesNotMatch(indexSource, /function attachHeartbeat/);
assert.doesNotMatch(indexSource, /function createMarketRealtimeTick/);
assert.doesNotMatch(indexSource, /function createUserTradePayload/);
assert.match(logExportStreamSource, /Readable\.from/);
assert.match(indexSource, /createJsonlStream\(logs\)/);
assert.doesNotMatch(indexSource, /logs\.map\(\(log\) => JSON\.stringify\(log\)\)\.join/);

assert.match(appSource, /snapshot\?\.coinbase\.candlesByInterval\[selectedInterval\]/);
assert.match(appSource, /CoinbaseComparisonChart/);
assert.match(appSource, /CB PTB/);
assert.match(appSource, /CB-Binance/);
assert.doesNotMatch(appSource, /snapshot\?\.chainlink/);
assert.match(appStoreSource, /tick\.coinbase\.candleUpdates/);
assert.doesNotMatch(appStoreSource, /tick\.chainlink/);

console.log("flow-regression-check ok");

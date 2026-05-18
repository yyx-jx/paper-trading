import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const serverTypesSource = readFileSync("apps/server/src/domain/types.ts", "utf8");
const clientTypesSource = readFileSync("apps/client/src/utils/api.ts", "utf8");
const serverIndexSource = readFileSync("apps/server/src/index.ts", "utf8");
const clientStoreSource = readFileSync("apps/client/src/store/useAppStore.ts", "utf8");

function extractInterface(source: string, name: string) {
  const start = source.indexOf(`export interface ${name}`);
  assert.ok(start >= 0, `Missing interface ${name}`);
  const nextInterface = source.indexOf("\nexport interface ", start + 1);
  return source.slice(start, nextInterface >= 0 ? nextInterface : undefined);
}

function extractFunction(source: string, name: string) {
  const start = source.indexOf(`function ${name}`);
  assert.ok(start >= 0, `Missing function ${name}`);
  const nextFunction = source.indexOf("\nfunction ", start + 1);
  return source.slice(start, nextFunction >= 0 ? nextFunction : undefined);
}

const serverTickType = extractInterface(serverTypesSource, "MarketRealtimeTick");
const clientTickType = extractInterface(clientTypesSource, "MarketRealtimeTick");
const createTickPayload = extractFunction(serverIndexSource, "createMarketRealtimeTick");
const mergeRealtimeTick = extractFunction(clientStoreSource, "mergeRealtimeTick");

assert.doesNotMatch(serverTickType, /\borderBooks\b/, "Server MarketRealtimeTick must not carry full orderBooks.");
assert.doesNotMatch(clientTickType, /\borderBooks\b/, "Client MarketRealtimeTick must not carry full orderBooks.");
assert.doesNotMatch(createTickPayload, /\borderBooks\s*:/, "createMarketRealtimeTick must not serialize full orderBooks.");
assert.doesNotMatch(mergeRealtimeTick, /tick\.orderBooks/, "mergeRealtimeTick must preserve full order books from snapshots.");
assert.match(serverTypesSource, /broadcastBuildMs\?: number/);
assert.match(serverTypesSource, /broadcastFanoutSize\?: number/);
assert.match(serverTypesSource, /droppedForBackpressure\?: boolean/);
assert.match(clientTypesSource, /broadcastBuildMs\?: number/);
assert.match(clientTypesSource, /broadcastFanoutSize\?: number/);
assert.match(clientTypesSource, /droppedForBackpressure\?: boolean/);
assert.match(serverIndexSource, /type MarketBroadcastFrame =/);
assert.match(serverIndexSource, /const marketBroadcastClients = new Set<MarketBroadcastClient>\(\)/);
assert.match(serverIndexSource, /function createMarketBroadcastFrame/);
assert.match(serverIndexSource, /JSON\.stringify\(\{ type: "market:tick", data \}\)/);
assert.match(serverIndexSource, /function broadcastMarketTickFrame/);
assert.match(serverIndexSource, /marketBroadcastClients\.size/);
assert.match(serverIndexSource, /client\.socket\.bufferedAmount > 0/);
assert.match(serverIndexSource, /droppedForBackpressure/);
assert.match(serverIndexSource, /recordMarketBroadcast/);
assert.match(serverIndexSource, /scheduleFullSnapshotForClient/);
assert.match(serverIndexSource, /MARKET_WS_FULL_SNAPSHOT_STAGGER_MS/);
assert.match(serverIndexSource, /MARKET_WS_INITIAL_FULL_SNAPSHOT_MAX_DELAY_MS/);
assert.doesNotMatch(serverIndexSource, /marketBroadcastPendingSince/);
assert.match(serverIndexSource, /store\.emitter\.on\("market:update", handleMarketUpdate\)/);
assert.match(serverIndexSource, /store\.emitter\.off\("market:update", handleMarketUpdate\)/);
assert.doesNotMatch(serverIndexSource, /store\.emitter\.on\("market:update", tickListener\)/);

const multiClientLatencySource = readFileSync("scripts/market-ws-multi-client-latency.ts", "utf8");
assert.match(multiClientLatencySource, /LOAD_TEST_CLOCK_SAMPLES/);
assert.match(multiClientLatencySource, /healthRttP95/);
assert.match(multiClientLatencySource, /rawTickServerToClientP95/);
assert.match(multiClientLatencySource, /wsSendStartToClientP95/);
assert.match(multiClientLatencySource, /payloadBytesP95/);

console.log("market-tick-payload-check ok");

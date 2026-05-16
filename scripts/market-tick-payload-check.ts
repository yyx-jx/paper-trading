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
assert.match(serverIndexSource, /if \(pendingTick \|\| Date\.now\(\) - lastTickSentAt >= MARKET_WS_MIN_INTERVAL_MS\)[\s\S]*?sendTick\(\)[\s\S]*?if \(pendingFull \|\| Date\.now\(\) - lastFullSentAt >= MARKET_WS_FULL_SNAPSHOT_INTERVAL_MS\)/);
assert.match(serverIndexSource, /const data = createMarketTickPayload\(coalescedCount, pendingSince\)/);
assert.match(serverIndexSource, /return sendEnvelope\("market:tick", data, "tick"\)/);
assert.match(serverIndexSource, /tickTimer = setInterval\(tickListener, Math\.max\(MARKET_WS_MIN_INTERVAL_MS, 50\)\)/);
assert.match(serverIndexSource, /fullTimer = setInterval\(fullListener, MARKET_WS_FULL_SNAPSHOT_INTERVAL_MS\)/);
assert.match(serverIndexSource, /store\.emitter\.on\("market:update", tickListener\)/);
assert.match(serverIndexSource, /store\.emitter\.off\("market:update", tickListener\)/);

console.log("market-tick-payload-check ok");

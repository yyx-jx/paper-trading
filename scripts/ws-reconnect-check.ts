import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";

function readText(filePath: string) {
  return readFileSync(filePath, "utf8");
}

const runtimePath = path.join("apps", "client", "src", "features", "realtime", "socket-runtime.ts");
const marketMessagePath = path.join("apps", "client", "src", "features", "market", "market-ws-message.ts");
const marketReconnectPolicyPath = path.join("apps", "client", "src", "features", "market", "market-reconnect-policy.ts");
const userHeartbeatPath = path.join("apps", "server", "src", "ws", "user-heartbeat.ts");
const userSocketSource = readText("apps/client/src/features/user/useUserSocket.ts");
const marketSocketSource = readText("apps/client/src/features/market/useMarketSocket.ts");
const clientApiSource = readText("apps/client/src/utils/api.ts");
const serverIndexSource = readText("apps/server/src/index.ts");
const coinbaseSource = readText("apps/server/src/services/connectors/coinbase.ts");
const binanceSource = readText("apps/server/src/services/connectors/binance.ts");

assert.equal(existsSync(runtimePath), true, "Missing shared realtime socket runtime helper.");
assert.equal(existsSync(marketMessagePath), true, "Missing market WS message helper.");
assert.equal(existsSync(marketReconnectPolicyPath), true, "Missing market reconnect policy helper.");
assert.equal(existsSync(userHeartbeatPath), true, "Missing server user heartbeat helper.");

const runtimeSource = readText(runtimePath);
const marketMessageSource = readText(marketMessagePath);
const marketReconnectPolicySource = readText(marketReconnectPolicyPath);
const userHeartbeatSource = readText(userHeartbeatPath);

assert.match(runtimeSource, /export function createRealtimeSocketRuntime/);
assert.match(runtimeSource, /export function evaluateRealtimeWatchdog/);
assert.match(marketMessageSource, /export function parseMarketWsMessage/);
assert.match(marketMessageSource, /export function marketPayloadAgeMs/);
assert.match(marketMessageSource, /export function isMarketPayloadTooOld/);
assert.match(marketReconnectPolicySource, /export function shouldRejectStaleMarketPayload/);
assert.match(marketReconnectPolicySource, /export function shouldRecoverClosedMarketSocket/);
assert.match(marketMessageSource, /reconnectStaleMs:\s*8_000/);
assert.match(marketMessageSource, /staleMs:\s*3_000/);
assert.match(marketMessageSource, /fallbackCooldownMs:\s*8_000/);
assert.match(userSocketSource, /createRealtimeSocketRuntime/);
assert.match(marketSocketSource, /createRealtimeSocketRuntime/);
assert.match(marketSocketSource, /parseMarketWsMessage/);
assert.match(marketSocketSource, /shouldRejectStaleMarketPayload/);
assert.match(marketSocketSource, /shouldRecoverClosedMarketSocket/);
assert.match(marketSocketSource, /acceptedIdleMs: marketRuntime\.acceptedIdleMs\(now\)/);
assert.doesNotMatch(
  marketSocketSource,
  /payload_too_old[\s\S]{0,220}socket\.close\(\);/,
  "stale market payloads should trigger refresh, not immediate socket close"
);
assert.doesNotMatch(
  marketSocketSource,
  /payload_too_old[\s\S]{0,160}refreshMarketSnapshot\(\)/,
  "stale market payloads should no longer trigger immediate HTTP fallback"
);
assert.doesNotMatch(
  marketSocketSource,
  /tick_payload_too_old[\s\S]{0,220}socket\.close\(\);/,
  "stale market ticks should trigger refresh, not immediate socket close"
);
assert.doesNotMatch(
  marketSocketSource,
  /tick_payload_too_old[\s\S]{0,160}refreshMarketSnapshot\(\)/,
  "stale market ticks should no longer trigger immediate HTTP fallback"
);
assert.doesNotMatch(
  marketSocketSource,
  /else if \(marketIdleMs > MARKET_SOCKET_TIMING\.staleMs\)\s*\{\s*void refreshMarketSnapshot\(\);\s*\}/,
  "foreground recovery should not refresh an open market socket just because data is stale"
);

assert.match(userSocketSource, /"user:heartbeat"/);
assert.match(userHeartbeatSource, /"user:heartbeat"/);
assert.match(userHeartbeatSource, /export function sendUserHeartbeat/);
assert.match(serverIndexSource, /sendUserHeartbeat\(\{[\s\S]*?\}\);\s*queueUserPayload\(createUserPayloadRequest\("full"\)\)/);
assert.match(clientApiSource, /export type UserWsMessage/);
assert.match(clientApiSource, /type: "user:heartbeat"/);
assert.match(coinbaseSource, /private closeSocketQuietly\(socket: WebSocket\)/);
assert.match(coinbaseSource, /socket\.removeAllListeners\(\);\s*socket\.once\("error", \(\) => undefined\);\s*socket\.close\(\);/s);
assert.match(binanceSource, /private closeSocketQuietly\(socket: WebSocket\)/);
assert.match(binanceSource, /socket\.removeAllListeners\(\);\s*socket\.once\("error", \(\) => undefined\);\s*socket\.close\(\);/s);
assert.doesNotMatch(userSocketSource, /const userReconnectStaleMs = 12_000/);
assert.doesNotMatch(userSocketSource, /socket\.close\(\);\s*\n\s*}\s*,\s*500\);/);
assert.doesNotMatch(marketSocketSource, /function transportAgeMs/);
assert.doesNotMatch(marketSocketSource, /function extractMarketPayloadPublishTs/);
assert.doesNotMatch(marketSocketSource, /console\.(log|warn|debug|info)/);
assert.ok(marketSocketSource.split(/\r?\n/).length <= 300, "Market socket hook should stay under 300 lines.");

async function testWatchdogDecisions() {
  const { evaluateRealtimeWatchdog } = await import("../apps/client/src/features/realtime/socket-runtime");

  assert.deepEqual(
    evaluateRealtimeWatchdog({
      socketState: WebSocket.OPEN,
      acceptedIdleMs: 13_000,
      idleMs: 13_000,
      fallbackCooldownMs: 5_000,
      reconnectStaleMs: undefined,
      staleMs: undefined,
      sinceLastFallbackMs: 13_000
    }),
    { shouldRefresh: false, shouldReconnect: false, shouldClose: false }
  );

  assert.deepEqual(
    evaluateRealtimeWatchdog({
      socketState: WebSocket.OPEN,
      idleMs: 400,
      acceptedIdleMs: 4_500,
      fallbackCooldownMs: 8_000,
      reconnectStaleMs: 8_000,
      staleMs: 3_000,
      sinceLastFallbackMs: 4_500
    }),
    { shouldRefresh: false, shouldReconnect: false, shouldClose: false }
  );

  assert.deepEqual(
    evaluateRealtimeWatchdog({
      socketState: WebSocket.OPEN,
      idleMs: 4_500,
      acceptedIdleMs: 4_500,
      fallbackCooldownMs: 8_000,
      reconnectStaleMs: 8_000,
      staleMs: 3_000,
      sinceLastFallbackMs: 4_500
    }),
    { shouldRefresh: false, shouldReconnect: false, shouldClose: false }
  );

  assert.deepEqual(
    evaluateRealtimeWatchdog({
      socketState: WebSocket.OPEN,
      idleMs: 4_500,
      acceptedIdleMs: 4_500,
      fallbackCooldownMs: 8_000,
      reconnectStaleMs: 8_000,
      staleMs: 3_000,
      sinceLastFallbackMs: 8_500
    }),
    { shouldRefresh: true, shouldReconnect: false, shouldClose: false }
  );

  assert.deepEqual(
    evaluateRealtimeWatchdog({
      socketState: WebSocket.OPEN,
      idleMs: 8_500,
      acceptedIdleMs: 8_500,
      fallbackCooldownMs: 8_000,
      reconnectStaleMs: 8_000,
      staleMs: 3_000,
      sinceLastFallbackMs: 8_500
    }),
    { shouldRefresh: true, shouldReconnect: false, shouldClose: true }
  );

  assert.deepEqual(
    evaluateRealtimeWatchdog({
      socketState: WebSocket.CONNECTING,
      acceptedIdleMs: 30_000,
      idleMs: 30_000,
      fallbackCooldownMs: 8_000,
      reconnectStaleMs: 8_000,
      staleMs: 3_000,
      sinceLastFallbackMs: 30_000,
      connectionAgeMs: 800,
      connectingStaleMs: 5_000
    }),
    { shouldRefresh: false, shouldReconnect: false, shouldClose: false }
  );

  assert.deepEqual(
    evaluateRealtimeWatchdog({
      socketState: WebSocket.CONNECTING,
      acceptedIdleMs: 30_000,
      idleMs: 30_000,
      fallbackCooldownMs: 8_000,
      reconnectStaleMs: 8_000,
      staleMs: 3_000,
      sinceLastFallbackMs: 30_000,
      connectionAgeMs: 6_000,
      connectingStaleMs: 5_000
    }),
    { shouldRefresh: false, shouldReconnect: false, shouldClose: true }
  );

  assert.deepEqual(
    evaluateRealtimeWatchdog({
      socketState: WebSocket.CLOSING,
      acceptedIdleMs: 30_000,
      idleMs: 30_000,
      fallbackCooldownMs: 8_000,
      reconnectStaleMs: 8_000,
      staleMs: 3_000,
      sinceLastFallbackMs: 30_000
    }),
    { shouldRefresh: false, shouldReconnect: false, shouldClose: false }
  );

  assert.deepEqual(
    evaluateRealtimeWatchdog({
      socketState: WebSocket.CLOSED,
      acceptedIdleMs: 30_000,
      idleMs: 30_000,
      fallbackCooldownMs: 8_000,
      reconnectStaleMs: 8_000,
      staleMs: 3_000,
      sinceLastFallbackMs: 30_000
    }),
    { shouldRefresh: true, shouldReconnect: true, shouldClose: false }
  );
}

async function testUserHeartbeatPayload() {
  const { createUserHeartbeatPayload } = await import("../apps/server/src/ws/user-heartbeat");
  const payload = createUserHeartbeatPayload(12345);
  assert.deepEqual(payload, {
    type: "user:heartbeat",
    data: { serverNow: 12345 }
  });
  assert.match(userHeartbeatSource, /setInterval/);
  assert.doesNotMatch(userHeartbeatSource, /createUserFullPayload/);
  assert.doesNotMatch(userHeartbeatSource, /createUserTradePayload/);
}

async function main() {
  await testWatchdogDecisions();
  await testUserHeartbeatPayload();
  console.log("ws-reconnect-check ok");
}

void main();

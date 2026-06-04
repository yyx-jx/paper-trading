import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";

function readText(filePath: string) {
  return readFileSync(filePath, "utf8");
}

const runtimePath = path.join("apps", "client", "src", "features", "realtime", "socket-runtime.ts");
const userHeartbeatPath = path.join("apps", "server", "src", "ws", "user-heartbeat.ts");
const userSocketSource = readText("apps/client/src/features/user/useUserSocket.ts");
const marketSocketSource = readText("apps/client/src/features/market/useMarketSocket.ts");
const clientApiSource = readText("apps/client/src/utils/api.ts");

assert.equal(existsSync(runtimePath), true, "Missing shared realtime socket runtime helper.");
assert.equal(existsSync(userHeartbeatPath), true, "Missing server user heartbeat helper.");

const runtimeSource = readText(runtimePath);
const userHeartbeatSource = readText(userHeartbeatPath);

assert.match(runtimeSource, /export function createRealtimeSocketRuntime/);
assert.match(runtimeSource, /export function evaluateRealtimeWatchdog/);
assert.match(userSocketSource, /createRealtimeSocketRuntime/);
assert.match(marketSocketSource, /createRealtimeSocketRuntime/);

assert.match(userSocketSource, /"user:heartbeat"/);
assert.match(userHeartbeatSource, /"user:heartbeat"/);
assert.match(clientApiSource, /export type UserWsMessage/);
assert.match(clientApiSource, /type: "user:heartbeat"/);
assert.doesNotMatch(userSocketSource, /const userReconnectStaleMs = 12_000/);
assert.doesNotMatch(userSocketSource, /socket\.close\(\);\s*\n\s*}\s*,\s*500\);/);

async function testWatchdogDecisions() {
  const { evaluateRealtimeWatchdog } = await import("../apps/client/src/features/realtime/socket-runtime");

  assert.deepEqual(
    evaluateRealtimeWatchdog({
      socketState: WebSocket.OPEN,
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
      idleMs: 4_500,
      fallbackCooldownMs: 1_500,
      reconnectStaleMs: 4_000,
      staleMs: 1_500,
      sinceLastFallbackMs: 4_500
    }),
    { shouldRefresh: true, shouldReconnect: false, shouldClose: true }
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

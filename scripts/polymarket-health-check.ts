import assert from "node:assert/strict";
import type { SourceComponentHealth } from "../apps/server/src/domain/types";
import {
  derivePolymarketSourceHealth,
  type PolymarketHealthComponent
} from "../apps/server/src/services/connectors/polymarket-health";

const now = 1778684000000;

function component(
  name: PolymarketHealthComponent,
  state: SourceComponentHealth["state"],
  ageMs: number,
  message: string
): SourceComponentHealth {
  return {
    name,
    state,
    sourceEventTs: now - ageMs,
    serverRecvTs: now - Math.min(ageMs, 500),
    message
  };
}

function derive(components: Partial<Record<PolymarketHealthComponent, SourceComponentHealth>>) {
  return derivePolymarketSourceHealth({
    symbol: "BTC",
    reconnectCount: 42,
    now,
    orderBookFreshMs: 15_000,
    components
  });
}

function testFreshBookKeepsClobHealthyWhenWsAndTradesFail() {
  const status = derive({
    discovery: component("discovery", "healthy", 1_000, "Tracking current BTC 5m market."),
    orderBook: component("orderBook", "healthy", 900, "Reading current UP/DOWN order books."),
    marketWs: component("marketWs", "degraded", 30_000, "Polymarket market WebSocket closed."),
    trades: component("trades", "degraded", 45_000, "Polymarket trades API fetch failed.")
  });

  assert.equal(status.state, "healthy");
  assert.equal(status.sourceEventTs, now - 900);
  assert.equal(status.components?.orderBook.state, "healthy");
  assert.equal(status.components?.marketWs.state, "degraded");
  assert.match(status.message ?? "", /Order book healthy/);
  assert.match(status.message ?? "", /Market WebSocket degraded/);
}

function testStaleBookDegradesClobEvenIfDiscoveryStillTracksMarket() {
  const status = derive({
    discovery: component("discovery", "healthy", 1_000, "Tracking current BTC 5m market."),
    orderBook: component("orderBook", "healthy", 20_000, "Last order book snapshot."),
    marketWs: component("marketWs", "degraded", 20_000, "Polymarket market WebSocket closed."),
    trades: component("trades", "healthy", 1_000, "Recent trades refreshed.")
  });

  assert.equal(status.state, "degraded");
  assert.match(status.message ?? "", /Order book stale/);
}

function testMissingBookLeavesClobReconnecting() {
  const status = derive({
    discovery: component("discovery", "healthy", 500, "Tracking current BTC 5m market."),
    orderBook: component("orderBook", "reconnecting", 60_000, "Waiting for order book snapshot.")
  });

  assert.equal(status.state, "reconnecting");
  assert.match(status.message ?? "", /Order book unavailable/);
}

testFreshBookKeepsClobHealthyWhenWsAndTradesFail();
testStaleBookDegradesClobEvenIfDiscoveryStillTracksMarket();
testMissingBookLeavesClobReconnecting();

console.log("polymarket health checks passed");

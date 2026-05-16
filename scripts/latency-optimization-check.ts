import assert from "node:assert/strict";
import { PolymarketConnector } from "../apps/server/src/services/connectors/polymarket";
import { SimulationEngine } from "../apps/server/src/services/simulation";
import type { OrderBookSnapshot, PositionRecord, RoundRecord, TradeSide, UserRecord } from "../apps/server/src/domain/types";

const now = Date.now();

function book(side: TradeSide, snapshotTs = now - 60_000): OrderBookSnapshot {
  return {
    snapshotId: `book-${side}-${snapshotTs}`,
    snapshotTs,
    bestBid: 0.49,
    bestAsk: 0.51,
    midPrice: 0.5,
    bids: [{ price: 0.49, qty: 1_000 }],
    asks: [{ price: 0.51, qty: 1_000 }]
  };
}

function round(): RoundRecord {
  return {
    id: "round-latency-test",
    marketId: "market-latency-test",
    symbol: "BTC",
    marketSlug: "btc-updown-5m-latency-test",
    upTokenId: "token-up",
    downTokenId: "token-down",
    startAt: now - 60_000,
    endAt: now + 240_000,
    priceToBeat: 80_000,
    status: "Trading",
    pollCount: 0,
    acceptingOrders: true
  };
}

function user(): UserRecord {
  return {
    id: "u-latency",
    username: "latency",
    password: "latency",
    displayName: "Latency Tester",
    role: "Tester",
    language: "zh-CN",
    permissionCodes: [],
    availableUsdc: 1_000,
    isActive: true,
    createdAt: now,
    updatedAt: now
  };
}

async function testCachedExecutionBookDoesNotCallRest() {
  const engine = Object.create(SimulationEngine.prototype) as Record<string, unknown>;
  let restCalls = 0;
  Object.assign(engine, {
    config: { polymarketBookPollMs: 1_000 },
    polymarketState: {
      currentMarket: { upTokenId: "token-up", downTokenId: "token-down" },
      orderBooks: {
        UP: book("UP", now - 120_000),
        DOWN: book("DOWN", now - 120_000)
      }
    },
    polymarketConnector: {
      fetchBookByToken: async () => {
        restCalls += 1;
        throw new Error("REST should not be called when a token-compatible cached book has depth.");
      },
      fetchBookForSide: async () => {
        restCalls += 1;
        throw new Error("REST should not be called when a token-compatible cached book has depth.");
      }
    }
  });

  const result = await (engine as any).fetchExecutionBook("UP", round());
  assert.equal(restCalls, 0);
  assert.equal(result.source, "stale_cache");
  assert.equal(result.book.snapshotId, "book-UP-" + (now - 120_000));
  assert.ok(result.ageMs >= 120_000);
}

async function testCloseSideAggregatesIntoOneSellOrder() {
  const positions: PositionRecord[] = [
    {
      id: "pos-1",
      userId: "u-latency",
      roundId: "round-latency-test",
      side: "UP",
      qty: 10,
      averageEntry: 0.5,
      notionalSpent: 5,
      currentMark: 0.5,
      unrealizedPnl: 0,
      realizedPnl: 0,
      status: "open",
      openedAt: now - 20_000
    },
    {
      id: "pos-2",
      userId: "u-latency",
      roundId: "round-latency-test",
      side: "UP",
      qty: 5,
      averageEntry: 0.5,
      notionalSpent: 2.5,
      currentMark: 0.5,
      unrealizedPnl: 0,
      realizedPnl: 0,
      status: "open",
      openedAt: now - 10_000
    }
  ];
  const calls: unknown[] = [];
  const testRound = round();
  const engine = Object.create(SimulationEngine.prototype) as Record<string, unknown>;
  Object.assign(engine, {
    config: { symbol: "BTC" },
    store: {
      positions,
      newTraceId: () => "trace-close",
      newId: (prefix: string) => `${prefix}-close`
    },
    getActiveRound: () => testRound,
    assertCanSellOrder: () => undefined,
    captureActionSnapshot: () => ({}),
    placeOrder: async (_user: UserRecord, payload: Record<string, unknown>) => {
      calls.push(payload);
      positions.forEach((position) => {
        position.status = "closed";
        position.qty = 0;
      });
      return {
        order: {
          id: "ord-close",
          status: "filled",
          filledQty: 15,
          notionalUsdc: 7.5,
          avgFillPrice: 0.5,
          matchLatencyMs: 2
        }
      };
    },
    writeAuditLog: async () => undefined,
    writeBehaviorLog: async () => undefined,
    createBehaviorLog: (input: unknown) => input
  });

  const result = await (engine as any).closeSide(user(), { side: "UP" });
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0], {
    action: "sell",
    side: "UP",
    qty: 15,
    orderKind: "market",
    clientSendTs: undefined,
    positionIds: ["pos-1", "pos-2"],
    exitType: "close_side"
  });
  assert.equal(result.closedPositionsCount, 2);
  assert.equal(result.totalQty, 15);
  assert.equal(result.matchLatencyMs, 2);
}

async function testFailedSellPositionThrows() {
  const testRound = round();
  const position: PositionRecord = {
    id: "pos-fail",
    userId: "u-latency",
    roundId: testRound.id,
    side: "DOWN",
    qty: 3,
    averageEntry: 0.5,
    notionalSpent: 1.5,
    currentMark: 0.5,
    unrealizedPnl: 0,
    realizedPnl: 0,
    status: "open",
    openedAt: now - 10_000
  };
  const engine = Object.create(SimulationEngine.prototype) as Record<string, unknown>;
  Object.assign(engine, {
    config: { symbol: "BTC" },
    store: {
      positions: [position],
      getRoundById: () => testRound,
      newTraceId: () => "trace-sell-fail",
      newId: (prefix: string) => `${prefix}-fail`
    },
    captureActionSnapshot: () => ({}),
    assertCanSellPosition: () => undefined,
    placeOrder: async () => ({
      order: {
        id: "ord-fail",
        traceId: "trace-order-fail",
        status: "failed",
        failureReason: "Polymarket CLOB depth was insufficient for full fill."
      }
    }),
    writeAuditLog: async () => undefined,
    writeBehaviorLog: async () => undefined,
    createBehaviorLog: (input: unknown) => input
  });

  await assert.rejects(
    () => (engine as any).sellPosition(user(), "pos-fail"),
    /Polymarket CLOB depth was insufficient/
  );
  assert.equal(position.status, "open");
}

function connectorFixture() {
  const market = {
    id: "market-latency-test",
    conditionId: "condition-latency-test",
    slug: "btc-updown-5m-latency-test",
    upTokenId: "token-up",
    downTokenId: "token-down",
    upOutcome: "Up",
    downOutcome: "Down",
    outcomePrices: [0.5, 0.5],
    startAt: now - 60_000,
    endAt: now + 240_000,
    acceptingOrders: true,
    closed: false
  };
  const connector = Object.create(PolymarketConnector.prototype) as any;
  let emits = 0;
  Object.assign(connector, {
    config: { symbol: "BTC", bookPollMs: 1000 },
    reconnectCount: 0,
    healthComponents: {},
    state: {
      currentMarket: market,
      orderBooks: {
        UP: {
          snapshotId: "up-initial",
          snapshotTs: now,
          bestBid: 0.49,
          bestAsk: 0.52,
          midPrice: 0.505,
          bids: [
            { price: 0.49, qty: 100 },
            { price: 0.48, qty: 100 }
          ],
          asks: [
            { price: 0.52, qty: 100 },
            { price: 0.53, qty: 100 }
          ]
        },
        DOWN: book("DOWN", now)
      },
      recentTrades: [],
      delta: 0,
      volume: 0,
      status: { source: "CLOB", symbol: "BTC", state: "healthy" }
    },
    emit: () => {
      emits += 1;
    }
  });
  return { connector, market, emits: () => emits };
}

async function testClobMarketWsBestBidAskUpdatesTopOnly() {
  const { connector, market, emits } = connectorFixture();

  connector.handleMarketWsMessage(
    {
      event_type: "best_bid_ask",
      asset_id: "token-up",
      best_bid: "0.50",
      best_ask: "0.51",
      timestamp: now + 100
    },
    market
  );

  assert.equal(connector.state.orderBooks.UP.bestBid, 0.5);
  assert.equal(connector.state.orderBooks.UP.bestAsk, 0.51);
  assert.equal(connector.state.orderBooks.UP.bids[0].price, 0.49);
  assert.equal(connector.state.orderBooks.UP.asks[0].price, 0.52);
  assert.equal(connector.state.status.state, "healthy");
  assert.match(connector.state.status.components.marketWs.message, /best bid\/ask/);
  assert.equal(emits(), 1);

  connector.handleMarketWsMessage(
    {
      event_type: "best_bid_ask",
      asset_id: "token-up",
      best_bid: "0.01",
      best_ask: "0.99",
      timestamp: now - 1
    },
    market
  );

  assert.equal(connector.state.orderBooks.UP.bestBid, 0.5);
  assert.equal(connector.state.orderBooks.UP.bestAsk, 0.51);
}

async function testClobMarketWsPriceChangeMaintainsDepth() {
  const { connector, market } = connectorFixture();

  connector.handleMarketWsMessage(
    {
      event_type: "price_change",
      price_changes: [
        { asset_id: "token-up", side: "BUY", price: "0.505", size: "25", timestamp: now + 100 },
        { asset_id: "token-up", side: "SELL", price: "0.52", size: "0", timestamp: now + 101 },
        { asset_id: "unknown-token", side: "BUY", price: "0.99", size: "99", timestamp: now + 102 }
      ],
      timestamp: now + 100
    },
    market
  );

  assert.equal(connector.state.orderBooks.UP.bids[0].price, 0.505);
  assert.equal(connector.state.orderBooks.UP.bids[0].qty, 25);
  assert.equal(connector.state.orderBooks.UP.bestBid, 0.505);
  assert.equal(connector.state.orderBooks.UP.asks[0].price, 0.53);
  assert.equal(connector.state.orderBooks.UP.bestAsk, 0.53);
  assert.equal(connector.state.orderBooks.UP.bids.some((level: { price: number }) => level.price === 0.99), false);
  assert.match(connector.state.status.components.marketWs.message, /price changes/);
}

async function testClobMarketWsBookStillCalibratesDepth() {
  const { connector, market } = connectorFixture();

  connector.handleMarketWsMessage(
    {
      event_type: "book",
      asset_id: "token-up",
      hash: "up-book-calibration",
      timestamp: now + 200,
      bids: [{ price: "0.47", size: "7" }],
      asks: [{ price: "0.54", size: "9" }]
    },
    market
  );

  assert.equal(connector.state.orderBooks.UP.snapshotId, "up-book-calibration");
  assert.deepEqual(connector.state.orderBooks.UP.bids, [{ price: 0.47, qty: 7 }]);
  assert.deepEqual(connector.state.orderBooks.UP.asks, [{ price: 0.54, qty: 9 }]);
  assert.equal(connector.state.orderBooks.UP.bestBid, 0.47);
  assert.equal(connector.state.orderBooks.UP.bestAsk, 0.54);
}

async function main() {
  await testCachedExecutionBookDoesNotCallRest();
  await testCloseSideAggregatesIntoOneSellOrder();
  await testFailedSellPositionThrows();
  await testClobMarketWsBestBidAskUpdatesTopOnly();
  await testClobMarketWsPriceChangeMaintainsDepth();
  await testClobMarketWsBookStillCalibratesDepth();
  console.log("latency-optimization-check ok");
}

void main();

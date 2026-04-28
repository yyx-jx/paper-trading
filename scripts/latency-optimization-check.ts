import assert from "node:assert/strict";
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
    assertCanCreateNewOrder: () => undefined,
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

async function main() {
  await testCachedExecutionBookDoesNotCallRest();
  await testCloseSideAggregatesIntoOneSellOrder();
  await testFailedSellPositionThrows();
  console.log("latency-optimization-check ok");
}

void main();

import { strict as assert } from "node:assert";
import type { OrderLifecycleRecord, PositionRecord } from "../apps/server/src/domain/types";
import { AppStore } from "../apps/server/src/services/store";

function upsertById<T extends { id: string }>(rows: T[], row: T) {
  const index = rows.findIndex((current) => current.id === row.id);
  if (index >= 0) {
    rows[index] = row;
    return;
  }
  rows.push(row);
}

function createPosition(id: string, userId = "user-1"): PositionRecord {
  return {
    id,
    buyOrderId: `buy-${id}`,
    userId,
    roundId: "round-1",
    side: "UP",
    qty: 1,
    lockedQty: 0,
    averageEntry: 0.5,
    notionalSpent: 0.5,
    currentMark: 0.5,
    unrealizedPnl: 0,
    realizedPnl: 0,
    status: "open",
    openedAt: 1_700_000_000_000
  };
}

function createLifecycle(id: string, buyOrderId: string, remainingTokenQty: number, orderTimestampMs: number): OrderLifecycleRecord {
  return {
    id,
    buyOrderId,
    traceId: `trace-${id}`,
    userId: "user-1",
    testerId: "tester-1",
    roundId: "round-1",
    symbol: "BTC",
    assetClass: "BTC",
    marketId: "market-1",
    marketSlug: "btc-5m-updown",
    direction: "UP",
    orderTimestampMs,
    entryTokenPrice: 0.5,
    volumeTokenQty: remainingTokenQty,
    remainingTokenQty,
    closedTokenQty: 0,
    positionNotional: remainingTokenQty * 0.5,
    exitNotional: 0,
    matchLatencyMs: 1,
    createdAt: orderTimestampMs,
    updatedAt: orderTimestampMs
  };
}

async function testPersistPositionsBatchesIntoSingleWrite() {
  const dbCalls: Array<{ query: string; params: unknown[] }> = [];
  const markedChanges: Array<{ userId: string; positionIds: string[] }> = [];
  const store = Object.create(AppStore.prototype) as AppStore & Record<string, unknown>;
  store.positions = [];
  store.upsertPositionInMemory = (position: PositionRecord) => upsertById(store.positions, position);
  store.pruneMemoryCaches = () => undefined;
  store.bumpHistoryRevision = () => undefined;
  store.markTradePositionChanges = (userId: string, positionIds: string[]) => markedChanges.push({ userId, positionIds });
  store.runDb = async (query: string, params: unknown[]) => {
    dbCalls.push({ query, params });
  };

  await store.persistPositions([createPosition("position-1"), createPosition("position-2")]);

  assert.equal(dbCalls.length, 1);
  assert.match(dbCalls[0]!.query, /INSERT INTO positions/i);
  assert.match(dbCalls[0]!.query, /\)\s*,\s*\(/);
  assert.equal(markedChanges.length, 1);
  assert.deepEqual(markedChanges[0], { userId: "user-1", positionIds: ["position-1", "position-2"] });
  assert.deepEqual(store.positions.map((position: PositionRecord) => position.id), ["position-1", "position-2"]);
}

async function testApplyLifecycleExitBatchesUpdatedRows() {
  const dbCalls: Array<{ query: string; params: unknown[] }> = [];
  const firstLog = createLifecycle("log-1", "buy-1", 1.5, 1_700_000_000_100);
  const secondLog = createLifecycle("log-2", "buy-2", 1, 1_700_000_000_200);
  const store = Object.create(AppStore.prototype) as AppStore & Record<string, unknown>;
  store.orderLifecycleLogs = [firstLog, secondLog];
  store.upsertOrderLifecycleInMemory = (log: OrderLifecycleRecord) => upsertById(store.orderLifecycleLogs, log);
  store.pruneMemoryCaches = () => undefined;
  store.bumpHistoryRevision = () => undefined;
  store.runDb = async (query: string, params: unknown[]) => {
    dbCalls.push({ query, params });
  };

  await store.applyLifecycleExit({
    userId: "user-1",
    roundId: "round-1",
    side: "UP",
    qty: 2,
    exitType: "manual_sell",
    exitTokenPrice: 0.6,
    exitFee: 0.2
  });

  assert.equal(dbCalls.length, 1);
  assert.match(dbCalls[0]!.query, /INSERT INTO order_lifecycle_logs/i);
  assert.match(dbCalls[0]!.query, /\)\s*,\s*\(/);
  assert.equal(store.orderLifecycleLogs[0]?.remainingTokenQty, 0);
  assert.equal(store.orderLifecycleLogs[1]?.remainingTokenQty, 0.5);
  assert.equal(store.orderLifecycleLogs[0]?.exitType, "manual_sell");
  assert.equal(store.orderLifecycleLogs[1]?.exitType, "manual_sell");
}

async function main() {
  await testPersistPositionsBatchesIntoSingleWrite();
  await testApplyLifecycleExitBatchesUpdatedRows();
  console.log("[trade-persistence-batch-check] ok");
}

void main();

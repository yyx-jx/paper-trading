import assert from "node:assert/strict";
import { readSimulationServiceSource, readStoreServiceSource } from "./source-contracts";

function assertIncludes(source: string, needle: string, label: string) {
  assert.ok(source.includes(needle), `${label} missing: ${needle}`);
}

function assertInOrder(source: string, first: string, second: string, label: string) {
  const firstIndex = source.indexOf(first);
  const secondIndex = source.indexOf(second, firstIndex + first.length);
  assert.ok(firstIndex >= 0, `${label} missing first marker: ${first}`);
  assert.ok(secondIndex > firstIndex, `${label} missing second marker after first: ${second}`);
}

function testStaticTransactionContracts() {
  const storeSource = readStoreServiceSource();
  const simulationSource = readSimulationServiceSource();

  assertIncludes(storeSource, "captureTradeMutationSnapshot()", "store trade memory snapshot");
  assertIncludes(storeSource, "restoreTradeMutationSnapshot(snapshot: TradeMutationMemorySnapshot)", "store trade memory restore");
  assertIncludes(storeSource, "this.rebuildHotIndexes();", "store restore index rebuild");
  assertIncludes(storeSource, "async withTransaction<T>(handler: () => Promise<T>)", "store transaction helper");
  assertIncludes(storeSource, "prepareOrderBookSnapshotForOrder(order: OrderRecord)", "store async order book snapshot preparation");
  assertIncludes(storeSource, "private async flushOrderBookSnapshotQueue()", "store background order book snapshot flush");
  assertIncludes(
    storeSource,
    "this.upsertIndexedRecord(this.orders, this.orderIndexById, order);",
    "order hot index upsert must use generic record helper"
  );

  assertIncludes(simulationSource, "private async runTradeWriteTransaction<T>", "simulation trade write helper");
  assertIncludes(simulationSource, "type TradePersistSegments", "simulation trade persist segment type");
  assertIncludes(simulationSource, "private async measureTradePersistSegment", "simulation trade persist segment measurement");
  assertIncludes(simulationSource, "tradePersistSegments", "simulation trade persist segment logging");
  assertIncludes(simulationSource, "this.store.captureTradeMutationSnapshot()", "simulation transaction memory capture");
  assertIncludes(simulationSource, "this.store.restoreTradeMutationSnapshot(memorySnapshot)", "simulation transaction memory restore");

  assertInOrder(
    simulationSource,
    "async placeOrder(",
    "await this.runTradeWriteTransaction(async () => {",
    "placeOrder write transaction"
  );
  assertInOrder(
    simulationSource,
    "await this.runTradeWriteTransaction(async () => {",
    "this.store.prepareOrderBookSnapshotForOrder(order)",
    "order book snapshot reference preparation"
  );
  assertInOrder(
    simulationSource,
    "async cancelOrder(user: UserRecord, orderId: string)",
    "await this.runTradeWriteTransaction(async () => {",
    "cancelOrder write transaction"
  );
  assertInOrder(
    simulationSource,
    "private async failPendingOrder(user: UserRecord, order: OrderRecord, reason: string)",
    "await this.runTradeWriteTransaction(async () => {",
    "pending fail transaction"
  );
  assertInOrder(
    simulationSource,
    "private async processPendingOrders()",
    "await this.runTradeWriteTransaction(async () => {",
    "pending trigger transaction"
  );
}

async function testMemorySnapshotRestoresOrderState() {
  const { AppStore } = (await import("../apps/server/src/services/store")) as {
    AppStore: new (...args: never[]) => unknown;
  };
  const store = Object.create(AppStore.prototype) as Record<string, unknown> & {
    users: Map<string, Record<string, unknown>>;
    orders: Array<Record<string, unknown>>;
    positions: Array<Record<string, unknown>>;
    orderLifecycleLogs: Array<Record<string, unknown>>;
    orderBookSnapshots: Map<string, Record<string, unknown>>;
    logs: Array<Record<string, unknown>>;
    behaviorLogs: Array<Record<string, unknown>>;
    orderIndexById: Map<string, number>;
    positionIndexById: Map<string, number>;
    orderLifecycleIndexById: Map<string, number>;
    captureTradeMutationSnapshot: () => unknown;
    restoreTradeMutationSnapshot: (snapshot: unknown) => void;
    getOrderById: (orderId: string) => Record<string, unknown> | undefined;
    getPositionById: (positionId: string) => Record<string, unknown> | undefined;
  };

  store.users = new Map([["u1", { id: "u1", availableUsdc: 100 }]]);
  store.orders = [{ id: "ord-1", userId: "u1", status: "pending", createdAt: 1 }];
  store.positions = [{ id: "pos-1", userId: "u1", qty: 10, lockedQty: 0, openedAt: 1 }];
  store.orderLifecycleLogs = [{ id: "ol-1", orderId: "ord-1", createdAt: 1, updatedAt: 1 }];
  store.orderBookSnapshots = new Map([
    [
      "book-1",
      {
        ref: "book-1",
        snapshotId: "book-1",
        snapshotTs: 1,
        bestBid: 0.49,
        bestAsk: 0.51,
        midPrice: 0.5,
        snapshot: { snapshotId: "book-1", snapshotTs: 1, bestBid: 0.49, bestAsk: 0.51, midPrice: 0.5, bids: [], asks: [] },
        createdAt: 1
      }
    ]
  ]);
  store.logs = [{ eventId: "evt-1" }];
  store.behaviorLogs = [{ logId: "beh-1" }];
  store.orderIndexById = new Map([["ord-1", 0]]);
  store.positionIndexById = new Map([["pos-1", 0]]);
  store.orderLifecycleIndexById = new Map([["ol-1", 0]]);
  store.historyRevision = 0;

  const snapshot = store.captureTradeMutationSnapshot();
  store.users.get("u1")!.availableUsdc = 50;
  store.orders[0]!.status = "filled";
  store.orders.push({ id: "ord-new", userId: "u1", status: "failed", createdAt: 2 });
  store.positions[0]!.lockedQty = 4;
  store.positions.push({ id: "pos-new", userId: "u1", qty: 1, openedAt: 2 });
  store.orderBookSnapshots.set("book-new", {
    ref: "book-new",
    snapshot: { snapshotId: "book-new", snapshotTs: 2, bestBid: 0.4, bestAsk: 0.6, midPrice: 0.5, bids: [], asks: [] }
  });
  store.logs.unshift({ eventId: "evt-new" });
  store.behaviorLogs.unshift({ logId: "beh-new" });

  store.restoreTradeMutationSnapshot(snapshot);

  assert.equal(store.users.get("u1")?.availableUsdc, 100);
  assert.equal(store.orders.length, 1);
  assert.equal(store.orders[0]?.status, "pending");
  assert.equal(store.getOrderById("ord-new"), undefined);
  assert.equal(store.positions.length, 1);
  assert.equal(store.positions[0]?.lockedQty, 0);
  assert.equal(store.getPositionById("pos-new"), undefined);
  assert.equal(store.orderBookSnapshots.has("book-new"), false);
  assert.deepEqual(store.logs.map((log) => log.eventId), ["evt-1"]);
  assert.deepEqual(store.behaviorLogs.map((log) => log.logId), ["beh-1"]);
}

async function testOrderHotIndexUpsertDoesNotRecurse() {
  const { AppStore } = (await import("../apps/server/src/services/store")) as {
    AppStore: new (...args: never[]) => unknown;
  };
  const store = Object.create(AppStore.prototype) as Record<string, unknown> & {
    orders: Array<Record<string, unknown>>;
    orderIndexById: Map<string, number>;
    ordersByUserId: Map<string, Array<Record<string, unknown>>>;
    operatedRoundIdsByUserId: Map<string, Set<string>>;
    upsertOrderInMemory: (order: Record<string, unknown>) => void;
  };

  store.orders = [];
  store.orderIndexById = new Map();
  store.ordersByUserId = new Map();
  store.operatedRoundIdsByUserId = new Map();

  store.upsertOrderInMemory({ id: "ord-1", userId: "u1", roundId: "round-1", createdAt: 1 });

  assert.equal(store.orders.length, 1);
  assert.equal(store.orderIndexById.get("ord-1"), 0);
  assert.equal(store.ordersByUserId.get("u1")?.length, 1);
  assert.equal(store.operatedRoundIdsByUserId.get("u1")?.has("round-1"), true);
}

async function main() {
  testStaticTransactionContracts();
  await testMemorySnapshotRestoresOrderState();
  await testOrderHotIndexUpsertDoesNotRecurse();
  console.log("order-transactions-check ok");
}

void main();

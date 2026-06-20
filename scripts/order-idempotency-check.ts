import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
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

async function testMemoryLookup() {
  const { AppStore } = (await import("../apps/server/src/services/store")) as {
    AppStore: new (...args: never[]) => unknown;
  };
  const store = Object.create(AppStore.prototype) as Record<string, unknown> & {
    orders: Array<Record<string, unknown>>;
    orderIndexById: Map<string, number>;
    findOrderByClientOrderId: (userId: string, clientOrderId?: string) => Promise<Record<string, unknown> | undefined>;
  };
  store.orders = [
    { id: "ord-original", userId: "u1", clientOrderId: "client-1", status: "pending", frozenUsdc: 25, createdAt: 1 },
    { id: "ord-other-user", userId: "u2", clientOrderId: "client-1", status: "pending", frozenUsdc: 25, createdAt: 2 }
  ];
  store.orderIndexById = new Map([["ord-original", 0], ["ord-other-user", 1]]);
  store.postgresEnabled = false;
  store.pool = undefined;

  const original = await store.findOrderByClientOrderId("u1", " client-1 ");
  assert.equal(original?.id, "ord-original");
  assert.equal(original?.frozenUsdc, 25);
  assert.equal(await store.findOrderByClientOrderId("u1", undefined), undefined);
  assert.equal(await store.findOrderByClientOrderId("u1", "missing"), undefined);
}

function testStaticContracts() {
  const migration = readFileSync("db/migrations/000004_order_client_idempotency.sql", "utf8");
  const storeSource = readStoreServiceSource();
  const simulationSource = readSimulationServiceSource();
  const indexSource = readFileSync("apps/server/src/index.ts", "utf8");
  const apiSource = readFileSync("apps/client/src/utils/api.ts", "utf8");

  assertIncludes(migration, "ALTER TABLE orders ADD COLUMN IF NOT EXISTS client_order_id TEXT;", "order idempotency migration");
  assertIncludes(migration, "idx_orders_user_client_order_id", "order idempotency unique index");
  assertIncludes(storeSource, "client_order_id TEXT", "orders schema client_order_id");
  assertIncludes(storeSource, "async findOrderByClientOrderId", "store idempotency lookup");
  assertIncludes(storeSource, "order.clientOrderId ?? null", "persistOrder clientOrderId bind");
  assertIncludes(storeSource, "clientOrderId: row.client_order_id ? String(row.client_order_id) : undefined", "rowToOrder clientOrderId");
  assertIncludes(indexSource, "clientOrderId: z.string().trim().min(1).max(128).optional()", "order schema clientOrderId");
  assertIncludes(apiSource, "globalThis.crypto?.randomUUID", "client clientOrderId generation");
  assertIncludes(apiSource, "clientOrderId,", "client order request body");
  assertInOrder(
    simulationSource,
    "await this.store.findOrderByClientOrderId(user.id, clientOrderId)",
    "this.assertCanBuyOrder(currentRound, now);",
    "duplicate clientOrderId check before trading mutation"
  );
  assertIncludes(simulationSource, "isClientOrderConflict(writeError)", "unique conflict recovery");
  assertIncludes(simulationSource, "return { order: existingOrderAfterConflict };", "conflict returns original order");
}

async function main() {
  testStaticContracts();
  await testMemoryLookup();
  console.log("order-idempotency-check ok");
}

void main();

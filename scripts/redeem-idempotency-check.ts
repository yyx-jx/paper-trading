import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

type UserStub = {
  id: string;
  username: string;
  role: "Tester";
  availableUsdc: number;
};

type PositionStub = {
  id: string;
  userId: string;
  roundId: string;
  side: "UP" | "DOWN";
  qty: number;
  lockedQty?: number;
  averageEntry: number;
  notionalSpent: number;
  currentMark: number;
  currentValue?: number;
  unrealizedPnl: number;
  realizedPnl: number;
  status: "open" | "closed";
  openedAt: number;
  closedAt?: number;
  settlementResult?: "win" | "loss" | "sold";
};

type RoundStub = {
  id: string;
  marketId: string;
  marketSlug: string;
  symbol: string;
  startAt: number;
  endAt: number;
  priceToBeat: number;
  status: "Redeeming" | "Closed";
  pollCount: number;
  settledSide: "UP" | "DOWN";
  settlementTs: number;
  settlementPrice: number;
  redeemFinishTs?: number;
  redeemScheduledAt?: number;
};

function createEngineStub() {
  const { SimulationEngine } = (require("../apps/server/src/services/simulation.ts") as {
    SimulationEngine: new (...args: never[]) => unknown;
  });
  return Object.create(SimulationEngine.prototype) as Record<string, unknown>;
}

function createStoreStub(input: { user: UserStub; position: PositionStub }) {
  const ledger = new Set<string>();
  const events: string[] = [];
  const users = new Map<string, UserStub>([[input.user.id, input.user]]);
  return {
    users,
    positions: [input.position],
    events,
    async withTransaction<T>(handler: () => Promise<T>) {
      events.push("tx:begin");
      try {
        const result = await handler();
        events.push("tx:commit");
        return result;
      } catch (error) {
        events.push("tx:rollback");
        throw error;
      }
    },
    async claimRedeemLedger(claim: { roundId: string; userId: string; positionId: string }) {
      const key = `${claim.roundId}:${claim.userId}:${claim.positionId}`;
      if (ledger.has(key)) {
        events.push(`claim:duplicate:${claim.positionId}`);
        return false;
      }
      ledger.add(key);
      events.push(`claim:new:${claim.positionId}`);
      return true;
    },
    getUserById(userId: string) {
      return users.get(userId);
    },
    async persistUser() {
      events.push("persist:user");
    },
    async persistPosition() {
      events.push("persist:position");
    },
    async settleOpenOrderLifecycles() {
      events.push("settle:lifecycle");
    },
    async upsertRound() {
      events.push("persist:round");
    },
    emitUserPayload(userId: string) {
      events.push(`emit:${userId}`);
    },
    newId(prefix: string) {
      return `${prefix}-test`;
    },
    newTraceId() {
      return "trace-test";
    }
  };
}

function createRound(): RoundStub {
  const now = Date.now();
  return {
    id: "round-idempotent",
    marketId: "market-idempotent",
    marketSlug: "btc-updown-5m-idempotent",
    symbol: "BTC",
    startAt: now - 10 * 60_000,
    endAt: now - 5 * 60_000,
    priceToBeat: 100000,
    status: "Redeeming",
    pollCount: 3,
    settledSide: "UP",
    settlementTs: now - 1000,
    settlementPrice: 1,
    redeemScheduledAt: now - 500
  };
}

async function testRedeemLedgerPreventsDuplicateCredit() {
  const user: UserStub = { id: "user-1", username: "tester", role: "Tester", availableUsdc: 100 };
  const position: PositionStub = {
    id: "position-1",
    userId: user.id,
    roundId: "round-idempotent",
    side: "UP",
    qty: 10,
    averageEntry: 0.5,
    notionalSpent: 5,
    currentMark: 0.9,
    currentValue: 9,
    unrealizedPnl: 4,
    realizedPnl: 0,
    status: "open",
    openedAt: Date.now() - 10_000
  };
  const store = createStoreStub({ user, position });
  const engine = createEngineStub();
  engine.store = store;
  engine.redeemLocks = new Set<string>();
  engine.captureActionSnapshot = () => ({});
  engine.createBehaviorLog = (input: unknown) => input;
  engine.writeAuditLog = async () => store.events.push("audit");
  engine.writeBehaviorLog = async () => store.events.push("behavior");
  engine.publishSettlementMarketSnapshot = async () => store.events.push("market:publish");

  const round = createRound();
  await (engine as { applyRedeem: (target: RoundStub) => Promise<void> }).applyRedeem(round);
  assert.equal(user.availableUsdc, 110);
  assert.equal(position.status, "closed");
  assert.equal(position.settlementResult, "win");
  assert.equal(round.status, "Closed");

  round.status = "Redeeming";
  round.redeemFinishTs = undefined;
  position.status = "open";
  position.closedAt = undefined;
  position.settlementResult = undefined;
  await (engine as { applyRedeem: (target: RoundStub) => Promise<void> }).applyRedeem(round);
  assert.equal(user.availableUsdc, 110);
  assert.ok(store.events.includes("claim:duplicate:position-1"));
}

function testStaticTransactionAndMigrationWiring() {
  const migration = readFileSync("db/migrations/000003_redeem_ledger.sql", "utf8");
  assert.match(migration, /CREATE TABLE IF NOT EXISTS redeem_ledger/);
  assert.match(migration, /UNIQUE \(round_id, user_id, position_id\)/);

  const storeSource = readFileSync("apps/server/src/services/store.ts", "utf8");
  assert.match(storeSource, /async withTransaction<T>/);
  assert.match(storeSource, /txStorage\.run\(client, handler\)/);
  assert.match(storeSource, /async claimRedeemLedger/);
  assert.match(storeSource, /ON CONFLICT \(round_id, user_id, position_id\) DO NOTHING/);

  const simulationSource = readFileSync("apps/server/src/services/simulation.ts", "utf8");
  assert.match(simulationSource, /await this\.store\.withTransaction/);
  assert.match(simulationSource, /claimRedeemLedger/);
  assert.match(simulationSource, /redeemedPositionCount/);

  const configSource = readFileSync("apps/server/src/config.ts", "utf8");
  assert.match(configSource, /EXPECTED_SCHEMA_MIGRATION_ID, "000004"/);
}

async function main() {
  testStaticTransactionAndMigrationWiring();
  await testRedeemLedgerPreventsDuplicateCredit();
  console.log("redeem-idempotency-check ok");
}

void main();

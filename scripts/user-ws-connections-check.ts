import assert from "node:assert/strict";
import jwt from "jsonwebtoken";
import { createUserConnectionRegistry } from "../apps/server/src/ws/user-connection-registry";
import { normalizeClientInstanceId, createWsSessionManager } from "../apps/server/src/ws/session";
import { createUserPayloadRequest, mergeUserPayloadRequest } from "../apps/server/src/ws/user-payload-scope";
import { getClientInstanceId, normalizeStoredClientInstanceId } from "../apps/client/src/features/realtime/client-instance";
import type { UserRecord } from "../apps/server/src/domain/types";

type RegistryConnection = Parameters<ReturnType<typeof createUserConnectionRegistry>["register"]>[0];

function createTestUser(id: string, role: UserRecord["role"] = "Tester"): UserRecord {
  const now = Date.now();
  return {
    id,
    username: id,
    password: "hash",
    displayName: id,
    role,
    language: "zh-CN",
    permissionCodes: ["trade:view", "profile:view"],
    availableUsdc: 1000,
    isActive: true,
    createdAt: now,
    updatedAt: now
  };
}

function createRegistryConnection(input: {
  id: string;
  actorId: string;
  viewedUserId: string;
  clientInstanceId?: string;
  openedAt?: number;
}) {
  let closes = 0;
  const reasons: string[] = [];
  const connection: RegistryConnection & { closes(): number; closeReasons(): string[] } = {
    ...input,
    openedAt: input.openedAt ?? Date.now(),
    close(reason) {
      closes += 1;
      reasons.push(reason);
    },
    closes() {
      return closes;
    },
    closeReasons() {
      return reasons;
    }
  };
  return connection;
}

function createMemoryStorage(seed?: Record<string, string>) {
  const values = new Map(Object.entries(seed ?? {}));
  return {
    getItem(key: string) {
      return values.has(key) ? values.get(key)! : null;
    },
    setItem(key: string, value: string) {
      values.set(key, value);
    },
    removeItem(key: string) {
      values.delete(key);
    }
  };
}

async function checkRegistry() {
  const registry = createUserConnectionRegistry({ legacyLimit: 5 });
  const first = createRegistryConnection({
    id: "a",
    actorId: "actor-1",
    viewedUserId: "view-1",
    clientInstanceId: "ci_same"
  });
  const second = createRegistryConnection({
    id: "b",
    actorId: "actor-1",
    viewedUserId: "view-1",
    clientInstanceId: "ci_same"
  });
  const firstRegistration = registry.register(first);
  const secondRegistration = registry.register(second);
  assert.equal(first.closes(), 1, "same clientInstanceId replacement must close the old connection");
  assert.equal(first.closeReasons()[0], "replaced");
  assert.equal(secondRegistration.replacedCount, 1);
  assert.equal(secondRegistration.groupSize, 1);
  firstRegistration.unregister();
  firstRegistration.unregister();
  assert.equal(registry.size(), 1, "stale unregister calls must not remove the replacement connection");

  const third = createRegistryConnection({
    id: "c",
    actorId: "actor-1",
    viewedUserId: "view-1",
    clientInstanceId: "ci_other"
  });
  const thirdRegistration = registry.register(third);
  assert.equal(thirdRegistration.groupSize, 1);
  assert.equal(registry.size(), 2, "different clientInstanceId connections can coexist");

  const otherActor = createRegistryConnection({
    id: "d",
    actorId: "actor-2",
    viewedUserId: "view-1",
    clientInstanceId: "ci_same"
  });
  registry.register(otherActor);
  assert.equal(first.closes(), 1, "different actor must not be affected by same clientInstanceId");

  const legacyRegistry = createUserConnectionRegistry({ legacyLimit: 5 });
  const legacy = Array.from({ length: 6 }, (_, index) =>
    createRegistryConnection({
      id: `legacy-${index}`,
      actorId: "actor-1",
      viewedUserId: "view-1",
      openedAt: Date.now() + index
    })
  );
  const legacyResults = legacy.map((connection) => legacyRegistry.register(connection));
  assert.equal(legacy[0].closes(), 1, "legacy cap must evict oldest connection");
  assert.equal(legacy[0].closeReasons()[0], "legacy_limit");
  assert.equal(legacyResults[5].evictedCount, 1);
  assert.equal(legacyRegistry.size(), 5);
}

async function checkSession() {
  const actor = createTestUser("actor-1", "Admin");
  const viewed = createTestUser("view-1");
  const users = new Map<string, UserRecord>([
    [actor.id, actor],
    [viewed.id, viewed]
  ]);
  const manager = createWsSessionManager({
    jwtSecret: "secret",
    store: {
      getUserById: (userId) => users.get(userId),
      listUserRecords: () => [...users.values()]
    },
    ticketTtlMs: 60_000
  });
  const token = jwt.sign({ userId: actor.id }, "secret");
  assert.equal(normalizeClientInstanceId("ci_valid-123"), "ci_valid-123");
  assert.equal(normalizeClientInstanceId("ci invalid"), undefined);
  assert.equal(normalizeClientInstanceId("x"), undefined);

  const ticket = manager.createWsTicket(actor, "user", viewed.id, "ci_ticket_1");
  const ticketSession = manager.getWsSession({ ticket: ticket.ticket }, "user");
  assert.equal(ticketSession?.clientInstanceId, "ci_ticket_1", "ticket session must restore clientInstanceId");

  const fallbackSession = manager.getWsSession(
    { token, viewUserId: viewed.id, clientInstanceId: "ci_query_1" },
    "user"
  );
  assert.equal(fallbackSession?.clientInstanceId, "ci_query_1", "query fallback must read clientInstanceId");

  const invalidSession = manager.getWsSession(
    { token, viewUserId: viewed.id, clientInstanceId: "bad value with spaces" },
    "user"
  );
  assert.equal(invalidSession?.clientInstanceId, undefined, "invalid clientInstanceId must be ignored");
}

async function checkClientInstance() {
  const storage = createMemoryStorage();
  const first = getClientInstanceId(storage);
  const second = getClientInstanceId(storage);
  assert.equal(first, second, "client instance id must be stable in storage");
  assert.match(first, /^ci_[A-Za-z0-9_-]+$/);
  assert.equal(normalizeStoredClientInstanceId("bad value"), undefined);
  assert.equal(normalizeStoredClientInstanceId("ci_valid_123"), "ci_valid_123");
}

async function checkPayloadScope() {
  assert.deepEqual(
    mergeUserPayloadRequest(undefined, createUserPayloadRequest("trade", ["pos-1"])),
    { scope: "trade", positionIds: ["pos-1"] }
  );
  assert.deepEqual(
    mergeUserPayloadRequest(createUserPayloadRequest("full"), createUserPayloadRequest("trade", ["pos-1"])),
    { scope: "full" }
  );
  assert.deepEqual(
    mergeUserPayloadRequest(createUserPayloadRequest("trade", ["pos-1"]), createUserPayloadRequest("full")),
    { scope: "full" }
  );
  assert.deepEqual(
    mergeUserPayloadRequest(
      createUserPayloadRequest("trade", ["pos-1", "pos-2"]),
      createUserPayloadRequest("trade", ["pos-2", "pos-3"])
    ),
    { scope: "trade", positionIds: ["pos-1", "pos-2", "pos-3"] }
  );
}

async function checkStaticSource() {
  const [userSocket, api, userPayload] = await Promise.all([
    import("node:fs/promises").then((fs) => fs.readFile("apps/client/src/features/user/useUserSocket.ts", "utf8")),
    import("node:fs/promises").then((fs) => fs.readFile("apps/client/src/utils/api.ts", "utf8")),
    import("node:fs/promises").then((fs) => fs.readFile("apps/server/src/payloads/user.ts", "utf8"))
  ]);
  assert.match(userSocket, /getClientInstanceId\(\)/, "useUserSocket must read the stable clientInstanceId");
  assert.match(userSocket, /createWsTicket\(token,\s*"user",\s*activeViewUserId,\s*clientInstanceId\)/);
  assert.match(userSocket, /createWsUrl\("\/ws\/user",\s*token,\s*activeViewUserId,\s*clientInstanceId\)/);
  const dependencyBlock = userSocket.slice(userSocket.lastIndexOf("}, ["));
  assert.doesNotMatch(dependencyBlock, /input\.me|input\.activeViewedUser/);
  assert.match(api, /createWsUrl\(path: string, token: string, viewUserId\?: string, clientInstanceId\?: string\)/);
  assert.match(api, /createWsTicket\(token: string, channel: "market" \| "user", viewUserId\?: string, clientInstanceId\?: string\)/);
  assert.match(userPayload, /getOperatedHistoryWithSettlementPreview\(200,\s*user\.id\)/);
  const tradePayloadSource = userPayload.slice(userPayload.indexOf("const createUserTradePayload"));
  assert.doesNotMatch(tradePayloadSource, /logs:/, "trade payload must not include logs");
}

async function main() {
  await checkRegistry();
  await checkSession();
  await checkClientInstance();
  await checkPayloadScope();
  await checkStaticSource();
  console.log("[user-ws-connections-check] all checks passed");
}

void main().catch((error) => {
  console.error(error);
  process.exit(1);
});

import assert from "node:assert/strict";
import { CSV_BULK_USER_TEMPLATE, parseBulkUsersCsv, validateBulkCreateUsers } from "../apps/server/src/services/bulk-users";
import { AppStore } from "../apps/server/src/services/store";

const store = new AppStore({
  initialBalance: 1000,
  logRetentionMs: 60_000,
  snapshotRetentionSeconds: 60,
  symbol: "BTC",
  databaseUrl: "",
  redisUrl: "",
  persistenceMode: "memory",
  coinbaseEnabled: false,
  strictPersistence: false,
  requireSchemaMigrations: false,
  allowDevSchemaBootstrap: true,
  expectedSchemaMigrationId: "000007",
  pgConnectionTimeoutMs: 1000,
  pgIdleTimeoutMs: 1000,
  pgMaxConnections: 1,
  pgKeepAlive: false,
  pgReconnectIntervalMs: 1000,
  pgReconnectMaxIntervalMs: 1000,
  orderBookSnapshotsMemoryMax: 100,
  orderBookSnapshotsMemoryMaxAgeMs: 60_000,
  ordersMemoryMax: 100,
  positionsMemoryMax: 100,
  auditLogsMemoryMax: 100,
  behaviorLogsMemoryMax: 100,
  orderLifecycleMemoryMax: 100,
  roundsMemoryMax: 100,
  serverHeapWarnMb: 512,
  serverHeapProtectMb: 1024
});

async function main() {
  const senior = await store.createUser({
    username: "senior01",
    password: "pass",
    displayName: "Senior 01",
    role: "Senior Tester",
    language: "zh-CN",
    availableUsdc: 1000
  });

  const parsed = parseBulkUsersCsv(CSV_BULK_USER_TEMPLATE, {
    findManagerByUsername: (username) => {
      const user = store.findUserByUsername(username);
      return user ? store.sanitizeUser(user) : undefined;
    }
  });
  assert.equal(parsed.total, 1);
  assert.equal(parsed.failed.length, 0);
  assert.equal(parsed.users[0].managerUserId, senior.id);
  assert.equal(parsed.users[0].permissionLevel, "Standard");
  assert.equal(parsed.users[0].mustChangePassword, true);

  const valid = validateBulkCreateUsers(parsed.users, {
    initialBalance: 1000,
    usernameExists: (username) => Boolean(store.findUserByUsername(username)),
    seniorTesterExists: (userId) => store.getUserById(userId)?.role === "Senior Tester"
  });
  assert.equal(valid.failed.length, 0);

  const missingManager = parseBulkUsersCsv(
    "username 用户名,password 密码,displayName 显示名,role 角色,language 语言,managerUsername 管理者用户名,availableUsdc 可用USDC,permissionLevel 权限等级,mustChangePassword 首次登录改密\n" +
      "bob,ChangeMe123,Bob,Tester,zh-CN,missing-manager,100,Initial,false\n",
    {
      findManagerByUsername: (username) => {
        const user = store.findUserByUsername(username);
        return user ? store.sanitizeUser(user) : undefined;
      }
    }
  );
  assert.match(missingManager.failed.map((item) => item.error).join("\n"), /managerUsername was not found/);

  const tooManyRows = Array.from({ length: 101 }, (_, index) => `user${index},pass${index},User ${index},Tester,zh-CN,senior01,1,Standard,false`).join("\n");
  const tooMany = parseBulkUsersCsv(tooManyRows, {
    findManagerByUsername: (username) => {
      const user = store.findUserByUsername(username);
      return user ? store.sanitizeUser(user) : undefined;
    }
  });
  assert.match(tooMany.failed.map((item) => item.error).join("\n"), /at most 100/);

  console.log("csv-bulk-user-check ok");
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

import assert from "node:assert/strict";
import { validateBulkCreateUsers } from "../apps/server/src/services/bulk-users";
import { AppStore } from "../apps/server/src/services/store";

const store = new AppStore({
  initialBalance: 1234,
  logRetentionMs: 60_000,
  snapshotRetentionSeconds: 60,
  symbol: "BTC",
  databaseUrl: "",
  redisUrl: "",
  coinbaseEnabled: false
});

async function main() {
  const admin = await store.createUser({
    username: "bulk-admin",
    password: "admin123",
    displayName: "bulk-admin",
    role: "Admin",
    language: "zh-CN",
    availableUsdc: 1234
  });
  const engineer = await store.createUser({
    username: "bulk-engineer",
    password: "engineer123",
    displayName: "bulk-engineer",
    role: "Test Engineer",
    language: "zh-CN",
    availableUsdc: 1234
  });
  const senior = await store.createUser({
    username: "bulk-senior",
    password: "senior123",
    displayName: "bulk-senior",
    role: "Senior Tester",
    language: "zh-CN",
    availableUsdc: 1234
  });
  await store.createUser({
    username: "bulk-existing",
    password: "existing123",
    displayName: "bulk-existing",
    role: "Tester",
    language: "zh-CN",
    seniorTesterId: senior.id,
    availableUsdc: 1234
  });

  assert.equal(admin.permissionCodes.includes("users:bulk-create"), true);
  assert.equal(engineer.permissionCodes.includes("users:bulk-create"), false);
  assert.equal(senior.permissionCodes.includes("users:bulk-create"), false);

  const context = {
    initialBalance: 1234,
    usernameExists: (username: string) => Boolean(store.findUserByUsername(username)),
    seniorTesterExists: (userId: string) => store.getUserById(userId)?.role === "Senior Tester"
  };

  const valid = validateBulkCreateUsers(
    [
      { username: "bulk-new-a", password: "pass-a" },
      {
        username: "bulk-new-b",
        password: "pass-b",
        displayName: "Bulk New B",
        role: "Tester",
        language: "en-US",
        seniorTesterId: senior.id,
        availableUsdc: 88
      }
    ],
    context
  );
  assert.equal(valid.failed.length, 0);
  assert.equal(valid.normalized[0].displayName, "bulk-new-a");
  assert.equal(valid.normalized[0].role, "Tester");
  assert.equal(valid.normalized[0].language, "zh-CN");
  assert.equal(valid.normalized[0].availableUsdc, 1234);
  assert.equal(valid.normalized[1].seniorTesterId, senior.id);

  for (const item of valid.normalized) {
    await store.createUser({
      username: item.username,
      password: item.password,
      displayName: item.displayName,
      role: item.role,
      language: item.language,
      seniorTesterId: item.seniorTesterId,
      availableUsdc: item.availableUsdc
    });
  }
  assert.equal(store.findUserByUsername("bulk-new-a")?.availableUsdc, 1234);
  assert.equal(store.findUserByUsername("bulk-new-b")?.language, "en-US");

  const invalid = validateBulkCreateUsers(
    [
      { username: "", password: "missing-username" },
      { username: "bulk-missing-password", password: "" },
      { username: "bulk-existing", password: "pass" },
      { username: "bulk-dupe", password: "pass" },
      { username: "bulk-dupe", password: "pass" },
      { username: "bulk-bad-senior", password: "pass", role: "Tester", seniorTesterId: "not-a-senior" },
      { username: "bulk-bad-role-senior", password: "pass", role: "Admin", seniorTesterId: senior.id },
      { username: "bulk-negative-balance", password: "pass", availableUsdc: -1 }
    ],
    context
  );
  const errorText = invalid.failed.map((item) => item.error).join("\n");
  assert.match(errorText, /username is required/);
  assert.match(errorText, /password is required/);
  assert.match(errorText, /Username already exists/);
  assert.match(errorText, /Duplicate username/);
  assert.match(errorText, /Senior Tester/);
  assert.match(errorText, /only valid for Tester/);
  assert.match(errorText, /non-negative/);

  console.log("bulk-user-check ok");
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

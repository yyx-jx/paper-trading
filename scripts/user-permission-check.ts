import assert from "node:assert/strict";
import type { AuditEvent, BehaviorActionLog, PublicUser, Role } from "../apps/server/src/domain/types";
import { AppStore } from "../apps/server/src/services/store";

const store = new AppStore({
  initialBalance: 1000,
  logRetentionMs: 60_000,
  snapshotRetentionSeconds: 60,
  symbol: "BTC",
  databaseUrl: "",
  redisUrl: "",
  chainlinkEnabled: false
});

function canManageTarget(actor: PublicUser, target: PublicUser) {
  if (actor.role === "Admin") {
    return true;
  }
  return actor.role === "Senior Tester" && target.role === "Tester" && target.seniorTesterId === actor.id;
}

function canSetBalance(actor: PublicUser, target: PublicUser) {
  return canManageTarget(actor, target) || (actor.role === "Senior Tester" && actor.id === target.id);
}

function canCreateUsers(user: PublicUser) {
  return user.permissionCodes.includes("users:create");
}

function canBulkCreateUsers(user: PublicUser) {
  return user.permissionCodes.includes("users:bulk-create");
}

function canViewAllLogs(user: PublicUser) {
  return user.role === "Admin" || user.role === "Test Engineer" || user.permissionCodes.includes("logs:view:all");
}

function canViewTeamLogs(user: PublicUser) {
  return user.permissionCodes.includes("logs:view:team");
}

async function create(username: string, role: Role, seniorTesterId?: string) {
  return store.createUser({
    username,
    password: `${username}123`,
    displayName: username,
    role,
    language: "zh-CN",
    seniorTesterId,
    availableUsdc: 1000
  });
}

function auditEvent(userId: string, roundId: string): AuditEvent {
  return {
    eventId: store.newId("evt"),
    traceId: store.newTraceId(),
    category: "operation",
    actionType: "test.action",
    actionStatus: "success",
    userId,
    role: store.getUserById(userId)?.role,
    pageName: "test",
    moduleName: "permissions",
    roundId,
    resultCode: "OK",
    resultMessage: "ok",
    serverRecvTs: Date.now(),
    serverPublishTs: Date.now(),
    backendLatencyMs: 0,
    details: {}
  };
}

function behaviorLog(userId: string, roundId: string): BehaviorActionLog {
  return {
    logId: store.newId("beh"),
    timestampMs: Date.now(),
    assetClass: "BTC_5M_UPDOWN",
    actionType: "test.action",
    actionStatus: "success",
    roundId,
    deltaClob: 0,
    volumeClob: 0,
    testerIdAnon: store.anonymizeUserId(userId),
    binanceSpotPrice: 0,
    binance1mLastClose: 0,
    binance5mLastClose: 0,
    binance1dLastClose: 0,
    chainlinkPrice: 0,
    priceToBeat: 0,
    upPrice: 0,
    downPrice: 0,
    upBookTop5: [],
    downBookTop5: [],
    recentTradesTop20: [],
    bookSnapshotEntry: {
      snapshotId: "test",
      snapshotTs: Date.now(),
      topBids: [],
      topAsks: []
    },
    sourceStates: {
      binance: { source: "Binance", state: "disabled", sourceEventTs: 0, serverRecvTs: 0, serverPublishTs: 0 },
      chainlink: { source: "Chainlink", state: "disabled", sourceEventTs: 0, serverRecvTs: 0, serverPublishTs: 0 },
      clob: { source: "CLOB", state: "disabled", sourceEventTs: 0, serverRecvTs: 0, serverPublishTs: 0 }
    },
    contextJson: {}
  };
}

async function main() {
  const senior = await create("senior-check", "Senior Tester");
  const otherSenior = await create("other-senior-check", "Senior Tester");
  const tester = await create("tester-check", "Tester", senior.id);
  const otherTester = await create("other-tester-check", "Tester", otherSenior.id);
  const engineer = await create("engineer-check", "Test Engineer");
  const admin = await create("admin-check", "Admin");

  assert.equal(canCreateUsers(admin), true);
  assert.equal(canCreateUsers(engineer), false);
  assert.equal(canCreateUsers(senior), false);
  assert.equal(canBulkCreateUsers(admin), true);
  assert.equal(canBulkCreateUsers(engineer), false);
  assert.equal(canBulkCreateUsers(senior), false);
  assert.equal(canBulkCreateUsers(tester), false);
  assert.equal(canViewAllLogs(admin), true);
  assert.equal(canViewAllLogs(engineer), true);
  assert.equal(canViewTeamLogs(senior), true);

  assert.equal(canManageTarget(senior, tester), true);
  assert.equal(canManageTarget(senior, otherTester), false);
  assert.equal(canManageTarget(senior, admin), false);
  assert.equal(canSetBalance(senior, senior), true);
  assert.equal(canSetBalance(senior, tester), true);
  assert.equal(canSetBalance(senior, otherTester), false);

  await store.disableUser(tester.id, senior.id);
  assert.equal(store.findUserByCredentials(tester.username, "tester-check123"), undefined);
  await store.enableUser(tester.id);
  assert.equal(store.findUserByCredentials(tester.username, "tester-check123")?.id, tester.id);

  await store.resetUserPassword(otherTester.id, "next-password");
  assert.equal(store.findUserByCredentials(otherTester.username, "next-password")?.id, otherTester.id);

  await store.setUserBalance(senior.id, 2345.67);
  assert.equal(store.getUserById(senior.id)?.availableUsdc, 2345.67);

  for (const event of [
    auditEvent(senior.id, "round-a"),
    auditEvent(tester.id, "round-a"),
    auditEvent(otherTester.id, "round-a"),
    auditEvent(admin.id, "round-b")
  ]) {
    store.logs.unshift(event);
  }
  for (const log of [
    behaviorLog(senior.id, "round-a"),
    behaviorLog(tester.id, "round-a"),
    behaviorLog(otherTester.id, "round-a"),
    behaviorLog(admin.id, "round-b")
  ]) {
    store.behaviorLogs.unshift(log);
  }

  const teamIds = [senior.id, tester.id];
  const teamAudit = store.getAuditLogs({ userIds: teamIds, roundId: "round-a" });
  assert.deepEqual(new Set(teamAudit.map((event) => event.userId)), new Set(teamIds));

  const teamBehavior = store.getBehaviorLogs({ userIds: teamIds, roundId: "round-a" });
  assert.deepEqual(new Set(teamBehavior.map((log) => log.testerIdAnon)), new Set(teamIds.map((id) => store.anonymizeUserId(id))));

  const allAudit = store.getAuditLogs({ roundId: "round-a" });
  assert.equal(allAudit.length, 3);

  const engineerVisibleAudit = store.getAuditLogs({ roundId: "round-a" });
  assert.deepEqual(new Set(engineerVisibleAudit.map((event) => event.userId)), new Set([senior.id, tester.id, otherTester.id]));
  assert.equal(canViewAllLogs(tester), false);

  console.log("user-permission-check ok");
}

void main();

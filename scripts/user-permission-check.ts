import assert from "node:assert/strict";
import type { AuditEvent, BehaviorActionLog, PublicUser, Role } from "../apps/server/src/domain/types";
import { canChangeUserGroupForActor, canCreateUserForActor, canExportUser, canManageUser, getVisibleUserIdsForActor } from "../apps/server/src/auth/scope";
import { ROLE_PERMISSIONS } from "../apps/server/src/auth/permissions";
import { AppStore } from "../apps/server/src/services/store";

const store = new AppStore({
  initialBalance: 1000,
  logRetentionMs: 60_000,
  snapshotRetentionSeconds: 60,
  symbol: "BTC",
  databaseUrl: "",
  redisUrl: "",
  coinbaseEnabled: false
});

function canManageTarget(actor: PublicUser, target: PublicUser) {
  if (actor.role === "Admin") {
    return true;
  }
  return (
    (actor.role === "Senior Tester" || actor.role === "Test Engineer") &&
    target.role === "Tester" &&
    (target.managerUserId ?? target.seniorTesterId) === actor.id
  );
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

function canUpdateUsers(user: PublicUser) {
  return user.permissionCodes.includes("users:update");
}

function canViewAllLogs(user: PublicUser) {
  return user.role === "Admin";
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
    coinbasePrice: 0,
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
      coinbase: { source: "Coinbase", state: "disabled", sourceEventTs: 0, serverRecvTs: 0, serverPublishTs: 0 },
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
  const engineerTester = await create("engineer-tester-check", "Tester", engineer.id);
  const admin = await create("admin-check", "Admin");
  const allUsers = store.listUserRecords();

  assert.equal(canCreateUsers(admin), true);
  assert.equal(canCreateUsers(engineer), true);
  assert.equal(canCreateUsers(senior), true);
  assert.equal(canCreateUserForActor(admin, "Admin"), true);
  assert.equal(canCreateUserForActor(admin, "Tester"), true);
  assert.equal(canCreateUserForActor(senior, "Tester"), true);
  assert.equal(canCreateUserForActor(engineer, "Tester"), true);
  assert.equal(canCreateUserForActor(senior, "Senior Tester"), false);
  assert.equal(canCreateUserForActor(engineer, "Test Engineer"), false);
  assert.equal(canCreateUserForActor(tester, "Tester"), false);
  assert.equal(canChangeUserGroupForActor(admin, tester), true);
  assert.equal(canChangeUserGroupForActor(admin, senior), false);
  assert.equal(canChangeUserGroupForActor(senior, tester), false);
  assert.equal(canChangeUserGroupForActor(engineer, engineerTester), false);
  assert.equal(canChangeUserGroupForActor(tester, tester), false);
  assert.equal(ROLE_PERMISSIONS["Test Engineer"].includes("users:manager:update"), false);
  assert.equal(canUpdateUsers(senior), true);
  assert.equal(canUpdateUsers(engineer), true);
  assert.equal(canBulkCreateUsers(admin), true);
  assert.equal(canBulkCreateUsers(engineer), false);
  assert.equal(canBulkCreateUsers(senior), false);
  assert.equal(canBulkCreateUsers(tester), false);
  assert.equal(canViewAllLogs(admin), true);
  assert.equal(canViewAllLogs(engineer), false);
  assert.equal(canViewTeamLogs(senior), true);
  assert.equal(canViewTeamLogs(engineer), true);

  assert.deepEqual(new Set(getVisibleUserIdsForActor(admin, allUsers)), new Set(allUsers.map((user) => user.id)));
  assert.deepEqual(new Set(getVisibleUserIdsForActor(senior, allUsers)), new Set([senior.id, tester.id]));
  assert.deepEqual(new Set(getVisibleUserIdsForActor(engineer, allUsers)), new Set([engineer.id, engineerTester.id]));
  assert.deepEqual(getVisibleUserIdsForActor(tester, allUsers), [tester.id]);

  assert.equal(canManageTarget(senior, tester), true);
  assert.equal(canManageTarget(senior, otherTester), false);
  assert.equal(canManageTarget(senior, admin), false);
  assert.equal(canManageTarget(engineer, engineerTester), true);
  assert.equal(canManageTarget(engineer, tester), false);
  assert.equal(canManageTarget(engineer, admin), false);
  assert.equal(canSetBalance(senior, senior), true);
  assert.equal(canSetBalance(senior, tester), true);
  assert.equal(canSetBalance(senior, otherTester), false);
  assert.equal(canManageUser(engineer, engineerTester, allUsers), true);
  assert.equal(canManageUser(engineer, senior, allUsers), false);
  assert.equal(canExportUser(engineer, engineerTester.id, allUsers), true);
  assert.equal(canExportUser(engineer, admin.id, allUsers), false);

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
    auditEvent(engineer.id, "round-a"),
    auditEvent(engineerTester.id, "round-a"),
    auditEvent(admin.id, "round-b")
  ]) {
    store.logs.unshift(event);
  }
  for (const log of [
    behaviorLog(senior.id, "round-a"),
    behaviorLog(tester.id, "round-a"),
    behaviorLog(otherTester.id, "round-a"),
    behaviorLog(engineer.id, "round-a"),
    behaviorLog(engineerTester.id, "round-a"),
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
  assert.equal(allAudit.length, 5);

  const engineerTeamIds = [engineer.id, engineerTester.id];
  const engineerTeamAudit = store.getAuditLogs({ userIds: engineerTeamIds, roundId: "round-a" });
  assert.deepEqual(new Set(engineerTeamAudit.map((event) => event.userId)), new Set(engineerTeamIds));
  assert.equal(canViewAllLogs(tester), false);

  console.log("user-permission-check ok");
}

void main();

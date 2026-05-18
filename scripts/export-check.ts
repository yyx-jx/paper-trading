import assert from "node:assert/strict";
import fs from "node:fs";
import type { AuditEvent, MatchingEventRecord, OrderLifecycleRecord, PositionRecord, PublicUser, Role, UserRecord } from "../apps/server/src/domain/types";
import {
  buildExportEntries,
  createZipArchive,
  filterOrdersForExport,
  filterPositionsForExport,
  resolveExportUsers,
  toCsv,
  type ExportUser,
  type UserExportData
} from "../apps/server/src/services/csv-zip-export";
import { AppStore } from "../apps/server/src/services/store";

function user(id: string, username: string, role: Role, seniorTesterId?: string): ExportUser {
  return {
    id,
    username,
    displayName: username,
    role,
    language: "zh-CN",
    permissionCodes:
      role === "Admin"
        ? ["logs:view:all"]
        : role === "Test Engineer"
          ? ["logs:view:all"]
          : role === "Senior Tester"
            ? ["logs:view:team"]
            : [],
    availableUsdc: 1000,
    isActive: true,
    seniorTesterId,
    createdAt: 1,
    updatedAt: 1,
    anonId: `anon-${id}`
  };
}

function actorFromPublic(publicUser: PublicUser): UserRecord {
  return {
    ...publicUser,
    password: "hidden",
    permissionCodes: publicUser.permissionCodes as never
  };
}

function unzipStoredEntries(zip: Buffer) {
  const entries = new Map<string, string>();
  let offset = 0;
  while (offset + 30 <= zip.length) {
    const signature = zip.readUInt32LE(offset);
    if (signature !== 0x04034b50) {
      break;
    }
    const method = zip.readUInt16LE(offset + 8);
    const compressedSize = zip.readUInt32LE(offset + 18);
    const nameLength = zip.readUInt16LE(offset + 26);
    const extraLength = zip.readUInt16LE(offset + 28);
    const nameStart = offset + 30;
    const name = zip.subarray(nameStart, nameStart + nameLength).toString("utf8");
    const dataStart = nameStart + nameLength + extraLength;
    assert.equal(method, 0);
    entries.set(name, zip.subarray(dataStart, dataStart + compressedSize).toString("utf8"));
    offset = dataStart + compressedSize;
  }
  return entries;
}

function audit(actionType: string, userId?: string): AuditEvent {
  return {
    eventId: `evt-${actionType}`,
    traceId: `trace-${actionType}`,
    category: actionType === "market_latency" ? "latency" : "operation",
    actionType,
    actionStatus: "success",
    userId,
    role: "Tester",
    pageName: "test",
    moduleName: "export",
    resultCode: "OK",
    resultMessage: `message, with "quotes"`,
    serverRecvTs: 1_700_000_000_000,
    serverPublishTs: 1_700_000_000_010,
    backendLatencyMs: 10,
    details: { nested: ["a", "b"] }
  };
}

function exportData(exportUser: ExportUser): UserExportData {
  return {
    user: exportUser,
    auditLogs: [audit("user_action", exportUser.id)],
    trainingLogs: [],
    matchingEvents: [],
    orders: [],
    positions: [],
    operatedRounds: [],
    profile: {
      totalEquity: 1000,
      availableUsdc: 1000,
      positionValue: 0,
      realizedPnlToday: 0,
      unrealizedPnl: 0,
      winRate: 0,
      roundsParticipatedTotal: 0,
      roundsParticipatedToday: 0
    }
  };
}

function matchingEvent(userId?: string): MatchingEventRecord {
  return {
    eventId: `mevt-${userId ?? "system"}`,
    bookKey: "book-a",
    roundId: "round-a",
    marketId: "market-a",
    bookSide: "UP",
    sequence: 12,
    eventType: userId ? "order_executed" : "external_book_synced",
    orderId: userId ? "order-1" : undefined,
    traceId: userId ? "trace-order-1" : undefined,
    payload: userId ? { request: { userId }, status: "filled" } : { sourceSnapshotId: "snap-a" },
    createdAt: 1_700_000_000_050
  };
}

const senior = user("u-senior", "senior", "Senior Tester");
const tester = user("u-tester", "tester", "Tester", senior.id);
const otherTester = user("u-other", "other", "Tester", "u-other-senior");
const engineer = user("u-engineer", "engineer", "Test Engineer");
const engineerTester = user("u-engineer-tester", "engineer-tester", "Tester", engineer.id);
const admin = user("u-admin", "admin", "Admin");
const users = [senior, tester, otherTester, engineer, engineerTester, admin];

assert.deepEqual(resolveExportUsers(actorFromPublic(admin), users).map((item) => item.id), users.map((item) => item.id));
assert.deepEqual(new Set(resolveExportUsers(actorFromPublic(engineer), users).map((item) => item.id)), new Set([engineer.id, engineerTester.id]));
assert.deepEqual(new Set(resolveExportUsers(actorFromPublic(senior), users).map((item) => item.id)), new Set([senior.id, tester.id]));
assert.deepEqual(resolveExportUsers(actorFromPublic(tester), users).map((item) => item.id), [tester.id]);
assert.throws(() => resolveExportUsers(actorFromPublic(senior), users, otherTester.id), /not available/);
assert.throws(() => resolveExportUsers(actorFromPublic(engineer), users, admin.id), /not available/);
assert.throws(() => resolveExportUsers(actorFromPublic(engineer), users, tester.id), /not available/);
assert.deepEqual(resolveExportUsers(actorFromPublic(admin), users, tester.id).map((item) => item.id), [tester.id]);

const escaped = toCsv([{ value: 'comma, quote " and\nnewline' }], [{ header: "value", value: (row) => row.value }]);
assert.match(escaped, /"comma, quote "" and\nnewline"/);

const order = {
  id: "lifecycle-1",
  buyOrderId: "order-1",
  userId: tester.id,
  testerId: tester.id,
  roundId: "round-a",
  traceId: "trace-order-1",
  symbol: "BTC",
  assetClass: "BTC",
  marketId: "market-a",
  direction: "UP",
  orderTimestampMs: 1_700_000_000_000,
  entryTokenPrice: 0.5,
  btcTradePrice: 80_010,
  btcOpenPriceToBeat: 80_000,
  deltaBtc: 10,
  volumeTokenQty: 20,
  remainingTokenQty: 20,
  closedTokenQty: 0,
  positionNotional: 10,
  exitNotional: 0,
  orderBookSnapshotRef: "obs-test",
  orderBookSnapshot: {
    snapshotId: "book-a",
    snapshotTs: 1_700_000_000_000,
    bestBid: 0.49,
    bestAsk: 0.5,
    midPrice: 0.495,
    bids: [{ price: 0.49, qty: 10 }],
    asks: [{ price: 0.5, qty: 20 }]
  },
  actualFillPrice: 0.5,
  slippageBps: 10,
  matchLatencyMs: 3,
  createdAt: 1_700_000_000_000,
  updatedAt: 1_700_000_000_000
} as OrderLifecycleRecord;
assert.equal(filterOrdersForExport([order], { userId: tester.id, roundId: "round-a" }).length, 1);
assert.equal(filterOrdersForExport([order], { userId: tester.id, roundId: "round-b" }).length, 0);
assert.equal(filterOrdersForExport([order], { userId: tester.id, orderId: "order-1" }).length, 1);

const position = {
  id: "position-1",
  userId: tester.id,
  roundId: "round-a",
  openedAt: 1_700_000_000_000
} as PositionRecord;
assert.equal(filterPositionsForExport([position], { userId: tester.id, roundId: "round-a" }).length, 1);
assert.equal(filterPositionsForExport([position], { userId: tester.id, positionId: "missing" }).length, 0);

const multiEntries = buildExportEntries({
  actor: admin,
  users: [exportData(tester), exportData(otherTester)],
  systemMatchingEvents: [matchingEvent()],
  systemLatencyLogs: [audit("market_latency")],
  query: { system: "all", roundId: "round-a" },
  generatedAt: 1_700_000_000_123,
  dateLabel: "2026-04-27",
  singleUser: false
});
const multiZipText = createZipArchive(multiEntries).toString("utf8");
assert.match(multiZipText, /manifest\.json/);
assert.match(multiZipText, /"roundId": "round-a"/);
assert.match(multiZipText, /export-2026-04-27\/users\/tester\/audit_logs\.csv/);
assert.match(multiZipText, /export-2026-04-27\/users\/tester\/matching_events\.csv/);
assert.match(multiZipText, /export-2026-04-27\/users\/other\/profile\.csv/);
assert.match(multiZipText, /export-2026-04-27\/system\/latency\.csv/);
assert.match(multiZipText, /export-2026-04-27\/system\/matching_events\.csv/);
assert.match(multiZipText, /market_latency/);

const postStyleEntries = buildExportEntries({
  actor: admin,
  users: [
    { ...exportData(tester), matchingEvents: [matchingEvent(tester.id)] },
    exportData(otherTester)
  ],
  systemMatchingEvents: [],
  systemLatencyLogs: [],
  query: {
    system: "all",
    systems: ["audit", "matching"],
    userIds: [tester.id, otherTester.id],
    from: 1_700_000_000_000,
    to: 1_700_000_001_000,
    roundId: "round-a",
    eventType: "order_executed"
  },
  generatedAt: 1_700_000_000_123,
  dateLabel: "2026-04-27",
  singleUser: false
});
const postStyleZip = createZipArchive(postStyleEntries);
const postStyleFiles = unzipStoredEntries(postStyleZip);
const manifestPath = [...postStyleFiles.keys()].find((entry) => entry.endsWith("/manifest.json"));
assert.ok(manifestPath);
const manifest = JSON.parse(postStyleFiles.get(manifestPath)!);
assert.deepEqual(manifest.query.systems, ["audit", "matching"]);
assert.deepEqual(manifest.query.userIds, [tester.id, otherTester.id]);
assert.equal(manifest.users.length, 2);
assert.ok(manifest.files.some((file: { path: string; rowCount: number }) => file.path.endsWith("/audit_logs.csv") && file.rowCount === 1));
assert.ok(postStyleFiles.get("export-2026-04-27/users/tester/matching_events.csv")?.includes("order_executed"));

const exportDir = "D:/测试数据";
fs.mkdirSync(exportDir, { recursive: true });
const exportPath = `${exportDir}/paper-trading-export-test.zip`;
fs.writeFileSync(exportPath, postStyleZip);
assert.equal(fs.statSync(exportPath).size, postStyleZip.length);

const singleEntries = buildExportEntries({
  actor: admin,
  users: [exportData(tester)],
  systemMatchingEvents: [matchingEvent()],
  systemLatencyLogs: [audit("market_latency")],
  query: { system: "all" },
  generatedAt: 1_700_000_000_123,
  dateLabel: "2026-04-27",
  singleUser: true
});
const singleZipText = createZipArchive(singleEntries).toString("utf8");
assert.match(singleZipText, /export-2026-04-27\/tester\/audit_logs\.csv/);
assert.doesNotMatch(singleZipText, /system\/latency\.csv/);
assert.doesNotMatch(singleZipText, /users\/tester/);

const ordersCsvText = buildExportEntries({
  actor: admin,
  users: [{ ...exportData(tester), matchingEvents: [matchingEvent(tester.id)], orders: [order] }],
  systemMatchingEvents: [],
  systemLatencyLogs: [],
  query: { system: "all" },
  generatedAt: 1_700_000_000_123,
  dateLabel: "2026-04-27",
  singleUser: true
}).find((entry) => entry.path.endsWith("/orders.csv"))!.content.toString();
assert.match(ordersCsvText, /order_timestamp_ms/);
assert.match(ordersCsvText, /order_book_snapshot_json/);
assert.match(ordersCsvText, /book-a/);

async function lifecycleStoreChecks() {
  const lifecycleStore = new AppStore({
    initialBalance: 1000,
    logRetentionMs: 60_000,
    snapshotRetentionSeconds: 60,
    symbol: "BTC",
    databaseUrl: "",
    redisUrl: "",
    chainlinkEnabled: false
  });
  const base = {
    traceId: "trace-life",
    userId: tester.id,
    testerId: tester.id,
    roundId: "round-life",
    symbol: "BTC",
    assetClass: "BTC" as const,
    marketId: "market-life",
    direction: "UP" as const,
    orderTimestampMs: 1_700_000_000_000,
    entryTokenPrice: 0.5,
    btcTradePrice: 80_010,
    btcOpenPriceToBeat: 80_000,
    deltaBtc: 10,
    positionNotional: 5,
    exitNotional: 0,
    actualFillPrice: 0.5,
    slippageBps: 10,
    matchLatencyMs: 2,
    createdAt: 1_700_000_000_000,
    updatedAt: 1_700_000_000_000
  };
  await lifecycleStore.persistOrderLifecycle({
    ...base,
    id: "life-a",
    buyOrderId: "buy-a",
    volumeTokenQty: 10,
    remainingTokenQty: 10,
    closedTokenQty: 0
  });
  await lifecycleStore.persistOrderLifecycle({
    ...base,
    id: "life-b",
    buyOrderId: "buy-b",
    orderTimestampMs: 1_700_000_000_010,
    volumeTokenQty: 8,
    remainingTokenQty: 8,
    closedTokenQty: 0
  });
  await lifecycleStore.applyLifecycleExit({
    userId: tester.id,
    roundId: "round-life",
    side: "UP",
    qty: 12,
    exitType: "manual_sell",
    exitTokenPrice: 0.7
  });
  const first = lifecycleStore.orderLifecycleLogs.find((item) => item.id === "life-a");
  const second = lifecycleStore.orderLifecycleLogs.find((item) => item.id === "life-b");
  assert.equal(first?.remainingTokenQty, 0);
  assert.equal(first?.exitType, "manual_sell");
  assert.equal(first?.exitTokenPrice, 0.7);
  assert.equal(second?.remainingTokenQty, 6);
  assert.equal(second?.closedTokenQty, 2);

  await lifecycleStore.settleOpenOrderLifecycles({
    userId: tester.id,
    roundId: "round-life",
    side: "UP",
    settlementResult: "win",
    settlementDirection: "UP",
    settlementTimeMs: 1_700_000_001_000,
    exitTokenPrice: 1
  });
  assert.equal(second?.remainingTokenQty, 0);
  assert.equal(second?.exitType, "mixed");
  assert.equal(second?.settlementResult, "win");
  assert.equal(second?.settlementDirection, "UP");
}

void lifecycleStoreChecks()
  .then(() => {
    console.log("export-check ok");
  })
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });

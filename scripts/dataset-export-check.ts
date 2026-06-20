import assert from "node:assert/strict";
import { buildDatasetExport, previewDatasetExport } from "../apps/server/src/services/dataset-export";
import type { BehaviorActionLog } from "../apps/server/src/domain/types";

const baseLog: BehaviorActionLog = {
  logId: "log_1",
  timestampMs: 1_700_000_000_000,
  assetClass: "BTC_5M_UPDOWN",
  actionType: "place_order",
  actionStatus: "success",
  roundId: "round_1",
  direction: "UP",
  deltaClob: 0,
  volumeClob: 0,
  positionNotional: 10,
  testerIdAnon: "real-user-should-not-export",
  traceId: "trace_real",
  orderId: "order_real",
  binanceSpotPrice: 1,
  binance1mLastClose: 1,
  binance5mLastClose: 1,
  binance1dLastClose: 1,
  coinbasePrice: 1,
  priceToBeat: 1,
  upPrice: 0.5,
  downPrice: 0.5,
  upBookTop5: [],
  downBookTop5: [],
  recentTradesTop20: [],
  bookSnapshotEntry: {
    snapshotId: "snap",
    snapshotTs: 1_700_000_000_000,
    topBids: [],
    topAsks: []
  },
  sourceStates: {
    binance: "healthy",
    coinbase: "disabled",
    clob: "healthy"
  },
  qualityGrade: "A"
};

const logs: BehaviorActionLog[] = [
  baseLog,
  { ...baseLog, logId: "log_2", orderId: "order_d", traceId: "trace_d", qualityGrade: "D" },
  { ...baseLog, logId: "log_3", orderId: "order_missing", traceId: "trace_missing", qualityGrade: undefined }
];

const preview = previewDatasetExport(logs, false);
assert.equal(preview.recordCount, 2);
assert.equal(preview.filteredDGradeCount, 1);
assert.equal(preview.missingQualityCount, 1);

const generated = buildDatasetExport({
  exportId: "export_1",
  actor: { id: "admin_1", role: "Admin" },
  request: { includeDGrade: false },
  userIds: ["user_1"],
  logs,
  anonymizationSecret: "secret"
});

assert.equal(generated.preview.recordCount, 2);
assert.equal(generated.manifest.files.some((file) => file.path.endsWith("customer_dataset.csv")), true);
assert.equal(generated.archive.length > 0, true);
assert.equal(generated.sha256.length, 64);

const archiveText = generated.archive.toString("latin1");
assert.equal(archiveText.includes("order_real"), false);
assert.equal(archiveText.includes("trace_real"), false);
assert.equal(archiveText.includes("real-user-should-not-export"), false);

console.log("dataset-export-check ok");

import { createHash, createHmac } from "node:crypto";
import type {
  BehaviorActionLog,
  DatasetExportAuditEvent,
  DatasetExportManifest,
  DatasetExportPreview,
  DatasetExportRequest,
  Role
} from "../domain/types";
import { createZipArchive, toCsv, type CsvColumn, type ZipEntry } from "./csv-zip-export";

export interface DatasetExportBuildInput {
  exportId: string;
  actor: { id: string; role: Role };
  request: DatasetExportRequest;
  userIds: string[];
  logs: BehaviorActionLog[];
  anonymizationSecret: string;
  generatedAt?: number;
}

export interface DatasetExportBuildResult {
  preview: DatasetExportPreview;
  manifest: DatasetExportManifest;
  audit: DatasetExportAuditEvent;
  archive: Buffer;
  sha256: string;
}

type DatasetRow = {
  anon_user_id: string;
  anon_order_id: string;
  anon_trace_id: string;
  timestamp_ms: number;
  timestamp_iso: string;
  action_type: string;
  action_status: string;
  asset_class: string;
  round_id: string;
  direction: string;
  entry_odds: number | "";
  exit_odds: number | "";
  position_notional: number | "";
  settlement_result: string;
  estimated_fee: number | "";
  actual_fee: number | "";
  quality_grade: string;
  market_regime_label: string;
  strategy_cluster_label: string;
};

function sha256(value: string | Buffer) {
  return createHash("sha256").update(value).digest("hex");
}

function anon(secret: string, namespace: string, value?: string) {
  if (!value) {
    return "";
  }
  return createHmac("sha256", secret).update(`${namespace}:${value}`).digest("hex").slice(0, 24);
}

function iso(ts: number) {
  return new Date(ts).toISOString();
}

function rowFromLog(secret: string, log: BehaviorActionLog): DatasetRow {
  return {
    anon_user_id: anon(secret, "user", log.testerIdAnon || log.logId),
    anon_order_id: anon(secret, "order", log.orderId),
    anon_trace_id: anon(secret, "trace", log.traceId),
    timestamp_ms: log.timestampMs,
    timestamp_iso: iso(log.timestampMs),
    action_type: log.actionType,
    action_status: log.actionStatus,
    asset_class: log.assetClass,
    round_id: log.roundId ?? "",
    direction: log.direction ?? "",
    entry_odds: log.entryOdds ?? "",
    exit_odds: log.exitOdds ?? "",
    position_notional: log.positionNotional ?? "",
    settlement_result: log.settlementResult ?? "",
    estimated_fee: log.estimatedFee ?? "",
    actual_fee: log.actualFee ?? "",
    quality_grade: log.qualityGrade ?? "",
    market_regime_label: log.marketRegimeLabel ?? "",
    strategy_cluster_label: log.strategyClusterLabel ?? ""
  };
}

const DATASET_COLUMNS: CsvColumn<DatasetRow>[] = [
  { header: "anon_user_id", value: (row) => row.anon_user_id },
  { header: "anon_order_id", value: (row) => row.anon_order_id },
  { header: "anon_trace_id", value: (row) => row.anon_trace_id },
  { header: "timestamp_ms", value: (row) => row.timestamp_ms },
  { header: "timestamp_iso", value: (row) => row.timestamp_iso },
  { header: "action_type", value: (row) => row.action_type },
  { header: "action_status", value: (row) => row.action_status },
  { header: "asset_class", value: (row) => row.asset_class },
  { header: "round_id", value: (row) => row.round_id },
  { header: "direction", value: (row) => row.direction },
  { header: "entry_odds", value: (row) => row.entry_odds },
  { header: "exit_odds", value: (row) => row.exit_odds },
  { header: "position_notional", value: (row) => row.position_notional },
  { header: "settlement_result", value: (row) => row.settlement_result },
  { header: "estimated_fee", value: (row) => row.estimated_fee },
  { header: "actual_fee", value: (row) => row.actual_fee },
  { header: "quality_grade", value: (row) => row.quality_grade },
  { header: "market_regime_label", value: (row) => row.market_regime_label },
  { header: "strategy_cluster_label", value: (row) => row.strategy_cluster_label }
];

function schemaJson() {
  return `${JSON.stringify(
    {
      name: "customer_dataset",
      version: 1,
      formats: ["csv", "jsonl"],
      pii_policy: "No real user_id, username, displayName, order id, or trace id is exported.",
      columns: DATASET_COLUMNS.map((column) => column.header)
    },
    null,
    2
  )}\n`;
}

export function previewDatasetExport(logs: BehaviorActionLog[], includeDGrade = false): DatasetExportPreview {
  const filteredDGradeCount = includeDGrade ? 0 : logs.filter((log) => log.qualityGrade === "D").length;
  const missingQualityCount = logs.filter((log) => !log.qualityGrade).length;
  const rows = includeDGrade ? logs : logs.filter((log) => log.qualityGrade !== "D");
  return {
    recordCount: rows.length,
    filteredDGradeCount,
    missingQualityCount,
    userCount: new Set(logs.map((log) => log.testerIdAnon)).size,
    formats: ["csv", "jsonl"]
  };
}

export function buildDatasetExport(input: DatasetExportBuildInput): DatasetExportBuildResult {
  const generatedAt = input.generatedAt ?? Date.now();
  const filteredLogs = input.request.includeDGrade ? input.logs : input.logs.filter((log) => log.qualityGrade !== "D");
  const rows = filteredLogs.map((log) => rowFromLog(input.anonymizationSecret, log));
  const preview = previewDatasetExport(input.logs, input.request.includeDGrade);
  const csv = toCsv(rows, DATASET_COLUMNS);
  const jsonl = rows.map((row) => JSON.stringify(row)).join("\n") + (rows.length ? "\n" : "");
  const schema = schemaJson();
  const root = `dataset-export-${new Date(generatedAt).toISOString().slice(0, 10)}`;
  const scope = {
    userIds: input.userIds.map((userId) => anon(input.anonymizationSecret, "user", userId)),
    includeDGrade: Boolean(input.request.includeDGrade),
    from: input.request.from,
    to: input.request.to
  };
  const manifest: DatasetExportManifest = {
    exportId: input.exportId,
    generatedAt,
    generatedAtIso: iso(generatedAt),
    formats: ["csv", "jsonl"],
    recordCount: rows.length,
    filteredDGradeCount: preview.filteredDGradeCount,
    missingQualityCount: preview.missingQualityCount,
    userCount: input.userIds.length,
    scope,
    files: [
      { path: `${root}/customer_dataset.csv`, sha256: sha256(csv), rowCount: rows.length },
      { path: `${root}/customer_dataset.jsonl`, sha256: sha256(jsonl), rowCount: rows.length },
      { path: `${root}/schema.json`, sha256: sha256(schema) }
    ]
  };
  const audit: DatasetExportAuditEvent = {
    exportId: input.exportId,
    actorUserId: input.actor.id,
    actorRole: input.actor.role,
    scope,
    formats: ["csv", "jsonl"],
    recordCount: rows.length,
    filteredDGradeCount: preview.filteredDGradeCount,
    missingQualityCount: preview.missingQualityCount,
    sha256: sha256(`${csv}\n${jsonl}`),
    createdAtMs: generatedAt
  };
  const auditJson = `${JSON.stringify(audit, null, 2)}\n`;
  const entries: ZipEntry[] = [
    { path: `${root}/manifest.json`, content: `${JSON.stringify(manifest, null, 2)}\n` },
    { path: `${root}/schema.json`, content: schema },
    { path: `${root}/customer_dataset.csv`, content: csv },
    { path: `${root}/customer_dataset.jsonl`, content: jsonl },
    { path: `${root}/export_audit.json`, content: auditJson }
  ];
  const archive = createZipArchive(entries);
  const archiveSha = sha256(archive);
  audit.sha256 = archiveSha;
  return {
    preview,
    manifest,
    audit,
    archive,
    sha256: archiveSha
  };
}

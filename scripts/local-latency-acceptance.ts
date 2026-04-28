import { appendFileSync, existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";

type Role = "Tester" | "Senior Tester" | "Test Engineer" | "Admin";
type Side = "UP" | "DOWN";

type Actor = {
  label: string;
  username: string;
  password: string;
  userId: string;
  role: Role;
  token: string;
};

type RequestRecord = {
  ts: number;
  iso: string;
  actor: string;
  userId?: string;
  role?: Role;
  method: string;
  route: string;
  status: number;
  ok: boolean;
  expectedFailure: boolean;
  latencyMs: number;
  sizeBytes: number;
  error?: string;
};

type OrderRecord = {
  id: string;
  action: "buy" | "sell";
  side: Side;
  status: "pending" | "filled" | "failed" | "cancelled";
  resultType?: string;
  filledQty: number;
  avgFillPrice?: number;
  matchLatencyMs: number;
  bookAcquireLatencyMs?: number;
  localMatchLatencyMs?: number;
  persistLatencyMs?: number;
  totalOrderLatencyMs?: number;
  failureReason?: string;
  orderBookSnapshot?: unknown;
};

type PositionRecord = {
  id: string;
  side: Side;
  status: "open" | "closed";
  qty: number;
};

type ApiResult<T> = {
  data?: T;
  status: number;
  ok: boolean;
  latencyMs: number;
  sizeBytes: number;
  error?: string;
};

const baseUrl = process.env.ACCEPTANCE_BASE_URL ?? "http://127.0.0.1:8787";
const stamp = new Date().toISOString().replace(/[-:]/g, "").replace("T", "-").slice(0, 15);
const artifactDir = path.resolve(`D:\\测试数据\\latency-optimization-${stamp}`);
const requestsPath = path.join(artifactDir, "requests.jsonl");
const exportsDir = path.join(artifactDir, "exports");
const unzippedDir = path.join(artifactDir, "exports-unzipped");

const requestRecords: RequestRecord[] = [];
const successfulBuyOrderIds = new Set<string>();
const cancelledOrderIds = new Set<string>();
const localMatchLatencies: number[] = [];
const bookAcquireLatencies: number[] = [];
const persistLatencies: number[] = [];
const totalOrderLatencies: number[] = [];

function mkdirp(dir: string) {
  mkdirSync(dir, { recursive: true });
}

function recordRequest(record: RequestRecord) {
  requestRecords.push(record);
  appendFileSync(requestsPath, `${JSON.stringify(record)}\n`, "utf-8");
}

function routeKey(route: string) {
  const clean = route.split("?")[0] ?? route;
  return clean
    .replace(/^\/api\/positions\/[^/]+\/sell$/, "/api/positions/:id/sell")
    .replace(/^\/api\/orders\/[^/]+\/cancel$/, "/api/orders/:id/cancel");
}

function percentile(sorted: number[], p: number) {
  if (sorted.length === 0) {
    return 0;
  }
  const index = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[index] ?? sorted[sorted.length - 1] ?? 0;
}

function stats(values: number[]) {
  const sorted = [...values].filter(Number.isFinite).sort((left, right) => left - right);
  const sum = sorted.reduce((total, value) => total + value, 0);
  return {
    count: sorted.length,
    min: sorted[0] ?? 0,
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    max: sorted[sorted.length - 1] ?? 0,
    avg: sorted.length ? sum / sorted.length : 0
  };
}

function csvCell(value: unknown) {
  const text = value === undefined || value === null ? "" : String(value);
  return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function toCsv(rows: Array<Record<string, unknown>>, headers: string[]) {
  return [
    headers.map(csvCell).join(","),
    ...rows.map((row) => headers.map((header) => csvCell(row[header])).join(","))
  ].join("\r\n") + "\r\n";
}

function parseCsv(content: string) {
  const rows: string[][] = [];
  let row: string[] = [];
  let cell = "";
  let quoted = false;
  for (let index = 0; index < content.length; index += 1) {
    const char = content[index];
    const next = content[index + 1];
    if (quoted) {
      if (char === '"' && next === '"') {
        cell += '"';
        index += 1;
      } else if (char === '"') {
        quoted = false;
      } else {
        cell += char;
      }
      continue;
    }
    if (char === '"') {
      quoted = true;
    } else if (char === ",") {
      row.push(cell);
      cell = "";
    } else if (char === "\n") {
      row.push(cell.replace(/\r$/, ""));
      rows.push(row);
      row = [];
      cell = "";
    } else {
      cell += char;
    }
  }
  if (cell || row.length) {
    row.push(cell);
    rows.push(row);
  }
  const [header = [], ...body] = rows.filter((item) => item.length > 1 || item[0]);
  return body.map((values) => Object.fromEntries(header.map((key, index) => [key, values[index] ?? ""])));
}

async function api<T>(
  actor: Pick<Actor, "label" | "token" | "userId" | "role"> | undefined,
  method: string,
  route: string,
  options: { body?: unknown; expectedFailure?: boolean; binary?: false } = {}
): Promise<ApiResult<T>> {
  const startedAt = performance.now();
  const headers: Record<string, string> = {};
  if (actor?.token) {
    headers.Authorization = `Bearer ${actor.token}`;
  }
  if (options.body !== undefined) {
    headers["Content-Type"] = "application/json";
  }
  const response = await fetch(`${baseUrl}${route}`, {
    method,
    headers,
    body: options.body === undefined ? undefined : JSON.stringify(options.body)
  });
  const text = await response.text();
  const latencyMs = Number((performance.now() - startedAt).toFixed(3));
  let parsed: unknown = undefined;
  try {
    parsed = text ? JSON.parse(text) : undefined;
  } catch {
    parsed = text;
  }
  const error =
    parsed && typeof parsed === "object" && "message" in parsed
      ? String((parsed as { message?: unknown }).message)
      : response.ok
        ? undefined
        : text.slice(0, 200);
  const ok = response.ok && !(parsed && typeof parsed === "object" && "error" in parsed);
  recordRequest({
    ts: Date.now(),
    iso: new Date().toISOString(),
    actor: actor?.label ?? "anonymous",
    userId: actor?.userId,
    role: actor?.role,
    method,
    route: routeKey(route),
    status: response.status,
    ok,
    expectedFailure: Boolean(options.expectedFailure),
    latencyMs,
    sizeBytes: Buffer.byteLength(text),
    error: ok ? undefined : error
  });
  return { data: parsed as T, status: response.status, ok, latencyMs, sizeBytes: Buffer.byteLength(text), error };
}

async function apiBinary(
  actor: Actor,
  method: string,
  route: string,
  options: { expectedFailure?: boolean } = {}
): Promise<ApiResult<Buffer>> {
  const startedAt = performance.now();
  const response = await fetch(`${baseUrl}${route}`, {
    method,
    headers: { Authorization: `Bearer ${actor.token}` }
  });
  const arrayBuffer = await response.arrayBuffer();
  const buffer = Buffer.from(arrayBuffer);
  const latencyMs = Number((performance.now() - startedAt).toFixed(3));
  const contentType = response.headers.get("content-type") ?? "";
  let error: string | undefined;
  let ok = response.ok && contentType.includes("zip");
  if (!ok) {
    const text = buffer.toString("utf-8");
    try {
      error = JSON.parse(text).message;
    } catch {
      error = text.slice(0, 200);
    }
  }
  recordRequest({
    ts: Date.now(),
    iso: new Date().toISOString(),
    actor: actor.label,
    userId: actor.userId,
    role: actor.role,
    method,
    route: routeKey(route),
    status: response.status,
    ok,
    expectedFailure: Boolean(options.expectedFailure),
    latencyMs,
    sizeBytes: buffer.length,
    error
  });
  return { data: ok ? buffer : undefined, status: response.status, ok, latencyMs, sizeBytes: buffer.length, error };
}

async function login(label: string, username: string, password: string, role: Role): Promise<Actor> {
  const result = await api<{ token: string; user_id: string }>(undefined, "POST", "/api/auth/login", {
    body: { username, password }
  });
  if (!result.ok || !result.data?.token) {
    throw new Error(`Login failed for ${username}: ${result.error}`);
  }
  return { label, username, password, role, userId: result.data.user_id, token: result.data.token };
}

function collectOrderLatency(order?: OrderRecord) {
  if (!order) {
    return;
  }
  if (typeof order.matchLatencyMs === "number") {
    localMatchLatencies.push(order.localMatchLatencyMs ?? order.matchLatencyMs);
  }
  if (typeof order.bookAcquireLatencyMs === "number") {
    bookAcquireLatencies.push(order.bookAcquireLatencyMs);
  }
  if (typeof order.persistLatencyMs === "number") {
    persistLatencies.push(order.persistLatencyMs);
  }
  if (typeof order.totalOrderLatencyMs === "number") {
    totalOrderLatencies.push(order.totalOrderLatencyMs);
  }
}

async function waitForTrading(actor: Actor) {
  for (let attempt = 0; attempt < 90; attempt += 1) {
    const result = await api<{ currentRound?: { status?: string }; snapshot?: { uiMeta?: { acceptingOrders?: boolean } } }>(
      actor,
      "GET",
      "/api/rounds/current"
    );
    if (result.ok && result.data?.currentRound?.status === "Trading" && result.data.snapshot?.uiMeta?.acceptingOrders) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1_000));
  }
  throw new Error("Timed out waiting for a trading round.");
}

async function createTestAccounts(admin: Actor) {
  const accounts: Actor[] = [
    admin,
    await login("senior", "senior", "senior123", "Senior Tester"),
    await login("tester", "tester", "tester123", "Tester"),
    await login("engineer", "engineer", "engineer123", "Test Engineer")
  ];
  for (let index = 0; index < 6; index += 1) {
    const username = `latency_${stamp}_${index}`;
    const password = `latency${index}!`;
    const created = await api<{ id: string }>(admin, "POST", "/api/users", {
      body: {
        username,
        password,
        displayName: `Latency Tester ${index}`,
        role: "Tester",
        language: "zh-CN",
        seniorTesterId: "u_senior",
        availableUsdc: 20_000
      }
    });
    if (!created.ok || !created.data?.id) {
      throw new Error(`Create user failed for ${username}: ${created.error}`);
    }
    accounts.push(await login(`tester-${index}`, username, password, "Tester"));
  }
  for (const actor of accounts) {
    await api(admin, "POST", `/api/users/${actor.userId}/balance`, {
      body: { availableUsdc: 20_000 }
    });
  }
  return accounts;
}

async function runTradeBatches(accounts: Actor[]) {
  for (let batch = 0; batch < 20; batch += 1) {
    await waitForTrading(accounts[0]!);
    const buyResults = await Promise.all(
      accounts.map(async (actor, index) => {
        const side: Side = (batch + index) % 2 === 0 ? "UP" : "DOWN";
        const result = await api<{ order: OrderRecord }>(actor, "POST", "/api/orders", {
          body: { action: "buy", side, orderKind: "market", amount: 5, clientSendTs: Date.now() }
        });
        const order = result.data?.order;
        collectOrderLatency(order);
        if (result.ok && order?.status === "filled") {
          successfulBuyOrderIds.add(order.id);
        }
        return { actor, side, order };
      })
    );

    await Promise.all(
      accounts.map(async (actor) => {
        await api(actor, "GET", "/api/orders/me");
        await api(actor, "GET", "/api/positions/me");
        await api(actor, "GET", "/api/profile/me");
      })
    );

    if (batch % 4 === 1) {
      await Promise.all(
        accounts.slice(0, 3).map(async (actor) => {
          const positions = await api<PositionRecord[]>(actor, "GET", "/api/positions/me");
          const open = positions.data?.find((position) => position.status === "open" && position.qty > 0);
          if (!open) {
            return;
          }
          const sell = await api<OrderRecord>(actor, "POST", `/api/positions/${open.id}/sell`);
          collectOrderLatency(sell.data);
        })
      );
    }

    if (batch % 5 === 2) {
      await Promise.all(
        accounts.slice(3, 6).map(async (actor) => {
          const positions = await api<PositionRecord[]>(actor, "GET", "/api/positions/me");
          const open = positions.data?.find((position) => position.status === "open" && position.qty > 0);
          if (!open) {
            return;
          }
          const close = await api<{ matchLatencyMs?: number }>(actor, "POST", "/api/positions/close-side", {
            body: { side: open.side, clientSendTs: Date.now() }
          });
          if (typeof close.data?.matchLatencyMs === "number") {
            localMatchLatencies.push(close.data.matchLatencyMs);
          }
        })
      );
    }

    if (batch % 7 === 3) {
      await Promise.all(
        accounts.slice(6, 8).map(async (actor) => {
          const positions = await api<PositionRecord[]>(actor, "GET", "/api/positions/me");
          const open = positions.data?.find((position) => position.status === "open" && position.qty > 0);
          if (!open) {
            return;
          }
          const reverse = await api<{ reverseOrder?: OrderRecord }>(actor, "POST", "/api/positions/reverse-side", {
            body: { side: open.side, clientSendTs: Date.now() }
          });
          collectOrderLatency(reverse.data?.reverseOrder);
        })
      );
    }

    if (batch % 5 === 0) {
      await Promise.all(
        accounts.slice(8, 10).map(async (actor, index) => {
          const side: Side = index % 2 === 0 ? "UP" : "DOWN";
          const pending = await api<{ order: OrderRecord }>(actor, "POST", "/api/orders", {
            body: { action: "buy", side, orderKind: "limit", limitPrice: 0.01, amount: 1, clientSendTs: Date.now() }
          });
          const order = pending.data?.order;
          collectOrderLatency(order);
          if (order?.status === "pending") {
            const cancelled = await api<OrderRecord>(actor, "POST", `/api/orders/${order.id}/cancel`);
            if (cancelled.ok) {
              cancelledOrderIds.add(order.id);
            }
          }
        })
      );
    }

    const failedUnexpected = buyResults.filter((item) => item.order && item.order.status !== "filled");
    if (failedUnexpected.length > 0) {
      console.warn(`[acceptance] batch ${batch} had non-filled buy orders: ${failedUnexpected.length}`);
    }
  }
}

async function exportLogs(actors: Actor[]) {
  const exportActors = [actors[0]!, actors[1]!, actors[2]!, actors[3]!];
  const result: Record<string, string> = {};
  for (const actor of exportActors) {
    const zip = await apiBinary(actor, "GET", "/api/logs/export");
    if (!zip.ok || !zip.data) {
      throw new Error(`Export failed for ${actor.label}: ${zip.error}`);
    }
    const zipPath = path.join(exportsDir, `${actor.label}.zip`);
    writeFileSync(zipPath, zip.data);
    result[actor.label] = zipPath;
  }
  const extractZip = (await import("extract-zip")).default;
  for (const [label, zipPath] of Object.entries(result)) {
    const dir = path.join(unzippedDir, label);
    mkdirp(dir);
    await extractZip(zipPath, { dir });
  }
  return result;
}

function findFiles(root: string, fileName: string): string[] {
  if (!existsSync(root)) {
    return [];
  }
  const result: string[] = [];
  for (const entry of readdirSync(root, { withFileTypes: true })) {
    const fullPath = path.join(root, entry.name);
    if (entry.isDirectory()) {
      result.push(...findFiles(fullPath, fileName));
    } else if (entry.name === fileName) {
      result.push(fullPath);
    }
  }
  return result;
}

function validateExports(startedAt: number, accounts: Actor[]) {
  const required = [
    "order_timestamp_ms",
    "order_timestamp_iso",
    "asset_class",
    "round_id",
    "direction",
    "entry_token_price",
    "btc_trade_price",
    "btc_open_price_to_beat",
    "delta_btc",
    "volume_token_qty",
    "position_notional",
    "exit_type",
    "exit_token_price",
    "settlement_result",
    "tester_id",
    "order_book_snapshot_ref",
    "order_book_snapshot_json",
    "actual_fill_price",
    "slippage_bps",
    "match_latency_ms",
    "settlement_time_ms",
    "settlement_direction"
  ];
  const adminOrderFiles = findFiles(path.join(unzippedDir, "admin"), "orders.csv");
  const adminRows = adminOrderFiles.flatMap((file) => parseCsv(readFileSync(file, "utf-8")));
  const testRows = adminRows.filter((row) => Number(row.order_timestamp_ms) >= startedAt);
  const headers = adminOrderFiles[0] ? readFileSync(adminOrderFiles[0], "utf-8").split(/\r?\n/)[0]?.split(",") ?? [] : [];
  const missingHeaders = required.filter((header) => !headers.includes(header));
  let parseableSnapshots = 0;
  let badSnapshots = 0;
  for (const row of testRows) {
    const raw = row.order_book_snapshot_json;
    if (!raw || raw === "--") {
      continue;
    }
    try {
      const snapshot = JSON.parse(raw);
      if (Array.isArray(snapshot.bids) && Array.isArray(snapshot.asks)) {
        parseableSnapshots += 1;
      } else {
        badSnapshots += 1;
      }
    } catch {
      badSnapshots += 1;
    }
  }
  const exportedBuyIds = new Set(testRows.map((row) => row.order_id));
  const missingBuyLifecycle = [...successfulBuyOrderIds].filter((id) => !exportedBuyIds.has(id));
  const cancelledLifecycleRows = [...cancelledOrderIds].filter((id) => exportedBuyIds.has(id));
  const testerOrderFiles = findFiles(path.join(unzippedDir, "tester"), "orders.csv");
  const engineerOrderFiles = findFiles(path.join(unzippedDir, "engineer"), "orders.csv");
  const seniorRows = findFiles(path.join(unzippedDir, "senior"), "orders.csv").flatMap((file) =>
    parseCsv(readFileSync(file, "utf-8"))
  );
  const seniorRowsForRun = seniorRows.filter((row) => Number(row.order_timestamp_ms) >= startedAt);
  const seniorUserIds = new Set(seniorRowsForRun.map((row) => row.user_id).filter(Boolean));
  const allowedSeniorIds = new Set(accounts.filter((actor) => actor.userId === "u_senior" || actor.role === "Tester").map((actor) => actor.userId));
  const seniorUnexpectedUsers = [...seniorUserIds].filter((id) => !allowedSeniorIds.has(id));
  return {
    requiredHeaders: { ok: missingHeaders.length === 0, missingHeaders },
    lifecycleRowsForRun: testRows.length,
    successfulBuyOrders: successfulBuyOrderIds.size,
    missingBuyLifecycle,
    cancelledLifecycleRows,
    snapshots: {
      parseableSnapshots,
      badSnapshots,
      ok: parseableSnapshots > 0 && badSnapshots === 0
    },
    permissions: {
      adminOrderFiles: adminOrderFiles.length,
      seniorUnexpectedUsers,
      testerOrderFiles: testerOrderFiles.length,
      engineerOrderFiles: engineerOrderFiles.length,
      ok: seniorUnexpectedUsers.length === 0 && testerOrderFiles.length === 1 && engineerOrderFiles.length === 1
    }
  };
}

async function main() {
  mkdirp(artifactDir);
  mkdirp(exportsDir);
  mkdirp(unzippedDir);
  const startedAt = Date.now();

  const admin = await login("admin", "admin", "admin123", "Admin");
  const accounts = await createTestAccounts(admin);
  writeFileSync(
    path.join(artifactDir, "accounts.csv"),
    toCsv(
      accounts.map((actor) => ({
        label: actor.label,
        user_id: actor.userId,
        username: actor.username,
        role: actor.role,
        senior_tester_id: actor.role === "Tester" && actor.userId !== "u_tester" ? "u_senior" : actor.userId === "u_tester" ? "u_senior" : ""
      })),
      ["label", "user_id", "username", "role", "senior_tester_id"]
    ),
    "utf-8"
  );

  const initialLatency = await api(accounts[0], "GET", "/api/system/market/latency");
  await runTradeBatches(accounts);
  const finalLatency = await api(accounts[0], "GET", "/api/system/market/latency");
  await exportLogs(accounts);
  const validation = validateExports(startedAt, accounts);

  const byRoute = Object.fromEntries(
    [...new Set(requestRecords.map((record) => `${record.method} ${record.route}`))].map((key) => [
      key,
      stats(requestRecords.filter((record) => `${record.method} ${record.route}` === key).map((record) => record.latencyMs))
    ])
  );
  const latencySummary = {
    generatedAt: Date.now(),
    baseUrl,
    http: stats(requestRecords.map((record) => record.latencyMs)),
    byRoute,
    localMatch: stats(localMatchLatencies),
    bookAcquire: stats(bookAcquireLatencies),
    persist: stats(persistLatencies),
    totalOrder: stats(totalOrderLatencies)
  };
  writeFileSync(path.join(artifactDir, "latency-summary.json"), JSON.stringify(latencySummary, null, 2), "utf-8");
  writeFileSync(
    path.join(artifactDir, "latency-summary.csv"),
    toCsv(
      [
        { metric: "http", ...latencySummary.http },
        { metric: "local_match", ...latencySummary.localMatch },
        { metric: "book_acquire", ...latencySummary.bookAcquire },
        { metric: "persist", ...latencySummary.persist },
        { metric: "total_order", ...latencySummary.totalOrder }
      ],
      ["metric", "count", "min", "p50", "p95", "p99", "max", "avg"]
    ),
    "utf-8"
  );

  const unexpectedFailures = requestRecords.filter((record) => !record.ok && !record.expectedFailure);
  const validationResults = {
    startedAt,
    finishedAt: Date.now(),
    accounts: accounts.length,
    orders: {
      successfulBuyOrders: successfulBuyOrderIds.size,
      cancelledOrders: cancelledOrderIds.size
    },
    latency: {
      readP95Within100ms:
        Math.max(
          byRoute["GET /api/orders/me"]?.p95 ?? 0,
          byRoute["GET /api/positions/me"]?.p95 ?? 0,
          byRoute["GET /api/profile/me"]?.p95 ?? 0
        ) <= 100,
      tradeHttpP95Within250ms: Math.max(
        byRoute["POST /api/orders"]?.p95 ?? 0,
        byRoute["POST /api/positions/:id/sell"]?.p95 ?? 0,
        byRoute["POST /api/positions/close-side"]?.p95 ?? 0,
        byRoute["POST /api/positions/reverse-side"]?.p95 ?? 0
      ) <= 250,
      localMatchP95Within20ms: latencySummary.localMatch.p95 <= 20
    },
    unexpectedFailures,
    export: validation,
    sourceLatency: {
      initial: initialLatency.data,
      final: finalLatency.data
    }
  };
  writeFileSync(path.join(artifactDir, "validation-results.json"), JSON.stringify(validationResults, null, 2), "utf-8");

  const passed =
    unexpectedFailures.length === 0 &&
    validation.requiredHeaders.ok &&
    validation.snapshots.ok &&
    validation.missingBuyLifecycle.length === 0 &&
    validation.cancelledLifecycleRows.length === 0 &&
    validation.permissions.ok;
  const report = `# 本机延迟优化验收报告

## 结论
- 功能与导出校验：${passed ? "通过" : "存在失败项，详见 validation-results.json"}
- HTTP P95：${latencySummary.http.p95.toFixed(3)}ms
- 本地撮合 P95：${latencySummary.localMatch.p95.toFixed(3)}ms
- 交易写接口 P95：POST /api/orders ${(byRoute["POST /api/orders"]?.p95 ?? 0).toFixed(3)}ms，sell ${(byRoute["POST /api/positions/:id/sell"]?.p95 ?? 0).toFixed(3)}ms，close ${(byRoute["POST /api/positions/close-side"]?.p95 ?? 0).toFixed(3)}ms，reverse ${(byRoute["POST /api/positions/reverse-side"]?.p95 ?? 0).toFixed(3)}ms

## 交易与写入
- 账户数：${accounts.length}
- 成功买入订单：${successfulBuyOrderIds.size}
- 取消 pending 限价单：${cancelledOrderIds.size}
- 非预期 API 失败：${unexpectedFailures.length}
- 导出中本轮生命周期订单行：${validation.lifecycleRowsForRun}
- 快照 JSON 可解析行：${validation.snapshots.parseableSnapshots}

## 权限与导出
- Admin orders.csv 文件数：${validation.permissions.adminOrderFiles}
- Senior 越权用户数：${validation.permissions.seniorUnexpectedUsers.length}
- Tester orders.csv 文件数：${validation.permissions.testerOrderFiles}
- Engineer orders.csv 文件数：${validation.permissions.engineerOrderFiles}

## 产物
- requests.jsonl：每个 HTTP 请求的状态与耗时
- latency-summary.json/csv：HTTP、盘口获取、本地撮合、落库、订单总耗时分布
- validation-results.json：字段、快照、生命周期、权限校验
- exports/：原始 ZIP
- exports-unzipped/：解压 CSV
`;
  writeFileSync(path.join(artifactDir, "测试报告.md"), report, "utf-8");
  console.log(JSON.stringify({ artifactDir, passed, latencySummary: latencySummary.http, localMatch: latencySummary.localMatch }, null, 2));
}

void main().catch((error) => {
  mkdirp(artifactDir);
  writeFileSync(path.join(artifactDir, "fatal-error.txt"), error instanceof Error ? error.stack ?? error.message : String(error), "utf-8");
  console.error(error);
  process.exitCode = 1;
});

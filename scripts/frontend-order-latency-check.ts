import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { setTimeout as sleep } from "node:timers/promises";
import WebSocket from "ws";

type Channel = "market" | "user";
type TradeSide = "UP" | "DOWN";

interface PublicUser {
  id: string;
  username: string;
  isActive: boolean;
}

interface LoginResponse extends PublicUser {
  token: string;
}

interface BulkCreateResponse {
  created: Array<{ rowNumber: number; user: PublicUser }>;
  failed: unknown[];
  total: number;
}

interface OrderRecord {
  id: string;
  status: string;
  bookAcquireLatencyMs?: number;
  localMatchLatencyMs?: number;
  persistLatencyMs?: number;
  totalOrderLatencyMs?: number;
}

interface TransportMeta {
  serverPublishTs?: number;
  wsSendStartTs?: number;
  snapshotBuildTs?: number;
  serverQueueMs?: number;
  payloadSeq?: number;
  broadcastBuildMs?: number;
  broadcastFanoutSize?: number;
}

interface SourceHealth {
  normalizedTs?: number;
  serverRecvTs?: number;
  sourceEventTs?: number;
  serverPublishTs?: number;
}

interface MarketPayloadLike {
  transportMeta?: TransportMeta;
  tick?: { sources?: Record<string, SourceHealth> };
  snapshot?: { sources?: Record<string, SourceHealth> };
}

interface SimClient {
  id: string;
  index: number;
  username: string;
  token: string;
  sockets: Partial<Record<Channel, WebSocket>>;
  messages: Record<Channel, number>;
  bytes: Record<Channel, number[]>;
  firstMessageMs: Partial<Record<Channel, number>>;
  lastMessageAt: Partial<Record<Channel, number>>;
  gaps: Record<Channel, number[]>;
  parseMs: Record<Channel, number[]>;
  unexpectedCloses: Record<Channel, number>;
  errors: Record<Channel, number>;
  intentionalClose: boolean;
}

interface PendingOrderProbe {
  clientIndex: number;
  startedAt: number;
  finishedAt?: number;
  firstUserMessageAfterStartMs?: number;
  firstUserMessageAfterFinishMs?: number;
}

interface OrderAttempt {
  clientIndex: number;
  username: string;
  side: TradeSide;
  clientOrderId: string;
  startedAt: number;
  finishedAt: number;
  httpTotalMs: number;
  ok: boolean;
  status?: string;
  orderId?: string;
  userWsAfterStartMs?: number;
  userWsAfterFinishMs?: number;
  bookAcquireLatencyMs?: number;
  localMatchLatencyMs?: number;
  persistLatencyMs?: number;
  totalOrderLatencyMs?: number;
  error?: string;
}

interface DockerStatsSample {
  raw: string;
  appServerCpuPercent?: number;
  appServerMemory?: string;
}

interface Sample {
  at: string;
  phase: string;
  metrics?: unknown;
  dockerStats?: DockerStatsSample;
  clients: ReturnType<typeof clientStats>;
}

const execFileAsync = promisify(execFile);
const startedAt = new Date();
const baseUrl = envText("LOAD_TEST_BASE_URL", "http://103.147.13.98:10001").replace(/\/+$/, "");
const adminUsername = process.env.LOAD_TEST_ADMIN_USERNAME?.trim() ?? "";
const adminPassword = process.env.LOAD_TEST_ADMIN_PASSWORD?.trim() ?? "";
const usersCount = envInt("FRONTEND_LATENCY_USERS", 3);
const ordersPerUser = envInt("FRONTEND_LATENCY_ORDERS_PER_USER", 2);
const orderConcurrency = envInt("FRONTEND_LATENCY_CONCURRENCY", usersCount);
const orderSpacingMs = envInt("FRONTEND_LATENCY_SPACING_MS", 250);
const amount = envNumber("FRONTEND_LATENCY_AMOUNT", 1);
const warmupMs = envInt("FRONTEND_LATENCY_WARMUP_MS", 5_000);
const settleMs = envInt("FRONTEND_LATENCY_SETTLE_MS", 5_000);
const sampleIntervalMs = envInt("FRONTEND_LATENCY_SAMPLE_MS", 1_000);
const firstMessageTimeoutMs = envInt("FRONTEND_LATENCY_FIRST_MESSAGE_TIMEOUT_MS", 10_000);
const requestTimeoutMs = envInt("FRONTEND_LATENCY_REQUEST_TIMEOUT_MS", 20_000);
const prefix = envText("FRONTEND_LATENCY_PREFIX", `frontend_latency_${timestampForName()}`);
const outputPath = process.env.FRONTEND_LATENCY_OUTPUT?.trim() || defaultOutputPath();
const sshTarget = process.env.FRONTEND_LATENCY_SSH_TARGET?.trim();
const skipCleanup = process.env.FRONTEND_LATENCY_SKIP_CLEANUP === "true";

const createdUsers: PublicUser[] = [];
const clients: SimClient[] = [];
const orders: OrderAttempt[] = [];
const samples: Sample[] = [];
const pendingOrderProbes = new Map<string, PendingOrderProbe>();
const intentionallyClosingSockets = new WeakSet<WebSocket>();
const marketServerToClientMs: number[] = [];
const marketWsSendToClientMs: number[] = [];
const marketSnapshotAgeMs: number[] = [];
const marketServerQueueMs: number[] = [];
const marketPayloadSeq: number[] = [];
const sourceAgeMs: Record<string, number[]> = {};
let clockOffsetMs = 0;
let adminToken = "";
let cancelled = false;
let failureMessage: string | undefined;
let sampleTask: Promise<void> | undefined;

if (process.argv.includes("--help")) {
  printHelp();
  process.exit(0);
}

process.on("SIGINT", () => {
  cancelled = true;
  console.warn("SIGINT received; cleaning up test sockets/users.");
});

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

async function main() {
  if (!adminUsername || !adminPassword) {
    throw new Error("Set LOAD_TEST_ADMIN_USERNAME and LOAD_TEST_ADMIN_PASSWORD.");
  }

  let passed = false;
  try {
    clockOffsetMs = await sampleClockOffset();
    adminToken = (await login(adminUsername, adminPassword)).token;
    await assertReady("precheck");
    await createTestUsers();
    clients.push(...(await loginClients()));
    await connectAllClients();
    await assertAcceptingOrders();
    startSampler();
    await recordSample("warmup-start");
    await sleep(warmupMs);
    await recordSample("before-orders");
    await runOrders();
    await recordSample("after-orders");
    await sleep(settleMs);
    await recordSample("settled");
    await assertReady("postcheck");
    passed = true;
  } catch (error) {
    failureMessage = error instanceof Error ? error.stack ?? error.message : String(error);
    console.error(failureMessage);
  } finally {
    cancelled = true;
    if (sampleTask) {
      await sampleTask.catch(() => undefined);
    }
    closeAllClients();
    if (!skipCleanup && adminToken && createdUsers.length > 0) {
      await disableCreatedUsers();
    }
    writeResult(passed);
  }

  if (!passed) {
    process.exitCode = 1;
  }
}

async function createTestUsers() {
  const balance = Math.max(10, amount * ordersPerUser + 5);
  const users = Array.from({ length: usersCount }, (_, index) => ({
    username: `${prefix}_${String(index + 1).padStart(3, "0")}`,
    password: passwordFor(index),
    displayName: `Frontend Latency ${index + 1}`,
    role: "Tester",
    language: "zh-CN",
    permissionLevel: "Standard",
    mustChangePassword: false,
    availableUsdc: balance
  }));
  const result = await request<BulkCreateResponse>("/api/users/bulk", {
    method: "POST",
    token: adminToken,
    body: { users }
  });
  if (result.failed.length > 0 || result.created.length !== usersCount) {
    throw new Error(`Bulk create failed: created=${result.created.length}, failed=${JSON.stringify(result.failed)}`);
  }
  createdUsers.push(...result.created.map((item) => item.user));
  console.log(`created users: ${createdUsers.length}`);
}

async function loginClients() {
  const loggedIn: SimClient[] = [];
  for (const [index, user] of createdUsers.entries()) {
    const response = await login(user.username, passwordFor(index));
    loggedIn.push(createClient(index, response.id, user.username, response.token));
  }
  console.log(`logged in users: ${loggedIn.length}`);
  return loggedIn;
}

async function connectAllClients() {
  await mapLimit(clients, Math.min(10, clients.length), async (client) => {
    const [market, user] = await Promise.all([connectChannel(client, "market"), connectChannel(client, "user")]);
    client.sockets.market = market;
    client.sockets.user = user;
  });
  console.log(`connected websocket pairs: ${clients.length}`);
}

async function connectChannel(client: SimClient, channel: Channel) {
  const ticket = await request<{ ticket: string }>("/api/ws/tickets", {
    method: "POST",
    token: client.token,
    body: { channel }
  });
  const wsUrl = `${baseUrl.replace("http://", "ws://").replace("https://", "wss://")}/ws/${channel}?ticket=${encodeURIComponent(ticket.ticket)}`;
  const openedAt = Date.now();
  return await new Promise<WebSocket>((resolve, reject) => {
    const socket = new WebSocket(wsUrl);
    let settled = false;
    const timer = setTimeout(() => {
      if (!settled) {
        settled = true;
        intentionallyClosingSockets.add(socket);
        socket.close();
        reject(new Error(`${client.username} ${channel} did not open in ${firstMessageTimeoutMs}ms`));
      }
    }, firstMessageTimeoutMs);

    socket.on("open", () => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        resolve(socket);
      }
    });
    socket.on("message", (raw) => {
      recordWsMessage(client, channel, raw);
      if (!client.firstMessageMs[channel]) {
        client.firstMessageMs[channel] = Date.now() - openedAt;
      }
    });
    socket.on("close", () => {
      const intentional = client.intentionalClose || intentionallyClosingSockets.has(socket);
      intentionallyClosingSockets.delete(socket);
      if (!intentional) {
        client.unexpectedCloses[channel] += 1;
      }
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        reject(new Error(`${client.username} ${channel} closed before first message`));
      }
    });
    socket.on("error", (error) => {
      client.errors[channel] += 1;
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        reject(error);
      }
    });
  });
}

function recordWsMessage(client: SimClient, channel: Channel, raw: WebSocket.RawData) {
  const receivedAt = Date.now();
  const lastMessageAt = client.lastMessageAt[channel];
  if (lastMessageAt) {
    client.gaps[channel].push(receivedAt - lastMessageAt);
  }
  client.lastMessageAt[channel] = receivedAt;
  client.messages[channel] += 1;
  client.bytes[channel].push(rawDataByteLength(raw));

  const parseStartedAt = Date.now();
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw.toString());
  } catch {
    client.parseMs[channel].push(Date.now() - parseStartedAt);
    return;
  }
  client.parseMs[channel].push(Date.now() - parseStartedAt);

  if (channel === "market") {
    recordMarketLatency(parsed, receivedAt);
  } else {
    recordUserLatency(client, receivedAt);
  }
}

function recordMarketLatency(parsed: unknown, receivedAt: number) {
  const message = parsed as { type?: string; data?: MarketPayloadLike };
  if (message.type !== "market" && message.type !== "market:tick") {
    return;
  }
  const meta = message.data?.transportMeta;
  if (typeof meta?.serverPublishTs === "number") {
    marketServerToClientMs.push(Math.max(receivedAt - meta.serverPublishTs - clockOffsetMs, 0));
  }
  if (typeof meta?.wsSendStartTs === "number") {
    marketWsSendToClientMs.push(Math.max(receivedAt - meta.wsSendStartTs - clockOffsetMs, 0));
  }
  if (typeof meta?.snapshotBuildTs === "number" && typeof meta.serverPublishTs === "number") {
    marketSnapshotAgeMs.push(Math.max(meta.serverPublishTs - meta.snapshotBuildTs, 0));
  }
  if (typeof meta?.serverQueueMs === "number") {
    marketServerQueueMs.push(meta.serverQueueMs);
  }
  if (typeof meta?.payloadSeq === "number") {
    marketPayloadSeq.push(meta.payloadSeq);
  }

  const sources = message.data?.tick?.sources ?? message.data?.snapshot?.sources;
  if (!sources) {
    return;
  }
  for (const [name, source] of Object.entries(sources)) {
    if (typeof source.normalizedTs !== "number") {
      continue;
    }
    const bucket = sourceAgeMs[name] ?? [];
    bucket.push(Math.max(receivedAt - source.normalizedTs - clockOffsetMs, 0));
    sourceAgeMs[name] = bucket;
  }
}

function recordUserLatency(client: SimClient, receivedAt: number) {
  for (const probe of pendingOrderProbes.values()) {
    if (probe.clientIndex !== client.index) {
      continue;
    }
    probe.firstUserMessageAfterStartMs ??= Math.max(receivedAt - probe.startedAt, 0);
    if (probe.finishedAt) {
      probe.firstUserMessageAfterFinishMs ??= Math.max(receivedAt - probe.finishedAt, 0);
    }
  }
}

async function assertAcceptingOrders() {
  const current = await request<Record<string, unknown>>("/api/rounds/current", { token: clients[0]?.token ?? adminToken });
  const currentRound = current.currentRound as { acceptingOrders?: boolean; id?: string; status?: string } | undefined;
  if (!currentRound?.id) {
    throw new Error("Current round is missing.");
  }
  if (currentRound.acceptingOrders === false) {
    throw new Error(`Current round ${currentRound.id} is not accepting orders.`);
  }
  console.log(`current round accepts orders: ${currentRound.id} status=${currentRound.status ?? "unknown"}`);
}

async function runOrders() {
  const tasks = clients.flatMap((client) =>
    Array.from({ length: ordersPerUser }, (_, orderIndex) => ({ client, orderIndex }))
  );
  let sent = 0;
  await mapLimit(tasks, Math.max(1, orderConcurrency), async ({ client, orderIndex }) => {
    const sequence = sent++;
    if (orderSpacingMs > 0) {
      await sleep(sequence * orderSpacingMs);
    }
    if (cancelled) {
      return;
    }
    const side: TradeSide = (client.index + orderIndex) % 2 === 0 ? "UP" : "DOWN";
    const clientOrderId = `${prefix}_${client.index}_${orderIndex}_${Date.now()}`;
    const started = Date.now();
    const probe: PendingOrderProbe = { clientIndex: client.index, startedAt: started };
    pendingOrderProbes.set(clientOrderId, probe);
    try {
      const response = await request<{ order: OrderRecord; tradePatch?: unknown }>("/api/orders", {
        method: "POST",
        token: client.token,
        body: {
          action: "buy",
          side,
          orderKind: "market",
          amount,
          clientOrderId,
          clientSendTs: started
        }
      });
      const finished = Date.now();
      probe.finishedAt = finished;
      await sleep(250);
      orders.push({
        clientIndex: client.index,
        username: client.username,
        side,
        clientOrderId,
        startedAt: started,
        finishedAt: finished,
        httpTotalMs: finished - started,
        ok: true,
        status: response.order.status,
        orderId: response.order.id,
        userWsAfterStartMs: probe.firstUserMessageAfterStartMs,
        userWsAfterFinishMs: probe.firstUserMessageAfterFinishMs,
        bookAcquireLatencyMs: response.order.bookAcquireLatencyMs,
        localMatchLatencyMs: response.order.localMatchLatencyMs,
        persistLatencyMs: response.order.persistLatencyMs,
        totalOrderLatencyMs: response.order.totalOrderLatencyMs
      });
    } catch (error) {
      const finished = Date.now();
      probe.finishedAt = finished;
      orders.push({
        clientIndex: client.index,
        username: client.username,
        side,
        clientOrderId,
        startedAt: started,
        finishedAt: finished,
        httpTotalMs: finished - started,
        ok: false,
        userWsAfterStartMs: probe.firstUserMessageAfterStartMs,
        userWsAfterFinishMs: probe.firstUserMessageAfterFinishMs,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  });
  console.log(`orders attempted: ${orders.length}`);
}

function startSampler() {
  sampleTask = (async () => {
    while (!cancelled) {
      await sleep(sampleIntervalMs);
      if (!cancelled) {
        await recordSample("running").catch((error) => {
          console.warn(`sample failed: ${error instanceof Error ? error.message : String(error)}`);
        });
      }
    }
  })();
}

async function recordSample(phase: string) {
  const [metrics, dockerStats] = await Promise.allSettled([
    request<unknown>("/api/metrics"),
    sampleDockerStats()
  ]);
  const sample: Sample = {
    at: new Date().toISOString(),
    phase,
    metrics: metrics.status === "fulfilled" ? metrics.value : { error: String(metrics.reason) },
    dockerStats: dockerStats.status === "fulfilled" ? dockerStats.value : undefined,
    clients: clientStats()
  };
  samples.push(sample);
  console.log(JSON.stringify({
    at: sample.at,
    phase,
    orders: orders.length,
    okOrders: orders.filter((order) => order.ok).length,
    failedOrders: orders.filter((order) => !order.ok).length,
    marketServerToClientP95: summarizeNumbers(marketServerToClientMs).p95,
    userWsAfterFinishP95: summarizeNumbers(orders.map((order) => order.userWsAfterFinishMs)).p95,
    clients: sample.clients,
    appServerCpu: sample.dockerStats?.appServerCpuPercent
  }));
}

async function sampleDockerStats(): Promise<DockerStatsSample | undefined> {
  if (!sshTarget) {
    return undefined;
  }
  const command = "docker stats --no-stream --format '{{.Name}}\\t{{.CPUPerc}}\\t{{.MemUsage}}'";
  const { stdout } = await execFileAsync("ssh", [sshTarget, command], { timeout: 10_000, maxBuffer: 1024 * 1024 });
  const line = stdout.split(/\r?\n/).find((item) => item.startsWith("app-app-server-1\t"));
  const parts = line?.split("\t") ?? [];
  return {
    raw: stdout.trim(),
    appServerCpuPercent: parts[1] ? Number(parts[1].replace("%", "")) : undefined,
    appServerMemory: parts[2]
  };
}

async function assertReady(label: string) {
  const ready = await request<{ ok?: boolean }>("/api/health/ready");
  if (!ready.ok) {
    throw new Error(`health ${label} returned ok=false`);
  }
  console.log(`health ${label}: ok`);
}

async function disableCreatedUsers() {
  for (const user of createdUsers) {
    await request<PublicUser>(`/api/users/${encodeURIComponent(user.id)}/disable`, {
      method: "POST",
      token: adminToken,
      body: {}
    }).catch((error) => {
      console.warn(`failed to disable ${user.username}: ${error instanceof Error ? error.message : String(error)}`);
    });
  }
  console.log(`disabled users: ${createdUsers.length}`);
}

function writeResult(passed: boolean) {
  const result = {
    passed,
    startedAt: startedAt.toISOString(),
    finishedAt: new Date().toISOString(),
    baseUrl,
    prefix,
    config: {
      usersCount,
      ordersPerUser,
      orderConcurrency,
      orderSpacingMs,
      amount,
      warmupMs,
      settleMs,
      sampleIntervalMs,
      sshTarget: sshTarget ? redactSshTarget(sshTarget) : undefined,
      skipCleanup
    },
    createdUsers: createdUsers.length,
    disabledUsers: skipCleanup ? 0 : createdUsers.length,
    failure: failureMessage,
    summary: summarize(),
    samples,
    orders
  };
  mkdirSync(path.dirname(outputPath), { recursive: true });
  writeFileSync(outputPath, `${JSON.stringify(result, null, 2)}\n`, "utf8");
  console.log(`result written: ${outputPath}`);
  console.log(JSON.stringify(result.summary, null, 2));
}

function summarize() {
  const successful = orders.filter((order) => order.ok);
  const failed = orders.filter((order) => !order.ok);
  return {
    totalOrders: orders.length,
    successfulOrders: successful.length,
    failedOrders: failed.length,
    frontendMarket: {
      serverToClientMs: summarizeNumbers(marketServerToClientMs),
      wsSendToClientMs: summarizeNumbers(marketWsSendToClientMs),
      snapshotAgeMs: summarizeNumbers(marketSnapshotAgeMs),
      serverQueueMs: summarizeNumbers(marketServerQueueMs),
      sourceAgeMs: Object.fromEntries(Object.entries(sourceAgeMs).map(([key, values]) => [key, summarizeNumbers(values)])),
      payloadSeq: {
        first: marketPayloadSeq[0],
        last: marketPayloadSeq[marketPayloadSeq.length - 1],
        count: marketPayloadSeq.length,
        outOfOrder: countOutOfOrder(marketPayloadSeq)
      }
    },
    frontendWs: clientStats(),
    orders: {
      httpTotalMs: summarizeNumbers(successful.map((order) => order.httpTotalMs)),
      userWsAfterStartMs: summarizeNumbers(successful.map((order) => order.userWsAfterStartMs)),
      userWsAfterFinishMs: summarizeNumbers(successful.map((order) => order.userWsAfterFinishMs)),
      bookAcquireLatencyMs: summarizeNumbers(successful.map((order) => order.bookAcquireLatencyMs)),
      localMatchLatencyMs: summarizeNumbers(successful.map((order) => order.localMatchLatencyMs)),
      persistLatencyMs: summarizeNumbers(successful.map((order) => order.persistLatencyMs)),
      totalOrderLatencyMs: summarizeNumbers(successful.map((order) => order.totalOrderLatencyMs))
    },
    failedReasons: countBy(failed.map((order) => order.error ?? "unknown"))
  };
}

function clientStats() {
  const now = Date.now();
  const marketGaps = clients.flatMap((client) => client.gaps.market);
  const userGaps = clients.flatMap((client) => client.gaps.user);
  return {
    openMarketSockets: clients.filter((client) => socketOpen(client.sockets.market)).length,
    openUserSockets: clients.filter((client) => socketOpen(client.sockets.user)).length,
    marketMessages: clients.reduce((sum, client) => sum + client.messages.market, 0),
    userMessages: clients.reduce((sum, client) => sum + client.messages.user, 0),
    unexpectedMarketCloses: clients.reduce((sum, client) => sum + client.unexpectedCloses.market, 0),
    unexpectedUserCloses: clients.reduce((sum, client) => sum + client.unexpectedCloses.user, 0),
    longestCurrentMarketGapMs: Math.max(0, ...clients.map((client) => gapMs(client, "market", now))),
    longestCurrentUserGapMs: Math.max(0, ...clients.map((client) => gapMs(client, "user", now))),
    marketGapMs: summarizeNumbers(marketGaps),
    userGapMs: summarizeNumbers(userGaps),
    marketBytes: summarizeNumbers(clients.flatMap((client) => client.bytes.market)),
    userBytes: summarizeNumbers(clients.flatMap((client) => client.bytes.user)),
    marketParseMs: summarizeNumbers(clients.flatMap((client) => client.parseMs.market)),
    userParseMs: summarizeNumbers(clients.flatMap((client) => client.parseMs.user)),
    firstMarketMessageMs: summarizeNumbers(clients.map((client) => client.firstMessageMs.market)),
    firstUserMessageMs: summarizeNumbers(clients.map((client) => client.firstMessageMs.user))
  };
}

function gapMs(client: SimClient, channel: Channel, now: number) {
  const last = client.lastMessageAt[channel];
  return last ? now - last : 0;
}

async function sampleClockOffset() {
  const samples: number[] = [];
  for (let index = 0; index < 3; index += 1) {
    const before = Date.now();
    const ready = await request<{ serverNow?: number }>("/api/health/ready");
    const after = Date.now();
    if (typeof ready.serverNow === "number") {
      samples.push(Math.round((before + after) / 2 - ready.serverNow));
    }
  }
  return median(samples) ?? 0;
}

async function login(username: string, password: string) {
  return await request<LoginResponse>("/api/auth/login", {
    method: "POST",
    body: { username, password }
  });
}

async function request<T>(pathname: string, options: { method?: string; token?: string; body?: unknown } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), requestTimeoutMs);
  try {
    const response = await fetch(`${baseUrl}${pathname}`, {
      method: options.method ?? "GET",
      headers: {
        "content-type": "application/json",
        ...(options.token ? { authorization: `Bearer ${options.token}` } : {})
      },
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
      signal: controller.signal
    });
    const text = await response.text();
    if (!response.ok) {
      throw new Error(`${options.method ?? "GET"} ${pathname} failed ${response.status}: ${text}`);
    }
    return (text ? JSON.parse(text) : undefined) as T;
  } finally {
    clearTimeout(timer);
  }
}

async function mapLimit<T>(items: T[], limit: number, worker: (item: T) => Promise<void>) {
  let cursor = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (cursor < items.length && !cancelled) {
      const item = items[cursor++];
      if (item) {
        await worker(item);
      }
    }
  });
  await Promise.all(workers);
}

function closeAllClients() {
  for (const client of clients) {
    client.intentionalClose = true;
    for (const socket of Object.values(client.sockets)) {
      if (socket && socket.readyState === WebSocket.OPEN) {
        intentionallyClosingSockets.add(socket);
        socket.close();
      }
    }
  }
}

function socketOpen(socket?: WebSocket) {
  return socket?.readyState === WebSocket.OPEN;
}

function createClient(index: number, id: string, username: string, token: string): SimClient {
  return {
    id,
    index,
    username,
    token,
    sockets: {},
    messages: { market: 0, user: 0 },
    bytes: { market: [], user: [] },
    firstMessageMs: {},
    lastMessageAt: {},
    gaps: { market: [], user: [] },
    parseMs: { market: [], user: [] },
    unexpectedCloses: { market: 0, user: 0 },
    errors: { market: 0, user: 0 },
    intentionalClose: false
  };
}

function rawDataByteLength(raw: WebSocket.RawData) {
  if (typeof raw === "string") {
    return Buffer.byteLength(raw);
  }
  if (Buffer.isBuffer(raw)) {
    return raw.byteLength;
  }
  if (raw instanceof ArrayBuffer) {
    return raw.byteLength;
  }
  return raw.reduce((sum, chunk) => sum + chunk.byteLength, 0);
}

function summarizeNumbers(values: Array<number | undefined>) {
  const sorted = values.filter((value): value is number => typeof value === "number" && Number.isFinite(value)).sort((a, b) => a - b);
  if (sorted.length === 0) {
    return { count: 0 };
  }
  return {
    count: sorted.length,
    min: sorted[0],
    p50: percentile(sorted, 0.5),
    p95: percentile(sorted, 0.95),
    p99: percentile(sorted, 0.99),
    max: sorted[sorted.length - 1],
    avg: Number((sorted.reduce((sum, value) => sum + value, 0) / sorted.length).toFixed(2))
  };
}

function percentile(sorted: number[], ratio: number) {
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * ratio) - 1));
  return sorted[index];
}

function median(values: number[]) {
  if (values.length === 0) {
    return undefined;
  }
  return percentile([...values].sort((a, b) => a - b), 0.5);
}

function countOutOfOrder(values: number[]) {
  let count = 0;
  let previous = 0;
  for (const value of values) {
    if (previous && value <= previous) {
      count += 1;
    }
    previous = value;
  }
  return count;
}

function countBy(values: string[]) {
  return values.reduce<Record<string, number>>((counts, value) => {
    counts[value] = (counts[value] ?? 0) + 1;
    return counts;
  }, {});
}

function envText(name: string, fallback: string) {
  const value = process.env[name]?.trim();
  return value || fallback;
}

function envInt(name: string, fallback: number) {
  const value = Number(process.env[name]);
  return Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;
}

function envNumber(name: string, fallback: number) {
  const value = Number(process.env[name]);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

function timestampForName() {
  return new Date().toISOString().replace(/[-:]/g, "").replace(/\..+$/, "").replace("T", "_");
}

function passwordFor(index: number) {
  return `FrontendLatency-${prefix}-${index + 1}!`;
}

function defaultOutputPath() {
  return path.join(process.cwd(), "deploy", `frontend-order-latency-${timestampForName()}.json`);
}

function redactSshTarget(target: string) {
  return target.replace(/^([^@]+)@/, "***@");
}

function printHelp() {
  console.log(`frontend-order-latency-check

Required:
  LOAD_TEST_ADMIN_USERNAME       Admin username.
  LOAD_TEST_ADMIN_PASSWORD       Admin password.

Common options:
  LOAD_TEST_BASE_URL             Default: http://103.147.13.98:10001
  FRONTEND_LATENCY_USERS         Default: 3
  FRONTEND_LATENCY_ORDERS_PER_USER Default: 2
  FRONTEND_LATENCY_CONCURRENCY   Default: users count
  FRONTEND_LATENCY_AMOUNT        Default: 1
  FRONTEND_LATENCY_WARMUP_MS     Default: 5000
  FRONTEND_LATENCY_SETTLE_MS     Default: 5000
  FRONTEND_LATENCY_OUTPUT        Default: deploy/frontend-order-latency-<timestamp>.json
  FRONTEND_LATENCY_SSH_TARGET    Optional, example: root@103.147.13.98
  FRONTEND_LATENCY_SKIP_CLEANUP  Set true to keep test users active.

Example:
  $env:LOAD_TEST_ADMIN_USERNAME="admin"; $env:LOAD_TEST_ADMIN_PASSWORD="***"; npx tsx scripts/frontend-order-latency-check.ts
`);
}

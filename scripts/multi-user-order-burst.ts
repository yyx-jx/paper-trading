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
  userId: string;
  side: TradeSide;
  status: string;
  requestedAmountUsdc?: number;
  notionalUsdc: number;
  bookAcquireLatencyMs?: number;
  localMatchLatencyMs?: number;
  persistLatencyMs?: number;
  totalOrderLatencyMs?: number;
  createdAt: number;
}

interface SimClient {
  id: string;
  index: number;
  username: string;
  password: string;
  token: string;
  sockets: Partial<Record<Channel, WebSocket>>;
  messages: Record<Channel, number>;
  opens: Record<Channel, number>;
  unexpectedCloses: Record<Channel, number>;
  errors: Record<Channel, number>;
  firstMessageMs: Partial<Record<Channel, number>>;
  lastMessageAt: Partial<Record<Channel, number>>;
  intentionalClose: boolean;
}

interface OrderAttempt {
  clientIndex: number;
  username: string;
  side: TradeSide;
  clientOrderId: string;
  startedAt: number;
  finishedAt: number;
  totalMs: number;
  ok: boolean;
  status?: string;
  orderId?: string;
  bookAcquireLatencyMs?: number;
  localMatchLatencyMs?: number;
  persistLatencyMs?: number;
  totalOrderLatencyMs?: number;
  error?: string;
}

interface Sample {
  at: string;
  phase: string;
  metrics?: unknown;
  ready?: unknown;
  dockerStats?: DockerStatsSample;
  clients: {
    openMarketSockets: number;
    openUserSockets: number;
    marketMessages: number;
    userMessages: number;
    unexpectedMarketCloses: number;
    unexpectedUserCloses: number;
    longestMarketGapMs: number;
    longestUserGapMs: number;
  };
}

interface DockerStatsSample {
  raw: string;
  appServerCpuPercent?: number;
  appServerMemory?: string;
}

const execFileAsync = promisify(execFile);
const startedAt = new Date();
const baseUrl = envText("LOAD_TEST_BASE_URL", "http://103.147.13.98:10001").replace(/\/+$/, "");
const adminUsername = process.env.LOAD_TEST_ADMIN_USERNAME?.trim() ?? "";
const adminPassword = process.env.LOAD_TEST_ADMIN_PASSWORD?.trim() ?? "";
const usersCount = envInt("ORDER_BURST_USERS", 10);
const ordersPerUser = envInt("ORDER_BURST_ORDERS_PER_USER", 5);
const amount = envNumber("ORDER_BURST_AMOUNT", 1);
const orderConcurrency = envInt("ORDER_BURST_CONCURRENCY", 10);
const orderSpacingMs = envInt("ORDER_BURST_SPACING_MS", 100);
const sampleIntervalMs = envInt("ORDER_BURST_SAMPLE_MS", 1_000);
const firstMessageTimeoutMs = envInt("ORDER_BURST_FIRST_MESSAGE_TIMEOUT_MS", 10_000);
const requestTimeoutMs = envInt("ORDER_BURST_REQUEST_TIMEOUT_MS", 15_000);
const prefix = envText("ORDER_BURST_PREFIX", `burst_${timestampForName()}`);
const skipCleanup = process.env.ORDER_BURST_SKIP_CLEANUP === "true";
const cleanupPrefix = process.env.ORDER_BURST_CLEANUP_PREFIX?.trim();
const sshTarget = process.env.ORDER_BURST_SSH_TARGET?.trim();
const outputPath = process.env.ORDER_BURST_OUTPUT?.trim() || defaultOutputPath();

const createdUsers: PublicUser[] = [];
const clients: SimClient[] = [];
const samples: Sample[] = [];
const orders: OrderAttempt[] = [];
const intentionallyClosingSockets = new WeakSet<WebSocket>();
let adminToken = "";
let cancelled = false;
let failureMessage: string | undefined;
let sampleTask: Promise<void> | undefined;

process.on("SIGINT", () => {
  cancelled = true;
  console.warn("SIGINT received; finishing cleanup before exit.");
});

if (process.argv.includes("--help")) {
  printHelp();
  process.exit(0);
}

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
    adminToken = (await login(adminUsername, adminPassword)).token;
    if (cleanupPrefix) {
      await cleanupUsersByPrefix(cleanupPrefix);
      passed = true;
      return;
    }

    await assertReady("precheck");
    await createTestUsers();
    clients.push(...(await loginClients()));
    await connectAllClients();
    await assertAcceptingOrders();
    startSampler();
    await recordSample("before-orders");
    await runOrderBurst();
    await recordSample("after-orders");
    await sleep(2_000);
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
    displayName: `Order Burst ${index + 1}`,
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
    loggedIn.push(createClient(index, response.id, user.username, passwordFor(index), response.token));
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
        reject(new Error(`${client.username} ${channel} did not receive first message in ${firstMessageTimeoutMs}ms`));
      }
    }, firstMessageTimeoutMs);

    socket.on("open", () => {
      client.opens[channel] += 1;
    });
    socket.on("message", () => {
      client.messages[channel] += 1;
      client.lastMessageAt[channel] = Date.now();
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        client.firstMessageMs[channel] = Date.now() - openedAt;
        resolve(socket);
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

async function runOrderBurst() {
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
      orders.push({
        clientIndex: client.index,
        username: client.username,
        side,
        clientOrderId,
        startedAt: started,
        finishedAt: finished,
        totalMs: finished - started,
        ok: true,
        status: response.order.status,
        orderId: response.order.id,
        bookAcquireLatencyMs: response.order.bookAcquireLatencyMs,
        localMatchLatencyMs: response.order.localMatchLatencyMs,
        persistLatencyMs: response.order.persistLatencyMs,
        totalOrderLatencyMs: response.order.totalOrderLatencyMs
      });
    } catch (error) {
      const finished = Date.now();
      orders.push({
        clientIndex: client.index,
        username: client.username,
        side,
        clientOrderId,
        startedAt: started,
        finishedAt: finished,
        totalMs: finished - started,
        ok: false,
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
        await recordSample("during-orders").catch((error) => {
          console.warn(`sample failed: ${error instanceof Error ? error.message : String(error)}`);
        });
      }
    }
  })();
}

async function recordSample(phase: string) {
  const [metrics, ready, dockerStats] = await Promise.allSettled([
    request<unknown>("/api/metrics"),
    request<unknown>("/api/health/ready"),
    sampleDockerStats()
  ]);
  const sample: Sample = {
    at: new Date().toISOString(),
    phase,
    metrics: metrics.status === "fulfilled" ? metrics.value : { error: String(metrics.reason) },
    ready: ready.status === "fulfilled" ? ready.value : { error: String(ready.reason) },
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

async function cleanupUsersByPrefix(targetPrefix: string) {
  const users = await request<PublicUser[]>("/api/users", { token: adminToken });
  const targets = users.filter((user) => user.username.startsWith(targetPrefix) && user.isActive);
  for (const user of targets) {
    await disableUser(user);
  }
  console.log(`cleanup disabled users: ${targets.length}`);
}

async function disableCreatedUsers() {
  for (const user of createdUsers) {
    await disableUser(user).catch((error) => {
      console.warn(`failed to disable ${user.username}: ${error instanceof Error ? error.message : String(error)}`);
    });
  }
  console.log(`disabled users: ${createdUsers.length}`);
}

async function disableUser(user: PublicUser) {
  await request<PublicUser>(`/api/users/${encodeURIComponent(user.id)}/disable`, {
    method: "POST",
    token: adminToken,
    body: {}
  });
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
      amount,
      orderConcurrency,
      orderSpacingMs,
      sampleIntervalMs,
      skipCleanup,
      sshTarget: sshTarget ? redactSshTarget(sshTarget) : undefined
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
    httpTotalMs: summarizeNumbers(successful.map((order) => order.totalMs)),
    bookAcquireLatencyMs: summarizeNumbers(successful.map((order) => order.bookAcquireLatencyMs)),
    localMatchLatencyMs: summarizeNumbers(successful.map((order) => order.localMatchLatencyMs)),
    persistLatencyMs: summarizeNumbers(successful.map((order) => order.persistLatencyMs)),
    totalOrderLatencyMs: summarizeNumbers(successful.map((order) => order.totalOrderLatencyMs)),
    ws: clientStats(),
    failedReasons: countBy(failed.map((order) => order.error ?? "unknown"))
  };
}

function clientStats() {
  const now = Date.now();
  return {
    openMarketSockets: clients.filter((client) => socketOpen(client.sockets.market)).length,
    openUserSockets: clients.filter((client) => socketOpen(client.sockets.user)).length,
    marketMessages: clients.reduce((sum, client) => sum + client.messages.market, 0),
    userMessages: clients.reduce((sum, client) => sum + client.messages.user, 0),
    unexpectedMarketCloses: clients.reduce((sum, client) => sum + client.unexpectedCloses.market, 0),
    unexpectedUserCloses: clients.reduce((sum, client) => sum + client.unexpectedCloses.user, 0),
    longestMarketGapMs: Math.max(0, ...clients.map((client) => gapMs(client, "market", now))),
    longestUserGapMs: Math.max(0, ...clients.map((client) => gapMs(client, "user", now)))
  };
}

function gapMs(client: SimClient, channel: Channel, now: number) {
  const last = client.lastMessageAt[channel];
  return last ? now - last : 0;
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

function countBy(values: string[]) {
  return values.reduce<Record<string, number>>((counts, value) => {
    counts[value] = (counts[value] ?? 0) + 1;
    return counts;
  }, {});
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

function createClient(index: number, id: string, username: string, password: string, token: string): SimClient {
  return {
    id,
    index,
    username,
    password,
    token,
    sockets: {},
    messages: { market: 0, user: 0 },
    opens: { market: 0, user: 0 },
    unexpectedCloses: { market: 0, user: 0 },
    errors: { market: 0, user: 0 },
    firstMessageMs: {},
    lastMessageAt: {},
    intentionalClose: false
  };
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
  return `Burst-${prefix}-${index + 1}!`;
}

function defaultOutputPath() {
  return path.join(process.cwd(), "deploy", `order-burst-${timestampForName()}.json`);
}

function redactSshTarget(target: string) {
  return target.replace(/^([^@]+)@/, "***@");
}

function printHelp() {
  console.log(`multi-user-order-burst

Required:
  LOAD_TEST_ADMIN_USERNAME       Admin username with bulk-create/disable permissions.
  LOAD_TEST_ADMIN_PASSWORD       Admin password.

Common options:
  LOAD_TEST_BASE_URL             Default: http://103.147.13.98:10001
  ORDER_BURST_USERS              Default: 10
  ORDER_BURST_ORDERS_PER_USER    Default: 5
  ORDER_BURST_AMOUNT             Default: 1
  ORDER_BURST_CONCURRENCY        Default: 10
  ORDER_BURST_SPACING_MS         Default: 100
  ORDER_BURST_SAMPLE_MS          Default: 1000
  ORDER_BURST_OUTPUT             Default: deploy/order-burst-<timestamp>.json
  ORDER_BURST_SSH_TARGET         Optional, example: root@103.147.13.98
  ORDER_BURST_SKIP_CLEANUP       Set true to keep test users active.
  ORDER_BURST_CLEANUP_PREFIX     Disable active users whose username starts with this prefix, then exit.

Example:
  $env:LOAD_TEST_ADMIN_USERNAME="admin"; $env:LOAD_TEST_ADMIN_PASSWORD="***"; npx tsx scripts/multi-user-order-burst.ts
`);
}

import { writeFileSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";
import WebSocket from "ws";

type Channel = "market" | "user";

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

interface SimClient {
  id: string;
  index: number;
  username: string;
  password: string;
  token: string;
  sockets: Partial<Record<Channel, WebSocket>>;
  intentionalClose: boolean;
  gapStartedAt?: number;
  messages: Record<Channel, number>;
  opens: Record<Channel, number>;
  unexpectedCloses: Record<Channel, number>;
  errors: Record<Channel, number>;
  reconnects: number;
  longestGapMs: number;
}

interface Sample {
  at: string;
  phase: string;
  onlineClients: number;
  openSockets: number;
  totalMessages: number;
  unexpectedCloses: number;
  longestGapMs: number;
  healthLive: string;
  healthReady: string;
}

const baseUrl = requiredEnv("LOAD_TEST_BASE_URL").replace(/\/+$/, "");
const adminUsername = requiredEnv("LOAD_TEST_ADMIN_USERNAME");
const adminPassword = requiredEnv("LOAD_TEST_ADMIN_PASSWORD");
const prefix = envText("LOAD_TEST_PREFIX", `load_${timestampForName()}`);
const outputPath = process.env.LOAD_TEST_OUTPUT?.trim();
const onlineClientCount = envInt("LOAD_TEST_ONLINE_CLIENTS", 30);
const onlineDurationMs = envInt("LOAD_TEST_ONLINE_MINUTES", 30) * 60_000;
const reconnectClientCount = envInt("LOAD_TEST_RECONNECT_CLIENTS", 50);
const reconnectRounds = envInt("LOAD_TEST_RECONNECT_ROUNDS", 5);
const reconnectHoldMs = envInt("LOAD_TEST_RECONNECT_HOLD_MS", 15_000);
const reconnectMinPauseMs = envInt("LOAD_TEST_RECONNECT_MIN_PAUSE_MS", 2_000);
const reconnectMaxPauseMs = envInt("LOAD_TEST_RECONNECT_MAX_PAUSE_MS", 5_000);
const loginBatchSize = envInt("LOAD_TEST_LOGIN_BATCH_SIZE", 20);
const loginBatchPauseMs = envInt("LOAD_TEST_LOGIN_BATCH_PAUSE_MS", 65_000);
const sampleIntervalMs = envInt("LOAD_TEST_SAMPLE_INTERVAL_MS", 60_000);
const connectConcurrency = envInt("LOAD_TEST_CONNECT_CONCURRENCY", 10);
const firstMessageTimeoutMs = envInt("LOAD_TEST_FIRST_MESSAGE_TIMEOUT_MS", 10_000);
const maxAllowedGapMs = envInt("LOAD_TEST_MAX_GAP_MS", 10_000);
const skipCleanup = process.env.LOAD_TEST_SKIP_CLEANUP === "true";

const createdUsers: PublicUser[] = [];
const samples: Sample[] = [];
const intentionallyClosingSockets = new WeakSet<WebSocket>();
const startedAt = new Date();
let adminToken = "";
let cancelled = false;
let failureMessage: string | undefined;

process.on("SIGINT", () => {
  cancelled = true;
  console.warn("SIGINT received; finishing cleanup before exit.");
});

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

async function main() {
  const clients: SimClient[] = [];
  let passed = false;
  try {
    await assertHealth("precheck");
    adminToken = (await login(adminUsername, adminPassword)).token;
    await createTestUsers();
    const credentials = createdUsers.map((user, index) => ({
      id: user.id,
      index,
      username: user.username,
      password: passwordFor(index)
    }));
    clients.push(...(await loginClients(credentials)));

    await runOnlineSoak(clients.slice(0, onlineClientCount));
    await runReconnectTest(clients.slice(0, reconnectClientCount));
    await assertHealth("post-test");
    passed = true;
  } catch (error) {
    failureMessage = error instanceof Error ? error.stack ?? error.message : String(error);
    console.error(failureMessage);
  } finally {
    closeAll(clients, true, true);
    if (!skipCleanup && adminToken) {
      await disableCreatedUsers();
    }
    writeResult(passed, clients);
  }

  if (!passed) {
    process.exitCode = 1;
  }
}

async function createTestUsers() {
  const total = Math.max(onlineClientCount, reconnectClientCount);
  const users = Array.from({ length: total }, (_, index) => ({
    username: `${prefix}_${String(index + 1).padStart(3, "0")}`,
    password: passwordFor(index),
    displayName: `Load Test ${index + 1}`,
    role: "Tester",
    language: "zh-CN",
    permissionLevel: "Standard",
    mustChangePassword: false,
    availableUsdc: 0
  }));
  const result = await request<BulkCreateResponse>("/api/users/bulk", {
    method: "POST",
    token: adminToken,
    body: { users }
  });
  if (result.failed.length > 0 || result.created.length !== total) {
    throw new Error(`Bulk create failed: created=${result.created.length}, failed=${JSON.stringify(result.failed)}`);
  }
  createdUsers.push(...result.created.map((item) => item.user));
  console.log(`created test users: ${createdUsers.length}`);
}

async function loginClients(credentials: Array<{ id: string; index: number; username: string; password: string }>) {
  const clients: SimClient[] = [];
  for (let offset = 0; offset < credentials.length; offset += loginBatchSize) {
    const batch = credentials.slice(offset, offset + loginBatchSize);
    const loggedIn = await Promise.all(
      batch.map(async (credential) => {
        const response = await login(credential.username, credential.password);
        return createClient(credential.index, response.id, credential.username, credential.password, response.token);
      })
    );
    clients.push(...loggedIn);
    console.log(`logged in ${clients.length}/${credentials.length} test clients`);
    if (offset + loginBatchSize < credentials.length) {
      await sleep(loginBatchPauseMs);
    }
  }
  return clients;
}

async function runOnlineSoak(clients: SimClient[]) {
  console.log(`online soak start: clients=${clients.length}, durationMs=${onlineDurationMs}`);
  await connectAll(clients);
  const endAt = Date.now() + onlineDurationMs;
  let nextSampleAt = Date.now();
  while (Date.now() < endAt && !cancelled) {
    await Promise.allSettled(clients.map((client) => ensureConnected(client)));
    if (Date.now() >= nextSampleAt) {
      await recordSample("online-soak", clients);
      nextSampleAt += sampleIntervalMs;
    }
    await sleep(2_000);
  }
  await recordSample("online-soak-final", clients);
  assertClientsOpen(clients, "online soak final");
  assertClientsReceivedMessages(clients, "online soak final");
  assertNoLongGap(clients, "online soak");
}

async function runReconnectTest(clients: SimClient[]) {
  console.log(`reconnect test start: clients=${clients.length}, rounds=${reconnectRounds}`);
  for (let round = 1; round <= reconnectRounds && !cancelled; round += 1) {
    await connectAll(clients);
    await sleep(reconnectHoldMs);
    await recordSample(`reconnect-round-${round}-connected`, clients);
    assertClientsOpen(clients, `reconnect round ${round}`);
    closeAll(clients, true);
    await sleep(randomInt(reconnectMinPauseMs, reconnectMaxPauseMs));
  }
  await connectAll(clients);
  await recordSample("reconnect-final", clients);
  assertClientsOpen(clients, "reconnect final");
  assertClientsReceivedMessages(clients, "reconnect final");
  assertNoLongGap(clients, "reconnect test");
}

async function connectAll(clients: SimClient[]) {
  await mapLimit(clients, connectConcurrency, async (client) => ensureConnected(client));
}

async function ensureConnected(client: SimClient) {
  if (pairOpen(client)) {
    return;
  }
  client.reconnects += client.opens.market + client.opens.user > 0 ? 1 : 0;
  closeClient(client, false);
  await connectPair(client);
  if (client.gapStartedAt) {
    client.longestGapMs = Math.max(client.longestGapMs, Date.now() - client.gapStartedAt);
    client.gapStartedAt = undefined;
  }
}

async function connectPair(client: SimClient) {
  client.intentionalClose = false;
  const [market, user] = await Promise.all([connectChannel(client, "market"), connectChannel(client, "user")]);
  client.sockets.market = market;
  client.sockets.user = user;
}

async function connectChannel(client: SimClient, channel: Channel) {
  const ticket = await request<{ ticket: string }>(`/api/ws/tickets`, {
    method: "POST",
    token: client.token,
    body: { channel }
  });
  const url = `${baseUrl.replace("http://", "ws://").replace("https://", "wss://")}/ws/${channel}?ticket=${encodeURIComponent(ticket.ticket)}`;
  return await new Promise<WebSocket>((resolve, reject) => {
    const socket = new WebSocket(url);
    let settled = false;
    const timer = setTimeout(() => {
      if (!settled) {
        settled = true;
        socket.close();
        reject(new Error(`${client.username} ${channel} did not open in ${firstMessageTimeoutMs}ms`));
      }
    }, firstMessageTimeoutMs);

    socket.on("open", () => {
      client.opens[channel] += 1;
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        resolve(socket);
      }
    });
    socket.on("message", () => {
      client.messages[channel] += 1;
    });
    socket.on("close", () => {
      const intentionallyClosing = client.intentionalClose || intentionallyClosingSockets.has(socket);
      intentionallyClosingSockets.delete(socket);
      if (!intentionallyClosing) {
        client.unexpectedCloses[channel] += 1;
        client.gapStartedAt ??= Date.now();
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

async function recordSample(phase: string, clients: SimClient[]) {
  const [live, ready] = await Promise.allSettled([fetchText("/api/health/live"), fetchText("/api/health/ready")]);
  const sample: Sample = {
    at: new Date().toISOString(),
    phase,
    onlineClients: clients.filter(pairOpen).length,
    openSockets: clients.reduce((sum, client) => sum + socketOpen(client.sockets.market) + socketOpen(client.sockets.user), 0),
    totalMessages: clients.reduce((sum, client) => sum + client.messages.market + client.messages.user, 0),
    unexpectedCloses: clients.reduce((sum, client) => sum + client.unexpectedCloses.market + client.unexpectedCloses.user, 0),
    longestGapMs: Math.max(0, ...clients.map((client) => currentLongestGap(client))),
    healthLive: live.status === "fulfilled" ? "ok" : `failed: ${String(live.reason)}`,
    healthReady: ready.status === "fulfilled" ? "ok" : `failed: ${String(ready.reason)}`
  };
  samples.push(sample);
  console.log(JSON.stringify(sample));
}

async function assertHealth(label: string) {
  await fetchText("/api/health/live");
  await fetchText("/api/health/ready");
  console.log(`health ${label}: ok`);
}

function assertClientsOpen(clients: SimClient[], label: string) {
  const online = clients.filter(pairOpen).length;
  if (online !== clients.length) {
    throw new Error(`${label}: expected ${clients.length} online clients, got ${online}`);
  }
}

function assertNoLongGap(clients: SimClient[], label: string) {
  const longestGapMs = Math.max(0, ...clients.map((client) => currentLongestGap(client)));
  if (longestGapMs > maxAllowedGapMs) {
    throw new Error(`${label}: longest connection gap ${longestGapMs}ms exceeded ${maxAllowedGapMs}ms`);
  }
}

function assertClientsReceivedMessages(clients: SimClient[], label: string) {
  const missing = clients.filter((client) => client.messages.market === 0 || client.messages.user === 0);
  if (missing.length > 0) {
    throw new Error(`${label}: ${missing.length} clients did not receive both market and user messages`);
  }
}

async function disableCreatedUsers() {
  for (const user of createdUsers) {
    try {
      await request<PublicUser>(`/api/users/${encodeURIComponent(user.id)}/disable`, {
        method: "POST",
        token: adminToken,
        body: {}
      });
    } catch (error) {
      console.warn(`failed to disable ${user.username}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
  console.log(`disabled test users: ${createdUsers.length}`);
}

function writeResult(passed: boolean, clients: SimClient[]) {
  const result = {
    passed,
    startedAt: startedAt.toISOString(),
    finishedAt: new Date().toISOString(),
    baseUrl,
    prefix,
    createdUsers: createdUsers.length,
    disabledUsers: skipCleanup ? 0 : createdUsers.length,
    failure: failureMessage,
    onlineClientCount,
    onlineDurationMs,
    reconnectClientCount,
    reconnectRounds,
    summary: summarizeClients(clients),
    samples
  };
  const text = `${JSON.stringify(result, null, 2)}\n`;
  if (outputPath) {
    writeFileSync(outputPath, text, "utf8");
  }
  console.log(text);
}

function summarizeClients(clients: SimClient[]) {
  return {
    finalOnlineClients: clients.filter(pairOpen).length,
    finalOpenSockets: clients.reduce((sum, client) => sum + socketOpen(client.sockets.market) + socketOpen(client.sockets.user), 0),
    totalMessages: clients.reduce((sum, client) => sum + client.messages.market + client.messages.user, 0),
    totalUnexpectedCloses: clients.reduce((sum, client) => sum + client.unexpectedCloses.market + client.unexpectedCloses.user, 0),
    totalErrors: clients.reduce((sum, client) => sum + client.errors.market + client.errors.user, 0),
    totalReconnects: clients.reduce((sum, client) => sum + client.reconnects, 0),
    longestGapMs: Math.max(0, ...clients.map((client) => currentLongestGap(client)))
  };
}

async function login(username: string, password: string) {
  return await request<LoginResponse>("/api/auth/login", {
    method: "POST",
    body: { username, password }
  });
}

async function request<T>(path: string, options: { method?: string; token?: string; body?: unknown } = {}) {
  const response = await fetch(`${baseUrl}${path}`, {
    method: options.method ?? "GET",
    headers: {
      "content-type": "application/json",
      ...(options.token ? { authorization: `Bearer ${options.token}` } : {})
    },
    body: options.body === undefined ? undefined : JSON.stringify(options.body)
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${options.method ?? "GET"} ${path} failed ${response.status}: ${text}`);
  }
  return (await response.json()) as T;
}

async function fetchText(path: string) {
  const response = await fetch(`${baseUrl}${path}`);
  if (!response.ok) {
    throw new Error(`${path} failed ${response.status}: ${await response.text()}`);
  }
  return await response.text();
}

async function mapLimit<T>(items: T[], limit: number, worker: (item: T) => Promise<void>) {
  let cursor = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (cursor < items.length) {
      const item = items[cursor++];
      if (item) {
        await worker(item);
      }
    }
  });
  await Promise.all(workers);
}

function createClient(index: number, id: string, username: string, password: string, token: string): SimClient {
  return {
    id,
    index,
    username,
    password,
    token,
    sockets: {},
    intentionalClose: false,
    messages: { market: 0, user: 0 },
    opens: { market: 0, user: 0 },
    unexpectedCloses: { market: 0, user: 0 },
    errors: { market: 0, user: 0 },
    reconnects: 0,
    longestGapMs: 0
  };
}

function closeAll(clients: SimClient[], intentional: boolean, terminate = false) {
  clients.forEach((client) => closeClient(client, intentional, terminate));
}

function closeClient(client: SimClient, intentional: boolean, terminate = false) {
  client.intentionalClose = intentional;
  for (const socket of [client.sockets.market, client.sockets.user]) {
    if (!socket) {
      continue;
    }
    if (intentional) {
      intentionallyClosingSockets.add(socket);
    }
    if (terminate) {
      socket.terminate();
    } else {
      socket.close();
    }
  }
  client.sockets = {};
}

function pairOpen(client: SimClient) {
  return Boolean(socketOpen(client.sockets.market) && socketOpen(client.sockets.user));
}

function socketOpen(socket: WebSocket | undefined) {
  return socket?.readyState === WebSocket.OPEN ? 1 : 0;
}

function currentLongestGap(client: SimClient) {
  return Math.max(client.longestGapMs, client.gapStartedAt ? Date.now() - client.gapStartedAt : 0);
}

function passwordFor(index: number) {
  return `${prefix}_Pwd_${String(index + 1).padStart(3, "0")}_A9!`;
}

function requiredEnv(name: string) {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required.`);
  }
  return value;
}

function envText(name: string, fallback: string) {
  return process.env[name]?.trim() || fallback;
}

function envInt(name: string, fallback: number) {
  const value = Number(process.env[name]);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

function timestampForName() {
  return new Date().toISOString().replace(/\D/g, "").slice(0, 14);
}

function randomInt(min: number, max: number) {
  return min + Math.floor(Math.random() * (max - min + 1));
}

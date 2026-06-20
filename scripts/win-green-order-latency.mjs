#!/usr/bin/env node
import crypto from "node:crypto";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import tls from "node:tls";
import { setTimeout as sleep } from "node:timers/promises";

const DEFAULT_BASE_URL = "http://103.147.13.98:10002";
const baseUrl = envText("BASE_URL", DEFAULT_BASE_URL).replace(/\/+$/, "");
const adminUsername = process.env.GREEN_ADMIN_USERNAME?.trim() ?? "";
const adminPassword = process.env.GREEN_ADMIN_PASSWORD?.trim() ?? "";
const usersCount = envInt("USERS", 5);
const ordersPerUser = envInt("ORDERS_PER_USER", 5);
const concurrency = envInt("CONCURRENCY", 3);
const amount = envNumber("AMOUNT", 1);
const requestTimeoutMs = envInt("REQUEST_TIMEOUT_MS", 20_000);
const wsTimeoutMs = envInt("WS_TIMEOUT_MS", 15_000);
const orderSpacingMs = envInt("ORDER_SPACING_MS", 150);
const outputPath = process.env.OUTPUT_PATH?.trim() || defaultOutputPath();
const skipCleanup = process.env.SKIP_CLEANUP === "true";
const allowNonGreenBaseUrl = process.env.ALLOW_NON_GREEN_BASE_URL === "true";
const prefix = envText("PREFIX", `win_green_latency_${timestampForName()}`);
const runStartedAt = new Date();

const createdUsers = [];
const clients = [];
const orders = [];
const pendingProbes = new Map();
let adminToken = "";
let cancelled = false;
let failureMessage;

if (process.argv.includes("--help")) {
  printHelp();
  process.exit(0);
}

process.on("SIGINT", () => {
  cancelled = true;
  console.warn("SIGINT received; cleaning up created users and sockets.");
});

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

async function main() {
  validateEnvironment();
  let passed = false;
  try {
    adminToken = (await login(adminUsername, adminPassword)).token;
    await assertReady("precheck");
    await assertCurrentRoundAcceptsOrders();
    await createTestUsers();
    await loginClients();
    await connectUserSockets();
    await runOrders();
    await waitForWsSettle();
    await assertReady("postcheck");
    const expectedOrders = usersCount * ordersPerUser;
    passed =
      orders.length === expectedOrders &&
      orders.every((order) => order.ok && typeof order.userWsAfterFinishMs === "number");
  } catch (error) {
    failureMessage = error instanceof Error ? error.stack ?? error.message : String(error);
    console.error(failureMessage);
  } finally {
    cancelled = true;
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

function validateEnvironment() {
  if (typeof fetch !== "function") {
    throw new Error("Node.js 18+ is required because this script uses built-in fetch.");
  }
  if (!adminUsername || !adminPassword) {
    throw new Error("Set GREEN_ADMIN_USERNAME and GREEN_ADMIN_PASSWORD.");
  }
  const url = new URL(baseUrl);
  if (!allowNonGreenBaseUrl && url.port !== "10002") {
    throw new Error(`Refusing to test non-green BASE_URL=${baseUrl}. Set ALLOW_NON_GREEN_BASE_URL=true to override.`);
  }
  if (!allowNonGreenBaseUrl && baseUrl.includes(":10001")) {
    throw new Error("Refusing to test production port 10001 without ALLOW_NON_GREEN_BASE_URL=true.");
  }
}

async function assertReady(label) {
  const ready = await request("/api/health/ready");
  if (!ready?.ok) {
    throw new Error(`health ${label} returned ok=false`);
  }
  console.log(`health ${label}: ok schema=${ready.schemaMigration ?? "unknown"}`);
}

async function assertCurrentRoundAcceptsOrders() {
  const current = await request("/api/rounds/current", { token: adminToken });
  const currentRound = current?.currentRound;
  if (!currentRound?.id) {
    throw new Error("Current round is missing.");
  }
  if (currentRound.acceptingOrders === false) {
    throw new Error(`Current round ${currentRound.id} is not accepting orders.`);
  }
  console.log(`current round accepts orders: ${currentRound.id} status=${currentRound.status ?? "unknown"}`);
}

async function createTestUsers() {
  const balance = Math.max(20, amount * ordersPerUser + 10);
  const users = Array.from({ length: usersCount }, (_, index) => ({
    username: `${prefix}_${String(index + 1).padStart(3, "0")}`,
    password: passwordFor(index),
    displayName: `Win Green Latency ${index + 1}`,
    role: "Tester",
    language: "zh-CN",
    permissionLevel: "Standard",
    mustChangePassword: false,
    availableUsdc: balance
  }));
  const result = await request("/api/users/bulk", {
    method: "POST",
    token: adminToken,
    body: { users }
  });
  if (!Array.isArray(result?.created) || result.created.length !== usersCount || result.failed?.length > 0) {
    throw new Error(`Bulk create failed: ${JSON.stringify(result)}`);
  }
  createdUsers.push(...result.created.map((item) => item.user));
  console.log(`created test users: ${createdUsers.length}`);
}

async function loginClients() {
  for (const [index, user] of createdUsers.entries()) {
    const loginResult = await login(user.username, passwordFor(index));
    clients.push({
      id: loginResult.id,
      index,
      username: user.username,
      token: loginResult.token,
      socket: undefined,
      messages: 0,
      opened: false,
      intentionalClose: false
    });
  }
  console.log(`logged in test users: ${clients.length}`);
}

async function connectUserSockets() {
  await mapLimit(clients, Math.min(8, clients.length), async (client) => {
    const ticket = await request("/api/ws/tickets", {
      method: "POST",
      token: client.token,
      body: { channel: "user" }
    });
    const wsUrl = createWsUrl("/ws/user", ticket.ticket);
    const socket = await createWebSocket(wsUrl, wsTimeoutMs);
    client.socket = socket;
    client.opened = true;
    socket.onMessage = (message) => handleUserMessage(client, message);
    socket.onClose = () => {
      if (!client.intentionalClose) {
        console.warn(`${client.username} user ws closed unexpectedly`);
      }
    };
  });
  console.log(`connected user websocket clients: ${clients.length}`);
}

function handleUserMessage(client, message) {
  client.messages += 1;
  let parsed;
  try {
    parsed = JSON.parse(message);
  } catch {
    return;
  }
  if (parsed?.type !== "user:trade" && parsed?.type !== "user") {
    return;
  }
  const receivedAt = Date.now();
  const recentOrders = Array.isArray(parsed.data?.orders) ? parsed.data.orders : [];
  for (const order of recentOrders) {
    const clientOrderId = order?.clientOrderId;
    const probe = typeof clientOrderId === "string" ? pendingProbes.get(clientOrderId) : undefined;
    if (!probe) {
      continue;
    }
    probe.firstUserMessageAt ??= receivedAt;
    probe.matchedOrderStatus ??= order.status;
    probe.matchedOrderId ??= order.id;
    if (probe.finishedAt) {
      probe.userWsAfterFinishMs ??= Math.max(receivedAt - probe.finishedAt, 0);
    }
    probe.userWsAfterStartMs ??= Math.max(receivedAt - probe.startedAt, 0);
  }
}

async function runOrders() {
  const tasks = clients.flatMap((client) =>
    Array.from({ length: ordersPerUser }, (_, orderIndex) => ({ client, orderIndex }))
  );
  let sent = 0;
  await mapLimit(tasks, Math.max(1, concurrency), async ({ client, orderIndex }) => {
    const sequence = sent++;
    if (orderSpacingMs > 0) {
      await sleep(sequence * orderSpacingMs);
    }
    if (cancelled) {
      return;
    }
    const side = (client.index + orderIndex) % 2 === 0 ? "UP" : "DOWN";
    const clientOrderId = `${prefix}_${client.index}_${orderIndex}_${Date.now()}_${randomId()}`;
    const startedAt = Date.now();
    const probe = { clientIndex: client.index, startedAt };
    pendingProbes.set(clientOrderId, probe);
    try {
      const response = await request("/api/orders", {
        method: "POST",
        token: client.token,
        body: {
          action: "buy",
          side,
          orderKind: "market",
          amount,
          clientOrderId,
          clientSendTs: startedAt
        }
      });
      const finishedAt = Date.now();
      probe.finishedAt = finishedAt;
      const order = response.order ?? {};
      const attempt = {
        username: client.username,
        side,
        clientOrderId,
        startedAt,
        finishedAt,
        httpTotalMs: finishedAt - startedAt,
        ok: true,
        status: order.status,
        orderId: order.id,
        bookAcquireLatencyMs: order.bookAcquireLatencyMs,
        localMatchLatencyMs: order.localMatchLatencyMs,
        persistLatencyMs: order.persistLatencyMs,
        totalOrderLatencyMs: order.totalOrderLatencyMs,
        userWsAfterStartMs: probe.userWsAfterStartMs,
        userWsAfterFinishMs: probe.userWsAfterFinishMs
      };
      orders.push(attempt);
      printOrderAttempt(attempt);
    } catch (error) {
      const finishedAt = Date.now();
      probe.finishedAt = finishedAt;
      const attempt = {
        username: client.username,
        side,
        clientOrderId,
        startedAt,
        finishedAt,
        httpTotalMs: finishedAt - startedAt,
        ok: false,
        userWsAfterStartMs: probe.userWsAfterStartMs,
        userWsAfterFinishMs: probe.userWsAfterFinishMs,
        error: error instanceof Error ? error.message : String(error)
      };
      orders.push(attempt);
      printOrderAttempt(attempt);
    }
  });
  console.log(`orders attempted: ${orders.length}`);
}

async function waitForWsSettle() {
  const deadline = Date.now() + Math.min(wsTimeoutMs, 10_000);
  while (Date.now() < deadline) {
    let pending = 0;
    for (const order of orders) {
      const probe = pendingProbes.get(order.clientOrderId);
      if (order.ok && probe && probe.userWsAfterFinishMs === undefined) {
        pending += 1;
      }
    }
    if (pending === 0) {
      break;
    }
    await sleep(200);
  }
  for (const order of orders) {
    const probe = pendingProbes.get(order.clientOrderId);
    if (!probe) {
      continue;
    }
    if (probe.firstUserMessageAt && probe.finishedAt && probe.userWsAfterFinishMs === undefined) {
      probe.userWsAfterFinishMs = Math.max(probe.firstUserMessageAt - probe.finishedAt, 0);
    }
    order.userWsAfterStartMs = probe.userWsAfterStartMs;
    order.userWsAfterFinishMs = probe.userWsAfterFinishMs;
    order.wsMatchedOrderStatus = probe.matchedOrderStatus;
    order.wsMatchedOrderId = probe.matchedOrderId;
  }
}

function printOrderAttempt(order) {
  console.log(JSON.stringify({
    username: order.username,
    side: order.side,
    ok: order.ok,
    status: order.status,
    httpTotalMs: order.httpTotalMs,
    persistLatencyMs: order.persistLatencyMs,
    totalOrderLatencyMs: order.totalOrderLatencyMs,
    userWsAfterStartMs: order.userWsAfterStartMs,
    userWsAfterFinishMs: order.userWsAfterFinishMs,
    error: order.error
  }));
}

async function disableCreatedUsers() {
  let disabled = 0;
  for (const user of createdUsers) {
    try {
      await request(`/api/users/${encodeURIComponent(user.id)}/disable`, {
        method: "POST",
        token: adminToken
      });
      disabled += 1;
    } catch (error) {
      console.warn(`failed to disable ${user.username}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
  console.log(`disabled test users: ${disabled}/${createdUsers.length}`);
}

async function login(username, password) {
  return await request("/api/auth/login", {
    method: "POST",
    body: { username, password }
  });
}

async function request(pathname, options = {}) {
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
    return text ? JSON.parse(text) : undefined;
  } finally {
    clearTimeout(timer);
  }
}

async function createWebSocket(wsUrl, timeoutMs) {
  const url = new URL(wsUrl);
  const secure = url.protocol === "wss:";
  const port = Number(url.port || (secure ? 443 : 80));
  const pathAndQuery = `${url.pathname}${url.search}`;
  const key = crypto.randomBytes(16).toString("base64");
  const expectedAccept = crypto
    .createHash("sha1")
    .update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)
    .digest("base64");

  const socket = secure
    ? tls.connect({ host: url.hostname, port, servername: url.hostname })
    : net.connect({ host: url.hostname, port });

  socket.setNoDelay(true);
  return await new Promise((resolve, reject) => {
    let settled = false;
    let handshakeBuffer = Buffer.alloc(0);
    const timer = setTimeout(() => fail(new Error(`WebSocket did not open within ${timeoutMs}ms`)), timeoutMs);

    const fail = (error) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      socket.destroy();
      reject(error);
    };

    const sendHandshake = () => {
      socket.write([
        `GET ${pathAndQuery} HTTP/1.1`,
        `Host: ${url.host}`,
        "Connection: Upgrade",
        "Upgrade: websocket",
        "Sec-WebSocket-Version: 13",
        `Sec-WebSocket-Key: ${key}`,
        "",
        ""
      ].join("\r\n"));
    };

    socket.once("connect", () => {
      if (!secure) {
        sendHandshake();
      }
    });
    socket.once("secureConnect", sendHandshake);
    socket.once("error", fail);
    socket.on("data", (chunk) => {
      if (settled) {
        return;
      }
      handshakeBuffer = Buffer.concat([handshakeBuffer, chunk]);
      const headerEnd = handshakeBuffer.indexOf("\r\n\r\n");
      if (headerEnd < 0) {
        return;
      }
      const headerText = handshakeBuffer.subarray(0, headerEnd).toString("utf8");
      if (!/^HTTP\/1\.1 101\b/.test(headerText) || !headerText.includes(expectedAccept)) {
        fail(new Error(`WebSocket handshake failed: ${headerText.split(/\r?\n/)[0] ?? "unknown"}`));
        return;
      }
      settled = true;
      clearTimeout(timer);
      socket.removeListener("error", fail);
      const client = new MinimalWebSocket(socket);
      const remaining = handshakeBuffer.subarray(headerEnd + 4);
      if (remaining.length > 0) {
        client.acceptData(remaining);
      }
      resolve(client);
    });
  });
}

class MinimalWebSocket {
  constructor(socket) {
    this.socket = socket;
    this.buffer = Buffer.alloc(0);
    this.closed = false;
    this.onMessage = undefined;
    this.onClose = undefined;
    socket.on("data", (chunk) => this.acceptData(chunk));
    socket.on("close", () => {
      this.closed = true;
      this.onClose?.();
    });
    socket.on("error", (error) => {
      this.closed = true;
      console.warn(`websocket socket error: ${error.message}`);
      this.onClose?.();
    });
  }

  acceptData(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    while (this.buffer.length >= 2) {
      const first = this.buffer[0];
      const second = this.buffer[1];
      const opcode = first & 0x0f;
      let offset = 2;
      let payloadLength = second & 0x7f;
      if (payloadLength === 126) {
        if (this.buffer.length < offset + 2) {
          return;
        }
        payloadLength = this.buffer.readUInt16BE(offset);
        offset += 2;
      } else if (payloadLength === 127) {
        if (this.buffer.length < offset + 8) {
          return;
        }
        const bigLength = this.buffer.readBigUInt64BE(offset);
        if (bigLength > BigInt(Number.MAX_SAFE_INTEGER)) {
          this.close();
          return;
        }
        payloadLength = Number(bigLength);
        offset += 8;
      }
      const masked = (second & 0x80) !== 0;
      let mask;
      if (masked) {
        if (this.buffer.length < offset + 4) {
          return;
        }
        mask = this.buffer.subarray(offset, offset + 4);
        offset += 4;
      }
      if (this.buffer.length < offset + payloadLength) {
        return;
      }
      let payload = this.buffer.subarray(offset, offset + payloadLength);
      this.buffer = this.buffer.subarray(offset + payloadLength);
      if (mask) {
        payload = Buffer.from(payload.map((byte, index) => byte ^ mask[index % 4]));
      }
      if (opcode === 0x1) {
        this.onMessage?.(payload.toString("utf8"));
      } else if (opcode === 0x8) {
        this.close();
      } else if (opcode === 0x9) {
        this.sendFrame(0xA, payload);
      }
    }
  }

  close() {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.sendFrame(0x8, Buffer.alloc(0));
    this.socket.end();
  }

  sendFrame(opcode, payload) {
    if (this.socket.destroyed) {
      return;
    }
    const data = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
    const mask = crypto.randomBytes(4);
    const lengthBytes = data.length < 126
      ? Buffer.from([0x80 | opcode, 0x80 | data.length])
      : Buffer.concat([Buffer.from([0x80 | opcode, 0x80 | 126]), uint16(data.length)]);
    const masked = Buffer.from(data.map((byte, index) => byte ^ mask[index % 4]));
    this.socket.write(Buffer.concat([lengthBytes, mask, masked]));
  }
}

function createWsUrl(pathname, ticket) {
  const httpUrl = new URL(baseUrl);
  httpUrl.protocol = httpUrl.protocol === "https:" ? "wss:" : "ws:";
  httpUrl.pathname = pathname;
  httpUrl.search = `?ticket=${encodeURIComponent(ticket)}`;
  return httpUrl.toString();
}

async function mapLimit(items, limit, worker) {
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
    client.socket?.close();
  }
}

function writeResult(passed) {
  const successful = orders.filter((order) => order.ok);
  const failed = orders.filter((order) => !order.ok);
  const summary = {
    passed,
    baseUrl,
    startedAt: runStartedAt.toISOString(),
    usersCount,
    ordersPerUser,
    concurrency,
    amount,
    successCount: successful.length,
    failureCount: failed.length,
    httpTotalMs: summarize(successful.map((order) => order.httpTotalMs)),
    persistLatencyMs: summarize(successful.map((order) => order.persistLatencyMs)),
    totalOrderLatencyMs: summarize(successful.map((order) => order.totalOrderLatencyMs)),
    userWsAfterStartMs: summarize(successful.map((order) => order.userWsAfterStartMs)),
    userWsAfterFinishMs: summarize(successful.map((order) => order.userWsAfterFinishMs)),
    statusCounts: countBy(successful.map((order) => order.status ?? "unknown")),
    failureReasons: countBy(failed.map((order) => order.error ?? "unknown")),
    createdUsers: createdUsers.map((user) => ({ id: user.id, username: user.username })),
    failureMessage
  };
  const result = {
    summary,
    orders
  };
  fs.mkdirSync(path.dirname(path.resolve(outputPath)), { recursive: true });
  fs.writeFileSync(outputPath, `${JSON.stringify(result, null, 2)}\n`, "utf8");
  console.log("summary:");
  console.log(JSON.stringify(summary, null, 2));
  console.log(`result written: ${outputPath}`);
}

function summarize(values) {
  const sorted = values.filter((value) => typeof value === "number" && Number.isFinite(value)).sort((a, b) => a - b);
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

function percentile(sorted, ratio) {
  return sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * ratio) - 1))];
}

function countBy(values) {
  return values.reduce((counts, value) => {
    counts[value] = (counts[value] ?? 0) + 1;
    return counts;
  }, {});
}

function envText(name, fallback) {
  const value = process.env[name]?.trim();
  return value || fallback;
}

function envInt(name, fallback) {
  const value = Number(process.env[name]);
  return Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;
}

function envNumber(name, fallback) {
  const value = Number(process.env[name]);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

function timestampForName() {
  return new Date().toISOString().replace(/[-:]/g, "").replace(/\..+$/, "").replace("T", "_");
}

function randomId() {
  return crypto.randomUUID?.() ?? crypto.randomBytes(8).toString("hex");
}

function passwordFor(index) {
  return `WinGreenLatency-${prefix}-${index + 1}!`;
}

function defaultOutputPath() {
  return path.join(process.cwd(), `green-order-latency-result-${timestampForName()}.json`);
}

function uint16(value) {
  const buffer = Buffer.allocUnsafe(2);
  buffer.writeUInt16BE(value, 0);
  return buffer;
}

function printHelp() {
  console.log(`win-green-order-latency

Required:
  GREEN_ADMIN_USERNAME          Green admin username.
  GREEN_ADMIN_PASSWORD          Green admin password.

Options:
  BASE_URL                      Default: ${DEFAULT_BASE_URL}
  USERS                         Default: 5
  ORDERS_PER_USER               Default: 5
  CONCURRENCY                   Default: 3
  AMOUNT                        Default: 1
  REQUEST_TIMEOUT_MS            Default: 20000
  WS_TIMEOUT_MS                 Default: 15000
  ORDER_SPACING_MS              Default: 150
  OUTPUT_PATH                   Default: green-order-latency-result-<timestamp>.json
  SKIP_CLEANUP                  Default: false
  ALLOW_NON_GREEN_BASE_URL      Default: false

PowerShell example:
  $env:GREEN_ADMIN_USERNAME="admin"
  $env:GREEN_ADMIN_PASSWORD="***"
  node .\\scripts\\win-green-order-latency.mjs
`);
}

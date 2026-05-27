import { writeFileSync } from "node:fs";
import http from "node:http";
import https from "node:https";
import { setTimeout as sleep } from "node:timers/promises";
import WebSocket from "ws";

interface LoginResponse {
  token: string;
}

interface WsTicketResponse {
  ticket: string;
}

interface TransportMeta {
  serverPublishTs?: number;
  payloadSeq?: number;
  serverQueueMs?: number;
  wsSendStartTs?: number;
  broadcastBuildMs?: number;
  broadcastFanoutSize?: number;
  droppedForBackpressure?: boolean;
}

interface SourceHealth {
  source: "Binance" | "Coinbase" | "CLOB";
  state: string;
  sourceEventTs: number;
  serverRecvTs: number;
  normalizedTs: number;
  serverPublishTs: number;
}

interface TickLike {
  sources?: Record<"binance" | "coinbase" | "clob", SourceHealth>;
}

type ClockOffsetSample = {
  rttMs: number;
  offsetMs: number;
  serverNow: number;
};

type ClockOffsetResult = {
  clockOffsetMs: number;
  samples: ClockOffsetSample[];
};

interface MarketMessage {
  type: "market" | "market:tick";
  data: {
    tick?: TickLike;
    snapshot?: TickLike;
    transportMeta?: TransportMeta;
  };
}

type ClientStats = {
  id: number;
  connected: boolean;
  tickCount: number;
  fullCount: number;
  outOfOrder: number;
  firstPayloadSeq: number;
  lastPayloadSeq: number;
  lastTickAt: number;
  tickIntervals: number[];
  rawTickServerToClient: number[];
  tickServerToClient: number[];
  wsSendStartToClient: number[];
  serverQueue: number[];
  broadcastBuild: number[];
  fanout: number[];
  clobSourceAge: number[];
  payloadBytes: number[];
  pausedRead: boolean;
  droppedFrames: number;
  errors: string[];
};

const baseUrl = requiredEnv("LOAD_TEST_BASE_URL").replace(/\/+$/, "");
const clientCount = envInt("LOAD_TEST_MARKET_CLIENTS", 50);
const durationMs = envInt("LOAD_TEST_SAMPLE_MS", 30 * 60_000);
const connectConcurrency = envInt("LOAD_TEST_CONNECT_CONCURRENCY", 5);
const pausedClientCount = Math.min(envInt("LOAD_TEST_PAUSED_CLIENTS", 0), clientCount);
const pauseAfterMs = envInt("LOAD_TEST_PAUSE_AFTER_MS", 5000);
const clockSampleCount = envInt("LOAD_TEST_CLOCK_SAMPLES", 7);
const outputPath = process.env.LOAD_TEST_OUTPUT?.trim();
const tokenFromEnv = process.env.LOAD_TEST_TOKEN?.trim();
const username = process.env.LOAD_TEST_USERNAME?.trim();
const password = process.env.LOAD_TEST_PASSWORD?.trim();
const localAddress = process.env.LOAD_TEST_LOCAL_ADDRESS?.trim();

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});

async function main() {
  const clock = await sampleClockOffset();
  const token = tokenFromEnv ?? (await login());
  const startedAt = Date.now();
  const stats = Array.from({ length: clientCount }, (_, id) => createStats(id + 1));
  const sockets: WebSocket[] = [];

  await runWithConcurrency(stats, connectConcurrency, async (stat) => {
    const socket = await connectClient(stat, token, clock.clockOffsetMs);
    sockets.push(socket);
  });

  await sleep(durationMs);
  for (const socket of sockets) {
    resumeSocketReads(socket);
    socket.close();
  }
  await sleep(250);

  const result = buildResult(startedAt, Date.now(), stats, clock);
  const text = JSON.stringify(result, null, 2);
  if (outputPath) {
    writeFileSync(outputPath, `${text}\n`, "utf8");
  }
  console.log(text);
}

function createStats(id: number): ClientStats {
  return {
    id,
    connected: false,
    tickCount: 0,
    fullCount: 0,
    outOfOrder: 0,
    firstPayloadSeq: 0,
    lastPayloadSeq: 0,
    lastTickAt: 0,
    tickIntervals: [],
    rawTickServerToClient: [],
    tickServerToClient: [],
    wsSendStartToClient: [],
    serverQueue: [],
    broadcastBuild: [],
    fanout: [],
    clobSourceAge: [],
    payloadBytes: [],
    pausedRead: id <= pausedClientCount,
    droppedFrames: 0,
    errors: []
  };
}

async function connectClient(stat: ClientStats, token: string, clockOffsetMs: number) {
  const ticket = await createTicket(token);
  const wsUrl = `${baseUrl.replace("http://", "ws://").replace("https://", "wss://")}/ws/market?ticket=${encodeURIComponent(ticket)}`;
  const socket = new WebSocket(wsUrl, localAddress ? { localAddress } : undefined);
  socket.on("message", (raw) => recordMessage(stat, raw, Date.now(), clockOffsetMs));
  socket.on("error", (error) => stat.errors.push(error.message));
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error(`Client ${stat.id} timed out waiting for WebSocket open.`)), 10_000);
    socket.once("open", () => {
      stat.connected = true;
      clearTimeout(timeout);
      if (stat.pausedRead) {
        setTimeout(() => pauseSocketReads(socket), pauseAfterMs).unref();
      }
      resolve();
    });
    socket.once("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });
  });
  return socket;
}

function recordMessage(stat: ClientStats, raw: WebSocket.RawData, receivedAt: number, clockOffsetMs: number) {
  const payloadBytes = rawDataByteLength(raw);
  let parsed: MarketMessage | undefined;
  try {
    parsed = JSON.parse(rawDataText(raw)) as MarketMessage;
  } catch {
    return;
  }
  if (parsed.type !== "market" && parsed.type !== "market:tick") {
    return;
  }
  stat.payloadBytes.push(payloadBytes);
  if (parsed.type === "market:tick") {
    if (stat.lastTickAt > 0) {
      stat.tickIntervals.push(receivedAt - stat.lastTickAt);
    }
    stat.lastTickAt = receivedAt;
    stat.tickCount += 1;
  } else {
    stat.fullCount += 1;
  }
  const transportMeta = parsed.data.transportMeta;
  if (!transportMeta) {
    return;
  }
  if (transportMeta.payloadSeq) {
    if (!stat.firstPayloadSeq) {
      stat.firstPayloadSeq = transportMeta.payloadSeq;
    }
    if (stat.lastPayloadSeq && transportMeta.payloadSeq <= stat.lastPayloadSeq) {
      stat.outOfOrder += 1;
    }
    stat.lastPayloadSeq = transportMeta.payloadSeq;
  }
  if (parsed.type === "market:tick" && transportMeta.serverPublishTs) {
    stat.rawTickServerToClient.push(receivedAt - transportMeta.serverPublishTs);
    stat.tickServerToClient.push(Math.max(receivedAt - transportMeta.serverPublishTs - clockOffsetMs, 0));
  }
  if (parsed.type === "market:tick" && transportMeta.wsSendStartTs) {
    stat.wsSendStartToClient.push(Math.max(receivedAt - transportMeta.wsSendStartTs - clockOffsetMs, 0));
  }
  if (typeof transportMeta.serverQueueMs === "number") {
    stat.serverQueue.push(transportMeta.serverQueueMs);
  }
  if (typeof transportMeta.broadcastBuildMs === "number") {
    stat.broadcastBuild.push(transportMeta.broadcastBuildMs);
  }
  if (typeof transportMeta.broadcastFanoutSize === "number") {
    stat.fanout.push(transportMeta.broadcastFanoutSize);
  }
  if (transportMeta.droppedForBackpressure) {
    stat.droppedFrames += 1;
  }
  const data = parsed.data.tick ?? parsed.data.snapshot;
  const clobSource = data?.sources?.clob;
  if (clobSource) {
    stat.clobSourceAge.push(Math.max(receivedAt - clobSource.normalizedTs - clockOffsetMs, 0));
  }
}

function buildResult(startedAt: number, finishedAt: number, stats: ClientStats[], clock: ClockOffsetResult) {
  const activeStats = stats.filter((stat) => !stat.pausedRead);
  const tickIntervals = stats.flatMap((stat) => stat.tickIntervals);
  const rawTickServerToClient = stats.flatMap((stat) => stat.rawTickServerToClient);
  const tickServerToClient = stats.flatMap((stat) => stat.tickServerToClient);
  const activeTickServerToClient = activeStats.flatMap((stat) => stat.tickServerToClient);
  const wsSendStartToClient = stats.flatMap((stat) => stat.wsSendStartToClient);
  const serverQueue = stats.flatMap((stat) => stat.serverQueue);
  const broadcastBuild = stats.flatMap((stat) => stat.broadcastBuild);
  const fanout = stats.flatMap((stat) => stat.fanout);
  const clobSourceAge = stats.flatMap((stat) => stat.clobSourceAge);
  const activeClobSourceAge = activeStats.flatMap((stat) => stat.clobSourceAge);
  const payloadBytes = stats.flatMap((stat) => stat.payloadBytes);
  const healthRtts = clock.samples.map((sample) => sample.rttMs);
  const clientSummaries = stats.map((stat) => ({
    id: stat.id,
    connected: stat.connected,
    pausedRead: stat.pausedRead,
    tickCount: stat.tickCount,
    fullCount: stat.fullCount,
    outOfOrder: stat.outOfOrder,
    tickIntervalP95: percentile(stat.tickIntervals, 95),
    rawTickServerToClientP95: percentile(stat.rawTickServerToClient, 95),
    tickServerToClientP95: percentile(stat.tickServerToClient, 95),
    tickServerToClientP99: percentile(stat.tickServerToClient, 99),
    wsSendStartToClientP95: percentile(stat.wsSendStartToClient, 95),
    payloadBytesP95: percentile(stat.payloadBytes, 95),
    serverQueueP95: percentile(stat.serverQueue, 95),
    clobSourceAgeP95: percentile(stat.clobSourceAge, 95),
    clobSourceAgeP99: percentile(stat.clobSourceAge, 99),
    droppedFrames: stat.droppedFrames,
    errors: stat.errors
  }));
  return {
    startedAt: new Date(startedAt).toISOString(),
    finishedAt: new Date(finishedAt).toISOString(),
    durationMs: finishedAt - startedAt,
    requestedClients: clientCount,
    connectedClients: stats.filter((stat) => stat.connected).length,
    pausedClients: stats.filter((stat) => stat.pausedRead).length,
    totalTicks: stats.reduce((sum, stat) => sum + stat.tickCount, 0),
    totalFullSnapshots: stats.reduce((sum, stat) => sum + stat.fullCount, 0),
    outOfOrder: stats.reduce((sum, stat) => sum + stat.outOfOrder, 0),
    clockOffsetMs: clock.clockOffsetMs,
    healthRttP50: percentile(healthRtts, 50),
    healthRttP95: percentile(healthRtts, 95),
    clockOffsetSamples: clock.samples,
    tickIntervalP95: percentile(tickIntervals, 95),
    rawTickServerToClientP95: percentile(rawTickServerToClient, 95),
    rawTickServerToClientP99: percentile(rawTickServerToClient, 99),
    tickServerToClientP95: percentile(tickServerToClient, 95),
    tickServerToClientP99: percentile(tickServerToClient, 99),
    activeClientTickServerToClientP95: percentile(activeTickServerToClient, 95),
    activeClientTickServerToClientP99: percentile(activeTickServerToClient, 99),
    wsSendStartToClientP95: percentile(wsSendStartToClient, 95),
    wsSendStartToClientP99: percentile(wsSendStartToClient, 99),
    serverQueueP95: percentile(serverQueue, 95),
    broadcastBuildP95: percentile(broadcastBuild, 95),
    broadcastFanoutP50: percentile(fanout, 50),
    clobSourceAgeP50: percentile(clobSourceAge, 50),
    clobSourceAgeP95: percentile(clobSourceAge, 95),
    clobSourceAgeP99: percentile(clobSourceAge, 99),
    clobSourceAgeMax: max(clobSourceAge),
    activeClientClobSourceAgeP99: percentile(activeClobSourceAge, 99),
    payloadBytesP95: percentile(payloadBytes, 95),
    droppedFrameSignals: stats.reduce((sum, stat) => sum + stat.droppedFrames, 0),
    worstClientTickServerToClientP99: max(clientSummaries.map((stat) => stat.tickServerToClientP99)),
    clients: clientSummaries
  };
}

function rawDataText(raw: WebSocket.RawData) {
  if (typeof raw === "string") {
    return raw;
  }
  if (Array.isArray(raw)) {
    return Buffer.concat(raw).toString("utf8");
  }
  return Buffer.from(raw).toString("utf8");
}

function rawDataByteLength(raw: WebSocket.RawData) {
  if (typeof raw === "string") {
    return Buffer.byteLength(raw);
  }
  if (Array.isArray(raw)) {
    return raw.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  }
  return Buffer.from(raw).byteLength;
}

function pauseSocketReads(socket: WebSocket) {
  const rawSocket = (socket as unknown as { _socket?: { pause?: () => void } })._socket;
  rawSocket?.pause?.();
}

function resumeSocketReads(socket: WebSocket) {
  const rawSocket = (socket as unknown as { _socket?: { resume?: () => void } })._socket;
  rawSocket?.resume?.();
}

async function runWithConcurrency<T>(items: T[], concurrency: number, worker: (item: T) => Promise<void>) {
  let index = 0;
  const workers = Array.from({ length: Math.max(concurrency, 1) }, async () => {
    while (index < items.length) {
      const item = items[index++];
      if (item) {
        await worker(item);
      }
    }
  });
  await Promise.all(workers);
}

async function sampleClockOffset(): Promise<ClockOffsetResult> {
  const samples: ClockOffsetSample[] = [];
  for (let index = 0; index < clockSampleCount; index += 1) {
    const sample = await sampleClockOffsetOnce();
    if (sample) {
      samples.push(sample);
    }
    if (index < clockSampleCount - 1) {
      await sleep(100);
    }
  }
  const bestSample = [...samples].sort((left, right) => left.rttMs - right.rttMs)[0];
  return {
    clockOffsetMs: bestSample?.offsetMs ?? 0,
    samples
  };
}

async function sampleClockOffsetOnce(): Promise<ClockOffsetSample | undefined> {
  try {
    const startedAt = Date.now();
    const data = await request<{ serverNow?: number }>("/health", undefined, undefined, 3000);
    const receivedAt = Date.now();
    if (typeof data.serverNow !== "number") {
      return undefined;
    }
    return {
      rttMs: receivedAt - startedAt,
      offsetMs: Math.round((startedAt + receivedAt) / 2 - data.serverNow),
      serverNow: data.serverNow
    };
  } catch {
    return undefined;
  }
}

async function login() {
  if (!username || !password) {
    throw new Error("Set LOAD_TEST_TOKEN or LOAD_TEST_USERNAME/LOAD_TEST_PASSWORD.");
  }
  const response = await request<LoginResponse>("/api/auth/login", undefined, {
    method: "POST",
    body: JSON.stringify({ username, password })
  });
  return response.token;
}

async function createTicket(token: string) {
  const response = await request<WsTicketResponse>("/api/ws/tickets", token, {
    method: "POST",
    body: JSON.stringify({ channel: "market" })
  });
  return response.ticket;
}

async function request<T>(path: string, token?: string, init?: RequestInit, timeoutMs = 10_000): Promise<T> {
  const url = new URL(`${baseUrl}${path}`);
  const body = typeof init?.body === "string" ? init.body : undefined;
  const isHttps = url.protocol === "https:";
  const headers: Record<string, string> = {
    "content-type": "application/json",
    ...(token ? { authorization: `Bearer ${token}` } : {})
  };
  return new Promise<T>((resolve, reject) => {
    const request = (isHttps ? https : http).request(
      url,
      {
        method: init?.method ?? "GET",
        headers,
        localAddress,
        timeout: timeoutMs
      },
      (response) => {
        let text = "";
        response.setEncoding("utf8");
        response.on("data", (chunk) => {
          text += chunk;
        });
        response.on("end", () => {
          if ((response.statusCode ?? 500) >= 400) {
            reject(new Error(`HTTP ${response.statusCode}: ${text}`));
            return;
          }
          try {
            resolve(JSON.parse(text) as T);
          } catch (error) {
            reject(error);
          }
        });
      }
    );
    request.setTimeout(timeoutMs, () => {
      request.destroy(new Error(`Request timed out after ${timeoutMs}ms.`));
    });
    request.on("error", reject);
    if (body) {
      request.write(body);
    }
    request.end();
  });
}

function percentile(values: number[], p: number) {
  if (values.length === 0) {
    return 0;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return Math.round(sorted[index] ?? 0);
}

function max(values: number[]) {
  return values.length > 0 ? Math.max(...values.map((value) => Math.round(value))) : 0;
}

function envInt(name: string, fallback: number) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function requiredEnv(name: string) {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`Missing ${name}.`);
  }
  return value;
}

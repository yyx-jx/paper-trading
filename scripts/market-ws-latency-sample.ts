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
  coalescedCount?: number;
  serverQueueMs?: number;
  snapshotBuildTs?: number;
  wsSendStartTs?: number;
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
  latencyBreakdown?: {
    sourceEventAge?: Record<"binance" | "coinbase" | "clob", number>;
    serverIngressLatency?: Record<"binance" | "coinbase" | "clob", number>;
  };
}

interface MarketMessage {
  type: "market" | "market:tick";
  data: {
    tick?: TickLike;
    snapshot?: TickLike;
    transportMeta?: TransportMeta;
  };
}

const baseUrl = requiredEnv("LOAD_TEST_BASE_URL").replace(/\/+$/, "");
const durationMs = envInt("LOAD_TEST_SAMPLE_MS", 120_000);
const outputPath = process.env.LOAD_TEST_OUTPUT?.trim();
const tokenFromEnv = process.env.LOAD_TEST_TOKEN?.trim();
const username = process.env.LOAD_TEST_USERNAME?.trim();
const password = process.env.LOAD_TEST_PASSWORD?.trim();
const localAddress = process.env.LOAD_TEST_LOCAL_ADDRESS?.trim();

const intervals: number[] = [];
const serverToClientLatencies: number[] = [];
const tickIntervals: number[] = [];
const tickServerToClientLatencies: number[] = [];
const fullServerToClientLatencies: number[] = [];
const queueLatencies: number[] = [];
const snapshotAges: number[] = [];
const wsSendAges: number[] = [];
const clientProcessLatencies: number[] = [];
const payloadBytes: number[] = [];
const tickPayloadBytes: number[] = [];
const fullPayloadBytes: number[] = [];
const sourceToBackend: Record<"binance" | "coinbase" | "clob", number[]> = {
  binance: [],
  coinbase: [],
  clob: []
};
const sourceAges: Record<"binance" | "coinbase" | "clob", number[]> = {
  binance: [],
  coinbase: [],
  clob: []
};

let tickCount = 0;
let fullCount = 0;
let openedAt = 0;
let firstTickAt = 0;
let firstFullAt = 0;
let lastMessageAt = 0;
let lastTickAt = 0;
let firstPayloadSeq = 0;
let lastPayloadSeq = 0;
let outOfOrder = 0;
let clockOffsetMs = 0;

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});

async function main() {
  clockOffsetMs = await sampleClockOffset();
  const token = tokenFromEnv ?? (await login());
  const ticket = await createTicket(token);
  const wsUrl = `${baseUrl.replace("http://", "ws://").replace("https://", "wss://")}/ws/market?ticket=${encodeURIComponent(ticket)}`;
  const startedAt = Date.now();
  const socket = new WebSocket(wsUrl, localAddress ? { localAddress } : undefined);

  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("Timed out waiting for WebSocket open.")), 10_000);
    socket.once("open", () => {
      openedAt = Date.now();
      clearTimeout(timeout);
      resolve();
    });
    socket.once("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });
  });

  socket.on("message", (raw) => {
    const receivedAt = Date.now();
    let parsed: MarketMessage | undefined;
    try {
      parsed = JSON.parse(raw.toString()) as MarketMessage;
    } catch {
      return;
    }
    if (parsed.type !== "market" && parsed.type !== "market:tick") {
      return;
    }
    const processedAt = Date.now();
    recordMessage(parsed, receivedAt, processedAt, raw);
  });

  await sleep(durationMs);
  socket.terminate();
  await sleep(100);

  const result = buildResult(startedAt, Date.now());
  const text = JSON.stringify(result, null, 2);
  if (outputPath) {
    writeFileSync(outputPath, `${text}\n`, "utf8");
  }
  console.log(text);
}

function recordMessage(message: MarketMessage, receivedAt: number, processedAt: number, raw: WebSocket.RawData) {
  const rawBytes = rawDataByteLength(raw);
  payloadBytes.push(rawBytes);
  if (lastMessageAt > 0) {
    intervals.push(receivedAt - lastMessageAt);
  }
  lastMessageAt = receivedAt;
  if (message.type === "market:tick") {
    tickPayloadBytes.push(rawBytes);
    if (!firstTickAt) {
      firstTickAt = receivedAt;
    }
    if (lastTickAt > 0) {
      tickIntervals.push(receivedAt - lastTickAt);
    }
    lastTickAt = receivedAt;
    tickCount += 1;
  } else {
    fullPayloadBytes.push(rawBytes);
    if (!firstFullAt) {
      firstFullAt = receivedAt;
    }
    fullCount += 1;
  }

  const transportMeta = message.data.transportMeta;
  if (transportMeta?.payloadSeq) {
    if (!firstPayloadSeq) {
      firstPayloadSeq = transportMeta.payloadSeq;
    }
    if (lastPayloadSeq && transportMeta.payloadSeq <= lastPayloadSeq) {
      outOfOrder += 1;
    }
    lastPayloadSeq = transportMeta.payloadSeq;
  }
  if (transportMeta?.serverPublishTs) {
    const latency = Math.max(receivedAt - transportMeta.serverPublishTs - clockOffsetMs, 0);
    serverToClientLatencies.push(latency);
    if (message.type === "market:tick") {
      tickServerToClientLatencies.push(latency);
    } else {
      fullServerToClientLatencies.push(latency);
    }
  }
  if (typeof transportMeta?.serverQueueMs === "number") {
    queueLatencies.push(transportMeta.serverQueueMs);
  }
  if (transportMeta?.snapshotBuildTs && transportMeta.serverPublishTs) {
    snapshotAges.push(Math.max(transportMeta.serverPublishTs - transportMeta.snapshotBuildTs, 0));
  }
  if (transportMeta?.wsSendStartTs) {
    wsSendAges.push(Math.max(receivedAt - transportMeta.wsSendStartTs - clockOffsetMs, 0));
  }
  clientProcessLatencies.push(Math.max(processedAt - receivedAt, 0));

  const data = message.data.tick ?? message.data.snapshot;
  const sources = data?.sources;
  if (!sources) {
    return;
  }
  for (const key of ["binance", "coinbase", "clob"] as const) {
    const source = sources[key];
    sourceToBackend[key].push(Math.max(source.serverRecvTs - source.sourceEventTs, 0));
    sourceAges[key].push(Math.max(receivedAt - source.normalizedTs - clockOffsetMs, 0));
  }
}

function buildResult(startedAt: number, finishedAt: number) {
  return {
    startedAt: new Date(startedAt).toISOString(),
    finishedAt: new Date(finishedAt).toISOString(),
    durationMs: finishedAt - startedAt,
    openMs: openedAt ? openedAt - startedAt : 0,
    firstTickMs: firstTickAt ? firstTickAt - startedAt : 0,
    firstFullMs: firstFullAt ? firstFullAt - startedAt : 0,
    tickCount,
    fullCount,
    firstPayloadSeq,
    lastPayloadSeq,
    outOfOrder,
    clockOffsetMs,
    payloadIntervalP50: percentile(intervals, 50),
    payloadIntervalP95: percentile(intervals, 95),
    payloadIntervalMax: max(intervals),
    tickIntervalP50: percentile(tickIntervals, 50),
    tickIntervalP95: percentile(tickIntervals, 95),
    tickIntervalMax: max(tickIntervals),
    serverToClientP50: percentile(serverToClientLatencies, 50),
    serverToClientP95: percentile(serverToClientLatencies, 95),
    serverToClientMax: max(serverToClientLatencies),
    tickServerToClientP50: percentile(tickServerToClientLatencies, 50),
    tickServerToClientP95: percentile(tickServerToClientLatencies, 95),
    tickServerToClientP99: percentile(tickServerToClientLatencies, 99),
    tickServerToClientMax: max(tickServerToClientLatencies),
    fullServerToClientP50: percentile(fullServerToClientLatencies, 50),
    fullServerToClientP95: percentile(fullServerToClientLatencies, 95),
    fullServerToClientMax: max(fullServerToClientLatencies),
    wsSendToClientP95: percentile(wsSendAges, 95),
    serverQueueP95: percentile(queueLatencies, 95),
    snapshotBuildAgeP95: percentile(snapshotAges, 95),
    renderCommitAgeP95: percentile(clientProcessLatencies, 95),
    payloadBytesP95: percentile(payloadBytes, 95),
    payloadBytesMax: max(payloadBytes),
    tickPayloadBytesP95: percentile(tickPayloadBytes, 95),
    fullPayloadBytesP95: percentile(fullPayloadBytes, 95),
    binanceSourceToBackendP95: percentile(sourceToBackend.binance, 95),
    clobSourceToBackendP95: percentile(sourceToBackend.clob, 95),
    coinbaseSourceToBackendP95: percentile(sourceToBackend.coinbase, 95),
    binanceSourceAgeP95: percentile(sourceAges.binance, 95),
    clobSourceAgeP95: percentile(sourceAges.clob, 95),
    coinbaseSourceAgeP95: percentile(sourceAges.coinbase, 95)
  };
}

async function sampleClockOffset() {
  try {
    const startedAt = Date.now();
    const data = await requestJson<{ serverNow?: number }>("/health", undefined, undefined, 3000);
    const receivedAt = Date.now();
    return typeof data.serverNow === "number" ? Math.round((startedAt + receivedAt) / 2 - data.serverNow) : 0;
  } catch {
    return 0;
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

async function request<T>(path: string, token?: string, init: RequestInit = {}) {
  return requestJson<T>(path, token, init, 10_000);
}

async function requestJson<T>(path: string, token?: string, init: RequestInit = {}, timeoutMs = 10_000) {
  const url = new URL(`${baseUrl}${path}`);
  const body =
    typeof init.body === "string" || Buffer.isBuffer(init.body)
      ? init.body
      : init.body
        ? String(init.body)
        : undefined;
  const headers: Record<string, string> = {
    "content-type": "application/json",
    ...(token ? { authorization: `Bearer ${token}` } : {})
  };
  for (const [key, value] of Object.entries((init.headers ?? {}) as Record<string, string>)) {
    headers[key] = value;
  }
  if (body) {
    headers["content-length"] = String(Buffer.byteLength(body));
  }

  const transport = url.protocol === "https:" ? https : http;
  return await new Promise<T>((resolve, reject) => {
    const request = transport.request(
      {
        protocol: url.protocol,
        hostname: url.hostname,
        port: url.port ? Number(url.port) : url.protocol === "https:" ? 443 : 80,
        path: `${url.pathname}${url.search}`,
        method: init.method ?? "GET",
        headers,
        localAddress
      },
      (response) => {
        const chunks: Buffer[] = [];
        response.on("data", (chunk) => chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
        response.on("end", () => {
          const text = Buffer.concat(chunks).toString("utf8");
          if (!response.statusCode || response.statusCode < 200 || response.statusCode >= 300) {
            reject(new Error(`Request failed: ${response.statusCode} ${response.statusMessage}`));
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
  const sorted = [...values].sort((left, right) => left - right);
  const index = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return Math.round(sorted[index]);
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

function max(values: number[]) {
  return values.length ? Math.round(Math.max(...values)) : 0;
}

function requiredEnv(name: string) {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required.`);
  }
  return value;
}

function envInt(name: string, fallback: number) {
  const value = process.env[name]?.trim();
  if (!value) {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

import { EventEmitter } from "node:events";
import { AsyncLocalStorage } from "node:async_hooks";
import { createHash } from "node:crypto";
import v8 from "node:v8";
import {
  closeSync,
  existsSync,
  mkdirSync,
  openSync,
  readSync,
  statSync
} from "node:fs";
import path from "node:path";
import { nanoid } from "nanoid";
import { Pool, type PoolClient, type QueryResult } from "pg";
import { createClient } from "redis";
import { ROLE_PERMISSIONS, normalizeRolePermissions } from "../auth/permissions";
import { hashPassword, isBcryptHash, verifyPassword } from "../auth/password";
import { AsyncJsonlWriter } from "./log-writer";
import {
  detailString,
  hasUnsupportedBehaviorSearchFilter,
  matchesAuditSearch,
  matchesBehaviorSearch,
  normalizeSearchLimit,
  resolveBehaviorSearchUserIds
} from "./store/log-search";
import {
  dedupeMarketCandles,
  isValidMarketCandle,
  marketCandleKey,
  mergeMarketCandlesIntoMemory
} from "./store/market-candles";
import { cloneOrderBookSnapshot, orderBookSnapshotRef } from "./store/order-book-snapshots";
import { buildProfileOverview } from "./store/profile";
import {
  rowToAuditEvent,
  rowToBehaviorLog,
  rowToMarketCandle,
  rowToOrder,
  rowToOrderBookSnapshotRecord,
  rowToOrderLifecycle,
  rowToPosition,
  rowToRound
} from "./store/row-mappers";
import { SCHEMA_SQL } from "./store/schema";
import type {
  AuditEvent,
  AuditLogQuery,
  BehaviorActionLog,
  BehaviorLogQuery,
  CandleBar,
  CandleInterval,
  Language,
  LogSearchQuery,
  MarketCandleQuery,
  MarketCandleRecord,
  MarketSnapshot,
  OrderBookSnapshotRecord,
  OrderLifecycleExitType,
  OrderLifecycleRecord,
  OrderBookSnapshot,
  OrderRecord,
  PermissionCode,
  PermissionLevel,
  PositionRecord,
  ProfileOverview,
  PublicUser,
  Role,
  RoundRecord,
  RoundStatus,
  SourceHealth,
  TradeSide,
  TradeTimeline,
  UserRecord
} from "../domain/types";

const STARTUP_CONNECT_RETRY_ATTEMPTS = 10;
const STARTUP_CONNECT_RETRY_DELAY_MS = 2000;
const FIVE_MINUTE_ROUND_MS = 5 * 60_000;
const QTY_EPSILON = 0.0000001;
const RETENTION_CLEANUP_INTERVAL_MS = 60_000;
const LOG_FILE_TAIL_BYTES = 512 * 1024;
const MEMORY_GUARD_INTERVAL_MS = 15_000;
const PERSISTENCE_FAILURE_THRESHOLD = 3;
const MARKET_CANDLE_MEMORY_RETENTION_MS = 24 * 60 * 60_000;
const MARKET_SNAPSHOT_CACHE_FLUSH_INTERVAL_MS = 1_000;
const ORDER_BOOK_SNAPSHOT_FLUSH_DELAY_MS = 1_000;
const txStorage = new AsyncLocalStorage<PoolClient>();

type MemoryProtectionState = "normal" | "warning" | "protect";
export type UserPayloadScope = "full" | "trade";

type PersistenceHealth = {
  enabled: boolean;
  writable: boolean;
  strict: boolean;
  state: "healthy" | "reconnecting" | "blocked";
  reconnecting: boolean;
  reconnectAttempts: number;
  consecutiveFailures: number;
  lastError?: string;
  lastFailureAt?: number;
  lastRecoveryAt?: number;
};

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const roundNumber = (value: number, digits = 8) => Number(value.toFixed(digits));

function isFiveMinuteRound(round: RoundRecord) {
  return round.endAt > round.startAt && round.endAt - round.startAt === FIVE_MINUTE_ROUND_MS;
}

function parseBtcFiveMinuteSlugStart(value?: string) {
  const match = value?.toLowerCase().match(/^btc-updown-5m-(\d+)$/);
  if (!match) {
    return undefined;
  }
  const seconds = Number(match[1]);
  return Number.isFinite(seconds) ? seconds * 1000 : undefined;
}

function inferFiveMinuteWindow(input: { roundId: string; marketSlug?: string; fallbackTs: number }) {
  const slugStartAt = parseBtcFiveMinuteSlugStart(input.marketSlug) ?? parseBtcFiveMinuteSlugStart(input.roundId);
  const startAt = slugStartAt ?? Math.floor(input.fallbackTs / FIVE_MINUTE_ROUND_MS) * FIVE_MINUTE_ROUND_MS;
  return {
    startAt,
    endAt: startAt + FIVE_MINUTE_ROUND_MS
  };
}

function readUtf8Tail(filePath: string, maxBytes: number) {
  if (!existsSync(filePath)) {
    return "";
  }
  const stats = statSync(filePath);
  const bytesToRead = Math.max(0, Math.min(maxBytes, stats.size));
  if (!bytesToRead) {
    return "";
  }
  const fd = openSync(filePath, "r");
  try {
    const buffer = Buffer.allocUnsafe(bytesToRead);
    const start = Math.max(0, stats.size - bytesToRead);
    const bytesRead = readSync(fd, buffer, 0, bytesToRead, start);
    return buffer.subarray(0, bytesRead).toString("utf-8");
  } finally {
    closeSync(fd);
  }
}

function readJsonlTail<T>(filePath: string, maxBytes: number, guard: (value: unknown) => value is T) {
  const text = readUtf8Tail(filePath, maxBytes);
  if (!text) {
    return [] as T[];
  }
  const normalized = text.includes("\n") ? text.slice(text.indexOf("\n") + 1) : text;
  return normalized
    .split(/\r?\n/)
    .filter(Boolean)
    .map((line) => {
      try {
        return JSON.parse(line) as T;
      } catch {
        return undefined;
      }
    })
    .filter(guard);
}

const LOG_DIR = path.resolve(process.cwd(), "data/logs");
const LOG_FILE = path.join(LOG_DIR, "audit-events.jsonl");
const BEHAVIOR_LOG_FILE = path.join(LOG_DIR, "behavior-action-logs.jsonl");
const JSONL_WRITER_OPTIONS = {
  batchSize: 100,
  flushIntervalMs: 500,
  maxFileBytes: 50 * 1024 * 1024,
  maxQueueDepth: 10_000
};

function createEmptyCandleBar(interval: CandleInterval, now: number): CandleBar {
  return {
    interval,
    startTs: now,
    endTs: now,
    open: 0,
    high: 0,
    low: 0,
    close: 0,
    volume: 0
  };
}

function createEmptyMarketSnapshot(symbol: string, coinbaseEnabled: boolean): MarketSnapshot {
  const now = Date.now();
  const emptySource = (source: "Binance" | "Coinbase" | "CLOB"): SourceHealth => ({
    source,
    symbol,
    state: source === "Coinbase" && !coinbaseEnabled ? "disabled" : "reconnecting",
    reconnectCount: 0,
    sourceEventTs: now,
    serverRecvTs: now,
    normalizedTs: now,
    serverPublishTs: now,
    acquireLatencyMs: 0,
    publishLatencyMs: 0,
    frontendLatencyMs: 0,
    message:
      source === "Coinbase" && !coinbaseEnabled
        ? "Coinbase is disabled in local testing mode."
        : `Waiting for ${source}.`
  });
  return {
    symbol,
    marketId: "",
    serverNow: now,
    binancePrice: 0,
    coinbasePrice: 0,
    currentPrice: 0,
    priceToBeat: 0,
    displayPriceToBeat: undefined,
    displayPriceToBeatSource: undefined,
    upPrice: 0,
    downPrice: 0,
    displayPrices: {
      UP: 0,
      DOWN: 0
    },
    displayPriceSource: {
      UP: "outcome_price",
      DOWN: "outcome_price"
    },
    displayPriceSpread: {
      UP: 0,
      DOWN: 0
    },
    latencyBreakdown: {
      sourceEventAge: { binance: 0, coinbase: 0, clob: 0 },
      serverIngressLatency: { binance: 0, coinbase: 0, clob: 0 },
      serverComputeLatency: 0
    },
    sources: {
      binance: emptySource("Binance"),
      coinbase: emptySource("Coinbase"),
      clob: emptySource("CLOB")
    },
    orderBooks: {
      UP: {
        snapshotId: `empty_up_${now}`,
        snapshotTs: now,
        bestBid: 0,
        bestAsk: 0,
        midPrice: 0,
        bids: [],
        asks: []
      },
      DOWN: {
        snapshotId: `empty_down_${now}`,
        snapshotTs: now,
        bestBid: 0,
        bestAsk: 0,
        midPrice: 0,
        bids: [],
        asks: []
      }
    },
    recentTrades: [],
    candles: [],
    binance: {
      spotPrice: 0,
      latestTick: {
        ts: now,
        price: 0
      },
      candlesByInterval: {
        "30s": [createEmptyCandleBar("30s", now)],
        "1m": [createEmptyCandleBar("1m", now)],
        "5m": [createEmptyCandleBar("5m", now)],
        "15m": [createEmptyCandleBar("15m", now)],
        "1h": [createEmptyCandleBar("1h", now)],
        "1d": [createEmptyCandleBar("1d", now)]
      }
    },
    coinbase: {
      referencePrice: 0,
      settlementReference: 0,
      candles5s: [],
      candlesByInterval: {
        "30s": [createEmptyCandleBar("30s", now)],
        "1m": [createEmptyCandleBar("1m", now)],
        "5m": [createEmptyCandleBar("5m", now)],
        "15m": [createEmptyCandleBar("15m", now)],
        "1h": [createEmptyCandleBar("1h", now)],
        "1d": [createEmptyCandleBar("1d", now)]
      }
    },
    clob: {
      delta: 0,
      volume: 0,
      upBook: {
        snapshotId: `empty_up_${now}`,
        snapshotTs: now,
        bestBid: 0,
        bestAsk: 0,
        midPrice: 0,
        bids: [],
        asks: []
      },
      downBook: {
        snapshotId: `empty_down_${now}`,
        snapshotTs: now,
        bestBid: 0,
        bestAsk: 0,
        midPrice: 0,
        bids: [],
        asks: []
      },
      recentTrades: [],
      currentRoundUpPriceSeries: [],
      marketInfo: {
        minimumTickSize: 0.01,
        minimumOrderSize: 1,
        makerFeeRate: 0,
        takerFeeRate: 0,
        feeRateAvailable: false,
        source: "conservative",
        conservative: true,
        updatedAt: Date.now()
      },
      bestBidAskSummary: {
        UP: {
          bestBid: 0,
          bestAsk: 0
        },
        DOWN: {
          bestBid: 0,
          bestAsk: 0
        }
      }
    },
    uiMeta: {
      marketTitle: `${symbol} 5-Min Round UTC`,
      countdownMs: 0,
      acceptingOrders: false,
      marketSwitchState: "market_not_ready",
      sourceStatusSummary: [
        { source: "Binance", state: "reconnecting" },
        { source: "Coinbase", state: coinbaseEnabled ? "reconnecting" : "disabled" },
        { source: "CLOB", state: "reconnecting" }
      ]
    }
  };
}

function normalizePermissionCodes(value: unknown): PermissionCode[] {
  if (Array.isArray(value)) {
    return value.map((item) => String(item) as PermissionCode);
  }
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value) as string[];
      return parsed.map((item) => item as PermissionCode);
    } catch {
      return [];
    }
  }
  return [];
}

export type TradeMutationMemorySnapshot = {
  users: Array<[string, UserRecord]>;
  orders: OrderRecord[];
  positions: PositionRecord[];
  orderLifecycleLogs: OrderLifecycleRecord[];
  orderBookSnapshots: Array<[string, OrderBookSnapshotRecord]>;
  logs: AuditEvent[];
  behaviorLogs: BehaviorActionLog[];
};

export class AppStore {
  public readonly emitter = new EventEmitter();
  public readonly users = new Map<string, UserRecord>();
  public readonly rounds: RoundRecord[] = [];
  public readonly orders: OrderRecord[] = [];
  public readonly orderLifecycleLogs: OrderLifecycleRecord[] = [];
  public readonly orderBookSnapshots = new Map<string, OrderBookSnapshotRecord>();
  private readonly pendingOrderBookSnapshotRecords = new Map<string, OrderBookSnapshotRecord>();
  public readonly positions: PositionRecord[] = [];
  private readonly marketCandles = new Map<string, MarketCandleRecord[]>();
  public readonly logs: AuditEvent[] = [];
  public readonly behaviorLogs: BehaviorActionLog[] = [];
  public marketSnapshot: MarketSnapshot;
  private historyRevision = 0;

  private pool?: Pool;
  private redis?: ReturnType<typeof createClient>;
  private readonly snapshotCacheKey: string;
  private readonly sourcesCacheKey: string;
  private postgresEnabled = false;
  private redisEnabled = false;
  private queuedMarketSnapshot?: MarketSnapshot;
  private marketSnapshotPersistRunning = false;
  private marketSnapshotPersistTimer?: ReturnType<typeof setTimeout>;
  private lastMarketSnapshotPersistAt = 0;
  private lastRetentionCleanupAt = 0;
  private retentionCleanupRunning = false;
  private lastMemoryGuardAt = 0;
  private readonly orderIndexById = new Map<string, number>();
  private readonly positionIndexById = new Map<string, number>();
  private readonly orderLifecycleIndexById = new Map<string, number>();
  private ordersByUserId = new Map<string, OrderRecord[]>();
  private positionsByUserId = new Map<string, PositionRecord[]>();
  private orderLifecyclesByUserId = new Map<string, OrderLifecycleRecord[]>();
  private operatedRoundIdsByUserId = new Map<string, Set<string>>();
  private readonly pendingUserPayloadIds = new Map<string, UserPayloadScope>();
  private readonly memoryRedeemLedgerKeys = new Set<string>();
  private userPayloadFlushScheduled = false;
  private orderBookSnapshotFlushRunning = false;
  private orderBookSnapshotFlushTimer?: ReturnType<typeof setTimeout>;
  private orderBookSnapshotFlushFailures = 0;
  private orderBookSnapshotLastFlushMs = 0;
  private memoryProtectionState: MemoryProtectionState = "normal";
  private postgresReconnectTask?: Promise<void>;
  private closed = false;
  private readonly persistenceHealth: {
    postgres: PersistenceHealth;
    redis: PersistenceHealth;
  };
  private readonly auditLogWriter = new AsyncJsonlWriter<AuditEvent>(LOG_FILE, JSONL_WRITER_OPTIONS);
  private readonly behaviorLogWriter = new AsyncJsonlWriter<BehaviorActionLog>(BEHAVIOR_LOG_FILE, JSONL_WRITER_OPTIONS);
  private readonly config: {
    initialBalance: number;
    logRetentionMs: number;
    snapshotRetentionSeconds: number;
    symbol: string;
    databaseUrl: string;
    redisUrl: string;
    persistenceMode: "external" | "memory";
    coinbaseEnabled: boolean;
    strictPersistence: boolean;
    seedDefaultUsers?: boolean;
    requireSchemaMigrations?: boolean;
    allowDevSchemaBootstrap?: boolean;
    expectedSchemaMigrationId?: string;
    pgConnectionTimeoutMs: number;
    pgIdleTimeoutMs: number;
    pgMaxConnections: number;
    pgKeepAlive: boolean;
    pgReconnectIntervalMs: number;
    pgReconnectMaxIntervalMs: number;
    orderBookSnapshotsMemoryMax: number;
    orderBookSnapshotsMemoryMaxAgeMs: number;
    ordersMemoryMax: number;
    positionsMemoryMax: number;
    auditLogsMemoryMax: number;
    behaviorLogsMemoryMax: number;
    orderLifecycleMemoryMax: number;
    roundsMemoryMax: number;
    serverHeapWarnMb: number;
    serverHeapProtectMb: number;
  };

  constructor(config: {
    initialBalance: number;
    logRetentionMs: number;
    snapshotRetentionSeconds: number;
    symbol: string;
    databaseUrl: string;
    redisUrl: string;
    persistenceMode: "external" | "memory";
    coinbaseEnabled: boolean;
    strictPersistence: boolean;
    seedDefaultUsers: boolean;
    requireSchemaMigrations: boolean;
    allowDevSchemaBootstrap: boolean;
    expectedSchemaMigrationId: string;
    pgConnectionTimeoutMs: number;
    pgIdleTimeoutMs: number;
    pgMaxConnections: number;
    pgKeepAlive: boolean;
    pgReconnectIntervalMs: number;
    pgReconnectMaxIntervalMs: number;
    orderBookSnapshotsMemoryMax: number;
    orderBookSnapshotsMemoryMaxAgeMs: number;
    ordersMemoryMax: number;
    positionsMemoryMax: number;
    auditLogsMemoryMax: number;
    behaviorLogsMemoryMax: number;
    orderLifecycleMemoryMax: number;
    roundsMemoryMax: number;
    serverHeapWarnMb: number;
    serverHeapProtectMb: number;
  }) {
    this.config = {
      ...config,
      seedDefaultUsers: config.seedDefaultUsers ?? true,
      requireSchemaMigrations: config.requireSchemaMigrations ?? false,
      allowDevSchemaBootstrap: config.allowDevSchemaBootstrap ?? true,
      expectedSchemaMigrationId: config.expectedSchemaMigrationId ?? "000007"
    };
    this.snapshotCacheKey = `market:snapshot:${config.symbol}`;
    this.sourcesCacheKey = `market:sources:${config.symbol}`;
    this.marketSnapshot = createEmptyMarketSnapshot(config.symbol, config.coinbaseEnabled);
    this.persistenceHealth = {
      postgres: {
        enabled: config.persistenceMode !== "memory",
        writable: config.persistenceMode !== "memory",
        strict: config.strictPersistence,
        state: config.persistenceMode === "memory" ? "blocked" : "healthy",
        reconnecting: false,
        reconnectAttempts: 0,
        consecutiveFailures: 0
      },
      redis: {
        enabled: config.persistenceMode !== "memory",
        writable: config.persistenceMode !== "memory",
        strict: false,
        state: config.persistenceMode === "memory" ? "blocked" : "healthy",
        reconnecting: false,
        reconnectAttempts: 0,
        consecutiveFailures: 0
      }
    };
    mkdirSync(LOG_DIR, { recursive: true });
  }

  async init() {
    console.log("[store] init start");
    await this.connectPostgres();
    console.log(`[store] connectPostgres done enabled=${this.postgresEnabled}`);
    if (this.config.seedDefaultUsers) {
      await this.seedUsers();
      console.log("[store] seedUsers done");
    } else {
      console.log("[store] seedUsers skipped");
    }
    await this.connectRedis();
    console.log(`[store] connectRedis done enabled=${this.redisEnabled}`);
    await this.loadStateFromPersistence();
    console.log("[store] loadStateFromPersistence done");
  }

  async close() {
    this.closed = true;
    if (this.redis?.isOpen) {
      await this.redis.quit().catch(() => undefined);
    }
    await this.closePostgresPool();
    await this.postgresReconnectTask?.catch(() => undefined);
    await Promise.all([this.auditLogWriter.close(), this.behaviorLogWriter.close()]);
  }

  sanitizeUser(user: UserRecord): PublicUser {
    return {
      id: user.id,
      username: user.username,
      displayName: user.displayName,
      role: user.role,
      language: user.language,
      permissionCodes: user.permissionCodes,
      availableUsdc: user.availableUsdc,
      isActive: user.isActive,
      seniorTesterId: user.seniorTesterId,
      managerUserId: user.managerUserId ?? user.seniorTesterId,
      permissionLevel: user.permissionLevel ?? "Standard",
      lockedUntil: user.lockedUntil,
      passwordChangedAt: user.passwordChangedAt,
      lastLoginAt: user.lastLoginAt,
      mustChangePassword: user.mustChangePassword,
      disabledAt: user.disabledAt,
      disabledBy: user.disabledBy,
      createdAt: user.createdAt,
      updatedAt: user.updatedAt
    };
  }

  findUserByCredentials(username: string, password: string) {
    for (const user of this.users.values()) {
      if (user.username === username && this.isUserLocked(user)) {
        return undefined;
      }
      if (user.username === username && verifyPassword(password, user.password) && user.isActive) {
        if (!isBcryptHash(user.password)) {
          user.password = hashPassword(password);
          user.passwordChangedAt = Date.now();
          user.updatedAt = Date.now();
          void this.persistUser(user);
        }
        return user;
      }
    }
    return undefined;
  }

  isUserLocked(user: UserRecord, now = Date.now()) {
    return typeof user.lockedUntil === "number" && user.lockedUntil > now;
  }

  async recordFailedLogin(user: UserRecord) {
    user.failedLoginCount = (user.failedLoginCount ?? 0) + 1;
    if (user.failedLoginCount >= 3) {
      user.lockedUntil = Date.now() + 10 * 60_000;
    }
    user.updatedAt = Date.now();
    await this.persistUser(user);
  }

  async recordSuccessfulLogin(user: UserRecord) {
    user.failedLoginCount = 0;
    user.lockedUntil = undefined;
    user.lastLoginAt = Date.now();
    user.updatedAt = user.lastLoginAt;
    await this.persistUser(user);
  }

  findUserByUsername(username: string) {
    return [...this.users.values()].find((user) => user.username === username);
  }

  verifyUserPassword(user: UserRecord, password: string) {
    return verifyPassword(password, user.password);
  }

  getUserById(userId: string) {
    return this.users.get(userId);
  }

  listUsers() {
    return [...this.users.values()]
      .sort((left, right) => left.createdAt - right.createdAt)
      .map((user) => this.sanitizeUser(user));
  }

  listUserRecords() {
    return [...this.users.values()].sort((left, right) => left.createdAt - right.createdAt);
  }

  getPersistenceStatus() {
    return {
      postgres: this.postgresEnabled,
      redis: this.redisEnabled,
      strict: this.config.strictPersistence,
      state: this.persistenceHealth,
      memoryProtectionState: this.memoryProtectionState,
      heapUsedMb: this.heapUsedMb(),
      heapLimitMb: this.heapLimitMb()
    };
  }

  getMemoryStatus() {
    return {
      heapUsedMb: this.heapUsedMb(),
      heapLimitMb: this.heapLimitMb(),
      memoryProtectionState: this.memoryProtectionState
    };
  }

  getJsonlStats() {
    return {
      audit: this.auditLogWriter.getStats(),
      behavior: this.behaviorLogWriter.getStats()
    };
  }

  getOrderBookSnapshotQueueStats() {
    return {
      pending: this.pendingOrderBookSnapshotRecords.size,
      flushing: this.orderBookSnapshotFlushRunning,
      failures: this.orderBookSnapshotFlushFailures,
      lastFlushMs: this.orderBookSnapshotLastFlushMs
    };
  }

  captureTradeMutationSnapshot(): TradeMutationMemorySnapshot {
    return {
      users: [...this.users.entries()].map(([id, user]) => [id, { ...user }]),
      orders: this.orders.map((order) => ({ ...order })),
      positions: this.positions.map((position) => ({ ...position })),
      orderLifecycleLogs: this.orderLifecycleLogs.map((log) => ({ ...log })),
      orderBookSnapshots: [...this.orderBookSnapshots.entries()].map(([ref, record]) => [
        ref,
        {
          ...record,
          snapshot: {
            ...record.snapshot,
            bids: record.snapshot.bids.map((level) => ({ ...level })),
            asks: record.snapshot.asks.map((level) => ({ ...level }))
          }
        }
      ]),
      logs: this.logs.map((log) => ({ ...log })),
      behaviorLogs: this.behaviorLogs.map((log) => ({ ...log }))
    };
  }

  restoreTradeMutationSnapshot(snapshot: TradeMutationMemorySnapshot) {
    this.users.clear();
    for (const [id, user] of snapshot.users) {
      this.users.set(id, { ...user });
    }
    this.orders.splice(0, this.orders.length, ...snapshot.orders.map((order) => ({ ...order })));
    this.positions.splice(0, this.positions.length, ...snapshot.positions.map((position) => ({ ...position })));
    this.orderLifecycleLogs.splice(
      0,
      this.orderLifecycleLogs.length,
      ...snapshot.orderLifecycleLogs.map((log) => ({ ...log }))
    );
    this.orderBookSnapshots.clear();
    for (const [ref, record] of snapshot.orderBookSnapshots) {
      this.orderBookSnapshots.set(ref, {
        ...record,
        snapshot: {
          ...record.snapshot,
          bids: record.snapshot.bids.map((level) => ({ ...level })),
          asks: record.snapshot.asks.map((level) => ({ ...level }))
        }
      });
    }
    this.logs.splice(0, this.logs.length, ...snapshot.logs.map((log) => ({ ...log })));
    this.behaviorLogs.splice(0, this.behaviorLogs.length, ...snapshot.behaviorLogs.map((log) => ({ ...log })));
    this.rebuildHotIndexes();
    this.bumpHistoryRevision();
  }

  prepareOrderBookSnapshotForOrder(order: OrderRecord) {
    if (!order.orderBookSnapshot) {
      return order.orderBookSnapshotRef;
    }
    const ref = this.enqueueOrderBookSnapshot(order.orderBookSnapshot);
    order.orderBookSnapshotRef = ref;
    order.orderBookSnapshot = undefined;
    return ref;
  }

  assertWritablePersistence(context: string) {
    if (
      this.config.persistenceMode === "external" &&
      this.config.strictPersistence &&
      (!this.postgresEnabled || !this.persistenceHealth.postgres.writable)
    ) {
      void this.schedulePostgresReconnect(`${context} blocked`);
      throw new Error(this.persistenceUnavailableMessage(context));
    }
  }

  async withTransaction<T>(handler: () => Promise<T>) {
    const activeClient = txStorage.getStore();
    if (activeClient) {
      return handler();
    }
    if (!this.postgresEnabled || !this.pool) {
      if (this.config.persistenceMode === "external" && this.config.strictPersistence) {
        void this.schedulePostgresReconnect("transaction requested while unavailable");
        throw new Error(this.persistenceUnavailableMessage("Transaction"));
      }
      return handler();
    }

    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await txStorage.run(client, handler);
      await client.query("COMMIT");
      this.notePersistenceSuccess("postgres");
      return result;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      await this.handlePostgresFailure(error, "transaction");
      throw error;
    } finally {
      client.release();
    }
  }

  async claimRedeemLedger(input: {
    roundId: string;
    userId: string;
    positionId: string;
    redeemAmountUsdc: number;
    realizedPnlUsdc: number;
    settlementResult: "win" | "loss";
    createdAtMs: number;
    details?: Record<string, unknown>;
  }) {
    const key = `${input.roundId}:${input.userId}:${input.positionId}`;
    if (!this.postgresEnabled || !this.pool) {
      if (this.memoryRedeemLedgerKeys.has(key)) {
        return false;
      }
      this.memoryRedeemLedgerKeys.add(key);
      return true;
    }

    const result = await this.queryDb(
      `
      INSERT INTO redeem_ledger (
        id, round_id, user_id, position_id, redeem_amount_usdc, realized_pnl_usdc,
        settlement_result, created_at_ms, details
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
      ON CONFLICT (round_id, user_id, position_id) DO NOTHING
      `,
      [
        `redeem_${createHash("sha256").update(key).digest("hex").slice(0, 24)}`,
        input.roundId,
        input.userId,
        input.positionId,
        input.redeemAmountUsdc,
        input.realizedPnlUsdc,
        input.settlementResult,
        input.createdAtMs,
        JSON.stringify(input.details ?? {})
      ]
    );
    return (result.rowCount ?? 0) > 0;
  }

  isPersistenceUnavailableError(error: unknown) {
    if (!(error instanceof Error)) {
      return false;
    }
    return (
      error.message.includes("PostgreSQL persistence is unavailable") ||
      error.message.includes("persistent storage is unavailable") ||
      error.message.includes("Matching PostgreSQL persistence is unavailable")
    );
  }

  private upsertIndexedRecord<T extends { id: string }>(records: T[], indexById: Map<string, number>, record: T) {
    const index = indexById.get(record.id);
    if (typeof index === "number") {
      records[index] = record;
      return;
    }
    indexById.set(record.id, records.length);
    records.push(record);
  }

  private removeUserIndexedRecord<T extends { id: string }>(recordsByUserId: Map<string, T[]>, record: T & { userId: string }) {
    const records = recordsByUserId.get(record.userId);
    if (!records) {
      return;
    }
    const next = records.filter((item) => item.id !== record.id);
    if (next.length > 0) {
      recordsByUserId.set(record.userId, next);
    } else {
      recordsByUserId.delete(record.userId);
    }
  }

  private upsertUserIndexedRecord<T extends { id: string; userId: string }>(recordsByUserId: Map<string, T[]>, record: T) {
    const records = recordsByUserId.get(record.userId) ?? [];
    const index = records.findIndex((item) => item.id === record.id);
    if (index >= 0) {
      records[index] = record;
    } else {
      records.push(record);
    }
    recordsByUserId.set(record.userId, records);
  }

  private rememberOperatedRound(userId: string, roundId?: string) {
    if (!roundId) {
      return;
    }
    const roundIds = this.operatedRoundIdsByUserId.get(userId) ?? new Set<string>();
    roundIds.add(roundId);
    this.operatedRoundIdsByUserId.set(userId, roundIds);
  }

  private upsertOrderInMemory(order: OrderRecord) {
    const previousIndex = this.orderIndexById.get(order.id);
    const previous = typeof previousIndex === "number" ? this.orders[previousIndex] : undefined;
    if (previous && previous.userId !== order.userId) {
      this.removeUserIndexedRecord(this.ordersByUserId, previous);
    }
    this.upsertIndexedRecord(this.orders, this.orderIndexById, order);
    this.upsertUserIndexedRecord(this.ordersByUserId, order);
    this.rememberOperatedRound(order.userId, order.roundId);
  }

  private upsertPositionInMemory(position: PositionRecord) {
    const previousIndex = this.positionIndexById.get(position.id);
    const previous = typeof previousIndex === "number" ? this.positions[previousIndex] : undefined;
    if (previous && previous.userId !== position.userId) {
      this.removeUserIndexedRecord(this.positionsByUserId, previous);
    }
    this.upsertIndexedRecord(this.positions, this.positionIndexById, position);
    this.upsertUserIndexedRecord(this.positionsByUserId, position);
    this.rememberOperatedRound(position.userId, position.roundId);
  }

  private upsertOrderLifecycleInMemory(log: OrderLifecycleRecord) {
    const previousIndex = this.orderLifecycleIndexById.get(log.id);
    const previous = typeof previousIndex === "number" ? this.orderLifecycleLogs[previousIndex] : undefined;
    if (previous && previous.userId !== log.userId) {
      this.removeUserIndexedRecord(this.orderLifecyclesByUserId, previous);
    }
    this.upsertIndexedRecord(this.orderLifecycleLogs, this.orderLifecycleIndexById, log);
    this.upsertUserIndexedRecord(this.orderLifecyclesByUserId, log);
    this.rememberOperatedRound(log.userId, log.roundId);
  }

  private rebuildHotIndexes() {
    this.ordersByUserId ??= new Map<string, OrderRecord[]>();
    this.positionsByUserId ??= new Map<string, PositionRecord[]>();
    this.orderLifecyclesByUserId ??= new Map<string, OrderLifecycleRecord[]>();
    this.operatedRoundIdsByUserId ??= new Map<string, Set<string>>();
    this.orderIndexById.clear();
    this.positionIndexById.clear();
    this.orderLifecycleIndexById.clear();
    this.ordersByUserId.clear();
    this.positionsByUserId.clear();
    this.orderLifecyclesByUserId.clear();
    this.operatedRoundIdsByUserId.clear();
    this.orders.forEach((order, index) => {
      this.orderIndexById.set(order.id, index);
      this.upsertUserIndexedRecord(this.ordersByUserId, order);
      this.rememberOperatedRound(order.userId, order.roundId);
    });
    this.positions.forEach((position, index) => {
      this.positionIndexById.set(position.id, index);
      this.upsertUserIndexedRecord(this.positionsByUserId, position);
      this.rememberOperatedRound(position.userId, position.roundId);
    });
    this.orderLifecycleLogs.forEach((log, index) => {
      this.orderLifecycleIndexById.set(log.id, index);
      this.upsertUserIndexedRecord(this.orderLifecyclesByUserId, log);
      this.rememberOperatedRound(log.userId, log.roundId);
    });
  }

  private trimArrayByCreatedAt<T>(
    records: T[],
    maxItems: number,
    getCreatedAt: (record: T) => number,
    onTrim?: (trimmed: T[]) => void
  ) {
    if (records.length <= maxItems) {
      return;
    }
    const retained = [...records]
      .sort((left, right) => getCreatedAt(right) - getCreatedAt(left))
      .slice(0, maxItems);
    const retainedSet = new Set(retained);
    const trimmed = records.filter((record) => !retainedSet.has(record));
    records.splice(0, records.length, ...retained);
    onTrim?.(trimmed);
  }

  private pruneMemoryCaches(now = Date.now()) {
    this.trimArrayByCreatedAt(this.rounds, this.config.roundsMemoryMax, (round) => round.startAt);
    this.trimArrayByCreatedAt(this.orders, this.config.ordersMemoryMax, (order) => order.createdAt);
    this.trimArrayByCreatedAt(
      this.orderLifecycleLogs,
      this.config.orderLifecycleMemoryMax,
      (log) => log.updatedAt ?? log.createdAt,
      () => this.rebuildHotIndexes()
    );
    this.trimArrayByCreatedAt(this.positions, this.config.positionsMemoryMax, (position) => position.openedAt, () =>
      this.rebuildHotIndexes()
    );
    this.trimArrayByCreatedAt(this.logs, this.config.auditLogsMemoryMax, (log) => log.serverRecvTs);
    this.trimArrayByCreatedAt(this.behaviorLogs, this.config.behaviorLogsMemoryMax, (log) => log.timestampMs);
    this.pruneOrderBookSnapshots(now);
    this.rebuildHotIndexes();
  }

  private pruneOrderBookSnapshots(now = Date.now()) {
    if (this.orderBookSnapshots.size <= this.config.orderBookSnapshotsMemoryMax) {
      const cutoff = now - this.config.orderBookSnapshotsMemoryMaxAgeMs;
      for (const [ref, snapshot] of this.orderBookSnapshots.entries()) {
        if (snapshot.createdAt < cutoff && !this.isSnapshotRefReferenced(ref)) {
          this.orderBookSnapshots.delete(ref);
        }
      }
      return;
    }

    const referenced = new Set<string>();
    for (const order of this.orders) {
      if (order.orderBookSnapshotRef) {
        referenced.add(order.orderBookSnapshotRef);
      }
    }
    for (const log of this.orderLifecycleLogs) {
      if (log.orderBookSnapshotRef) {
        referenced.add(log.orderBookSnapshotRef);
      }
    }

    const snapshots = [...this.orderBookSnapshots.values()].sort((left, right) => right.createdAt - left.createdAt);
    let retainedCount = 0;
    const cutoff = now - this.config.orderBookSnapshotsMemoryMaxAgeMs;
    for (const snapshot of snapshots) {
      const mustKeep = referenced.has(snapshot.ref);
      const withinLimit = retainedCount < this.config.orderBookSnapshotsMemoryMax;
      const withinAge = snapshot.createdAt >= cutoff;
      if (mustKeep || withinLimit || withinAge) {
        retainedCount += 1;
        continue;
      }
      this.orderBookSnapshots.delete(snapshot.ref);
    }
  }

  private isSnapshotRefReferenced(ref: string) {
    return this.orders.some((order) => order.orderBookSnapshotRef === ref) ||
      this.orderLifecycleLogs.some((log) => log.orderBookSnapshotRef === ref);
  }

  private heapUsedMb() {
    return Number((process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1));
  }

  private heapLimitMb() {
    return Math.round(v8.getHeapStatistics().heap_size_limit / 1024 / 1024);
  }

  private updateMemoryProtectionState() {
    const heapUsedMb = this.heapUsedMb();
    this.memoryProtectionState =
      heapUsedMb >= this.config.serverHeapProtectMb
        ? "protect"
        : heapUsedMb >= this.config.serverHeapWarnMb
          ? "warning"
          : "normal";
  }

  async setUserLanguage(userId: string, language: Language) {
    const user = this.users.get(userId);
    if (!user) {
      return undefined;
    }
    user.language = language;
    user.updatedAt = Date.now();
    await this.persistUser(user);
    return user;
  }

  async persistUser(user: UserRecord) {
    user.permissionCodes = ROLE_PERMISSIONS[user.role];
    user.managerUserId = user.managerUserId ?? user.seniorTesterId;
    user.permissionLevel = user.permissionLevel ?? "Standard";
    user.updatedAt = user.updatedAt || Date.now();
    this.users.set(user.id, user);
    await this.runDb(
      `
      INSERT INTO users (
        id, username, password, display_name, role, language, permission_codes, available_usdc,
        is_active, senior_tester_id, disabled_at, disabled_by, manager_user_id, permission_level,
        failed_login_count, locked_until, password_changed_at, last_login_at, must_change_password,
        created_at, updated_at
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
      ON CONFLICT (id) DO UPDATE SET
        username = EXCLUDED.username,
        password = EXCLUDED.password,
        display_name = EXCLUDED.display_name,
        role = EXCLUDED.role,
        language = EXCLUDED.language,
        permission_codes = EXCLUDED.permission_codes,
        available_usdc = EXCLUDED.available_usdc,
        is_active = EXCLUDED.is_active,
        senior_tester_id = EXCLUDED.senior_tester_id,
        disabled_at = EXCLUDED.disabled_at,
        disabled_by = EXCLUDED.disabled_by,
        manager_user_id = EXCLUDED.manager_user_id,
        permission_level = EXCLUDED.permission_level,
        failed_login_count = EXCLUDED.failed_login_count,
        locked_until = EXCLUDED.locked_until,
        password_changed_at = EXCLUDED.password_changed_at,
        last_login_at = EXCLUDED.last_login_at,
        must_change_password = EXCLUDED.must_change_password,
        updated_at = EXCLUDED.updated_at
      `,
      [
        user.id,
        user.username,
        user.password,
        user.displayName,
        user.role,
        user.language,
        JSON.stringify(user.permissionCodes),
        user.availableUsdc,
        user.isActive,
        user.seniorTesterId ?? null,
        user.disabledAt ?? null,
        user.disabledBy ?? null,
        user.managerUserId ?? null,
        user.permissionLevel ?? "Standard",
        user.failedLoginCount ?? 0,
        user.lockedUntil ?? null,
        user.passwordChangedAt ?? null,
        user.lastLoginAt ?? null,
        user.mustChangePassword ?? false,
        user.createdAt,
        user.updatedAt
      ]
    );
  }

  async createUser(input: {
    username: string;
    password: string;
    displayName: string;
    role: Role;
    language: Language;
    seniorTesterId?: string;
    managerUserId?: string;
    permissionLevel?: PermissionLevel;
    mustChangePassword?: boolean;
    availableUsdc: number;
  }) {
    if (this.findUserByUsername(input.username)) {
      throw new Error("Username already exists.");
    }
    const now = Date.now();
    const user: UserRecord = {
      id: this.newId("u"),
      username: input.username,
      password: hashPassword(input.password),
      displayName: input.displayName,
      role: input.role,
      language: input.language,
      permissionCodes: ROLE_PERMISSIONS[input.role],
      availableUsdc: input.availableUsdc,
      isActive: true,
      seniorTesterId: input.seniorTesterId,
      managerUserId: input.managerUserId ?? input.seniorTesterId,
      permissionLevel: input.permissionLevel ?? "Standard",
      failedLoginCount: 0,
      passwordChangedAt: now,
      mustChangePassword: input.mustChangePassword ?? false,
      createdAt: now,
      updatedAt: now
    };
    await this.persistUser(user);
    return user;
  }

  async updateUserProfile(
    userId: string,
    input: {
      displayName?: string;
      role?: Role;
      language?: Language;
      seniorTesterId?: string;
      managerUserId?: string;
      permissionLevel?: PermissionLevel;
      availableUsdc?: number;
      isActive?: boolean;
      disabledBy?: string;
    }
  ) {
    const user = this.users.get(userId);
    if (!user) {
      throw new Error("User was not found.");
    }
    if (typeof input.displayName === "string") {
      user.displayName = input.displayName;
    }
    if (input.role) {
      user.role = input.role;
    }
    if (input.language) {
      user.language = input.language;
    }
    if (Object.prototype.hasOwnProperty.call(input, "seniorTesterId") || Object.prototype.hasOwnProperty.call(input, "managerUserId")) {
      const managerUserId = input.managerUserId ?? input.seniorTesterId;
      user.seniorTesterId = user.role === "Tester" ? managerUserId : undefined;
      user.managerUserId = user.role === "Tester" ? managerUserId : undefined;
    }
    if (input.permissionLevel) {
      user.permissionLevel = input.permissionLevel;
    }
    if (typeof input.availableUsdc === "number") {
      user.availableUsdc = Number(input.availableUsdc.toFixed(2));
    }
    if (typeof input.isActive === "boolean") {
      user.isActive = input.isActive;
      user.disabledAt = input.isActive ? undefined : Date.now();
      user.disabledBy = input.isActive ? undefined : input.disabledBy;
    }
    user.updatedAt = Date.now();
    await this.persistUser(user);
    this.emitUserPayload(user.id);
    return user;
  }

  async disableUser(userId: string, disabledBy: string) {
    const user = this.users.get(userId);
    if (!user) {
      throw new Error("User was not found.");
    }
    const now = Date.now();
    user.isActive = false;
    user.disabledAt = now;
    user.disabledBy = disabledBy;
    user.updatedAt = now;
    await this.persistUser(user);
    this.emitUserPayload(user.id);
    return user;
  }

  async enableUser(userId: string) {
    const user = this.users.get(userId);
    if (!user) {
      throw new Error("User was not found.");
    }
    user.isActive = true;
    user.disabledAt = undefined;
    user.disabledBy = undefined;
    user.updatedAt = Date.now();
    await this.persistUser(user);
    this.emitUserPayload(user.id);
    return user;
  }

  async resetUserPassword(userId: string, password: string) {
    const user = this.users.get(userId);
    if (!user) {
      throw new Error("User was not found.");
    }
    user.password = hashPassword(password);
    user.passwordChangedAt = Date.now();
    user.failedLoginCount = 0;
    user.lockedUntil = undefined;
    user.mustChangePassword = false;
    user.updatedAt = Date.now();
    await this.persistUser(user);
    return user;
  }

  async setUserBalance(userId: string, availableUsdc: number) {
    const user = this.users.get(userId);
    if (!user) {
      throw new Error("User was not found.");
    }
    user.availableUsdc = Number(availableUsdc.toFixed(2));
    user.updatedAt = Date.now();
    await this.persistUser(user);
    this.emitUserPayload(user.id);
    return user;
  }

  async setMarketSnapshot(snapshot: MarketSnapshot) {
    this.marketSnapshot = snapshot;
    this.emitter.emit("market:update", snapshot);
    if (!this.redisEnabled || !this.redis?.isOpen) {
      return;
    }
    this.queuedMarketSnapshot = snapshot;
    if (this.marketSnapshotPersistRunning || this.marketSnapshotPersistTimer) {
      return;
    }
    this.scheduleMarketSnapshotCacheFlush();
  }

  private scheduleMarketSnapshotCacheFlush(delayMs = 0) {
    if (this.marketSnapshotPersistTimer) {
      return;
    }
    this.marketSnapshotPersistTimer = setTimeout(() => {
      this.marketSnapshotPersistTimer = undefined;
      void this.flushMarketSnapshotCache();
    }, Math.max(delayMs, 0));
  }

  private async flushMarketSnapshotCache() {
    if (this.marketSnapshotPersistRunning) {
      return;
    }
    this.marketSnapshotPersistRunning = true;
    try {
      const snapshot = this.queuedMarketSnapshot;
      if (!snapshot) {
        return;
      }
      const nextAllowedAt = this.lastMarketSnapshotPersistAt + MARKET_SNAPSHOT_CACHE_FLUSH_INTERVAL_MS;
      const delayMs = nextAllowedAt - Date.now();
      if (delayMs > 0) {
        return;
      }
      this.queuedMarketSnapshot = undefined;
      await this.persistMarketSnapshotCache(snapshot);
      this.lastMarketSnapshotPersistAt = Date.now();
    } finally {
      this.marketSnapshotPersistRunning = false;
      if (this.queuedMarketSnapshot) {
        const nextAllowedAt = this.lastMarketSnapshotPersistAt + MARKET_SNAPSHOT_CACHE_FLUSH_INTERVAL_MS;
        this.scheduleMarketSnapshotCacheFlush(Math.max(nextAllowedAt - Date.now(), 0));
      }
    }
  }

  private async persistMarketSnapshotCache(snapshot: MarketSnapshot) {
    if (!this.redisEnabled || !this.redis?.isOpen) {
      return;
    }
    try {
      const snapshotJson = JSON.stringify(snapshot);
      const sourcesJson = JSON.stringify(Object.values(snapshot.sources));
      await Promise.all([
        this.redis.set(this.snapshotCacheKey, snapshotJson, {
          expiration: {
            type: "EX",
            value: this.config.snapshotRetentionSeconds
          }
        }),
        this.redis.set(this.sourcesCacheKey, sourcesJson, {
          expiration: {
            type: "EX",
            value: this.config.snapshotRetentionSeconds
          }
        }),
        this.redis.publish(`market:update:${this.config.symbol}`, snapshotJson)
      ]);
    } catch (error) {
      this.redisEnabled = false;
      this.notePersistenceFailure("redis", error);
      console.warn("[store] Redis snapshot cache is unavailable:", error);
    }
  }

  getHistoryRevision() {
    return this.historyRevision;
  }

  private bumpHistoryRevision() {
    this.historyRevision += 1;
  }

  private roundHistorySignature(round: RoundRecord) {
    return JSON.stringify({
      id: round.id,
      marketId: round.marketId,
      marketSlug: round.marketSlug,
      title: round.title,
      startAt: round.startAt,
      endAt: round.endAt,
      priceToBeat: round.priceToBeat,
      status: round.status,
      acceptingOrders: round.acceptingOrders,
      settledSide: round.settledSide,
      settlementPrice: round.settlementPrice,
      settlementTs: round.settlementTs,
      settlementSource: round.settlementSource,
      manualReason: round.manualReason,
      coinbaseOpenPrice: round.coinbaseOpenPrice,
      coinbaseClosePrice: round.coinbaseClosePrice
    });
  }

  getCurrentRound(now = Date.now()) {
    const active = [...this.rounds]
      .filter((round) => isFiveMinuteRound(round) && round.startAt <= now && round.endAt > now)
      .sort((left, right) => right.startAt - left.startAt)[0];
    if (active) {
      return active;
    }

    const nextUpcoming = [...this.rounds]
      .filter((round) => isFiveMinuteRound(round) && round.startAt > now && round.startAt - now <= FIVE_MINUTE_ROUND_MS)
      .sort((left, right) => left.startAt - right.startAt)[0];
    if (nextUpcoming) {
      return nextUpcoming;
    }

    return undefined;
  }

  getRoundById(roundId: string) {
    return this.rounds.find((round) => round.id === roundId);
  }

  getHistory(limit = 10, userId?: string) {
    const sorted = [...this.rounds]
      .filter((round) => isFiveMinuteRound(round) && round.startAt <= Date.now())
      .sort((left, right) => right.startAt - left.startAt)
      .slice(0, limit);
    return sorted.map((round) => ({
      ...round,
      userPnl: userId ? this.getRoundUserPnl(round.id, userId, round) : 0
    }));
  }

  getOperatedHistory(limit = 500, userId: string) {
    const orderByRoundId = new Map<string, OrderRecord>();
    const positionByRoundId = new Map<string, PositionRecord>();
    for (const order of this.ordersByUserId.get(userId) ?? []) {
      if (!orderByRoundId.has(order.roundId)) {
        orderByRoundId.set(order.roundId, order);
      }
    }
    for (const position of this.positionsByUserId.get(userId) ?? []) {
      if (!positionByRoundId.has(position.roundId)) {
        positionByRoundId.set(position.roundId, position);
      }
    }

    const roundById = new Map(this.rounds.map((round) => [round.id, round]));
    return [...(this.operatedRoundIdsByUserId.get(userId) ?? new Set<string>())]
      .map((roundId) => roundById.get(roundId) ?? this.createOperatedFallbackRound(roundId, orderByRoundId.get(roundId), positionByRoundId.get(roundId)))
      .filter(
        (round): round is RoundRecord =>
          round !== undefined && isFiveMinuteRound(round) && round.startAt <= Date.now()
      )
      .sort((left, right) => right.startAt - left.startAt)
      .slice(0, limit)
      .map((round) => ({
        ...round,
        userPnl: this.getRoundUserPnl(round.id, userId, round)
      }));
  }

  getPositions(userId: string) {
    return [...(this.positionsByUserId.get(userId) ?? [])]
      .sort((left, right) => right.openedAt - left.openedAt)
      .map((position) => this.decoratePosition(position));
  }

  getPositionById(positionId: string) {
    const index = this.positionIndexById?.get(positionId);
    return typeof index === "number" ? this.positions[index] : this.positions.find((position) => position.id === positionId);
  }

  getOrders(userId: string) {
    return [...(this.ordersByUserId.get(userId) ?? [])]
      .sort((left, right) => right.createdAt - left.createdAt)
      .map((order) => this.sanitizeOrder(order));
  }

  getRecentTradeOrders(userId: string, limit = 50) {
    return [...(this.ordersByUserId.get(userId) ?? [])]
      .sort((left, right) => right.createdAt - left.createdAt)
      .slice(0, Math.max(1, Math.floor(limit)))
      .map((order) => this.sanitizeOrder(order));
  }

  getOrderById(orderId: string) {
    const index = this.orderIndexById?.get(orderId);
    return typeof index === "number" ? this.orders[index] : this.orders.find((order) => order.id === orderId);
  }

  async findOrderByClientOrderId(userId: string, clientOrderId?: string) {
    const normalizedClientOrderId = clientOrderId?.trim();
    if (!normalizedClientOrderId) {
      return undefined;
    }
    const memoryOrder = this.orders.find((order) => order.userId === userId && order.clientOrderId === normalizedClientOrderId);
    if (memoryOrder) {
      return memoryOrder;
    }
    if (!this.postgresEnabled || !this.pool) {
      return undefined;
    }
    const result = await this.queryDb(
      "SELECT * FROM orders WHERE user_id = $1 AND client_order_id = $2 ORDER BY created_at DESC LIMIT 1",
      [userId, normalizedClientOrderId]
    );
    const row = result.rows[0];
    if (!row) {
      return undefined;
    }
    const order = rowToOrder(row);
    this.upsertOrderInMemory(order);
    this.pruneMemoryCaches();
    return order;
  }

  sanitizeOrder(order: OrderRecord): OrderRecord {
    return {
      ...order,
      orderBookSnapshot: undefined
    };
  }

  getOrderLifecycleLogs(userId: string, options?: { includeSnapshots?: boolean }) {
    return this.orderLifecycleLogs
      .filter((log) => log.userId === userId)
      .sort((left, right) => right.orderTimestampMs - left.orderTimestampMs)
      .map((log) => {
        const snapshot = options?.includeSnapshots && log.orderBookSnapshotRef
          ? this.getOrderBookSnapshot(log.orderBookSnapshotRef)
          : undefined;
        return {
          ...log,
          orderBookSnapshot: snapshot
        };
      });
  }

  getOrderBookSnapshot(ref: string) {
    return this.orderBookSnapshots.get(ref)?.snapshot;
  }

  getRecentLogs(userId: string) {
    const threshold = Date.now() - this.config.logRetentionMs;
    return this.logs
      .filter((log) => log.serverRecvTs >= threshold && (!userId || log.userId === userId))
      .sort((left, right) => right.serverRecvTs - left.serverRecvTs);
  }

  async searchAuditLogs(filters?: LogSearchQuery, options?: { limit?: number; offset?: number }) {
    const limit = normalizeSearchLimit(options?.limit);
    const offset = Math.max(0, Math.floor(options?.offset ?? 0));
    if (filters?.userIds && filters.userIds.length === 0) {
      return [] as AuditEvent[];
    }
    if (filters?.matchingLogKind === "engine") {
      return [] as AuditEvent[];
    }
    if (filters?.eventType || filters?.sequenceFrom || filters?.sequenceTo || filters?.bookKey || filters?.bookSide) {
      return [] as AuditEvent[];
    }

    if (this.postgresEnabled && this.pool) {
      const where: string[] = [];
      const values: unknown[] = [];
      const add = (clause: string, value: unknown) => {
        values.push(value);
        where.push(`${clause} $${values.length}`);
      };

      if (typeof filters?.from === "number") {
        add("server_recv_ts >=", filters.from);
      }
      if (typeof filters?.to === "number") {
        add("server_recv_ts <=", filters.to);
      }
      if (filters?.userId) {
        add("user_id =", filters.userId);
      } else if (filters?.userIds?.length) {
        values.push(filters.userIds);
        where.push(`user_id = ANY($${values.length}::text[])`);
      }
      if (filters?.role) {
        add("role =", filters.role);
      }
      if (filters?.category) {
        add("category =", filters.category);
      }
      if (filters?.logGroup) {
        if (filters.logGroup === "matching_action") {
          add("category =", "matching");
        } else if (filters.logGroup === "market_latency") {
          where.push(`category = 'latency' AND module_name = ANY(ARRAY['binance','coinbase','clob']::text[])`);
        } else if (filters.logGroup === "system_latency") {
          where.push(`category = 'latency' AND module_name <> ALL(ARRAY['binance','coinbase','clob']::text[])`);
        } else {
          add("category =", filters.logGroup);
        }
      }
      if (filters?.actionType) {
        add("action_type =", filters.actionType);
      }
      if (filters?.actionStatus) {
        add("action_status =", filters.actionStatus);
      }
      if (filters?.moduleName) {
        add("module_name =", filters.moduleName);
      }
      if (filters?.pageName) {
        add("page_name =", filters.pageName);
      }
      if (filters?.symbol) {
        add("symbol =", filters.symbol);
      }
      if (filters?.roundId) {
        add("round_id =", filters.roundId);
      }
      if (filters?.traceId) {
        add("trace_id =", filters.traceId);
      }
      if (filters?.resultCode) {
        add("result_code =", filters.resultCode);
      }
      if (filters?.orderId) {
        add("details->>'orderId' =", filters.orderId);
      }
      if (filters?.positionId) {
        add("details->>'positionId' =", filters.positionId);
      }
      if (filters?.marketId) {
        add("details->>'marketId' =", filters.marketId);
      }
      if (filters?.marketSlug) {
        add("details->>'marketSlug' =", filters.marketSlug);
      }
      if (filters?.direction) {
        values.push(filters.direction);
        where.push(`COALESCE(details->>'direction', details->>'side') = $${values.length}`);
      }
      if (filters?.roundStatus) {
        add("details->>'roundStatus' =", filters.roundStatus);
      }
      if (filters?.settlementResult) {
        add("details->>'settlementResult' =", filters.settlementResult);
      }
      if (filters?.latencySource) {
        if (filters.latencySource === "system") {
          where.push(`category = 'latency' AND module_name <> ALL(ARRAY['binance','coinbase','clob']::text[])`);
        } else {
          add("module_name =", filters.latencySource);
          where.push(`category = 'latency'`);
        }
      }
      if (filters?.connectionState) {
        add("details->>'connectionState' =", filters.connectionState);
      }
      const latencyExpression =
        filters?.latencyPhase === "acquire"
          ? `NULLIF(details->>'acquireLatencyMs', '')::double precision`
          : filters?.latencyPhase === "publish"
            ? `NULLIF(details->>'publishLatencyMs', '')::double precision`
            : filters?.latencyPhase === "frontend"
              ? `COALESCE(frontend_latency_ms, NULLIF(details->>'frontendLatencyMs', '')::double precision)`
              : "backend_latency_ms";
      if (typeof filters?.latencyMinMs === "number") {
        add(`${latencyExpression} >=`, filters.latencyMinMs);
      }
      if (typeof filters?.latencyMaxMs === "number") {
        add(`${latencyExpression} <=`, filters.latencyMaxMs);
      }

      values.push(limit, offset);
      try {
        const rows = await this.pool.query(
          `
          SELECT *
          FROM audit_events
          ${where.length ? `WHERE ${where.join(" AND ")}` : ""}
          ORDER BY server_recv_ts DESC, event_id DESC
          LIMIT $${values.length - 1} OFFSET $${values.length}
          `,
          values as never[]
        );
        return rows.rows.map((row) => rowToAuditEvent(row));
      } catch (error) {
        console.warn("[store] PostgreSQL audit search failed, falling back to memory:", error);
      }
    }

    return this.logs
      .filter((log) => matchesAuditSearch(log, filters))
      .sort((left, right) => right.serverRecvTs - left.serverRecvTs || right.eventId.localeCompare(left.eventId))
      .slice(offset, offset + limit);
  }

  async searchBehaviorLogs(filters?: LogSearchQuery, options?: { limit?: number; offset?: number }) {
    const limit = normalizeSearchLimit(options?.limit);
    const offset = Math.max(0, Math.floor(options?.offset ?? 0));
    const effectiveUserIds = resolveBehaviorSearchUserIds(filters, this.users.values());
    if (effectiveUserIds && effectiveUserIds.length === 0) {
      return [] as BehaviorActionLog[];
    }
    if (hasUnsupportedBehaviorSearchFilter(filters)) {
      return [] as BehaviorActionLog[];
    }

    if (this.postgresEnabled && this.pool) {
      const where: string[] = [];
      const values: unknown[] = [];
      const add = (clause: string, value: unknown) => {
        values.push(value);
        where.push(`${clause} $${values.length}`);
      };

      if (typeof filters?.from === "number") {
        add("timestamp_ms >=", filters.from);
      }
      if (typeof filters?.to === "number") {
        add("timestamp_ms <=", filters.to);
      }
      if (effectiveUserIds?.length) {
        values.push(effectiveUserIds.map((userId) => this.anonymizeUserId(userId)));
        where.push(`tester_id_anon = ANY($${values.length}::text[])`);
      }
      if (filters?.roundId) {
        add("round_id =", filters.roundId);
      }
      if (filters?.actionType) {
        add("action_type =", filters.actionType);
      }
      if (filters?.actionStatus) {
        add("action_status =", filters.actionStatus);
      }
      if (filters?.traceId) {
        add("trace_id =", filters.traceId);
      }
      if (filters?.orderId) {
        add("order_id =", filters.orderId);
      }
      if (filters?.marketId) {
        add("market_id =", filters.marketId);
      }
      if (filters?.marketSlug) {
        add("market_slug =", filters.marketSlug);
      }
      if (filters?.direction) {
        add("direction =", filters.direction);
      }
      if (filters?.roundStatus) {
        add("round_status =", filters.roundStatus);
      }
      if (filters?.settlementResult) {
        add("settlement_result =", filters.settlementResult);
      }

      values.push(limit, offset);
      try {
        const rows = await this.pool.query(
          `
          SELECT *
          FROM behavior_action_logs
          ${where.length ? `WHERE ${where.join(" AND ")}` : ""}
          ORDER BY timestamp_ms DESC, log_id DESC
          LIMIT $${values.length - 1} OFFSET $${values.length}
          `,
          values as never[]
        );
        return rows.rows.map((row) => rowToBehaviorLog(row));
      } catch (error) {
        console.warn("[store] PostgreSQL training log search failed, falling back to memory:", error);
      }
    }

    return this.behaviorLogs
      .filter((log) => matchesBehaviorSearch(log, filters, effectiveUserIds, (userId) => this.anonymizeUserId(userId)))
      .sort((left, right) => right.timestampMs - left.timestampMs || right.logId.localeCompare(left.logId))
      .slice(offset, offset + limit);
  }

  getAuditLogs(filters?: AuditLogQuery) {
    return this.logs
      .filter((log) => {
        if (typeof filters?.from === "number" && log.serverRecvTs < filters.from) {
          return false;
        }
        if (typeof filters?.to === "number" && log.serverRecvTs > filters.to) {
          return false;
        }
        if (filters?.userId && log.userId !== filters.userId) {
          return false;
        }
        if (filters?.userIds && !filters.userIds.includes(log.userId ?? "")) {
          return false;
        }
        if (filters?.roundId && log.roundId !== filters.roundId) {
          return false;
        }
        if (filters?.category && log.category !== filters.category) {
          return false;
        }
        if (filters?.actionType && log.actionType !== filters.actionType) {
          return false;
        }
        if (filters?.actionStatus && log.actionStatus !== filters.actionStatus) {
          return false;
        }
        if (filters?.traceId && log.traceId !== filters.traceId) {
          return false;
        }
        if (filters?.resultCode && log.resultCode !== filters.resultCode) {
          return false;
        }
        if (filters?.orderId && detailString(log.details, "orderId") !== filters.orderId) {
          return false;
        }
        if (filters?.positionId && detailString(log.details, "positionId") !== filters.positionId) {
          return false;
        }
        return true;
      })
      .sort((left, right) => right.serverRecvTs - left.serverRecvTs);
  }

  getBehaviorLogs(filters?: BehaviorLogQuery) {
    return this.behaviorLogs
      .filter((log) => {
        if (typeof filters?.from === "number" && log.timestampMs < filters.from) {
          return false;
        }
        if (typeof filters?.to === "number" && log.timestampMs > filters.to) {
          return false;
        }
        if (filters?.userId && log.testerIdAnon !== this.anonymizeUserId(filters.userId)) {
          return false;
        }
        if (filters?.userIds) {
          const allowedAnonIds = new Set(filters.userIds.map((userId) => this.anonymizeUserId(userId)));
          if (!allowedAnonIds.has(log.testerIdAnon)) {
            return false;
          }
        }
        if (filters?.roundId && log.roundId !== filters.roundId) {
          return false;
        }
        if (filters?.actionType && log.actionType !== filters.actionType) {
          return false;
        }
        if (filters?.actionStatus && log.actionStatus !== filters.actionStatus) {
          return false;
        }
        if (filters?.traceId && log.traceId !== filters.traceId) {
          return false;
        }
        if (filters?.orderId && log.orderId !== filters.orderId) {
          return false;
        }
        if (filters?.marketId && log.marketId !== filters.marketId) {
          return false;
        }
        if (filters?.marketSlug && log.marketSlug !== filters.marketSlug) {
          return false;
        }
        return true;
      })
      .sort((left, right) => right.timestampMs - left.timestampMs);
  }

  getTradeTimeline(orderId: string): TradeTimeline | undefined {
    const order = this.getOrderById(orderId);
    if (!order) {
      return undefined;
    }

    const allAuditEvents = this.getAuditLogs();
    const allBehaviorLogs = this.getBehaviorLogs();
    const positionId =
      allAuditEvents
        .filter((event) => event.traceId === order.traceId || detailString(event.details, "orderId") === order.id)
        .map((event) => detailString(event.details, "positionId"))
        .find(Boolean) ??
      allBehaviorLogs
        .filter((log) => log.traceId === order.traceId || log.orderId === order.id)
        .map((log) => detailString(log.contextJson, "positionId"))
        .find(Boolean);
    const rawPosition = positionId
      ? this.positions.find((item) => item.id === positionId)
      : this.positions.find(
          (item) => item.userId === order.userId && item.roundId === order.roundId && item.side === order.side
        );
    const position = rawPosition ? this.decoratePosition(rawPosition) : undefined;
    const auditEvents = allAuditEvents.filter((event) => {
      return (
        event.traceId === order.traceId ||
        detailString(event.details, "orderId") === order.id ||
        Boolean(positionId && detailString(event.details, "positionId") === positionId)
      );
    });
    const behaviorLogs = allBehaviorLogs.filter((log) => {
      return (
        log.traceId === order.traceId ||
        log.orderId === order.id ||
        Boolean(positionId && detailString(log.contextJson, "positionId") === positionId)
      );
    });

    return {
      order,
      position,
      auditEvents,
      behaviorLogs
    };
  }

  getProfile(userId: string): ProfileOverview {
    return buildProfileOverview(this.users.get(userId), this.getPositions(userId), this.getOrders(userId));
  }

  emitUserPayload(userId: string, scope: UserPayloadScope = "full") {
    const existingScope = this.pendingUserPayloadIds.get(userId);
    this.pendingUserPayloadIds.set(userId, existingScope === "full" || scope === "full" ? "full" : "trade");
    if (this.userPayloadFlushScheduled) {
      return;
    }
    this.userPayloadFlushScheduled = true;
    setImmediate(() => this.flushUserPayloads());
  }

  private flushUserPayloads() {
    const payloadRequests = [...this.pendingUserPayloadIds.entries()];
    this.pendingUserPayloadIds.clear();
    this.userPayloadFlushScheduled = false;
    for (const [userId, scope] of payloadRequests) {
      this.emitter.emit(`user:${userId}`, scope);
    }
  }

  private decoratePosition(position: PositionRecord) {
    const round = this.getRoundById(position.roundId);
    const displayStatus = this.getPositionDisplayStatus(position, round);
    const entryFeeUsdc = position.entryFeeUsdc ?? 0;
    const exitFeeUsdc = position.exitFeeUsdc ?? 0;
    const costBasisUsdc = position.costBasisUsdc ?? position.notionalSpent;
    const markValue = position.currentValue ?? position.qty * position.currentMark;
    const executableValue = typeof position.currentBid === "number" ? position.qty * position.currentBid : markValue;
    const feeFields = {
      entryFeeUsdc,
      exitFeeUsdc,
      totalFeeUsdc: position.totalFeeUsdc ?? roundNumber(entryFeeUsdc + exitFeeUsdc, 8),
      costBasisUsdc,
      markPnlUsdc: position.markPnlUsdc ?? roundNumber(markValue - costBasisUsdc, 2),
      executablePnlUsdc: position.executablePnlUsdc ?? roundNumber(executableValue - costBasisUsdc, 2)
    };
    const sanitized =
      displayStatus === "open"
        ? { ...position, ...feeFields }
        : {
            ...position,
            ...feeFields,
            currentBid: undefined,
            currentAsk: undefined,
            currentMid: undefined,
            sourceLatencyMs: undefined,
            currentValue: displayStatus === "sold" ? 0 : position.currentValue
          };
    return {
      ...sanitized,
      displayStatus
    };
  }

  private createOperatedFallbackRound(roundId: string, order?: OrderRecord, position?: PositionRecord): RoundRecord | undefined {
    if (!order && !position) {
      return undefined;
    }
    const fallbackTs = order?.createdAt ?? position?.openedAt ?? Date.now();
    const { startAt, endAt } = inferFiveMinuteWindow({
      roundId,
      marketSlug: order?.marketSlug,
      fallbackTs
    });
    return {
      id: roundId,
      marketId: order?.marketId ?? roundId,
      marketSlug: order?.marketSlug ?? roundId,
      symbol: order?.symbol ?? "BTC",
      startAt,
      endAt,
      priceToBeat: 0,
      status: Date.now() < endAt ? "Trading" : "Closed",
      pollCount: 0
    };
  }

  private getRoundUserPnl(roundId: string, userId: string, round = this.getRoundById(roundId)) {
    return this.positions
      .filter((position) => position.userId === userId && position.roundId === roundId)
      .reduce(
        (sum, position) =>
          sum +
          position.realizedPnl +
          (this.getPositionDisplayStatus(position, round) === "open" ? position.unrealizedPnl : 0),
        0
      );
  }

  private getPositionDisplayStatus(position: PositionRecord, round = this.getRoundById(position.roundId)) {
    if (position.status === "closed") {
      if (position.settlementResult === "sold") {
        return "sold" as const;
      }
      return "settled" as const;
    }
    if (round && Date.now() >= round.endAt) {
      return "pending_settlement" as const;
    }
    return "open" as const;
  }

  getSourceStatus() {
    return Object.values(this.marketSnapshot.sources).map((source: SourceHealth) => source);
  }

  async upsertMarketCandles(candles: MarketCandleRecord[]) {
    const validCandles = dedupeMarketCandles(candles.filter((candle) => isValidMarketCandle(candle)));
    if (validCandles.length === 0) {
      return;
    }
    mergeMarketCandlesIntoMemory(this.marketCandles, validCandles, MARKET_CANDLE_MEMORY_RETENTION_MS);
    if (!this.postgresEnabled || !this.pool) {
      return;
    }

    const values: string[] = [];
    const params: unknown[] = [];
    validCandles.forEach((candle, index) => {
      const offset = index * 12;
      values.push(
        `($${offset + 1},$${offset + 2},$${offset + 3},$${offset + 4},$${offset + 5},$${offset + 6},$${offset + 7},$${offset + 8},$${offset + 9},$${offset + 10},$${offset + 11},$${offset + 12})`
      );
      params.push(
        candle.source,
        candle.symbol,
        candle.interval,
        candle.openTs,
        candle.closeTs,
        candle.open,
        candle.high,
        candle.low,
        candle.close,
        candle.volume,
        candle.origin,
        candle.updatedAt
      );
    });
    await this.runDb(
      `
      INSERT INTO market_candles (
        source, symbol, interval, open_ts, close_ts, open, high, low, close, volume, origin, updated_at
      ) VALUES ${values.join(",")}
      ON CONFLICT (source, symbol, interval, open_ts) DO UPDATE SET
        close_ts = EXCLUDED.close_ts,
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        origin = EXCLUDED.origin,
        updated_at = EXCLUDED.updated_at
      WHERE market_candles.origin <> 'rtds_30s' OR EXCLUDED.origin = 'rtds_30s'
      `,
      params
    );
  }

  getMarketCandles(query: MarketCandleQuery) {
    const key = marketCandleKey(query.source, query.symbol, query.interval);
    const rows = this.marketCandles.get(key) ?? [];
    const filtered = rows.filter(
      (candle) =>
        (typeof query.fromOpenTs !== "number" || candle.openTs >= query.fromOpenTs) &&
        (typeof query.toOpenTs !== "number" || candle.openTs <= query.toOpenTs)
    );
    const limit = query.limit && query.limit > 0 ? query.limit : filtered.length;
    return filtered.slice(-limit).map((candle) => ({ ...candle }));
  }

  async upsertRound(round: RoundRecord) {
    const index = this.rounds.findIndex((item) => item.id === round.id);
    const previous = index >= 0 ? this.rounds[index] : undefined;
    const historyChanged = !previous || this.roundHistorySignature(previous) !== this.roundHistorySignature(round);
    if (index >= 0) {
      this.rounds[index] = round;
    } else {
      this.rounds.push(round);
    }
    this.rounds.sort((left, right) => right.startAt - left.startAt);
    this.pruneMemoryCaches();
    if (historyChanged) {
      this.bumpHistoryRevision();
    }

    await this.runDb(
      `
      INSERT INTO rounds (
        id, market_id, symbol, event_id, market_slug, event_slug, condition_id, series_slug,
        up_token_id, down_token_id, title, resolution_source, start_at, end_at, price_to_beat,
        price_to_beat_source, price_to_beat_captured_at,
        status, poll_count, poll_start_at, last_poll_at, closing_spot_price, settled_side,
        settlement_price, settlement_ts, redeem_start_ts, redeem_finish_ts, manual_reason,
        accepting_orders, closing_price_source, settlement_source, polymarket_settlement_price,
        polymarket_settlement_status, polymarket_open_price, polymarket_close_price,
        polymarket_open_price_source, polymarket_close_price_source,
        settlement_received_at, redeem_scheduled_at,
        binance_open_price, binance_close_price, chainlink_open_price, chainlink_close_price
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,
        $29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43
      )
      ON CONFLICT (id) DO UPDATE SET
        market_id = EXCLUDED.market_id,
        symbol = EXCLUDED.symbol,
        event_id = EXCLUDED.event_id,
        market_slug = EXCLUDED.market_slug,
        event_slug = EXCLUDED.event_slug,
        condition_id = EXCLUDED.condition_id,
        series_slug = EXCLUDED.series_slug,
        up_token_id = EXCLUDED.up_token_id,
        down_token_id = EXCLUDED.down_token_id,
        title = EXCLUDED.title,
        resolution_source = EXCLUDED.resolution_source,
        start_at = EXCLUDED.start_at,
        end_at = EXCLUDED.end_at,
        price_to_beat = EXCLUDED.price_to_beat,
        price_to_beat_source = EXCLUDED.price_to_beat_source,
        price_to_beat_captured_at = EXCLUDED.price_to_beat_captured_at,
        status = EXCLUDED.status,
        poll_count = EXCLUDED.poll_count,
        poll_start_at = EXCLUDED.poll_start_at,
        last_poll_at = EXCLUDED.last_poll_at,
        closing_spot_price = EXCLUDED.closing_spot_price,
        settled_side = EXCLUDED.settled_side,
        settlement_price = EXCLUDED.settlement_price,
        settlement_ts = EXCLUDED.settlement_ts,
        redeem_start_ts = EXCLUDED.redeem_start_ts,
        redeem_finish_ts = EXCLUDED.redeem_finish_ts,
        manual_reason = EXCLUDED.manual_reason,
        accepting_orders = EXCLUDED.accepting_orders,
        closing_price_source = EXCLUDED.closing_price_source,
        settlement_source = EXCLUDED.settlement_source,
        polymarket_settlement_price = EXCLUDED.polymarket_settlement_price,
        polymarket_settlement_status = EXCLUDED.polymarket_settlement_status,
        polymarket_open_price = EXCLUDED.polymarket_open_price,
        polymarket_close_price = EXCLUDED.polymarket_close_price,
        polymarket_open_price_source = EXCLUDED.polymarket_open_price_source,
        polymarket_close_price_source = EXCLUDED.polymarket_close_price_source,
        settlement_received_at = EXCLUDED.settlement_received_at,
        redeem_scheduled_at = EXCLUDED.redeem_scheduled_at,
        binance_open_price = EXCLUDED.binance_open_price,
        binance_close_price = EXCLUDED.binance_close_price,
        chainlink_open_price = EXCLUDED.chainlink_open_price,
        chainlink_close_price = EXCLUDED.chainlink_close_price
      `,
      [
        round.id,
        round.marketId,
        round.symbol,
        round.eventId ?? null,
        round.marketSlug ?? null,
        round.eventSlug ?? null,
        round.conditionId ?? null,
        round.seriesSlug ?? null,
        round.upTokenId ?? null,
        round.downTokenId ?? null,
        round.title ?? null,
        round.resolutionSource ?? null,
        round.startAt,
        round.endAt,
        round.priceToBeat,
        round.priceToBeatSource ?? null,
        round.priceToBeatCapturedAt ?? null,
        round.status,
        round.pollCount,
        round.pollStartAt ?? null,
        round.lastPollAt ?? null,
        round.closingSpotPrice ?? null,
        round.settledSide ?? null,
        round.settlementPrice ?? null,
        round.settlementTs ?? null,
        round.redeemStartTs ?? null,
        round.redeemFinishTs ?? null,
        round.manualReason ?? null,
        round.acceptingOrders ?? null,
        round.closingPriceSource ?? null,
        round.settlementSource ?? null,
        round.polymarketSettlementPrice ?? null,
        round.polymarketSettlementStatus ?? null,
        round.polymarketOpenPrice ?? null,
        round.polymarketClosePrice ?? null,
        round.polymarketOpenPriceSource ?? null,
        round.polymarketClosePriceSource ?? null,
        round.settlementReceivedAt ?? null,
        round.redeemScheduledAt ?? null,
        round.binanceOpenPrice ?? null,
        round.binanceClosePrice ?? null,
        round.coinbaseOpenPrice ?? null,
        round.coinbaseClosePrice ?? null
      ]
    );
  }

  private enqueueOrderBookSnapshot(snapshot: OrderBookSnapshot) {
    const snapshotCopy = cloneOrderBookSnapshot(snapshot);
    const ref = orderBookSnapshotRef(snapshotCopy);
    const existing = this.orderBookSnapshots.get(ref);
    if (!existing) {
      const record: OrderBookSnapshotRecord = {
        ref,
        snapshotId: snapshotCopy.snapshotId,
        snapshotTs: snapshotCopy.snapshotTs,
        bestBid: snapshotCopy.bestBid,
        bestAsk: snapshotCopy.bestAsk,
        midPrice: snapshotCopy.midPrice,
        snapshot: snapshotCopy,
        createdAt: Date.now()
      };
      this.orderBookSnapshots.set(ref, record);
      this.pendingOrderBookSnapshotRecords.set(ref, record);
      this.scheduleOrderBookSnapshotFlush();
    }
    return ref;
  }

  async persistOrderBookSnapshot(snapshot: OrderBookSnapshot) {
    const ref = this.enqueueOrderBookSnapshot(snapshot);
    await this.flushOrderBookSnapshotQueue();
    return ref;
  }

  private scheduleOrderBookSnapshotFlush() {
    if (this.orderBookSnapshotFlushTimer || this.orderBookSnapshotFlushRunning) {
      return;
    }
    txStorage.exit(() => {
      this.orderBookSnapshotFlushTimer = setTimeout(() => {
        this.orderBookSnapshotFlushTimer = undefined;
        void this.flushOrderBookSnapshotQueue();
      }, ORDER_BOOK_SNAPSHOT_FLUSH_DELAY_MS);
    });
  }

  private async flushOrderBookSnapshotQueue() {
    if (this.orderBookSnapshotFlushRunning || this.pendingOrderBookSnapshotRecords.size === 0) {
      return;
    }
    if (!this.postgresEnabled || !this.pool) {
      return;
    }
    this.orderBookSnapshotFlushRunning = true;
    const startedAt = Date.now();
    const records = [...this.pendingOrderBookSnapshotRecords.values()];
    this.pendingOrderBookSnapshotRecords.clear();
    try {
      const values: string[] = [];
      const params: unknown[] = [];
      records.forEach((record, index) => {
        const offset = index * 8;
        values.push(
          `($${offset + 1},$${offset + 2},$${offset + 3},$${offset + 4},$${offset + 5},$${offset + 6},$${offset + 7},$${offset + 8})`
        );
        params.push(
          record.ref,
          record.snapshotId,
          record.snapshotTs,
          record.bestBid,
          record.bestAsk,
          record.midPrice,
          JSON.stringify(record.snapshot),
          record.createdAt
        );
      });
      await this.runDb(
        `
        INSERT INTO order_book_snapshots (
          ref, snapshot_id, snapshot_ts, best_bid, best_ask, mid_price, snapshot, created_at
        ) VALUES ${values.join(",")}
        ON CONFLICT (ref) DO NOTHING
        `,
        params
      );
      this.orderBookSnapshotLastFlushMs = Date.now() - startedAt;
    } catch (error) {
      this.orderBookSnapshotFlushFailures += 1;
      for (const record of records) {
        this.pendingOrderBookSnapshotRecords.set(record.ref, record);
      }
      console.warn("[store] Background order book snapshot flush failed:", error);
    } finally {
      this.orderBookSnapshotFlushRunning = false;
      if (this.pendingOrderBookSnapshotRecords.size > 0) {
        this.scheduleOrderBookSnapshotFlush();
      }
    }
  }

  async persistOrderLifecycle(log: OrderLifecycleRecord) {
    this.upsertOrderLifecycleInMemory(log);
    this.pruneMemoryCaches();
    this.bumpHistoryRevision();

    await this.runDb(
      `
      INSERT INTO order_lifecycle_logs (
        id, buy_order_id, trace_id, user_id, tester_id, round_id, symbol, asset_class, market_id,
        market_slug, direction, order_timestamp_ms, entry_token_price, btc_trade_price,
        btc_open_price_to_beat, delta_btc, volume_token_qty, remaining_token_qty,
        closed_token_qty, position_notional, exit_type, exit_token_price, exit_notional,
        settlement_result, order_book_snapshot_ref, actual_fill_price, slippage_bps,
        match_latency_ms, settlement_time_ms, settlement_direction, entry_fee, exit_fee, fee_currency, created_at, updated_at
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
        $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
        $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,
        $33,$34,$35
      )
      ON CONFLICT (id) DO UPDATE SET
        remaining_token_qty = EXCLUDED.remaining_token_qty,
        closed_token_qty = EXCLUDED.closed_token_qty,
        exit_type = EXCLUDED.exit_type,
        exit_token_price = EXCLUDED.exit_token_price,
        exit_notional = EXCLUDED.exit_notional,
        settlement_result = EXCLUDED.settlement_result,
        settlement_time_ms = EXCLUDED.settlement_time_ms,
        settlement_direction = EXCLUDED.settlement_direction,
        exit_fee = EXCLUDED.exit_fee,
        fee_currency = EXCLUDED.fee_currency,
        updated_at = EXCLUDED.updated_at
      `,
      [
        log.id,
        log.buyOrderId,
        log.traceId,
        log.userId,
        log.testerId,
        log.roundId,
        log.symbol,
        log.assetClass,
        log.marketId,
        log.marketSlug ?? null,
        log.direction,
        log.orderTimestampMs,
        log.entryTokenPrice ?? null,
        log.btcTradePrice ?? null,
        log.btcOpenPriceToBeat ?? null,
        log.deltaBtc ?? null,
        log.volumeTokenQty,
        log.remainingTokenQty,
        log.closedTokenQty,
        log.positionNotional,
        log.exitType ?? null,
        log.exitTokenPrice ?? null,
        log.exitNotional,
        log.settlementResult ?? null,
        log.orderBookSnapshotRef ?? null,
        log.actualFillPrice ?? null,
        log.slippageBps ?? null,
        log.matchLatencyMs,
        log.settlementTimeMs ?? null,
        log.settlementDirection ?? null,
        log.entryFee ?? null,
        log.exitFee ?? null,
        log.feeCurrency ?? null,
        log.createdAt,
        log.updatedAt
      ]
    );
  }

  async applyLifecycleExit(input: {
    userId: string;
    roundId: string;
    side: TradeSide;
    qty: number;
    exitType: Exclude<OrderLifecycleExitType, "settlement">;
    exitTokenPrice?: number;
    exitFee?: number;
    buyOrderIds?: string[];
  }) {
    if (input.qty <= QTY_EPSILON || typeof input.exitTokenPrice !== "number") {
      return;
    }
    const buyOrderIds = input.buyOrderIds ? new Set(input.buyOrderIds) : undefined;
    let remaining = roundNumber(input.qty, 4);
    const logs = this.orderLifecycleLogs
      .filter(
        (log) =>
          log.userId === input.userId &&
          log.roundId === input.roundId &&
          log.direction === input.side &&
          (!buyOrderIds || buyOrderIds.has(log.buyOrderId)) &&
          log.remainingTokenQty > QTY_EPSILON
      )
      .sort((left, right) => left.orderTimestampMs - right.orderTimestampMs);

    const feePerQty = (input.exitFee ?? 0) / Math.max(input.qty, QTY_EPSILON);
    for (const log of logs) {
      if (remaining <= QTY_EPSILON) {
        break;
      }
      const take = roundNumber(Math.min(log.remainingTokenQty, remaining), 4);
      if (take <= QTY_EPSILON) {
        continue;
      }
      const exitNotionalDelta = roundNumber(take * input.exitTokenPrice, 8);
      log.remainingTokenQty = roundNumber(Math.max(log.remainingTokenQty - take, 0), 4);
      log.closedTokenQty = roundNumber(log.closedTokenQty + take, 4);
      log.exitNotional = roundNumber(log.exitNotional + exitNotionalDelta, 8);
      log.exitFee = roundNumber((log.exitFee ?? 0) + take * feePerQty, 8);
      log.feeCurrency = "USD";
      log.exitTokenPrice = roundNumber(log.exitNotional / Math.max(log.closedTokenQty, QTY_EPSILON), 4);
      log.exitType = log.exitType && log.exitType !== input.exitType ? "mixed" : input.exitType;
      log.updatedAt = Date.now();
      await this.persistOrderLifecycle(log);
      remaining = roundNumber(Math.max(remaining - take, 0), 4);
    }
  }

  async settleOpenOrderLifecycles(input: {
    userId: string;
    roundId: string;
    side: TradeSide;
    settlementResult: "win" | "loss";
    settlementDirection: TradeSide;
    settlementTimeMs: number;
    exitTokenPrice: number;
  }) {
    const logs = this.orderLifecycleLogs.filter(
      (log) =>
        log.userId === input.userId &&
        log.roundId === input.roundId &&
        log.direction === input.side &&
        log.remainingTokenQty > QTY_EPSILON
    );
    for (const log of logs) {
      const remaining = log.remainingTokenQty;
      log.closedTokenQty = roundNumber(log.closedTokenQty + remaining, 4);
      log.exitNotional = roundNumber(log.exitNotional + remaining * input.exitTokenPrice, 8);
      log.exitTokenPrice = roundNumber(log.exitNotional / Math.max(log.closedTokenQty, QTY_EPSILON), 4);
      log.remainingTokenQty = 0;
      log.exitType = log.exitType && log.exitType !== "settlement" ? "mixed" : "settlement";
      log.settlementResult = input.settlementResult;
      log.settlementDirection = input.settlementDirection;
      log.settlementTimeMs = input.settlementTimeMs;
      log.feeCurrency = log.feeCurrency ?? "USD";
      log.updatedAt = Date.now();
      await this.persistOrderLifecycle(log);
    }
  }

  async persistOrder(order: OrderRecord) {
    if (order.orderBookSnapshot) {
      this.prepareOrderBookSnapshotForOrder(order);
    }
    this.upsertOrderInMemory(order);
    this.pruneMemoryCaches();
    this.bumpHistoryRevision();

    await this.runDb(
      `
      INSERT INTO orders (
        id, trace_id, user_id, round_id, symbol, market_id, order_kind, time_in_force, limit_price,
        lifecycle_status, result_type, token_id, book_key, book_hash, requested_amount_usdc,
        requested_qty, frozen_usdc, frozen_qty, fills, estimated_fee, actual_fee, fee_breakdown, fee_currency,
        source_latency_ms, market_slug, order_book_snapshot_ref, order_book_snapshot,
        action, side, status, notional_usdc,
        expected_qty, filled_qty, unfilled_qty, avg_fill_price, best_bid, best_ask, mid_price,
        book_snapshot_ts, partial_filled, slippage_bps, match_latency_ms,
        book_acquire_latency_ms, local_match_latency_ms, persist_latency_ms, total_order_latency_ms, failure_reason,
        client_order_id, client_send_ts, server_recv_ts, server_publish_ts, created_at
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
        $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
        $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,
        $31,$32,$33,$34,$35,$36,$37,$38,$39,$40,
        $41,$42,$43,$44,$45,$46,$47,$48,$49,$50,
        $51,$52
      )
      ON CONFLICT (id) DO UPDATE SET
        lifecycle_status = EXCLUDED.lifecycle_status,
        result_type = EXCLUDED.result_type,
        status = EXCLUDED.status,
        filled_qty = EXCLUDED.filled_qty,
        unfilled_qty = EXCLUDED.unfilled_qty,
        avg_fill_price = EXCLUDED.avg_fill_price,
        notional_usdc = EXCLUDED.notional_usdc,
        partial_filled = EXCLUDED.partial_filled,
        slippage_bps = EXCLUDED.slippage_bps,
        match_latency_ms = EXCLUDED.match_latency_ms,
        book_acquire_latency_ms = EXCLUDED.book_acquire_latency_ms,
        local_match_latency_ms = EXCLUDED.local_match_latency_ms,
        persist_latency_ms = EXCLUDED.persist_latency_ms,
        total_order_latency_ms = EXCLUDED.total_order_latency_ms,
        frozen_usdc = EXCLUDED.frozen_usdc,
        frozen_qty = EXCLUDED.frozen_qty,
        fills = EXCLUDED.fills,
        estimated_fee = EXCLUDED.estimated_fee,
        actual_fee = EXCLUDED.actual_fee,
        fee_breakdown = EXCLUDED.fee_breakdown,
        fee_currency = EXCLUDED.fee_currency,
        order_book_snapshot_ref = EXCLUDED.order_book_snapshot_ref,
        failure_reason = EXCLUDED.failure_reason,
        client_order_id = EXCLUDED.client_order_id,
        server_publish_ts = EXCLUDED.server_publish_ts
      `,
      [
        order.id,
        order.traceId,
        order.userId,
        order.roundId,
        order.symbol,
        order.marketId,
        order.orderKind ?? null,
        order.timeInForce ?? null,
        order.limitPrice ?? null,
        order.lifecycleStatus ?? order.status,
        order.resultType ?? null,
        order.tokenId ?? null,
        order.bookKey ?? null,
        order.bookHash ?? null,
        order.requestedAmountUsdc ?? null,
        order.requestedQty ?? null,
        order.frozenUsdc ?? null,
        order.frozenQty ?? null,
        JSON.stringify(order.fills ?? []),
        order.estimatedFee ?? null,
        order.actualFee ?? null,
        JSON.stringify(order.feeBreakdown ?? null),
        order.feeCurrency ?? null,
        order.sourceLatencyMs ?? null,
        order.marketSlug ?? null,
        order.orderBookSnapshotRef ?? null,
        null,
        order.action,
        order.side,
        order.status,
        order.notionalUsdc,
        order.expectedQty,
        order.filledQty,
        order.unfilledQty,
        order.avgFillPrice ?? null,
        order.bestBid,
        order.bestAsk,
        order.midPrice,
        order.bookSnapshotTs,
        order.partialFilled,
        order.slippageBps ?? null,
        order.matchLatencyMs,
        order.bookAcquireLatencyMs ?? null,
        order.localMatchLatencyMs ?? null,
        order.persistLatencyMs ?? null,
        order.totalOrderLatencyMs ?? null,
        order.failureReason ?? null,
        order.clientOrderId ?? null,
        order.clientSendTs ?? null,
        order.serverRecvTs,
        order.serverPublishTs,
        order.createdAt
      ]
    );
  }

  async persistPosition(position: PositionRecord) {
    this.upsertPositionInMemory(position);
    this.pruneMemoryCaches();
    this.bumpHistoryRevision();

    await this.runDb(
      `
      INSERT INTO positions (
        id, buy_order_id, user_id, round_id, side, qty, locked_qty, average_entry, notional_spent, current_mark,
        current_bid, current_ask, current_mid, current_value, source_latency_ms,
        unrealized_pnl, realized_pnl, entry_fee_usdc, exit_fee_usdc, total_fee_usdc,
        cost_basis_usdc, mark_pnl_usdc, executable_pnl_usdc, status, opened_at, closed_at, settlement_result
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
        $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
        $21,$22,$23,$24,$25,$26,$27
      )
      ON CONFLICT (id) DO UPDATE SET
        buy_order_id = EXCLUDED.buy_order_id,
        qty = EXCLUDED.qty,
        locked_qty = EXCLUDED.locked_qty,
        average_entry = EXCLUDED.average_entry,
        notional_spent = EXCLUDED.notional_spent,
        current_mark = EXCLUDED.current_mark,
        current_bid = EXCLUDED.current_bid,
        current_ask = EXCLUDED.current_ask,
        current_mid = EXCLUDED.current_mid,
        current_value = EXCLUDED.current_value,
        source_latency_ms = EXCLUDED.source_latency_ms,
        unrealized_pnl = EXCLUDED.unrealized_pnl,
        realized_pnl = EXCLUDED.realized_pnl,
        entry_fee_usdc = EXCLUDED.entry_fee_usdc,
        exit_fee_usdc = EXCLUDED.exit_fee_usdc,
        total_fee_usdc = EXCLUDED.total_fee_usdc,
        cost_basis_usdc = EXCLUDED.cost_basis_usdc,
        mark_pnl_usdc = EXCLUDED.mark_pnl_usdc,
        executable_pnl_usdc = EXCLUDED.executable_pnl_usdc,
        status = EXCLUDED.status,
        closed_at = EXCLUDED.closed_at,
        settlement_result = EXCLUDED.settlement_result
      `,
      [
        position.id,
        position.buyOrderId ?? null,
        position.userId,
        position.roundId,
        position.side,
        position.qty,
        position.lockedQty ?? 0,
        position.averageEntry,
        position.notionalSpent,
        position.currentMark,
        position.currentBid ?? null,
        position.currentAsk ?? null,
        position.currentMid ?? null,
        position.currentValue ?? null,
        position.sourceLatencyMs ?? null,
        position.unrealizedPnl,
        position.realizedPnl,
        position.entryFeeUsdc ?? 0,
        position.exitFeeUsdc ?? 0,
        position.totalFeeUsdc ?? roundNumber((position.entryFeeUsdc ?? 0) + (position.exitFeeUsdc ?? 0), 8),
        position.costBasisUsdc ?? position.notionalSpent,
        position.markPnlUsdc ?? roundNumber((position.currentValue ?? position.qty * position.currentMark) - (position.costBasisUsdc ?? position.notionalSpent), 2),
        position.executablePnlUsdc ?? roundNumber(((position.currentBid ?? position.currentMark) * position.qty) - (position.costBasisUsdc ?? position.notionalSpent), 2),
        position.status,
        position.openedAt,
        position.closedAt ?? null,
        position.settlementResult ?? null
      ]
    );
  }

  async recordLog(event: AuditEvent, options?: { emitUserPayload?: boolean; payloadScope?: UserPayloadScope }) {
    this.logs.unshift(event);
    this.pruneMemoryCaches(event.serverRecvTs);
    this.auditLogWriter.write(event);
    await this.runDb(
      `
      INSERT INTO audit_events (
        event_id, trace_id, category, action_type, action_status, user_id, role,
        page_name, module_name, symbol, round_id, result_code, result_message,
        client_send_ts, server_recv_ts, engine_start_ts, engine_finish_ts,
        server_publish_ts, backend_latency_ms, frontend_latency_ms, details
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,
        $8,$9,$10,$11,$12,$13,
        $14,$15,$16,$17,
        $18,$19,$20,$21
      )
      ON CONFLICT (event_id) DO NOTHING
      `,
      [
        event.eventId,
        event.traceId,
        event.category,
        event.actionType,
        event.actionStatus,
        event.userId ?? null,
        event.role ?? null,
        event.pageName,
        event.moduleName,
        event.symbol ?? null,
        event.roundId ?? null,
        event.resultCode,
        event.resultMessage,
        event.clientSendTs ?? null,
        event.serverRecvTs,
        event.engineStartTs ?? null,
        event.engineFinishTs ?? null,
        event.serverPublishTs,
        event.backendLatencyMs,
        event.frontendLatencyMs ?? null,
        JSON.stringify(event.details ?? {})
      ]
    );
    this.cleanupRetentionIfDue(event.serverRecvTs);
    if (event.userId && options?.emitUserPayload !== false) {
      this.emitUserPayload(event.userId, options?.payloadScope ?? "full");
    }
  }

  async recordBehaviorLog(log: BehaviorActionLog) {
    this.behaviorLogs.unshift(log);
    this.pruneMemoryCaches(log.timestampMs);
    this.behaviorLogWriter.write(log);
    await this.runDb(
      `
      INSERT INTO behavior_action_logs (
        log_id, timestamp_ms, asset_class, action_type, action_status, round_id, direction,
        entry_odds, delta_clob, volume_clob, position_notional, exit_type, exit_odds,
        settlement_result, tester_id_anon, trace_id, order_id, market_id, market_slug,
        round_status, countdown_ms, binance_spot_price, binance_1m_last_close,
        binance_5m_last_close, binance_1d_last_close, chainlink_price, price_to_beat,
        up_price, down_price, up_book_top5, down_book_top5, recent_trades_top20,
        book_snapshot_entry, actual_fill_price, slippage_bps, partial_filled, unfilled_qty,
        execution_latency_ms, settlement_direction, settlement_time_ms, gamma_poll_count,
        redeem_finish_time_ms, source_states, strategy_cluster_label, market_regime_label,
        quality_grade, context_json
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,
        $8,$9,$10,$11,$12,$13,
        $14,$15,$16,$17,$18,$19,
        $20,$21,$22,$23,
        $24,$25,$26,$27,
        $28,$29,$30,$31,$32,
        $33,$34,$35,$36,$37,
        $38,$39,$40,$41,
        $42,$43,$44,$45,
        $46,$47
      )
      ON CONFLICT (log_id) DO NOTHING
      `,
      [
        log.logId,
        log.timestampMs,
        log.assetClass,
        log.actionType,
        log.actionStatus,
        log.roundId ?? null,
        log.direction ?? null,
        log.entryOdds ?? null,
        log.deltaClob,
        log.volumeClob,
        log.positionNotional ?? null,
        log.exitType ?? null,
        log.exitOdds ?? null,
        log.settlementResult ?? null,
        log.testerIdAnon,
        log.traceId ?? null,
        log.orderId ?? null,
        log.marketId ?? null,
        log.marketSlug ?? null,
        log.roundStatus ?? null,
        log.countdownMs ?? null,
        log.binanceSpotPrice,
        log.binance1mLastClose,
        log.binance5mLastClose,
        log.binance1dLastClose,
        log.coinbasePrice,
        log.priceToBeat,
        log.upPrice,
        log.downPrice,
        JSON.stringify(log.upBookTop5),
        JSON.stringify(log.downBookTop5),
        JSON.stringify(log.recentTradesTop20),
        JSON.stringify(log.bookSnapshotEntry),
        log.actualFillPrice ?? null,
        log.slippageBps ?? null,
        log.partialFilled ?? null,
        log.unfilledQty ?? null,
        log.executionLatencyMs ?? null,
        log.settlementDirection ?? null,
        log.settlementTimeMs ?? null,
        log.gammaPollCount ?? null,
        log.redeemFinishTimeMs ?? null,
        JSON.stringify(log.sourceStates),
        log.strategyClusterLabel ?? null,
        log.marketRegimeLabel ?? null,
        log.qualityGrade ?? null,
        JSON.stringify(log.contextJson ?? {})
      ]
    );
  }

  async recordExportAudit(input: {
    exportId: string;
    actorUserId: string;
    actorRole: Role;
    exportType: string;
    format: string;
    scope: Record<string, unknown>;
    recordCount: number;
    filteredDGradeCount: number;
    missingQualityCount: number;
    fileSha256: string;
    createdAtMs: number;
    details?: Record<string, unknown>;
  }) {
    await this.runDb(
      `
      INSERT INTO export_audit_logs (
        export_id, actor_user_id, actor_role, export_type, format, scope, record_count,
        filtered_d_grade_count, missing_quality_count, file_sha256, created_at_ms, details
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
      ON CONFLICT (export_id) DO NOTHING
      `,
      [
        input.exportId,
        input.actorUserId,
        input.actorRole,
        input.exportType,
        input.format,
        JSON.stringify(input.scope),
        input.recordCount,
        input.filteredDGradeCount,
        input.missingQualityCount,
        input.fileSha256,
        input.createdAtMs,
        JSON.stringify(input.details ?? {})
      ]
    );
  }

  anonymizeUserId(userId: string) {
    return createHash("sha256").update(`paper-trading:${userId}`).digest("hex").slice(0, 16);
  }

  newTraceId() {
    return `tr_${nanoid(10)}`;
  }

  newId(prefix: string) {
    return `${prefix}_${nanoid(12)}`;
  }

  private notePersistenceSuccess(target: "postgres" | "redis") {
    const health = this.persistenceHealth[target];
    const recovered = target === "postgres" && (health.state !== "healthy" || health.reconnecting || !health.lastRecoveryAt);
    health.enabled = true;
    health.writable = true;
    health.state = "healthy";
    health.reconnecting = false;
    health.reconnectAttempts = 0;
    health.consecutiveFailures = 0;
    health.lastError = undefined;
    health.lastFailureAt = undefined;
    if (recovered) {
      health.lastRecoveryAt = Date.now();
    }
  }

  private notePersistenceFailure(target: "postgres" | "redis", error: unknown) {
    const health = this.persistenceHealth[target];
    health.enabled = false;
    health.state = "blocked";
    health.reconnecting = false;
    health.consecutiveFailures += 1;
    health.lastError = error instanceof Error ? error.message : String(error);
    health.lastFailureAt = Date.now();
    if (health.strict && health.consecutiveFailures >= PERSISTENCE_FAILURE_THRESHOLD) {
      health.writable = false;
    }
  }

  private createPostgresPool() {
    const pool = new Pool({
      connectionString: this.config.databaseUrl,
      connectionTimeoutMillis: this.config.pgConnectionTimeoutMs,
      idleTimeoutMillis: this.config.pgIdleTimeoutMs,
      max: this.config.pgMaxConnections,
      keepAlive: this.config.pgKeepAlive,
      keepAliveInitialDelayMillis: this.config.pgKeepAlive ? 10_000 : undefined
    });
    pool.on("error", (error) => {
      if (this.closed) {
        return;
      }
      // `pg` emits this for idle clients; the pool has already removed the
      // broken client, so keep the pool available and let active query failures
      // drive strict persistence reconnects.
      console.warn("[store] PostgreSQL idle client error:", error);
    });
    return pool;
  }

  private async closePostgresPool() {
    const pool = this.pool;
    this.pool = undefined;
    if (pool) {
      await pool.end().catch(() => undefined);
    }
  }

  private persistenceUnavailableMessage(context: string) {
    const health = this.persistenceHealth.postgres;
    const base = `${context} is blocked because persistent storage is unavailable.`;
    if (health.reconnecting || health.state === "reconnecting") {
      return `${base} PostgreSQL is reconnecting.`;
    }
    if (health.lastError) {
      return `${base} ${health.lastError}`;
    }
    return `${base} PostgreSQL persistence is unavailable.`;
  }

  private async handlePostgresFailure(error: unknown, source: string) {
    this.postgresEnabled = false;
    this.notePersistenceFailure("postgres", error);
    await this.closePostgresPool();
    console.warn(`[store] PostgreSQL ${source} failed:`, error);
    void this.schedulePostgresReconnect(source);
  }

  private async schedulePostgresReconnect(reason: string) {
    if (this.closed || this.config.persistenceMode === "memory") {
      return;
    }
    if (this.postgresReconnectTask) {
      return this.postgresReconnectTask;
    }
    const health = this.persistenceHealth.postgres;
    health.enabled = false;
    health.writable = false;
    health.state = "reconnecting";
    health.reconnecting = true;
    console.warn(`[store] PostgreSQL entering reconnecting state (${reason}).`);
    this.postgresReconnectTask = this.reconnectPostgresLoop()
      .catch((error) => {
        console.warn("[store] PostgreSQL reconnect loop stopped with error:", error);
      })
      .finally(() => {
        this.postgresReconnectTask = undefined;
        if (!this.postgresEnabled && !this.closed) {
          health.state = "blocked";
          health.reconnecting = false;
        }
      });
    return this.postgresReconnectTask;
  }

  private async reconnectPostgresLoop() {
    const health = this.persistenceHealth.postgres;
    let delayMs = this.config.pgReconnectIntervalMs;
    while (!this.closed && this.config.persistenceMode !== "memory" && !this.postgresEnabled) {
      health.reconnectAttempts += 1;
      try {
        await this.closePostgresPool();
        const pool = this.createPostgresPool();
        await pool.query("SELECT 1");
        if (this.config.allowDevSchemaBootstrap) {
          await pool.query(SCHEMA_SQL);
        }
        await this.assertSchemaMigrations(pool);
        this.pool = pool;
        this.postgresEnabled = true;
        this.notePersistenceSuccess("postgres");
        console.log("[store] PostgreSQL reconnect succeeded.");
        return;
      } catch (error) {
        this.postgresEnabled = false;
        this.notePersistenceFailure("postgres", error);
        health.state = "reconnecting";
        health.reconnecting = true;
        console.warn(
          `[store] PostgreSQL reconnect attempt ${health.reconnectAttempts} failed; retrying in ${delayMs}ms:`,
          error
        );
        await sleep(delayMs);
        delayMs = Math.min(delayMs * 2, this.config.pgReconnectMaxIntervalMs);
      }
    }
  }

  private async connectPostgres() {
    if (this.config.persistenceMode === "memory") {
      console.warn("[store] PERSISTENCE_MODE=memory; skipping PostgreSQL connection.");
      this.persistenceHealth.postgres.enabled = false;
      this.persistenceHealth.postgres.writable = false;
      this.persistenceHealth.postgres.state = "blocked";
      return;
    }

    let lastError: unknown;

    for (let attempt = 1; attempt <= STARTUP_CONNECT_RETRY_ATTEMPTS; attempt += 1) {
      try {
        this.pool = this.createPostgresPool();
        await this.pool.query("SELECT 1");
        if (this.config.allowDevSchemaBootstrap) {
          await this.pool.query(SCHEMA_SQL);
        }
        await this.assertSchemaMigrations(this.pool);
        this.postgresEnabled = true;
        this.notePersistenceSuccess("postgres");
        return;
      } catch (error) {
        lastError = error;
        this.postgresEnabled = false;
        this.notePersistenceFailure("postgres", error);
        await this.closePostgresPool();
        if (attempt < STARTUP_CONNECT_RETRY_ATTEMPTS) {
          console.warn(
            `[store] PostgreSQL is not ready yet (attempt ${attempt}/${STARTUP_CONNECT_RETRY_ATTEMPTS}); retrying in ${STARTUP_CONNECT_RETRY_DELAY_MS}ms`
          );
          await sleep(STARTUP_CONNECT_RETRY_DELAY_MS);
        }
      }
    }

    if (this.config.strictPersistence) {
      throw new Error(
        `[store] PostgreSQL is required but unavailable: ${
          lastError instanceof Error ? lastError.message : String(lastError)
        }`
      );
    }
    console.warn("[store] PostgreSQL is unavailable, using in-memory persistence only:", lastError);
  }

  private async assertSchemaMigrations(pool: Pool) {
    if (!this.config.requireSchemaMigrations) {
      return;
    }
    const tableResult = await pool.query<{ exists: boolean }>(
      "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'schema_migrations') AS exists"
    );
    if (!tableResult.rows[0]?.exists) {
      throw new Error(
        "[store] SERVER_REQUIRE_MIGRATIONS=true but schema_migrations is missing. Run npm run db:migrate first."
      );
    }
    const migrationResult = await pool.query("SELECT 1 FROM schema_migrations WHERE id = $1", [
      this.config.expectedSchemaMigrationId
    ]);
    if (migrationResult.rowCount === 0) {
      throw new Error(
        `[store] Required migration ${this.config.expectedSchemaMigrationId} is not applied. Run npm run db:migrate first.`
      );
    }
  }

  private async connectRedis() {
    if (this.config.persistenceMode === "memory") {
      console.warn("[store] PERSISTENCE_MODE=memory; skipping Redis connection.");
      this.persistenceHealth.redis.enabled = false;
      this.persistenceHealth.redis.writable = false;
      return;
    }

    let lastError: unknown;

    for (let attempt = 1; attempt <= STARTUP_CONNECT_RETRY_ATTEMPTS; attempt += 1) {
      try {
        const client = createClient({
          url: this.config.redisUrl,
          socket: {
            connectTimeout: 3000,
            reconnectStrategy: false
          }
        });
        client.on("error", (error) => {
          this.redisEnabled = false;
          this.notePersistenceFailure("redis", error);
          console.warn("[store] Redis connection error:", error);
        });
        await client.connect();
        this.redis = client;
        this.redisEnabled = true;
        this.notePersistenceSuccess("redis");
        return;
      } catch (error) {
        lastError = error;
        this.redisEnabled = false;
        this.notePersistenceFailure("redis", error);
        if (this.redis?.isOpen) {
          await this.redis.quit().catch(() => undefined);
        }
        this.redis = undefined;
        if (attempt < STARTUP_CONNECT_RETRY_ATTEMPTS) {
          console.warn(
            `[store] Redis is not ready yet (attempt ${attempt}/${STARTUP_CONNECT_RETRY_ATTEMPTS}); retrying in ${STARTUP_CONNECT_RETRY_DELAY_MS}ms`
          );
          await sleep(STARTUP_CONNECT_RETRY_DELAY_MS);
        }
      }
    }

    console.warn("[store] Redis is unavailable, skipping snapshot cache:", lastError);
  }

  private async seedUsers() {
    const seededUsers = [
      ["u_tester", "tester", "tester123", "Tester A", "Tester", "zh-CN", "u_senior"],
      ["u_senior", "senior", "senior123", "Senior Tester", "Senior Tester", "en-US", undefined],
      ["u_engineer", "engineer", "engineer123", "Test Engineer", "Test Engineer", "zh-CN", undefined],
      ["u_admin", "admin", "admin123", "Admin", "Admin", "zh-CN", undefined]
    ] as const;

    for (const [id, username, password, displayName, role, language, seniorTesterId] of seededUsers) {
      if (!this.postgresEnabled || !this.pool) {
        if (!this.users.has(id)) {
          const now = Date.now();
          this.users.set(id, {
            id,
            username,
            password: hashPassword(password),
            displayName,
            role,
            language,
            permissionCodes: ROLE_PERMISSIONS[role],
            availableUsdc: this.config.initialBalance,
            isActive: true,
            seniorTesterId,
            createdAt: now,
            updatedAt: now
          });
        }
        continue;
      }

      const existing = await this.pool.query("SELECT id FROM users WHERE id = $1", [id]);
      if (!existing.rowCount) {
        const now = Date.now();
        await this.pool.query(
          `
          INSERT INTO users (
            id, username, password, display_name, role, language, permission_codes, available_usdc,
            is_active, senior_tester_id, created_at, updated_at
          )
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
          `,
          [
            id,
            username,
            hashPassword(password),
            displayName,
            role,
            language,
            JSON.stringify(ROLE_PERMISSIONS[role]),
            this.config.initialBalance,
            true,
            seniorTesterId ?? null,
            now,
            now
          ]
        );
      } else if (seniorTesterId) {
        await this.pool.query(
          "UPDATE users SET senior_tester_id = COALESCE(senior_tester_id, $2), updated_at = COALESCE(updated_at, created_at) WHERE id = $1",
          [id, seniorTesterId]
        );
      }
    }
  }

  private async loadStateFromPersistence() {
    if (this.postgresEnabled && this.pool) {
      const [
        userRows,
        roundRows,
        marketCandleRows,
        snapshotRows,
        orderRows,
        lifecycleRows,
        positionRows,
        logRows,
        behaviorRows
      ] = await Promise.all([
        this.pool.query("SELECT * FROM users ORDER BY created_at ASC"),
        this.pool.query("SELECT * FROM rounds ORDER BY start_at DESC LIMIT 80"),
        this.pool.query(
          "SELECT * FROM market_candles WHERE source = $1 AND symbol = $2 AND interval = $3 AND open_ts >= $4 ORDER BY open_ts ASC",
          ["coinbase", this.config.symbol, "30s", Date.now() - MARKET_CANDLE_MEMORY_RETENTION_MS]
        ),
        this.pool.query("SELECT * FROM order_book_snapshots ORDER BY snapshot_ts DESC LIMIT 5000"),
        this.pool.query("SELECT * FROM orders ORDER BY created_at DESC LIMIT 2000"),
        this.pool.query("SELECT * FROM order_lifecycle_logs ORDER BY order_timestamp_ms DESC LIMIT 5000"),
        this.pool.query("SELECT * FROM positions ORDER BY opened_at DESC LIMIT 2000"),
        this.pool.query(
          "SELECT * FROM audit_events WHERE server_recv_ts >= $1 ORDER BY server_recv_ts DESC LIMIT 2000",
          [Date.now() - this.config.logRetentionMs]
        ),
        this.pool.query("SELECT * FROM behavior_action_logs ORDER BY timestamp_ms DESC LIMIT 5000")
      ]);

      this.users.clear();
      for (const row of userRows.rows) {
        const createdAt = Number(row.created_at);
        const role = String(row.role) as Role;
        this.users.set(row.id, {
          id: row.id,
          username: row.username,
          password: row.password,
          displayName: row.display_name,
          role,
          language: row.language,
          permissionCodes: normalizeRolePermissions(role, normalizePermissionCodes(row.permission_codes)),
          availableUsdc: Number(row.available_usdc),
          isActive: row.is_active === null || typeof row.is_active === "undefined" ? true : Boolean(row.is_active),
          seniorTesterId: row.senior_tester_id ? String(row.senior_tester_id) : undefined,
          managerUserId: row.manager_user_id
            ? String(row.manager_user_id)
            : row.senior_tester_id
              ? String(row.senior_tester_id)
              : undefined,
          permissionLevel: row.permission_level === "Initial" ? "Initial" : "Standard",
          failedLoginCount: Number(row.failed_login_count ?? 0),
          lockedUntil: row.locked_until ? Number(row.locked_until) : undefined,
          passwordChangedAt: row.password_changed_at ? Number(row.password_changed_at) : undefined,
          lastLoginAt: row.last_login_at ? Number(row.last_login_at) : undefined,
          mustChangePassword: Boolean(row.must_change_password),
          disabledAt: row.disabled_at ? Number(row.disabled_at) : undefined,
          disabledBy: row.disabled_by ? String(row.disabled_by) : undefined,
          createdAt,
          updatedAt: row.updated_at ? Number(row.updated_at) : createdAt
        });
      }

      this.rounds.splice(0, this.rounds.length, ...roundRows.rows.map((row) => rowToRound(row)));
      this.marketCandles.clear();
      mergeMarketCandlesIntoMemory(
        this.marketCandles,
        marketCandleRows.rows.map((row) => rowToMarketCandle(row)),
        MARKET_CANDLE_MEMORY_RETENTION_MS
      );
      this.orderBookSnapshots.clear();
      for (const row of snapshotRows.rows) {
        const snapshotRecord = rowToOrderBookSnapshotRecord(row);
        this.orderBookSnapshots.set(snapshotRecord.ref, snapshotRecord);
      }
      this.orders.splice(0, this.orders.length, ...orderRows.rows.map((row) => rowToOrder(row)));
      this.orderLifecycleLogs.splice(
        0,
        this.orderLifecycleLogs.length,
        ...lifecycleRows.rows.map((row) => rowToOrderLifecycle(row))
      );
      this.positions.splice(0, this.positions.length, ...positionRows.rows.map((row) => rowToPosition(row)));
      this.logs.splice(0, this.logs.length, ...logRows.rows.map((row) => rowToAuditEvent(row)));
      this.behaviorLogs.splice(
        0,
        this.behaviorLogs.length,
        ...behaviorRows.rows.map((row) => rowToBehaviorLog(row))
      );
      this.rebuildHotIndexes();
      await this.rebuildLegacyOpenPositionLotsFromLifecycle();
      this.pruneMemoryCaches();
    }

    if (this.redisEnabled && this.redis?.isOpen) {
      const snapshotJson = await this.redis.get(this.snapshotCacheKey);
      if (snapshotJson) {
        this.marketSnapshot = JSON.parse(snapshotJson) as MarketSnapshot;
      }
    }
  }

  private legacyPositionLotId(buyOrderId: string) {
    return `pos_lot_${createHash("sha256").update(`position-lot:${buyOrderId}`).digest("hex").slice(0, 24)}`;
  }

  private async rebuildLegacyOpenPositionLotsFromLifecycle(now = Date.now()) {
    const groupKey = (input: { userId: string; roundId: string; side: TradeSide }) =>
      `${input.userId}\u0000${input.roundId}\u0000${input.side}`;
    const legacyGroups = new Map<
      string,
      { userId: string; roundId: string; side: TradeSide; positions: PositionRecord[] }
    >();
    const groupsWithOpenLots = new Set<string>();

    for (const position of this.positions) {
      if (position.status !== "open") {
        continue;
      }
      const key = groupKey(position);
      if (position.buyOrderId) {
        groupsWithOpenLots.add(key);
        continue;
      }
      const group = legacyGroups.get(key) ?? {
        userId: position.userId,
        roundId: position.roundId,
        side: position.side,
        positions: []
      };
      group.positions.push(position);
      legacyGroups.set(key, group);
    }

    let migratedGroups = 0;
    for (const [key, group] of legacyGroups) {
      if (groupsWithOpenLots.has(key) || group.positions.some((position) => (position.lockedQty ?? 0) > QTY_EPSILON)) {
        continue;
      }
      const lots = this.orderLifecycleLogs
        .filter(
          (log) =>
            log.userId === group.userId &&
            log.roundId === group.roundId &&
            log.direction === group.side &&
            log.remainingTokenQty > QTY_EPSILON
        )
        .sort((left, right) => left.orderTimestampMs - right.orderTimestampMs);
      if (lots.length === 0) {
        continue;
      }

      const legacyQty = roundNumber(
        group.positions.reduce((sum, position) => sum + Math.max(position.qty, 0), 0),
        4
      );
      const lifecycleQty = roundNumber(
        lots.reduce((sum, log) => sum + Math.max(log.remainingTokenQty, 0), 0),
        4
      );
      if (Math.abs(legacyQty - lifecycleQty) > 0.0001) {
        console.warn(
          `[store] skipped legacy position lot rebuild for ${group.userId}/${group.roundId}/${group.side}: position qty ${legacyQty} != lifecycle qty ${lifecycleQty}`
        );
        continue;
      }

      const aggregate = group.positions[0];
      if (!aggregate) {
        continue;
      }

      for (const log of lots) {
        const qty = roundNumber(log.remainingTokenQty, 4);
        const volumeQty = Math.max(log.volumeTokenQty, QTY_EPSILON);
        const entryFeeUsdc = roundNumber(((log.entryFee ?? 0) * qty) / volumeQty, 8);
        const fullNotionalWithoutFee = Math.max(log.positionNotional - (log.entryFee ?? 0), 0);
        const fallbackNotional = typeof log.entryTokenPrice === "number" ? log.entryTokenPrice * qty : 0;
        const notionalSpent = roundNumber(
          fullNotionalWithoutFee > QTY_EPSILON ? (fullNotionalWithoutFee * qty) / volumeQty : fallbackNotional,
          4
        );
        const currentMark = roundNumber(aggregate.currentMark, 4);
        const currentBid = typeof aggregate.currentBid === "number" ? roundNumber(aggregate.currentBid, 4) : undefined;
        const currentValue = roundNumber(qty * currentMark, 2);
        const markPnlUsdc = roundNumber(currentValue - notionalSpent, 2);
        const executablePnlUsdc =
          typeof currentBid === "number" ? roundNumber(currentBid * qty - notionalSpent, 2) : markPnlUsdc;
        const position: PositionRecord = {
          id: this.legacyPositionLotId(log.buyOrderId),
          buyOrderId: log.buyOrderId,
          userId: group.userId,
          roundId: group.roundId,
          side: group.side,
          qty,
          lockedQty: 0,
          averageEntry: roundNumber(notionalSpent / Math.max(qty, QTY_EPSILON), 4),
          notionalSpent,
          currentMark,
          currentBid,
          currentAsk: typeof aggregate.currentAsk === "number" ? roundNumber(aggregate.currentAsk, 4) : undefined,
          currentMid: typeof aggregate.currentMid === "number" ? roundNumber(aggregate.currentMid, 4) : undefined,
          currentValue,
          sourceLatencyMs: aggregate.sourceLatencyMs,
          unrealizedPnl: markPnlUsdc,
          realizedPnl: 0,
          entryFeeUsdc,
          exitFeeUsdc: 0,
          totalFeeUsdc: entryFeeUsdc,
          costBasisUsdc: notionalSpent,
          markPnlUsdc,
          executablePnlUsdc,
          status: "open",
          openedAt: log.orderTimestampMs
        };
        await this.persistPosition(position);
      }

      for (const position of group.positions) {
        await this.persistPosition({
          ...position,
          qty: 0,
          lockedQty: 0,
          currentValue: 0,
          unrealizedPnl: 0,
          markPnlUsdc: 0,
          executablePnlUsdc: 0,
          status: "closed",
          closedAt: position.closedAt ?? now,
          settlementResult: "sold"
        });
      }
      migratedGroups += 1;
    }

    if (migratedGroups > 0) {
      console.log(`[store] rebuilt ${migratedGroups} legacy open position group(s) into order-level lots`);
    }
  }

  private cleanupRetentionIfDue(now = Date.now()) {
    if (now - this.lastMemoryGuardAt >= MEMORY_GUARD_INTERVAL_MS) {
      this.lastMemoryGuardAt = now;
      this.updateMemoryProtectionState();
      if (this.memoryProtectionState !== "normal") {
        this.pruneMemoryCaches(now);
      }
    }
    if (!this.retentionCleanupRunning && now - this.lastRetentionCleanupAt >= RETENTION_CLEANUP_INTERVAL_MS) {
      this.lastRetentionCleanupAt = now;
      this.retentionCleanupRunning = true;
      void this.cleanupRetention(now)
        .catch((error) => console.warn("[store] Log retention cleanup failed:", error))
        .finally(() => {
          this.retentionCleanupRunning = false;
        });
    }
  }

  private async cleanupRetention(now = Date.now()) {
    const threshold = now - this.config.logRetentionMs;
    const retained = this.logs
      .filter((log) => log.serverRecvTs >= threshold)
      .sort((left, right) => right.serverRecvTs - left.serverRecvTs);
    this.logs.splice(0, this.logs.length, ...retained);
    await this.runDb("DELETE FROM audit_events WHERE server_recv_ts < $1", [threshold]);
    await this.pruneLogFile(threshold);
  }

  private async pruneLogFile(threshold: number) {
    if (!existsSync(LOG_FILE)) {
      return;
    }
    const filtered = readJsonlTail<AuditEvent>(
      LOG_FILE,
      LOG_FILE_TAIL_BYTES,
      (event): event is AuditEvent =>
        Boolean(
          event &&
            typeof event === "object" &&
            typeof (event as AuditEvent).eventId === "string" &&
            typeof (event as AuditEvent).serverRecvTs === "number"
        )
    ).filter((event) => event.serverRecvTs >= threshold);
    await this.auditLogWriter.rewrite(filtered);
  }

  private async queryDb(query: string, params: unknown[]): Promise<QueryResult> {
    if (!this.postgresEnabled || !this.pool) {
      if (this.config.persistenceMode === "external" && this.config.strictPersistence) {
        void this.schedulePostgresReconnect("write requested while unavailable");
        throw new Error(this.persistenceUnavailableMessage("Write"));
      }
      return { rows: [], rowCount: 0, command: "", oid: 0, fields: [] };
    }
    try {
      const txClient = txStorage.getStore();
      const result = txClient
        ? await txClient.query(query, params as never[])
        : await this.pool.query(query, params as never[]);
      this.notePersistenceSuccess("postgres");
      return result;
    } catch (error) {
      await this.handlePostgresFailure(error, "write");
      if (this.config.strictPersistence) {
        throw new Error(this.persistenceUnavailableMessage("Write"));
      }
      return { rows: [], rowCount: 0, command: "", oid: 0, fields: [] };
    }
  }

  private async runDb(query: string, params: unknown[]) {
    await this.queryDb(query, params);
  }
}

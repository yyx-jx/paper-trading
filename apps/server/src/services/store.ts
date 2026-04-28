import { EventEmitter } from "node:events";
import { createHash } from "node:crypto";
import { appendFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { nanoid } from "nanoid";
import { Pool } from "pg";
import { createClient } from "redis";
import type {
  AuditEvent,
  AuditLogQuery,
  BehaviorActionLog,
  BehaviorLogQuery,
  CandleBar,
  CandleInterval,
  Language,
  LogSearchQuery,
  MarketSnapshot,
  OrderBookSnapshotRecord,
  OrderLifecycleExitType,
  OrderLifecycleRecord,
  OrderBookSnapshot,
  OrderRecord,
  PermissionCode,
  PositionRecord,
  ProfileOverview,
  PublicUser,
  Role,
  RoundRecord,
  RoundStatus,
  SourceHealth,
  TradeSide,
  TradeTimeline,
  UserPayload,
  UserRecord
} from "../domain/types";

const STARTUP_CONNECT_RETRY_ATTEMPTS = 10;
const STARTUP_CONNECT_RETRY_DELAY_MS = 2000;
const FIVE_MINUTE_ROUND_MS = 5 * 60_000;
const QTY_EPSILON = 0.0000001;
const RETENTION_CLEANUP_INTERVAL_MS = 60_000;

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const roundNumber = (value: number, digits = 8) => Number(value.toFixed(digits));

function isFiveMinuteRound(round: RoundRecord) {
  return round.endAt > round.startAt && round.endAt - round.startAt === FIVE_MINUTE_ROUND_MS;
}

function cloneOrderBookSnapshot(snapshot: OrderBookSnapshot): OrderBookSnapshot {
  return {
    snapshotId: snapshot.snapshotId,
    snapshotTs: snapshot.snapshotTs,
    bestBid: snapshot.bestBid,
    bestAsk: snapshot.bestAsk,
    midPrice: snapshot.midPrice,
    bids: snapshot.bids.map((level) => ({ ...level })),
    asks: snapshot.asks.map((level) => ({ ...level }))
  };
}

function orderBookSnapshotRef(snapshot: OrderBookSnapshot) {
  const normalized = cloneOrderBookSnapshot(snapshot);
  return `obs_${createHash("sha256").update(JSON.stringify(normalized)).digest("hex").slice(0, 32)}`;
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

function sanitizePolymarketBtcReference(price?: number) {
  return typeof price === "number" && Number.isFinite(price) && price > 1000 ? price : undefined;
}

const ROLE_PERMISSIONS: Record<Role, PermissionCode[]> = {
  Tester: ["trade:view", "trade:order", "trade:cancel", "trade:sell", "profile:view"],
  "Senior Tester": [
    "trade:view",
    "trade:order",
    "trade:cancel",
    "trade:sell",
    "profile:view",
    "users:list",
    "users:disable",
    "users:reset-password",
    "users:balance:set",
    "logs:view:team"
  ],
  "Test Engineer": [
    "trade:view",
    "trade:order",
    "trade:cancel",
    "trade:sell",
    "profile:view",
    "system:status:view",
    "users:list",
    "logs:view:all"
  ],
  Admin: [
    "trade:view",
    "trade:order",
    "trade:cancel",
    "trade:sell",
    "profile:view",
    "system:status:view",
    "audit:view",
    "users:list",
    "users:create",
    "users:bulk-create",
    "users:disable",
    "users:reset-password",
    "users:balance:set",
    "logs:view:all"
  ]
};

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  username TEXT UNIQUE NOT NULL,
  password TEXT NOT NULL,
  display_name TEXT NOT NULL,
  role TEXT NOT NULL,
  language TEXT NOT NULL,
  permission_codes JSONB NOT NULL,
  available_usdc DOUBLE PRECISION NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  senior_tester_id TEXT,
  disabled_at BIGINT,
  disabled_by TEXT,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS rounds (
  id TEXT PRIMARY KEY,
  market_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  event_id TEXT,
  market_slug TEXT,
  event_slug TEXT,
  condition_id TEXT,
  series_slug TEXT,
  up_token_id TEXT,
  down_token_id TEXT,
  title TEXT,
  resolution_source TEXT,
  start_at BIGINT NOT NULL,
  end_at BIGINT NOT NULL,
  price_to_beat DOUBLE PRECISION NOT NULL,
  price_to_beat_source TEXT,
  price_to_beat_captured_at BIGINT,
  status TEXT NOT NULL,
  poll_count INTEGER NOT NULL,
  poll_start_at BIGINT,
  last_poll_at BIGINT,
  closing_spot_price DOUBLE PRECISION,
  settled_side TEXT,
  settlement_price DOUBLE PRECISION,
  settlement_ts BIGINT,
  redeem_start_ts BIGINT,
  redeem_finish_ts BIGINT,
  manual_reason TEXT,
  accepting_orders BOOLEAN,
  closing_price_source TEXT,
  settlement_source TEXT,
  polymarket_settlement_price DOUBLE PRECISION,
  polymarket_settlement_status TEXT,
  polymarket_open_price DOUBLE PRECISION,
  polymarket_close_price DOUBLE PRECISION,
  polymarket_open_price_source TEXT,
  polymarket_close_price_source TEXT,
  settlement_received_at BIGINT,
  redeem_scheduled_at BIGINT,
  binance_open_price DOUBLE PRECISION,
  binance_close_price DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS order_book_snapshots (
  ref TEXT PRIMARY KEY,
  snapshot_id TEXT NOT NULL,
  snapshot_ts BIGINT NOT NULL,
  best_bid DOUBLE PRECISION NOT NULL,
  best_ask DOUBLE PRECISION NOT NULL,
  mid_price DOUBLE PRECISION NOT NULL,
  snapshot JSONB NOT NULL,
  created_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
  id TEXT PRIMARY KEY,
  trace_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  round_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  market_id TEXT NOT NULL,
  order_kind TEXT,
  time_in_force TEXT,
  limit_price DOUBLE PRECISION,
  lifecycle_status TEXT,
  result_type TEXT,
  token_id TEXT,
  book_key TEXT,
  book_hash TEXT,
  requested_amount_usdc DOUBLE PRECISION,
  requested_qty DOUBLE PRECISION,
  frozen_usdc DOUBLE PRECISION,
  frozen_qty DOUBLE PRECISION,
  fills JSONB,
  source_latency_ms DOUBLE PRECISION,
  market_slug TEXT,
  order_book_snapshot_ref TEXT,
  order_book_snapshot JSONB,
  action TEXT NOT NULL,
  side TEXT NOT NULL,
  status TEXT NOT NULL,
  notional_usdc DOUBLE PRECISION NOT NULL,
  expected_qty DOUBLE PRECISION NOT NULL,
  filled_qty DOUBLE PRECISION NOT NULL,
  unfilled_qty DOUBLE PRECISION NOT NULL,
  avg_fill_price DOUBLE PRECISION,
  best_bid DOUBLE PRECISION NOT NULL,
  best_ask DOUBLE PRECISION NOT NULL,
  mid_price DOUBLE PRECISION NOT NULL,
  book_snapshot_ts BIGINT NOT NULL,
  partial_filled BOOLEAN NOT NULL,
  slippage_bps DOUBLE PRECISION,
  match_latency_ms DOUBLE PRECISION NOT NULL,
  book_acquire_latency_ms DOUBLE PRECISION,
  local_match_latency_ms DOUBLE PRECISION,
  persist_latency_ms DOUBLE PRECISION,
  total_order_latency_ms DOUBLE PRECISION,
  failure_reason TEXT,
  client_send_ts BIGINT,
  server_recv_ts BIGINT NOT NULL,
  server_publish_ts BIGINT NOT NULL,
  created_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS order_lifecycle_logs (
  id TEXT PRIMARY KEY,
  buy_order_id TEXT UNIQUE NOT NULL,
  trace_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  tester_id TEXT NOT NULL,
  round_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  asset_class TEXT NOT NULL,
  market_id TEXT NOT NULL,
  market_slug TEXT,
  direction TEXT NOT NULL,
  order_timestamp_ms BIGINT NOT NULL,
  entry_token_price DOUBLE PRECISION,
  btc_trade_price DOUBLE PRECISION,
  btc_open_price_to_beat DOUBLE PRECISION,
  delta_btc DOUBLE PRECISION,
  volume_token_qty DOUBLE PRECISION NOT NULL,
  remaining_token_qty DOUBLE PRECISION NOT NULL,
  closed_token_qty DOUBLE PRECISION NOT NULL,
  position_notional DOUBLE PRECISION NOT NULL,
  exit_type TEXT,
  exit_token_price DOUBLE PRECISION,
  exit_notional DOUBLE PRECISION NOT NULL,
  settlement_result TEXT,
  order_book_snapshot_ref TEXT,
  actual_fill_price DOUBLE PRECISION,
  slippage_bps DOUBLE PRECISION,
  match_latency_ms DOUBLE PRECISION NOT NULL,
  settlement_time_ms BIGINT,
  settlement_direction TEXT,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS positions (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  round_id TEXT NOT NULL,
  side TEXT NOT NULL,
  qty DOUBLE PRECISION NOT NULL,
  locked_qty DOUBLE PRECISION,
  average_entry DOUBLE PRECISION NOT NULL,
  notional_spent DOUBLE PRECISION NOT NULL,
  current_mark DOUBLE PRECISION NOT NULL,
  current_bid DOUBLE PRECISION,
  current_ask DOUBLE PRECISION,
  current_mid DOUBLE PRECISION,
  current_value DOUBLE PRECISION,
  source_latency_ms DOUBLE PRECISION,
  unrealized_pnl DOUBLE PRECISION NOT NULL,
  realized_pnl DOUBLE PRECISION NOT NULL,
  status TEXT NOT NULL,
  opened_at BIGINT NOT NULL,
  closed_at BIGINT,
  settlement_result TEXT
);

CREATE TABLE IF NOT EXISTS audit_events (
  event_id TEXT PRIMARY KEY,
  trace_id TEXT NOT NULL,
  category TEXT NOT NULL,
  action_type TEXT NOT NULL,
  action_status TEXT NOT NULL,
  user_id TEXT,
  role TEXT,
  page_name TEXT NOT NULL,
  module_name TEXT NOT NULL,
  symbol TEXT,
  round_id TEXT,
  result_code TEXT NOT NULL,
  result_message TEXT NOT NULL,
  client_send_ts BIGINT,
  server_recv_ts BIGINT NOT NULL,
  engine_start_ts BIGINT,
  engine_finish_ts BIGINT,
  server_publish_ts BIGINT NOT NULL,
  backend_latency_ms DOUBLE PRECISION NOT NULL,
  frontend_latency_ms DOUBLE PRECISION,
  details JSONB
);

CREATE TABLE IF NOT EXISTS behavior_action_logs (
  log_id TEXT PRIMARY KEY,
  timestamp_ms BIGINT NOT NULL,
  asset_class TEXT NOT NULL,
  action_type TEXT NOT NULL,
  action_status TEXT NOT NULL,
  round_id TEXT,
  direction TEXT,
  entry_odds DOUBLE PRECISION,
  delta_clob DOUBLE PRECISION NOT NULL,
  volume_clob DOUBLE PRECISION NOT NULL,
  position_notional DOUBLE PRECISION,
  exit_type TEXT,
  exit_odds DOUBLE PRECISION,
  settlement_result TEXT,
  tester_id_anon TEXT NOT NULL,
  trace_id TEXT,
  order_id TEXT,
  market_id TEXT,
  market_slug TEXT,
  round_status TEXT,
  countdown_ms BIGINT,
  binance_spot_price DOUBLE PRECISION NOT NULL,
  binance_1m_last_close DOUBLE PRECISION NOT NULL,
  binance_5m_last_close DOUBLE PRECISION NOT NULL,
  binance_1d_last_close DOUBLE PRECISION NOT NULL,
  chainlink_price DOUBLE PRECISION NOT NULL,
  price_to_beat DOUBLE PRECISION NOT NULL,
  up_price DOUBLE PRECISION NOT NULL,
  down_price DOUBLE PRECISION NOT NULL,
  up_book_top5 JSONB NOT NULL,
  down_book_top5 JSONB NOT NULL,
  recent_trades_top20 JSONB NOT NULL,
  book_snapshot_entry JSONB NOT NULL,
  actual_fill_price DOUBLE PRECISION,
  slippage_bps DOUBLE PRECISION,
  partial_filled BOOLEAN,
  unfilled_qty DOUBLE PRECISION,
  execution_latency_ms DOUBLE PRECISION,
  settlement_direction TEXT,
  settlement_time_ms BIGINT,
  gamma_poll_count INTEGER,
  redeem_finish_time_ms BIGINT,
  source_states JSONB NOT NULL,
  strategy_cluster_label TEXT,
  market_regime_label TEXT,
  quality_grade TEXT,
  context_json JSONB
);

CREATE INDEX IF NOT EXISTS idx_rounds_start_at ON rounds(start_at DESC);
CREATE INDEX IF NOT EXISTS idx_order_book_snapshots_ts ON order_book_snapshots(snapshot_ts DESC);
CREATE INDEX IF NOT EXISTS idx_orders_user_created ON orders(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_order_lifecycle_user_time ON order_lifecycle_logs(user_id, order_timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_order_lifecycle_round ON order_lifecycle_logs(round_id, direction, order_timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_positions_user_opened ON positions(user_id, opened_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_user_recv ON audit_events(user_id, server_recv_ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_round_recv ON audit_events(round_id, server_recv_ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_category_action_recv ON audit_events(category, action_type, server_recv_ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_trace_recv ON audit_events(trace_id, server_recv_ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_module_recv ON audit_events(module_name, server_recv_ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_latency_recv ON audit_events(category, module_name, backend_latency_ms, server_recv_ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_details_order_id ON audit_events((details->>'orderId'));
CREATE INDEX IF NOT EXISTS idx_audit_events_details_position_id ON audit_events((details->>'positionId'));
CREATE INDEX IF NOT EXISTS idx_audit_events_details_market_id ON audit_events((details->>'marketId'));
CREATE INDEX IF NOT EXISTS idx_audit_events_details_market_slug ON audit_events((details->>'marketSlug'));
CREATE INDEX IF NOT EXISTS idx_audit_events_details_connection_state ON audit_events((details->>'connectionState'), server_recv_ts DESC);
CREATE INDEX IF NOT EXISTS idx_behavior_logs_timestamp ON behavior_action_logs(timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_behavior_logs_user_round ON behavior_action_logs(tester_id_anon, round_id, timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_behavior_logs_market_time ON behavior_action_logs(market_id, timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_behavior_logs_trace_time ON behavior_action_logs(trace_id, timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_behavior_logs_order_time ON behavior_action_logs(order_id, timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_behavior_logs_action_time ON behavior_action_logs(action_type, action_status, timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_behavior_logs_round_status_time ON behavior_action_logs(round_status, timestamp_ms DESC);

ALTER TABLE users ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS senior_tester_id TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS disabled_at BIGINT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS disabled_by TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS updated_at BIGINT;
UPDATE users SET updated_at = created_at WHERE updated_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_users_senior_tester ON users(senior_tester_id);
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS settlement_source TEXT;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS polymarket_settlement_price DOUBLE PRECISION;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS polymarket_settlement_status TEXT;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS polymarket_open_price DOUBLE PRECISION;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS polymarket_close_price DOUBLE PRECISION;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS polymarket_open_price_source TEXT;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS polymarket_close_price_source TEXT;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS settlement_received_at BIGINT;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS redeem_scheduled_at BIGINT;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS binance_open_price DOUBLE PRECISION;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS binance_close_price DOUBLE PRECISION;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS price_to_beat_source TEXT;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS price_to_beat_captured_at BIGINT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS order_kind TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS time_in_force TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS limit_price DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS lifecycle_status TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS result_type TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS token_id TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS book_key TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS book_hash TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS requested_amount_usdc DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS requested_qty DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS frozen_usdc DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS frozen_qty DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS fills JSONB;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS source_latency_ms DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS market_slug TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS order_book_snapshot_ref TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS order_book_snapshot JSONB;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS book_acquire_latency_ms DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS local_match_latency_ms DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS persist_latency_ms DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS total_order_latency_ms DOUBLE PRECISION;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS locked_qty DOUBLE PRECISION;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS current_bid DOUBLE PRECISION;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS current_ask DOUBLE PRECISION;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS current_mid DOUBLE PRECISION;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS current_value DOUBLE PRECISION;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS source_latency_ms DOUBLE PRECISION;
`;

const LOG_DIR = path.resolve(process.cwd(), "data/logs");
const LOG_FILE = path.join(LOG_DIR, "audit-events.jsonl");
const BEHAVIOR_LOG_FILE = path.join(LOG_DIR, "behavior-action-logs.jsonl");

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

function createEmptyMarketSnapshot(symbol: string, chainlinkEnabled: boolean): MarketSnapshot {
  const now = Date.now();
  const emptySource = (source: "Binance" | "Chainlink" | "CLOB"): SourceHealth => ({
    source,
    symbol,
    state: source === "Chainlink" && !chainlinkEnabled ? "disabled" : "reconnecting",
    reconnectCount: 0,
    sourceEventTs: now,
    serverRecvTs: now,
    normalizedTs: now,
    serverPublishTs: now,
    acquireLatencyMs: 0,
    publishLatencyMs: 0,
    frontendLatencyMs: 0,
    message:
      source === "Chainlink" && !chainlinkEnabled
        ? "Chainlink is disabled in local testing mode."
        : `Waiting for ${source}.`
  });
  return {
    symbol,
    marketId: "",
    serverNow: now,
    binancePrice: 0,
    chainlinkPrice: 0,
    currentPrice: 0,
    priceToBeat: 0,
    upPrice: 0,
    downPrice: 0,
    sources: {
      binance: emptySource("Binance"),
      chainlink: emptySource("Chainlink"),
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
        "1m": [createEmptyCandleBar("1m", now)],
        "5m": [createEmptyCandleBar("5m", now)],
        "1d": [createEmptyCandleBar("1d", now)]
      }
    },
    chainlink: {
      referencePrice: 0,
      settlementReference: 0
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
        { source: "Chainlink", state: chainlinkEnabled ? "reconnecting" : "disabled" },
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

export class AppStore {
  public readonly emitter = new EventEmitter();
  public readonly users = new Map<string, UserRecord>();
  public readonly rounds: RoundRecord[] = [];
  public readonly orders: OrderRecord[] = [];
  public readonly orderLifecycleLogs: OrderLifecycleRecord[] = [];
  public readonly orderBookSnapshots = new Map<string, OrderBookSnapshotRecord>();
  public readonly positions: PositionRecord[] = [];
  public readonly logs: AuditEvent[] = [];
  public readonly behaviorLogs: BehaviorActionLog[] = [];
  public marketSnapshot: MarketSnapshot;

  private pool?: Pool;
  private redis?: ReturnType<typeof createClient>;
  private readonly snapshotCacheKey: string;
  private readonly sourcesCacheKey: string;
  private postgresEnabled = false;
  private redisEnabled = false;
  private lastRetentionCleanupAt = 0;
  private readonly orderIndexById = new Map<string, number>();
  private readonly positionIndexById = new Map<string, number>();
  private readonly orderLifecycleIndexById = new Map<string, number>();
  private readonly pendingUserPayloadIds = new Set<string>();
  private userPayloadFlushScheduled = false;
  private readonly config: {
    initialBalance: number;
    logRetentionMs: number;
    snapshotRetentionSeconds: number;
    symbol: string;
    databaseUrl: string;
    redisUrl: string;
    persistenceMode: "external" | "memory";
    chainlinkEnabled: boolean;
  };

  constructor(config: {
    initialBalance: number;
    logRetentionMs: number;
    snapshotRetentionSeconds: number;
    symbol: string;
    databaseUrl: string;
    redisUrl: string;
    persistenceMode: "external" | "memory";
    chainlinkEnabled: boolean;
  }) {
    this.config = config;
    this.snapshotCacheKey = `market:snapshot:${config.symbol}`;
    this.sourcesCacheKey = `market:sources:${config.symbol}`;
    this.marketSnapshot = createEmptyMarketSnapshot(config.symbol, config.chainlinkEnabled);
    mkdirSync(LOG_DIR, { recursive: true });
  }

  async init() {
    await this.connectPostgres();
    await this.seedUsers();
    await this.connectRedis();
    await this.loadStateFromPersistence();
  }

  async close() {
    if (this.redis?.isOpen) {
      await this.redis.quit().catch(() => undefined);
    }
    if (this.pool) {
      await this.pool.end().catch(() => undefined);
    }
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
      disabledAt: user.disabledAt,
      disabledBy: user.disabledBy,
      createdAt: user.createdAt,
      updatedAt: user.updatedAt
    };
  }

  findUserByCredentials(username: string, password: string) {
    for (const user of this.users.values()) {
      if (user.username === username && user.password === password && user.isActive) {
        return user;
      }
    }
    return undefined;
  }

  findUserByUsername(username: string) {
    return [...this.users.values()].find((user) => user.username === username);
  }

  getUserById(userId: string) {
    return this.users.get(userId);
  }

  listUsers() {
    return [...this.users.values()]
      .sort((left, right) => left.createdAt - right.createdAt)
      .map((user) => this.sanitizeUser(user));
  }

  getPersistenceStatus() {
    return {
      postgres: this.postgresEnabled,
      redis: this.redisEnabled
    };
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

  private rebuildHotIndexes() {
    this.orderIndexById.clear();
    this.positionIndexById.clear();
    this.orderLifecycleIndexById.clear();
    this.orders.forEach((order, index) => this.orderIndexById.set(order.id, index));
    this.positions.forEach((position, index) => this.positionIndexById.set(position.id, index));
    this.orderLifecycleLogs.forEach((log, index) => this.orderLifecycleIndexById.set(log.id, index));
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
    user.updatedAt = user.updatedAt || Date.now();
    this.users.set(user.id, user);
    await this.runDb(
      `
      INSERT INTO users (
        id, username, password, display_name, role, language, permission_codes, available_usdc,
        is_active, senior_tester_id, disabled_at, disabled_by, created_at, updated_at
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
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
    availableUsdc: number;
  }) {
    if (this.findUserByUsername(input.username)) {
      throw new Error("Username already exists.");
    }
    const now = Date.now();
    const user: UserRecord = {
      id: this.newId("u"),
      username: input.username,
      password: input.password,
      displayName: input.displayName,
      role: input.role,
      language: input.language,
      permissionCodes: ROLE_PERMISSIONS[input.role],
      availableUsdc: input.availableUsdc,
      isActive: true,
      seniorTesterId: input.seniorTesterId,
      createdAt: now,
      updatedAt: now
    };
    await this.persistUser(user);
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
    user.password = password;
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
    if (this.redisEnabled && this.redis?.isOpen) {
      try {
        await Promise.all([
          this.redis.set(this.snapshotCacheKey, JSON.stringify(snapshot), {
            expiration: {
              type: "EX",
              value: this.config.snapshotRetentionSeconds
            }
          }),
          this.redis.set(this.sourcesCacheKey, JSON.stringify(Object.values(snapshot.sources)), {
            expiration: {
              type: "EX",
              value: this.config.snapshotRetentionSeconds
            }
          }),
          this.redis.publish(`market:update:${this.config.symbol}`, JSON.stringify(snapshot))
        ]);
      } catch (error) {
        this.redisEnabled = false;
        console.warn("[store] Redis snapshot cache is unavailable:", error);
      }
    }
    this.emitter.emit("market:update", snapshot);
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
    const operatedRoundIds = new Set<string>();
    const orderByRoundId = new Map<string, OrderRecord>();
    const positionByRoundId = new Map<string, PositionRecord>();
    for (const order of this.orders) {
      if (order.userId === userId) {
        operatedRoundIds.add(order.roundId);
        if (!orderByRoundId.has(order.roundId)) {
          orderByRoundId.set(order.roundId, order);
        }
      }
    }
    for (const position of this.positions) {
      if (position.userId === userId) {
        operatedRoundIds.add(position.roundId);
        if (!positionByRoundId.has(position.roundId)) {
          positionByRoundId.set(position.roundId, position);
        }
      }
    }

    const roundById = new Map(this.rounds.map((round) => [round.id, round]));
    return [...operatedRoundIds]
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
    return this.positions
      .filter((position) => position.userId === userId)
      .sort((left, right) => right.openedAt - left.openedAt)
      .map((position) => this.decoratePosition(position));
  }

  getPositionById(positionId: string) {
    const index = this.positionIndexById?.get(positionId);
    return typeof index === "number" ? this.positions[index] : this.positions.find((position) => position.id === positionId);
  }

  getOrders(userId: string) {
    return this.orders
      .filter((order) => order.userId === userId)
      .sort((left, right) => right.createdAt - left.createdAt)
      .map((order) => this.sanitizeOrder(order));
  }

  getOrderById(orderId: string) {
    const index = this.orderIndexById?.get(orderId);
    return typeof index === "number" ? this.orders[index] : this.orders.find((order) => order.id === orderId);
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
    const limit = this.normalizeSearchLimit(options?.limit);
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
          where.push(`category = 'latency' AND module_name = ANY(ARRAY['binance','chainlink','clob']::text[])`);
        } else if (filters.logGroup === "system_latency") {
          where.push(`category = 'latency' AND module_name <> ALL(ARRAY['binance','chainlink','clob']::text[])`);
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
          where.push(`category = 'latency' AND module_name <> ALL(ARRAY['binance','chainlink','clob']::text[])`);
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
        return rows.rows.map((row) => this.rowToAuditEvent(row));
      } catch (error) {
        console.warn("[store] PostgreSQL audit search failed, falling back to memory:", error);
      }
    }

    return this.logs
      .filter((log) => this.matchesAuditSearch(log, filters))
      .sort((left, right) => right.serverRecvTs - left.serverRecvTs || right.eventId.localeCompare(left.eventId))
      .slice(offset, offset + limit);
  }

  async searchBehaviorLogs(filters?: LogSearchQuery, options?: { limit?: number; offset?: number }) {
    const limit = this.normalizeSearchLimit(options?.limit);
    const offset = Math.max(0, Math.floor(options?.offset ?? 0));
    const effectiveUserIds = this.resolveBehaviorSearchUserIds(filters);
    if (effectiveUserIds && effectiveUserIds.length === 0) {
      return [] as BehaviorActionLog[];
    }
    if (this.hasUnsupportedBehaviorSearchFilter(filters)) {
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
        return rows.rows.map((row) => this.rowToBehaviorLog(row));
      } catch (error) {
        console.warn("[store] PostgreSQL training log search failed, falling back to memory:", error);
      }
    }

    return this.behaviorLogs
      .filter((log) => this.matchesBehaviorSearch(log, filters, effectiveUserIds))
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
        if (filters?.orderId && this.detailString(log.details, "orderId") !== filters.orderId) {
          return false;
        }
        if (filters?.positionId && this.detailString(log.details, "positionId") !== filters.positionId) {
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
        .filter((event) => event.traceId === order.traceId || this.detailString(event.details, "orderId") === order.id)
        .map((event) => this.detailString(event.details, "positionId"))
        .find(Boolean) ??
      allBehaviorLogs
        .filter((log) => log.traceId === order.traceId || log.orderId === order.id)
        .map((log) => this.detailString(log.contextJson, "positionId"))
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
        this.detailString(event.details, "orderId") === order.id ||
        Boolean(positionId && this.detailString(event.details, "positionId") === positionId)
      );
    });
    const behaviorLogs = allBehaviorLogs.filter((log) => {
      return (
        log.traceId === order.traceId ||
        log.orderId === order.id ||
        Boolean(positionId && this.detailString(log.contextJson, "positionId") === positionId)
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
    const user = this.users.get(userId);
    if (!user) {
      return {
        totalEquity: 0,
        availableUsdc: 0,
        positionValue: 0,
        realizedPnlToday: 0,
        unrealizedPnl: 0,
        winRate: 0,
        roundsParticipatedTotal: 0,
        roundsParticipatedToday: 0
      };
    }

    const positions = this.getPositions(userId);
    const livePositions = positions.filter((position) => position.displayStatus === "open");
    const positionValue = livePositions.reduce((sum, position) => sum + (position.currentValue ?? position.qty * position.currentMark), 0);

    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);
    const todayTs = todayStart.getTime();

    const realizedPnlToday = positions
      .filter((position) => (position.closedAt ?? position.openedAt) >= todayTs)
      .reduce((sum, position) => sum + position.realizedPnl, 0);
    const unrealizedPnl = livePositions.reduce((sum, position) => sum + position.unrealizedPnl, 0);
    const settledPositions = positions.filter(
      (position) =>
        position.status === "closed" &&
        (position.settlementResult === "win" || position.settlementResult === "loss")
    );
    const wins = settledPositions.filter((position) => position.settlementResult === "win").length;
    const userOrders = this.getOrders(userId);
    const roundsParticipatedTotal = new Set([
      ...positions.map((position) => position.roundId),
      ...userOrders.map((order) => order.roundId)
    ]).size;
    const roundsParticipatedToday = new Set([
      ...positions
        .filter((position) => position.openedAt >= todayTs || (position.closedAt ?? 0) >= todayTs)
        .map((position) => position.roundId),
      ...userOrders.filter((order) => order.createdAt >= todayTs).map((order) => order.roundId)
    ]).size;

    return {
      totalEquity: Number((user.availableUsdc + positionValue).toFixed(2)),
      availableUsdc: Number(user.availableUsdc.toFixed(2)),
      positionValue: Number(positionValue.toFixed(2)),
      realizedPnlToday: Number(realizedPnlToday.toFixed(2)),
      unrealizedPnl: Number(unrealizedPnl.toFixed(2)),
      winRate: settledPositions.length > 0 ? wins / settledPositions.length : 0,
      roundsParticipatedTotal,
      roundsParticipatedToday
    };
  }

  emitUserPayload(userId: string) {
    this.pendingUserPayloadIds.add(userId);
    if (this.userPayloadFlushScheduled) {
      return;
    }
    this.userPayloadFlushScheduled = true;
    setImmediate(() => this.flushUserPayloads());
  }

  private flushUserPayloads() {
    const userIds = [...this.pendingUserPayloadIds];
    this.pendingUserPayloadIds.clear();
    this.userPayloadFlushScheduled = false;
    for (const userId of userIds) {
      const payload: UserPayload = {
        profile: this.getProfile(userId),
        operatedHistory: this.getOperatedHistory(500, userId),
        positions: this.getPositions(userId),
        orders: this.getOrders(userId),
        logs: this.getRecentLogs(userId)
      };
      this.emitter.emit(`user:${userId}`, payload);
    }
  }

  private decoratePosition(position: PositionRecord) {
    const round = this.getRoundById(position.roundId);
    const displayStatus = this.getPositionDisplayStatus(position, round);
    const sanitized =
      displayStatus === "open"
        ? position
        : {
            ...position,
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

  private detailString(details: Record<string, unknown> | undefined, key: string) {
    const value = details?.[key];
    return typeof value === "string" || typeof value === "number" ? String(value) : undefined;
  }

  private normalizeSearchLimit(limit?: number) {
    return Math.max(1, Math.min(Math.floor(limit ?? 100), 501));
  }

  private matchesAuditSearch(log: AuditEvent, filters?: LogSearchQuery) {
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
    if (filters?.role && log.role !== filters.role) {
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
    if (filters?.moduleName && log.moduleName !== filters.moduleName) {
      return false;
    }
    if (filters?.pageName && log.pageName !== filters.pageName) {
      return false;
    }
    if (filters?.symbol && log.symbol !== filters.symbol) {
      return false;
    }
    if (filters?.roundId && log.roundId !== filters.roundId) {
      return false;
    }
    if (filters?.traceId && log.traceId !== filters.traceId) {
      return false;
    }
    if (filters?.resultCode && log.resultCode !== filters.resultCode) {
      return false;
    }
    if (filters?.orderId && this.detailString(log.details, "orderId") !== filters.orderId) {
      return false;
    }
    if (filters?.positionId && this.detailString(log.details, "positionId") !== filters.positionId) {
      return false;
    }
    if (filters?.marketId && this.detailString(log.details, "marketId") !== filters.marketId) {
      return false;
    }
    if (filters?.marketSlug && this.detailString(log.details, "marketSlug") !== filters.marketSlug) {
      return false;
    }
    const direction = this.detailString(log.details, "direction") ?? this.detailString(log.details, "side");
    if (filters?.direction && direction !== filters.direction) {
      return false;
    }
    if (filters?.roundStatus && this.detailString(log.details, "roundStatus") !== filters.roundStatus) {
      return false;
    }
    if (
      filters?.settlementResult &&
      this.detailString(log.details, "settlementResult") !== filters.settlementResult
    ) {
      return false;
    }
    if (filters?.matchingLogKind === "engine") {
      return false;
    }
    if (filters?.bookKey || filters?.bookSide || filters?.eventType || filters?.sequenceFrom || filters?.sequenceTo) {
      return false;
    }
    if (filters?.logGroup && this.auditLogGroup(log) !== filters.logGroup) {
      return false;
    }
    if (filters?.matchingLogKind === "action" && log.category !== "matching") {
      return false;
    }
    if (filters?.latencySource && this.auditLatencySource(log) !== filters.latencySource) {
      return false;
    }
    if (filters?.connectionState && this.detailString(log.details, "connectionState") !== filters.connectionState) {
      return false;
    }
    if (typeof filters?.latencyMinMs === "number" || typeof filters?.latencyMaxMs === "number") {
      const latency = this.auditLatencyValue(log, filters.latencyPhase);
      if (typeof latency !== "number") {
        return false;
      }
      if (typeof filters.latencyMinMs === "number" && latency < filters.latencyMinMs) {
        return false;
      }
      if (typeof filters.latencyMaxMs === "number" && latency > filters.latencyMaxMs) {
        return false;
      }
    }
    return true;
  }

  private auditLogGroup(log: AuditEvent) {
    if (log.category === "matching") {
      return "matching_action";
    }
    if (log.category === "settlement") {
      return "settlement";
    }
    if (log.category === "latency") {
      return ["binance", "chainlink", "clob"].includes(log.moduleName) ? "market_latency" : "system_latency";
    }
    return "operation";
  }

  private auditLatencySource(log: AuditEvent) {
    if (log.category !== "latency") {
      return undefined;
    }
    return ["binance", "chainlink", "clob"].includes(log.moduleName) ? log.moduleName : "system";
  }

  private detailNumber(details: Record<string, unknown> | undefined, key: string) {
    const value = details?.[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    if (typeof value === "string" && Number.isFinite(Number(value))) {
      return Number(value);
    }
    return undefined;
  }

  private auditLatencyValue(log: AuditEvent, phase?: LogSearchQuery["latencyPhase"]) {
    if (phase === "acquire") {
      return this.detailNumber(log.details, "acquireLatencyMs");
    }
    if (phase === "publish") {
      return this.detailNumber(log.details, "publishLatencyMs");
    }
    if (phase === "frontend") {
      return log.frontendLatencyMs ?? this.detailNumber(log.details, "frontendLatencyMs");
    }
    return log.backendLatencyMs;
  }

  private resolveBehaviorSearchUserIds(filters?: LogSearchQuery) {
    if (filters?.userId) {
      const user = this.getUserById(filters.userId);
      if (filters.role && user?.role !== filters.role) {
        return [] as string[];
      }
      return [filters.userId];
    }
    const baseIds = filters?.userIds;
    if (!filters?.role) {
      return baseIds;
    }
    const roleIds = new Set(
      [...this.users.values()].filter((user) => user.role === filters.role).map((user) => user.id)
    );
    return baseIds ? baseIds.filter((userId) => roleIds.has(userId)) : [...roleIds];
  }

  private hasUnsupportedBehaviorSearchFilter(filters?: LogSearchQuery) {
    return Boolean(
      filters?.category ||
        filters?.moduleName ||
        filters?.pageName ||
        filters?.symbol ||
        filters?.positionId ||
        filters?.resultCode ||
        filters?.bookKey ||
        filters?.bookSide ||
        filters?.eventType ||
        filters?.logGroup ||
        filters?.latencySource ||
        filters?.connectionState ||
        filters?.latencyPhase ||
        typeof filters?.latencyMinMs === "number" ||
        typeof filters?.latencyMaxMs === "number" ||
        filters?.matchingLogKind ||
        typeof filters?.sequenceFrom === "number" ||
        typeof filters?.sequenceTo === "number"
    );
  }

  private matchesBehaviorSearch(log: BehaviorActionLog, filters?: LogSearchQuery, userIds?: string[]) {
    if (typeof filters?.from === "number" && log.timestampMs < filters.from) {
      return false;
    }
    if (typeof filters?.to === "number" && log.timestampMs > filters.to) {
      return false;
    }
    if (userIds?.length) {
      const allowedAnonIds = new Set(userIds.map((userId) => this.anonymizeUserId(userId)));
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
    if (filters?.direction && log.direction !== filters.direction) {
      return false;
    }
    if (filters?.roundStatus && log.roundStatus !== filters.roundStatus) {
      return false;
    }
    if (filters?.settlementResult && log.settlementResult !== filters.settlementResult) {
      return false;
    }
    return true;
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

  async upsertRound(round: RoundRecord) {
    const index = this.rounds.findIndex((item) => item.id === round.id);
    if (index >= 0) {
      this.rounds[index] = round;
    } else {
      this.rounds.push(round);
    }
    this.rounds.sort((left, right) => right.startAt - left.startAt);

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
        binance_open_price, binance_close_price
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,
        $29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41
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
        binance_close_price = EXCLUDED.binance_close_price
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
        round.binanceClosePrice ?? null
      ]
    );
  }

  async persistOrderBookSnapshot(snapshot: OrderBookSnapshot) {
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
      await this.runDb(
        `
        INSERT INTO order_book_snapshots (
          ref, snapshot_id, snapshot_ts, best_bid, best_ask, mid_price, snapshot, created_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT (ref) DO NOTHING
        `,
        [
          record.ref,
          record.snapshotId,
          record.snapshotTs,
          record.bestBid,
          record.bestAsk,
          record.midPrice,
          JSON.stringify(record.snapshot),
          record.createdAt
        ]
      );
    }
    return ref;
  }

  async persistOrderLifecycle(log: OrderLifecycleRecord) {
    this.upsertIndexedRecord(this.orderLifecycleLogs, this.orderLifecycleIndexById, log);

    await this.runDb(
      `
      INSERT INTO order_lifecycle_logs (
        id, buy_order_id, trace_id, user_id, tester_id, round_id, symbol, asset_class, market_id,
        market_slug, direction, order_timestamp_ms, entry_token_price, btc_trade_price,
        btc_open_price_to_beat, delta_btc, volume_token_qty, remaining_token_qty,
        closed_token_qty, position_notional, exit_type, exit_token_price, exit_notional,
        settlement_result, order_book_snapshot_ref, actual_fill_price, slippage_bps,
        match_latency_ms, settlement_time_ms, settlement_direction, created_at, updated_at
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
        $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
        $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32
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
  }) {
    if (input.qty <= QTY_EPSILON || typeof input.exitTokenPrice !== "number") {
      return;
    }
    let remaining = roundNumber(input.qty, 4);
    const logs = this.orderLifecycleLogs
      .filter(
        (log) =>
          log.userId === input.userId &&
          log.roundId === input.roundId &&
          log.direction === input.side &&
          log.remainingTokenQty > QTY_EPSILON
      )
      .sort((left, right) => left.orderTimestampMs - right.orderTimestampMs);

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
      log.updatedAt = Date.now();
      await this.persistOrderLifecycle(log);
    }
  }

  async persistOrder(order: OrderRecord) {
    if (order.orderBookSnapshot) {
      order.orderBookSnapshotRef = await this.persistOrderBookSnapshot(order.orderBookSnapshot);
      order.orderBookSnapshot = undefined;
    }
    this.upsertIndexedRecord(this.orders, this.orderIndexById, order);

    await this.runDb(
      `
      INSERT INTO orders (
        id, trace_id, user_id, round_id, symbol, market_id, order_kind, time_in_force, limit_price,
        lifecycle_status, result_type, token_id, book_key, book_hash, requested_amount_usdc,
        requested_qty, frozen_usdc, frozen_qty, fills, source_latency_ms, market_slug, order_book_snapshot_ref, order_book_snapshot,
        action, side, status, notional_usdc,
        expected_qty, filled_qty, unfilled_qty, avg_fill_price, best_bid, best_ask, mid_price,
        book_snapshot_ts, partial_filled, slippage_bps, match_latency_ms,
        book_acquire_latency_ms, local_match_latency_ms, persist_latency_ms, total_order_latency_ms, failure_reason,
        client_send_ts, server_recv_ts, server_publish_ts, created_at
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
        $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
        $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,
        $31,$32,$33,$34,$35,$36,$37,$38,$39,$40,
        $41,$42,$43,$44,$45,$46,$47
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
        order_book_snapshot_ref = EXCLUDED.order_book_snapshot_ref,
        failure_reason = EXCLUDED.failure_reason,
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
        order.clientSendTs ?? null,
        order.serverRecvTs,
        order.serverPublishTs,
        order.createdAt
      ]
    );
  }

  async persistPosition(position: PositionRecord) {
    this.upsertIndexedRecord(this.positions, this.positionIndexById, position);

    await this.runDb(
      `
      INSERT INTO positions (
        id, user_id, round_id, side, qty, locked_qty, average_entry, notional_spent, current_mark,
        current_bid, current_ask, current_mid, current_value, source_latency_ms,
        unrealized_pnl, realized_pnl, status, opened_at, closed_at, settlement_result
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
        $11,$12,$13,$14,$15,$16,$17,$18,$19,$20
      )
      ON CONFLICT (id) DO UPDATE SET
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
        status = EXCLUDED.status,
        closed_at = EXCLUDED.closed_at,
        settlement_result = EXCLUDED.settlement_result
      `,
      [
        position.id,
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
        position.status,
        position.openedAt,
        position.closedAt ?? null,
        position.settlementResult ?? null
      ]
    );
  }

  async recordLog(event: AuditEvent) {
    this.logs.unshift(event);
    appendFileSync(LOG_FILE, `${JSON.stringify(event)}\n`, "utf-8");
    void this.runDb(
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
    void this.cleanupRetentionIfDue(event.serverRecvTs);
    if (event.userId) {
      this.emitUserPayload(event.userId);
    }
  }

  async recordBehaviorLog(log: BehaviorActionLog) {
    this.behaviorLogs.unshift(log);
    appendFileSync(BEHAVIOR_LOG_FILE, `${JSON.stringify(log)}\n`, "utf-8");
    void this.runDb(
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
        log.chainlinkPrice,
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

  anonymizeUserId(userId: string) {
    return createHash("sha256").update(`paper-trading:${userId}`).digest("hex").slice(0, 16);
  }

  newTraceId() {
    return `tr_${nanoid(10)}`;
  }

  newId(prefix: string) {
    return `${prefix}_${nanoid(12)}`;
  }

  private async connectPostgres() {
    if (this.config.persistenceMode === "memory") {
      console.warn("[store] PERSISTENCE_MODE=memory; skipping PostgreSQL connection.");
      return;
    }

    let lastError: unknown;

    for (let attempt = 1; attempt <= STARTUP_CONNECT_RETRY_ATTEMPTS; attempt += 1) {
      try {
        this.pool = new Pool({
          connectionString: this.config.databaseUrl,
          connectionTimeoutMillis: 3000
        });
        await this.pool.query("SELECT 1");
        await this.pool.query(SCHEMA_SQL);
        this.postgresEnabled = true;
        return;
      } catch (error) {
        lastError = error;
        this.postgresEnabled = false;
        if (this.pool) {
          await this.pool.end().catch(() => undefined);
          this.pool = undefined;
        }
        if (attempt < STARTUP_CONNECT_RETRY_ATTEMPTS) {
          console.warn(
            `[store] PostgreSQL is not ready yet (attempt ${attempt}/${STARTUP_CONNECT_RETRY_ATTEMPTS}); retrying in ${STARTUP_CONNECT_RETRY_DELAY_MS}ms`
          );
          await sleep(STARTUP_CONNECT_RETRY_DELAY_MS);
        }
      }
    }

    console.warn("[store] PostgreSQL is unavailable, using in-memory persistence only:", lastError);
  }

  private async connectRedis() {
    if (this.config.persistenceMode === "memory") {
      console.warn("[store] PERSISTENCE_MODE=memory; skipping Redis connection.");
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
          console.warn("[store] Redis connection error:", error);
        });
        await client.connect();
        this.redis = client;
        this.redisEnabled = true;
        return;
      } catch (error) {
        lastError = error;
        this.redisEnabled = false;
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
            password,
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
            password,
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
      const [userRows, roundRows, snapshotRows, orderRows, lifecycleRows, positionRows, logRows, behaviorRows] = await Promise.all([
        this.pool.query("SELECT * FROM users ORDER BY created_at ASC"),
        this.pool.query("SELECT * FROM rounds ORDER BY start_at DESC LIMIT 80"),
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
          permissionCodes: [...new Set([...normalizePermissionCodes(row.permission_codes), ...ROLE_PERMISSIONS[role]])],
          availableUsdc: Number(row.available_usdc),
          isActive: row.is_active === null || typeof row.is_active === "undefined" ? true : Boolean(row.is_active),
          seniorTesterId: row.senior_tester_id ? String(row.senior_tester_id) : undefined,
          disabledAt: row.disabled_at ? Number(row.disabled_at) : undefined,
          disabledBy: row.disabled_by ? String(row.disabled_by) : undefined,
          createdAt,
          updatedAt: row.updated_at ? Number(row.updated_at) : createdAt
        });
      }

      this.rounds.splice(0, this.rounds.length, ...roundRows.rows.map((row) => this.rowToRound(row)));
      this.orderBookSnapshots.clear();
      for (const row of snapshotRows.rows) {
        const snapshotRecord = this.rowToOrderBookSnapshotRecord(row);
        this.orderBookSnapshots.set(snapshotRecord.ref, snapshotRecord);
      }
      this.orders.splice(0, this.orders.length, ...orderRows.rows.map((row) => this.rowToOrder(row)));
      this.orderLifecycleLogs.splice(
        0,
        this.orderLifecycleLogs.length,
        ...lifecycleRows.rows.map((row) => this.rowToOrderLifecycle(row))
      );
      this.positions.splice(0, this.positions.length, ...positionRows.rows.map((row) => this.rowToPosition(row)));
      this.logs.splice(0, this.logs.length, ...logRows.rows.map((row) => this.rowToAuditEvent(row)));
      this.behaviorLogs.splice(
        0,
        this.behaviorLogs.length,
        ...behaviorRows.rows.map((row) => this.rowToBehaviorLog(row))
      );
      this.rebuildHotIndexes();
    }

    if (this.redisEnabled && this.redis?.isOpen) {
      const snapshotJson = await this.redis.get(this.snapshotCacheKey);
      if (snapshotJson) {
        this.marketSnapshot = JSON.parse(snapshotJson) as MarketSnapshot;
      }
    }
  }

  private async cleanupRetentionIfDue(now = Date.now()) {
    if (now - this.lastRetentionCleanupAt < RETENTION_CLEANUP_INTERVAL_MS) {
      return;
    }
    this.lastRetentionCleanupAt = now;
    await this.cleanupRetention(now);
  }

  private async cleanupRetention(now = Date.now()) {
    const threshold = now - this.config.logRetentionMs;
    const retained = this.logs
      .filter((log) => log.serverRecvTs >= threshold)
      .sort((left, right) => right.serverRecvTs - left.serverRecvTs);
    this.logs.splice(0, this.logs.length, ...retained);
    await this.runDb("DELETE FROM audit_events WHERE server_recv_ts < $1", [threshold]);
    this.pruneLogFile(threshold);
  }

  private pruneLogFile(threshold: number) {
    if (!existsSync(LOG_FILE)) {
      return;
    }
    const filtered = readFileSync(LOG_FILE, "utf-8")
      .split(/\r?\n/)
      .filter(Boolean)
      .map((line) => {
        try {
          return JSON.parse(line) as AuditEvent;
        } catch {
          return undefined;
        }
      })
      .filter((event): event is AuditEvent => Boolean(event && event.serverRecvTs >= threshold));
    writeFileSync(
      LOG_FILE,
      filtered.map((event) => JSON.stringify(event)).join("\n") + (filtered.length ? "\n" : ""),
      "utf-8"
    );
  }

  private rowToRound(row: Record<string, unknown>): RoundRecord {
    const numberOrUndefined = (value: unknown) => value === null || typeof value === "undefined" ? undefined : Number(value);
    const polymarketOpenPrice = sanitizePolymarketBtcReference(numberOrUndefined(row.polymarket_open_price));
    const polymarketClosePrice = sanitizePolymarketBtcReference(numberOrUndefined(row.polymarket_close_price));
    return {
      id: String(row.id),
      marketId: String(row.market_id),
      symbol: String(row.symbol),
      eventId: row.event_id ? String(row.event_id) : undefined,
      marketSlug: row.market_slug ? String(row.market_slug) : undefined,
      eventSlug: row.event_slug ? String(row.event_slug) : undefined,
      conditionId: row.condition_id ? String(row.condition_id) : undefined,
      seriesSlug: row.series_slug ? String(row.series_slug) : undefined,
      upTokenId: row.up_token_id ? String(row.up_token_id) : undefined,
      downTokenId: row.down_token_id ? String(row.down_token_id) : undefined,
      title: row.title ? String(row.title) : undefined,
      resolutionSource: row.resolution_source ? String(row.resolution_source) : undefined,
      startAt: Number(row.start_at),
      endAt: Number(row.end_at),
      priceToBeat: Number(row.price_to_beat),
      priceToBeatSource: row.price_to_beat_source ? String(row.price_to_beat_source) : undefined,
      priceToBeatCapturedAt: row.price_to_beat_captured_at ? Number(row.price_to_beat_captured_at) : undefined,
      status: String(row.status) as RoundStatus,
      pollCount: Number(row.poll_count),
      pollStartAt: row.poll_start_at ? Number(row.poll_start_at) : undefined,
      lastPollAt: row.last_poll_at ? Number(row.last_poll_at) : undefined,
      closingSpotPrice: row.closing_spot_price !== null ? Number(row.closing_spot_price) : undefined,
      settledSide: row.settled_side ? (String(row.settled_side) as RoundRecord["settledSide"]) : undefined,
      settlementPrice: row.settlement_price !== null ? Number(row.settlement_price) : undefined,
      settlementTs: row.settlement_ts ? Number(row.settlement_ts) : undefined,
      settlementSource: row.settlement_source ? (String(row.settlement_source) as RoundRecord["settlementSource"]) : undefined,
      polymarketSettlementPrice: numberOrUndefined(row.polymarket_settlement_price),
      polymarketSettlementStatus: row.polymarket_settlement_status
        ? (String(row.polymarket_settlement_status) as RoundRecord["polymarketSettlementStatus"])
        : undefined,
      polymarketOpenPrice,
      polymarketClosePrice,
      polymarketOpenPriceSource:
        polymarketOpenPrice && row.polymarket_open_price_source ? String(row.polymarket_open_price_source) : undefined,
      polymarketClosePriceSource:
        polymarketClosePrice && row.polymarket_close_price_source ? String(row.polymarket_close_price_source) : undefined,
      settlementReceivedAt: numberOrUndefined(row.settlement_received_at),
      redeemScheduledAt: numberOrUndefined(row.redeem_scheduled_at),
      binanceOpenPrice: numberOrUndefined(row.binance_open_price),
      binanceClosePrice: numberOrUndefined(row.binance_close_price),
      redeemStartTs: row.redeem_start_ts ? Number(row.redeem_start_ts) : undefined,
      redeemFinishTs: row.redeem_finish_ts ? Number(row.redeem_finish_ts) : undefined,
      manualReason: row.manual_reason ? String(row.manual_reason) : undefined,
      acceptingOrders: row.accepting_orders !== null ? Boolean(row.accepting_orders) : undefined,
      closingPriceSource: row.closing_price_source
        ? (String(row.closing_price_source) as RoundRecord["closingPriceSource"])
        : undefined
    };
  }

  private rowToOrderBookSnapshotRecord(row: Record<string, unknown>): OrderBookSnapshotRecord {
    const parseJson = <T>(value: unknown, fallback: T): T => {
      if (typeof value === "string") {
        try {
          return JSON.parse(value) as T;
        } catch {
          return fallback;
        }
      }
      return (value as T) ?? fallback;
    };
    const snapshot = parseJson<OrderBookSnapshot>(row.snapshot, {
      snapshotId: String(row.snapshot_id),
      snapshotTs: Number(row.snapshot_ts),
      bestBid: Number(row.best_bid),
      bestAsk: Number(row.best_ask),
      midPrice: Number(row.mid_price),
      bids: [],
      asks: []
    });
    return {
      ref: String(row.ref),
      snapshotId: String(row.snapshot_id),
      snapshotTs: Number(row.snapshot_ts),
      bestBid: Number(row.best_bid),
      bestAsk: Number(row.best_ask),
      midPrice: Number(row.mid_price),
      snapshot: cloneOrderBookSnapshot(snapshot),
      createdAt: Number(row.created_at)
    };
  }

  private rowToOrderLifecycle(row: Record<string, unknown>): OrderLifecycleRecord {
    const numberOrUndefined = (value: unknown) => value === null || typeof value === "undefined" ? undefined : Number(value);
    return {
      id: String(row.id),
      buyOrderId: String(row.buy_order_id),
      traceId: String(row.trace_id),
      userId: String(row.user_id),
      testerId: String(row.tester_id),
      roundId: String(row.round_id),
      symbol: String(row.symbol),
      assetClass: "BTC",
      marketId: String(row.market_id),
      marketSlug: row.market_slug ? String(row.market_slug) : undefined,
      direction: row.direction as TradeSide,
      orderTimestampMs: Number(row.order_timestamp_ms),
      entryTokenPrice: numberOrUndefined(row.entry_token_price),
      btcTradePrice: numberOrUndefined(row.btc_trade_price),
      btcOpenPriceToBeat: numberOrUndefined(row.btc_open_price_to_beat),
      deltaBtc: numberOrUndefined(row.delta_btc),
      volumeTokenQty: Number(row.volume_token_qty),
      remainingTokenQty: Number(row.remaining_token_qty),
      closedTokenQty: Number(row.closed_token_qty),
      positionNotional: Number(row.position_notional),
      exitType: row.exit_type ? (String(row.exit_type) as OrderLifecycleExitType) : undefined,
      exitTokenPrice: numberOrUndefined(row.exit_token_price),
      exitNotional: Number(row.exit_notional),
      settlementResult: row.settlement_result
        ? (String(row.settlement_result) as OrderLifecycleRecord["settlementResult"])
        : undefined,
      orderBookSnapshotRef: row.order_book_snapshot_ref ? String(row.order_book_snapshot_ref) : undefined,
      actualFillPrice: numberOrUndefined(row.actual_fill_price),
      slippageBps: numberOrUndefined(row.slippage_bps),
      matchLatencyMs: Number(row.match_latency_ms),
      settlementTimeMs: numberOrUndefined(row.settlement_time_ms),
      settlementDirection: row.settlement_direction ? (String(row.settlement_direction) as TradeSide) : undefined,
      createdAt: Number(row.created_at),
      updatedAt: Number(row.updated_at)
    };
  }

  private rowToOrder(row: Record<string, unknown>): OrderRecord {
    const parseJson = <T>(value: unknown, fallback: T): T => {
      if (typeof value === "string") {
        try {
          return JSON.parse(value) as T;
        } catch {
          return fallback;
        }
      }
      return (value as T) ?? fallback;
    };
    const numberOrUndefined = (value: unknown) => value === null || typeof value === "undefined" ? undefined : Number(value);
    return {
      id: String(row.id),
      traceId: String(row.trace_id),
      userId: String(row.user_id),
      roundId: String(row.round_id),
      symbol: String(row.symbol),
      marketId: String(row.market_id),
      action: row.action as OrderRecord["action"],
      side: row.side as OrderRecord["side"],
      status: row.status as OrderRecord["status"],
      orderKind: row.order_kind ? (String(row.order_kind) as OrderRecord["orderKind"]) : undefined,
      timeInForce: row.time_in_force ? (String(row.time_in_force) as OrderRecord["timeInForce"]) : undefined,
      limitPrice: numberOrUndefined(row.limit_price),
      lifecycleStatus: row.lifecycle_status ? (String(row.lifecycle_status) as OrderRecord["lifecycleStatus"]) : undefined,
      resultType: row.result_type ? (String(row.result_type) as OrderRecord["resultType"]) : undefined,
      tokenId: row.token_id ? String(row.token_id) : undefined,
      bookKey: row.book_key ? String(row.book_key) : undefined,
      bookHash: row.book_hash ? String(row.book_hash) : undefined,
      requestedAmountUsdc: numberOrUndefined(row.requested_amount_usdc),
      requestedQty: numberOrUndefined(row.requested_qty),
      frozenUsdc: numberOrUndefined(row.frozen_usdc),
      frozenQty: numberOrUndefined(row.frozen_qty),
      fills: parseJson(row.fills, []),
      sourceLatencyMs: numberOrUndefined(row.source_latency_ms),
      marketSlug: row.market_slug ? String(row.market_slug) : undefined,
      orderBookSnapshotRef: row.order_book_snapshot_ref ? String(row.order_book_snapshot_ref) : undefined,
      orderBookSnapshot: parseJson<OrderBookSnapshot | undefined>(row.order_book_snapshot, undefined),
      notionalUsdc: Number(row.notional_usdc),
      expectedQty: Number(row.expected_qty),
      filledQty: Number(row.filled_qty),
      unfilledQty: Number(row.unfilled_qty),
      avgFillPrice: row.avg_fill_price !== null ? Number(row.avg_fill_price) : undefined,
      bestBid: Number(row.best_bid),
      bestAsk: Number(row.best_ask),
      midPrice: Number(row.mid_price),
      bookSnapshotTs: Number(row.book_snapshot_ts),
      partialFilled: Boolean(row.partial_filled),
      slippageBps: row.slippage_bps !== null ? Number(row.slippage_bps) : undefined,
      matchLatencyMs: Number(row.match_latency_ms),
      bookAcquireLatencyMs: numberOrUndefined(row.book_acquire_latency_ms),
      localMatchLatencyMs: numberOrUndefined(row.local_match_latency_ms),
      persistLatencyMs: numberOrUndefined(row.persist_latency_ms),
      totalOrderLatencyMs: numberOrUndefined(row.total_order_latency_ms),
      failureReason: row.failure_reason ? String(row.failure_reason) : undefined,
      clientSendTs: row.client_send_ts ? Number(row.client_send_ts) : undefined,
      serverRecvTs: Number(row.server_recv_ts),
      serverPublishTs: Number(row.server_publish_ts),
      createdAt: Number(row.created_at)
    };
  }

  private rowToPosition(row: Record<string, unknown>): PositionRecord {
    const numberOrUndefined = (value: unknown) => value === null || typeof value === "undefined" ? undefined : Number(value);
    return {
      id: String(row.id),
      userId: String(row.user_id),
      roundId: String(row.round_id),
      side: row.side as PositionRecord["side"],
      qty: Number(row.qty),
      lockedQty: numberOrUndefined(row.locked_qty),
      averageEntry: Number(row.average_entry),
      notionalSpent: Number(row.notional_spent),
      currentMark: Number(row.current_mark),
      currentBid: numberOrUndefined(row.current_bid),
      currentAsk: numberOrUndefined(row.current_ask),
      currentMid: numberOrUndefined(row.current_mid),
      currentValue: numberOrUndefined(row.current_value),
      sourceLatencyMs: numberOrUndefined(row.source_latency_ms),
      unrealizedPnl: Number(row.unrealized_pnl),
      realizedPnl: Number(row.realized_pnl),
      status: row.status as PositionRecord["status"],
      openedAt: Number(row.opened_at),
      closedAt: row.closed_at ? Number(row.closed_at) : undefined,
      settlementResult: row.settlement_result
        ? (String(row.settlement_result) as PositionRecord["settlementResult"])
        : undefined
    };
  }

  private rowToAuditEvent(row: Record<string, unknown>): AuditEvent {
    return {
      eventId: String(row.event_id),
      traceId: String(row.trace_id),
      category: row.category as AuditEvent["category"],
      actionType: String(row.action_type),
      actionStatus: row.action_status as AuditEvent["actionStatus"],
      userId: row.user_id ? String(row.user_id) : undefined,
      role: row.role ? (String(row.role) as Role) : undefined,
      pageName: String(row.page_name),
      moduleName: String(row.module_name),
      symbol: row.symbol ? String(row.symbol) : undefined,
      roundId: row.round_id ? String(row.round_id) : undefined,
      resultCode: String(row.result_code),
      resultMessage: String(row.result_message),
      clientSendTs: row.client_send_ts ? Number(row.client_send_ts) : undefined,
      serverRecvTs: Number(row.server_recv_ts),
      engineStartTs: row.engine_start_ts ? Number(row.engine_start_ts) : undefined,
      engineFinishTs: row.engine_finish_ts ? Number(row.engine_finish_ts) : undefined,
      serverPublishTs: Number(row.server_publish_ts),
      backendLatencyMs: Number(row.backend_latency_ms),
      frontendLatencyMs: row.frontend_latency_ms !== null ? Number(row.frontend_latency_ms) : undefined,
      details: (row.details as Record<string, unknown> | null) ?? undefined
    };
  }

  private rowToBehaviorLog(row: Record<string, unknown>): BehaviorActionLog {
    const parseJson = <T>(value: unknown, fallback: T): T => {
      if (typeof value === "string") {
        try {
          return JSON.parse(value) as T;
        } catch {
          return fallback;
        }
      }
      return (value as T) ?? fallback;
    };

    return {
      logId: String(row.log_id),
      timestampMs: Number(row.timestamp_ms),
      assetClass: "BTC_5M_UPDOWN",
      actionType: String(row.action_type),
      actionStatus: row.action_status as BehaviorActionLog["actionStatus"],
      roundId: row.round_id ? String(row.round_id) : undefined,
      direction: row.direction ? (String(row.direction) as BehaviorActionLog["direction"]) : undefined,
      entryOdds: row.entry_odds !== null ? Number(row.entry_odds) : undefined,
      deltaClob: Number(row.delta_clob),
      volumeClob: Number(row.volume_clob),
      positionNotional: row.position_notional !== null ? Number(row.position_notional) : undefined,
      exitType: row.exit_type ? String(row.exit_type) : undefined,
      exitOdds: row.exit_odds !== null ? Number(row.exit_odds) : undefined,
      settlementResult: row.settlement_result
        ? (String(row.settlement_result) as BehaviorActionLog["settlementResult"])
        : undefined,
      testerIdAnon: String(row.tester_id_anon),
      traceId: row.trace_id ? String(row.trace_id) : undefined,
      orderId: row.order_id ? String(row.order_id) : undefined,
      marketId: row.market_id ? String(row.market_id) : undefined,
      marketSlug: row.market_slug ? String(row.market_slug) : undefined,
      roundStatus: row.round_status ? (String(row.round_status) as RoundStatus) : undefined,
      countdownMs: row.countdown_ms !== null ? Number(row.countdown_ms) : undefined,
      binanceSpotPrice: Number(row.binance_spot_price),
      binance1mLastClose: Number(row.binance_1m_last_close),
      binance5mLastClose: Number(row.binance_5m_last_close),
      binance1dLastClose: Number(row.binance_1d_last_close),
      chainlinkPrice: Number(row.chainlink_price),
      priceToBeat: Number(row.price_to_beat),
      upPrice: Number(row.up_price),
      downPrice: Number(row.down_price),
      upBookTop5: parseJson(row.up_book_top5, []),
      downBookTop5: parseJson(row.down_book_top5, []),
      recentTradesTop20: parseJson(row.recent_trades_top20, []),
      bookSnapshotEntry: parseJson(row.book_snapshot_entry, {
        snapshotId: "",
        snapshotTs: 0,
        topBids: [],
        topAsks: []
      }),
      actualFillPrice: row.actual_fill_price !== null ? Number(row.actual_fill_price) : undefined,
      slippageBps: row.slippage_bps !== null ? Number(row.slippage_bps) : undefined,
      partialFilled: row.partial_filled !== null ? Boolean(row.partial_filled) : undefined,
      unfilledQty: row.unfilled_qty !== null ? Number(row.unfilled_qty) : undefined,
      executionLatencyMs: row.execution_latency_ms !== null ? Number(row.execution_latency_ms) : undefined,
      settlementDirection: row.settlement_direction
        ? (String(row.settlement_direction) as TradeSide)
        : undefined,
      settlementTimeMs: row.settlement_time_ms !== null ? Number(row.settlement_time_ms) : undefined,
      gammaPollCount: row.gamma_poll_count !== null ? Number(row.gamma_poll_count) : undefined,
      redeemFinishTimeMs: row.redeem_finish_time_ms !== null ? Number(row.redeem_finish_time_ms) : undefined,
      sourceStates: parseJson(row.source_states, {
        binance: { source: "Binance", state: "reconnecting", sourceEventTs: 0, serverRecvTs: 0, serverPublishTs: 0 },
        chainlink: { source: "Chainlink", state: "reconnecting", sourceEventTs: 0, serverRecvTs: 0, serverPublishTs: 0 },
        clob: { source: "CLOB", state: "reconnecting", sourceEventTs: 0, serverRecvTs: 0, serverPublishTs: 0 }
      }),
      strategyClusterLabel: row.strategy_cluster_label ? String(row.strategy_cluster_label) : undefined,
      marketRegimeLabel: row.market_regime_label ? String(row.market_regime_label) : undefined,
      qualityGrade: row.quality_grade ? String(row.quality_grade) : undefined,
      contextJson: parseJson(row.context_json, {})
    };
  }

  private async runDb(query: string, params: unknown[]) {
    if (!this.postgresEnabled || !this.pool) {
      return;
    }
    try {
      await this.pool.query(query, params as never[]);
    } catch (error) {
      console.warn("[store] PostgreSQL write failed:", error);
    }
  }
}

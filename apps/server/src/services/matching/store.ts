import { closeSync, existsSync, mkdirSync, openSync, readSync, statSync } from "node:fs";
import path from "node:path";
import { nanoid } from "nanoid";
import { Pool } from "pg";
import { createClient } from "redis";
import type {
  MatchingBookState,
  MatchingEventRecord,
  MatchingLogQuery,
  MatchingReplayResult,
  MatchingReplayStep
} from "../../domain/types";
import { AsyncJsonlWriter } from "../log-writer";

const STARTUP_CONNECT_RETRY_ATTEMPTS = 10;
const STARTUP_CONNECT_RETRY_DELAY_MS = 2000;
const MATCHING_FILE_TAIL_BYTES = 512 * 1024;
const PERSISTENCE_FAILURE_THRESHOLD = 3;

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

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS matching_book_snapshots (
  snapshot_id TEXT PRIMARY KEY,
  book_key TEXT NOT NULL,
  round_id TEXT,
  market_id TEXT,
  book_side TEXT NOT NULL,
  sequence BIGINT NOT NULL,
  priority_sequence BIGINT NOT NULL,
  snapshot_ts BIGINT NOT NULL,
  best_bid DOUBLE PRECISION NOT NULL,
  best_ask DOUBLE PRECISION NOT NULL,
  mid_price DOUBLE PRECISION NOT NULL,
  snapshot JSONB NOT NULL,
  state JSONB NOT NULL,
  source_snapshot_id TEXT,
  created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_matching_snapshots_book_seq
  ON matching_book_snapshots(book_key, sequence DESC);

CREATE TABLE IF NOT EXISTS matching_events (
  event_id TEXT PRIMARY KEY,
  book_key TEXT NOT NULL,
  round_id TEXT,
  market_id TEXT,
  book_side TEXT NOT NULL,
  sequence BIGINT NOT NULL,
  event_type TEXT NOT NULL,
  order_id TEXT,
  trace_id TEXT,
  payload JSONB NOT NULL,
  created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_matching_events_book_seq
  ON matching_events(book_key, sequence DESC);
CREATE INDEX IF NOT EXISTS idx_matching_events_created_at
  ON matching_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_matching_events_round_created
  ON matching_events(round_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_matching_events_market_created
  ON matching_events(market_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_matching_events_type_created
  ON matching_events(event_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_matching_events_trace_created
  ON matching_events(trace_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_matching_events_order_created
  ON matching_events(order_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_matching_events_payload_user_id
  ON matching_events((payload #>> '{request,userId}'));
`;

const LOG_DIR = path.resolve(process.cwd(), "data/logs");
const EVENT_LOG_FILE = path.join(LOG_DIR, "matching-events.jsonl");
const SNAPSHOT_LOG_FILE = path.join(LOG_DIR, "matching-snapshots.jsonl");
const JSONL_WRITER_OPTIONS = {
  batchSize: 100,
  flushIntervalMs: 500,
  maxFileBytes: 50 * 1024 * 1024,
  maxQueueDepth: 10_000
};

interface StoredSnapshotLine {
  bookKey: string;
  state: MatchingBookState;
}

export class MatchingStore {
  private pool?: Pool;
  private redis?: ReturnType<typeof createClient>;
  private postgresEnabled = false;
  private redisEnabled = false;
  private postgresReconnectTask?: Promise<void>;
  private closed = false;
  private readonly books = new Map<string, MatchingBookState>();
  private readonly events: MatchingEventRecord[] = [];
  private readonly eventLogWriter = new AsyncJsonlWriter<MatchingEventRecord>(EVENT_LOG_FILE, JSONL_WRITER_OPTIONS);
  private readonly snapshotLogWriter = new AsyncJsonlWriter<{ bookKey: string; state: MatchingBookState }>(
    SNAPSHOT_LOG_FILE,
    JSONL_WRITER_OPTIONS
  );
  private readonly persistenceHealth: {
    postgres: PersistenceHealth;
    redis: PersistenceHealth;
  };

  constructor(
    private readonly config: {
      databaseUrl: string;
      redisUrl: string;
      persistenceMode: "external" | "memory";
      redisSnapshotSeconds: number;
      strictPersistence: boolean;
      pgConnectionTimeoutMs: number;
      pgIdleTimeoutMs: number;
      pgMaxConnections: number;
      pgKeepAlive: boolean;
      pgReconnectIntervalMs: number;
      pgReconnectMaxIntervalMs: number;
      eventsMemoryMax: number;
      eventsMemoryMaxAgeMs: number;
      booksMemoryMax: number;
    }
  ) {
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
    await this.connectPostgres();
    await this.connectRedis();
    await this.loadWarmState();
  }

  async close() {
    this.closed = true;
    if (this.redis?.isOpen) {
      await this.redis.quit().catch(() => undefined);
    }
    await this.eventLogWriter.close();
    await this.snapshotLogWriter.close();
    await this.closePostgresPool();
    await this.postgresReconnectTask?.catch(() => undefined);
  }

  getPersistenceStatus() {
    return {
      postgres: this.postgresEnabled,
      redis: this.redisEnabled,
      strict: this.config.strictPersistence,
      state: this.persistenceHealth
    };
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
      void this.handlePostgresFailure(error, "pool error");
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

  private persistenceUnavailableMessage() {
    const health = this.persistenceHealth.postgres;
    const base = "Matching PostgreSQL persistence is unavailable.";
    if (health.reconnecting || health.state === "reconnecting") {
      return `${base} PostgreSQL is reconnecting.`;
    }
    if (health.lastError) {
      return `${base} ${health.lastError}`;
    }
    return base;
  }

  private async handlePostgresFailure(error: unknown, source: string) {
    this.postgresEnabled = false;
    this.notePersistenceFailure("postgres", error);
    await this.closePostgresPool();
    console.warn(`[matching] PostgreSQL ${source} failed:`, error);
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
    console.warn(`[matching] PostgreSQL entering reconnecting state (${reason}).`);
    this.postgresReconnectTask = this.reconnectPostgresLoop()
      .catch((error) => {
        console.warn("[matching] PostgreSQL reconnect loop stopped with error:", error);
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
        await pool.query(SCHEMA_SQL);
        this.pool = pool;
        this.postgresEnabled = true;
        this.notePersistenceSuccess("postgres");
        console.log("[matching] PostgreSQL reconnect succeeded.");
        return;
      } catch (error) {
        this.postgresEnabled = false;
        this.notePersistenceFailure("postgres", error);
        health.state = "reconnecting";
        health.reconnecting = true;
        console.warn(
          `[matching] PostgreSQL reconnect attempt ${health.reconnectAttempts} failed; retrying in ${delayMs}ms:`,
          error
        );
        await sleep(delayMs);
        delayMs = Math.min(delayMs * 2, this.config.pgReconnectMaxIntervalMs);
      }
    }
  }

  private pruneMemory(now = Date.now()) {
    const cutoff = now - this.config.eventsMemoryMaxAgeMs;
    const retainedEvents = this.events
      .filter((event) => event.createdAt >= cutoff)
      .sort((left, right) => right.createdAt - left.createdAt)
      .slice(0, this.config.eventsMemoryMax)
      .reverse();
    this.events.splice(0, this.events.length, ...retainedEvents);

    const activeBookKeys = new Set(this.events.map((event) => event.bookKey));
    const sortedBooks = [...this.books.values()].sort((left, right) => right.updatedAt - left.updatedAt);
    let retainedCount = 0;
    for (const book of sortedBooks) {
      if (activeBookKeys.has(book.bookKey) || retainedCount < this.config.booksMemoryMax) {
        retainedCount += 1;
        continue;
      }
      this.books.delete(book.bookKey);
    }
  }

  getCurrentBook(bookKey: string) {
    const state = this.books.get(bookKey);
    return state ? this.cloneState(state) : undefined;
  }

  async saveStep(event: MatchingEventRecord, state: MatchingBookState) {
    if (this.config.persistenceMode === "external" && this.config.strictPersistence && !this.persistenceHealth.postgres.writable) {
      throw new Error("Matching persistence is unavailable.");
    }
    const stateCopy = this.cloneState(state);
    this.books.set(state.bookKey, stateCopy);
    this.events.push({ ...event, payload: { ...event.payload } });
    this.pruneMemory(event.createdAt);

    this.eventLogWriter.write(event);
    this.snapshotLogWriter.write({
      bookKey: state.bookKey,
      state: stateCopy
    });

    await Promise.all([
      this.runDb(
        `
        INSERT INTO matching_events (
          event_id, book_key, round_id, market_id, book_side, sequence,
          event_type, order_id, trace_id, payload, created_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        ON CONFLICT (event_id) DO NOTHING
        `,
        [
          event.eventId,
          event.bookKey,
          event.roundId ?? null,
          event.marketId ?? null,
          event.bookSide,
          event.sequence,
          event.eventType,
          event.orderId ?? null,
          event.traceId ?? null,
          JSON.stringify(event.payload),
          event.createdAt
        ]
      ),
      this.runDb(
        `
        INSERT INTO matching_book_snapshots (
          snapshot_id, book_key, round_id, market_id, book_side, sequence,
          priority_sequence, snapshot_ts, best_bid, best_ask, mid_price,
          snapshot, state, source_snapshot_id, created_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
        ON CONFLICT (snapshot_id) DO NOTHING
        `,
        [
          state.snapshot.snapshotId,
          state.bookKey,
          state.roundId ?? null,
          state.marketId ?? null,
          state.bookSide,
          state.sequence,
          state.prioritySequence,
          state.snapshot.snapshotTs,
          state.snapshot.bestBid,
          state.snapshot.bestAsk,
          state.snapshot.midPrice,
          JSON.stringify(state.snapshot),
          JSON.stringify(state),
          state.sourceSnapshotId ?? null,
          state.updatedAt
        ]
      ),
      this.writeRedisSnapshot(stateCopy)
    ]);
  }

  async getReplay(
    bookKey: string,
    options?: { fromSequence?: number; toSequence?: number; limit?: number }
  ): Promise<MatchingReplayResult> {
    const limit = Math.max(1, Math.min(options?.limit ?? 50, 500));

    if (this.postgresEnabled && this.pool) {
      const filters: string[] = ["book_key = $1"];
      const values: Array<string | number> = [bookKey];

      if (typeof options?.fromSequence === "number") {
        values.push(options.fromSequence);
        filters.push(`sequence >= $${values.length}`);
      }
      if (typeof options?.toSequence === "number") {
        values.push(options.toSequence);
        filters.push(`sequence <= $${values.length}`);
      }
      values.push(limit);

      const clause = filters.join(" AND ");
      const [eventRows, snapshotRows] = await Promise.all([
        this.pool.query(
          `
          SELECT *
          FROM (
            SELECT *
            FROM matching_events
            WHERE ${clause}
            ORDER BY sequence DESC
            LIMIT $${values.length}
          ) recent
          ORDER BY sequence ASC
          `,
          values
        ),
        this.pool.query(
          `
          SELECT *
          FROM (
            SELECT *
            FROM matching_book_snapshots
            WHERE ${clause}
            ORDER BY sequence DESC
            LIMIT $${values.length}
          ) recent
          ORDER BY sequence ASC
          `,
          values
        )
      ]);

      const snapshotsBySequence = new Map<number, MatchingBookState>();
      for (const row of snapshotRows.rows) {
        const state = this.rowToState(row);
        snapshotsBySequence.set(state.sequence, state);
      }

      const steps: MatchingReplayStep[] = eventRows.rows
        .map((row) => this.rowToEvent(row))
        .map((event) => {
          const snapshot = snapshotsBySequence.get(event.sequence);
          return snapshot
            ? {
                event,
                snapshot
              }
            : undefined;
        })
        .filter((step): step is MatchingReplayStep => Boolean(step));

      return {
        bookKey,
        latest: this.getCurrentBook(bookKey),
        steps
      };
    }

    const filteredEvents = this.events
      .filter((event) => {
        if (event.bookKey !== bookKey) {
          return false;
        }
        if (typeof options?.fromSequence === "number" && event.sequence < options.fromSequence) {
          return false;
        }
        if (typeof options?.toSequence === "number" && event.sequence > options.toSequence) {
          return false;
        }
        return true;
      })
      .slice(-limit);

    const snapshotsBySequence = new Map(
      this.booksFromFile()
        .filter((line) => line.bookKey === bookKey)
        .map((line) => [line.state.sequence, line.state] as const)
    );

    const steps = filteredEvents
      .map((event) => {
        const snapshot = snapshotsBySequence.get(event.sequence);
        return snapshot
          ? {
              event: { ...event, payload: { ...event.payload } },
              snapshot
            }
          : undefined;
      })
      .filter((step): step is MatchingReplayStep => Boolean(step));

    return {
      bookKey,
      latest: this.getCurrentBook(bookKey),
      steps
    };
  }

  async searchEvents(query: MatchingLogQuery = {}) {
    const limit = Math.max(1, Math.min(Math.floor(query.limit ?? 100), 501));
    const offset = Math.max(0, Math.floor(query.offset ?? 0));
    const scopedUserIds = query.userId ? [query.userId] : query.userIds;
    if (scopedUserIds && scopedUserIds.length === 0) {
      return [] as MatchingEventRecord[];
    }

    if (this.postgresEnabled && this.pool) {
      const filters: string[] = [];
      const values: unknown[] = [];
      const add = (clause: string, value: unknown) => {
        values.push(value);
        filters.push(`${clause} $${values.length}`);
      };

      if (typeof query.from === "number") {
        add("created_at >=", query.from);
      }
      if (typeof query.to === "number") {
        add("created_at <=", query.to);
      }
      if (query.bookKey) {
        add("book_key =", query.bookKey);
      }
      if (query.bookSide) {
        add("book_side =", query.bookSide);
      }
      if (query.eventType) {
        add("event_type =", query.eventType);
      }
      if (query.roundId) {
        add("round_id =", query.roundId);
      }
      if (query.marketId) {
        add("market_id =", query.marketId);
      }
      if (query.traceId) {
        add("trace_id =", query.traceId);
      }
      if (query.orderId) {
        add("order_id =", query.orderId);
      }
      if (typeof query.sequenceFrom === "number") {
        add("sequence >=", query.sequenceFrom);
      }
      if (typeof query.sequenceTo === "number") {
        add("sequence <=", query.sequenceTo);
      }
      if (scopedUserIds?.length) {
        values.push(scopedUserIds);
        filters.push(`payload #>> '{request,userId}' = ANY($${values.length}::text[])`);
      }

      values.push(limit, offset);
      const rows = await this.pool.query(
        `
        SELECT *
        FROM matching_events
        ${filters.length ? `WHERE ${filters.join(" AND ")}` : ""}
        ORDER BY created_at DESC, sequence DESC, event_id DESC
        LIMIT $${values.length - 1} OFFSET $${values.length}
        `,
        values as never[]
      );
      return rows.rows.map((row) => this.rowToEvent(row));
    }

    return this.events
      .filter((event) => this.matchesSearch(event, query, scopedUserIds))
      .sort(
        (left, right) =>
          right.createdAt - left.createdAt ||
          right.sequence - left.sequence ||
          right.eventId.localeCompare(left.eventId)
      )
      .slice(offset, offset + limit)
      .map((event) => ({ ...event, payload: { ...event.payload } }));
  }

  newId(prefix: string) {
    return `${prefix}_${nanoid(12)}`;
  }

  private async connectPostgres() {
    if (this.config.persistenceMode === "memory") {
      console.warn("[matching] PERSISTENCE_MODE=memory; skipping PostgreSQL connection.");
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
        await this.pool.query(SCHEMA_SQL);
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
            `[matching] PostgreSQL is not ready yet (attempt ${attempt}/${STARTUP_CONNECT_RETRY_ATTEMPTS}); retrying in ${STARTUP_CONNECT_RETRY_DELAY_MS}ms`
          );
          await sleep(STARTUP_CONNECT_RETRY_DELAY_MS);
        }
      }
    }

    if (this.config.strictPersistence) {
      throw new Error(
        `[matching] PostgreSQL is required but unavailable: ${
          lastError instanceof Error ? lastError.message : String(lastError)
        }`
      );
    }
    console.warn("[matching] PostgreSQL is unavailable, using file-backed replay only:", lastError);
  }

  private async connectRedis() {
    if (this.config.persistenceMode === "memory") {
      console.warn("[matching] PERSISTENCE_MODE=memory; skipping Redis connection.");
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
          console.warn("[matching] Redis connection error:", error);
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
            `[matching] Redis is not ready yet (attempt ${attempt}/${STARTUP_CONNECT_RETRY_ATTEMPTS}); retrying in ${STARTUP_CONNECT_RETRY_DELAY_MS}ms`
          );
          await sleep(STARTUP_CONNECT_RETRY_DELAY_MS);
        }
      }
    }

    console.warn("[matching] Redis is unavailable, skipping order book cache:", lastError);
  }

  private async loadWarmState() {
    if (this.postgresEnabled && this.pool) {
      const snapshotRows = await this.pool.query(`
        SELECT DISTINCT ON (book_key) *
        FROM matching_book_snapshots
        ORDER BY book_key, sequence DESC
      `);
      for (const row of snapshotRows.rows) {
        const state = this.rowToState(row);
        this.books.set(state.bookKey, state);
      }

      const eventRows = await this.pool.query(`
        SELECT *
        FROM matching_events
        ORDER BY sequence DESC
        LIMIT 1000
      `);
      this.events.splice(
        0,
        this.events.length,
        ...eventRows.rows.reverse().map((row) => this.rowToEvent(row))
      );
      this.pruneMemory();
      return;
    }

    for (const line of this.booksFromFile()) {
      const existing = this.books.get(line.bookKey);
      if (!existing || existing.sequence < line.state.sequence) {
        this.books.set(line.bookKey, line.state);
      }
    }

    for (const event of this.eventsFromFile().slice(-1000)) {
      this.events.push(event);
    }
    this.pruneMemory();
  }

  private async writeRedisSnapshot(state: MatchingBookState) {
    if (!this.redisEnabled || !this.redis?.isOpen) {
      return;
    }
    try {
      await this.redis.set(`matching:book:${state.bookKey}`, JSON.stringify(state), {
        expiration: {
          type: "EX",
          value: this.config.redisSnapshotSeconds
        }
      });
      this.notePersistenceSuccess("redis");
    } catch (error) {
      this.redisEnabled = false;
      this.notePersistenceFailure("redis", error);
      console.warn("[matching] Redis cache write failed:", error);
    }
  }

  private async runDb(query: string, values: unknown[]) {
    if (!this.postgresEnabled || !this.pool) {
      if (this.config.persistenceMode === "external" && this.config.strictPersistence) {
        void this.schedulePostgresReconnect("write requested while unavailable");
        throw new Error(this.persistenceUnavailableMessage());
      }
      return;
    }
    try {
      await this.pool.query(query, values);
      this.notePersistenceSuccess("postgres");
    } catch (error) {
      await this.handlePostgresFailure(error, "write");
      if (this.config.strictPersistence) {
        throw new Error(this.persistenceUnavailableMessage());
      }
    }
  }

  private booksFromFile() {
    const text = readUtf8Tail(SNAPSHOT_LOG_FILE, MATCHING_FILE_TAIL_BYTES);
    const normalized = text.includes("\n") ? text.slice(text.indexOf("\n") + 1) : text;
    return normalized
      .split(/\r?\n/)
      .filter(Boolean)
      .map((line) => {
        try {
          return JSON.parse(line) as StoredSnapshotLine;
        } catch {
          return undefined;
        }
      })
      .filter((line): line is StoredSnapshotLine => Boolean(line?.bookKey && line.state));
  }

  private eventsFromFile() {
    const text = readUtf8Tail(EVENT_LOG_FILE, MATCHING_FILE_TAIL_BYTES);
    const normalized = text.includes("\n") ? text.slice(text.indexOf("\n") + 1) : text;
    return normalized
      .split(/\r?\n/)
      .filter(Boolean)
      .map((line) => {
        try {
          return JSON.parse(line) as MatchingEventRecord;
        } catch {
          return undefined;
        }
      })
      .filter((event): event is MatchingEventRecord => Boolean(event?.eventId));
  }

  private matchesSearch(event: MatchingEventRecord, query: MatchingLogQuery, userIds?: string[]) {
    if (typeof query.from === "number" && event.createdAt < query.from) {
      return false;
    }
    if (typeof query.to === "number" && event.createdAt > query.to) {
      return false;
    }
    if (query.bookKey && event.bookKey !== query.bookKey) {
      return false;
    }
    if (query.bookSide && event.bookSide !== query.bookSide) {
      return false;
    }
    if (query.eventType && event.eventType !== query.eventType) {
      return false;
    }
    if (query.roundId && event.roundId !== query.roundId) {
      return false;
    }
    if (query.marketId && event.marketId !== query.marketId) {
      return false;
    }
    if (query.traceId && event.traceId !== query.traceId) {
      return false;
    }
    if (query.orderId && event.orderId !== query.orderId) {
      return false;
    }
    if (typeof query.sequenceFrom === "number" && event.sequence < query.sequenceFrom) {
      return false;
    }
    if (typeof query.sequenceTo === "number" && event.sequence > query.sequenceTo) {
      return false;
    }
    if (userIds?.length) {
      const request = event.payload.request as { userId?: string } | undefined;
      if (!request?.userId || !userIds.includes(request.userId)) {
        return false;
      }
    }
    return true;
  }

  private rowToState(row: Record<string, unknown>) {
    const rawState =
      typeof row.state === "string" ? (JSON.parse(row.state) as MatchingBookState) : (row.state as MatchingBookState);
    return this.cloneState(rawState);
  }

  private rowToEvent(row: Record<string, unknown>): MatchingEventRecord {
    const payload =
      typeof row.payload === "string" ? (JSON.parse(row.payload) as Record<string, unknown>) : (row.payload as Record<string, unknown>);
    return {
      eventId: String(row.event_id),
      bookKey: String(row.book_key),
      roundId: row.round_id ? String(row.round_id) : undefined,
      marketId: row.market_id ? String(row.market_id) : undefined,
      bookSide: String(row.book_side) as MatchingEventRecord["bookSide"],
      sequence: Number(row.sequence),
      eventType: String(row.event_type) as MatchingEventRecord["eventType"],
      orderId: row.order_id ? String(row.order_id) : undefined,
      traceId: row.trace_id ? String(row.trace_id) : undefined,
      payload,
      createdAt: Number(row.created_at)
    };
  }

  private cloneState(state: MatchingBookState): MatchingBookState {
    return {
      ...state,
      snapshot: {
        ...state.snapshot,
        bids: state.snapshot.bids.map((level) => ({ ...level })),
        asks: state.snapshot.asks.map((level) => ({ ...level }))
      },
      bids: state.bids.map((order) => ({
        ...order,
        meta: order.meta ? { ...order.meta } : undefined
      })),
      asks: state.asks.map((order) => ({
        ...order,
        meta: order.meta ? { ...order.meta } : undefined
      }))
    };
  }
}

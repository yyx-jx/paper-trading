import { appendFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { nanoid } from "nanoid";
import { Pool } from "pg";
import { createClient } from "redis";
import type {
  MatchingBookState,
  MatchingEventRecord,
  MatchingReplayResult,
  MatchingReplayStep
} from "../../domain/types";

const STARTUP_CONNECT_RETRY_ATTEMPTS = 10;
const STARTUP_CONNECT_RETRY_DELAY_MS = 2000;

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
`;

const LOG_DIR = path.resolve(process.cwd(), "data/logs");
const EVENT_LOG_FILE = path.join(LOG_DIR, "matching-events.jsonl");
const SNAPSHOT_LOG_FILE = path.join(LOG_DIR, "matching-snapshots.jsonl");

interface StoredSnapshotLine {
  bookKey: string;
  state: MatchingBookState;
}

export class MatchingStore {
  private pool?: Pool;
  private redis?: ReturnType<typeof createClient>;
  private postgresEnabled = false;
  private redisEnabled = false;
  private readonly books = new Map<string, MatchingBookState>();
  private readonly events: MatchingEventRecord[] = [];

  constructor(
    private readonly config: {
      databaseUrl: string;
      redisUrl: string;
      redisSnapshotSeconds: number;
    }
  ) {
    mkdirSync(LOG_DIR, { recursive: true });
  }

  async init() {
    await this.connectPostgres();
    await this.connectRedis();
    await this.loadWarmState();
  }

  async close() {
    if (this.redis?.isOpen) {
      await this.redis.quit().catch(() => undefined);
    }
    if (this.pool) {
      await this.pool.end().catch(() => undefined);
    }
  }

  getPersistenceStatus() {
    return {
      postgres: this.postgresEnabled,
      redis: this.redisEnabled
    };
  }

  getCurrentBook(bookKey: string) {
    const state = this.books.get(bookKey);
    return state ? this.cloneState(state) : undefined;
  }

  async saveStep(event: MatchingEventRecord, state: MatchingBookState) {
    const stateCopy = this.cloneState(state);
    this.books.set(state.bookKey, stateCopy);
    this.events.push({ ...event, payload: { ...event.payload } });

    appendFileSync(EVENT_LOG_FILE, `${JSON.stringify(event)}\n`, "utf-8");
    appendFileSync(
      SNAPSHOT_LOG_FILE,
      `${JSON.stringify({
        bookKey: state.bookKey,
        state: stateCopy
      })}\n`,
      "utf-8"
    );

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

    const steps = filteredEvents
      .map((event) => {
        const snapshot = this.booksFromFile()
          .filter((line) => line.bookKey === bookKey && line.state.sequence === event.sequence)
          .map((line) => line.state)[0];
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

  newId(prefix: string) {
    return `${prefix}_${nanoid(12)}`;
  }

  private async connectPostgres() {
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
            `[matching] PostgreSQL is not ready yet (attempt ${attempt}/${STARTUP_CONNECT_RETRY_ATTEMPTS}); retrying in ${STARTUP_CONNECT_RETRY_DELAY_MS}ms`
          );
          await sleep(STARTUP_CONNECT_RETRY_DELAY_MS);
        }
      }
    }

    console.warn("[matching] PostgreSQL is unavailable, using file-backed replay only:", lastError);
  }

  private async connectRedis() {
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
          console.warn("[matching] Redis connection error:", error);
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
    } catch (error) {
      this.redisEnabled = false;
      console.warn("[matching] Redis cache write failed:", error);
    }
  }

  private async runDb(query: string, values: unknown[]) {
    if (!this.postgresEnabled || !this.pool) {
      return;
    }
    await this.pool.query(query, values);
  }

  private booksFromFile() {
    if (!existsSync(SNAPSHOT_LOG_FILE)) {
      return [] as StoredSnapshotLine[];
    }
    return readFileSync(SNAPSHOT_LOG_FILE, "utf-8")
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
    if (!existsSync(EVENT_LOG_FILE)) {
      return [] as MatchingEventRecord[];
    }
    return readFileSync(EVENT_LOG_FILE, "utf-8")
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

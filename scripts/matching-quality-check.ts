import assert from "node:assert/strict";
import { mkdtemp, rm, mkdir } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { PriceTimeOrderBook } from "../apps/server/src/services/matching/order-book";
import { MatchingService } from "../apps/server/src/services/matching/service";
import { MatchingStore } from "../apps/server/src/services/matching/store";
import { MatchingServiceClient } from "../apps/server/src/services/matching/client";
import { serverConfig } from "../apps/server/src/config";
import type {
  MatchingBookState,
  MatchingEventRecord,
  MatchingExecutionRequest,
  MatchingFill,
  MatchingReplayResult,
  MatchingSyncRequest,
  OrderBookSnapshot,
  TradeSide
} from "../apps/server/src/domain/types";

type MetricSummary = {
  count: number;
  totalMs: number;
  avgMs: number;
  p95Ms: number;
  p99Ms: number;
};

type QualityReport = {
  correctness: string[];
  replayAndPersistence: string[];
  performance: string[];
  knownGaps: string[];
  integration?: string;
};

class InMemoryMatchingStore {
  private readonly currentBooks = new Map<string, MatchingBookState>();
  private readonly steps = new Map<string, Array<{ event: MatchingEventRecord; snapshot: MatchingBookState }>>();
  private sequence = 0;

  async init() {}

  async close() {}

  getPersistenceStatus() {
    return {
      postgres: false,
      redis: false
    };
  }

  getCurrentBook(bookKey: string) {
    const current = this.currentBooks.get(bookKey);
    return current ? clone(current) : undefined;
  }

  async saveStep(event: MatchingEventRecord, state: MatchingBookState) {
    const snapshot = clone(state);
    this.currentBooks.set(state.bookKey, snapshot);
    const steps = this.steps.get(state.bookKey) ?? [];
    steps.push({
      event: clone(event),
      snapshot
    });
    this.steps.set(state.bookKey, steps);
  }

  async getReplay(
    bookKey: string,
    options?: { fromSequence?: number; toSequence?: number; limit?: number }
  ): Promise<MatchingReplayResult> {
    const steps = (this.steps.get(bookKey) ?? []).filter((step) => {
      if (typeof options?.fromSequence === "number" && step.event.sequence < options.fromSequence) {
        return false;
      }
      if (typeof options?.toSequence === "number" && step.event.sequence > options.toSequence) {
        return false;
      }
      return true;
    });

    const limited =
      typeof options?.limit === "number" ? steps.slice(Math.max(steps.length - options.limit, 0)) : steps;

    return {
      bookKey,
      latest: this.getCurrentBook(bookKey),
      steps: limited.map((step) => ({
        event: clone(step.event),
        snapshot: clone(step.snapshot)
      }))
    };
  }

  newId(prefix: string) {
    this.sequence += 1;
    return `${prefix}_${this.sequence}`;
  }
}

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function makeBookSnapshot(overrides?: Partial<OrderBookSnapshot>): OrderBookSnapshot {
  return {
    snapshotId: overrides?.snapshotId ?? "snapshot-1",
    snapshotTs: overrides?.snapshotTs ?? Date.now(),
    bestBid: overrides?.bestBid ?? 0.48,
    bestAsk: overrides?.bestAsk ?? 0.5,
    midPrice: overrides?.midPrice ?? 0.49,
    bids: overrides?.bids ?? [
      { price: 0.48, qty: 120 },
      { price: 0.47, qty: 160 },
      { price: 0.46, qty: 220 }
    ],
    asks: overrides?.asks ?? [
      { price: 0.5, qty: 100 },
      { price: 0.51, qty: 140 },
      { price: 0.52, qty: 240 }
    ]
  };
}

function makeSyncRequest(bookKey: string, bookSide: TradeSide, snapshotId: string, now: number, snapshot?: Partial<OrderBookSnapshot>): MatchingSyncRequest {
  return {
    bookKey,
    roundId: "round-test",
    marketId: "market-test",
    bookSide,
    source: "Polymarket",
    sourceSnapshot: makeBookSnapshot({
      snapshotId,
      snapshotTs: now,
      ...snapshot
    }),
    syncedAt: now
  };
}

function makeExecutionRequest(input: Partial<MatchingExecutionRequest> & Pick<MatchingExecutionRequest, "orderId" | "traceId" | "userId" | "bookKey" | "bookSide" | "action" | "orderType" | "timeInForce" | "createdAt">): MatchingExecutionRequest {
  return {
    roundId: "round-test",
    marketId: "market-test",
    ...input
  };
}

function summarizeMetrics(values: number[]): MetricSummary {
  const sorted = [...values].sort((left, right) => left - right);
  const percentile = (ratio: number) => {
    if (sorted.length === 0) {
      return 0;
    }
    const index = Math.min(sorted.length - 1, Math.ceil(sorted.length * ratio) - 1);
    return sorted[index] ?? 0;
  };

  const totalMs = sorted.reduce((sum, value) => sum + value, 0);
  return {
    count: sorted.length,
    totalMs,
    avgMs: sorted.length > 0 ? totalMs / sorted.length : 0,
    p95Ms: percentile(0.95),
    p99Ms: percentile(0.99)
  };
}

function formatMetrics(name: string, metrics: MetricSummary) {
  return `${name}: count=${metrics.count}, total=${metrics.totalMs.toFixed(2)}ms, avg=${metrics.avgMs.toFixed(4)}ms, p95=${metrics.p95Ms.toFixed(4)}ms, p99=${metrics.p99Ms.toFixed(4)}ms`;
}

function assertBestBidAsk(snapshot: OrderBookSnapshot) {
  if (snapshot.bestBid > 0 && snapshot.bestAsk > 0) {
    assert.ok(snapshot.bestBid <= snapshot.bestAsk, `best bid ${snapshot.bestBid} exceeded best ask ${snapshot.bestAsk}`);
  }
}

function collectMakerOrderIds(fills: MatchingFill[]) {
  return fills.map((fill) => fill.makerOrderId);
}

async function testCorrectness(report: QualityReport) {
  const book = new PriceTimeOrderBook("correctness:UP", "UP");
  const now = Date.now();

  const askFirst = book.execute(
    makeExecutionRequest({
      orderId: "sell-rest-1",
      traceId: "trace-sell-rest-1",
      userId: "u-maker-1",
      bookKey: "correctness:UP",
      bookSide: "UP",
      action: "sell",
      orderType: "limit",
      timeInForce: "GTC",
      qty: 10,
      limitPrice: 0.6,
      createdAt: now
    })
  );
  const askBetter = book.execute(
    makeExecutionRequest({
      orderId: "sell-rest-2",
      traceId: "trace-sell-rest-2",
      userId: "u-maker-2",
      bookKey: "correctness:UP",
      bookSide: "UP",
      action: "sell",
      orderType: "limit",
      timeInForce: "GTC",
      qty: 5,
      limitPrice: 0.58,
      createdAt: now + 1
    })
  );
  const askSamePriceLater = book.execute(
    makeExecutionRequest({
      orderId: "sell-rest-3",
      traceId: "trace-sell-rest-3",
      userId: "u-maker-3",
      bookKey: "correctness:UP",
      bookSide: "UP",
      action: "sell",
      orderType: "limit",
      timeInForce: "GTC",
      qty: 4,
      limitPrice: 0.6,
      createdAt: now + 2
    })
  );

  assert.equal(askFirst.status, "resting");
  assert.equal(askBetter.status, "resting");
  assert.equal(askSamePriceLater.status, "resting");

  const marketBuy = book.execute(
    makeExecutionRequest({
      orderId: "buy-taker-1",
      traceId: "trace-buy-taker-1",
      userId: "u-taker",
      bookKey: "correctness:UP",
      bookSide: "UP",
      action: "buy",
      orderType: "market",
      timeInForce: "IOC",
      qty: 16,
      createdAt: now + 3
    })
  );

  assert.equal(marketBuy.status, "filled");
  assert.equal(marketBuy.filledQty, 16);
  assert.deepEqual(collectMakerOrderIds(marketBuy.fills), ["sell-rest-2", "sell-rest-1", "sell-rest-3"]);
  assert.equal(marketBuy.fills[0]?.qty, 5);
  assert.equal(marketBuy.fills[1]?.qty, 10);
  assert.equal(marketBuy.fills[2]?.qty, 1);
  assert.equal(marketBuy.afterSnapshot.asks[0]?.price, 0.6);
  assert.equal(marketBuy.afterSnapshot.asks[0]?.qty, 3);
  assertBestBidAsk(marketBuy.afterSnapshot);

  const limitBook = new PriceTimeOrderBook("resting:UP", "UP");
  const limitResting = limitBook.execute(
    makeExecutionRequest({
      orderId: "buy-resting-1",
      traceId: "trace-buy-resting-1",
      userId: "u-bidder",
      bookKey: "resting:UP",
      bookSide: "UP",
      action: "buy",
      orderType: "limit",
      timeInForce: "GTC",
      qty: 6,
      limitPrice: 0.45,
      createdAt: now + 10
    })
  );

  assert.equal(limitResting.status, "resting");
  assert.equal(limitResting.beforeSnapshot.bestBid, 0);
  assert.equal(limitResting.afterSnapshot.bestBid, 0.45);
  assert.equal(limitResting.afterSnapshot.bids[0]?.qty, 6);
  assertBestBidAsk(limitResting.afterSnapshot);

  const cancelled = limitBook.cancelOrder("buy-resting-1", now + 11);
  assert.equal(cancelled.cancelled, true);
  assert.equal(cancelled.afterSnapshot.bestBid, 0);

  const externalBook = new PriceTimeOrderBook("sync:UP", "UP");
  const firstSync = externalBook.syncExternalLiquidity(
    makeSyncRequest("sync:UP", "UP", "snapshot-a", now + 20, {
      bids: [
        { price: 0.48, qty: 120 },
        { price: 0.479999999, qty: 0.00000001 }
      ],
      asks: [
        { price: 0.5, qty: 150 },
        { price: 0.500000001, qty: 0.00000001 }
      ]
    })
  );
  const secondSync = externalBook.syncExternalLiquidity(
    makeSyncRequest("sync:UP", "UP", "snapshot-b", now + 21, {
      bids: [{ price: 0.49, qty: 80 }],
      asks: [{ price: 0.51, qty: 90 }]
    })
  );

  assert.equal(firstSync.bids.length, 1);
  assert.equal(firstSync.asks.length, 1);
  assert.equal(secondSync.bids.length, 1);
  assert.equal(secondSync.asks.length, 1);
  assert.equal(secondSync.bids[0]?.price, 0.49);
  assert.equal(secondSync.asks[0]?.price, 0.51);
  assert.equal(secondSync.sourceSnapshotId, "snapshot-b");
  assert.ok(secondSync.sequence > firstSync.sequence);
  assertBestBidAsk(secondSync.snapshot);

  report.correctness.push("price priority and same-price FIFO passed");
  report.correctness.push("resting limit order, cancel, and before/after snapshot consistency passed");
  report.correctness.push("duplicate external sync replaced liquidity without accumulating stale levels");
}

async function testServiceReplay(report: QualityReport) {
  const store = new InMemoryMatchingStore();
  const service = new MatchingService(store as never);
  await service.init();
  const now = Date.now();

  await service.syncExternalBook(
    makeSyncRequest("service:UP", "UP", "service-snapshot", now, {
      bids: [{ price: 0.42, qty: 120 }],
      asks: [{ price: 0.44, qty: 100 }]
    })
  );
  const resting = await service.execute(
    makeExecutionRequest({
      orderId: "service-resting",
      traceId: "service-resting-trace",
      userId: "user-1",
      bookKey: "service:UP",
      bookSide: "UP",
      action: "buy",
      orderType: "limit",
      timeInForce: "GTC",
      qty: 10,
      limitPrice: 0.41,
      createdAt: now + 1
    })
  );
  const cancel = await service.cancel("service:UP", "UP", "service-resting", now + 2);
  const replay = await service.replay("service:UP");
  const current = service.getCurrentBook("service:UP");

  assert.equal(resting.status, "resting");
  assert.equal(cancel.cancelled, true);
  assert.ok(replay.steps.length >= 3);
  assert.deepEqual(
    replay.steps.map((step) => step.event.eventType),
    ["external_book_synced", "order_executed", "order_cancelled"]
  );
  assert.deepEqual(
    replay.steps.map((step) => step.event.sequence),
    replay.steps.map((step) => step.snapshot.sequence)
  );
  assert.equal(current?.snapshot.bestBid, 0.42);
  assert.equal(current?.snapshot.bestAsk, 0.44);
  await service.close();

  report.replayAndPersistence.push("service replay kept event order and sequence-to-snapshot alignment");
}

async function testFileFallbackPersistence(report: QualityReport) {
  const tempRoot = await mkdtemp(path.join(os.tmpdir(), "matching-quality-"));
  const workdir = path.join(tempRoot, "workspace");
  const originalCwd = process.cwd();
  await mkdir(workdir, { recursive: true });

  try {
    process.chdir(workdir);
    const store = new MatchingStore({
      databaseUrl: "postgresql://127.0.0.1:1/does_not_exist",
      redisUrl: "redis://127.0.0.1:1",
      redisSnapshotSeconds: 60
    });

    const book = new PriceTimeOrderBook("persist:UP", "UP");
    const syncedState = book.syncExternalLiquidity(
      makeSyncRequest("persist:UP", "UP", "persist-snapshot", Date.now(), {
        bids: [{ price: 0.61, qty: 110 }],
        asks: [{ price: 0.63, qty: 115 }]
      })
    );
    const persistedState = book.exportState(Date.now());
    const event: MatchingEventRecord = {
      eventId: "evt-persist-1",
      bookKey: "persist:UP",
      roundId: "round-test",
      marketId: "market-test",
      bookSide: "UP",
      sequence: syncedState.sequence,
      eventType: "external_book_synced",
      payload: {
        sourceSnapshotId: "persist-snapshot"
      },
      createdAt: Date.now()
    };

    await store.saveStep(event, persistedState);

    const warmedStore = new MatchingStore({
      databaseUrl: "postgresql://127.0.0.1:1/does_not_exist",
      redisUrl: "redis://127.0.0.1:1",
      redisSnapshotSeconds: 60
    });
    await (warmedStore as unknown as { loadWarmState: () => Promise<void> }).loadWarmState();

    const current = warmedStore.getCurrentBook("persist:UP");
    const replay = await warmedStore.getReplay("persist:UP");

    assert.equal(current?.sequence, persistedState.sequence);
    assert.equal(current?.snapshot.bestBid, 0.61);
    assert.equal(current?.snapshot.bestAsk, 0.63);
    assert.equal(replay.steps.length, 1);
    assert.equal(replay.steps[0]?.event.sequence, replay.steps[0]?.snapshot.sequence);
    assert.equal(replay.latest?.snapshot.snapshotId, persistedState.snapshot.snapshotId);

    report.replayAndPersistence.push("file fallback replay and warm load restored the latest persisted book");
  } finally {
    process.chdir(originalCwd);
    await rm(tempRoot, { recursive: true, force: true });
  }
}

function buildDeepSnapshot(snapshotId: string, ts: number): OrderBookSnapshot {
  const bids = Array.from({ length: 40 }, (_, index) => ({
    price: Number((0.6 - index * 0.0025).toFixed(8)),
    qty: 500 + index * 15
  }));
  const asks = Array.from({ length: 40 }, (_, index) => ({
    price: Number((0.61 + index * 0.0025).toFixed(8)),
    qty: 480 + index * 14
  }));
  return {
    snapshotId,
    snapshotTs: ts,
    bestBid: bids[0]?.price ?? 0,
    bestAsk: asks[0]?.price ?? 0,
    midPrice: Number((((bids[0]?.price ?? 0) + (asks[0]?.price ?? 0)) / 2).toFixed(8)),
    bids,
    asks
  };
}

function runTimed<T>(fn: () => T) {
  const startedAt = performance.now();
  const result = fn();
  return {
    result,
    elapsedMs: performance.now() - startedAt
  };
}

async function testPerformance(report: QualityReport) {
  const singleBook = new PriceTimeOrderBook("perf-single:UP", "UP");
  singleBook.syncExternalLiquidity({
    bookKey: "perf-single:UP",
    roundId: "round-test",
    marketId: "market-test",
    bookSide: "UP",
    source: "Polymarket",
    sourceSnapshot: buildDeepSnapshot("perf-single-seed", Date.now()),
    syncedAt: Date.now()
  });

  const singleBookDurations: number[] = [];
  for (let index = 0; index < 1000; index += 1) {
    const now = Date.now() + index;
    if (index > 0 && index % 50 === 0) {
      singleBook.syncExternalLiquidity({
        bookKey: "perf-single:UP",
        roundId: "round-test",
        marketId: "market-test",
        bookSide: "UP",
        source: "Polymarket",
        sourceSnapshot: buildDeepSnapshot(`perf-single-refresh-${index}`, now),
        syncedAt: now
      });
    }

    const timed = runTimed(() =>
      singleBook.execute(
        makeExecutionRequest({
          orderId: `perf-single-${index}`,
          traceId: `perf-single-trace-${index}`,
          userId: "user-perf",
          bookKey: "perf-single:UP",
          bookSide: "UP",
          action: index % 2 === 0 ? "buy" : "sell",
          orderType: "market",
          timeInForce: "IOC",
          qty: 25,
          createdAt: now
        })
      )
    );
    singleBookDurations.push(timed.elapsedMs);
    assert.notEqual(timed.result.status, "failed");
    assertBestBidAsk(timed.result.afterSnapshot);
  }

  const mixedBook = new PriceTimeOrderBook("perf-mixed:UP", "UP");
  const mixedDurations: number[] = [];
  for (let index = 0; index < 1000; index += 1) {
    const now = Date.now() + index;
    if (index % 3 === 0) {
      const timed = runTimed(() =>
        mixedBook.syncExternalLiquidity({
          bookKey: "perf-mixed:UP",
          roundId: "round-test",
          marketId: "market-test",
          bookSide: "UP",
          source: "Polymarket",
          sourceSnapshot: buildDeepSnapshot(`perf-mixed-sync-${index}`, now),
          syncedAt: now
        })
      );
      mixedDurations.push(timed.elapsedMs);
    } else {
      const timed = runTimed(() =>
        mixedBook.execute(
          makeExecutionRequest({
            orderId: `perf-mixed-order-${index}`,
            traceId: `perf-mixed-trace-${index}`,
            userId: "user-perf",
            bookKey: "perf-mixed:UP",
            bookSide: "UP",
            action: index % 2 === 0 ? "buy" : "sell",
            orderType: "market",
            timeInForce: "IOC",
            qty: 12,
            createdAt: now
          })
        )
      );
      mixedDurations.push(timed.elapsedMs);
      assertBestBidAsk(timed.result.afterSnapshot);
    }
  }

  const multiBookDurations: number[] = [];
  const multiBooks = ["UP", "DOWN", "UP", "DOWN"].map((bookSide, index) => ({
    key: `perf-multi-${index}:${bookSide}`,
    side: bookSide as TradeSide,
    book: new PriceTimeOrderBook(`perf-multi-${index}:${bookSide}`, bookSide as TradeSide)
  }));

  for (const entry of multiBooks) {
    entry.book.syncExternalLiquidity({
      bookKey: entry.key,
      roundId: "round-test",
      marketId: "market-test",
      bookSide: entry.side,
      source: "Polymarket",
      sourceSnapshot: buildDeepSnapshot(`perf-multi-seed-${entry.key}`, Date.now()),
      syncedAt: Date.now()
    });
  }

  for (let index = 0; index < 4000; index += 1) {
    const entry = multiBooks[index % multiBooks.length];
    const now = Date.now() + index;
    if (index > 0 && index % 100 === 0) {
      entry.book.syncExternalLiquidity({
        bookKey: entry.key,
        roundId: "round-test",
        marketId: "market-test",
        bookSide: entry.side,
        source: "Polymarket",
        sourceSnapshot: buildDeepSnapshot(`perf-multi-refresh-${index}`, now),
        syncedAt: now
      });
    }

    const timed = runTimed(() =>
      entry.book.execute(
        makeExecutionRequest({
          orderId: `perf-multi-order-${index}`,
          traceId: `perf-multi-trace-${index}`,
          userId: "user-perf",
          bookKey: entry.key,
          bookSide: entry.side,
          action: index % 2 === 0 ? "buy" : "sell",
          orderType: "market",
          timeInForce: "IOC",
          qty: 8,
          createdAt: now
        })
      )
    );
    multiBookDurations.push(timed.elapsedMs);
    assert.notEqual(timed.result.status, "failed");
    assertBestBidAsk(timed.result.afterSnapshot);
  }

  report.performance.push(formatMetrics("single-book execute", summarizeMetrics(singleBookDurations)));
  report.performance.push(formatMetrics("mixed sync+execute", summarizeMetrics(mixedDurations)));
  report.performance.push(formatMetrics("multi-book execute", summarizeMetrics(multiBookDurations)));
}

async function checkIntegrationHealth(report: QualityReport) {
  const client = new MatchingServiceClient({
    baseUrl: serverConfig.matchingServiceUrl,
    timeoutMs: 1500
  });

  try {
    const health = await client.health();
    report.integration = `integration health: ok=${health.ok}, postgres=${health.persistence.postgres}, redis=${health.persistence.redis}`;
  } catch (error) {
    const message = error instanceof Error ? error.message : "unavailable";
    report.integration = `integration health: skipped (${message})`;
    report.knownGaps.push("live matching-service health smoke was unavailable in this run");
  }
}

async function main() {
  const report: QualityReport = {
    correctness: [],
    replayAndPersistence: [],
    performance: [],
    knownGaps: [
      "does not attempt a destructive failover test against real PostgreSQL/Redis instances",
      "does not benchmark cross-process HTTP overhead for every execute call"
    ]
  };

  await testCorrectness(report);
  await testServiceReplay(report);
  await testFileFallbackPersistence(report);
  await testPerformance(report);
  await checkIntegrationHealth(report);

  console.log("matching-quality-check");
  console.log("");
  console.log("Correctness: pass");
  for (const line of report.correctness) {
    console.log(`- ${line}`);
  }
  console.log("");
  console.log("Replay/Persistence: pass");
  for (const line of report.replayAndPersistence) {
    console.log(`- ${line}`);
  }
  console.log("");
  console.log("Performance: baseline");
  for (const line of report.performance) {
    console.log(`- ${line}`);
  }
  if (report.integration) {
    console.log(`- ${report.integration}`);
  }
  console.log("");
  console.log("Known gaps:");
  for (const line of report.knownGaps) {
    console.log(`- ${line}`);
  }
}

void main();

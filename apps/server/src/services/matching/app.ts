import Fastify from "fastify";
import { z } from "zod";
import type { MatchingExecutionRequest, MatchingSyncRequest, TradeSide } from "../../domain/types";
import { MatchingStore } from "./store";
import { MatchingService } from "./service";

const syncSchema = z.object({
  bookKey: z.string().min(1),
  roundId: z.string().optional(),
  marketId: z.string().optional(),
  bookSide: z.enum(["UP", "DOWN"]),
  source: z.literal("Polymarket"),
  sourceSnapshot: z.object({
    snapshotId: z.string(),
    snapshotTs: z.number(),
    bestBid: z.number(),
    bestAsk: z.number(),
    midPrice: z.number(),
    bids: z.array(
      z.object({
        price: z.number(),
        qty: z.number()
      })
    ),
    asks: z.array(
      z.object({
        price: z.number(),
        qty: z.number()
      })
    )
  }),
  syncedAt: z.number()
});

const executeSchema = z.object({
  orderId: z.string().min(1),
  traceId: z.string().min(1),
  userId: z.string().min(1),
  roundId: z.string().optional(),
  marketId: z.string().optional(),
  bookKey: z.string().min(1),
  bookSide: z.enum(["UP", "DOWN"]),
  action: z.enum(["buy", "sell"]),
  orderType: z.enum(["market", "limit"]),
  timeInForce: z.enum(["IOC", "GTC"]),
  notional: z.number().optional(),
  qty: z.number().optional(),
  limitPrice: z.number().optional(),
  createdAt: z.number(),
  meta: z.record(z.string(), z.unknown()).optional()
});

const cancelSchema = z.object({
  bookSide: z.enum(["UP", "DOWN"]),
  cancelledAt: z.number()
});

export async function createMatchingServiceApp(config: {
  databaseUrl: string;
  redisUrl: string;
  redisSnapshotSeconds: number;
}) {
  const app = Fastify({ logger: false });
  const store = new MatchingStore(config);
  const service = new MatchingService(store);
  await service.init();

  async function safeRoute<T>(handler: () => Promise<T>) {
    try {
      return await handler();
    } catch (error) {
      return {
        error: true,
        message: error instanceof Error ? error.message : "Unexpected matching service error."
      } as T;
    }
  }

  app.get("/health", async () => ({
    ok: true,
    serverNow: Date.now(),
    persistence: service.getPersistenceStatus()
  }));

  app.post("/books/sync", async (request) =>
    safeRoute(async () => {
      const payload = syncSchema.parse(request.body) as MatchingSyncRequest;
      return {
        book: await service.syncExternalBook(payload)
      };
    })
  );

  app.post("/orders/execute", async (request) =>
    safeRoute(async () => {
      const payload = executeSchema.parse(request.body) as MatchingExecutionRequest;
      return await service.execute(payload);
    })
  );

  app.post("/orders/:id/cancel", async (request) =>
    safeRoute(async () => {
      const params = request.params as { id: string };
      const payload = cancelSchema.parse(request.body) as { bookSide: TradeSide; cancelledAt: number };
      const query = request.query as { bookKey?: string };
      if (!query.bookKey) {
        throw new Error("bookKey is required.");
      }
      return await service.cancel(query.bookKey, payload.bookSide, params.id, payload.cancelledAt);
    })
  );

  app.get("/books/:bookKey/current", async (request) =>
    safeRoute(async () => {
      const params = request.params as { bookKey: string };
      return {
        book: service.getCurrentBook(params.bookKey)
      };
    })
  );

  app.get("/books/:bookKey/replay", async (request) =>
    safeRoute(async () => {
      const params = request.params as { bookKey: string };
      const query = request.query as {
        fromSequence?: string;
        toSequence?: string;
        limit?: string;
      };
      return await service.replay(params.bookKey, {
        fromSequence: query.fromSequence ? Number(query.fromSequence) : undefined,
        toSequence: query.toSequence ? Number(query.toSequence) : undefined,
        limit: query.limit ? Number(query.limit) : undefined
      });
    })
  );

  return {
    app,
    service,
    close: async () => {
      await service.close();
      await app.close().catch(() => undefined);
    }
  };
}

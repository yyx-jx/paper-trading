import type {
  MatchingBookState,
  MatchingCancelResult,
  MatchingExecutionRequest,
  MatchingExecutionResult,
  MatchingEventRecord,
  MatchingReplayResult,
  MatchingSyncRequest
} from "../../domain/types";
import { PriceTimeOrderBook } from "./order-book";
import { MatchingStore } from "./store";

export class MatchingService {
  private readonly books = new Map<string, PriceTimeOrderBook>();

  constructor(private readonly store: MatchingStore) {}

  async init() {
    await this.store.init();
  }

  async close() {
    await this.store.close();
  }

  getPersistenceStatus() {
    return this.store.getPersistenceStatus();
  }

  getCurrentBook(bookKey: string) {
    const book = this.books.get(bookKey);
    if (book) {
      return book.exportState();
    }
    return this.store.getCurrentBook(bookKey);
  }

  async syncExternalBook(request: MatchingSyncRequest): Promise<MatchingBookState> {
    const book = this.getOrCreateBook(request.bookKey, request.bookSide);
    const state = book.syncExternalLiquidity(request);
    await this.store.saveStep(
      this.newEvent({
        bookKey: request.bookKey,
        roundId: request.roundId,
        marketId: request.marketId,
        bookSide: request.bookSide,
        sequence: state.sequence,
        eventType: "external_book_synced",
        payload: {
          source: request.source,
          sourceSnapshotId: request.sourceSnapshot.snapshotId,
          snapshotTs: request.sourceSnapshot.snapshotTs,
          bids: request.sourceSnapshot.bids,
          asks: request.sourceSnapshot.asks
        },
        createdAt: request.syncedAt
      }),
      state
    );
    return state;
  }

  async execute(request: MatchingExecutionRequest): Promise<MatchingExecutionResult> {
    const book = this.getOrCreateBook(request.bookKey, request.bookSide);
    const result = book.execute(request);
    await this.store.saveStep(
      this.newEvent({
        bookKey: request.bookKey,
        roundId: request.roundId,
        marketId: request.marketId,
        bookSide: request.bookSide,
        sequence: result.sequence,
        eventType: "order_executed",
        orderId: request.orderId,
        traceId: request.traceId,
        payload: {
          request,
          status: result.status,
          fills: result.fills,
          filledQty: result.filledQty,
          matchedNotional: result.matchedNotional,
          remainingQty: result.remainingQty,
          remainingNotional: result.remainingNotional,
          avgPrice: result.avgPrice,
          failureReason: result.failureReason
        },
        createdAt: result.matchedAt
      }),
      book.exportState(result.matchedAt)
    );
    return result;
  }

  async cancel(bookKey: string, bookSide: MatchingExecutionRequest["bookSide"], orderId: string, cancelledAt: number) {
    const book = this.getOrCreateBook(bookKey, bookSide);
    const result = book.cancelOrder(orderId, cancelledAt);
    if (result.cancelled) {
      await this.store.saveStep(
        this.newEvent({
          bookKey,
          bookSide,
          sequence: result.sequence,
          eventType: "order_cancelled",
          orderId,
          payload: {
            reason: result.reason
          },
          createdAt: result.cancelledAt
        }),
        book.exportState(result.cancelledAt)
      );
    }
    return result;
  }

  async replay(
    bookKey: string,
    options?: { fromSequence?: number; toSequence?: number; limit?: number }
  ): Promise<MatchingReplayResult> {
    return this.store.getReplay(bookKey, options);
  }

  private getOrCreateBook(bookKey: string, bookSide: MatchingExecutionRequest["bookSide"]) {
    let book = this.books.get(bookKey);
    if (book) {
      return book;
    }

    const persisted = this.store.getCurrentBook(bookKey);
    book = new PriceTimeOrderBook(bookKey, bookSide, persisted);
    this.books.set(bookKey, book);
    return book;
  }

  private newEvent(input: Omit<MatchingEventRecord, "eventId">): MatchingEventRecord {
    return {
      eventId: this.store.newId("mevt"),
      ...input
    };
  }
}

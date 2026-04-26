import type {
  MatchingBookOrder,
  MatchingBookState,
  MatchingCancelResult,
  MatchingExecutionRequest,
  MatchingExecutionResult,
  MatchingFill,
  MatchingSyncRequest,
  OrderBookSnapshot,
  TradeSide
} from "../../domain/types";

const PRICE_EPSILON = 0.0000001;
const QTY_EPSILON = 0.000001;

const roundNumber = (value: number, digits = 8) => Number(value.toFixed(digits));

function cloneOrder(order: MatchingBookOrder): MatchingBookOrder {
  return {
    ...order,
    meta: order.meta ? { ...order.meta } : undefined
  };
}

function cloneSnapshot(snapshot: OrderBookSnapshot): OrderBookSnapshot {
  return {
    ...snapshot,
    bids: snapshot.bids.map((level) => ({ ...level })),
    asks: snapshot.asks.map((level) => ({ ...level }))
  };
}

function compareBid(left: MatchingBookOrder, right: MatchingBookOrder) {
  const priceDelta = (right.price ?? 0) - (left.price ?? 0);
  if (Math.abs(priceDelta) > PRICE_EPSILON) {
    return priceDelta > 0 ? 1 : -1;
  }
  return left.prioritySequence - right.prioritySequence;
}

function compareAsk(left: MatchingBookOrder, right: MatchingBookOrder) {
  const priceDelta = (left.price ?? 0) - (right.price ?? 0);
  if (Math.abs(priceDelta) > PRICE_EPSILON) {
    return priceDelta > 0 ? 1 : -1;
  }
  return left.prioritySequence - right.prioritySequence;
}

function aggregateLevels(orders: MatchingBookOrder[], direction: "bid" | "ask") {
  const levels = new Map<string, { price: number; qty: number }>();
  for (const order of orders) {
    if (order.remainingQty <= QTY_EPSILON || typeof order.price !== "number") {
      continue;
    }
    const key = order.price.toFixed(8);
    const existing = levels.get(key);
    if (existing) {
      existing.qty = roundNumber(existing.qty + order.remainingQty);
    } else {
      levels.set(key, {
        price: roundNumber(order.price),
        qty: roundNumber(order.remainingQty)
      });
    }
  }

  const result = [...levels.values()].sort((left, right) =>
    direction === "bid" ? right.price - left.price : left.price - right.price
  );
  return result;
}

export class PriceTimeOrderBook {
  private roundId?: string;
  private marketId?: string;
  private bookSide: TradeSide;
  private sequence = 0;
  private prioritySequence = 0;
  private sourceSnapshotId?: string;
  private updatedAt = Date.now();
  private bids: MatchingBookOrder[] = [];
  private asks: MatchingBookOrder[] = [];

  constructor(
    private readonly bookKey: string,
    bookSide: TradeSide,
    initialState?: MatchingBookState
  ) {
    this.bookSide = bookSide;
    if (initialState) {
      this.hydrate(initialState);
    }
  }

  hydrate(state: MatchingBookState) {
    this.roundId = state.roundId;
    this.marketId = state.marketId;
    this.bookSide = state.bookSide;
    this.sequence = state.sequence;
    this.prioritySequence = state.prioritySequence;
    this.sourceSnapshotId = state.sourceSnapshotId;
    this.updatedAt = state.updatedAt;
    this.bids = state.bids.map(cloneOrder).sort(compareBid);
    this.asks = state.asks.map(cloneOrder).sort(compareAsk);
  }

  syncExternalLiquidity(request: MatchingSyncRequest): MatchingBookState {
    this.sequence += 1;
    this.roundId = request.roundId ?? this.roundId;
    this.marketId = request.marketId ?? this.marketId;
    this.bookSide = request.bookSide;
    this.sourceSnapshotId = request.sourceSnapshot.snapshotId;
    this.updatedAt = request.syncedAt;
    this.bids = this.bids.filter((order) => order.ownerType !== "external");
    this.asks = this.asks.filter((order) => order.ownerType !== "external");

    request.sourceSnapshot.bids.forEach((level, index) => {
      if (level.qty <= QTY_EPSILON) {
        return;
      }
      this.bids.push({
        id: `${request.sourceSnapshot.snapshotId}:bid:${index}`,
        ownerId: "external:polymarket",
        ownerType: "external",
        bookKey: request.bookKey,
        roundId: request.roundId,
        marketId: request.marketId,
        bookSide: request.bookSide,
        direction: "bid",
        orderType: "limit",
        timeInForce: "GTC",
        price: roundNumber(level.price),
        originalQty: roundNumber(level.qty),
        remainingQty: roundNumber(level.qty),
        createdAt: request.syncedAt,
        prioritySequence: ++this.prioritySequence,
        meta: {
          source: request.source,
          sourceSnapshotId: request.sourceSnapshot.snapshotId
        }
      });
    });

    request.sourceSnapshot.asks.forEach((level, index) => {
      if (level.qty <= QTY_EPSILON) {
        return;
      }
      this.asks.push({
        id: `${request.sourceSnapshot.snapshotId}:ask:${index}`,
        ownerId: "external:polymarket",
        ownerType: "external",
        bookKey: request.bookKey,
        roundId: request.roundId,
        marketId: request.marketId,
        bookSide: request.bookSide,
        direction: "ask",
        orderType: "limit",
        timeInForce: "GTC",
        price: roundNumber(level.price),
        originalQty: roundNumber(level.qty),
        remainingQty: roundNumber(level.qty),
        createdAt: request.syncedAt,
        prioritySequence: ++this.prioritySequence,
        meta: {
          source: request.source,
          sourceSnapshotId: request.sourceSnapshot.snapshotId
        }
      });
    });

    this.sortBook();
    return this.exportState(request.syncedAt);
  }

  execute(request: MatchingExecutionRequest): MatchingExecutionResult {
    if (request.action === "buy" && (request.notional ?? 0) <= 0 && (request.qty ?? 0) <= 0) {
      throw new Error("Buy orders require notional or quantity.");
    }
    if (request.action === "sell" && (request.qty ?? 0) <= 0) {
      throw new Error("Sell orders require quantity.");
    }
    if (request.orderType === "limit" && typeof request.limitPrice !== "number") {
      throw new Error("Limit orders require limitPrice.");
    }

    const beforeSnapshot = this.exportSnapshot(request.createdAt);
    this.sequence += 1;
    this.roundId = request.roundId ?? this.roundId;
    this.marketId = request.marketId ?? this.marketId;
    this.bookSide = request.bookSide;
    this.updatedAt = request.createdAt;

    const fills: MatchingFill[] = [];
    let matchedNotional = 0;
    let filledQty = 0;
    let remainingQty = roundNumber(request.qty ?? 0);
    let remainingNotional = typeof request.notional === "number" ? roundNumber(request.notional) : undefined;
    const opposingOrders = request.action === "buy" ? this.asks : this.bids;

    for (const maker of opposingOrders) {
      if (maker.remainingQty <= QTY_EPSILON || typeof maker.price !== "number") {
        continue;
      }
      if (!this.canMatch(request, maker.price)) {
        break;
      }

      const byQty = request.qty ? remainingQty : Number.POSITIVE_INFINITY;
      const byNotional =
        typeof remainingNotional === "number" ? remainingNotional / Math.max(maker.price, PRICE_EPSILON) : Number.POSITIVE_INFINITY;
      const fillQty = roundNumber(Math.min(maker.remainingQty, byQty, byNotional));
      if (fillQty <= QTY_EPSILON) {
        continue;
      }

      const fillNotional = roundNumber(fillQty * maker.price, 8);
      maker.remainingQty = roundNumber(Math.max(maker.remainingQty - fillQty, 0));
      filledQty = roundNumber(filledQty + fillQty);
      matchedNotional = roundNumber(matchedNotional + fillNotional, 8);
      if (request.qty) {
        remainingQty = roundNumber(Math.max(remainingQty - fillQty, 0));
      }
      if (typeof remainingNotional === "number") {
        remainingNotional = roundNumber(Math.max(remainingNotional - fillNotional, 0), 8);
      }

      fills.push({
        fillId: `${request.orderId}:fill:${fills.length + 1}`,
        makerOrderId: maker.id,
        takerOrderId: request.orderId,
        price: roundNumber(maker.price),
        qty: fillQty,
        notional: fillNotional,
        makerOwnerId: maker.ownerId,
        makerOwnerType: maker.ownerType,
        executedAt: request.createdAt
      });

      if (request.qty && remainingQty <= QTY_EPSILON) {
        break;
      }
      if (typeof remainingNotional === "number" && remainingNotional <= QTY_EPSILON) {
        break;
      }
    }

    this.pruneEmptyOrders();

    let restingOrder: MatchingBookOrder | undefined;
    if (
      request.orderType === "limit" &&
      request.timeInForce === "GTC" &&
      request.qty &&
      remainingQty > QTY_EPSILON
    ) {
      restingOrder = {
        id: request.orderId,
        ownerId: request.userId,
        ownerType: "user",
        bookKey: request.bookKey,
        roundId: request.roundId,
        marketId: request.marketId,
        bookSide: request.bookSide,
        direction: request.action === "buy" ? "bid" : "ask",
        orderType: request.orderType,
        timeInForce: request.timeInForce,
        price: request.limitPrice,
        originalQty: roundNumber(request.qty),
        remainingQty: roundNumber(remainingQty),
        createdAt: request.createdAt,
        prioritySequence: ++this.prioritySequence,
        meta: request.meta ? { ...request.meta } : undefined
      };
      if (restingOrder.direction === "bid") {
        this.bids.push(restingOrder);
      } else {
        this.asks.push(restingOrder);
      }
      this.sortBook();
    }

    const afterSnapshot = this.exportSnapshot(request.createdAt);
    return {
      request,
      status: this.resolveStatus({
        fills,
        restingOrder,
        remainingQty,
        remainingNotional
      }),
      fills,
      filledQty: roundNumber(filledQty),
      remainingQty: roundNumber(remainingQty),
      matchedNotional: roundNumber(matchedNotional, 8),
      remainingNotional: typeof remainingNotional === "number" ? roundNumber(remainingNotional, 8) : undefined,
      avgPrice: filledQty > QTY_EPSILON ? roundNumber(matchedNotional / filledQty) : undefined,
      restingOrder: restingOrder ? cloneOrder(restingOrder) : undefined,
      beforeSnapshot,
      afterSnapshot,
      sequence: this.sequence,
      matchedAt: request.createdAt,
      failureReason: fills.length === 0 && !restingOrder ? this.resolveFailureReason(request) : undefined
    };
  }

  cancelOrder(orderId: string, cancelledAt: number): MatchingCancelResult {
    const beforeSnapshot = this.exportSnapshot(cancelledAt);
    const bidIndex = this.bids.findIndex((order) => order.id === orderId && order.ownerType === "user");
    const askIndex = this.asks.findIndex((order) => order.id === orderId && order.ownerType === "user");

    let cancelledOrder: MatchingBookOrder | undefined;
    if (bidIndex >= 0) {
      this.sequence += 1;
      cancelledOrder = this.bids.splice(bidIndex, 1)[0];
    } else if (askIndex >= 0) {
      this.sequence += 1;
      cancelledOrder = this.asks.splice(askIndex, 1)[0];
    }

    this.updatedAt = cancelledAt;
    const afterSnapshot = this.exportSnapshot(cancelledAt);
    return {
      bookKey: this.bookKey,
      orderId,
      cancelled: Boolean(cancelledOrder),
      reason: cancelledOrder ? undefined : "Order not found in resting queue.",
      cancelledOrder: cancelledOrder ? cloneOrder(cancelledOrder) : undefined,
      beforeSnapshot,
      afterSnapshot,
      sequence: this.sequence,
      cancelledAt
    };
  }

  exportState(snapshotTs = this.updatedAt): MatchingBookState {
    return {
      bookKey: this.bookKey,
      roundId: this.roundId,
      marketId: this.marketId,
      bookSide: this.bookSide,
      sequence: this.sequence,
      prioritySequence: this.prioritySequence,
      snapshot: this.exportSnapshot(snapshotTs),
      bids: this.bids.map(cloneOrder),
      asks: this.asks.map(cloneOrder),
      sourceSnapshotId: this.sourceSnapshotId,
      updatedAt: snapshotTs
    };
  }

  private exportSnapshot(snapshotTs: number): OrderBookSnapshot {
    const bids = aggregateLevels(this.bids, "bid");
    const asks = aggregateLevels(this.asks, "ask");
    const bestBid = bids[0]?.price ?? 0;
    const bestAsk = asks[0]?.price ?? 0;
    return cloneSnapshot({
      snapshotId: `${this.bookKey}:${this.sequence}`,
      snapshotTs,
      bestBid: roundNumber(bestBid),
      bestAsk: roundNumber(bestAsk),
      midPrice:
        bestBid > 0 && bestAsk > 0 ? roundNumber((bestBid + bestAsk) / 2) : roundNumber(bestBid || bestAsk || 0),
      bids,
      asks
    });
  }

  private canMatch(request: MatchingExecutionRequest, makerPrice: number) {
    if (request.orderType === "market") {
      return true;
    }
    if (typeof request.limitPrice !== "number") {
      return false;
    }
    return request.action === "buy" ? makerPrice <= request.limitPrice : makerPrice >= request.limitPrice;
  }

  private resolveStatus(input: {
    fills: MatchingFill[];
    restingOrder?: MatchingBookOrder;
    remainingQty: number;
    remainingNotional?: number;
  }) {
    if (input.restingOrder) {
      return "resting" as const;
    }
    if (input.fills.length === 0) {
      return "failed" as const;
    }
    const qtyDone = input.remainingQty <= QTY_EPSILON;
    const notionalDone =
      typeof input.remainingNotional !== "number" || input.remainingNotional <= QTY_EPSILON;
    return qtyDone && notionalDone ? "filled" : "partial";
  }

  private resolveFailureReason(request: MatchingExecutionRequest) {
    if (request.orderType === "limit") {
      return "No opposing liquidity crossed the limit price.";
    }
    return request.action === "buy"
      ? "Ask queue had insufficient liquidity for the incoming buy order."
      : "Bid queue had insufficient liquidity for the incoming sell order.";
  }

  private pruneEmptyOrders() {
    this.bids = this.bids.filter((order) => order.remainingQty > QTY_EPSILON).sort(compareBid);
    this.asks = this.asks.filter((order) => order.remainingQty > QTY_EPSILON).sort(compareAsk);
  }

  private sortBook() {
    this.bids.sort(compareBid);
    this.asks.sort(compareAsk);
  }
}

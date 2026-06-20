import type { CandlePoint, OrderBookSnapshot } from "../../domain/types";

export type ExecutionBookResult = {
  book: OrderBookSnapshot;
  source: "cache" | "stale_cache" | "rest" | "rest_empty_cache_fallback";
  ageMs: number;
  fallbackReason?: string;
};

export function cloneOrderBookSnapshot(snapshot: OrderBookSnapshot): OrderBookSnapshot {
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

export function hasOrderBookDepth(snapshot?: OrderBookSnapshot) {
  return Boolean(snapshot && (snapshot.bids.length > 0 || snapshot.asks.length > 0));
}

export function cloneCandlePoint(point: CandlePoint): CandlePoint {
  return {
    ts: point.ts,
    price: point.price
  };
}

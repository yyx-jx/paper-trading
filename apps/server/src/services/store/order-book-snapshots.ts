import { createHash } from "node:crypto";
import type { OrderBookSnapshot } from "../../domain/types";

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

export function orderBookSnapshotRef(snapshot: OrderBookSnapshot) {
  const normalized = cloneOrderBookSnapshot(snapshot);
  return `obs_${createHash("sha256").update(JSON.stringify(normalized)).digest("hex").slice(0, 32)}`;
}

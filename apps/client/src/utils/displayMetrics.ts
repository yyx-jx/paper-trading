import type { OrderBookSnapshot } from "./api";

export const ORDER_BOOK_STALE_WARNING_MS = 2_000;

export function orderBookAgeMs(book: Pick<OrderBookSnapshot, "snapshotTs"> | undefined, nowMs: number) {
  if (!book || typeof book.snapshotTs !== "number") {
    return undefined;
  }
  return Math.max(nowMs - book.snapshotTs, 0);
}

export function isOrderBookStale(
  book: Pick<OrderBookSnapshot, "snapshotTs"> | undefined,
  nowMs: number,
  thresholdMs = ORDER_BOOK_STALE_WARNING_MS
) {
  const ageMs = orderBookAgeMs(book, nowMs);
  return typeof ageMs === "number" && ageMs > thresholdMs;
}

export function sourceFreshnessLabelKey(sourceName?: string) {
  return sourceName?.toLowerCase() === "chainlink" ? "chainlinkFeedAge" : "endToEnd";
}

export function sourceFreshnessAlertKey(sourceName?: string) {
  return sourceName?.toLowerCase() === "chainlink" ? "chainlinkFeedStale" : "latencyOver3s";
}

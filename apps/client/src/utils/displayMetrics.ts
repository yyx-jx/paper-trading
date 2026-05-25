import type { SourceComponentHealth } from "./api";

export const ORDER_BOOK_STALE_WARNING_MS = 2_000;

export function orderBookBackendLatencyMs(bookComponent: Pick<SourceComponentHealth, "sourceEventTs" | "serverRecvTs"> | undefined) {
  if (
    !bookComponent ||
    typeof bookComponent.sourceEventTs !== "number" ||
    typeof bookComponent.serverRecvTs !== "number" ||
    bookComponent.sourceEventTs <= 0 ||
    bookComponent.serverRecvTs <= 0
  ) {
    return undefined;
  }
  return Math.max(bookComponent.serverRecvTs - bookComponent.sourceEventTs, 0);
}

export function isOrderBookBackendStale(
  bookComponent: Pick<SourceComponentHealth, "sourceEventTs" | "serverRecvTs"> | undefined,
  thresholdMs = ORDER_BOOK_STALE_WARNING_MS
) {
  const latencyMs = orderBookBackendLatencyMs(bookComponent);
  return typeof latencyMs === "number" && latencyMs > thresholdMs;
}

export function sourceFreshnessLabelKey(sourceName?: string) {
  return sourceName?.toLowerCase() === "coinbase" ? "coinbaseFeedAge" : "endToEnd";
}

export function sourceFreshnessAlertKey(sourceName?: string) {
  return sourceName?.toLowerCase() === "coinbase" ? "coinbaseFeedStale" : "latencyOver3s";
}

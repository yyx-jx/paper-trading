import type { MarketCandleRecord } from "../../domain/types";

const MARKET_CANDLE_PRIORITY: Record<MarketCandleRecord["origin"], number> = {
  history_1m_split: 1,
  rtds_30s: 2
};

export function marketCandleKey(
  source: MarketCandleRecord["source"],
  symbol: string,
  interval: MarketCandleRecord["interval"]
) {
  return `${source}:${symbol}:${interval}`;
}

export function marketCandleDedupeKey(candle: MarketCandleRecord) {
  return `${marketCandleKey(candle.source, candle.symbol, candle.interval)}:${candle.openTs}`;
}

export function shouldReplaceMarketCandle(existing: MarketCandleRecord | undefined, incoming: MarketCandleRecord) {
  if (!existing) {
    return true;
  }
  const incomingPriority = MARKET_CANDLE_PRIORITY[incoming.origin];
  const existingPriority = MARKET_CANDLE_PRIORITY[existing.origin];
  return incomingPriority > existingPriority || (incomingPriority === existingPriority && incoming.updatedAt >= existing.updatedAt);
}

export function dedupeMarketCandles(candles: MarketCandleRecord[]) {
  const byKey = new Map<string, MarketCandleRecord>();
  for (const candle of candles) {
    const key = marketCandleDedupeKey(candle);
    const existing = byKey.get(key);
    if (shouldReplaceMarketCandle(existing, candle)) {
      byKey.set(key, candle);
    }
  }
  return [...byKey.values()];
}

export function isValidMarketCandle(candle: MarketCandleRecord) {
  return (
    candle.source === "coinbase" &&
    candle.interval === "30s" &&
    Number.isFinite(candle.openTs) &&
    Number.isFinite(candle.closeTs) &&
    candle.openTs % 30_000 === 0 &&
    candle.closeTs === candle.openTs + 30_000 &&
    [candle.open, candle.high, candle.low, candle.close].every((value) => Number.isFinite(value) && value > 0)
  );
}

export function mergeMarketCandlesIntoMemory(
  marketCandles: Map<string, MarketCandleRecord[]>,
  candles: MarketCandleRecord[],
  retentionMs: number,
  now = Date.now()
) {
  const threshold = now - retentionMs;
  const grouped = new Map<string, MarketCandleRecord[]>();
  for (const candle of candles) {
    const key = marketCandleKey(candle.source, candle.symbol, candle.interval);
    const group = grouped.get(key);
    if (group) {
      group.push(candle);
    } else {
      grouped.set(key, [candle]);
    }
  }
  for (const [key, incoming] of grouped) {
    const existingRows = marketCandles.get(key) ?? [];
    const byOpenTs = new Map(existingRows.map((row) => [row.openTs, row]));
    for (const candle of incoming) {
      const existing = byOpenTs.get(candle.openTs);
      if (shouldReplaceMarketCandle(existing, candle)) {
        byOpenTs.set(candle.openTs, { ...candle });
      }
    }
    marketCandles.set(
      key,
      [...byOpenTs.values()]
        .filter((row) => row.openTs >= threshold)
        .sort((left, right) => left.openTs - right.openTs)
    );
  }
}

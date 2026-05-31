import type { CandleBar, MarketCandleRecord } from "../../domain/types";
import { isPositivePrice } from "./pricing";
import { roundNumber } from "./trade-calculations";

export const TRADE_CHART_INTERVALS = ["30s", "1m", "5m", "15m", "1h"] as const;

export const COINBASE_BAR_LIMITS: Record<(typeof TRADE_CHART_INTERVALS)[number], number> = {
  "30s": 120,
  "1m": 60,
  "5m": 30,
  "15m": 24,
  "1h": 24
};

export const COINBASE_INTERVAL_MS: Record<(typeof TRADE_CHART_INTERVALS)[number], number> = {
  "30s": 30_000,
  "1m": 60_000,
  "5m": 5 * 60_000,
  "15m": 15 * 60_000,
  "1h": 60 * 60_000
};

const COINBASE_MARKET_CANDLE_PRIORITY: Record<MarketCandleRecord["origin"], number> = {
  history_1m_split: 1,
  rtds_30s: 2
};

export function createEmptyCoinbaseIntervalBars() {
  return {
    "30s": [] as CandleBar[],
    "1m": [] as CandleBar[],
    "5m": [] as CandleBar[],
    "15m": [] as CandleBar[],
    "1h": [] as CandleBar[]
  };
}

export function normalizeCoinbaseBar(interval: (typeof TRADE_CHART_INTERVALS)[number], bar: CandleBar): CandleBar {
  const bucketSize = COINBASE_INTERVAL_MS[interval];
  const startTs = Math.floor(bar.startTs / bucketSize) * bucketSize;
  return {
    interval,
    startTs,
    endTs: startTs + bucketSize,
    open: roundNumber(bar.open, 2),
    high: roundNumber(bar.high, 2),
    low: roundNumber(bar.low, 2),
    close: roundNumber(bar.close, 2),
    volume: roundNumber(bar.volume ?? 0, 6)
  };
}

export function mergeCoinbaseHistoryBars(
  current: CandleBar[],
  incoming: CandleBar[] | undefined,
  interval: (typeof TRADE_CHART_INTERVALS)[number]
) {
  if (!incoming?.length) {
    return current;
  }

  const barsByStartTs = new Map<number, CandleBar>();
  for (const bar of incoming) {
    if (isPositivePrice(bar.close) && isPositivePrice(bar.high) && isPositivePrice(bar.low)) {
      const normalized = normalizeCoinbaseBar(interval, bar);
      barsByStartTs.set(normalized.startTs, normalized);
    }
  }
  for (const bar of current) {
    if (isPositivePrice(bar.close) && isPositivePrice(bar.high) && isPositivePrice(bar.low)) {
      const normalized = normalizeCoinbaseBar(interval, bar);
      barsByStartTs.set(normalized.startTs, normalized);
    }
  }

  return [...barsByStartTs.values()]
    .sort((left, right) => left.startTs - right.startTs)
    .slice(-COINBASE_BAR_LIMITS[interval]);
}

export function aggregateCoinbaseBars(
  interval: Exclude<(typeof TRADE_CHART_INTERVALS)[number], "30s">,
  sourceBars: CandleBar[]
) {
  const bucketSize = COINBASE_INTERVAL_MS[interval];
  const grouped = new Map<number, CandleBar>();
  for (const bar of [...sourceBars].sort((left, right) => left.startTs - right.startTs)) {
    if (!isPositivePrice(bar.close) || !isPositivePrice(bar.high) || !isPositivePrice(bar.low)) {
      continue;
    }
    const startTs = Math.floor(bar.startTs / bucketSize) * bucketSize;
    const existing = grouped.get(startTs);
    if (!existing) {
      grouped.set(startTs, {
        interval,
        startTs,
        endTs: startTs + bucketSize,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: bar.volume
      });
      continue;
    }
    existing.high = roundNumber(Math.max(existing.high, bar.high), 2);
    existing.low = roundNumber(Math.min(existing.low, bar.low), 2);
    existing.close = roundNumber(bar.close, 2);
    existing.volume = roundNumber(existing.volume + bar.volume, 6);
  }
  return [...grouped.values()]
    .sort((left, right) => left.startTs - right.startTs)
    .slice(-COINBASE_BAR_LIMITS[interval]);
}

export function marketCandleToBar(candle: MarketCandleRecord): CandleBar {
  return {
    interval: "30s",
    startTs: candle.openTs,
    endTs: candle.closeTs,
    open: candle.open,
    high: candle.high,
    low: candle.low,
    close: candle.close,
    volume: candle.volume
  };
}

export function shouldReplaceCoinbaseMarketCandle(existing: MarketCandleRecord | undefined, incoming: MarketCandleRecord) {
  if (!existing) {
    return true;
  }
  const existingPriority = COINBASE_MARKET_CANDLE_PRIORITY[existing.origin];
  const incomingPriority = COINBASE_MARKET_CANDLE_PRIORITY[incoming.origin];
  return incomingPriority > existingPriority || (incomingPriority === existingPriority && incoming.updatedAt >= existing.updatedAt);
}

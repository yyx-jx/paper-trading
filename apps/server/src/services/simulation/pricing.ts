import type {
  ClobMarketInfo,
  DisplayPriceSource,
  FeeBreakdown,
  MarketTrade,
  OrderBookSnapshot,
  PolymarketMarketDetail,
  TradeSide
} from "../../domain/types";
import { calculateClobFees } from "../clob-fees";
import { QTY_EPSILON, roundCurrency, roundNumber } from "./trade-calculations";

const CONSERVATIVE_CLOB_MARKET_INFO: ClobMarketInfo = {
  minimumTickSize: 0.01,
  minimumOrderSize: 1,
  makerFeeRate: 0,
  takerFeeRate: 0,
  platformFeeRate: 0,
  platformFeeExponent: 1,
  platformFeeTakerOnly: true,
  feeRateAvailable: false,
  source: "conservative",
  conservative: true,
  updatedAt: 0
};

export function isPositivePrice(value?: number): value is number {
  return typeof value === "number" && Number.isFinite(value) && value > 0;
}

function latestTradePrice(trades: MarketTrade[], side: TradeSide): number | undefined {
  for (let index = trades.length - 1; index >= 0; index -= 1) {
    const trade = trades[index];
    if (trade.side === side && isPositivePrice(trade.price)) {
      return trade.price;
    }
  }
  return undefined;
}

function resolveDisplayPrice(input: {
  bestBid: number;
  bestAsk: number;
  lastTradePrice?: number;
  outcomePrice?: number;
}): { value: number; source: DisplayPriceSource; spread: number } {
  const bestBid = isPositivePrice(input.bestBid) ? input.bestBid : undefined;
  const bestAsk = isPositivePrice(input.bestAsk) ? input.bestAsk : undefined;
  if (bestBid === undefined || bestAsk === undefined) {
    return {
      value: 0,
      source: "outcome_price",
      spread: 0
    };
  }
  const spread = bestBid !== undefined && bestAsk !== undefined ? Math.max(bestAsk - bestBid, 0) : 0;
  const midPrice = bestBid !== undefined && bestAsk !== undefined ? (bestBid + bestAsk) / 2 : undefined;

  if (midPrice !== undefined) {
    if (spread > 0.1) {
      if (isPositivePrice(input.lastTradePrice)) {
        return {
          value: roundNumber(input.lastTradePrice, 4),
          source: "last_trade",
          spread: roundNumber(spread, 4)
        };
      }
      return {
        value: 0,
        source: "outcome_price",
        spread: roundNumber(spread, 4)
      };
    }
    return {
      value: roundNumber(midPrice, 4),
      source: "mid",
      spread: roundNumber(spread, 4)
    };
  }

  if (isPositivePrice(input.lastTradePrice)) {
    return {
      value: roundNumber(input.lastTradePrice, 4),
      source: "last_trade",
      spread: roundNumber(spread, 4)
    };
  }

  if (isPositivePrice(input.outcomePrice)) {
    return {
      value: roundNumber(input.outcomePrice, 4),
      source: "outcome_price",
      spread: roundNumber(spread, 4)
    };
  }

  return {
    value: 0,
    source: "outcome_price",
    spread: roundNumber(spread, 4)
  };
}

export function resolvePairedDisplayPrices(input: {
  upBook: OrderBookSnapshot;
  downBook: OrderBookSnapshot;
  recentTrades: MarketTrade[];
  outcomePrices?: [number, number];
}): Record<TradeSide, { value: number; source: DisplayPriceSource; spread: number }> {
  const upAskDepthAvailable = input.upBook.asks.length > 0 && isPositivePrice(input.upBook.bestAsk);
  const downAskDepthAvailable = input.downBook.asks.length > 0 && isPositivePrice(input.downBook.bestAsk);
  if (!upAskDepthAvailable && !downAskDepthAvailable) {
    return {
      UP: { value: 0, source: "outcome_price", spread: 0 },
      DOWN: { value: 0, source: "outcome_price", spread: 0 }
    };
  }
  if (!upAskDepthAvailable) {
    return {
      UP: { value: 0, source: "outcome_price", spread: 0 },
      DOWN: { value: 0.01, source: "outcome_price", spread: 0 }
    };
  }
  if (!downAskDepthAvailable) {
    return {
      UP: { value: 0.01, source: "outcome_price", spread: 0 },
      DOWN: { value: 0, source: "outcome_price", spread: 0 }
    };
  }
  return {
    UP: resolveDisplayPrice({
      bestBid: input.upBook.bestBid,
      bestAsk: input.upBook.bestAsk,
      lastTradePrice: latestTradePrice(input.recentTrades, "UP"),
      outcomePrice: input.outcomePrices?.[0]
    }),
    DOWN: resolveDisplayPrice({
      bestBid: input.downBook.bestBid,
      bestAsk: input.downBook.bestAsk,
      lastTradePrice: latestTradePrice(input.recentTrades, "DOWN"),
      outcomePrice: input.outcomePrices?.[1]
    })
  };
}

export function isBtcReferencePrice(value?: number): value is number {
  return typeof value === "number" && Number.isFinite(value) && value > 1000;
}

export function isOfficialPtbSource(source?: string) {
  const normalized = source?.toLowerCase() ?? "";
  return normalized.includes("coinbase");
}

export function clobMarketInfoFor(market?: PolymarketMarketDetail): ClobMarketInfo {
  return market?.marketInfo ?? {
    ...CONSERVATIVE_CLOB_MARKET_INFO,
    conditionId: market?.conditionId,
    updatedAt: Date.now()
  };
}

export function isAlignedToTick(price: number, tickSize: number) {
  if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(tickSize) || tickSize <= 0) {
    return false;
  }
  const units = price / tickSize;
  return Math.abs(units - Math.round(units)) < 0.000001;
}

export function calculateClobFee(input: {
  role: "maker" | "taker";
  marketInfo: ClobMarketInfo;
  price?: number;
  quantity: number;
  notional: number;
}): { fee: number; breakdown?: FeeBreakdown } {
  const price = input.price ?? input.notional / Math.max(input.quantity, QTY_EPSILON);
  const result = calculateClobFees({
    role: input.role,
    feeRate: input.marketInfo.takerFeeRate,
    platformFeeRate: input.marketInfo.platformFeeRate ?? input.marketInfo.takerFeeRate,
    platformFeeExponent: input.marketInfo.platformFeeExponent,
    platformFeeTakerOnly: input.marketInfo.platformFeeTakerOnly,
    price,
    quantity: input.quantity,
    notional: input.notional,
    digits: 6
  });
  return { fee: roundCurrency(result.fee), breakdown: result.breakdown };
}

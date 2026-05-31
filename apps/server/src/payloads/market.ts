import type { SimulationEngine } from "../services/simulation";
import type { AppStore } from "../services/store";
import type {
  BookLevel,
  CandleBar,
  CandleInterval,
  MarketHistoryPatchPayload,
  MarketPayload,
  MarketRealtimeTick,
  MarketSnapshot,
  MarketTickPayload,
  MarketTransportMeta,
  RoundRecord,
  SettlementPreview,
  SourceHealth,
  TradeSide
} from "../domain/types";

const MARKET_TRANSPORT_CANDLE_LIMIT = 120;
const MARKET_TRANSPORT_ORDER_BOOK_LEVEL_LIMIT = 10;
const MARKET_TRANSPORT_RECENT_TRADE_LIMIT = 30;
const MARKET_TRANSPORT_ODDS_POINT_LIMIT = 120;

type CachedMarketHistory = {
  revision: number;
  limit: number;
  rows: Array<RoundRecord & { userPnl: number }>;
};

export type MarketPayloadBuilder = ReturnType<typeof createMarketPayloadBuilder>;

function stampSourceForTransport(source: SourceHealth, serverPublishTs: number): SourceHealth {
  return {
    ...source,
    serverPublishTs,
    frontendLatencyMs: 0
  };
}

function stampSnapshotForTransport(snapshot: MarketSnapshot, serverPublishTs = Date.now()): MarketSnapshot {
  return {
    ...snapshot,
    latencyBreakdown: {
      ...snapshot.latencyBreakdown,
      serverComputeLatency: Math.max(serverPublishTs - snapshot.serverNow, 0)
    },
    sources: {
      binance: stampSourceForTransport(snapshot.sources.binance, serverPublishTs),
      coinbase: stampSourceForTransport(snapshot.sources.coinbase, serverPublishTs),
      clob: stampSourceForTransport(snapshot.sources.clob, serverPublishTs)
    }
  };
}

function compactCandlesByInterval(candlesByInterval: Record<CandleInterval, CandleBar[]>) {
  return Object.fromEntries(
    Object.entries(candlesByInterval).map(([interval, bars]) => [
      interval,
      bars.slice(-MARKET_TRANSPORT_CANDLE_LIMIT)
    ])
  ) as Record<CandleInterval, CandleBar[]>;
}

function compactSnapshotForTransport(snapshot: MarketSnapshot): MarketSnapshot {
  const orderBooks = {
    UP: {
      ...snapshot.orderBooks.UP,
      bids: snapshot.orderBooks.UP.bids.slice(0, MARKET_TRANSPORT_ORDER_BOOK_LEVEL_LIMIT),
      asks: snapshot.orderBooks.UP.asks.slice(0, MARKET_TRANSPORT_ORDER_BOOK_LEVEL_LIMIT)
    },
    DOWN: {
      ...snapshot.orderBooks.DOWN,
      bids: snapshot.orderBooks.DOWN.bids.slice(0, MARKET_TRANSPORT_ORDER_BOOK_LEVEL_LIMIT),
      asks: snapshot.orderBooks.DOWN.asks.slice(0, MARKET_TRANSPORT_ORDER_BOOK_LEVEL_LIMIT)
    }
  };
  return {
    ...snapshot,
    orderBooks,
    recentTrades: snapshot.recentTrades.slice(0, MARKET_TRANSPORT_RECENT_TRADE_LIMIT),
    candles: snapshot.candles.slice(-MARKET_TRANSPORT_CANDLE_LIMIT),
    binance: {
      ...snapshot.binance,
      candlesByInterval: compactCandlesByInterval(snapshot.binance.candlesByInterval)
    },
    coinbase: {
      ...snapshot.coinbase,
      candles5s: snapshot.coinbase.candles5s.slice(-MARKET_TRANSPORT_CANDLE_LIMIT),
      candlesByInterval: compactCandlesByInterval(snapshot.coinbase.candlesByInterval)
    },
    clob: {
      ...snapshot.clob,
      upBook: orderBooks.UP,
      downBook: orderBooks.DOWN,
      recentTrades: snapshot.clob.recentTrades.slice(0, MARKET_TRANSPORT_RECENT_TRADE_LIMIT),
      currentRoundUpPriceSeries: snapshot.clob.currentRoundUpPriceSeries.slice(-MARKET_TRANSPORT_ODDS_POINT_LIMIT)
    }
  };
}

function countdownTargetTsFor(snapshot: MarketSnapshot) {
  const countdownMs = snapshot.uiMeta.countdownMs;
  return Number.isFinite(countdownMs) && countdownMs > 0 ? snapshot.serverNow + countdownMs : undefined;
}

export function latestCandleUpdates(candlesByInterval: Record<CandleInterval, CandleBar[]>) {
  const updates: Partial<Record<CandleInterval, CandleBar>> = {};
  for (const [interval, bars] of Object.entries(candlesByInterval) as Array<[CandleInterval, CandleBar[]]>) {
    const latest = bars.at(-1);
    if (latest) {
      updates[interval] = latest;
    }
  }
  return updates;
}

function topLevelsForTick(snapshot: MarketSnapshot): Record<TradeSide, { bids: BookLevel[]; asks: BookLevel[] }> {
  return {
    UP: {
      bids: snapshot.orderBooks.UP.bids.slice(0, 5),
      asks: snapshot.orderBooks.UP.asks.slice(0, 5)
    },
    DOWN: {
      bids: snapshot.orderBooks.DOWN.bids.slice(0, 5),
      asks: snapshot.orderBooks.DOWN.asks.slice(0, 5)
    }
  };
}

export function createMarketRealtimeTick(snapshot: MarketSnapshot, serverPublishTs: number): MarketRealtimeTick {
  const stamped = stampSnapshotForTransport(snapshot, serverPublishTs);
  const currentRoundUpPricePoint = stamped.clob.currentRoundUpPriceSeries.at(-1);
  return {
    symbol: stamped.symbol,
    marketId: stamped.marketId,
    marketSlug: stamped.marketSlug,
    serverNow: stamped.serverNow,
    currentPrice: stamped.currentPrice,
    binancePrice: stamped.binancePrice,
    coinbasePrice: stamped.coinbasePrice,
    priceToBeat: stamped.priceToBeat,
    displayPriceToBeat: stamped.displayPriceToBeat,
    displayPriceToBeatSource: stamped.displayPriceToBeatSource,
    upPrice: stamped.upPrice,
    downPrice: stamped.downPrice,
    displayPrices: stamped.displayPrices,
    displayPriceSource: stamped.displayPriceSource,
    displayPriceSpread: stamped.displayPriceSpread,
    latencyBreakdown: stamped.latencyBreakdown,
    sources: stamped.sources,
    binance: {
      spotPrice: stamped.binance.spotPrice,
      latestTick: stamped.binance.latestTick,
      candleUpdates: latestCandleUpdates(stamped.binance.candlesByInterval)
    },
    coinbase: {
      referencePrice: stamped.coinbase.referencePrice,
      settlementReference: stamped.coinbase.settlementReference,
      currentRoundOpenReference: stamped.coinbase.currentRoundOpenReference,
      candleUpdates: latestCandleUpdates(stamped.coinbase.candlesByInterval),
      latestTick:
        stamped.coinbase.referencePrice > 0
          ? { ts: stamped.sources.coinbase.normalizedTs || stamped.serverNow, price: stamped.coinbase.referencePrice }
          : undefined
    },
    clob: {
      delta: stamped.clob.delta,
      volume: stamped.clob.volume,
      currentRoundUpPricePoint,
      bestBidAskSummary: stamped.clob.bestBidAskSummary,
      topLevels: topLevelsForTick(stamped)
    },
    uiMeta: {
      countdownMs: stamped.uiMeta.countdownMs,
      countdownTargetTs: countdownTargetTsFor(stamped),
      acceptingOrders: stamped.uiMeta.acceptingOrders,
      marketSwitchState: stamped.uiMeta.marketSwitchState,
      sourceStatusSummary: stamped.uiMeta.sourceStatusSummary
    }
  };
}

export function createMarketPayloadBuilder(input: {
  store: AppStore;
  engine: SimulationEngine;
  historyCacheMaxUsers: number;
}) {
  let marketPayloadSeq = 0;
  const marketHistoryCache = new Map<string, CachedMarketHistory>();

  const nextMarketTransportMeta = (coalescedCount = 0, snapshotBuildTs?: number): MarketTransportMeta => {
    const serverPublishTs = Date.now();
    marketPayloadSeq += 1;
    return {
      serverPublishTs,
      payloadSeq: marketPayloadSeq,
      coalescedCount: coalescedCount > 0 ? coalescedCount : undefined,
      snapshotBuildTs
    };
  };

  const markTransportSendStart = (transportMeta: MarketTransportMeta) => {
    const sendStartedAt = Date.now();
    transportMeta.wsSendStartTs = sendStartedAt;
    transportMeta.serverQueueMs = Math.max(sendStartedAt - transportMeta.serverPublishTs, 0);
    return sendStartedAt;
  };

  const decorateRoundWithSettlementPreview = <T extends RoundRecord & { userPnl?: number }>(
    round: T
  ): T & { settlementPreview?: SettlementPreview } => {
    const settlementPreview = input.engine.getSettlementPreview(round);
    return settlementPreview ? { ...round, settlementPreview } : round;
  };

  const decorateCurrentRoundForTransport = (round: RoundRecord | undefined) => {
    const displayRound = input.engine.withCurrentRoundBinanceOpenReference(
      input.engine.withCurrentRoundCoinbaseOpenReference(round)
    );
    return displayRound ? decorateRoundWithSettlementPreview(displayRound) : undefined;
  };

  const getCachedHistory = (limit: number, userId?: string) => {
    const revision = input.store.getHistoryRevision();
    const cacheKey = `${userId ?? "__public__"}:${limit}`;
    const cached = marketHistoryCache.get(cacheKey);
    if (cached && cached.revision === revision && cached.limit === limit) {
      return cached.rows;
    }
    const rows = input.store.getHistory(limit, userId);
    marketHistoryCache.set(cacheKey, { revision, limit, rows });
    if (marketHistoryCache.size > input.historyCacheMaxUsers) {
      const oldestKey = marketHistoryCache.keys().next().value;
      if (oldestKey) {
        marketHistoryCache.delete(oldestKey);
      }
    }
    return rows;
  };

  const getHistoryWithSettlementPreview = (limit: number, userId?: string) =>
    getCachedHistory(limit, userId).map((round) => decorateRoundWithSettlementPreview(round));

  const getOperatedHistoryWithSettlementPreview = (limit: number, userId: string) =>
    input.store.getOperatedHistory(limit, userId).map((round) => decorateRoundWithSettlementPreview(round));

  const createCurrentRoundPayload = (coalescedCount = 0, viewedUserId?: string) => {
    const transportMeta = nextMarketTransportMeta(coalescedCount, input.store.marketSnapshot.serverNow);
    const currentRound = input.store.getCurrentRound();
    const history = getHistoryWithSettlementPreview(10, viewedUserId);
    const settlementPreview =
      (currentRound ? input.engine.getSettlementPreview(currentRound) : undefined) ??
      input.engine.getLatestSettlementPreview(history);
    return {
      viewedUserId,
      currentRound: decorateCurrentRoundForTransport(currentRound),
      snapshot: compactSnapshotForTransport(stampSnapshotForTransport(input.store.marketSnapshot, transportMeta.serverPublishTs)),
      settlementPreview,
      transportMeta
    };
  };

  const createMarketPayload = (viewedUserId: string, coalescedCount = 0): MarketPayload => ({
    ...createCurrentRoundPayload(coalescedCount, viewedUserId),
    viewedUserId,
    history: getHistoryWithSettlementPreview(10, viewedUserId),
    historyRevision: input.store.getHistoryRevision()
  });

  const createMarketTickPayload = (coalescedCount = 0, viewedUserId = ""): MarketTickPayload => {
    const snapshot = input.store.marketSnapshot;
    const transportMeta = nextMarketTransportMeta(coalescedCount, snapshot.serverNow);
    const currentRound = input.store.getCurrentRound();
    return {
      viewedUserId,
      currentRound: decorateCurrentRoundForTransport(currentRound),
      tick: createMarketRealtimeTick(snapshot, transportMeta.serverPublishTs),
      transportMeta
    };
  };

  const createMarketHistoryPatchPayload = (viewedUserId: string, historyRevision: number): MarketHistoryPatchPayload => ({
    viewedUserId,
    history: getHistoryWithSettlementPreview(10, viewedUserId),
    historyRevision,
    serverPublishTs: Date.now()
  });

  return {
    createCurrentRoundPayload,
    createMarketPayload,
    createMarketTickPayload,
    createMarketHistoryPatchPayload,
    decorateRoundWithSettlementPreview,
    getHistoryWithSettlementPreview,
    getOperatedHistoryWithSettlementPreview,
    markTransportSendStart
  };
}

export function createMarketTickPayload(builder: MarketPayloadBuilder, coalescedCount = 0, viewedUserId = "") {
  return builder.createMarketTickPayload(coalescedCount, viewedUserId);
}

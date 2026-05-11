import type {
  MarketBookPayload,
  MarketFastTick,
  MarketFastTickPayload,
  MarketPayload,
  MarketRealtimeTick,
  MarketSnapshot,
  MarketTickPayload,
  MarketTransportMeta,
  RoundRecord,
  SettlementPreview,
  SourceHealth,
  UserRecord
} from "../domain/types";
import type { SimulationEngine } from "./simulation";
import type { AppStore } from "./store";

type CachedMarketHistory = {
  revision: number;
  limit: number;
  rows: Array<RoundRecord & { userPnl: number }>;
};

export type CurrentRoundPayload = {
  currentRound?: RoundRecord & { settlementPreview?: SettlementPreview };
  snapshot: MarketSnapshot;
  settlementPreview?: SettlementPreview;
  transportMeta: MarketTransportMeta;
};

export class MarketPayloadBuilder {
  private readonly marketHistoryCache = new Map<string, CachedMarketHistory>();
  private marketPayloadSeq = 0;

  constructor(
    private readonly options: {
      store: AppStore;
      engine: SimulationEngine;
      historyCacheMaxUsers: number;
    }
  ) {}

  createCurrentRoundPayload(coalescedCount = 0, pendingSince?: number): CurrentRoundPayload {
    const { engine, store } = this.options;
    const transportMeta = this.nextTransportMeta(coalescedCount, pendingSince, store.marketSnapshot.serverNow);
    const currentRound = store.getCurrentRound();
    const history = this.getHistoryWithSettlementPreview(10);
    const settlementPreview =
      (currentRound ? engine.getSettlementPreview(currentRound) : undefined) ??
      engine.getLatestSettlementPreview(history);
    return {
      currentRound: currentRound ? this.decorateRoundWithSettlementPreview(currentRound) : undefined,
      snapshot: this.stampSnapshotForTransport(store.marketSnapshot, transportMeta.serverPublishTs),
      settlementPreview,
      transportMeta
    };
  }

  createMarketPayload(userId: string, coalescedCount = 0, pendingSince?: number): MarketPayload {
    return {
      ...this.createCurrentRoundPayload(coalescedCount, pendingSince),
      history: this.getHistoryWithSettlementPreview(10, userId)
    };
  }

  createTickPayload(coalescedCount = 0, pendingSince?: number): MarketTickPayload {
    const { engine, store } = this.options;
    const snapshot = store.marketSnapshot;
    const transportMeta = this.nextTransportMeta(coalescedCount, pendingSince, snapshot.serverNow);
    const currentRound = store.getCurrentRound();
    const settlementPreview = currentRound ? engine.getSettlementPreview(currentRound) : undefined;
    return {
      currentRound: currentRound ? this.decorateRoundWithSettlementPreview(currentRound) : undefined,
      tick: this.createRealtimeTick(snapshot, transportMeta.serverPublishTs),
      settlementPreview,
      transportMeta
    };
  }

  createFastTickPayload(coalescedCount = 0, pendingSince?: number): MarketFastTickPayload {
    const { engine, store } = this.options;
    const snapshot = store.marketSnapshot;
    const transportMeta = this.nextTransportMeta(coalescedCount, pendingSince, snapshot.serverNow);
    const currentRound = store.getCurrentRound();
    const settlementPreview = currentRound ? engine.getSettlementPreview(currentRound) : undefined;
    return {
      currentRound: currentRound ? this.decorateRoundWithSettlementPreview(currentRound) : undefined,
      tick: this.createFastTick(snapshot, transportMeta.serverPublishTs),
      settlementPreview,
      transportMeta
    };
  }

  createBookPayload(coalescedCount = 0, pendingSince?: number): MarketBookPayload {
    const { store } = this.options;
    const snapshot = store.marketSnapshot;
    const transportMeta = this.nextTransportMeta(coalescedCount, pendingSince, snapshot.serverNow);
    const stamped = this.stampSnapshotForTransport(snapshot, transportMeta.serverPublishTs);
    return {
      marketId: stamped.marketId,
      marketSlug: stamped.marketSlug,
      serverNow: stamped.serverNow,
      orderBooks: stamped.orderBooks,
      bestBidAskSummary: stamped.clob.bestBidAskSummary,
      transportMeta
    };
  }

  createBootstrapPayload(user: UserRecord) {
    const { store } = this.options;
    const market = this.createCurrentRoundPayload();
    return {
      ...market,
      history: this.getHistoryWithSettlementPreview(60, user.id),
      me: store.sanitizeUser(user),
      operatedHistory: this.getOperatedHistoryWithSettlementPreview(500, user.id),
      profile: store.getProfile(user.id),
      positions: store.getPositions(user.id),
      orders: store.getOrders(user.id),
      logs: store.getRecentLogs(user.id),
      sourceStatus: user.permissionCodes.includes("system:status:view" as never) ? store.getSourceStatus() : []
    };
  }

  getHistoryWithSettlementPreview(limit: number, userId?: string) {
    return this.getCachedHistory(limit, userId).map((round) => this.decorateRoundWithSettlementPreview(round));
  }

  getOperatedHistoryWithSettlementPreview(limit: number, userId: string) {
    return this.options.store
      .getOperatedHistory(limit, userId)
      .map((round) => this.decorateRoundWithSettlementPreview(round));
  }

  decorateRoundWithSettlementPreview<T extends RoundRecord & { userPnl?: number }>(
    round: T
  ): T & { settlementPreview?: SettlementPreview } {
    const settlementPreview = this.options.engine.getSettlementPreview(round);
    return settlementPreview ? { ...round, settlementPreview } : round;
  }

  private createRealtimeTick(snapshot: MarketSnapshot, serverPublishTs: number): MarketRealtimeTick {
    const stamped = this.stampSnapshotForTransport(snapshot, serverPublishTs);
    return {
      ...this.createFastTickFromStampedSnapshot(stamped),
      orderBooks: stamped.orderBooks
    };
  }

  private createFastTick(snapshot: MarketSnapshot, serverPublishTs: number): MarketFastTick {
    const stamped = this.stampSnapshotForTransport(snapshot, serverPublishTs);
    return this.createFastTickFromStampedSnapshot(stamped);
  }

  private createFastTickFromStampedSnapshot(stamped: MarketSnapshot): MarketFastTick {
    const currentRoundUpPricePoint = stamped.clob.currentRoundUpPriceSeries.at(-1);
    return {
      symbol: stamped.symbol,
      marketId: stamped.marketId,
      marketSlug: stamped.marketSlug,
      serverNow: stamped.serverNow,
      currentPrice: stamped.currentPrice,
      binancePrice: stamped.binancePrice,
      chainlinkPrice: stamped.chainlinkPrice,
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
        latestTick: stamped.binance.latestTick
      },
      chainlink: {
        referencePrice: stamped.chainlink.referencePrice,
        settlementReference: stamped.chainlink.settlementReference,
        latestTick:
          stamped.chainlink.referencePrice > 0
            ? { ts: stamped.sources.chainlink.normalizedTs || stamped.serverNow, price: stamped.chainlink.referencePrice }
            : undefined
      },
      clob: {
        delta: stamped.clob.delta,
        volume: stamped.clob.volume,
        currentRoundUpPricePoint,
        bestBidAskSummary: stamped.clob.bestBidAskSummary
      },
      uiMeta: {
        countdownMs: stamped.uiMeta.countdownMs,
        countdownTargetTs: this.countdownTargetTsFor(stamped),
        acceptingOrders: stamped.uiMeta.acceptingOrders,
        marketSwitchState: stamped.uiMeta.marketSwitchState,
        sourceStatusSummary: stamped.uiMeta.sourceStatusSummary
      }
    };
  }

  private stampSourceForTransport(source: SourceHealth, serverPublishTs: number): SourceHealth {
    return {
      ...source,
      serverPublishTs,
      frontendLatencyMs: 0
    };
  }

  private stampSnapshotForTransport(snapshot: MarketSnapshot, serverPublishTs = Date.now()): MarketSnapshot {
    return {
      ...snapshot,
      latencyBreakdown: {
        ...snapshot.latencyBreakdown,
        serverComputeLatency: Math.max(serverPublishTs - snapshot.serverNow, 0)
      },
      sources: {
        binance: this.stampSourceForTransport(snapshot.sources.binance, serverPublishTs),
        chainlink: this.stampSourceForTransport(snapshot.sources.chainlink, serverPublishTs),
        clob: this.stampSourceForTransport(snapshot.sources.clob, serverPublishTs)
      }
    };
  }

  private nextTransportMeta(coalescedCount = 0, pendingSince?: number, snapshotBuildTs?: number): MarketTransportMeta {
    const serverPublishTs = Date.now();
    this.marketPayloadSeq += 1;
    return {
      serverPublishTs,
      payloadSeq: this.marketPayloadSeq,
      coalescedCount: coalescedCount > 0 ? coalescedCount : undefined,
      serverQueueMs: pendingSince ? Math.max(serverPublishTs - pendingSince, 0) : undefined,
      snapshotBuildTs
    };
  }

  private getCachedHistory(limit: number, userId?: string) {
    const { store } = this.options;
    const revision = store.getHistoryRevision();
    const cacheKey = `${userId ?? "__public__"}:${limit}`;
    const cached = this.marketHistoryCache.get(cacheKey);
    if (cached && cached.revision === revision && cached.limit === limit) {
      return cached.rows;
    }
    const rows = store.getHistory(limit, userId);
    this.marketHistoryCache.set(cacheKey, { revision, limit, rows });
    if (this.marketHistoryCache.size > this.options.historyCacheMaxUsers) {
      const oldestKey = this.marketHistoryCache.keys().next().value;
      if (oldestKey) {
        this.marketHistoryCache.delete(oldestKey);
      }
    }
    return rows;
  }

  private countdownTargetTsFor(snapshot: MarketSnapshot) {
    const countdownMs = snapshot.uiMeta.countdownMs;
    return Number.isFinite(countdownMs) && countdownMs > 0 ? snapshot.serverNow + countdownMs : undefined;
  }
}

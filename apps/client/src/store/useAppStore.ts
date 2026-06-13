import { create } from "zustand";
import type {
  AuditEvent,
  BootstrapPayload,
  HistoryRound,
  MarketHistoryPatchPayload,
  MarketPayload,
  MarketRealtimeTick,
  MarketTickPayload,
  MarketSnapshot,
  MarketTransportMeta,
  OrderLifecycleRecord,
  OrderRecord,
  PositionRecord,
  ProfileOverview,
  PublicUser,
  RoundRecord,
  SettlementPreview,
  SourceHealth,
  TradeSide,
  UserPayload,
  UserTradePayload
} from "../utils/api";
import { mergeUserTradePayload as mergeTradePayloadState } from "../features/user/user-trade-payload";

interface AppState {
  token?: string;
  me?: PublicUser;
  viewedUserId?: string;
  viewedUser?: PublicUser;
  currentPage: "trade" | "home" | "profile" | "logs";
  currentRound?: RoundRecord;
  history: HistoryRound[];
  operatedHistory: HistoryRound[];
  snapshot?: MarketSnapshot;
  profile?: ProfileOverview;
  positions: PositionRecord[];
  orders: OrderRecord[];
  orderLifecycles: OrderLifecycleRecord[];
  logs: AuditEvent[];
  sourceStatus: SourceHealth[];
  lastOrderLatencyMs?: number;
  lastMarketRecvTs?: number;
  lastMarketRenderCommitTs?: number;
  lastMarketRenderLatencyMs?: number;
  lastMarketPayloadSeq?: number;
  lastMarketServerPublishTs?: number;
  lastHistoryRevision?: number;
  settlementPreview?: SettlementPreview;
  setAuth: (token: string, me?: PublicUser) => void;
  setUser: (me: PublicUser) => void;
  setViewedUserTarget: (viewedUserId: string, viewedUser?: PublicUser) => void;
  clearAuth: () => void;
  setCurrentPage: (page: "trade" | "home" | "profile" | "logs") => void;
  setBootstrap: (data: BootstrapPayload) => void;
  setMarketPayload: (data: MarketPayload, clientRecvTs?: number, clientClockOffsetMs?: number) => boolean;
  setMarketTickPayload: (data: MarketTickPayload, clientRecvTs?: number, clientClockOffsetMs?: number) => boolean;
  setMarketHistoryPatch: (data: MarketHistoryPatchPayload) => boolean;
  markMarketRenderCommit: (clientRecvTs?: number) => void;
  setUserPayload: (data: UserPayload) => void;
  setUserTradePayload: (data: UserTradePayload) => void;
  appendUserHistory: (data: {
    orders?: OrderRecord[];
    orderLifecycles?: OrderLifecycleRecord[];
    logs?: AuditEvent[];
  }) => void;
  setSourceStatus: (status: SourceHealth[]) => void;
  setLastOrderLatencyMs: (latency?: number) => void;
}

const savedToken = typeof window !== "undefined" ? window.localStorage.getItem("paper-trading-token") : undefined;

function fallbackTransportMeta(snapshot: MarketSnapshot): MarketTransportMeta {
  return {
    serverPublishTs: Math.max(
      snapshot.sources.binance.serverPublishTs,
      snapshot.sources.coinbase.serverPublishTs,
      snapshot.sources.clob.serverPublishTs
    ),
    payloadSeq: 0
  };
}

function shouldAcceptMarketPayload(
  state: AppState,
  transportMeta: MarketTransportMeta
) {
  if (transportMeta.payloadSeq > 0 && typeof state.lastMarketPayloadSeq === "number") {
    return transportMeta.payloadSeq > state.lastMarketPayloadSeq;
  }
  if (transportMeta.payloadSeq > 0) {
    return true;
  }
  if (typeof state.lastMarketServerPublishTs === "number") {
    return transportMeta.serverPublishTs >= state.lastMarketServerPublishTs;
  }
  return true;
}

function shouldAcceptViewedPayload(state: AppState, viewedUserId?: string) {
  return !state.viewedUserId || !viewedUserId || state.viewedUserId === viewedUserId;
}

function stampSourceReceipt(source: SourceHealth, clientRecvTs: number, serverPublishTs: number, clientClockOffsetMs = 0): SourceHealth {
  return {
    ...source,
    clientRecvTs,
    serverPublishTs,
    frontendLatencyMs: Math.max(clientRecvTs - serverPublishTs - clientClockOffsetMs, 0)
  };
}

function stampSnapshotReceipt(
  snapshot: MarketSnapshot,
  clientRecvTs: number,
  transportMeta = fallbackTransportMeta(snapshot),
  clientClockOffsetMs = 0
): MarketSnapshot {
  return {
    ...snapshot,
    latencyBreakdown: {
      ...snapshot.latencyBreakdown,
      clientTransportLatency: Math.max(clientRecvTs - transportMeta.serverPublishTs - clientClockOffsetMs, 0)
    },
    sources: {
      binance: stampSourceReceipt(snapshot.sources.binance, clientRecvTs, transportMeta.serverPublishTs, clientClockOffsetMs),
      coinbase: stampSourceReceipt(snapshot.sources.coinbase, clientRecvTs, transportMeta.serverPublishTs, clientClockOffsetMs),
      clob: stampSourceReceipt(snapshot.sources.clob, clientRecvTs, transportMeta.serverPublishTs, clientClockOffsetMs)
    }
  };
}

function appendRealtimePoint(points: MarketSnapshot["clob"]["currentRoundUpPriceSeries"], point?: { ts: number; price: number }) {
  if (!point || !Number.isFinite(point.ts) || !Number.isFinite(point.price) || point.price <= 0) {
    return points;
  }
  const last = points.at(-1);
  if (last && point.ts <= last.ts) {
    return [...points.slice(0, -1), point].slice(-240);
  }
  if (last && last.price === point.price && point.ts - last.ts < 1000) {
    return [...points.slice(0, -1), point].slice(-240);
  }
  return [...points, point].slice(-240);
}

function mergeBookTop(book: MarketSnapshot["orderBooks"][TradeSide], top: { bestBid: number; bestAsk: number }) {
  const bestBid = Number.isFinite(top.bestBid) ? top.bestBid : book.bestBid;
  const bestAsk = Number.isFinite(top.bestAsk) ? top.bestAsk : book.bestAsk;
  const midPrice = bestBid > 0 && bestAsk > 0 ? (bestBid + bestAsk) / 2 : book.midPrice;
  return {
    ...book,
    bestBid,
    bestAsk,
    midPrice
  };
}

function mergeBookTopLevels(
  book: MarketSnapshot["orderBooks"][TradeSide],
  top: { bestBid: number; bestAsk: number },
  levels?: { bids: MarketSnapshot["orderBooks"][TradeSide]["bids"]; asks: MarketSnapshot["orderBooks"][TradeSide]["asks"] }
) {
  const mergedTop = mergeBookTop(book, top);
  return {
    ...mergedTop,
    bids: levels?.bids ?? mergedTop.bids,
    asks: levels?.asks ?? mergedTop.asks
  };
}

function mergeCandleBar<T extends { startTs: number }>(bars: T[], update?: T) {
  if (!update) {
    return bars;
  }
  const next = bars.filter((bar) => bar.startTs !== update.startTs);
  next.push(update);
  return next.sort((left, right) => left.startTs - right.startTs).slice(-240);
}

function mergeCandleUpdates(
  candlesByInterval: MarketSnapshot["binance"]["candlesByInterval"],
  updates?: MarketRealtimeTick["binance"]["candleUpdates"]
) {
  if (!updates) {
    return candlesByInterval;
  }
  return {
    ...candlesByInterval,
    "30s": mergeCandleBar(candlesByInterval["30s"], updates["30s"]),
    "1m": mergeCandleBar(candlesByInterval["1m"], updates["1m"]),
    "5m": mergeCandleBar(candlesByInterval["5m"], updates["5m"]),
    "15m": mergeCandleBar(candlesByInterval["15m"], updates["15m"]),
    "1h": mergeCandleBar(candlesByInterval["1h"], updates["1h"]),
    "1d": mergeCandleBar(candlesByInterval["1d"], updates["1d"])
  };
}

function mergeRecordsById<T extends { id: string }>(current: T[], updates: T[], limit = 500) {
  return mergeRecordsByKey(current, updates, (item) => item.id, limit);
}

function mergeRecordsByKey<T>(current: T[], updates: T[], getKey: (item: T) => string, limit = 500) {
  const byId = new Map<string, T>();
  for (const item of current) {
    byId.set(getKey(item), item);
  }
  for (const item of updates) {
    byId.set(getKey(item), item);
  }
  return [...byId.values()].slice(0, limit);
}

function mergeRealtimeTick(snapshot: MarketSnapshot, tick: MarketRealtimeTick): MarketSnapshot {
  const topOrderBooks = {
    UP: mergeBookTopLevels(snapshot.orderBooks.UP, tick.clob.bestBidAskSummary.UP, tick.clob.topLevels?.UP),
    DOWN: mergeBookTopLevels(snapshot.orderBooks.DOWN, tick.clob.bestBidAskSummary.DOWN, tick.clob.topLevels?.DOWN)
  };
  return {
    ...snapshot,
    marketId: tick.marketId,
    marketSlug: tick.marketSlug,
    serverNow: tick.serverNow,
    currentPrice: tick.currentPrice,
    binancePrice: tick.binancePrice,
    coinbasePrice: tick.coinbasePrice,
    priceToBeat: tick.priceToBeat,
    displayPriceToBeat: tick.displayPriceToBeat,
    displayPriceToBeatSource: tick.displayPriceToBeatSource,
    upPrice: tick.upPrice,
    downPrice: tick.downPrice,
    displayPrices: tick.displayPrices,
    displayPriceSource: tick.displayPriceSource,
    displayPriceSpread: tick.displayPriceSpread,
    latencyBreakdown: tick.latencyBreakdown,
    sources: tick.sources,
    orderBooks: topOrderBooks,
    binance: {
      ...snapshot.binance,
      spotPrice: tick.binance.spotPrice,
      latestTick: tick.binance.latestTick,
      candlesByInterval: mergeCandleUpdates(snapshot.binance.candlesByInterval, tick.binance.candleUpdates)
    },
    coinbase: {
      ...snapshot.coinbase,
      referencePrice: tick.coinbase.referencePrice,
      settlementReference: tick.coinbase.settlementReference,
      currentRoundOpenReference: tick.coinbase.currentRoundOpenReference,
      candlesByInterval: mergeCandleUpdates(snapshot.coinbase.candlesByInterval, tick.coinbase.candleUpdates)
    },
    clob: {
      ...snapshot.clob,
      delta: tick.clob.delta,
      volume: tick.clob.volume,
      upBook: topOrderBooks.UP,
      downBook: topOrderBooks.DOWN,
      currentRoundUpPriceSeries: appendRealtimePoint(snapshot.clob.currentRoundUpPriceSeries, tick.clob.currentRoundUpPricePoint),
      bestBidAskSummary: tick.clob.bestBidAskSummary
    },
    uiMeta: {
      ...snapshot.uiMeta,
      countdownMs: tick.uiMeta.countdownMs,
      countdownTargetTs: tick.uiMeta.countdownTargetTs,
      acceptingOrders: tick.uiMeta.acceptingOrders,
      marketSwitchState: tick.uiMeta.marketSwitchState,
      sourceStatusSummary: tick.uiMeta.sourceStatusSummary
    }
  };
}

export const useAppStore = create<AppState>((set) => ({
  token: savedToken ?? undefined,
  currentPage: "trade",
  history: [],
  operatedHistory: [],
  positions: [],
  orders: [],
  orderLifecycles: [],
  logs: [],
  sourceStatus: [],
  setAuth: (token, me) => {
    window.localStorage.setItem("paper-trading-token", token);
    set({ token, me, viewedUserId: me?.id, viewedUser: me });
  },
  setUser: (me) => set({ me }),
  setViewedUserTarget: (viewedUserId, viewedUser) =>
    set({
      viewedUserId,
      viewedUser,
      profile: undefined,
      positions: [],
      orders: [],
      orderLifecycles: [],
      logs: [],
      operatedHistory: []
    }),
  clearAuth: () => {
    window.localStorage.removeItem("paper-trading-token");
    set({
      token: undefined,
      me: undefined,
      viewedUserId: undefined,
      viewedUser: undefined,
      currentRound: undefined,
      snapshot: undefined,
      profile: undefined,
      history: [],
      operatedHistory: [],
      positions: [],
      orders: [],
      orderLifecycles: [],
      logs: [],
      sourceStatus: [],
      lastOrderLatencyMs: undefined,
      lastMarketRecvTs: undefined,
      lastMarketRenderCommitTs: undefined,
      lastMarketRenderLatencyMs: undefined,
      lastMarketPayloadSeq: undefined,
      lastMarketServerPublishTs: undefined,
      lastHistoryRevision: undefined,
      settlementPreview: undefined,
      currentPage: "trade"
    });
  },
  setCurrentPage: (currentPage) => set({ currentPage }),
  setBootstrap: (data) => {
    const clientRecvTs = Date.now();
    const transportMeta = data.transportMeta ?? fallbackTransportMeta(data.snapshot);
    set({
      me: data.me,
      viewedUserId: data.viewedUserId,
      viewedUser: data.viewedUser,
      currentRound: data.currentRound,
      history: data.history,
      operatedHistory: data.operatedHistory ?? [],
      profile: data.profile,
      positions: data.positions,
      orders: data.orders,
      orderLifecycles: data.orderLifecycles,
      logs: data.logs,
      sourceStatus: data.sourceStatus ?? [],
      snapshot: stampSnapshotReceipt(data.snapshot, clientRecvTs, transportMeta),
      lastMarketRecvTs: clientRecvTs,
      lastMarketRenderCommitTs: clientRecvTs,
      lastMarketRenderLatencyMs: 0,
      lastMarketPayloadSeq: transportMeta.payloadSeq,
      lastMarketServerPublishTs: transportMeta.serverPublishTs,
      lastHistoryRevision: data.historyRevision,
      settlementPreview: data.settlementPreview
    });
  },
  setMarketPayload: (data, clientRecvTs = Date.now(), clientClockOffsetMs = 0) => {
    let accepted = false;
    const transportMeta = data.transportMeta ?? fallbackTransportMeta(data.snapshot);
    set((state) => {
      if (!shouldAcceptViewedPayload(state, data.viewedUserId)) {
        return state;
      }
      if (!shouldAcceptMarketPayload(state, transportMeta)) {
        return state;
      }
      accepted = true;
      const usePayloadHistory =
        typeof data.historyRevision !== "number" ||
        typeof state.lastHistoryRevision !== "number" ||
        data.historyRevision >= state.lastHistoryRevision;
      return {
        currentRound: data.currentRound,
        history: usePayloadHistory ? data.history : state.history,
        snapshot: stampSnapshotReceipt(data.snapshot, clientRecvTs, transportMeta, clientClockOffsetMs),
        lastMarketRecvTs: clientRecvTs,
        lastMarketRenderCommitTs: clientRecvTs,
        lastMarketRenderLatencyMs: 0,
        lastMarketPayloadSeq: transportMeta.payloadSeq || state.lastMarketPayloadSeq,
        lastMarketServerPublishTs: transportMeta.serverPublishTs,
        lastHistoryRevision: usePayloadHistory ? data.historyRevision ?? state.lastHistoryRevision : state.lastHistoryRevision,
        settlementPreview: data.settlementPreview
      };
    });
    return accepted;
  },
  setMarketTickPayload: (data, clientRecvTs = Date.now(), clientClockOffsetMs = 0) => {
    let accepted = false;
    const transportMeta = data.transportMeta;
    if (!transportMeta) {
      return false;
    }
    set((state) => {
      if (!state.snapshot || !shouldAcceptViewedPayload(state, data.viewedUserId) || !shouldAcceptMarketPayload(state, transportMeta)) {
        return state;
      }
      accepted = true;
      const mergedSnapshot = mergeRealtimeTick(state.snapshot, data.tick);
      return {
        currentRound: data.currentRound ?? state.currentRound,
        snapshot: stampSnapshotReceipt(mergedSnapshot, clientRecvTs, transportMeta, clientClockOffsetMs),
        lastMarketRecvTs: clientRecvTs,
        lastMarketRenderCommitTs: clientRecvTs,
        lastMarketRenderLatencyMs: 0,
        lastMarketPayloadSeq: transportMeta.payloadSeq || state.lastMarketPayloadSeq,
        lastMarketServerPublishTs: transportMeta.serverPublishTs
      };
    });
    return accepted;
  },
  setMarketHistoryPatch: (data) => {
    let accepted = false;
    set((state) => {
      if (!shouldAcceptViewedPayload(state, data.viewedUserId)) {
        return state;
      }
      if (typeof state.lastHistoryRevision === "number" && data.historyRevision < state.lastHistoryRevision) {
        return state;
      }
      accepted = true;
      return {
        history: data.history,
        lastHistoryRevision: data.historyRevision
      };
    });
    return accepted;
  },
  markMarketRenderCommit: (clientRecvTs) =>
    set({
      lastMarketRenderCommitTs: Date.now(),
      lastMarketRenderLatencyMs: clientRecvTs ? Math.max(Date.now() - clientRecvTs, 0) : undefined
    }),
  setUserPayload: (data) =>
    set((state) =>
      shouldAcceptViewedPayload(state, data.viewedUserId)
        ? {
          viewedUserId: data.viewedUserId,
          viewedUser: data.viewedUser,
          profile: data.profile,
          operatedHistory: data.operatedHistory ?? [],
          positions: data.positions,
          orders: data.orders,
          orderLifecycles: data.orderLifecycles,
          logs: data.logs
        }
        : state
    ),
  setUserTradePayload: (data) =>
    set((state) =>
      shouldAcceptViewedPayload(state, data.viewedUserId)
        ? mergeTradePayloadState(state, data)
        : state
    ),
  appendUserHistory: (data) =>
    set((state) => ({
      orders: data.orders
        ? mergeRecordsById(state.orders, data.orders).sort((left, right) => right.createdAt - left.createdAt)
        : state.orders,
      orderLifecycles: data.orderLifecycles
        ? mergeRecordsById(state.orderLifecycles, data.orderLifecycles).sort(
          (left, right) => right.orderTimestampMs - left.orderTimestampMs
        )
        : state.orderLifecycles,
      logs: data.logs
        ? mergeRecordsByKey(state.logs, data.logs, (log) => log.eventId).sort((left, right) => right.serverRecvTs - left.serverRecvTs)
        : state.logs
    })),
  setSourceStatus: (sourceStatus) => set({ sourceStatus }),
  setLastOrderLatencyMs: (lastOrderLatencyMs) => set({ lastOrderLatencyMs })
}));

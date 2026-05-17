import { create } from "zustand";
import type {
  AuditEvent,
  BootstrapPayload,
  HistoryRound,
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

interface AppState {
  token?: string;
  me?: PublicUser;
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
  settlementPreview?: SettlementPreview;
  setAuth: (token: string, me?: PublicUser) => void;
  setUser: (me: PublicUser) => void;
  clearAuth: () => void;
  setCurrentPage: (page: "trade" | "home" | "profile" | "logs") => void;
  setBootstrap: (data: BootstrapPayload) => void;
  setMarketPayload: (data: MarketPayload, clientRecvTs?: number, clientClockOffsetMs?: number) => boolean;
  setMarketTickPayload: (data: MarketTickPayload, clientRecvTs?: number, clientClockOffsetMs?: number) => boolean;
  markMarketRenderCommit: (clientRecvTs?: number) => void;
  setUserPayload: (data: UserPayload) => void;
  setUserTradePayload: (data: UserTradePayload) => void;
  setSourceStatus: (status: SourceHealth[]) => void;
  setLastOrderLatencyMs: (latency?: number) => void;
}

const savedToken = typeof window !== "undefined" ? window.localStorage.getItem("paper-trading-token") : undefined;

function fallbackTransportMeta(snapshot: MarketSnapshot): MarketTransportMeta {
  return {
    serverPublishTs: Math.max(
      snapshot.sources.binance.serverPublishTs,
      snapshot.sources.chainlink.serverPublishTs,
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
      chainlink: stampSourceReceipt(snapshot.sources.chainlink, clientRecvTs, transportMeta.serverPublishTs, clientClockOffsetMs),
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

function mergeRealtimeTick(snapshot: MarketSnapshot, tick: MarketRealtimeTick): MarketSnapshot {
  const topOrderBooks = {
    UP: mergeBookTop(snapshot.orderBooks.UP, tick.clob.bestBidAskSummary.UP),
    DOWN: mergeBookTop(snapshot.orderBooks.DOWN, tick.clob.bestBidAskSummary.DOWN)
  };
  return {
    ...snapshot,
    marketId: tick.marketId,
    marketSlug: tick.marketSlug,
    serverNow: tick.serverNow,
    currentPrice: tick.currentPrice,
    binancePrice: tick.binancePrice,
    chainlinkPrice: tick.chainlinkPrice,
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
      latestTick: tick.binance.latestTick
    },
    chainlink: {
      ...snapshot.chainlink,
      referencePrice: tick.chainlink.referencePrice,
      settlementReference: tick.chainlink.settlementReference,
      currentRoundOpenReference: tick.chainlink.currentRoundOpenReference
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
    set({ token, me });
  },
  setUser: (me) => set({ me }),
  clearAuth: () => {
    window.localStorage.removeItem("paper-trading-token");
    set({
      token: undefined,
      me: undefined,
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
      settlementPreview: data.settlementPreview
    });
  },
  setMarketPayload: (data, clientRecvTs = Date.now(), clientClockOffsetMs = 0) => {
    let accepted = false;
    const transportMeta = data.transportMeta ?? fallbackTransportMeta(data.snapshot);
    set((state) => {
      if (!shouldAcceptMarketPayload(state, transportMeta)) {
        return state;
      }
      accepted = true;
      return {
        currentRound: data.currentRound,
        history: data.history,
        snapshot: stampSnapshotReceipt(data.snapshot, clientRecvTs, transportMeta, clientClockOffsetMs),
        lastMarketRecvTs: clientRecvTs,
        lastMarketRenderCommitTs: clientRecvTs,
        lastMarketRenderLatencyMs: 0,
        lastMarketPayloadSeq: transportMeta.payloadSeq || state.lastMarketPayloadSeq,
        lastMarketServerPublishTs: transportMeta.serverPublishTs,
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
      if (!state.snapshot || !shouldAcceptMarketPayload(state, transportMeta)) {
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
        lastMarketServerPublishTs: transportMeta.serverPublishTs,
        settlementPreview: data.settlementPreview ?? state.settlementPreview
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
    set({
      profile: data.profile,
      operatedHistory: data.operatedHistory ?? [],
      positions: data.positions,
      orders: data.orders,
      orderLifecycles: data.orderLifecycles,
      logs: data.logs
    }),
  setUserTradePayload: (data) =>
    set({
      profile: data.profile,
      positions: data.positions,
      orders: data.orders,
      orderLifecycles: data.orderLifecycles
    }),
  setSourceStatus: (sourceStatus) => set({ sourceStatus }),
  setLastOrderLatencyMs: (lastOrderLatencyMs) => set({ lastOrderLatencyMs })
}));

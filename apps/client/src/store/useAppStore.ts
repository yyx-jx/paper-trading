import { create } from "zustand";
import type {
  AuditEvent,
  BootstrapPayload,
  HistoryRound,
  MarketPayload,
  MarketSnapshot,
  MarketTransportMeta,
  OrderRecord,
  PositionRecord,
  ProfileOverview,
  PublicUser,
  RoundRecord,
  SettlementPreview,
  SourceHealth,
  UserPayload
} from "../utils/api";

interface AppState {
  token?: string;
  me?: PublicUser;
  currentPage: "trade" | "profile" | "logs" | "replay" | "users";
  currentRound?: RoundRecord;
  history: HistoryRound[];
  operatedHistory: HistoryRound[];
  snapshot?: MarketSnapshot;
  profile?: ProfileOverview;
  positions: PositionRecord[];
  orders: OrderRecord[];
  logs: AuditEvent[];
  sourceStatus: SourceHealth[];
  lastOrderLatencyMs?: number;
  lastMarketRecvTs?: number;
  lastMarketPayloadSeq?: number;
  lastMarketServerPublishTs?: number;
  settlementPreview?: SettlementPreview;
  setAuth: (token: string, me?: PublicUser) => void;
  setUser: (me: PublicUser) => void;
  clearAuth: () => void;
  setCurrentPage: (page: "trade" | "profile" | "logs" | "replay" | "users") => void;
  setBootstrap: (data: BootstrapPayload) => void;
  setMarketPayload: (data: MarketPayload, clientRecvTs?: number) => boolean;
  setUserPayload: (data: UserPayload) => void;
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

function stampSourceReceipt(source: SourceHealth, clientRecvTs: number, serverPublishTs: number): SourceHealth {
  return {
    ...source,
    clientRecvTs,
    serverPublishTs,
    frontendLatencyMs: Math.max(clientRecvTs - serverPublishTs, 0)
  };
}

function stampSnapshotReceipt(
  snapshot: MarketSnapshot,
  clientRecvTs: number,
  transportMeta = fallbackTransportMeta(snapshot)
): MarketSnapshot {
  return {
    ...snapshot,
    latencyBreakdown: {
      ...snapshot.latencyBreakdown,
      clientTransportLatency: Math.max(clientRecvTs - transportMeta.serverPublishTs, 0)
    },
    sources: {
      binance: stampSourceReceipt(snapshot.sources.binance, clientRecvTs, transportMeta.serverPublishTs),
      chainlink: stampSourceReceipt(snapshot.sources.chainlink, clientRecvTs, transportMeta.serverPublishTs),
      clob: stampSourceReceipt(snapshot.sources.clob, clientRecvTs, transportMeta.serverPublishTs)
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
      logs: [],
      sourceStatus: [],
      lastOrderLatencyMs: undefined,
      lastMarketRecvTs: undefined,
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
      logs: data.logs,
      sourceStatus: data.sourceStatus ?? [],
      snapshot: stampSnapshotReceipt(data.snapshot, clientRecvTs, transportMeta),
      lastMarketRecvTs: clientRecvTs,
      lastMarketPayloadSeq: transportMeta.payloadSeq,
      lastMarketServerPublishTs: transportMeta.serverPublishTs,
      settlementPreview: data.settlementPreview
    });
  },
  setMarketPayload: (data, clientRecvTs = Date.now()) => {
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
        snapshot: stampSnapshotReceipt(data.snapshot, clientRecvTs, transportMeta),
        lastMarketRecvTs: clientRecvTs,
        lastMarketPayloadSeq: transportMeta.payloadSeq || state.lastMarketPayloadSeq,
        lastMarketServerPublishTs: transportMeta.serverPublishTs,
        settlementPreview: data.settlementPreview
      };
    });
    return accepted;
  },
  setUserPayload: (data) =>
    set({
      profile: data.profile,
      operatedHistory: data.operatedHistory ?? [],
      positions: data.positions,
      orders: data.orders,
      logs: data.logs
    }),
  setSourceStatus: (sourceStatus) => set({ sourceStatus }),
  setLastOrderLatencyMs: (lastOrderLatencyMs) => set({ lastOrderLatencyMs })
}));

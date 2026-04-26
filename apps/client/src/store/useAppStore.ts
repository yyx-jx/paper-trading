import { create } from "zustand";
import type {
  AuditEvent,
  HistoryRound,
  MarketSnapshot,
  OrderRecord,
  PositionRecord,
  ProfileOverview,
  PublicUser,
  RoundRecord,
  SourceHealth
} from "../utils/api";

interface AppState {
  token?: string;
  me?: PublicUser;
  currentPage: "trade" | "profile" | "logs";
  currentRound?: RoundRecord;
  history: HistoryRound[];
  snapshot?: MarketSnapshot;
  profile?: ProfileOverview;
  positions: PositionRecord[];
  orders: OrderRecord[];
  logs: AuditEvent[];
  sourceStatus: SourceHealth[];
  lastOrderLatencyMs?: number;
  lastMarketRecvTs?: number;
  setAuth: (token: string, me?: PublicUser) => void;
  setUser: (me: PublicUser) => void;
  clearAuth: () => void;
  setCurrentPage: (page: "trade" | "profile" | "logs") => void;
  setShellData: (data: {
    currentRound?: RoundRecord;
    history: HistoryRound[];
    snapshot: MarketSnapshot;
    profile: ProfileOverview;
    positions: PositionRecord[];
    orders: OrderRecord[];
    logs: AuditEvent[];
  }) => void;
  setMarketPayload: (
    data: { currentRound?: RoundRecord; history: HistoryRound[]; snapshot: MarketSnapshot },
    clientRecvTs?: number
  ) => void;
  setUserPayload: (data: { profile: ProfileOverview; positions: PositionRecord[]; orders: OrderRecord[]; logs: AuditEvent[] }) => void;
  setSourceStatus: (status: SourceHealth[]) => void;
  setLastOrderLatencyMs: (latency?: number) => void;
}

const savedToken = typeof window !== "undefined" ? window.localStorage.getItem("paper-trading-token") : undefined;

function stampSourceReceipt(source: SourceHealth, clientRecvTs: number): SourceHealth {
  return {
    ...source,
    clientRecvTs,
    frontendLatencyMs: Math.max(clientRecvTs - source.serverPublishTs, 0)
  };
}

function stampSnapshotReceipt(snapshot: MarketSnapshot, clientRecvTs: number): MarketSnapshot {
  return {
    ...snapshot,
    sources: {
      binance: stampSourceReceipt(snapshot.sources.binance, clientRecvTs),
      chainlink: stampSourceReceipt(snapshot.sources.chainlink, clientRecvTs),
      clob: stampSourceReceipt(snapshot.sources.clob, clientRecvTs)
    }
  };
}

export const useAppStore = create<AppState>((set) => ({
  token: savedToken ?? undefined,
  currentPage: "trade",
  history: [],
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
      positions: [],
      orders: [],
      logs: [],
      sourceStatus: [],
      lastOrderLatencyMs: undefined,
      lastMarketRecvTs: undefined,
      currentPage: "trade"
    });
  },
  setCurrentPage: (currentPage) => set({ currentPage }),
  setShellData: (data) => {
    const clientRecvTs = Date.now();
    set({ ...data, snapshot: stampSnapshotReceipt(data.snapshot, clientRecvTs), lastMarketRecvTs: clientRecvTs });
  },
  setMarketPayload: (data, clientRecvTs = Date.now()) =>
    set({
      currentRound: data.currentRound,
      history: data.history,
      snapshot: stampSnapshotReceipt(data.snapshot, clientRecvTs),
      lastMarketRecvTs: clientRecvTs
    }),
  setUserPayload: (data) => set({ profile: data.profile, positions: data.positions, orders: data.orders, logs: data.logs }),
  setSourceStatus: (sourceStatus) => set({ sourceStatus }),
  setLastOrderLatencyMs: (lastOrderLatencyMs) => set({ lastOrderLatencyMs })
}));

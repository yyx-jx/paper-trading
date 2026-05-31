import { useEffect, type Dispatch, type RefObject, type SetStateAction } from "react";
import {
  api,
  type MarketHistoryPatchPayload,
  type MarketPayload,
  type MarketTickPayload
} from "../../utils/api";
import { redactNetworkAddresses } from "../../utils/redaction";
import { useAppStore } from "../../store/useAppStore";
import {
  MARKET_LIVE_RECOVERY_PAYLOADS,
  transitionRealtimeChannel,
  type RealtimeChannelStatus,
  type RealtimeStatus
} from "../realtime/status";

function transportAgeMs(receivedAt: number, publishTs: number, clientClockOffsetMs = 0) {
  return Math.max(receivedAt - publishTs - clientClockOffsetMs, 0);
}

function extractMarketPayloadPublishTs(payload?: Pick<MarketPayload, "snapshot" | "transportMeta">) {
  if (!payload?.snapshot) {
    return 0;
  }
  if (payload.transportMeta?.serverPublishTs) {
    return payload.transportMeta.serverPublishTs;
  }
  const snapshot = payload.snapshot;
  return Math.max(
    snapshot.sources.binance.serverPublishTs,
    snapshot.sources.coinbase.serverPublishTs,
    snapshot.sources.clob.serverPublishTs
  );
}

export function useMarketSocket(input: {
  token?: string;
  meId?: string;
  activeViewUserId?: string;
  clientClockOffsetMsRef: RefObject<number>;
  setRealtimeStatus: Dispatch<SetStateAction<RealtimeStatus>>;
  updateRealtimeChannel: (
    channel: "market",
    patch: Partial<RealtimeChannelStatus>,
    options?: { now?: number; force?: boolean; failure?: boolean; recoverPayloads?: number }
  ) => void;
  setMarketPayload: (data: MarketPayload, clientRecvTs?: number, clientClockOffsetMs?: number) => boolean;
  setMarketTickPayload: (data: MarketTickPayload, clientRecvTs?: number, clientClockOffsetMs?: number) => boolean;
  setMarketHistoryPatch: (data: MarketHistoryPatchPayload) => boolean;
  markMarketRenderCommit: (clientRecvTs?: number) => void;
}) {
  useEffect(() => {
    if (!input.token || !input.meId) {
      return;
    }

    const activeViewUserId = input.activeViewUserId ?? input.meId;
    let disposed = false;
    let marketSocket: WebSocket | undefined;
    let marketReconnectTimer: number | undefined;
    let marketWatchdogTimer: number | undefined;
    let lastMarketMessageAt = Date.now();
    let refreshingMarket = false;
    let lastMarketFallbackAt = 0;
    const reconnectDelayMs = 1000;
    const marketPayloadRejectMs = 4000;
    const marketStaleMs = 1500;
    const marketReconnectStaleMs = 4000;
    const marketFallbackCooldownMs = 1500;
    let pendingMarketTick: { data: MarketTickPayload; receivedAt: number } | undefined;
    let marketTickFrame: number | undefined;

    const markMarketActivity = (receivedAt = Date.now()) => {
      lastMarketMessageAt = receivedAt;
      input.updateRealtimeChannel(
        "market",
        { state: "live", lastMessageAt: receivedAt, lastError: undefined },
        { now: receivedAt, recoverPayloads: MARKET_LIVE_RECOVERY_PAYLOADS }
      );
    };

    const markMarketRendered = (receivedAt: number) => {
      window.requestAnimationFrame(() => {
        if (!disposed) {
          input.markMarketRenderCommit(receivedAt);
        }
      });
    };

    const refreshMarketSnapshot = async () => {
      if (disposed || refreshingMarket) {
        return;
      }
      refreshingMarket = true;
      lastMarketFallbackAt = Date.now();
      input.updateRealtimeChannel("market", { state: "fallback", fallbackAt: lastMarketFallbackAt }, { now: lastMarketFallbackAt, failure: true });
      try {
        const roundData = await api.getCurrentRound(input.token!, activeViewUserId);
        if (!disposed) {
          const receivedAt = Date.now();
          const payload = {
            viewedUserId: roundData.viewedUserId ?? activeViewUserId,
            currentRound: roundData.currentRound,
            history: useAppStore.getState().history,
            snapshot: roundData.snapshot,
            settlementPreview: roundData.settlementPreview,
            transportMeta: roundData.transportMeta
          };
          if (input.setMarketPayload(payload, receivedAt, input.clientClockOffsetMsRef.current)) {
            markMarketActivity(receivedAt);
            markMarketRendered(receivedAt);
          } else {
            input.updateRealtimeChannel(
              "market",
              { state: marketSocket?.readyState === WebSocket.OPEN ? "live" : "fallback" },
              { now: receivedAt, recoverPayloads: MARKET_LIVE_RECOVERY_PAYLOADS }
            );
          }
        }
      } catch (refreshError) {
        input.updateRealtimeChannel("market", {
          state: "offline",
          lastError: refreshError instanceof Error ? redactNetworkAddresses(refreshError.message) : "Market refresh failed."
        }, { failure: true });
      } finally {
        refreshingMarket = false;
      }
    };

    const scheduleMarketReconnect = () => {
      if (disposed || typeof marketReconnectTimer === "number") {
        return;
      }
      input.setRealtimeStatus((current) => ({
        ...current,
        market: transitionRealtimeChannel(current.market, {
          state: "reconnecting",
          reconnects: current.market.reconnects + 1
        }, { failure: true })
      }));
      marketReconnectTimer = window.setTimeout(() => {
        marketReconnectTimer = undefined;
        void connectMarketSocket();
      }, reconnectDelayMs);
    };

    const connectMarketSocket = async () => {
      if (disposed) {
        return;
      }
      marketSocket?.close();
      input.updateRealtimeChannel("market", { state: "connecting", lastError: undefined }, { force: true });
      let wsUrl = api.createWsUrl("/ws/market", input.token!, activeViewUserId);
      try {
        const ticket = await api.createWsTicket(input.token!, "market", activeViewUserId);
        wsUrl = api.createWsTicketUrl("/ws/market", ticket.ticket);
      } catch {
        wsUrl = api.createWsUrl("/ws/market", input.token!, activeViewUserId);
      }
      if (disposed) {
        return;
      }
      const socket = new WebSocket(wsUrl);
      marketSocket = socket;
      socket.onopen = () => {
        input.updateRealtimeChannel("market", { state: "connecting", lastError: undefined });
      };
      socket.onmessage = (event) => {
        const receivedAt = Date.now();
        let parsed: {
          type: "market" | "market:tick" | "market:history-patch";
          data: MarketPayload | MarketTickPayload | MarketHistoryPatchPayload;
        };
        try {
          parsed = JSON.parse(event.data) as {
            type: "market" | "market:tick" | "market:history-patch";
            data: MarketPayload | MarketTickPayload | MarketHistoryPatchPayload;
          };
        } catch (parseError) {
          input.updateRealtimeChannel("market", {
            lastError: parseError instanceof Error ? redactNetworkAddresses(parseError.message) : "Invalid market message."
          });
          return;
        }
        if (parsed.type === "market") {
          const data = parsed.data as MarketPayload;
          const publishTs = extractMarketPayloadPublishTs(data);
          const payloadAgeMs =
            publishTs > 0 ? transportAgeMs(receivedAt, publishTs, input.clientClockOffsetMsRef.current) : 0;
          if (publishTs > 0 && payloadAgeMs > marketPayloadRejectMs) {
            void refreshMarketSnapshot();
            if (payloadAgeMs > marketReconnectStaleMs && socket.readyState === WebSocket.OPEN) {
              socket.close();
            }
            return;
          }
          if (input.setMarketPayload(data, receivedAt, input.clientClockOffsetMsRef.current)) {
            markMarketActivity(receivedAt);
            markMarketRendered(receivedAt);
          }
          return;
        }
        if (parsed.type === "market:history-patch") {
          if (input.setMarketHistoryPatch(parsed.data as MarketHistoryPatchPayload)) {
            markMarketActivity(receivedAt);
          }
          return;
        }
        if (parsed.type === "market:tick") {
          pendingMarketTick = { data: parsed.data as MarketTickPayload, receivedAt };
          if (typeof marketTickFrame !== "number") {
            marketTickFrame = window.requestAnimationFrame(() => {
              marketTickFrame = undefined;
              const pending = pendingMarketTick;
              pendingMarketTick = undefined;
              if (!pending || disposed) {
                return;
              }
              const publishTs = pending.data.transportMeta?.serverPublishTs ?? 0;
              const payloadAgeMs =
                publishTs > 0 ? transportAgeMs(pending.receivedAt, publishTs, input.clientClockOffsetMsRef.current) : 0;
              if (publishTs > 0 && payloadAgeMs > marketPayloadRejectMs) {
                if (payloadAgeMs > marketReconnectStaleMs && socket.readyState === WebSocket.OPEN) {
                  socket.close();
                }
                return;
              }
              if (input.setMarketTickPayload(pending.data, pending.receivedAt, input.clientClockOffsetMsRef.current)) {
                markMarketActivity(pending.receivedAt);
                input.markMarketRenderCommit(pending.receivedAt);
              }
            });
          }
        }
      };
      socket.onerror = () => {
        input.updateRealtimeChannel("market", { state: "reconnecting", lastError: "Market stream error." }, { failure: true });
        socket.close();
      };
      socket.onclose = () => {
        if (marketSocket === socket) {
          marketSocket = undefined;
        }
        scheduleMarketReconnect();
      };
    };

    const handleForegroundRecovery = () => {
      if (disposed) {
        return;
      }
      const now = Date.now();
      const marketIdleMs = now - lastMarketMessageAt;
      if (!marketSocket || marketSocket.readyState !== WebSocket.OPEN) {
        void refreshMarketSnapshot();
        scheduleMarketReconnect();
      } else if (marketIdleMs > marketStaleMs) {
        void refreshMarketSnapshot();
      }
    };

    const handleVisibilityChange = () => {
      if (document.visibilityState === "visible") {
        handleForegroundRecovery();
      }
    };

    void connectMarketSocket();
    document.addEventListener("visibilitychange", handleVisibilityChange);
    window.addEventListener("focus", handleForegroundRecovery);
    window.addEventListener("pageshow", handleForegroundRecovery);
    marketWatchdogTimer = window.setInterval(() => {
      if (disposed) {
        return;
      }
      const now = Date.now();
      const socket = marketSocket;
      if (!socket || socket.readyState !== WebSocket.OPEN) {
        if (now - lastMarketFallbackAt > marketFallbackCooldownMs) {
          void refreshMarketSnapshot();
        }
        if (!socket || socket.readyState === WebSocket.CLOSED) {
          scheduleMarketReconnect();
        }
      } else {
        const idleMs = now - lastMarketMessageAt;
        if (socket.readyState === WebSocket.OPEN && idleMs > marketStaleMs && now - lastMarketFallbackAt > marketFallbackCooldownMs) {
          void refreshMarketSnapshot();
        }
        if (socket.readyState === WebSocket.OPEN && idleMs > marketReconnectStaleMs) {
          socket.close();
        }
      }
    }, 250);

    return () => {
      disposed = true;
      if (typeof marketReconnectTimer === "number") {
        window.clearTimeout(marketReconnectTimer);
      }
      if (typeof marketWatchdogTimer === "number") {
        window.clearInterval(marketWatchdogTimer);
      }
      if (typeof marketTickFrame === "number") {
        window.cancelAnimationFrame(marketTickFrame);
      }
      document.removeEventListener("visibilitychange", handleVisibilityChange);
      window.removeEventListener("focus", handleForegroundRecovery);
      window.removeEventListener("pageshow", handleForegroundRecovery);
      marketSocket?.close();
    };
  }, [
    input.token,
    input.meId,
    input.activeViewUserId,
    input.clientClockOffsetMsRef,
    input.setRealtimeStatus,
    input.updateRealtimeChannel,
    input.setMarketPayload,
    input.setMarketTickPayload,
    input.setMarketHistoryPatch,
    input.markMarketRenderCommit
  ]);
}

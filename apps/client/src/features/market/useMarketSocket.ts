import { useEffect, type Dispatch, type RefObject, type SetStateAction } from "react";
import { api, type MarketHistoryPatchPayload, type MarketPayload, type MarketTickPayload } from "../../utils/api";
import { redactNetworkAddresses } from "../../utils/redaction";
import { useAppStore } from "../../store/useAppStore";
import {
  MARKET_LIVE_RECOVERY_PAYLOADS,
  transitionRealtimeChannel,
  type RealtimeChannelStatus,
  type RealtimeStatus
} from "../realtime/status";
import { createRealtimeSocketRuntime, evaluateRealtimeWatchdog } from "../realtime/socket-runtime";
import {
  createMarketFallbackPayload,
  extractMarketPayloadPublishTs,
  MARKET_SOCKET_TIMING,
  parseMarketWsMessage,
  type MarketWsMessage
} from "./market-ws-message";
import { shouldRecoverClosedMarketSocket, shouldRejectStaleMarketPayload } from "./market-reconnect-policy";

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
    const marketRuntime = createRealtimeSocketRuntime();
    let refreshingMarket = false;
    let pendingMarketTick: { data: MarketTickPayload; receivedAt: number } | undefined;
    let marketTickFrame: number | undefined;
    const markMarketActivity = (receivedAt = Date.now()) => {
      marketRuntime.markFrame(receivedAt);
      marketRuntime.markAccepted(receivedAt);
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
      const fallbackAt = marketRuntime.markFallback();
      input.updateRealtimeChannel("market", { state: "fallback", fallbackAt }, { now: fallbackAt, failure: true });
      try {
        const roundData = await api.getCurrentRound(input.token!, activeViewUserId);
        if (!disposed) {
          const receivedAt = Date.now();
          const payload = createMarketFallbackPayload({
            roundData,
            activeViewUserId,
            history: useAppStore.getState().history
          });
          if (input.setMarketPayload(payload, receivedAt, input.clientClockOffsetMsRef.current)) {
            markMarketActivity(receivedAt);
            markMarketRendered(receivedAt);
          } else {
            marketRuntime.markRejected("http_snapshot_store_rejected");
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
      if (disposed || typeof marketRuntime.getReconnectTimer() === "number") {
        return;
      }
      input.setRealtimeStatus((current) => ({
        ...current,
        market: transitionRealtimeChannel(current.market, {
          state: "reconnecting",
          reconnects: current.market.reconnects + 1
        }, { failure: true })
      }));
      marketRuntime.setReconnectTimer(window.setTimeout(() => {
        marketRuntime.setReconnectTimer(undefined);
        void connectMarketSocket();
      }, MARKET_SOCKET_TIMING.reconnectDelayMs));
    };

    const connectMarketSocket = async () => {
      if (disposed) {
        return;
      }
      marketSocket?.close();
      marketRuntime.markConnectAttempt();
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
        marketRuntime.markFrame(receivedAt);
        let parsed: MarketWsMessage;
        try {
          parsed = parseMarketWsMessage(event.data);
        } catch (parseError) {
          input.updateRealtimeChannel("market", {
            lastError: parseError instanceof Error ? redactNetworkAddresses(parseError.message) : "Invalid market message."
          });
          return;
        }
        if (parsed.type === "market") {
          const data = parsed.data;
          const publishTs = extractMarketPayloadPublishTs(data);
          if (shouldRejectStaleMarketPayload(receivedAt, publishTs, input.clientClockOffsetMsRef.current)) {
            marketRuntime.markRejected("payload_too_old");
            return;
          }
          if (input.setMarketPayload(data, receivedAt, input.clientClockOffsetMsRef.current)) {
            markMarketActivity(receivedAt);
            markMarketRendered(receivedAt);
          } else {
            marketRuntime.markRejected("market_store_rejected");
          }
          return;
        }
        if (parsed.type === "market:history-patch") {
          if (input.setMarketHistoryPatch(parsed.data)) {
            markMarketActivity(receivedAt);
          } else {
            marketRuntime.markRejected("history_patch_rejected");
          }
          return;
        }
        if (parsed.type === "market:tick") {
          pendingMarketTick = { data: parsed.data, receivedAt };
          if (typeof marketTickFrame !== "number") {
            marketTickFrame = window.requestAnimationFrame(() => {
              marketTickFrame = undefined;
              const pending = pendingMarketTick;
              pendingMarketTick = undefined;
              if (!pending || disposed) {
                return;
              }
              const publishTs = pending.data.transportMeta?.serverPublishTs ?? 0;
              if (shouldRejectStaleMarketPayload(pending.receivedAt, publishTs, input.clientClockOffsetMsRef.current)) {
                marketRuntime.markRejected("tick_payload_too_old");
                return;
              }
              if (input.setMarketTickPayload(pending.data, pending.receivedAt, input.clientClockOffsetMsRef.current)) {
                markMarketActivity(pending.receivedAt);
                input.markMarketRenderCommit(pending.receivedAt);
              } else {
                marketRuntime.markRejected("tick_store_rejected");
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
      if (shouldRecoverClosedMarketSocket(marketSocket?.readyState)) {
        void refreshMarketSnapshot();
        scheduleMarketReconnect();
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
    marketRuntime.setWatchdogTimer(window.setInterval(() => {
      if (disposed) {
        return;
      }
      const now = Date.now();
      const socket = marketSocket;
      const decision = evaluateRealtimeWatchdog({
        socketState: socket?.readyState,
        idleMs: marketRuntime.frameIdleMs(now),
        acceptedIdleMs: marketRuntime.acceptedIdleMs(now),
        fallbackCooldownMs: MARKET_SOCKET_TIMING.fallbackCooldownMs,
        staleMs: MARKET_SOCKET_TIMING.staleMs,
        reconnectStaleMs: MARKET_SOCKET_TIMING.reconnectStaleMs,
        sinceLastFallbackMs: marketRuntime.sinceLastFallbackMs(now),
        connectionAgeMs: marketRuntime.connectionAgeMs(now),
        connectingStaleMs: MARKET_SOCKET_TIMING.connectingStaleMs
      });
      if (decision.shouldRefresh) {
        void refreshMarketSnapshot();
      }
      if (decision.shouldReconnect) {
        scheduleMarketReconnect();
      }
      if (decision.shouldClose && socket) {
        socket.close();
      }
    }, 250));

    return () => {
      disposed = true;
      marketRuntime.cleanup((timer) => window.clearTimeout(timer));
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

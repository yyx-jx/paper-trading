import { startTransition, useEffect, useRef, type Dispatch, type SetStateAction } from "react";
import {
  api,
  type PublicUser,
  type UserPayload,
  type UserWsMessage,
  type UserTradePayload
} from "../../utils/api";
import { redactNetworkAddresses } from "../../utils/redaction";
import {
  transitionRealtimeChannel,
  USER_LIVE_RECOVERY_PAYLOADS,
  type RealtimeChannelStatus,
  type RealtimeStatus
} from "../realtime/status";
import { getClientInstanceId } from "../realtime/client-instance";
import { createRealtimeSocketRuntime, evaluateRealtimeWatchdog } from "../realtime/socket-runtime";

export function useUserSocket(input: {
  token?: string;
  me?: PublicUser;
  activeViewUserId?: string;
  activeViewedUser?: PublicUser;
  setRealtimeStatus: Dispatch<SetStateAction<RealtimeStatus>>;
  updateRealtimeChannel: (
    channel: "user",
    patch: Partial<RealtimeChannelStatus>,
    options?: { now?: number; force?: boolean; failure?: boolean; recoverPayloads?: number }
  ) => void;
  setUserPayload: (data: UserPayload) => void;
  setUserTradePayload: (data: UserTradePayload) => void;
}) {
  const latestUsersRef = useRef<{
    me?: PublicUser;
    activeViewedUser?: PublicUser;
  }>({});
  latestUsersRef.current = {
    me: input.me,
    activeViewedUser: input.activeViewedUser
  };

  const token = input.token;
  const meId = input.me?.id;
  const activeViewUserId = input.activeViewUserId ?? meId;
  const clientInstanceId = getClientInstanceId();

  useEffect(() => {
    if (!token || !meId || !activeViewUserId) {
      return;
    }

    let disposed = false;
    let userSocket: WebSocket | undefined;
    let refreshingUser = false;
    const userRuntime = createRealtimeSocketRuntime();
    const reconnectDelayMs = 1000;
    const userFrameReconnectStaleMs = 30_000;
    const userConnectingStaleMs = 8000;
    const userFallbackCooldownMs = 5000;

    const markUserActivity = (receivedAt = Date.now()) => {
      userRuntime.markFrame(receivedAt);
      userRuntime.markAccepted(receivedAt);
      input.updateRealtimeChannel(
        "user",
        { state: "live", lastMessageAt: receivedAt, lastError: undefined },
        { now: receivedAt, recoverPayloads: USER_LIVE_RECOVERY_PAYLOADS }
      );
    };

    const refreshUserSnapshot = async () => {
      if (disposed || refreshingUser) {
        return;
      }
      refreshingUser = true;
      const fallbackAt = userRuntime.markFallback();
      input.updateRealtimeChannel("user", { state: "fallback", fallbackAt }, { now: fallbackAt, failure: true });
      try {
        const [nextProfile, nextOperatedHistory, nextPositions, nextOrders, nextOrderLifecycles, nextLogs] = await Promise.all([
          api.getProfile(token, activeViewUserId),
          api.getOperatedHistory(token, 200, activeViewUserId),
          api.getPositions(token, activeViewUserId),
          api.getOrders(token, activeViewUserId),
          api.getOrderLifecycles(token, activeViewUserId),
          api.getLogs(token, activeViewUserId)
        ]);
        if (!disposed) {
          const receivedAt = Date.now();
          const latestViewedUser = latestUsersRef.current.activeViewedUser ?? latestUsersRef.current.me;
          if (!latestViewedUser) {
            return;
          }
          input.setUserPayload({
            viewedUserId: activeViewUserId,
            viewedUser: latestViewedUser,
            profile: nextProfile,
            operatedHistory: nextOperatedHistory,
            positions: nextPositions,
            orders: nextOrders,
            orderLifecycles: nextOrderLifecycles,
            logs: nextLogs
          });
          markUserActivity(receivedAt);
        }
      } catch (refreshError) {
        input.updateRealtimeChannel("user", {
          state: "offline",
          lastError: refreshError instanceof Error ? redactNetworkAddresses(refreshError.message) : "User refresh failed."
        }, { failure: true });
      } finally {
        refreshingUser = false;
      }
    };

    const scheduleUserReconnect = () => {
      if (disposed || typeof userRuntime.getReconnectTimer() === "number") {
        return;
      }
      input.setRealtimeStatus((current) => ({
        ...current,
        user: transitionRealtimeChannel(current.user, {
          state: "reconnecting",
          reconnects: current.user.reconnects + 1
        }, { failure: true })
      }));
      userRuntime.setReconnectTimer(window.setTimeout(() => {
        userRuntime.setReconnectTimer(undefined);
        void connectUserSocket();
      }, reconnectDelayMs));
    };

    const connectUserSocket = async () => {
      if (disposed) {
        return;
      }
      userSocket?.close();
      userRuntime.markConnectAttempt();
      input.updateRealtimeChannel("user", { state: "connecting", lastError: undefined }, { force: true });
      let wsUrl = api.createWsUrl("/ws/user", token, activeViewUserId, clientInstanceId);
      try {
        const ticket = await api.createWsTicket(token, "user", activeViewUserId, clientInstanceId);
        wsUrl = api.createWsTicketUrl("/ws/user", ticket.ticket);
      } catch {
        wsUrl = api.createWsUrl("/ws/user", token, activeViewUserId, clientInstanceId);
      }
      if (disposed) {
        return;
      }
      const socket = new WebSocket(wsUrl);
      userSocket = socket;
      socket.onopen = () => {
        input.updateRealtimeChannel("user", { state: "connecting", lastError: undefined });
      };
      socket.onmessage = (event) => {
        const receivedAt = Date.now();
        userRuntime.markFrame(receivedAt);
        const processingStartedAt = performance.now();
        let parsed: UserWsMessage;
        try {
          parsed = JSON.parse(event.data) as UserWsMessage;
        } catch (parseError) {
          input.updateRealtimeChannel("user", {
            lastError: parseError instanceof Error ? redactNetworkAddresses(parseError.message) : "Invalid user message."
          });
          return;
        }
        if (parsed.type === "user:heartbeat") {
          markUserActivity(receivedAt);
          return;
        }
        const payloadBytes = typeof event.data === "string" ? event.data.length : 0;
        const warnSlowUserMessage = () => {
          const elapsedMs = Math.round(performance.now() - processingStartedAt);
          if (elapsedMs > 50 || payloadBytes > 200_000) {
            console.warn(`[ws:user] processed type=${parsed.type} bytes=${payloadBytes} elapsedMs=${elapsedMs}`);
          }
        };
        if (parsed.type === "user" || parsed.type === "user:trade") {
          window.requestAnimationFrame(() => {
            if (disposed) {
              return;
            }
            startTransition(() => {
              if (parsed.type === "user:trade") {
                input.setUserTradePayload(parsed.data as UserTradePayload);
              } else {
                input.setUserPayload(parsed.data as UserPayload);
              }
              markUserActivity(receivedAt);
              warnSlowUserMessage();
            });
          });
        }
      };
      socket.onerror = () => {
        input.updateRealtimeChannel("user", { state: "reconnecting", lastError: "User stream error." }, { failure: true });
        socket.close();
      };
      socket.onclose = () => {
        if (userSocket === socket) {
          userSocket = undefined;
        }
        scheduleUserReconnect();
      };
    };

    const handleForegroundRecovery = () => {
      if (disposed) {
        return;
      }
      if (userSocket?.readyState === WebSocket.CONNECTING || userSocket?.readyState === WebSocket.CLOSING) {
        return;
      }
      if (!userSocket || userSocket.readyState === WebSocket.CLOSED || userSocket.readyState === WebSocket.CLOSING) {
        void refreshUserSnapshot();
        scheduleUserReconnect();
      }
    };

    const handleVisibilityChange = () => {
      if (document.visibilityState === "visible") {
        handleForegroundRecovery();
      }
    };

    void connectUserSocket();
    document.addEventListener("visibilitychange", handleVisibilityChange);
    window.addEventListener("focus", handleForegroundRecovery);
    window.addEventListener("pageshow", handleForegroundRecovery);
    userRuntime.setWatchdogTimer(window.setInterval(() => {
      if (disposed) {
        return;
      }
      const now = Date.now();
      const decision = evaluateRealtimeWatchdog({
        socketState: userSocket?.readyState,
        idleMs: userRuntime.frameIdleMs(now),
        fallbackCooldownMs: userFallbackCooldownMs,
        reconnectStaleMs: userSocket?.readyState === WebSocket.OPEN ? userFrameReconnectStaleMs : undefined,
        sinceLastFallbackMs: userRuntime.sinceLastFallbackMs(now),
        connectionAgeMs: userRuntime.connectionAgeMs(now),
        connectingStaleMs: userConnectingStaleMs
      });
      if (decision.shouldRefresh) {
        void refreshUserSnapshot();
      }
      if (decision.shouldReconnect) {
        scheduleUserReconnect();
      }
      if (decision.shouldClose && userSocket) {
        userSocket.close();
      }
    }, 500));

    return () => {
      disposed = true;
      userRuntime.cleanup((timer) => window.clearTimeout(timer));
      document.removeEventListener("visibilitychange", handleVisibilityChange);
      window.removeEventListener("focus", handleForegroundRecovery);
      window.removeEventListener("pageshow", handleForegroundRecovery);
      userSocket?.close();
    };
  }, [
    token,
    meId,
    activeViewUserId,
    clientInstanceId,
    input.setRealtimeStatus,
    input.updateRealtimeChannel,
    input.setUserPayload,
    input.setUserTradePayload
  ]);
}

import { startTransition, useEffect, type Dispatch, type SetStateAction } from "react";
import {
  api,
  type PublicUser,
  type UserPayload,
  type UserTradePayload
} from "../../utils/api";
import { redactNetworkAddresses } from "../../utils/redaction";
import {
  transitionRealtimeChannel,
  USER_LIVE_RECOVERY_PAYLOADS,
  type RealtimeChannelStatus,
  type RealtimeStatus
} from "../realtime/status";

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
  useEffect(() => {
    if (!input.token || !input.me) {
      return;
    }

    const activeViewUserId = input.activeViewUserId ?? input.me.id;
    const activeViewedUser = input.activeViewedUser ?? input.me;
    let disposed = false;
    let userSocket: WebSocket | undefined;
    let userReconnectTimer: number | undefined;
    let userWatchdogTimer: number | undefined;
    let refreshingUser = false;
    let lastUserMessageAt = Date.now();
    let lastUserFallbackAt = 0;
    const reconnectDelayMs = 1000;
    const userReconnectStaleMs = 12_000;
    const userFallbackCooldownMs = 5000;

    const markUserActivity = (receivedAt = Date.now()) => {
      lastUserMessageAt = receivedAt;
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
      lastUserFallbackAt = Date.now();
      input.updateRealtimeChannel("user", { state: "fallback", fallbackAt: lastUserFallbackAt }, { now: lastUserFallbackAt, failure: true });
      try {
        const [nextProfile, nextOperatedHistory, nextPositions, nextOrders, nextOrderLifecycles, nextLogs] = await Promise.all([
          api.getProfile(input.token!, activeViewUserId),
          api.getOperatedHistory(input.token!, 200, activeViewUserId),
          api.getPositions(input.token!, activeViewUserId),
          api.getOrders(input.token!, activeViewUserId),
          api.getOrderLifecycles(input.token!, activeViewUserId),
          api.getLogs(input.token!, activeViewUserId)
        ]);
        if (!disposed) {
          const receivedAt = Date.now();
          input.setUserPayload({
            viewedUserId: activeViewUserId,
            viewedUser: activeViewedUser,
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
      if (disposed || typeof userReconnectTimer === "number") {
        return;
      }
      input.setRealtimeStatus((current) => ({
        ...current,
        user: transitionRealtimeChannel(current.user, {
          state: "reconnecting",
          reconnects: current.user.reconnects + 1
        }, { failure: true })
      }));
      userReconnectTimer = window.setTimeout(() => {
        userReconnectTimer = undefined;
        void connectUserSocket();
      }, reconnectDelayMs);
    };

    const connectUserSocket = async () => {
      if (disposed) {
        return;
      }
      userSocket?.close();
      input.updateRealtimeChannel("user", { state: "connecting", lastError: undefined }, { force: true });
      let wsUrl = api.createWsUrl("/ws/user", input.token!, activeViewUserId);
      try {
        const ticket = await api.createWsTicket(input.token!, "user", activeViewUserId);
        wsUrl = api.createWsTicketUrl("/ws/user", ticket.ticket);
      } catch {
        wsUrl = api.createWsUrl("/ws/user", input.token!, activeViewUserId);
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
        const processingStartedAt = performance.now();
        let parsed: {
          type: "user" | "user:trade";
          data: UserPayload | UserTradePayload;
        };
        try {
          parsed = JSON.parse(event.data) as {
            type: "user" | "user:trade";
            data: UserPayload | UserTradePayload;
          };
        } catch (parseError) {
          input.updateRealtimeChannel("user", {
            lastError: parseError instanceof Error ? redactNetworkAddresses(parseError.message) : "Invalid user message."
          });
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
      if (!userSocket || userSocket.readyState !== WebSocket.OPEN) {
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
    userWatchdogTimer = window.setInterval(() => {
      if (disposed) {
        return;
      }
      const now = Date.now();
      if (!userSocket || userSocket.readyState !== WebSocket.OPEN) {
        if (now - lastUserFallbackAt > userFallbackCooldownMs) {
          void refreshUserSnapshot();
        }
        if (!userSocket || userSocket.readyState === WebSocket.CLOSED) {
          scheduleUserReconnect();
        }
      } else if (now - lastUserMessageAt > userReconnectStaleMs) {
        userSocket.close();
      }
    }, 500);

    return () => {
      disposed = true;
      if (typeof userReconnectTimer === "number") {
        window.clearTimeout(userReconnectTimer);
      }
      if (typeof userWatchdogTimer === "number") {
        window.clearInterval(userWatchdogTimer);
      }
      document.removeEventListener("visibilitychange", handleVisibilityChange);
      window.removeEventListener("focus", handleForegroundRecovery);
      window.removeEventListener("pageshow", handleForegroundRecovery);
      userSocket?.close();
    };
  }, [
    input.token,
    input.me,
    input.activeViewUserId,
    input.activeViewedUser,
    input.setRealtimeStatus,
    input.updateRealtimeChannel,
    input.setUserPayload,
    input.setUserTradePayload
  ]);
}

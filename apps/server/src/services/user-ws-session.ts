import { WebSocket as WsWebSocket } from "ws";
import type { UserRecord } from "../domain/types";
import type { AppStore, UserPayloadScope } from "./store";
import type { UserPayloadBuilder } from "./user-payloads";

type UserWsMetrics = {
  recordWsSend(channel: "user", bytes: number, durationMs: number, success: boolean): void;
};

export function attachUserWsSession(input: {
  socket: WsWebSocket;
  user: UserRecord;
  store: AppStore;
  payloads: UserPayloadBuilder;
  metrics: UserWsMetrics;
  onClose: () => void;
}) {
  const { metrics, onClose, payloads, socket, store, user } = input;
  const eventName = `user:${user.id}`;
  const sendPayload = (scope: UserPayloadScope = "full") => {
    const currentUser = store.getUserById(user.id);
    if (!currentUser?.isActive) {
      socket.close();
      return;
    }
    const buildStartedAt = Date.now();
    const outbound = JSON.stringify({
      type: scope === "trade" ? "user:trade" : "user",
      data: scope === "trade" ? payloads.createTradePayload(currentUser) : payloads.createFullPayload(currentUser)
    });
    const buildLatencyMs = Date.now() - buildStartedAt;
    if (buildLatencyMs > 50 || Buffer.byteLength(outbound) > 200_000) {
      console.warn(`[ws:user] payload scope=${scope} bytes=${Buffer.byteLength(outbound)} buildMs=${buildLatencyMs}`);
    }
    const sendStartedAt = Date.now();
    socket.send(outbound, (error?: Error) => {
      metrics.recordWsSend("user", Buffer.byteLength(outbound), Date.now() - sendStartedAt, !error);
    });
  };

  const listener = (scope?: UserPayloadScope) => sendPayload(scope ?? "full");
  sendPayload();
  store.emitter.on(eventName, listener);
  socket.on("close", () => {
    onClose();
    store.emitter.off(eventName, listener);
  });
}

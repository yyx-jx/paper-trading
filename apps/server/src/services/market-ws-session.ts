import { WebSocket as WsWebSocket } from "ws";
import type { MarketPayload, MarketTickPayload, UserRecord } from "../domain/types";
import type { MarketPayloadBuilder } from "./market-payloads";
import type { AppStore } from "./store";

type MarketWsMetrics = {
  recordWsSend(channel: "market", bytes: number, durationMs: number, success: boolean): void;
};

export function attachMarketWsSession(input: {
  socket: WsWebSocket;
  user: UserRecord;
  store: AppStore;
  payloads: MarketPayloadBuilder;
  metrics: MarketWsMetrics;
  minIntervalMs: number;
  retryMs: number;
  fullSnapshotIntervalMs: number;
  onClose: () => void;
}) {
  const { fullSnapshotIntervalMs, metrics, minIntervalMs, onClose, payloads, retryMs, socket, store, user } = input;
  let lastTickSentAt = 0;
  let lastFullSentAt = 0;
  let lastSentAt = 0;
  let sending = false;
  let pendingTick = false;
  let pendingFull = false;
  let pendingSince: number | undefined;
  let coalescedCount = 0;
  let retryTimer: NodeJS.Timeout | undefined;
  let tickTimer: NodeJS.Timeout | undefined;
  let fullTimer: NodeJS.Timeout | undefined;
  let closed = false;

  const isSocketOpen = () => !closed && socket.readyState === WsWebSocket.OPEN;
  const scheduleRetry = (delayMs = retryMs) => {
    if (closed || retryTimer) {
      return;
    }
    retryTimer = setTimeout(() => {
      retryTimer = undefined;
      if (pendingFull || pendingTick) {
        flushPending();
      }
    }, Math.max(delayMs, retryMs));
  };

  const deferLatest = (kind: "tick" | "full") => {
    if (kind === "full") {
      pendingFull = true;
    } else {
      pendingTick = true;
    }
    pendingSince ??= Date.now();
    coalescedCount += 1;
    const elapsedSinceLastSend = lastSentAt ? Date.now() - lastSentAt : minIntervalMs;
    const pacingDelay = Math.max(minIntervalMs - elapsedSinceLastSend, 0);
    scheduleRetry(pacingDelay);
  };

  const sendEnvelope = (
    type: "market:tick" | "market",
    data: MarketTickPayload | MarketPayload,
    kind: "tick" | "full"
  ) => {
    if (!isSocketOpen()) {
      return false;
    }
    const currentUser = store.getUserById(user.id);
    if (!currentUser?.isActive) {
      socket.close();
      return false;
    }
    if (sending || socket.bufferedAmount > 0) {
      deferLatest(kind);
      return false;
    }
    const elapsedSinceLastSend = lastSentAt ? Date.now() - lastSentAt : minIntervalMs;
    if (elapsedSinceLastSend < minIntervalMs) {
      deferLatest(kind);
      return false;
    }
    sending = true;
    const sendStartedAt = Date.now();
    data.transportMeta.wsSendStartTs = sendStartedAt;
    const outbound = JSON.stringify({ type, data });
    socket.send(outbound, (error?: Error) => {
      sending = false;
      metrics.recordWsSend("market", Buffer.byteLength(outbound), Date.now() - sendStartedAt, !error);
      if (!error) {
        lastSentAt = Date.now();
        if (kind === "tick") {
          lastTickSentAt = lastSentAt;
        } else {
          lastFullSentAt = lastSentAt;
        }
      }
      if (pendingFull || pendingTick) {
        scheduleRetry();
      }
    });
    return true;
  };

  const sendTick = () => {
    const data = payloads.createTickPayload(coalescedCount, pendingSince);
    coalescedCount = 0;
    pendingSince = undefined;
    pendingTick = false;
    return sendEnvelope("market:tick", data, "tick");
  };

  const sendFull = () => {
    const data = payloads.createMarketPayload(user.id, coalescedCount, pendingSince);
    coalescedCount = 0;
    pendingSince = undefined;
    pendingFull = false;
    return sendEnvelope("market", data, "full");
  };

  const flushPending = () => {
    if (!isSocketOpen()) {
      return;
    }
    if (pendingFull || Date.now() - lastFullSentAt >= fullSnapshotIntervalMs) {
      if (sendFull()) {
        return;
      }
    }
    if (pendingTick || Date.now() - lastTickSentAt >= minIntervalMs) {
      sendTick();
    }
  };

  const tickListener = () => {
    if (sending || socket.bufferedAmount > 0) {
      deferLatest("tick");
      return;
    }
    sendTick();
  };
  const fullListener = () => {
    if (sending || socket.bufferedAmount > 0) {
      deferLatest("full");
      return;
    }
    sendFull();
  };

  sendFull();
  tickTimer = setInterval(tickListener, Math.max(minIntervalMs, 50));
  fullTimer = setInterval(fullListener, fullSnapshotIntervalMs);
  store.emitter.on("market:update", tickListener);
  socket.on("close", () => {
    closed = true;
    onClose();
    if (retryTimer) {
      clearTimeout(retryTimer);
    }
    if (tickTimer) {
      clearInterval(tickTimer);
    }
    if (fullTimer) {
      clearInterval(fullTimer);
    }
    store.emitter.off("market:update", tickListener);
  });
}

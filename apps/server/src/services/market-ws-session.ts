import { WebSocket as WsWebSocket } from "ws";
import type { MarketBookPayload, MarketFastTickPayload, MarketPayload, MarketTickPayload, UserRecord } from "../domain/types";
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
  bookIntervalMs: number;
  retryMs: number;
  fullSnapshotIntervalMs: number;
  streamMode: "legacy" | "layered";
  onClose: () => void;
}) {
  const { bookIntervalMs, fullSnapshotIntervalMs, metrics, minIntervalMs, onClose, payloads, retryMs, socket, store, streamMode, user } = input;
  let lastTickSentAt = 0;
  let lastBookSentAt = 0;
  let lastFullSentAt = 0;
  let lastSentAt = 0;
  let sending = false;
  let pendingTick = false;
  let pendingBook = false;
  let pendingFull = false;
  let pendingSince: number | undefined;
  let coalescedCount = 0;
  let retryTimer: NodeJS.Timeout | undefined;
  let tickTimer: NodeJS.Timeout | undefined;
  let bookTimer: NodeJS.Timeout | undefined;
  let fullTimer: NodeJS.Timeout | undefined;
  let closed = false;

  const isSocketOpen = () => !closed && socket.readyState === WsWebSocket.OPEN;
  const scheduleRetry = (delayMs = retryMs) => {
    if (closed || retryTimer) {
      return;
    }
    retryTimer = setTimeout(() => {
      retryTimer = undefined;
      if (pendingFull || pendingBook || pendingTick) {
        flushPending();
      }
    }, Math.max(delayMs, retryMs));
  };

  const deferLatest = (kind: "tick" | "book" | "full") => {
    if (kind === "full") {
      pendingFull = true;
    } else if (kind === "book") {
      pendingBook = true;
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
    type: "market:tick" | "market:fast_tick" | "market:book" | "market",
    data: MarketTickPayload | MarketFastTickPayload | MarketBookPayload | MarketPayload,
    kind: "tick" | "book" | "full"
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
        } else if (kind === "book") {
          lastBookSentAt = lastSentAt;
        } else {
          lastFullSentAt = lastSentAt;
        }
      }
      if (pendingFull || pendingBook || pendingTick) {
        scheduleRetry();
      }
    });
    return true;
  };

  const sendTick = () => {
    const data =
      streamMode === "layered"
        ? payloads.createFastTickPayload(coalescedCount, pendingSince)
        : payloads.createTickPayload(coalescedCount, pendingSince);
    coalescedCount = 0;
    pendingSince = undefined;
    pendingTick = false;
    return sendEnvelope(streamMode === "layered" ? "market:fast_tick" : "market:tick", data, "tick");
  };

  const sendBook = () => {
    const data = payloads.createBookPayload(coalescedCount, pendingSince);
    coalescedCount = 0;
    pendingSince = undefined;
    pendingBook = false;
    return sendEnvelope("market:book", data, "book");
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
    if (pendingTick || Date.now() - lastTickSentAt >= minIntervalMs) {
      if (sendTick()) {
        return;
      }
    }
    if (streamMode === "layered" && (pendingBook || Date.now() - lastBookSentAt >= bookIntervalMs)) {
      if (sendBook()) {
        return;
      }
    }
    if (pendingFull || Date.now() - lastFullSentAt >= fullSnapshotIntervalMs) {
      sendFull();
    }
  };

  const tickListener = () => {
    if (sending || socket.bufferedAmount > 0) {
      deferLatest("tick");
      return;
    }
    sendTick();
  };
  const bookListener = () => {
    if (sending || socket.bufferedAmount > 0) {
      deferLatest("book");
      return;
    }
    sendBook();
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
  if (streamMode === "layered") {
    bookTimer = setInterval(bookListener, Math.max(bookIntervalMs, minIntervalMs));
  }
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
    if (bookTimer) {
      clearInterval(bookTimer);
    }
    if (fullTimer) {
      clearInterval(fullTimer);
    }
    store.emitter.off("market:update", tickListener);
  });
}

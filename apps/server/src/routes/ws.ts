import type { FastifyInstance } from "fastify";
import type { WebSocket as WsWebSocket } from "ws";
import type { UserRecord } from "../domain/types";
import type { MarketPayloadBuilder } from "../services/market-payloads";
import { attachMarketWsSession } from "../services/market-ws-session";
import type { AppStore } from "../services/store";
import type { UserPayloadBuilder } from "../services/user-payloads";
import { attachUserWsSession } from "../services/user-ws-session";

type WsMetrics = {
  setWsConnections(channel: "market" | "user", count: number): void;
  recordWsDisconnect(channel: "market" | "user", reason: string): void;
  recordWsSend(channel: "market" | "user", bytes: number, durationMs: number, success: boolean): void;
};

export function registerWsRoutes(
  app: FastifyInstance,
  context: {
    store: AppStore;
    marketPayloads: MarketPayloadBuilder;
    userPayloads: UserPayloadBuilder;
    metrics: WsMetrics;
    wsConnectionCounts: { market: number; user: number };
    marketWsMinIntervalMs: number;
    marketWsRetryMs: number;
    marketWsFullSnapshotIntervalMs: number;
    getWsUser: (query: { token?: string; ticket?: string }, channel: "market" | "user") => UserRecord | undefined;
    attachHeartbeat: (socket: WsWebSocket, channel: "market" | "user") => void;
    consumeHeartbeatTimeout: (socket: WsWebSocket) => boolean;
  }
) {
  app.get("/ws/market", { websocket: true }, (socket, request) => {
    try {
      const query = request.query as { token?: string; ticket?: string };
      const user = context.getWsUser(query, "market");
      if (!user || !user.isActive) {
        socket.close();
        return;
      }
      context.attachHeartbeat(socket, "market");
      context.wsConnectionCounts.market += 1;
      context.metrics.setWsConnections("market", context.wsConnectionCounts.market);

      attachMarketWsSession({
        socket,
        user,
        store: context.store,
        payloads: context.marketPayloads,
        metrics: context.metrics,
        minIntervalMs: context.marketWsMinIntervalMs,
        retryMs: context.marketWsRetryMs,
        fullSnapshotIntervalMs: context.marketWsFullSnapshotIntervalMs,
        onClose: () => {
          context.wsConnectionCounts.market = Math.max(0, context.wsConnectionCounts.market - 1);
          context.metrics.setWsConnections("market", context.wsConnectionCounts.market);
          if (!context.consumeHeartbeatTimeout(socket)) {
            context.metrics.recordWsDisconnect("market", "close");
          }
        }
      });
    } catch {
      socket.close();
    }
  });

  app.get("/ws/user", { websocket: true }, (socket, request) => {
    try {
      const query = request.query as { token?: string; ticket?: string };
      const user = context.getWsUser(query, "user");
      if (!user || !user.isActive) {
        socket.close();
        return;
      }
      context.attachHeartbeat(socket, "user");
      context.wsConnectionCounts.user += 1;
      context.metrics.setWsConnections("user", context.wsConnectionCounts.user);

      attachUserWsSession({
        socket,
        user,
        store: context.store,
        payloads: context.userPayloads,
        metrics: context.metrics,
        onClose: () => {
          context.wsConnectionCounts.user = Math.max(0, context.wsConnectionCounts.user - 1);
          context.metrics.setWsConnections("user", context.wsConnectionCounts.user);
          if (!context.consumeHeartbeatTimeout(socket)) {
            context.metrics.recordWsDisconnect("user", "close");
          }
        }
      });
    } catch {
      socket.close();
    }
  });
}

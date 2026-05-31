import { WebSocket as WsWebSocket } from "ws";
import type { WsChannel } from "./session";

export type HeartbeatController = {
  attachHeartbeat(socket: WsWebSocket, channel: WsChannel): void;
  consumeHeartbeatTimeout(socket: WsWebSocket): boolean;
};

export function createHeartbeatController(input: {
  heartbeatMs?: number;
  recordDisconnect?: (channel: WsChannel, reason: string) => void;
}): HeartbeatController {
  const heartbeatMs = input.heartbeatMs ?? 25_000;
  const heartbeatTimeoutSockets = new WeakSet<WsWebSocket>();

  const attachHeartbeat = (socket: WsWebSocket, channel: WsChannel) => {
    let alive = true;
    let missedPongs = 0;
    socket.on("pong", () => {
      alive = true;
      missedPongs = 0;
    });
    const timer = setInterval(() => {
      if (socket.readyState !== WsWebSocket.OPEN) {
        clearInterval(timer);
        return;
      }
      if (!alive) {
        missedPongs += 1;
        if (missedPongs >= 3) {
          heartbeatTimeoutSockets.add(socket);
          input.recordDisconnect?.(channel, "heartbeat_timeout");
          socket.close();
          clearInterval(timer);
          return;
        }
      }
      alive = false;
      socket.ping();
    }, heartbeatMs);
    socket.on("close", () => clearInterval(timer));
  };

  const consumeHeartbeatTimeout = (socket: WsWebSocket) => {
    if (!heartbeatTimeoutSockets.has(socket)) {
      return false;
    }
    heartbeatTimeoutSockets.delete(socket);
    return true;
  };

  return {
    attachHeartbeat,
    consumeHeartbeatTimeout
  };
}

export const attachHeartbeat = (controller: HeartbeatController) => controller.attachHeartbeat;

import { WebSocket as WsWebSocket } from "ws";

export function createUserHeartbeatPayload(serverNow = Date.now()) {
  return {
    type: "user:heartbeat" as const,
    data: { serverNow }
  };
}

export function sendUserHeartbeat(input: {
  socket: WsWebSocket;
  canSend?: () => boolean;
  onSend: (bytes: number, durationMs: number, ok: boolean) => void;
}) {
  if (input.socket.readyState !== WsWebSocket.OPEN || (input.canSend && !input.canSend())) {
    return false;
  }
  const outbound = JSON.stringify(createUserHeartbeatPayload());
  const bytes = Buffer.byteLength(outbound);
  const sendStartedAt = Date.now();
  input.socket.send(outbound, (error?: Error) => {
    input.onSend(bytes, Date.now() - sendStartedAt, !error);
  });
  return true;
}

export function startUserHeartbeat(input: {
  socket: WsWebSocket;
  intervalMs?: number;
  canSend: () => boolean;
  onSend: (bytes: number, durationMs: number, ok: boolean) => void;
}) {
  const intervalMs = input.intervalMs ?? 10_000;
  const timer = setInterval(() => {
    sendUserHeartbeat({
      socket: input.socket,
      canSend: input.canSend,
      onSend: input.onSend
    });
  }, intervalMs);
  input.socket.on("close", () => clearInterval(timer));
  return () => clearInterval(timer);
}

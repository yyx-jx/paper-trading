import { isMarketPayloadTooOld, MARKET_SOCKET_TIMING } from "./market-ws-message";

const socketClosedState = 3;

export function shouldRejectStaleMarketPayload(
  receivedAt: number,
  publishTs: number,
  clientClockOffsetMs: number
) {
  return isMarketPayloadTooOld(
    receivedAt,
    publishTs,
    clientClockOffsetMs,
    MARKET_SOCKET_TIMING.payloadRejectMs
  );
}

export function shouldRecoverClosedMarketSocket(socketState?: number) {
  return typeof socketState !== "number" || socketState === socketClosedState;
}

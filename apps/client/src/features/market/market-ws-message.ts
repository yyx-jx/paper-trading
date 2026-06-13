import type { MarketHistoryPatchPayload, MarketPayload, MarketTickPayload } from "../../utils/api";

export type MarketWsMessage =
  | { type: "market"; data: MarketPayload }
  | { type: "market:tick"; data: MarketTickPayload }
  | { type: "market:history-patch"; data: MarketHistoryPatchPayload };

export const MARKET_SOCKET_TIMING = {
  reconnectDelayMs: 1000,
  payloadRejectMs: 4000,
  staleMs: 3_000,
  reconnectStaleMs: 8_000,
  connectingStaleMs: 8000,
  fallbackCooldownMs: 8_000
} as const;

export function parseMarketWsMessage(data: string): MarketWsMessage {
  return JSON.parse(data) as MarketWsMessage;
}

export function extractMarketPayloadPublishTs(payload?: Pick<MarketPayload, "snapshot" | "transportMeta">) {
  if (!payload?.snapshot) {
    return 0;
  }
  if (payload.transportMeta?.serverPublishTs) {
    return payload.transportMeta.serverPublishTs;
  }
  const snapshot = payload.snapshot;
  return Math.max(
    snapshot.sources.binance.serverPublishTs,
    snapshot.sources.coinbase.serverPublishTs,
    snapshot.sources.clob.serverPublishTs
  );
}

export function marketPayloadAgeMs(receivedAt: number, publishTs: number, clientClockOffsetMs = 0) {
  return Math.max(receivedAt - publishTs - clientClockOffsetMs, 0);
}

export function isMarketPayloadTooOld(
  receivedAt: number,
  publishTs: number,
  clientClockOffsetMs: number,
  rejectMs: number
) {
  if (publishTs <= 0) {
    return false;
  }
  return marketPayloadAgeMs(receivedAt, publishTs, clientClockOffsetMs) > rejectMs;
}

export function createMarketFallbackPayload(input: {
  roundData: {
    viewedUserId?: string;
    currentRound?: MarketPayload["currentRound"];
    snapshot: MarketPayload["snapshot"];
    settlementPreview?: MarketPayload["settlementPreview"];
    transportMeta?: MarketPayload["transportMeta"];
  };
  activeViewUserId: string;
  history: MarketPayload["history"];
}): MarketPayload {
  return {
    viewedUserId: input.roundData.viewedUserId ?? input.activeViewUserId,
    currentRound: input.roundData.currentRound,
    history: input.history,
    snapshot: input.roundData.snapshot,
    settlementPreview: input.roundData.settlementPreview,
    transportMeta: input.roundData.transportMeta
  };
}

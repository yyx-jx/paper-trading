import type { SourceHealth } from "../../utils/api";

export type SourceLatency = {
  sourceToBackendLatencyMs: number;
  backendToFrontendLatencyMs: number | undefined;
  endToEndLatencyMs: number | undefined;
  dataAgeMs: number;
  sourceDataAgeMs: number;
  marketUpdateAgeMs: number;
  disabled: boolean;
};

export function serverClockNowMs(clientNow = Date.now(), clientClockOffsetMs = 0) {
  return clientNow - clientClockOffsetMs;
}

export function transportAgeMs(receivedAt: number, publishTs: number, clientClockOffsetMs = 0) {
  return Math.max(receivedAt - publishTs - clientClockOffsetMs, 0);
}

export function latencyForSource(
  source?: SourceHealth,
  now = Date.now(),
  clientRecvTs?: number,
  clientClockOffsetMs = 0
): SourceLatency {
  if (!source || source.state === "disabled") {
    return {
      sourceToBackendLatencyMs: 0,
      backendToFrontendLatencyMs: undefined,
      endToEndLatencyMs: undefined,
      dataAgeMs: 0,
      sourceDataAgeMs: 0,
      marketUpdateAgeMs: 0,
      disabled: true
    };
  }

  const backendToFrontendLatencyMs =
    typeof source.clientRecvTs === "number"
      ? transportAgeMs(source.clientRecvTs, source.serverPublishTs, clientClockOffsetMs)
      : typeof clientRecvTs === "number"
        ? transportAgeMs(clientRecvTs, source.serverPublishTs, clientClockOffsetMs)
        : typeof source.frontendLatencyMs === "number"
          ? Math.max(source.frontendLatencyMs, 0)
          : undefined;
  const adjustedServerNow = serverClockNowMs(now, clientClockOffsetMs);

  return {
    sourceToBackendLatencyMs: Math.max(source.acquireLatencyMs, 0),
    backendToFrontendLatencyMs,
    endToEndLatencyMs:
      typeof backendToFrontendLatencyMs === "number"
        ? Math.max(source.serverPublishTs - source.sourceEventTs + backendToFrontendLatencyMs, 0)
        : undefined,
    dataAgeMs: Math.max(adjustedServerNow - source.normalizedTs, 0),
    sourceDataAgeMs: Math.max(adjustedServerNow - source.normalizedTs, 0),
    marketUpdateAgeMs:
      typeof source.clientRecvTs === "number"
        ? Math.max(now - source.clientRecvTs, 0)
        : typeof clientRecvTs === "number"
          ? Math.max(now - clientRecvTs, 0)
          : 0,
    disabled: false
  };
}

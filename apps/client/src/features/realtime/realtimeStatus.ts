import type { Language } from "../../utils/api";
import { localLabel } from "../../utils/format";

export type RealtimeChannel = "market" | "user";
export type RealtimeChannelState = "connecting" | "live" | "reconnecting" | "fallback" | "offline";

export interface RealtimeChannelStatus {
  state: RealtimeChannelState;
  lastMessageAt?: number;
  fallbackAt?: number;
  reconnects: number;
  stateChangedAt: number;
  livePayloads: number;
  consecutiveFailures: number;
  lastError?: string;
}

export type RealtimeStatus = Record<RealtimeChannel, RealtimeChannelStatus>;

export const REALTIME_STATUS_MIN_HOLD_MS = 1500;
export const REALTIME_FAILURES_BEFORE_DEGRADE = 2;
export const MARKET_LIVE_RECOVERY_PAYLOADS = 2;
export const USER_LIVE_RECOVERY_PAYLOADS = 1;

export const initialRealtimeStatus = (): RealtimeStatus => ({
  market: { state: "connecting", reconnects: 0, stateChangedAt: Date.now(), livePayloads: 0, consecutiveFailures: 0 },
  user: { state: "connecting", reconnects: 0, stateChangedAt: Date.now(), livePayloads: 0, consecutiveFailures: 0 }
});

export function transitionRealtimeChannel(
  current: RealtimeChannelStatus,
  patch: Partial<RealtimeChannelStatus>,
  options: { now?: number; force?: boolean; failure?: boolean; recoverPayloads?: number } = {}
): RealtimeChannelStatus {
  const now = options.now ?? Date.now();
  const currentStateChangedAt = current.stateChangedAt || now;
  let next: RealtimeChannelStatus = {
    ...current,
    ...patch,
    stateChangedAt: currentStateChangedAt,
    livePayloads: patch.livePayloads ?? current.livePayloads ?? 0,
    consecutiveFailures: patch.consecutiveFailures ?? current.consecutiveFailures ?? 0
  };

  if (options.failure) {
    next = {
      ...next,
      livePayloads: 0,
      consecutiveFailures: (current.consecutiveFailures ?? 0) + 1
    };
  }

  if (patch.state === "live") {
    const livePayloads = (current.state === "live" ? current.livePayloads : current.livePayloads + 1) || 1;
    next = {
      ...next,
      livePayloads,
      consecutiveFailures: 0
    };
    const requiredPayloads = options.recoverPayloads ?? 1;
    if (current.state !== "live" && livePayloads < requiredPayloads && !options.force) {
      return {
        ...next,
        state: current.state,
        stateChangedAt: currentStateChangedAt
      };
    }
  }

  if (
    current.state === "live" &&
    patch.state &&
    patch.state !== "live" &&
    !options.force &&
    (next.consecutiveFailures < REALTIME_FAILURES_BEFORE_DEGRADE || now - currentStateChangedAt < REALTIME_STATUS_MIN_HOLD_MS)
  ) {
    return {
      ...next,
      state: "live",
      stateChangedAt: currentStateChangedAt
    };
  }

  if (patch.state && patch.state !== current.state) {
    next.stateChangedAt = now;
  }
  return next;
}

export function realtimeStatusLabel(status: RealtimeStatus, language: Language) {
  const states = [status.market.state, status.user.state];
  if (states.includes("offline")) {
    return localLabel(language, "后端离线", "Backend offline");
  }
  if (states.includes("fallback")) {
    return localLabel(language, "兜底刷新中", "Fallback refresh");
  }
  if (states.includes("reconnecting") || states.includes("connecting")) {
    return localLabel(language, "重连中", "Reconnecting");
  }
  return localLabel(language, "实时连接中", "Live");
}

export function realtimeStatusTone(status: RealtimeStatus) {
  const states = [status.market.state, status.user.state];
  if (states.includes("offline")) {
    return "offline";
  }
  if (states.includes("fallback")) {
    return "fallback";
  }
  if (states.includes("reconnecting") || states.includes("connecting")) {
    return "reconnecting";
  }
  return "live";
}

export function realtimeStatusDetail(status: RealtimeStatus, nowMs: number, language: Language) {
  const ageText = (at?: number) => (at ? `${Math.max(0, Math.round((nowMs - at) / 1000))}s` : "--");
  return localLabel(
    language,
    `行情 ${ageText(status.market.lastMessageAt)} / 用户 ${ageText(status.user.lastMessageAt)}`,
    `Market ${ageText(status.market.lastMessageAt)} / User ${ageText(status.user.lastMessageAt)}`
  );
}

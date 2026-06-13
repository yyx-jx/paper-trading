export interface RealtimeWatchdogInput {
  socketState?: number;
  idleMs: number;
  acceptedIdleMs?: number;
  fallbackCooldownMs: number;
  reconnectStaleMs?: number;
  staleMs?: number;
  sinceLastFallbackMs: number;
  connectionAgeMs?: number;
  connectingStaleMs?: number;
}

export interface RealtimeWatchdogDecision {
  shouldRefresh: boolean;
  shouldReconnect: boolean;
  shouldClose: boolean;
}

const socketOpenState = 1;
const socketConnectingState = 0;
const socketClosingState = 2;
const socketClosedState = 3;

export function evaluateRealtimeWatchdog(input: RealtimeWatchdogInput): RealtimeWatchdogDecision {
  const socketIsOpen = input.socketState === socketOpenState;
  const socketIsConnecting = input.socketState === socketConnectingState;
  const socketIsClosed = typeof input.socketState !== "number" || input.socketState === socketClosedState;
  const fallbackReady = input.sinceLastFallbackMs > input.fallbackCooldownMs;
  const acceptedIdleMs = typeof input.acceptedIdleMs === "number" ? input.acceptedIdleMs : input.idleMs;
  const staleForRefresh = typeof input.staleMs === "number" && acceptedIdleMs > input.staleMs;
  const staleForReconnect =
    typeof input.reconnectStaleMs === "number" &&
    acceptedIdleMs > input.reconnectStaleMs &&
    input.idleMs > input.reconnectStaleMs;
  const connectingTimedOut =
    typeof input.connectingStaleMs === "number" &&
    typeof input.connectionAgeMs === "number" &&
    input.connectionAgeMs > input.connectingStaleMs;

  if (socketIsOpen) {
    return {
      shouldRefresh: staleForRefresh && fallbackReady,
      shouldReconnect: false,
      shouldClose: staleForReconnect
    };
  }

  if (socketIsConnecting) {
    return {
      shouldRefresh: false,
      shouldReconnect: false,
      shouldClose: connectingTimedOut
    };
  }

  if (input.socketState === socketClosingState) {
    return {
      shouldRefresh: false,
      shouldReconnect: false,
      shouldClose: false
    };
  }

  return {
    shouldRefresh: fallbackReady,
    shouldReconnect: socketIsClosed,
    shouldClose: false
  };
}

export function createRealtimeSocketRuntime(now: () => number = () => Date.now()) {
  let lastFrameAt = now();
  let lastAcceptedAt = lastFrameAt;
  let lastConnectAttemptAt = lastFrameAt;
  let lastFallbackAt = 0;
  let reconnectTimer: number | undefined;
  let watchdogTimer: number | undefined;
  let lastRejectReason: string | undefined;

  return {
    markFrame(receivedAt = now()) {
      lastFrameAt = receivedAt;
      return receivedAt;
    },
    markAccepted(receivedAt = now()) {
      lastAcceptedAt = receivedAt;
      lastRejectReason = undefined;
      return receivedAt;
    },
    markConnectAttempt(at = now()) {
      lastConnectAttemptAt = at;
      return at;
    },
    markFallback(fallbackAt = now()) {
      lastFallbackAt = fallbackAt;
      return fallbackAt;
    },
    markRejected(reason: string) {
      lastRejectReason = reason;
    },
    frameIdleMs(at = now()) {
      return at - lastFrameAt;
    },
    acceptedIdleMs(at = now()) {
      return at - lastAcceptedAt;
    },
    sinceLastFallbackMs(at = now()) {
      return at - lastFallbackAt;
    },
    connectionAgeMs(at = now()) {
      return at - lastConnectAttemptAt;
    },
    getDiagnostics() {
      return {
        lastFrameAt,
        lastAcceptedAt,
        lastConnectAttemptAt,
        lastFallbackAt,
        lastRejectReason
      };
    },
    setReconnectTimer(timer: number | undefined) {
      reconnectTimer = timer;
    },
    getReconnectTimer() {
      return reconnectTimer;
    },
    setWatchdogTimer(timer: number | undefined) {
      watchdogTimer = timer;
    },
    cleanup(clearTimer: (timer: number) => void) {
      if (typeof reconnectTimer === "number") {
        clearTimer(reconnectTimer);
        reconnectTimer = undefined;
      }
      if (typeof watchdogTimer === "number") {
        clearTimer(watchdogTimer);
        watchdogTimer = undefined;
      }
    }
  };
}

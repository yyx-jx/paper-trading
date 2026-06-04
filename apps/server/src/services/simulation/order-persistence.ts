export type TradePersistSegmentName =
  | "persistOrderBookSnapshot"
  | "persistOrder"
  | "persistPosition"
  | "persistUser"
  | "persistOrderLifecycle"
  | "commitAndOverhead"
  | "transactionTotal";

export type TradePersistSegments = Partial<Record<TradePersistSegmentName, number>>;

export type TradePersistStep = {
  name: TradePersistSegmentName;
  run: () => Promise<unknown> | unknown;
};

export type TradePersistObserver = {
  onSegmentObserved?: (segment: TradePersistSegmentName, durationMs: number) => void;
};

export async function measureTradePersistSegment<T>(
  segments: TradePersistSegments | undefined,
  name: TradePersistSegmentName,
  handler: () => Promise<T> | T,
  observer?: TradePersistObserver
) {
  const startedAt = Date.now();
  try {
    return await handler();
  } finally {
    const durationMs = Math.max(Date.now() - startedAt, 0);
    if (segments) {
      segments[name] = (segments[name] ?? 0) + durationMs;
    }
    observer?.onSegmentObserved?.(name, durationMs);
  }
}

export async function persistTradeStepsSequentially(
  segments: TradePersistSegments | undefined,
  steps: TradePersistStep[],
  observer?: TradePersistObserver
) {
  for (const step of steps) {
    await measureTradePersistSegment(segments, step.name, step.run, observer);
  }
}

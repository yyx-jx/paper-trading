export const CHART_VISIBLE_COUNT_MIN = 10;
export const CHART_VISIBLE_COUNT_MAX = 200;
export const CHART_Y_ZOOM_MIN = 0.45;
export const CHART_Y_ZOOM_MAX = 4;
export const CHART_PRICE_LABEL_HEIGHT = 19;
export const CHART_PRICE_LABEL_BASELINE_TOP_OFFSET = 13;
export const CHART_PRICE_LABEL_GAP = 4;

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(value, max));
}

export function chartWheelStep(deltaY: number) {
  if (!Number.isFinite(deltaY) || deltaY === 0) {
    return 0;
  }
  return Math.max(1, Math.min(6, Math.round(Math.abs(deltaY) / 80) || 1));
}

export function nextChartVisibleCount(currentCount: number, deltaY: number) {
  const step = chartWheelStep(deltaY);
  if (step === 0) {
    return clamp(Math.round(currentCount), CHART_VISIBLE_COUNT_MIN, CHART_VISIBLE_COUNT_MAX);
  }
  return clamp(
    Math.round(currentCount + (deltaY > 0 ? 10 : -10) * step),
    CHART_VISIBLE_COUNT_MIN,
    CHART_VISIBLE_COUNT_MAX
  );
}

export function nextChartYZoom(currentZoom: number, deltaY: number) {
  const step = chartWheelStep(deltaY);
  if (step === 0) {
    return clamp(currentZoom, CHART_Y_ZOOM_MIN, CHART_Y_ZOOM_MAX);
  }
  let nextZoom = currentZoom;
  for (let index = 0; index < step; index += 1) {
    nextZoom *= deltaY < 0 ? 1.12 : 0.88;
  }
  return clamp(nextZoom, CHART_Y_ZOOM_MIN, CHART_Y_ZOOM_MAX);
}

export function layoutChartPriceLabels(input: {
  targetY?: number;
  latestY?: number;
  plotTop: number;
  plotBottom: number;
}) {
  const baselineBottomOffset = CHART_PRICE_LABEL_HEIGHT - CHART_PRICE_LABEL_BASELINE_TOP_OFFSET;
  const minBaseline = input.plotTop + CHART_PRICE_LABEL_BASELINE_TOP_OFFSET;
  const maxBaseline = Math.max(minBaseline, input.plotBottom - baselineBottomOffset);
  const minBaselineGap = CHART_PRICE_LABEL_HEIGHT + CHART_PRICE_LABEL_GAP;
  const hasTarget = typeof input.targetY === "number" && Number.isFinite(input.targetY);
  const hasLatest = typeof input.latestY === "number" && Number.isFinite(input.latestY);
  const clampBaseline = (value: number) => clamp(value, minBaseline, maxBaseline);

  if (!hasTarget && !hasLatest) {
    return {};
  }

  if (!hasTarget) {
    return { latestLabelY: clampBaseline(input.latestY! + 14) };
  }

  if (!hasLatest) {
    return { targetLabelY: clampBaseline(input.targetY! - 6) };
  }

  let targetLabelY = clampBaseline(input.targetY! - 6);
  let latestLabelY = clampBaseline(input.latestY! + 14);
  if (latestLabelY - targetLabelY < minBaselineGap) {
    const midpoint = (targetLabelY + latestLabelY) / 2;
    targetLabelY = midpoint - minBaselineGap / 2;
    latestLabelY = midpoint + minBaselineGap / 2;
    if (targetLabelY < minBaseline) {
      latestLabelY += minBaseline - targetLabelY;
      targetLabelY = minBaseline;
    }
    if (latestLabelY > maxBaseline) {
      targetLabelY -= latestLabelY - maxBaseline;
      latestLabelY = maxBaseline;
    }
    targetLabelY = clampBaseline(targetLabelY);
    latestLabelY = clampBaseline(Math.max(latestLabelY, targetLabelY + minBaselineGap));
  }

  return { targetLabelY, latestLabelY };
}

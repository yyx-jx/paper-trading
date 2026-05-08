import type { PositionRecord } from "../../utils/api";

const roundMoney = (value: number) => Math.round(value * 100) / 100;

export function positionMarkPnl(position: PositionRecord) {
  if (typeof position.markPnlUsdc === "number") {
    return position.markPnlUsdc;
  }
  if (position.displayStatus === "open") {
    return position.unrealizedPnl;
  }
  return position.realizedPnl;
}

export function positionExecutablePnl(position: PositionRecord) {
  if (typeof position.executablePnlUsdc === "number") {
    return position.executablePnlUsdc;
  }
  if (position.displayStatus === "open") {
    const bidValue =
      typeof position.currentBid === "number"
        ? position.currentBid * position.qty
        : position.currentValue ?? position.qty * position.currentMark;
    const costBasis = position.costBasisUsdc ?? position.notionalSpent;
    return roundMoney(bidValue - costBasis);
  }
  return position.realizedPnl;
}

export function positionTotalFees(position: PositionRecord) {
  if (typeof position.totalFeeUsdc === "number") {
    return position.totalFeeUsdc;
  }
  return (position.entryFeeUsdc ?? 0) + (position.exitFeeUsdc ?? 0);
}

export function positionDisplayedPnl(position: PositionRecord) {
  return position.displayStatus === "open" ? positionMarkPnl(position) : position.realizedPnl;
}

export function summarizePositionPnl(positions: PositionRecord[]) {
  return positions.reduce(
    (summary, position) => ({
      markPnlUsdc: summary.markPnlUsdc + positionMarkPnl(position),
      executablePnlUsdc: summary.executablePnlUsdc + positionExecutablePnl(position),
      entryFeeUsdc: summary.entryFeeUsdc + (position.entryFeeUsdc ?? 0),
      exitFeeUsdc: summary.exitFeeUsdc + (position.exitFeeUsdc ?? 0),
      totalFeeUsdc: summary.totalFeeUsdc + positionTotalFees(position),
      costBasisUsdc: summary.costBasisUsdc + (position.costBasisUsdc ?? position.notionalSpent)
    }),
    {
      markPnlUsdc: 0,
      executablePnlUsdc: 0,
      entryFeeUsdc: 0,
      exitFeeUsdc: 0,
      totalFeeUsdc: 0,
      costBasisUsdc: 0
    }
  );
}

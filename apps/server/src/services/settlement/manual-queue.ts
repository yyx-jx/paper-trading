import type { ManualSettlementCandidate, OrderRecord, PositionRecord, RoundRecord } from "../../domain/types";
import { roundNumber } from "../simulation/trade-calculations";

export function shouldRequireManualSettlement(
  round: RoundRecord,
  now: number,
  timeoutMs: number,
  pollDelayMs: number
) {
  if (round.settledSide || round.redeemFinishTs) {
    return false;
  }
  if (round.status === "Closed" || round.status === "Redeeming" || round.status === "Settled" || round.status === "Manual") {
    return false;
  }
  return now >= round.endAt + Math.max(pollDelayMs, 0) + Math.max(timeoutMs, 0);
}

export function buildManualSettlementCandidates(
  rounds: RoundRecord[],
  positions: PositionRecord[],
  orders: OrderRecord[],
  limit: number
): ManualSettlementCandidate[] {
  const normalizedLimit = Math.max(0, Math.floor(limit));
  if (normalizedLimit === 0) {
    return [];
  }

  const candidates = rounds
    .filter((round) => round.status === "Manual" && !round.settledSide && !round.redeemFinishTs)
    .sort((left, right) => right.endAt - left.endAt)
    .slice(0, normalizedLimit);

  return candidates.map((round) => {
    const openPositions = positions.filter((position) => position.roundId === round.id && position.status === "open");
    const pendingOrders = orders.filter((order) => order.roundId === round.id && order.status === "pending");
    const participants = new Set(openPositions.map((position) => position.userId));
    for (const order of pendingOrders) {
      participants.add(order.userId);
    }
    return {
      roundId: round.id,
      symbol: round.symbol,
      marketId: round.marketId,
      marketSlug: round.marketSlug,
      title: round.title,
      startAt: round.startAt,
      endAt: round.endAt,
      status: round.status,
      pollCount: round.pollCount,
      pollStartAt: round.pollStartAt,
      lastPollAt: round.lastPollAt,
      manualReason: round.manualReason,
      participantCount: participants.size,
      openPositionCount: openPositions.length,
      pendingOrderCount: pendingOrders.length,
      upOpenQty: roundNumber(
        openPositions.filter((position) => position.side === "UP").reduce((sum, position) => sum + Math.max(position.qty, 0), 0),
        4
      ),
      downOpenQty: roundNumber(
        openPositions.filter((position) => position.side === "DOWN").reduce((sum, position) => sum + Math.max(position.qty, 0), 0),
        4
      )
    };
  });
}

import type { OrderRecord, PositionRecord, ProfileOverview, UserRecord } from "../../domain/types";

type ProfilePosition = PositionRecord & { displayStatus?: "open" | "sold" | "settled" | "pending_settlement" };

const emptyProfileOverview = (): ProfileOverview => ({
  totalEquity: 0,
  availableUsdc: 0,
  positionValue: 0,
  realizedPnlToday: 0,
  unrealizedPnl: 0,
  winRate: 0,
  roundsParticipatedTotal: 0,
  roundsParticipatedToday: 0
});

export function buildProfileOverview(
  user: UserRecord | undefined,
  positions: ProfilePosition[],
  userOrders: OrderRecord[]
): ProfileOverview {
  if (!user) {
    return emptyProfileOverview();
  }

  const livePositions = positions.filter((position) => position.displayStatus === "open");
  const positionValue = livePositions.reduce((sum, position) => sum + (position.currentValue ?? position.qty * position.currentMark), 0);

  const todayStart = new Date();
  todayStart.setUTCHours(0, 0, 0, 0);
  const todayTs = todayStart.getTime();

  const realizedPnlToday = positions
    .filter((position) => (position.closedAt ?? position.openedAt) >= todayTs)
    .reduce((sum, position) => sum + position.realizedPnl, 0);
  const unrealizedPnl = livePositions.reduce((sum, position) => sum + position.unrealizedPnl, 0);
  const settledPositions = positions.filter(
    (position) =>
      position.status === "closed" &&
      (position.settlementResult === "win" || position.settlementResult === "loss")
  );
  const wins = settledPositions.filter((position) => position.settlementResult === "win").length;
  const roundsParticipatedTotal = new Set([
    ...positions.map((position) => position.roundId),
    ...userOrders.map((order) => order.roundId)
  ]).size;
  const roundsParticipatedToday = new Set([
    ...positions
      .filter((position) => position.openedAt >= todayTs || (position.closedAt ?? 0) >= todayTs)
      .map((position) => position.roundId),
    ...userOrders.filter((order) => order.createdAt >= todayTs).map((order) => order.roundId)
  ]).size;

  return {
    totalEquity: Number((user.availableUsdc + positionValue).toFixed(2)),
    availableUsdc: Number(user.availableUsdc.toFixed(2)),
    positionValue: Number(positionValue.toFixed(2)),
    realizedPnlToday: Number(realizedPnlToday.toFixed(2)),
    unrealizedPnl: Number(unrealizedPnl.toFixed(2)),
    winRate: settledPositions.length > 0 ? wins / settledPositions.length : 0,
    roundsParticipatedTotal,
    roundsParticipatedToday
  };
}

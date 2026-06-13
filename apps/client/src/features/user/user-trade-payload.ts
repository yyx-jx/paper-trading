import type { OrderLifecycleRecord, OrderRecord, PositionRecord, UserTradePayload } from "../../utils/api";

type UserTradeMergeState = {
  viewedUserId?: string;
  profile?: UserTradePayload["profile"];
  positions: PositionRecord[];
  orders: OrderRecord[];
  orderLifecycles: OrderLifecycleRecord[];
};

function mergeRecordsByKey<T>(current: T[], updates: T[], getKey: (item: T) => string, limit?: number) {
  const byId = new Map<string, T>();
  for (const item of current) {
    byId.set(getKey(item), item);
  }
  for (const item of updates) {
    byId.set(getKey(item), item);
  }
  const merged = [...byId.values()];
  return typeof limit === "number" ? merged.slice(0, limit) : merged;
}

function mergePositions(current: PositionRecord[], updates: PositionRecord[], mode: UserTradePayload["positionsMode"]) {
  if (mode !== "delta") {
    return updates;
  }
  return mergeRecordsByKey(current, updates, (position) => position.id).sort((left, right) => right.openedAt - left.openedAt);
}

function mergeOrders(current: OrderRecord[], updates: OrderRecord[]) {
  return mergeRecordsByKey(current, updates, (order) => order.id, 500).sort((left, right) => right.createdAt - left.createdAt);
}

function mergeOrderLifecycles(current: OrderLifecycleRecord[], updates: OrderLifecycleRecord[]) {
  return mergeRecordsByKey(current, updates, (log) => log.id, 500).sort(
    (left, right) => right.orderTimestampMs - left.orderTimestampMs
  );
}

export function mergeUserTradePayload(state: UserTradeMergeState, data: UserTradePayload): UserTradeMergeState {
  return {
    viewedUserId: data.viewedUserId,
    profile: data.profile,
    positions: mergePositions(state.positions, data.positions, data.positionsMode),
    orders: mergeOrders(state.orders, data.orders),
    orderLifecycles: mergeOrderLifecycles(state.orderLifecycles, data.orderLifecycles)
  };
}

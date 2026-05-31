import type { AppStore } from "../services/store";
import type { UserRecord } from "../domain/types";
import type { MarketPayloadBuilder } from "./market";

const USER_TRADE_ORDER_LIMIT = 50;
const USER_TRADE_LIFECYCLE_LIMIT = 80;

export type UserPayloadBuilder = ReturnType<typeof createUserPayloadBuilder>;

export function createUserPayloadBuilder(input: {
  store: AppStore;
  marketPayloads: Pick<MarketPayloadBuilder, "getOperatedHistoryWithSettlementPreview">;
}) {
  const createUserFullPayload = (user: UserRecord) => ({
    viewedUserId: user.id,
    viewedUser: input.store.sanitizeUser(user),
    profile: input.store.getProfile(user.id),
    operatedHistory: input.marketPayloads.getOperatedHistoryWithSettlementPreview(500, user.id),
    positions: input.store.getPositions(user.id),
    orders: input.store.getOrders(user.id),
    orderLifecycles: input.store.getOrderLifecycleLogs(user.id),
    logs: input.store.getRecentLogs(user.id)
  });

  const createUserTradePayload = (user: UserRecord) => ({
    viewedUserId: user.id,
    profile: input.store.getProfile(user.id),
    positions: input.store.getPositions(user.id),
    orders: input.store.getRecentTradeOrders(user.id, USER_TRADE_ORDER_LIMIT),
    orderLifecycles: input.store.getOrderLifecycleLogs(user.id).slice(0, USER_TRADE_LIFECYCLE_LIMIT)
  });

  return {
    createUserFullPayload,
    createUserTradePayload
  };
}

export function createUserTradePayload(builder: UserPayloadBuilder, user: UserRecord) {
  return builder.createUserTradePayload(user);
}

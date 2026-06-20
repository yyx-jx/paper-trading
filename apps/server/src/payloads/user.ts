import type { AppStore } from "../services/store";
import type { UserRecord } from "../domain/types";
import type { MarketPayloadBuilder } from "./market";
import { USER_HISTORY_PAGE_SIZE } from "../http/history-pagination";
import {
  USER_TRADE_LIFECYCLE_LIMIT,
  USER_TRADE_ORDER_LIMIT,
  toTradeLifecyclePayload,
  toTradeOrderPayload
} from "./user-trade";

export type UserPayloadBuilder = ReturnType<typeof createUserPayloadBuilder>;
type UserTradePayloadOptions = {
  positionIds?: string[];
};

export function createUserPayloadBuilder(input: {
  store: AppStore;
  marketPayloads: Pick<MarketPayloadBuilder, "getOperatedHistoryWithSettlementPreview">;
}) {
  const createUserFullPayload = (user: UserRecord) => ({
    viewedUserId: user.id,
    viewedUser: input.store.sanitizeUser(user),
    profile: input.store.getProfile(user.id),
    operatedHistory: input.marketPayloads.getOperatedHistoryWithSettlementPreview(200, user.id),
    positions: input.store.getPositions(user.id),
    orders: input.store.getOrders(user.id, { limit: USER_HISTORY_PAGE_SIZE }),
    orderLifecycles: input.store.getOrderLifecycleLogs(user.id, { limit: USER_HISTORY_PAGE_SIZE }),
    logs: input.store.getRecentLogs(user.id, { limit: USER_HISTORY_PAGE_SIZE })
  });

  const createUserTradePayload = (user: UserRecord, options?: UserTradePayloadOptions) => ({
    viewedUserId: user.id,
    profile: input.store.getProfile(user.id),
    positionsMode: options?.positionIds ? ("delta" as const) : ("replace" as const),
    positions: input.store.getTradePositions(user.id, options?.positionIds),
    orders: input.store.getRecentTradeOrders(user.id, USER_TRADE_ORDER_LIMIT).map(toTradeOrderPayload),
    orderLifecycles: input.store.getOrderLifecycleLogs(user.id, { limit: USER_TRADE_LIFECYCLE_LIMIT }).map(toTradeLifecyclePayload)
  });

  return {
    createUserFullPayload,
    createUserTradePayload
  };
}

export function createUserTradePayload(builder: UserPayloadBuilder, user: UserRecord, options?: UserTradePayloadOptions) {
  return builder.createUserTradePayload(user, options);
}

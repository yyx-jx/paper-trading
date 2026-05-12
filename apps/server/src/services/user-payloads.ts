import type { UserPayload, UserRecord, UserTradePayload } from "../domain/types";
import type { MarketPayloadBuilder } from "./market-payloads";
import type { AppStore } from "./store";

export class UserPayloadBuilder {
  constructor(
    private readonly options: {
      store: AppStore;
      marketPayloads: MarketPayloadBuilder;
    }
  ) {}

  createFullPayload(user: UserRecord): UserPayload {
    const { marketPayloads, store } = this.options;
    return {
      profile: store.getProfile(user.id),
      operatedHistory: marketPayloads.getOperatedHistoryWithSettlementPreview(500, user.id),
      positions: store.getPositions(user.id),
      orders: store.getOrders(user.id),
      logs: store.getRecentLogs(user.id)
    };
  }

  createTradePayload(user: UserRecord): UserTradePayload {
    const { store } = this.options;
    return {
      profile: store.getProfile(user.id),
      positions: store.getPositions(user.id),
      orders: store.getRecentTradeOrders(user.id, 50)
    };
  }
}

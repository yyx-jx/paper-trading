import i18n from "../../i18n";
import { useRef, useState, type Dispatch, type SetStateAction } from "react";
import {
  api,
  type Language,
  type OrderAction,
  type PaperOrderKind,
  type ProfileOverview,
  type PublicUser,
  type TradeSide,
  type UserTradePayload
} from "../../utils/api";
import { money } from "../../utils/format";
const t = (key: string, options?: Record<string, unknown>) => i18n.t(key, options);


function parseLimitPriceCentsInput(value: string) {
  const trimmed = value.trim();
  if (!/^\d{1,2}$/.test(trimmed)) {
    return undefined;
  }
  const cents = Number(trimmed);
  return Number.isInteger(cents) && cents >= 1 && cents <= 99 ? cents : undefined;
}

function createClientOrderId() {
  return globalThis.crypto?.randomUUID
    ? globalThis.crypto.randomUUID()
    : `client_${Date.now()}_${Math.random().toString(36).slice(2)}`;
}

export function useOrderActions(input: {
  token?: string;
  me?: PublicUser;
  isViewingSelf: boolean;
  language: Language;
  profile?: ProfileOverview;
  orderAmount: string;
  orderQty: string;
  limitPrice: string;
  orderAction: OrderAction;
  selectedSide: TradeSide;
  orderKind: PaperOrderKind;
  setSelectedSide: Dispatch<SetStateAction<TradeSide>>;
  setError: Dispatch<SetStateAction<string | undefined>>;
  setUserTradePayload: (data: UserTradePayload) => void;
  setLastOrderLatencyMs: (latency?: number) => void;
}) {
  const [tradeBusy, setTradeBusy] = useState(false);
  const [quickBusy, setQuickBusy] = useState(false);
  const [cancelBusyOrderId, setCancelBusyOrderId] = useState<string>();
  const [sellBusyPositionId, setSellBusyPositionId] = useState<string>();
  const [sellFeedback, setSellFeedback] = useState<{ positionId?: string; message: string }>();
  const [pendingOrderClientId, setPendingOrderClientId] = useState<string>();
  const cancellingOrderIdsRef = useRef(new Set<string>());

  const ensureViewingSelfForMutation = () => {
    if (input.isViewingSelf) {
      return true;
    }
    input.setError(t("uiTradingActionsAreLockedWhileViewing307d89df"));
    return false;
  };

  const handlePlaceOrder = async () => {
    if (!input.token || !input.me || !ensureViewingSelfForMutation()) {
      return;
    }
    const limitPriceCents = input.orderKind === "limit" ? parseLimitPriceCentsInput(input.limitPrice) : undefined;
    if (input.orderKind === "limit" && typeof limitPriceCents !== "number") {
      input.setError(t("uiLimitOrdersOnlySupportWholeCent8ea4aa71"));
      return;
    }
    const clientOrderId = createClientOrderId();
    setPendingOrderClientId(clientOrderId);
    setTradeBusy(true);
    input.setError(undefined);
    try {
      const result = await api.placeOrder(input.token, {
        action: input.orderAction,
        side: input.selectedSide,
        orderKind: input.orderKind,
        amount: input.orderAction === "buy" ? Number(input.orderAmount) : undefined,
        qty: input.orderAction === "sell" ? Number(input.orderQty) : undefined,
        limitPrice: input.orderKind === "limit" ? limitPriceCents! / 100 : undefined,
        clientOrderId
      });
      if (result.tradePatch) {
        input.setUserTradePayload(result.tradePatch);
      }
      input.setLastOrderLatencyMs(result.order.totalOrderLatencyMs ?? result.order.matchLatencyMs);
    } catch (placeOrderError) {
      const message = placeOrderError instanceof Error ? placeOrderError.message : "Order failed.";
      if (message.includes("Insufficient virtual balance")) {
        input.setError(
          t("uiInsufficientAvailableBalanceThisOrderWould6c2e619d", { p0: money(Number(input.orderAmount || 0)), p1: money(input.profile?.availableUsdc ?? 0) })
        );
      } else {
        input.setError(message);
      }
    } finally {
      setTradeBusy(false);
      setPendingOrderClientId(undefined);
    }
  };

  const handleCloseSide = async (side = input.selectedSide) => {
    if (!input.token || !input.me || !ensureViewingSelfForMutation()) {
      return;
    }
    try {
      setQuickBusy(true);
      input.setError(undefined);
      const result = await api.closeSide(input.token, side);
      if (result.tradePatch) {
        input.setUserTradePayload(result.tradePatch);
      }
      input.setLastOrderLatencyMs(result.matchLatencyMs);
    } catch (closeError) {
      input.setError(closeError instanceof Error ? closeError.message : "Close side failed.");
    } finally {
      setQuickBusy(false);
    }
  };

  const handleReverseSide = async () => {
    if (!input.token || !input.me || !ensureViewingSelfForMutation()) {
      return;
    }
    try {
      setQuickBusy(true);
      input.setError(undefined);
      const result = await api.reverseSide(input.token, input.selectedSide);
      if (result.tradePatch) {
        input.setUserTradePayload(result.tradePatch);
      }
      input.setSelectedSide(result.reverseSide);
      input.setLastOrderLatencyMs(result.reverseOrder.matchLatencyMs);
    } catch (reverseError) {
      input.setError(reverseError instanceof Error ? reverseError.message : "Reverse side failed.");
    } finally {
      setQuickBusy(false);
    }
  };

  const handleCancelOrder = async (orderId: string) => {
    if (!input.token || !input.me || !ensureViewingSelfForMutation()) {
      return;
    }
    if (cancellingOrderIdsRef.current.has(orderId)) {
      return;
    }
    cancellingOrderIdsRef.current.add(orderId);
    setCancelBusyOrderId(orderId);
    try {
      input.setError(undefined);
      const result = await api.cancelOrder(input.token, orderId);
      if (result.tradePatch) {
        input.setUserTradePayload(result.tradePatch);
      }
    } catch (cancelError) {
      input.setError(cancelError instanceof Error ? cancelError.message : "Cancel failed.");
    } finally {
      cancellingOrderIdsRef.current.delete(orderId);
      setCancelBusyOrderId(undefined);
    }
  };

  const handleSell = async (positionId: string) => {
    if (!input.token || !input.me || !ensureViewingSelfForMutation()) {
      return;
    }
    try {
      setSellBusyPositionId(positionId);
      setSellFeedback(undefined);
      input.setError(undefined);
      const result = await api.sellPosition(input.token, positionId);
      if (result.tradePatch) {
        input.setUserTradePayload(result.tradePatch);
      }
      input.setLastOrderLatencyMs(result.order.totalOrderLatencyMs ?? result.order.matchLatencyMs);
    } catch (sellError) {
      const message = sellError instanceof Error ? sellError.message : "Sell failed.";
      input.setError(message);
      setSellFeedback({ positionId, message });
    } finally {
      setSellBusyPositionId(undefined);
    }
  };

  return {
    tradeBusy,
    quickBusy,
    cancelBusyOrderId,
    sellBusyPositionId,
    sellFeedback,
    pendingOrderClientId,
    ensureViewingSelfForMutation,
    handlePlaceOrder,
    handleCloseSide,
    handleReverseSide,
    handleCancelOrder,
    handleSell
  };
}

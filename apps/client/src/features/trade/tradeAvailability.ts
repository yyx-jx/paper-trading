import type { Language, MarketSnapshot, OrderAction, OrderRecord, PositionRecord, RoundRecord, TradeSide } from "../../utils/api";
import { localLabel } from "../../utils/format";

function activeRoundTradeBlockReason(round: RoundRecord | undefined, nowMs: number, language: Language) {
  if (!round || nowMs < round.startAt || nowMs >= round.endAt) {
    return localLabel(language, "当前没有可交易轮次。", "No active tradable round.");
  }
  if (round.status !== "Trading") {
    return localLabel(language, `当前轮次状态为 ${round.status}。`, `Current round is ${round.status}.`);
  }
  if (round.endAt - nowMs <= 10_000) {
    return localLabel(language, "当前轮次进入最后 10 秒冻结窗口。", "The round entered the final 10-second freeze window.");
  }
  return undefined;
}

export function buildTradeAvailability(input: {
  language: Language;
  currentRound?: RoundRecord;
  nowMs: number;
  selectedSide: TradeSide;
  orderAction: OrderAction;
  canPlaceOrder: boolean;
  canSell: boolean;
  acceptingOrders?: boolean;
  tradeBlockReason?: string;
  openSidePositions: PositionRecord[];
  orderBook?: MarketSnapshot["orderBooks"][TradeSide];
  oppositeOrderBook?: MarketSnapshot["orderBooks"][TradeSide];
  parsedQty: number;
}) {
  const roundBlockReason = activeRoundTradeBlockReason(input.currentRound, input.nowMs, input.language);
  const selectedAskAvailable = (input.orderBook?.bestAsk ?? 0) > 0 && (input.orderBook?.asks.length ?? 0) > 0;
  const selectedBidAvailable = (input.orderBook?.bestBid ?? 0) > 0 && (input.orderBook?.bids.length ?? 0) > 0;
  const oppositeAskAvailable = (input.oppositeOrderBook?.bestAsk ?? 0) > 0 && (input.oppositeOrderBook?.asks.length ?? 0) > 0;
  const hasOpenSidePositions = input.openSidePositions.some((position) => position.qty > 0);
  const buyReason =
    roundBlockReason ??
    (!input.canPlaceOrder ? localLabel(input.language, "当前用户没有下单权限。", "Current user cannot place orders.") : undefined) ??
    (input.acceptingOrders === false ? localLabel(input.language, "当前市场不接受新买入订单。", "The market is not accepting new buy orders.") : undefined) ??
    (!selectedAskAvailable ? localLabel(input.language, "当前方向没有可买入盘口。", "No ask depth is available for this side.") : undefined) ??
    input.tradeBlockReason;
  const sellReason =
    roundBlockReason ??
    (!input.canSell ? localLabel(input.language, "当前用户没有卖出权限。", "Current user cannot sell.") : undefined) ??
    (!hasOpenSidePositions ? localLabel(input.language, "当前方向没有可卖持仓。", "No open position on this side.") : undefined) ??
    (!selectedBidAvailable ? localLabel(input.language, "当前方向没有可卖出盘口。", "No bid depth is available for this side.") : undefined) ??
    (input.orderAction === "sell" && input.parsedQty <= 0
      ? localLabel(input.language, "请输入有效卖出数量。", "Enter a valid sell quantity.")
      : undefined) ??
    input.tradeBlockReason;
  const reverseReason =
    sellReason ??
    (!input.canPlaceOrder ? localLabel(input.language, "当前用户没有反向买入权限。", "Current user cannot place the reverse buy order.") : undefined) ??
    (input.acceptingOrders === false ? localLabel(input.language, "当前市场不接受反向买入订单。", "The market is not accepting the reverse buy order.") : undefined) ??
    (!oppositeAskAvailable ? localLabel(input.language, "反方向没有可买入盘口。", "No ask depth is available for the reverse side.") : undefined);
  return {
    canBuy: !buyReason,
    canSell: !sellReason,
    canCloseSide: !sellReason,
    canReverseSide: !reverseReason,
    buyReason,
    sellReason,
    closeSideReason: sellReason,
    reverseReason
  };
}

export function isCurrentRoundOrder(order: OrderRecord, currentRound?: RoundRecord) {
  if (!currentRound) {
    return false;
  }
  return order.roundId === currentRound.id || Boolean(order.marketSlug && order.marketSlug === currentRound.marketSlug);
}

export function sortOrdersForTradingPage(left: OrderRecord, right: OrderRecord, currentRound?: RoundRecord) {
  const leftCurrent = isCurrentRoundOrder(left, currentRound) ? 1 : 0;
  const rightCurrent = isCurrentRoundOrder(right, currentRound) ? 1 : 0;
  if (leftCurrent !== rightCurrent) {
    return rightCurrent - leftCurrent;
  }
  const leftPending = left.status === "pending" ? 1 : 0;
  const rightPending = right.status === "pending" ? 1 : 0;
  if (leftPending !== rightPending) {
    return rightPending - leftPending;
  }
  return right.createdAt - left.createdAt;
}


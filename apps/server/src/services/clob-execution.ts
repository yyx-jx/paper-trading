import type { BookLevel, MatchingFill, OrderAction, OrderBookSnapshot } from "../domain/types";

const PRICE_EPSILON = 0.0000001;
const QTY_EPSILON = 0.0000001;

const roundNumber = (value: number, digits = 8) => Number(value.toFixed(digits));

export interface ClobExecutionInput {
  action: OrderAction;
  book: OrderBookSnapshot;
  orderId: string;
  notional?: number;
  qty?: number;
  limitPrice?: number;
  executedAt: number;
}

export interface ClobExecutionEstimate {
  fullyMatched: boolean;
  fills: MatchingFill[];
  filledQty: number;
  matchedNotional: number;
  remainingQty: number;
  remainingNotional: number;
  avgPrice?: number;
  worstPrice?: number;
  failureReason?: string;
}

function sortedLevels(input: BookLevel[], action: OrderAction) {
  return [...input]
    .filter((level) => level.price > 0 && level.qty > 0)
    .sort((left, right) => (action === "buy" ? left.price - right.price : right.price - left.price));
}

function crossesLimit(action: OrderAction, price: number, limitPrice?: number) {
  if (typeof limitPrice !== "number") {
    return true;
  }
  return action === "buy" ? price <= limitPrice + PRICE_EPSILON : price >= limitPrice - PRICE_EPSILON;
}

export function estimateClobExecution(input: ClobExecutionInput): ClobExecutionEstimate {
  if (input.action === "buy" && (input.notional ?? 0) <= 0) {
    throw new Error("Buy orders require positive notional.");
  }
  if (input.action === "sell" && (input.qty ?? 0) <= 0) {
    throw new Error("Sell orders require positive quantity.");
  }

  const levels = sortedLevels(input.action === "buy" ? input.book.asks : input.book.bids, input.action);
  const fills: MatchingFill[] = [];
  let filledQty = 0;
  let matchedNotional = 0;
  let remainingQty = roundNumber(input.qty ?? 0);
  let remainingNotional = roundNumber(input.notional ?? 0);
  let worstPrice: number | undefined;

  for (const level of levels) {
    if (!crossesLimit(input.action, level.price, input.limitPrice)) {
      break;
    }

    const byQty = input.action === "sell" ? remainingQty : Number.POSITIVE_INFINITY;
    const byNotional =
      input.action === "buy" ? remainingNotional / Math.max(level.price, PRICE_EPSILON) : Number.POSITIVE_INFINITY;
    const fillQty = roundNumber(Math.min(level.qty, byQty, byNotional));
    if (fillQty <= QTY_EPSILON) {
      continue;
    }

    const fillNotional = roundNumber(fillQty * level.price);
    filledQty = roundNumber(filledQty + fillQty);
    matchedNotional = roundNumber(matchedNotional + fillNotional);
    worstPrice = level.price;

    if (input.action === "buy") {
      remainingNotional = roundNumber(Math.max(remainingNotional - fillNotional, 0));
    } else {
      remainingQty = roundNumber(Math.max(remainingQty - fillQty, 0));
    }

    fills.push({
      fillId: `${input.orderId}:fill:${fills.length + 1}`,
      makerOrderId: `${input.book.snapshotId}:${input.action === "buy" ? "ask" : "bid"}:${fills.length + 1}`,
      takerOrderId: input.orderId,
      price: roundNumber(level.price),
      qty: fillQty,
      notional: fillNotional,
      makerOwnerId: "external:polymarket",
      makerOwnerType: "external",
      executedAt: input.executedAt
    });

    if (input.action === "buy" && remainingNotional <= 0.01) {
      remainingNotional = 0;
      break;
    }
    if (input.action === "sell" && remainingQty <= QTY_EPSILON) {
      remainingQty = 0;
      break;
    }
  }

  const fullyMatched = input.action === "buy" ? remainingNotional <= 0.01 : remainingQty <= QTY_EPSILON;
  const avgPrice = filledQty > QTY_EPSILON ? roundNumber(matchedNotional / filledQty) : undefined;
  const failureReason = fullyMatched
    ? undefined
    : typeof input.limitPrice === "number"
      ? "Polymarket CLOB depth did not fully cross the limit price."
      : "Polymarket CLOB depth was insufficient for full fill.";

  return {
    fullyMatched,
    fills,
    filledQty: roundNumber(filledQty),
    matchedNotional: roundNumber(matchedNotional),
    remainingQty: roundNumber(remainingQty),
    remainingNotional: roundNumber(remainingNotional),
    avgPrice,
    worstPrice,
    failureReason
  };
}


import type { HistoryRound, Language, OrderRecord, PositionRecord, TradeSide } from "../../utils/api";
import { localLabel, utcParts } from "../../utils/format";
import { positionDisplayedPnl } from "../trade/pnl";

export type AnalyticsPeriod = "all" | "year" | "month" | "week" | "day" | "trades";
export type AnalyticsResult = "WIN" | "LOSE" | "SOLD" | "OPEN" | "UNFILLED";
export type AnalyticsTone = "positive" | "negative" | "neutral" | "warning";
export type AnalyticsSettlementState = "SETTLED" | "UNSETTLED";

export const ANALYTICS_INITIAL_TRADE_LIMIT = 200;
export const ANALYTICS_TRADE_LIMIT_STEP = 200;

export interface AnalyticsTradeRow {
  id: string;
  ts: number;
  result: AnalyticsResult;
  roundId: string;
  roundCloseAt?: number;
  roundLabel: string;
  side: TradeSide;
  invested: number;
  entryPrice: number;
  settlementPrice?: number;
  shares: number;
  fees: number;
  pnl: number;
  analysisText: string;
  analysisTone: AnalyticsTone;
  settlementState: AnalyticsSettlementState;
}

function chartTimeText(value?: number) {
  if (!value) {
    return "--";
  }
  const { hour, minute } = utcParts(value);
  return `${hour}:${minute}`;
}

function analyticsRoundLabel(endAt?: number, fallbackTs?: number) {
  const resolvedTs = endAt ?? fallbackTs;
  return resolvedTs ? `B5-${chartTimeText(resolvedTs)} UTC` : "B5--";
}

function isClobDepthFailure(order: OrderRecord) {
  return typeof order.failureReason === "string" && /insufficient CLOB depth/i.test(order.failureReason);
}

export function buildAnalyticsRows(
  history: HistoryRound[],
  positions: PositionRecord[],
  orders: OrderRecord[],
  language: Language
) {
  const rows: AnalyticsTradeRow[] = [];
  const roundsById = new Map(history.map((round) => [round.id, round]));
  const ordersByRoundSide = new Map<string, OrderRecord[]>();
  for (const order of orders) {
    const key = `${order.roundId}:${order.side}`;
    const next = ordersByRoundSide.get(key) ?? [];
    next.push(order);
    ordersByRoundSide.set(key, next);
  }
  for (const position of positions) {
    const relatedOrders = ordersByRoundSide.get(`${position.roundId}:${position.side}`) ?? [];
    const fees = relatedOrders.reduce((sum, order) => sum + (order.actualFee ?? order.estimatedFee ?? 0), 0);
    const result: AnalyticsResult =
      position.status === "open"
        ? "OPEN"
        : position.settlementResult === "win"
          ? "WIN"
          : position.settlementResult === "sold"
            ? "SOLD"
            : "LOSE";
    const round = roundsById.get(position.roundId);
    const settlementState = result === "OPEN" ? "UNSETTLED" : "SETTLED";
    const analysis = analyticsRowAnalysis(result, language);
    rows.push({
      id: `position:${position.id}`,
      ts: position.closedAt ?? position.openedAt,
      result,
      roundId: position.roundId,
      roundCloseAt: round?.endAt,
      roundLabel: analyticsRoundLabel(round?.endAt, position.closedAt ?? position.openedAt),
      side: position.side,
      invested: position.notionalSpent,
      entryPrice: position.averageEntry,
      settlementPrice: settlementState === "SETTLED" ? position.currentMark : undefined,
      shares: position.qty,
      fees,
      pnl: positionDisplayedPnl(position),
      analysisText: analysis.text,
      analysisTone: analysis.tone,
      settlementState
    });
  }
  for (const order of orders) {
    if (order.status !== "failed" || !isClobDepthFailure(order)) {
      continue;
    }
    const round = roundsById.get(order.roundId);
    const analysis = analyticsRowAnalysis("UNFILLED", language);
    rows.push({
      id: `order:${order.id}`,
      ts: order.createdAt,
      result: "UNFILLED",
      roundId: order.roundId,
      roundCloseAt: round?.endAt,
      roundLabel: analyticsRoundLabel(round?.endAt, order.createdAt),
      side: order.side,
      invested: order.notionalUsdc,
      entryPrice: order.limitPrice ?? order.bestAsk ?? order.midPrice ?? 0,
      settlementPrice: undefined,
      shares: order.expectedQty,
      fees: order.actualFee ?? order.estimatedFee ?? 0,
      pnl: 0,
      analysisText: analysis.text,
      analysisTone: analysis.tone,
      settlementState: "UNSETTLED"
    });
  }
  return rows.sort((left, right) => right.ts - left.ts);
}

export function filterAnalyticsPeriod(rows: AnalyticsTradeRow[], period: AnalyticsPeriod) {
  const now = Date.now();
  const cutoff =
    period === "day"
      ? now - 24 * 60 * 60_000
      : period === "week"
        ? now - 7 * 24 * 60 * 60_000
        : period === "month"
          ? now - 30 * 24 * 60 * 60_000
          : period === "year"
            ? now - 365 * 24 * 60 * 60_000
            : undefined;
  if (period === "trades") {
    return rows;
  }
  if (typeof cutoff !== "number") {
    return rows;
  }
  return rows.filter((row) => row.ts >= cutoff);
}

export function analyticsSummary(rows: AnalyticsTradeRow[]) {
  const closedRows = rows.filter((row) => row.result !== "OPEN" && row.result !== "UNFILLED");
  const wins = closedRows.filter((row) => row.result === "WIN").length;
  const losses = closedRows.filter((row) => row.result === "LOSE").length;
  const totalPnl = closedRows.reduce((sum, row) => sum + row.pnl, 0);
  const totalFees = rows.reduce((sum, row) => sum + row.fees, 0);
  const bestTrade = closedRows.reduce((best, row) => Math.max(best, row.pnl), Number.NEGATIVE_INFINITY);
  const worstTrade = closedRows.reduce((worst, row) => Math.min(worst, row.pnl), Number.POSITIVE_INFINITY);
  return {
    totalPnl,
    totalFees,
    wins,
    losses,
    trades: closedRows.length,
    winRate: closedRows.length > 0 ? wins / closedRows.length : 0,
    bestTrade: Number.isFinite(bestTrade) ? bestTrade : 0,
    worstTrade: Number.isFinite(worstTrade) ? worstTrade : 0
  };
}

export function analyticsPeriodLabel(period: AnalyticsPeriod, language: Language) {
  const labels: Record<AnalyticsPeriod, { zh: string; en: string }> = {
    all: { zh: "全部", en: "All" },
    year: { zh: "年", en: "Year" },
    month: { zh: "月", en: "Month" },
    week: { zh: "周", en: "Week" },
    day: { zh: "日", en: "Day" },
    trades: { zh: "交易", en: "Trades" }
  };
  return localLabel(language, labels[period].zh, labels[period].en);
}

export function analyticsResultLabel(result: AnalyticsResult, language: Language) {
  const labels: Record<AnalyticsResult, { zh: string; en: string }> = {
    WIN: { zh: "盈利", en: "Win" },
    LOSE: { zh: "亏损", en: "Loss" },
    SOLD: { zh: "已卖出", en: "Sold" },
    OPEN: { zh: "持仓中", en: "Open" },
    UNFILLED: { zh: "未成交", en: "Unfilled" }
  };
  return localLabel(language, labels[result].zh, labels[result].en);
}

export function analyticsSettlementLabel(state: AnalyticsSettlementState, language: Language) {
  return state === "SETTLED"
    ? localLabel(language, "已结算", "Settled")
    : localLabel(language, "未结算", "Unsettled");
}

function analyticsRowAnalysis(result: AnalyticsResult, language: Language): { text: string; tone: AnalyticsTone } {
  if (result === "WIN") {
    return {
      tone: "positive",
      text: localLabel(language, "本轮兑现盈利，入场价格与结算方向匹配。", "Profit was realized; entry price aligned with the settled side.")
    };
  }
  if (result === "LOSE") {
    return {
      tone: "negative",
      text: localLabel(language, "方向未兑现，复盘入场价和封盘前风险。", "The side did not resolve; review entry price and late-round risk.")
    };
  }
  if (result === "SOLD") {
    return {
      tone: "warning",
      text: localLabel(language, "提前退出，关注退出纪律和滑点。", "Exited before settlement; check exit discipline and slippage.")
    };
  }
  if (result === "UNFILLED") {
    return {
      tone: "warning",
      text: localLabel(language, "盘口深度不足，订单没有形成有效仓位。", "Book depth was insufficient; the order did not form a position.")
    };
  }
  return {
    tone: "neutral",
    text: localLabel(language, "仍在生命周期中，先观察结算结果。", "Still in its lifecycle; wait for the settlement result.")
  };
}

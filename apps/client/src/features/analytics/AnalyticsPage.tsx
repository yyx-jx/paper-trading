import { useEffect, useMemo, useState } from "react";
import type {
  AuditEvent,
  HistoryRound,
  Language,
  OrderRecord,
  PositionRecord,
  ProfileOverview,
  RoundRecord,
  TradeSide
} from "../../utils/api";
import { decimal, localLabel, money, signedMoney, timeText, tokenPriceText } from "../../utils/format";
import { summarizePositionPnl } from "../trade/pnl";
import {
  ANALYTICS_INITIAL_TRADE_LIMIT,
  ANALYTICS_TRADE_LIMIT_STEP,
  analyticsPeriodLabel,
  analyticsResultLabel,
  analyticsSettlementLabel,
  analyticsSummary,
  buildAnalyticsRows,
  filterAnalyticsPeriod,
  type AnalyticsPeriod,
  type AnalyticsResult
} from "./analyticsModel";

const compactPercent = (value = 0) => `${(value * 100).toFixed(1)}%`;

export interface RoundCalendarItem {
  roundId: string;
  marketSlug?: string;
  status: RoundRecord["status"];
  startAt: number;
  endAt: number;
  roundPnl: number;
  orderCount: number;
  sequence: number;
  label: string;
  datedLabel: string;
}

export function AnalyticsPage(props: {
  t: (key: string, options?: Record<string, unknown>) => string;
  token: string;
  language: Language;
  profile?: ProfileOverview;
  history: HistoryRound[];
  operatedHistory: HistoryRound[];
  positions: PositionRecord[];
  orders: OrderRecord[];
  logs: AuditEvent[];
  onSell: (positionId: string) => Promise<void>;
  onTimeline: (orderId: string) => Promise<void>;
  onOpenRoundLogs: (item: RoundCalendarItem) => Promise<void>;
  timelineBusyOrderId?: string;
  selectedRoundLogId?: string;
  roundLogBusyRoundId?: string;
}) {
  const { language } = props;
  type AnalyticsResultFilter = "ALL" | AnalyticsResult;
  const [period, setPeriod] = useState<AnalyticsPeriod>("all");
  const [direction, setDirection] = useState<"ALL" | TradeSide>("ALL");
  const [resultFilter, setResultFilter] = useState<AnalyticsResultFilter>("ALL");
  const [visibleTradeLimit, setVisibleTradeLimit] = useState(ANALYTICS_INITIAL_TRADE_LIMIT);
  useEffect(() => {
    setVisibleTradeLimit(ANALYTICS_INITIAL_TRADE_LIMIT);
  }, [direction, period, resultFilter]);
  const rows = useMemo(
    () => buildAnalyticsRows(props.history, props.positions, props.orders, language),
    [props.history, props.positions, props.orders, language]
  );
  const periodRows = useMemo(() => filterAnalyticsPeriod(rows, period), [rows, period]);
  const filteredRows = useMemo(
    () =>
      periodRows.filter((row) => {
        if (direction !== "ALL" && row.side !== direction) {
          return false;
        }
        if (resultFilter !== "ALL" && row.result !== resultFilter) {
          return false;
        }
        return true;
      }),
    [direction, periodRows, resultFilter]
  );
  const summary = useMemo(() => analyticsSummary(filteredRows), [filteredRows]);
  const displayedRows = useMemo(
    () => (period === "trades" ? filteredRows.slice(0, visibleTradeLimit) : filteredRows),
    [filteredRows, period, visibleTradeLimit]
  );
  const openPnlSummary = useMemo(
    () => summarizePositionPnl(props.positions.filter((position) => position.status === "open")),
    [props.positions]
  );
  const periodOptions = [
    { id: "all", label: analyticsPeriodLabel("all", language) },
    { id: "year", label: analyticsPeriodLabel("year", language) },
    { id: "month", label: analyticsPeriodLabel("month", language) },
    { id: "week", label: analyticsPeriodLabel("week", language) },
    { id: "day", label: analyticsPeriodLabel("day", language) },
    { id: "trades", label: analyticsPeriodLabel("trades", language) }
  ] as const;

  return (
    <section className="analytics-terminal-page">
      <div className="analytics-headband">
        <div className="analytics-head-copy">
          <b>{localLabel(language, "交易分析", "Analytics")}</b>
          <span>{localLabel(language, "按 UTC 展示 BTC 模拟盘交易生命周期", "BTC paper trading lifecycle shown in UTC")}</span>
        </div>
        <div className="analytics-head-meta">
          <span>{filteredRows.length} {localLabel(language, "条记录", "rows")}</span>
          <span>{summary.trades} {localLabel(language, "已结算", "settled")}</span>
          <span>{summary.wins}W / {summary.losses}L</span>
        </div>
      </div>

      <div className="analytics-summary">
        <div className="analytics-card">
          <span>{localLabel(language, "总盈亏", "Total PnL")}</span>
          <strong>{signedMoney(summary.totalPnl)}</strong>
          <small>
            {localLabel(language, "总资产", "Total equity")} {money(props.profile?.totalEquity ?? 0)}
            {" · "}
            {localLabel(language, "可用", "Available")} {money(props.profile?.availableUsdc ?? 0)}
          </small>
        </div>
        <div className="analytics-card">
          <span>{localLabel(language, "胜率", "Win Rate")}</span>
          <strong>{summary.trades > 0 ? compactPercent(summary.winRate) : "—"}</strong>
          <small>{summary.wins}W / {summary.losses}L</small>
        </div>
        <div className="analytics-card">
          <span>{localLabel(language, "交易笔数", "Trades")}</span>
          <strong>{summary.trades}</strong>
          <small>{filteredRows.length} {localLabel(language, "条记录", "rows")}</small>
        </div>
        <div className="analytics-card">
          <span>{localLabel(language, "总费用", "Total Fees")}</span>
          <strong>{money(summary.totalFees, 4)}</strong>
          <small>
            {localLabel(language, "持仓费用", "Position fees")} {money(openPnlSummary.totalFeeUsdc, 4)}
          </small>
        </div>
        <div className="analytics-card">
          <span>{localLabel(language, "Mark PnL", "Mark PnL")}</span>
          <strong>{signedMoney(openPnlSummary.markPnlUsdc)}</strong>
          <small>{localLabel(language, "mid mark，优先使用含成本/手续费字段", "mid mark, fee-adjusted when available")}</small>
        </div>
        <div className="analytics-card">
          <span>{localLabel(language, "可成交 PnL", "Executable PnL")}</span>
          <strong>{signedMoney(openPnlSummary.executablePnlUsdc)}</strong>
          <small>{localLabel(language, "best bid 可退出口径", "best bid executable view")}</small>
        </div>
        <div className="analytics-card">
          <span>{localLabel(language, "最佳单笔", "Best Trade")}</span>
          <strong>{summary.trades > 0 ? signedMoney(summary.bestTrade) : "—"}</strong>
          <small>{localLabel(language, "最佳已结算结果", "Best settled result")}</small>
        </div>
        <div className="analytics-card">
          <span>{localLabel(language, "最差单笔", "Worst Trade")}</span>
          <strong>{summary.trades > 0 ? signedMoney(summary.worstTrade) : "—"}</strong>
          <small>{localLabel(language, "最差已结算结果", "Worst settled result")}</small>
        </div>
      </div>

      <div className="analytics-controls">
        <div className="analytics-period-tabs">
          {periodOptions.map((option) => (
            <button key={option.id} className={period === option.id ? "active" : ""} onClick={() => setPeriod(option.id)}>
              {option.label}
            </button>
          ))}
        </div>
        <label>
          <span>{localLabel(language, "标的", "Symbol")}</span>
          <select value="BTC" disabled>
            <option value="BTC">BTC</option>
          </select>
        </label>
        <label>
          <span>{localLabel(language, "方向", "Direction")}</span>
          <select value={direction} onChange={(event) => setDirection(event.target.value as "ALL" | TradeSide)}>
            <option value="ALL">{localLabel(language, "全部", "All")}</option>
            <option value="UP">UP</option>
            <option value="DOWN">DOWN</option>
          </select>
        </label>
        <label>
          <span>{localLabel(language, "结果", "Result")}</span>
          <select value={resultFilter} onChange={(event) => setResultFilter(event.target.value as AnalyticsResultFilter)}>
            <option value="ALL">{localLabel(language, "全部", "All")}</option>
            <option value="WIN">{analyticsResultLabel("WIN", language)}</option>
            <option value="LOSE">{analyticsResultLabel("LOSE", language)}</option>
            <option value="SOLD">{analyticsResultLabel("SOLD", language)}</option>
            <option value="OPEN">{analyticsResultLabel("OPEN", language)}</option>
            <option value="UNFILLED">{analyticsResultLabel("UNFILLED", language)}</option>
          </select>
        </label>
        <span>{localLabel(language, "所有时间均以 UTC 显示", "All times shown in UTC")}</span>
      </div>

      <div className="analytics-table-panel">
        {displayedRows.length === 0 ? (
          <div className="analytics-empty">
            <b>{localLabel(language, "空结果", "Empty")}</b>
            <div>{localLabel(language, "当前筛选条件下没有可展示的分析记录。", "No analytics rows match the current filters.")}</div>
          </div>
        ) : (
          <div className="analytics-table-wrap">
            <table>
              <thead>
                <tr>
                  <th>{localLabel(language, "时间", "Time")}</th>
                  <th>{localLabel(language, "轮次", "Round")}</th>
                  <th>{localLabel(language, "方向", "Direction")}</th>
                  <th>{localLabel(language, "投入", "Invested")}</th>
                  <th>{localLabel(language, "入场价", "Entry")}</th>
                  <th>{localLabel(language, "结算价/退出价", "Settle/Exit")}</th>
                  <th>{localLabel(language, "份额", "Shares")}</th>
                  <th>{localLabel(language, "费用", "Fees")}</th>
                  <th>PnL</th>
                  <th>{localLabel(language, "状态", "State")}</th>
                  <th>{localLabel(language, "结果", "Result")}</th>
                  <th>{localLabel(language, "分析", "Analysis")}</th>
                </tr>
              </thead>
              <tbody>
                {displayedRows.map((row) => (
                  <tr key={row.id}>
                    <td>{timeText(row.ts)}</td>
                    <td>{row.roundLabel}</td>
                    <td><span className={`analytics-tag ${row.side === "UP" ? "up" : "down"}`}>{row.side}</span></td>
                    <td>{money(row.invested)}</td>
                    <td>{tokenPriceText(row.entryPrice)}</td>
                    <td>{typeof row.settlementPrice === "number" ? tokenPriceText(row.settlementPrice) : "—"}</td>
                    <td>{decimal(row.shares, 4)}</td>
                    <td>{money(row.fees, 4)}</td>
                    <td>{signedMoney(row.pnl)}</td>
                    <td>
                      <span className={`analytics-tag state-${row.settlementState.toLowerCase()}`}>
                        {analyticsSettlementLabel(row.settlementState, language)}
                      </span>
                    </td>
                    <td><span className={`analytics-tag result-${row.result.toLowerCase()}`}>{analyticsResultLabel(row.result, language)}</span></td>
                    <td><span className={`analytics-row-analysis ${row.analysisTone}`}>{row.analysisText}</span></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {period === "trades" && visibleTradeLimit < filteredRows.length ? (
          <button
            type="button"
            className="analytics-load-more global"
            onClick={() => setVisibleTradeLimit((limit) => limit + ANALYTICS_TRADE_LIMIT_STEP)}
          >
            {localLabel(language, `加载更多 ${Math.min(filteredRows.length - visibleTradeLimit, ANALYTICS_TRADE_LIMIT_STEP)} 条交易`, `Load ${Math.min(filteredRows.length - visibleTradeLimit, ANALYTICS_TRADE_LIMIT_STEP)} more trades`)}
          </button>
        ) : null}
      </div>
    </section>
  );
}

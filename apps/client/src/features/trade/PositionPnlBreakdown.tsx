import type { Language } from "../../utils/api";
import { localLabel, money, signedMoney } from "../../utils/format";

export function PositionPnlBreakdown(props: {
  language: Language;
  markPnlUsdc: number;
  executablePnlUsdc: number;
  entryFeeUsdc: number;
  exitFeeUsdc: number;
  totalFeeUsdc: number;
}) {
  const feeTitle = localLabel(
    props.language,
    "PnL 口径包含买入成本；费用字段单独列示。",
    "PnL uses fee-adjusted cost basis where available; fees are shown separately."
  );
  return (
    <div className="position-pnl-breakdown" title={feeTitle}>
      <span>
        <small>{localLabel(props.language, "Mark PnL", "Mark PnL")}</small>
        <b className={props.markPnlUsdc >= 0 ? "terminal-green" : "terminal-red"}>{signedMoney(props.markPnlUsdc)}</b>
      </span>
      <span>
        <small>{localLabel(props.language, "可成交 PnL", "Executable PnL")}</small>
        <b className={props.executablePnlUsdc >= 0 ? "terminal-green" : "terminal-red"}>
          {signedMoney(props.executablePnlUsdc)}
        </b>
      </span>
      <span>
        <small>{localLabel(props.language, "费用", "Fees")}</small>
        <b>{money(props.totalFeeUsdc, 4)}</b>
      </span>
      <em>
        {localLabel(props.language, "入场", "Entry")} {money(props.entryFeeUsdc, 4)} ·{" "}
        {localLabel(props.language, "出场", "Exit")} {money(props.exitFeeUsdc, 4)}
      </em>
    </div>
  );
}

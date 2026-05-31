import i18n from "../../i18n";
import type { Language } from "../../utils/api";
import { money, signedMoney } from "../../utils/format";
const t = (key: string, options?: Record<string, unknown>) => i18n.t(key, options);



export function PositionPnlBreakdown(props: {
  language: Language;
  markPnlUsdc: number;
  executablePnlUsdc: number;
  entryFeeUsdc: number;
  exitFeeUsdc: number;
  totalFeeUsdc: number;
}) {
  const feeTitle = t("uiPnLUsesFeeAdjustedCostBasis401c0c01");
  return (
    <div className="position-pnl-breakdown" title={feeTitle}>
      <span>
        <small>{t("uiMarkPnL6430b836")}</small>
        <b className={props.markPnlUsdc >= 0 ? "terminal-green" : "terminal-red"}>{signedMoney(props.markPnlUsdc)}</b>
      </span>
      <span>
        <small>{t("uiExecutablePnL89182743")}</small>
        <b className={props.executablePnlUsdc >= 0 ? "terminal-green" : "terminal-red"}>
          {signedMoney(props.executablePnlUsdc)}
        </b>
      </span>
      <span>
        <small>{t("uiFees5ef20e69")}</small>
        <b>{money(props.totalFeeUsdc, 4)}</b>
      </span>
      <em>
        {t("uiEntrye98aa4ac")} {money(props.entryFeeUsdc, 4)} ·{" "}
        {t("uiExitf7288879")} {money(props.exitFeeUsdc, 4)}
      </em>
    </div>
  );
}

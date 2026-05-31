import { useEffect, useState } from "react";
import { api, type ManualSettlementCandidate, type TradeSide } from "../../utils/api";
import { dateTimeText, decimal } from "../../utils/format";

type ManualSettlementQueueProps = {
  token: string;
  t: (key: string, options?: Record<string, unknown>) => string;
  canManualSettle: boolean;
  onManualSettlementComplete: () => Promise<void>;
};

export function ManualSettlementQueue(props: ManualSettlementQueueProps) {
  const { token, t, canManualSettle } = props;
  const [items, setItems] = useState<ManualSettlementCandidate[]>([]);
  const [busy, setBusy] = useState(false);
  const [settlingRoundId, setSettlingRoundId] = useState<string>();
  const [error, setError] = useState<string>();

  const loadQueue = async () => {
    if (!canManualSettle) {
      setItems([]);
      return;
    }
    try {
      setBusy(true);
      setError(undefined);
      setItems(await api.getManualSettlementQueue(token, 100));
    } catch (loadError) {
      setError(loadError instanceof Error ? loadError.message : "Manual settlement queue failed.");
    } finally {
      setBusy(false);
    }
  };

  useEffect(() => {
    void loadQueue();
  }, [canManualSettle, token]);

  const settle = async (item: ManualSettlementCandidate, side: TradeSide) => {
    const ok = window.confirm(`${t("manualReview")}: ${item.marketSlug ?? item.roundId} -> ${side}`);
    if (!ok) {
      return;
    }
    try {
      setSettlingRoundId(item.roundId);
      setError(undefined);
      await api.manualSettleRound(token, item.roundId, {
        side,
        reason: `Admin manual settlement queue selected ${side}.`
      });
      await props.onManualSettlementComplete();
      await loadQueue();
    } catch (settleError) {
      setError(settleError instanceof Error ? settleError.message : "Manual settlement failed.");
    } finally {
      setSettlingRoundId(undefined);
    }
  };

  if (!canManualSettle) {
    return null;
  }

  return (
    <div className="analytics-table-panel manual-settlement-queue">
      <div className="analytics-panel-heading">
        <div>
          <b>{t("manualReview")}</b>
          <span>{t("settlementStuckManualInputNeeded")}</span>
        </div>
        <button type="button" className="secondary-button" onClick={() => void loadQueue()} disabled={busy}>
          {busy ? t("loading") : t("refresh")}
        </button>
      </div>
      {error ? <div className="inline-error-banner compact-feedback">{error}</div> : null}
      {items.length === 0 ? (
        <div className="analytics-empty">
          <b>{t("uiEmptya6461b8d")}</b>
          <div>{t("manualReviewIsRequiredBeforeSettlementIsConfirmed")}</div>
        </div>
      ) : (
        <div className="analytics-table-wrap">
          <table>
            <thead>
              <tr>
                <th>{t("uiRound86342c68")}</th>
                <th>{t("uiExitSettleTime0e60f35d")}</th>
                <th>{t("uiState4fbe6edf")}</th>
                <th>{t("uiRows308a8de9")}</th>
                <th>UP</th>
                <th>DOWN</th>
                <th>{t("manualReview")}</th>
              </tr>
            </thead>
            <tbody>
              {items.map((item) => (
                <tr key={item.roundId}>
                  <td>
                    <strong>{item.marketSlug ?? item.roundId}</strong>
                    <small>{item.manualReason ?? "Manual settlement required"}</small>
                  </td>
                  <td>{dateTimeText(item.endAt)}</td>
                  <td>
                    <span className="analytics-tag state-unsettled">{item.status}</span>
                    <small>{item.pollCount} polls</small>
                  </td>
                  <td>
                    {item.participantCount} users / {item.openPositionCount} open / {item.pendingOrderCount} pending
                  </td>
                  <td>{decimal(item.upOpenQty, 4)}</td>
                  <td>{decimal(item.downOpenQty, 4)}</td>
                  <td>
                    <div className="quick-row">
                      <button disabled={settlingRoundId === item.roundId} onClick={() => void settle(item, "UP")}>
                        Settle UP
                      </button>
                      <button disabled={settlingRoundId === item.roundId} onClick={() => void settle(item, "DOWN")}>
                        Settle DN
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

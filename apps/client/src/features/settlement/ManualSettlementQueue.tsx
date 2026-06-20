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
  const [isOpen, setIsOpen] = useState(false);
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

  useEffect(() => {
    if (isOpen) {
      void loadQueue();
    }
  }, [isOpen]);

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
    <div className="manual-settlement-queue">
      <div className="manual-settlement-entry">
        <div>
          <b>{t("auditActionManualSettlement")}</b>
          <span>{t("settlementStuckManualInputNeeded")}</span>
        </div>
        <button
          type="button"
          className="secondary-button manual-settlement-entry-button"
          onClick={() => setIsOpen(true)}
          disabled={busy && items.length === 0}
        >
          <span>{busy && items.length === 0 ? t("loading") : t("manualReview")}</span>
          {items.length > 0 ? <em>{items.length}</em> : null}
        </button>
      </div>

      {isOpen ? (
        <div className="modal-backdrop" onClick={() => setIsOpen(false)}>
          <section className="panel manual-settlement-dialog" onClick={(event) => event.stopPropagation()}>
            <div className="manual-settlement-dialog-header">
              <div>
                <h2>{t("auditActionManualSettlement")}</h2>
                <span>{t("manualReviewIsRequiredBeforeSettlementIsConfirmed")}</span>
              </div>
              <button type="button" className="manual-settlement-close" onClick={() => setIsOpen(false)} aria-label={t("close")}>
                &times;
              </button>
            </div>

            <div className="manual-settlement-dialog-toolbar">
              <div>
                <b>{items.length}</b>
                <span>{t("manualReview")}</span>
              </div>
              <button type="button" className="secondary-button" onClick={() => void loadQueue()} disabled={busy}>
                {busy ? t("loading") : t("refresh")}
              </button>
            </div>

            {error ? <div className="inline-error-banner compact-feedback">{error}</div> : null}

            {busy && items.length === 0 ? (
              <div className="settlement-loading">
                <span>{t("loading")}</span>
              </div>
            ) : items.length === 0 ? (
              <div className="analytics-empty manual-settlement-empty">
                <b>{t("uiEmptya6461b8d")}</b>
                <div>{t("manualReviewIsRequiredBeforeSettlementIsConfirmed")}</div>
              </div>
            ) : (
              <div className="manual-settlement-round-list">
                {items.map((item) => {
                  const isSettling = settlingRoundId === item.roundId;
                  return (
                    <article key={item.roundId} className="manual-settlement-round-card">
                      <div className="manual-settlement-card-top">
                        <div>
                          <strong>{item.marketSlug ?? item.roundId}</strong>
                          <small>{item.manualReason ?? "Manual settlement required"}</small>
                        </div>
                        <span className="analytics-tag state-unsettled">{item.status}</span>
                      </div>

                      <div className="manual-settlement-card-body">
                        <div className="manual-settlement-metric-grid">
                          <span>
                            <small>{t("uiExitSettleTime0e60f35d")}</small>
                            <b>{dateTimeText(item.endAt)}</b>
                          </span>
                          <span>
                            <small>{t("uiState4fbe6edf")}</small>
                            <b>{item.pollCount} polls</b>
                          </span>
                          <span>
                            <small>{t("uiRows308a8de9")}</small>
                            <b>{item.participantCount} users</b>
                          </span>
                          <span>
                            <small>Open</small>
                            <b>{item.openPositionCount} positions</b>
                          </span>
                          <span>
                            <small>Pending</small>
                            <b>{item.pendingOrderCount} orders</b>
                          </span>
                          <span>
                            <small>UP / DOWN</small>
                            <b>
                              {decimal(item.upOpenQty, 4)} / {decimal(item.downOpenQty, 4)}
                            </b>
                          </span>
                        </div>

                        <div className="manual-settlement-actions">
                          <button type="button" className="settle-direction-btn up" disabled={isSettling} onClick={() => void settle(item, "UP")}>
                            {isSettling ? t("loading") : "Settle UP"}
                          </button>
                          <button type="button" className="settle-direction-btn down" disabled={isSettling} onClick={() => void settle(item, "DOWN")}>
                            {isSettling ? t("loading") : "Settle DN"}
                          </button>
                        </div>
                      </div>
                    </article>
                  );
                })}
              </div>
            )}

            <div className="manual-settlement-dialog-footer">
              <button type="button" className="secondary-button" onClick={() => setIsOpen(false)}>
                {t("close")}
              </button>
            </div>
          </section>
        </div>
      ) : null}
    </div>
  );
}

-- Add a database-level idempotency ledger for settlement redeem.

CREATE TABLE IF NOT EXISTS redeem_ledger (
  id TEXT PRIMARY KEY,
  round_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  position_id TEXT NOT NULL,
  redeem_amount_usdc DOUBLE PRECISION NOT NULL,
  realized_pnl_usdc DOUBLE PRECISION NOT NULL,
  settlement_result TEXT NOT NULL,
  created_at_ms BIGINT NOT NULL,
  details JSONB,
  UNIQUE (round_id, user_id, position_id)
);

CREATE INDEX IF NOT EXISTS idx_redeem_ledger_round_user ON redeem_ledger(round_id, user_id);

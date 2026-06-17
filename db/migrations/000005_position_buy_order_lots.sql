ALTER TABLE positions ADD COLUMN IF NOT EXISTS buy_order_id TEXT;

CREATE INDEX IF NOT EXISTS idx_positions_buy_order_id
  ON positions(buy_order_id);

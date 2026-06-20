CREATE INDEX IF NOT EXISTS idx_rounds_status_end_at ON rounds(status, end_at DESC);
CREATE INDEX IF NOT EXISTS idx_positions_round_status_side ON positions(round_id, status, side);
CREATE INDEX IF NOT EXISTS idx_orders_round_status ON orders(round_id, status);

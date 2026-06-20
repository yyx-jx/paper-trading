export const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  username TEXT UNIQUE NOT NULL,
  password TEXT NOT NULL,
  display_name TEXT NOT NULL,
  role TEXT NOT NULL,
  language TEXT NOT NULL,
  permission_codes JSONB NOT NULL,
  available_usdc DOUBLE PRECISION NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  senior_tester_id TEXT,
  disabled_at BIGINT,
  disabled_by TEXT,
  manager_user_id TEXT,
  permission_level TEXT NOT NULL DEFAULT 'Standard',
  failed_login_count INTEGER NOT NULL DEFAULT 0,
  locked_until BIGINT,
  password_changed_at BIGINT,
  last_login_at BIGINT,
  must_change_password BOOLEAN NOT NULL DEFAULT FALSE,
  data_version INTEGER NOT NULL DEFAULT 1,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS rounds (
  id TEXT PRIMARY KEY,
  market_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  event_id TEXT,
  market_slug TEXT,
  event_slug TEXT,
  condition_id TEXT,
  series_slug TEXT,
  up_token_id TEXT,
  down_token_id TEXT,
  title TEXT,
  resolution_source TEXT,
  start_at BIGINT NOT NULL,
  end_at BIGINT NOT NULL,
  price_to_beat DOUBLE PRECISION NOT NULL,
  price_to_beat_source TEXT,
  price_to_beat_captured_at BIGINT,
  status TEXT NOT NULL,
  poll_count INTEGER NOT NULL,
  poll_start_at BIGINT,
  last_poll_at BIGINT,
  closing_spot_price DOUBLE PRECISION,
  settled_side TEXT,
  settlement_price DOUBLE PRECISION,
  settlement_ts BIGINT,
  redeem_start_ts BIGINT,
  redeem_finish_ts BIGINT,
  manual_reason TEXT,
  accepting_orders BOOLEAN,
  closing_price_source TEXT,
  settlement_source TEXT,
  polymarket_settlement_price DOUBLE PRECISION,
  polymarket_settlement_status TEXT,
  polymarket_open_price DOUBLE PRECISION,
  polymarket_close_price DOUBLE PRECISION,
  polymarket_open_price_source TEXT,
  polymarket_close_price_source TEXT,
  settlement_received_at BIGINT,
  redeem_scheduled_at BIGINT,
  binance_open_price DOUBLE PRECISION,
  binance_close_price DOUBLE PRECISION,
  chainlink_open_price DOUBLE PRECISION,
  chainlink_close_price DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS market_candles (
  source TEXT NOT NULL,
  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,
  open_ts BIGINT NOT NULL,
  close_ts BIGINT NOT NULL,
  open DOUBLE PRECISION NOT NULL,
  high DOUBLE PRECISION NOT NULL,
  low DOUBLE PRECISION NOT NULL,
  close DOUBLE PRECISION NOT NULL,
  volume DOUBLE PRECISION NOT NULL DEFAULT 0,
  origin TEXT NOT NULL,
  updated_at BIGINT NOT NULL,
  PRIMARY KEY (source, symbol, interval, open_ts),
  CHECK (open_ts % 30000 = 0),
  CHECK (close_ts = open_ts + 30000)
);

CREATE INDEX IF NOT EXISTS idx_market_candles_lookup
  ON market_candles(source, symbol, interval, open_ts DESC);

CREATE TABLE IF NOT EXISTS order_book_snapshots (
  ref TEXT PRIMARY KEY,
  snapshot_id TEXT NOT NULL,
  snapshot_ts BIGINT NOT NULL,
  best_bid DOUBLE PRECISION NOT NULL,
  best_ask DOUBLE PRECISION NOT NULL,
  mid_price DOUBLE PRECISION NOT NULL,
  snapshot JSONB NOT NULL,
  created_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
  id TEXT PRIMARY KEY,
  trace_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  round_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  market_id TEXT NOT NULL,
  order_kind TEXT,
  time_in_force TEXT,
  limit_price DOUBLE PRECISION,
  lifecycle_status TEXT,
  result_type TEXT,
  token_id TEXT,
  book_key TEXT,
  book_hash TEXT,
  requested_amount_usdc DOUBLE PRECISION,
  requested_qty DOUBLE PRECISION,
  frozen_usdc DOUBLE PRECISION,
  frozen_qty DOUBLE PRECISION,
  fills JSONB,
  estimated_fee DOUBLE PRECISION,
  actual_fee DOUBLE PRECISION,
  fee_breakdown JSONB,
  fee_currency TEXT,
  source_latency_ms DOUBLE PRECISION,
  market_slug TEXT,
  order_book_snapshot_ref TEXT,
  order_book_snapshot JSONB,
  action TEXT NOT NULL,
  side TEXT NOT NULL,
  status TEXT NOT NULL,
  notional_usdc DOUBLE PRECISION NOT NULL,
  expected_qty DOUBLE PRECISION NOT NULL,
  filled_qty DOUBLE PRECISION NOT NULL,
  unfilled_qty DOUBLE PRECISION NOT NULL,
  avg_fill_price DOUBLE PRECISION,
  best_bid DOUBLE PRECISION NOT NULL,
  best_ask DOUBLE PRECISION NOT NULL,
  mid_price DOUBLE PRECISION NOT NULL,
  book_snapshot_ts BIGINT NOT NULL,
  partial_filled BOOLEAN NOT NULL,
  slippage_bps DOUBLE PRECISION,
  match_latency_ms DOUBLE PRECISION NOT NULL,
  book_acquire_latency_ms DOUBLE PRECISION,
  local_match_latency_ms DOUBLE PRECISION,
  persist_latency_ms DOUBLE PRECISION,
  total_order_latency_ms DOUBLE PRECISION,
  failure_reason TEXT,
  client_order_id TEXT,
  client_send_ts BIGINT,
  server_recv_ts BIGINT NOT NULL,
  server_publish_ts BIGINT NOT NULL,
  created_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS order_lifecycle_logs (
  id TEXT PRIMARY KEY,
  buy_order_id TEXT UNIQUE NOT NULL,
  trace_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  tester_id TEXT NOT NULL,
  round_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  asset_class TEXT NOT NULL,
  market_id TEXT NOT NULL,
  market_slug TEXT,
  direction TEXT NOT NULL,
  order_timestamp_ms BIGINT NOT NULL,
  entry_token_price DOUBLE PRECISION,
  btc_trade_price DOUBLE PRECISION,
  btc_open_price_to_beat DOUBLE PRECISION,
  delta_btc DOUBLE PRECISION,
  volume_token_qty DOUBLE PRECISION NOT NULL,
  remaining_token_qty DOUBLE PRECISION NOT NULL,
  closed_token_qty DOUBLE PRECISION NOT NULL,
  position_notional DOUBLE PRECISION NOT NULL,
  exit_type TEXT,
  exit_token_price DOUBLE PRECISION,
  exit_notional DOUBLE PRECISION NOT NULL,
  settlement_result TEXT,
  order_book_snapshot_ref TEXT,
  actual_fill_price DOUBLE PRECISION,
  slippage_bps DOUBLE PRECISION,
  match_latency_ms DOUBLE PRECISION NOT NULL,
  settlement_time_ms BIGINT,
  settlement_direction TEXT,
  entry_fee DOUBLE PRECISION,
  exit_fee DOUBLE PRECISION,
  fee_currency TEXT,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS positions (
  id TEXT PRIMARY KEY,
  buy_order_id TEXT,
  user_id TEXT NOT NULL,
  round_id TEXT NOT NULL,
  side TEXT NOT NULL,
  qty DOUBLE PRECISION NOT NULL,
  locked_qty DOUBLE PRECISION,
  average_entry DOUBLE PRECISION NOT NULL,
  notional_spent DOUBLE PRECISION NOT NULL,
  current_mark DOUBLE PRECISION NOT NULL,
  current_bid DOUBLE PRECISION,
  current_ask DOUBLE PRECISION,
  current_mid DOUBLE PRECISION,
  current_value DOUBLE PRECISION,
  source_latency_ms DOUBLE PRECISION,
  unrealized_pnl DOUBLE PRECISION NOT NULL,
  realized_pnl DOUBLE PRECISION NOT NULL,
  entry_fee_usdc DOUBLE PRECISION,
  exit_fee_usdc DOUBLE PRECISION,
  total_fee_usdc DOUBLE PRECISION,
  cost_basis_usdc DOUBLE PRECISION,
  mark_pnl_usdc DOUBLE PRECISION,
  executable_pnl_usdc DOUBLE PRECISION,
  data_version INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL,
  opened_at BIGINT NOT NULL,
  closed_at BIGINT,
  settlement_result TEXT
);

CREATE TABLE IF NOT EXISTS audit_events (
  event_id TEXT PRIMARY KEY,
  trace_id TEXT NOT NULL,
  category TEXT NOT NULL,
  action_type TEXT NOT NULL,
  action_status TEXT NOT NULL,
  user_id TEXT,
  role TEXT,
  page_name TEXT NOT NULL,
  module_name TEXT NOT NULL,
  symbol TEXT,
  round_id TEXT,
  result_code TEXT NOT NULL,
  result_message TEXT NOT NULL,
  client_send_ts BIGINT,
  server_recv_ts BIGINT NOT NULL,
  engine_start_ts BIGINT,
  engine_finish_ts BIGINT,
  server_publish_ts BIGINT NOT NULL,
  backend_latency_ms DOUBLE PRECISION NOT NULL,
  frontend_latency_ms DOUBLE PRECISION,
  details JSONB
);

CREATE TABLE IF NOT EXISTS behavior_action_logs (
  log_id TEXT PRIMARY KEY,
  timestamp_ms BIGINT NOT NULL,
  asset_class TEXT NOT NULL,
  action_type TEXT NOT NULL,
  action_status TEXT NOT NULL,
  round_id TEXT,
  direction TEXT,
  entry_odds DOUBLE PRECISION,
  delta_clob DOUBLE PRECISION NOT NULL,
  volume_clob DOUBLE PRECISION NOT NULL,
  position_notional DOUBLE PRECISION,
  exit_type TEXT,
  exit_odds DOUBLE PRECISION,
  settlement_result TEXT,
  tester_id_anon TEXT NOT NULL,
  trace_id TEXT,
  order_id TEXT,
  market_id TEXT,
  market_slug TEXT,
  round_status TEXT,
  countdown_ms BIGINT,
  binance_spot_price DOUBLE PRECISION NOT NULL,
  binance_1m_last_close DOUBLE PRECISION NOT NULL,
  binance_5m_last_close DOUBLE PRECISION NOT NULL,
  binance_1d_last_close DOUBLE PRECISION NOT NULL,
  chainlink_price DOUBLE PRECISION NOT NULL,
  price_to_beat DOUBLE PRECISION NOT NULL,
  up_price DOUBLE PRECISION NOT NULL,
  down_price DOUBLE PRECISION NOT NULL,
  up_book_top5 JSONB NOT NULL,
  down_book_top5 JSONB NOT NULL,
  recent_trades_top20 JSONB NOT NULL,
  book_snapshot_entry JSONB NOT NULL,
  actual_fill_price DOUBLE PRECISION,
  slippage_bps DOUBLE PRECISION,
  partial_filled BOOLEAN,
  unfilled_qty DOUBLE PRECISION,
  execution_latency_ms DOUBLE PRECISION,
  settlement_direction TEXT,
  settlement_time_ms BIGINT,
  gamma_poll_count INTEGER,
  redeem_finish_time_ms BIGINT,
  source_states JSONB NOT NULL,
  strategy_cluster_label TEXT,
  market_regime_label TEXT,
  quality_grade TEXT,
  context_json JSONB
);

CREATE TABLE IF NOT EXISTS export_audit_logs (
  export_id TEXT PRIMARY KEY,
  actor_user_id TEXT NOT NULL,
  actor_role TEXT NOT NULL,
  export_type TEXT NOT NULL,
  format TEXT NOT NULL,
  scope JSONB NOT NULL,
  record_count INTEGER NOT NULL,
  filtered_d_grade_count INTEGER NOT NULL,
  missing_quality_count INTEGER NOT NULL,
  file_sha256 TEXT NOT NULL,
  created_at_ms BIGINT NOT NULL,
  details JSONB
);

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

CREATE INDEX IF NOT EXISTS idx_rounds_start_at ON rounds(start_at DESC);
CREATE INDEX IF NOT EXISTS idx_rounds_status_end_at ON rounds(status, end_at DESC);
CREATE INDEX IF NOT EXISTS idx_order_book_snapshots_ts ON order_book_snapshots(snapshot_ts DESC);
CREATE INDEX IF NOT EXISTS idx_orders_user_created ON orders(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_round_status ON orders(round_id, status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_user_client_order_id ON orders(user_id, client_order_id) WHERE client_order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_order_lifecycle_user_time ON order_lifecycle_logs(user_id, order_timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_order_lifecycle_round ON order_lifecycle_logs(round_id, direction, order_timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_positions_user_opened ON positions(user_id, opened_at DESC);
CREATE INDEX IF NOT EXISTS idx_positions_buy_order_id ON positions(buy_order_id);
CREATE INDEX IF NOT EXISTS idx_positions_round_status_side ON positions(round_id, status, side);
CREATE INDEX IF NOT EXISTS idx_audit_events_user_recv ON audit_events(user_id, server_recv_ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_round_recv ON audit_events(round_id, server_recv_ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_category_action_recv ON audit_events(category, action_type, server_recv_ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_trace_recv ON audit_events(trace_id, server_recv_ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_module_recv ON audit_events(module_name, server_recv_ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_latency_recv ON audit_events(category, module_name, backend_latency_ms, server_recv_ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_details_order_id ON audit_events((details->>'orderId'));
CREATE INDEX IF NOT EXISTS idx_audit_events_details_position_id ON audit_events((details->>'positionId'));
CREATE INDEX IF NOT EXISTS idx_audit_events_details_market_id ON audit_events((details->>'marketId'));
CREATE INDEX IF NOT EXISTS idx_audit_events_details_market_slug ON audit_events((details->>'marketSlug'));
CREATE INDEX IF NOT EXISTS idx_audit_events_details_connection_state ON audit_events((details->>'connectionState'), server_recv_ts DESC);
CREATE INDEX IF NOT EXISTS idx_behavior_logs_timestamp ON behavior_action_logs(timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_behavior_logs_user_round ON behavior_action_logs(tester_id_anon, round_id, timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_behavior_logs_market_time ON behavior_action_logs(market_id, timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_behavior_logs_trace_time ON behavior_action_logs(trace_id, timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_behavior_logs_order_time ON behavior_action_logs(order_id, timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_behavior_logs_action_time ON behavior_action_logs(action_type, action_status, timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_behavior_logs_round_status_time ON behavior_action_logs(round_status, timestamp_ms DESC);

ALTER TABLE users ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS senior_tester_id TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS disabled_at BIGINT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS disabled_by TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS manager_user_id TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS permission_level TEXT NOT NULL DEFAULT 'Standard';
ALTER TABLE users ADD COLUMN IF NOT EXISTS failed_login_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE users ADD COLUMN IF NOT EXISTS locked_until BIGINT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_changed_at BIGINT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_login_at BIGINT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS must_change_password BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS data_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE users ADD COLUMN IF NOT EXISTS updated_at BIGINT;
UPDATE users SET updated_at = created_at WHERE updated_at IS NULL;
UPDATE users SET manager_user_id = senior_tester_id WHERE manager_user_id IS NULL AND senior_tester_id IS NOT NULL;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS buy_order_id TEXT;
CREATE INDEX IF NOT EXISTS idx_users_senior_tester ON users(senior_tester_id);
CREATE INDEX IF NOT EXISTS idx_users_manager_user ON users(manager_user_id);
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS data_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS settlement_source TEXT;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS polymarket_settlement_price DOUBLE PRECISION;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS polymarket_settlement_status TEXT;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS polymarket_open_price DOUBLE PRECISION;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS polymarket_close_price DOUBLE PRECISION;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS polymarket_open_price_source TEXT;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS polymarket_close_price_source TEXT;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS settlement_received_at BIGINT;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS redeem_scheduled_at BIGINT;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS binance_open_price DOUBLE PRECISION;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS binance_close_price DOUBLE PRECISION;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS chainlink_open_price DOUBLE PRECISION;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS chainlink_close_price DOUBLE PRECISION;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS price_to_beat_source TEXT;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS price_to_beat_captured_at BIGINT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS order_kind TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS time_in_force TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS limit_price DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS lifecycle_status TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS result_type TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS token_id TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS book_key TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS book_hash TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS requested_amount_usdc DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS requested_qty DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS frozen_usdc DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS frozen_qty DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS fills JSONB;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS estimated_fee DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS actual_fee DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS fee_breakdown JSONB;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS fee_currency TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS source_latency_ms DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS market_slug TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS order_book_snapshot_ref TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS order_book_snapshot JSONB;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS book_acquire_latency_ms DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS local_match_latency_ms DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS persist_latency_ms DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS total_order_latency_ms DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS client_order_id TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS data_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS locked_qty DOUBLE PRECISION;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS current_bid DOUBLE PRECISION;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS current_ask DOUBLE PRECISION;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS current_mid DOUBLE PRECISION;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS current_value DOUBLE PRECISION;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS source_latency_ms DOUBLE PRECISION;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS entry_fee_usdc DOUBLE PRECISION;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS exit_fee_usdc DOUBLE PRECISION;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS total_fee_usdc DOUBLE PRECISION;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS cost_basis_usdc DOUBLE PRECISION;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS mark_pnl_usdc DOUBLE PRECISION;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS executable_pnl_usdc DOUBLE PRECISION;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS data_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE order_lifecycle_logs ADD COLUMN IF NOT EXISTS entry_fee DOUBLE PRECISION;
ALTER TABLE order_lifecycle_logs ADD COLUMN IF NOT EXISTS exit_fee DOUBLE PRECISION;
ALTER TABLE order_lifecycle_logs ADD COLUMN IF NOT EXISTS fee_currency TEXT;
ALTER TABLE order_lifecycle_logs ADD COLUMN IF NOT EXISTS data_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS data_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE behavior_action_logs ADD COLUMN IF NOT EXISTS data_version INTEGER NOT NULL DEFAULT 1;
CREATE INDEX IF NOT EXISTS idx_export_audit_logs_actor_created ON export_audit_logs(actor_user_id, created_at_ms DESC);
CREATE INDEX IF NOT EXISTS idx_redeem_ledger_round_user ON redeem_ledger(round_id, user_id);
`;

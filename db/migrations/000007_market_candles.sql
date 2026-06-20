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

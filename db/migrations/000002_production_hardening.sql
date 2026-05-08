-- Production hardening expand migration.
-- Safe for existing databases; it only adds compatible columns, indexes, and
-- the export audit table.

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'users') THEN
    ALTER TABLE users ADD COLUMN IF NOT EXISTS manager_user_id TEXT;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS permission_level TEXT NOT NULL DEFAULT 'Standard';
    ALTER TABLE users ADD COLUMN IF NOT EXISTS failed_login_count INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS locked_until BIGINT;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS password_changed_at BIGINT;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS last_login_at BIGINT;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS must_change_password BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS data_version INTEGER NOT NULL DEFAULT 1;
    UPDATE users SET manager_user_id = senior_tester_id WHERE manager_user_id IS NULL AND senior_tester_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_users_manager_user ON users(manager_user_id);
  END IF;

  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'rounds') THEN
    ALTER TABLE rounds ADD COLUMN IF NOT EXISTS data_version INTEGER NOT NULL DEFAULT 1;
  END IF;

  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'orders') THEN
    ALTER TABLE orders ADD COLUMN IF NOT EXISTS data_version INTEGER NOT NULL DEFAULT 1;
  END IF;

  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'positions') THEN
    ALTER TABLE positions ADD COLUMN IF NOT EXISTS entry_fee_usdc DOUBLE PRECISION;
    ALTER TABLE positions ADD COLUMN IF NOT EXISTS exit_fee_usdc DOUBLE PRECISION;
    ALTER TABLE positions ADD COLUMN IF NOT EXISTS total_fee_usdc DOUBLE PRECISION;
    ALTER TABLE positions ADD COLUMN IF NOT EXISTS cost_basis_usdc DOUBLE PRECISION;
    ALTER TABLE positions ADD COLUMN IF NOT EXISTS mark_pnl_usdc DOUBLE PRECISION;
    ALTER TABLE positions ADD COLUMN IF NOT EXISTS executable_pnl_usdc DOUBLE PRECISION;
    ALTER TABLE positions ADD COLUMN IF NOT EXISTS data_version INTEGER NOT NULL DEFAULT 1;
    UPDATE positions
      SET cost_basis_usdc = notional_spent
      WHERE cost_basis_usdc IS NULL;
  END IF;

  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'audit_events') THEN
    ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS data_version INTEGER NOT NULL DEFAULT 1;
  END IF;

  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'behavior_action_logs') THEN
    ALTER TABLE behavior_action_logs ADD COLUMN IF NOT EXISTS data_version INTEGER NOT NULL DEFAULT 1;
  END IF;

  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'order_lifecycle_logs') THEN
    ALTER TABLE order_lifecycle_logs ADD COLUMN IF NOT EXISTS data_version INTEGER NOT NULL DEFAULT 1;
  END IF;
END $$;

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

CREATE INDEX IF NOT EXISTS idx_export_audit_logs_actor_created ON export_audit_logs(actor_user_id, created_at_ms DESC);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'positions') THEN
    CREATE INDEX IF NOT EXISTS idx_positions_user_status ON positions(user_id, status);
    CREATE INDEX IF NOT EXISTS idx_positions_round_side ON positions(round_id, side);
  END IF;
END $$;

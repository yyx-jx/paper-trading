-- Adds forward-compatible user management fields.
-- This migration is intentionally safe on older development databases where the
-- users table may still be created by the current AppStore bootstrap.

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'users'
  ) THEN
    ALTER TABLE users ADD COLUMN IF NOT EXISTS manager_user_id TEXT;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS permission_level TEXT NOT NULL DEFAULT 'Standard';
    UPDATE users
      SET manager_user_id = senior_tester_id
      WHERE manager_user_id IS NULL
        AND senior_tester_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_users_manager_user ON users(manager_user_id);
  END IF;
END $$;

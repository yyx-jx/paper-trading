ALTER TABLE orders ADD COLUMN IF NOT EXISTS client_order_id TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_user_client_order_id
  ON orders(user_id, client_order_id)
  WHERE client_order_id IS NOT NULL;

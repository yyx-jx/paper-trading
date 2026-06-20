export const QTY_EPSILON = 0.0001;

export const roundNumber = (value: number, digits = 2) => Number(value.toFixed(digits));
export const roundCurrency = (value: number) => roundNumber(value, 6);

export function isClientOrderConflict(error: unknown) {
  if (!error || typeof error !== "object") {
    return false;
  }
  const candidate = error as { code?: unknown; constraint?: unknown; detail?: unknown; message?: unknown };
  if (candidate.code !== "23505") {
    return false;
  }
  return [candidate.constraint, candidate.detail, candidate.message]
    .filter(Boolean)
    .some((value) => String(value).includes("idx_orders_user_client_order_id"));
}

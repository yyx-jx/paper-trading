import type { PolymarketMarketDetail } from "../../domain/types";

export const PRELIMINARY_SETTLEMENT_THRESHOLD = 0.9;
export const GAMMA_SETTLED_WIN_PRICE_THRESHOLD = 0.995;
export const GAMMA_SETTLED_LOSE_PRICE_THRESHOLD = 1 - GAMMA_SETTLED_WIN_PRICE_THRESHOLD;

export function isMarketResolved(detail: PolymarketMarketDetail): boolean {
  if (detail.automaticallyResolved) return true;
  if (detail.winningTokenId) return true;
  if (detail.winningOutcome) return true;
  if (detail.closed) {
    const [up, down] = detail.outcomePrices;
    if (up >= GAMMA_SETTLED_WIN_PRICE_THRESHOLD && down <= GAMMA_SETTLED_LOSE_PRICE_THRESHOLD) return true;
    if (down >= GAMMA_SETTLED_WIN_PRICE_THRESHOLD && up <= GAMMA_SETTLED_LOSE_PRICE_THRESHOLD) return true;
  }
  return false;
}

import type { FeeBreakdown } from "../domain/types";

const QTY_EPSILON = 0.0000001;

const roundNumber = (value: number, digits = 8) => Number(value.toFixed(digits));

export interface ClobFeeInput {
  role: "maker" | "taker";
  price?: number;
  quantity: number;
  notional: number;
  feeRate?: number;
  platformFeeRate?: number;
  platformFeeExponent?: number;
  platformFeeTakerOnly?: boolean;
  digits?: number;
}

export function calculateClobFees(input: ClobFeeInput): { fee: number; breakdown?: FeeBreakdown } {
  const price = input.price ?? input.notional / Math.max(input.quantity, QTY_EPSILON);
  const platformFeeRate = Math.max(input.platformFeeRate ?? input.feeRate ?? 0, 0);
  const platformFeeExponent = Number.isFinite(input.platformFeeExponent) ? input.platformFeeExponent : 1;
  const platformFeeTakerOnly = input.platformFeeTakerOnly ?? true;
  const platformFeeApplies = input.role === "taker" || !platformFeeTakerOnly;
  const platformFee =
    input.quantity > QTY_EPSILON && price > 0 && platformFeeApplies
      ? input.quantity * platformFeeRate * price * Math.max(1 - price, 0)
      : 0;
  const totalFee = roundNumber(platformFee, input.digits ?? 8);
  return {
    fee: totalFee,
    breakdown:
      input.quantity > QTY_EPSILON
        ? {
            role: input.role,
            feeRate: platformFeeRate,
            formula: "C * feeRate * p * (1 - p)",
            price: roundNumber(price, 8),
            quantity: roundNumber(input.quantity, 8),
            notional: roundNumber(input.notional, 8),
            platformFee: roundNumber(platformFee, input.digits ?? 8),
            platformFeeExponent,
            platformFeeTakerOnly,
            totalFee,
            amount: totalFee
          }
        : undefined
  };
}

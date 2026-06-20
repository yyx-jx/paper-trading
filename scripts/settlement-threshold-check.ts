import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import {
  GAMMA_SETTLED_WIN_PRICE_THRESHOLD,
  isMarketResolved,
  resolveExactSettledSideFromOutcomePrices
} from "../apps/server/src/services/simulation/settlement-rules";
import type { PolymarketMarketDetail } from "../apps/server/src/domain/types";

assert.equal(GAMMA_SETTLED_WIN_PRICE_THRESHOLD, 0.995);
assert.equal(resolveExactSettledSideFromOutcomePrices([0.995, 0.3]), "UP");
assert.equal(resolveExactSettledSideFromOutcomePrices([0.2, 0.999]), "DOWN");
assert.equal(resolveExactSettledSideFromOutcomePrices([0.9949, 0.4]), undefined);

const detail: PolymarketMarketDetail = {
  id: "market_1",
  conditionId: "condition_1",
  slug: "btc-updown-5m-test",
  title: "BTC Up Down Test",
  eventSlug: "btc-event",
  seriesSlug: "btc-series",
  endDate: new Date().toISOString(),
  resolutionSource: "Gamma",
  bestBid: 0,
  bestAsk: 0,
  lastTradePrice: 0,
  minimumTickSize: 0.01,
  minimumOrderSize: 1,
  feeRateBps: 0,
  feeDetails: undefined,
  feeSchedule: undefined,
  rfqEnabled: false,
  acceptingOrders: false,
  closed: true,
  automaticallyResolved: false,
  outcomes: ["Up", "Down"],
  upOutcome: "Up",
  downOutcome: "Down",
  outcomePrices: [0.997, 0.22],
  clobTokenIds: ["token_up", "token_down"],
  upTokenId: "token_up",
  downTokenId: "token_down",
  eventStartTime: undefined,
  upPrice: 0.997,
  downPrice: 0.22,
  midpoint: 0.6085,
  settlementStatus: "pending",
  settlementPrice: undefined
};

assert.equal(isMarketResolved(detail), true);

const simulationSource = readFileSync("apps/server/src/services/simulation.ts", "utf8");
assert.match(simulationSource, /return resolveExactSettledSideFromOutcomePrices\(detail\.outcomePrices\);/);
assert.match(simulationSource, /Gamma exact outcome price threshold confirmed settlement\./);
assert.doesNotMatch(simulationSource, /confirmExactGammaOutcome/);
assert.doesNotMatch(simulationSource, /resolved market detail confirmed settlement/);
assert.doesNotMatch(simulationSource, /consecutive polls/);

console.log("settlement-threshold-check ok");

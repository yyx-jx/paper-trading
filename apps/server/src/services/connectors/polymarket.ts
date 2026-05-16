import { nanoid } from "nanoid";
import WebSocket from "ws";
import type { Agent } from "node:http";
import { createProxyDispatcher, createProxyWsAgent, fetchJsonWithTimeout } from "./network";
import {
  createPolymarketComponent,
  derivePolymarketSourceHealth,
  type PolymarketHealthComponent,
  type PolymarketHealthComponents
} from "./polymarket-health";
import type {
  ClobMarketInfo,
  MarketTrade,
  OrderBookSnapshot,
  PolymarketConnectorState,
  PolymarketMarketDetail,
  RoundRecord,
  SourceHealth,
  TradeSide
} from "../../domain/types";

interface SearchEvent {
  id: string;
  slug: string;
  title: string;
  endDate: string;
  markets?: Array<{
    slug?: string;
  }>;
}

interface DetailedMarketPayload {
  id: string;
  conditionId: string;
  slug: string;
  question: string;
  endDate: string;
  eventStartTime?: string;
  resolutionSource?: string;
  bestBid?: number;
  bestAsk?: number;
  lastTradePrice?: number;
  minimum_tick_size?: number | string;
  minimumTickSize?: number | string;
  minimum_order_size?: number | string;
  minimumOrderSize?: number | string;
  fee_rate_bps?: number | string;
  feeRateBps?: number | string;
  fee_details?: Record<string, unknown>;
  feeDetails?: Record<string, unknown>;
  fee_schedule?: {
    rate?: number | string;
    exponent?: number | string;
    takerOnly?: boolean | string;
    taker_only?: boolean | string;
  };
  feeSchedule?: {
    rate?: number | string;
    exponent?: number | string;
    takerOnly?: boolean | string;
    taker_only?: boolean | string;
  };
  rfq_enabled?: boolean;
  rfqEnabled?: boolean;
  acceptingOrders?: boolean;
  closed?: boolean;
  winner?: string;
  winningOutcome?: string;
  winningTokenId?: string;
  resolutionOutcome?: string;
  automaticallyResolved?: boolean;
  outcomes: string;
  outcomePrices: string;
  clobTokenIds: string;
  tokens?: Array<{
    token_id?: string;
    tokenId?: string;
    id?: string;
    outcome?: string;
    title?: string;
    price?: number | string;
  }>;
  events?: Array<{
    id: string;
    slug: string;
    title: string;
    seriesSlug?: string;
    startTime?: string;
    startDate?: string;
  }>;
}

const MARKET_WS_STALE_MS = 2500;
const MARKET_WS_RECONNECT_MS = 1000;

function initialHealthComponents(now = Date.now()): PolymarketHealthComponents {
  return {
    discovery: createPolymarketComponent({
      name: "discovery",
      state: "reconnecting",
      sourceEventTs: now,
      serverRecvTs: now,
      message: "Waiting for Polymarket market discovery."
    }),
    orderBook: createPolymarketComponent({
      name: "orderBook",
      state: "reconnecting",
      sourceEventTs: 0,
      serverRecvTs: now,
      message: "Waiting for Polymarket order books."
    }),
    marketWs: createPolymarketComponent({
      name: "marketWs",
      state: "reconnecting",
      sourceEventTs: 0,
      serverRecvTs: now,
      message: "Waiting for Polymarket market WebSocket."
    }),
    trades: createPolymarketComponent({
      name: "trades",
      state: "reconnecting",
      sourceEventTs: 0,
      serverRecvTs: now,
      message: "Waiting for Polymarket recent trades."
    })
  };
}

function emptyBook(side: TradeSide): OrderBookSnapshot {
  const now = Date.now();
  return {
    snapshotId: `empty_${side}_${now}`,
    snapshotTs: now,
    bestBid: 0,
    bestAsk: 0,
    midPrice: 0,
    bids: [],
    asks: []
  };
}

function normalizeWsTimestamp(value: unknown, fallback = Date.now()) {
  if (typeof value === "string") {
    const trimmed = value.trim();
    const numeric = Number(trimmed);
    if (Number.isFinite(numeric)) {
      return normalizeWsTimestamp(numeric, fallback);
    }
    const parsed = Date.parse(trimmed);
    return Number.isFinite(parsed) ? parsed : fallback;
  }
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    return fallback;
  }
  return value < 10_000_000_000 ? Math.round(value * 1000) : Math.round(value);
}

function recomputeBookTop(book: Pick<OrderBookSnapshot, "bids" | "asks">) {
  const bids = [...book.bids]
    .filter((level) => level.price > 0 && level.qty > 0)
    .sort((left, right) => right.price - left.price);
  const asks = [...book.asks]
    .filter((level) => level.price > 0 && level.qty > 0)
    .sort((left, right) => left.price - right.price);
  const bestBid = bids[0]?.price ?? 0;
  const bestAsk = asks[0]?.price ?? 0;
  return {
    bids,
    asks,
    bestBid,
    bestAsk,
    midPrice: bestBid && bestAsk ? Number(((bestBid + bestAsk) / 2).toFixed(4)) : bestBid || bestAsk
  };
}

function applyTopToBook(
  book: OrderBookSnapshot,
  input: { bestBid?: number; bestAsk?: number; snapshotTs: number; snapshotId?: string }
): OrderBookSnapshot | undefined {
  if (input.snapshotTs < book.snapshotTs) {
    return undefined;
  }
  const bestBid = typeof input.bestBid === "number" && Number.isFinite(input.bestBid) && input.bestBid > 0
    ? input.bestBid
    : book.bestBid;
  const bestAsk = typeof input.bestAsk === "number" && Number.isFinite(input.bestAsk) && input.bestAsk > 0
    ? input.bestAsk
    : book.bestAsk;
  return {
    ...book,
    snapshotId: input.snapshotId ?? `ws_top_${input.snapshotTs}`,
    snapshotTs: input.snapshotTs,
    bestBid,
    bestAsk,
    midPrice: bestBid && bestAsk ? Number(((bestBid + bestAsk) / 2).toFixed(4)) : bestBid || bestAsk
  };
}

function upsertBookLevel(
  levels: Array<{ price: number; qty: number }>,
  input: { price: number; qty: number }
) {
  const nextLevels = levels.filter((level) => Math.abs(level.price - input.price) > 0.0000001);
  if (input.qty > 0) {
    nextLevels.push({ price: input.price, qty: input.qty });
  }
  return nextLevels;
}

function applyPriceChangeToBook(
  book: OrderBookSnapshot,
  input: { side: "BUY" | "SELL"; price: number; qty: number; snapshotTs: number; snapshotId?: string }
): OrderBookSnapshot | undefined {
  if (input.snapshotTs < book.snapshotTs) {
    return undefined;
  }
  const nextBook =
    input.side === "BUY"
      ? { ...book, bids: upsertBookLevel(book.bids, input) }
      : { ...book, asks: upsertBookLevel(book.asks, input) };
  const top = recomputeBookTop(nextBook);
  return {
    ...nextBook,
    ...top,
    snapshotId: input.snapshotId ?? `ws_delta_${input.snapshotTs}`,
    snapshotTs: input.snapshotTs
  };
}

function numberFromUnknown(value: unknown): number | undefined {
  const parsed = typeof value === "string" || typeof value === "number" ? Number(value) : undefined;
  return typeof parsed === "number" && Number.isFinite(parsed) ? parsed : undefined;
}

function decimalFeeRateFromUnknown(value: unknown): number | undefined {
  const parsed = numberFromUnknown(value);
  if (typeof parsed !== "number") {
    return undefined;
  }
  return parsed > 1 ? parsed / 10_000 : parsed;
}

function boolFromUnknown(value: unknown): boolean | undefined {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["true", "1", "yes"].includes(normalized)) return true;
    if (["false", "0", "no"].includes(normalized)) return false;
  }
  return undefined;
}

function conservativeMarketInfo(conditionId?: string): ClobMarketInfo {
  return {
    conditionId,
    minimumTickSize: 0.01,
    minimumOrderSize: 1,
    makerFeeRate: 0,
    takerFeeRate: 0,
    platformFeeRate: 0,
    platformFeeExponent: 1,
    platformFeeTakerOnly: true,
    feeRateAvailable: false,
    source: "conservative",
    conservative: true,
    updatedAt: Date.now()
  };
}

function normalizeMarketInfo(input: Record<string, unknown>, conditionId?: string, source: ClobMarketInfo["source"] = "clob"): ClobMarketInfo {
  const minimumTickSize =
    numberFromUnknown(input.minimum_tick_size) ??
    numberFromUnknown(input.minimumTickSize) ??
    numberFromUnknown(input.mts) ??
    0.01;
  const minimumOrderSize =
    numberFromUnknown(input.minimum_order_size) ??
    numberFromUnknown(input.minimumOrderSize) ??
    numberFromUnknown(input.mos) ??
    1;
  const feeRateBps = numberFromUnknown(input.fee_rate_bps) ?? numberFromUnknown(input.feeRateBps);
  const feeDetails = (input.fee_details ?? input.feeDetails ?? input.fd) as Record<string, unknown> | undefined;
  const feeSchedule = (input.fee_schedule ?? input.feeSchedule) as Record<string, unknown> | undefined;
  const rawPlatformFeeRate =
    decimalFeeRateFromUnknown(feeSchedule?.rate) ??
    decimalFeeRateFromUnknown(feeDetails?.r) ??
    decimalFeeRateFromUnknown(input.platform_fee_rate) ??
    decimalFeeRateFromUnknown(input.platformFeeRate);
  const platformFeeExponent =
    numberFromUnknown(feeSchedule?.exponent) ??
    numberFromUnknown(feeDetails?.e) ??
    numberFromUnknown(input.platformFeeExponent) ??
    1;
  const platformFeeTakerOnly =
    boolFromUnknown(feeSchedule?.takerOnly) ??
    boolFromUnknown(feeSchedule?.taker_only) ??
    boolFromUnknown(feeDetails?.to) ??
    boolFromUnknown(input.platformFeeTakerOnly) ??
    true;
  const rawMakerFeeRate =
    decimalFeeRateFromUnknown(input.maker_fee_rate) ??
    decimalFeeRateFromUnknown(input.makerFeeRate);
  const rawTakerFeeRate =
    rawPlatformFeeRate ??
    decimalFeeRateFromUnknown(input.taker_fee_rate) ??
    decimalFeeRateFromUnknown(input.takerFeeRate) ??
    (typeof feeRateBps === "number" ? feeRateBps / 10_000 : undefined);
  const makerFeeRate = rawMakerFeeRate ?? 0;
  const takerFeeRate = rawTakerFeeRate ?? 0;
  const feeRateAvailable =
    rawPlatformFeeRate !== undefined ||
    rawMakerFeeRate !== undefined ||
    rawTakerFeeRate !== undefined ||
    typeof feeRateBps === "number";
  const rawTokens = Array.isArray(input.tokens) ? input.tokens : Array.isArray(input.t) ? input.t : undefined;
  return {
    conditionId: String(input.condition_id ?? input.conditionId ?? conditionId ?? "") || undefined,
    minimumTickSize,
    minimumOrderSize,
    makerFeeRate,
    takerFeeRate,
    platformFeeRate: takerFeeRate,
    platformFeeExponent,
    platformFeeTakerOnly,
    feeRateAvailable,
    feeRateBps,
    feeDetails,
    tokens: rawTokens
      ?.flatMap((token) => {
        const raw = token as Record<string, unknown>;
        const tokenId = String(raw.token_id ?? raw.tokenId ?? raw.id ?? raw.t ?? "");
        return tokenId
          ? [{
              tokenId,
              outcome: raw.outcome || raw.o ? String(raw.outcome ?? raw.o) : undefined,
              minimumTickSize: numberFromUnknown(raw.minimum_tick_size ?? raw.minimumTickSize),
              minimumOrderSize: numberFromUnknown(raw.minimum_order_size ?? raw.minimumOrderSize)
            }]
          : [];
      }),
    rfqEnabled: Boolean(input.rfq_enabled ?? input.rfqEnabled ?? input.rfqe ?? false),
    source,
    conservative: source === "conservative",
    updatedAt: Date.now()
  };
}

function parseJsonArray<T>(value: string): T[] {
  try {
    return JSON.parse(value) as T[];
  } catch {
    return [];
  }
}

function toFloat(value: number | string | undefined) {
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "string") {
    return Number(value);
  }
  return 0;
}

function normalizeOutcomeText(value?: string) {
  return String(value ?? "").trim().toLowerCase();
}

function outcomeSide(value?: string): TradeSide | undefined {
  const normalized = normalizeOutcomeText(value);
  if (!normalized) {
    return undefined;
  }
  if (normalized === "up" || normalized.includes(" up") || normalized.includes("above")) {
    return "UP";
  }
  if (normalized === "down" || normalized.includes("down") || normalized.includes("below")) {
    return "DOWN";
  }
  return undefined;
}

function normalizeMarketOutcomes(payload: DetailedMarketPayload) {
  const outcomes = parseJsonArray<string>(payload.outcomes);
  const outcomePrices = parseJsonArray<string>(payload.outcomePrices).map((value) => Number(value));
  const clobTokenIds = parseJsonArray<string>(payload.clobTokenIds);
  const rows = outcomes.map((outcome, index) => ({
    side: outcomeSide(outcome),
    outcome,
    tokenId: clobTokenIds[index] ?? "",
    price: toFloat(outcomePrices[index])
  }));

  for (const token of payload.tokens ?? []) {
    const side = outcomeSide(token.outcome ?? token.title);
    if (!side) {
      continue;
    }
    const existing = rows.find((row) => row.side === side);
    if (existing) {
      existing.tokenId = String(token.token_id ?? token.tokenId ?? token.id ?? existing.tokenId);
      existing.outcome = String(token.outcome ?? token.title ?? existing.outcome);
      existing.price = toFloat(token.price ?? existing.price);
    } else {
      rows.push({
        side,
        outcome: String(token.outcome ?? token.title ?? side),
        tokenId: String(token.token_id ?? token.tokenId ?? token.id ?? ""),
        price: toFloat(token.price)
      });
    }
  }

  const fallbackUpIndex = rows.findIndex((row) => row.side === "UP");
  const fallbackDownIndex = rows.findIndex((row) => row.side === "DOWN");
  const up = rows[fallbackUpIndex >= 0 ? fallbackUpIndex : 0] ?? { outcome: "Up", tokenId: clobTokenIds[0] ?? "", price: toFloat(outcomePrices[0]) };
  const down =
    rows[fallbackDownIndex >= 0 ? fallbackDownIndex : 1] ??
    { outcome: "Down", tokenId: clobTokenIds[1] ?? "", price: toFloat(outcomePrices[1]) };

  return {
    upOutcome: up.outcome || "Up",
    downOutcome: down.outcome || "Down",
    upTokenId: up.tokenId || "",
    downTokenId: down.tokenId || "",
    upPrice: toFloat(up.price),
    downPrice: toFloat(down.price)
  };
}

function normalizeText(value?: string) {
  return (value ?? "").toLowerCase();
}

const FIVE_MINUTE_MS = 5 * 60_000;
const BTC_FIVE_MINUTE_MARKET_SLUG = /^btc-updown-5m-\d+$/;
const BTC_CHAINLINK_STREAM_PATH = "/streams/btc-usd";
const swallowWsCloseError = () => {};

function parseBtcFiveMinuteSlugStart(slug?: string) {
  const match = normalizeText(slug).match(/^btc-updown-5m-(\d+)$/);
  if (!match?.[1]) {
    return undefined;
  }
  const seconds = Number(match[1]);
  const startAt = seconds * 1000;
  return Number.isSafeInteger(startAt) && startAt > 0 ? startAt : undefined;
}

function marketSlugForStart(startAt: number) {
  return `btc-updown-5m-${Math.floor(startAt / 1000)}`;
}

function currentFiveMinuteStart(now = Date.now()) {
  return Math.floor(now / FIVE_MINUTE_MS) * FIVE_MINUTE_MS;
}

function hasFiveMinuteSignature(value: string) {
  return ["5m", "5-minute", "5 minute", "5min", "5 min"].some((token) => value.includes(token));
}

export class PolymarketConnector {
  private discoveryTimer?: NodeJS.Timeout;
  private booksTimer?: NodeJS.Timeout;
  private tradesTimer?: NodeJS.Timeout;
  private marketWsStaleTimer?: NodeJS.Timeout;
  private marketWsReconnectTimer?: NodeJS.Timeout;
  private marketWs?: WebSocket;
  private subscribedAssetKey = "";
  private reconnectCount = 0;
  private lastMarketWsMessageAt = 0;
  private healthComponents: PolymarketHealthComponents;
  private readonly listeners = new Set<(state: PolymarketConnectorState) => void>();
  private state: PolymarketConnectorState;
  private readonly proxyDispatcher;
  private readonly proxyWsAgent;

  constructor(
    private readonly config: {
      symbol: string;
      gammaBaseUrl: string;
      clobBaseUrl: string;
      dataApiBaseUrl: string;
      marketId?: string;
      marketSlug?: string;
      searchQuery: string;
      seriesSlug: string;
      discoveryKeywords: string[];
      discoveryTimeoutMs: number;
      discoveryIntervalMs: number;
      bookPollMs: number;
      tradesPollMs: number;
      upstreamProxyUrl?: string;
    }
  ) {
    this.proxyDispatcher = createProxyDispatcher(config.upstreamProxyUrl);
    this.proxyWsAgent = createProxyWsAgent(config.upstreamProxyUrl);
    this.healthComponents = initialHealthComponents();
    this.state = {
      nextMarket: undefined,
      discoveredRounds: [],
      orderBooks: {
        UP: emptyBook("UP"),
        DOWN: emptyBook("DOWN")
      },
      recentTrades: [],
      delta: 0,
      volume: 0,
      resolvedMarkets: [],
      status: this.deriveStatus()
    };
  }

  start() {
    void this.discoverRounds();
    this.discoveryTimer = setInterval(() => {
      void this.discoverRounds();
    }, this.config.discoveryIntervalMs);
    this.booksTimer = setInterval(() => {
      void this.refreshBooks();
    }, this.config.bookPollMs);
    this.tradesTimer = setInterval(() => {
      void this.refreshTrades();
    }, this.config.tradesPollMs);
    this.marketWsStaleTimer = setInterval(() => {
      this.checkMarketWsStale();
    }, 1000);
  }

  stop() {
    if (this.discoveryTimer) {
      clearInterval(this.discoveryTimer);
      this.discoveryTimer = undefined;
    }
    if (this.booksTimer) {
      clearInterval(this.booksTimer);
      this.booksTimer = undefined;
    }
    if (this.tradesTimer) {
      clearInterval(this.tradesTimer);
      this.tradesTimer = undefined;
    }
    if (this.marketWsStaleTimer) {
      clearInterval(this.marketWsStaleTimer);
      this.marketWsStaleTimer = undefined;
    }
    if (this.marketWsReconnectTimer) {
      clearTimeout(this.marketWsReconnectTimer);
      this.marketWsReconnectTimer = undefined;
    }
    this.closeMarketWs();
  }

  subscribe(listener: (state: PolymarketConnectorState) => void) {
    this.listeners.add(listener);
    listener(this.state);
    return () => {
      this.listeners.delete(listener);
    };
  }

  getState() {
    return this.state;
  }

  private orderBookFreshMs() {
    return Math.max(this.config.bookPollMs * 3, 5_000);
  }

  private deriveStatus(now = Date.now()) {
    return derivePolymarketSourceHealth({
      symbol: this.config.symbol,
      reconnectCount: this.reconnectCount,
      now,
      orderBookFreshMs: this.orderBookFreshMs(),
      components: this.healthComponents
    });
  }

  private updateHealthComponent(
    name: PolymarketHealthComponent,
    state: SourceHealth["state"],
    input: { sourceEventTs?: number; serverRecvTs?: number; message?: string } = {}
  ) {
    const now = input.serverRecvTs ?? Date.now();
    const previous = this.healthComponents[name];
    this.healthComponents = {
      ...this.healthComponents,
      [name]: createPolymarketComponent({
        name,
        state,
        sourceEventTs: input.sourceEventTs ?? previous?.sourceEventTs ?? now,
        serverRecvTs: now,
        message: input.message
      })
    };
  }

  private refreshStatus(now = Date.now()) {
    this.state = {
      ...this.state,
      status: this.deriveStatus(now)
    };
  }

  private resetMarketScopedHealth(message: string, now = Date.now()) {
    this.updateHealthComponent("orderBook", "reconnecting", {
      sourceEventTs: 0,
      serverRecvTs: now,
      message: `Waiting for ${message} order books.`
    });
    this.updateHealthComponent("marketWs", "reconnecting", {
      sourceEventTs: 0,
      serverRecvTs: now,
      message: `Waiting for ${message} market WebSocket.`
    });
    this.updateHealthComponent("trades", "reconnecting", {
      sourceEventTs: 0,
      serverRecvTs: now,
      message: `Waiting for ${message} recent trades.`
    });
  }

  private componentFailureState(name: PolymarketHealthComponent) {
    const previous = this.healthComponents[name];
    return previous && previous.sourceEventTs > 0 && previous.state !== "reconnecting" ? "degraded" : "reconnecting";
  }

  async fetchMarketBySlug(slug: string) {
    const url = `${this.config.gammaBaseUrl}/markets?slug=${encodeURIComponent(slug)}`;
    const payload = await this.fetchJson<DetailedMarketPayload[]>(url);
    if (payload.length === 0) {
      throw new Error(`Gamma did not return market ${slug}.`);
    }
    return this.toMarketDetail(payload[0]);
  }

  async fetchBookByToken(tokenId: string): Promise<OrderBookSnapshot> {
    return this.fetchBook(tokenId);
  }

  async fetchBookForSide(side: TradeSide, market = this.state.currentMarket): Promise<OrderBookSnapshot> {
    const tokenId = side === "UP" ? market?.upTokenId : market?.downTokenId;
    if (!tokenId) {
      throw new Error(`Polymarket token id is unavailable for ${side}.`);
    }
    return this.fetchBook(tokenId);
  }

  async fetchClobMarketInfo(conditionId?: string): Promise<ClobMarketInfo> {
    if (!conditionId) {
      return conservativeMarketInfo();
    }
    const urls = [
      `${this.config.clobBaseUrl}/markets/${encodeURIComponent(conditionId)}`,
      `${this.config.clobBaseUrl}/market?condition_id=${encodeURIComponent(conditionId)}`
    ];
    for (const url of urls) {
      try {
        return normalizeMarketInfo(await this.fetchJson<Record<string, unknown>>(url), conditionId, "clob");
      } catch {
        // Try the next public CLOB shape before falling back.
      }
    }
    return conservativeMarketInfo(conditionId);
  }

  async focusMarket(round?: RoundRecord | Pick<RoundRecord, "marketSlug" | "marketId">) {
    if (!round?.marketSlug && !round?.marketId) {
      return;
    }

    const rawDetail =
      this.detailFromRound(round) ??
      (round.marketSlug ? await this.fetchMarketBySlug(round.marketSlug) : await this.fetchMarketById(String(round.marketId)));
    const detail = await this.withMarketInfo(rawDetail);
    const switched = this.state.currentMarket?.slug !== detail.slug;
    const now = Date.now();
    if (switched) {
      this.resetMarketScopedHealth(detail.slug, now);
    }
    this.updateHealthComponent("discovery", "healthy", {
      sourceEventTs: detail.startAt || now,
      serverRecvTs: now,
      message: `Tracking ${detail.slug}.`
    });

    this.state = {
      ...this.state,
      currentMarket: detail,
      nextMarket: this.state.nextMarket?.slug === detail.slug ? undefined : this.state.nextMarket,
      orderBooks: switched
        ? {
            UP: emptyBook("UP"),
            DOWN: emptyBook("DOWN")
          }
        : this.state.orderBooks,
      recentTrades: switched ? [] : this.state.recentTrades,
      delta: switched ? 0 : this.state.delta,
      volume: switched ? 0 : this.state.volume,
      status: this.deriveStatus(now)
    };
    this.emit();
    this.ensureMarketWs(detail);
    void Promise.all([this.refreshBooks(detail), this.refreshTrades(detail)]).catch(() => undefined);
  }

  private detailFromRound(round: RoundRecord | Pick<RoundRecord, "marketSlug" | "marketId">) {
    if (!("upTokenId" in round) || !round.upTokenId || !round.downTokenId || !round.conditionId || !round.marketSlug) {
      return undefined;
    }
    const now = Date.now();
    return {
      id: String(round.marketId),
      conditionId: round.conditionId,
      slug: round.marketSlug,
      title: round.title ?? round.marketSlug,
      startAt: round.startAt,
      endAt: round.endAt,
      eventId: round.eventId ?? round.marketId,
      eventSlug: round.eventSlug ?? round.marketSlug,
      seriesSlug: round.seriesSlug,
      upTokenId: round.upTokenId,
      downTokenId: round.downTokenId,
      upOutcome: "Up",
      downOutcome: "Down",
      outcomePrices: [0, 0],
      referencePrice: round.polymarketOpenPrice,
      referencePriceSource: round.polymarketOpenPriceSource,
      referenceOpenPrice: round.polymarketOpenPrice,
      referenceOpenPriceSource: round.polymarketOpenPriceSource,
      referenceClosePrice: round.polymarketClosePrice,
      referenceClosePriceSource: round.polymarketClosePriceSource,
      bestBid: 0,
      bestAsk: 0,
      lastTradePrice: 0,
      acceptingOrders: round.acceptingOrders ?? now < round.endAt,
      closed: now >= round.endAt,
      resolutionSource: round.resolutionSource,
      marketInfo: round.marketInfo
    } satisfies PolymarketMarketDetail;
  }

  private async withMarketInfo(detail: PolymarketMarketDetail): Promise<PolymarketMarketDetail> {
    if (detail.marketInfo && !detail.marketInfo.conservative) {
      return detail;
    }
    return {
      ...detail,
      marketInfo: await this.fetchClobMarketInfo(detail.conditionId)
    };
  }

  async fetchMarketById(id: string) {
    const directUrl = `${this.config.gammaBaseUrl}/markets/${encodeURIComponent(id)}`;
    try {
      const directPayload = await this.fetchJson<DetailedMarketPayload>(directUrl);
      return this.toMarketDetail(directPayload);
    } catch {
      const listUrl = `${this.config.gammaBaseUrl}/markets?id=${encodeURIComponent(id)}`;
      const payload = await this.fetchJson<DetailedMarketPayload[]>(listUrl);
      if (payload.length === 0) {
        throw new Error(`Gamma did not return market id ${id}.`);
      }
      return this.toMarketDetail(payload[0]);
    }
  }

  private emit() {
    for (const listener of this.listeners) {
      listener(this.state);
    }
  }

  private async discoverRounds() {
    try {
      const deterministicCandidates = await this.fetchDeterministicCandidates();
      const directCandidates = await this.fetchDirectCandidates();
      const searchCandidates = await this.fetchSearchCandidates();
      const mergedCandidates = [...deterministicCandidates, ...directCandidates, ...searchCandidates];
      const uniqueBySlug = new Map<string, PolymarketMarketDetail>();
      for (const candidate of mergedCandidates) {
        if (!candidate.slug) {
          continue;
        }
        uniqueBySlug.set(candidate.slug, candidate);
      }

      const detailedMarkets = [...uniqueBySlug.values()]
        .filter((detail) => this.matchesDetail(detail))
        .sort((left, right) => left.startAt - right.startAt);
      const prefetchedNextMarket = await this.prefetchNextMarket(detailedMarkets);
      if (prefetchedNextMarket) {
        uniqueBySlug.set(prefetchedNextMarket.slug, prefetchedNextMarket);
      }
      const eligibleMarkets = [...uniqueBySlug.values()]
        .filter((detail) => this.matchesDetail(detail))
        .sort((left, right) => left.startAt - right.startAt);
      const trackedMarkets = this.selectTrackedMarkets(eligibleMarkets);
      const currentMarket = trackedMarkets.currentMarket;
      const nextMarket = trackedMarkets.nextMarket;
      const discoveredRounds = eligibleMarkets.map((detail) => this.toRound(detail));
      const now = Date.now();
      const switched = Boolean(currentMarket?.slug && this.state.currentMarket?.slug !== currentMarket.slug);
      if (switched && currentMarket) {
        this.resetMarketScopedHealth(currentMarket.slug, now);
      }
      this.updateHealthComponent("discovery", currentMarket ? "healthy" : "degraded", {
        sourceEventTs: currentMarket?.startAt ?? now,
        serverRecvTs: now,
        message: currentMarket
          ? `Tracking ${currentMarket.slug}.`
          : "No active Polymarket BTC 5m market was discovered."
      });

      this.state = {
        ...this.state,
        currentMarket,
        nextMarket,
        discoveredRounds,
        status: this.deriveStatus(now)
      };
      this.emit();
      this.ensureMarketWs(currentMarket);
      await this.refreshBooks(currentMarket);
      await this.refreshTrades(currentMarket);
    } catch (error) {
      this.reconnectCount += 1;
      this.updateHealthComponent("discovery", this.componentFailureState("discovery"), {
        serverRecvTs: Date.now(),
        message: error instanceof Error ? error.message : "Polymarket discovery failed."
      });
      this.state = {
        ...this.state,
        status: this.deriveStatus()
      };
      this.emit();
    }
  }

  private async fetchDirectCandidates() {
    const tasks: Array<Promise<PolymarketMarketDetail>> = [];
    if (this.config.marketId) {
      tasks.push(this.fetchMarketById(this.config.marketId));
    }
    if (this.config.marketSlug) {
      tasks.push(this.fetchMarketBySlug(this.config.marketSlug));
    }
    const settled = await Promise.allSettled(tasks);
    return settled
      .filter((result): result is PromiseFulfilledResult<PolymarketMarketDetail> => result.status === "fulfilled")
      .map((result) => result.value);
  }

  private async fetchDeterministicCandidates() {
    const startAt = currentFiveMinuteStart();
    const slugs = [marketSlugForStart(startAt), marketSlugForStart(startAt + FIVE_MINUTE_MS)];
    const settled = await Promise.allSettled(slugs.map((slug) => this.fetchMarketBySlug(slug)));
    return settled
      .filter((result): result is PromiseFulfilledResult<PolymarketMarketDetail> => result.status === "fulfilled")
      .map((result) => result.value)
      .filter((detail) => this.matchesDetail(detail));
  }

  private async fetchSearchCandidates() {
    const searchTerms = [...new Set([this.buildSearchQueryWithDate(), this.config.searchQuery, this.config.symbol])];
    const slugCandidates = new Set<string>();

    for (const searchTerm of searchTerms) {
      const searchUrl = `${this.config.gammaBaseUrl}/public-search?q=${encodeURIComponent(searchTerm)}&limit_per_type=50&optimized=true`;
      const payload = await this.fetchJson<{ events?: SearchEvent[] }>(searchUrl);
      for (const event of payload.events ?? []) {
        if (this.matchesEvent(event)) {
          slugCandidates.add(event.slug);
        }
        for (const market of event.markets ?? []) {
          if (market.slug && this.matchesSlug(market.slug)) {
            slugCandidates.add(market.slug);
          }
        }
      }
    }

    const detailed = await Promise.allSettled([...slugCandidates].map((slug) => this.fetchMarketBySlug(slug)));
    return detailed
      .filter((result): result is PromiseFulfilledResult<PolymarketMarketDetail> => result.status === "fulfilled")
      .map((result) => result.value)
      .filter((detail) => this.matchesDetail(detail));
  }

  private selectTrackedMarkets(markets: PolymarketMarketDetail[]) {
    const now = Date.now();
    const activeMarket =
      markets.find((detail) => detail.startAt <= now && detail.endAt > now && !detail.closed && detail.acceptingOrders) ??
      markets.find((detail) => detail.startAt <= now && detail.endAt > now && !detail.closed);
    let currentMarket = activeMarket;
    if (!currentMarket) {
      const nearestUpcoming = markets.find(
        (detail) => detail.startAt > now && detail.startAt - now <= FIVE_MINUTE_MS && !detail.closed
      );
      currentMarket = nearestUpcoming;
    }
    const selectedMarket = currentMarket;
    let nextMarket =
      selectedMarket ? markets.find((detail) => detail.startAt === selectedMarket.endAt && !detail.closed) : undefined;
    if (currentMarket && now >= currentMarket.endAt && nextMarket && nextMarket.startAt === currentMarket.endAt) {
      currentMarket = nextMarket;
      const promotedMarket = currentMarket;
      nextMarket = markets.find((detail) => detail.startAt === promotedMarket.endAt && !detail.closed);
    }
    return {
      currentMarket,
      nextMarket
    };
  }

  private matchesEvent(event: SearchEvent) {
    return this.matchesSlug(event.slug) || normalizeText(event.title).includes("bitcoin up or down");
  }

  private matchesSlug(slug: string) {
    return BTC_FIVE_MINUTE_MARKET_SLUG.test(normalizeText(slug));
  }

  private matchesDetail(detail: PolymarketMarketDetail) {
    const slugStartAt = parseBtcFiveMinuteSlugStart(detail.slug) ?? parseBtcFiveMinuteSlugStart(detail.eventSlug);
    const haystack = normalizeText(
      `${detail.slug} ${detail.title} ${detail.seriesSlug ?? ""} ${detail.eventSlug} ${detail.upOutcome} ${detail.downOutcome}`
    );
    const withinWindow = detail.endAt > Date.now() - 10 * 60_000;
    const durationMatches = detail.endAt > detail.startAt && detail.endAt - detail.startAt === FIVE_MINUTE_MS;
    const slugTimeMatches = typeof slugStartAt === "number" && detail.startAt === slugStartAt && detail.endAt === slugStartAt + FIVE_MINUTE_MS;
    const signatureMatches = hasFiveMinuteSignature(haystack);
    const slugMatches = BTC_FIVE_MINUTE_MARKET_SLUG.test(detail.slug) || BTC_FIVE_MINUTE_MARKET_SLUG.test(detail.eventSlug);
    const seriesMatches = detail.seriesSlug === this.config.seriesSlug || slugMatches;
    const titleMatches = haystack.includes("bitcoin") && haystack.includes("up") && haystack.includes("down");
    const resolutionMatches = normalizeText(detail.resolutionSource).includes(BTC_CHAINLINK_STREAM_PATH);
    return (
      withinWindow &&
      durationMatches &&
      slugTimeMatches &&
      signatureMatches &&
      slugMatches &&
      seriesMatches &&
      titleMatches &&
      resolutionMatches
    );
  }

  private matchesText(haystack: string) {
    return haystack.includes("bitcoin") && haystack.includes("up") && haystack.includes("down");
  }

  private async refreshBooks(targetMarket = this.state.currentMarket) {
    if (!targetMarket) {
      return;
    }

    try {
      const targetSlug = targetMarket.slug;
      const [upBookPayload, downBookPayload, rawMarketDetail] = await Promise.all([
        this.fetchBook(targetMarket.upTokenId),
        this.fetchBook(targetMarket.downTokenId),
        this.fetchMarketBySlug(targetSlug)
      ]);
      const marketDetail = await this.withMarketInfo(rawMarketDetail);
      if (this.state.currentMarket?.slug !== targetSlug) {
        return;
      }
      const existingUpBook = this.state.orderBooks.UP;
      const existingDownBook = this.state.orderBooks.DOWN;
      const upBook = upBookPayload.snapshotTs >= existingUpBook.snapshotTs ? upBookPayload : existingUpBook;
      const downBook = downBookPayload.snapshotTs >= existingDownBook.snapshotTs ? downBookPayload : existingDownBook;
      const sourceEventTs = Math.max(upBook.snapshotTs, downBook.snapshotTs);
      const now = Date.now();
      this.updateHealthComponent("discovery", "healthy", {
        sourceEventTs: marketDetail.startAt || now,
        serverRecvTs: now,
        message: `Tracking ${marketDetail.slug}.`
      });
      this.updateHealthComponent("orderBook", "healthy", {
        sourceEventTs,
        serverRecvTs: now,
        message: `Reading order books for ${marketDetail.slug}.`
      });

      this.state = {
        ...this.state,
        currentMarket: marketDetail,
        orderBooks: {
          UP: upBook,
          DOWN: downBook
        },
        status: this.deriveStatus(now)
      };
      this.emit();
      this.ensureMarketWs(marketDetail);
    } catch (error) {
      if (this.state.currentMarket?.slug !== targetMarket.slug) {
        return;
      }
      this.reconnectCount += 1;
      this.updateHealthComponent("orderBook", this.componentFailureState("orderBook"), {
        serverRecvTs: Date.now(),
        message: error instanceof Error ? error.message : "Failed to refresh Polymarket order books."
      });
      this.state = {
        ...this.state,
        status: this.deriveStatus()
      };
      this.emit();
    }
  }

  private async refreshTrades(targetMarket = this.state.currentMarket) {
    if (!targetMarket) {
      return;
    }
    try {
      const targetSlug = targetMarket.slug;
      const payload = await this.fetchJson<
        Array<{
          slug: string;
          outcome: string;
          price: number;
          size: number;
          timestamp: number;
          transactionHash: string;
        }>
      >(`${this.config.dataApiBaseUrl}/trades`);
      if (this.state.currentMarket?.slug !== targetSlug) {
        return;
      }

      const recentTrades: MarketTrade[] = payload
        .filter((trade) => trade.slug === targetSlug)
        .slice(0, 20)
        .map((trade) => ({
          id: trade.transactionHash || `trade_${nanoid(10)}`,
          side: trade.outcome.toUpperCase() === "UP" ? "UP" : "DOWN",
          price: Number(trade.price),
          qty: Number(trade.size),
          ts: Number(trade.timestamp) * 1000
        }));

      const volume = recentTrades.reduce((sum, trade) => sum + trade.qty, 0);
      const delta = recentTrades.reduce((sum, trade) => sum + (trade.side === "UP" ? trade.qty : -trade.qty), 0);
      const now = Date.now();
      this.updateHealthComponent("trades", "healthy", {
        sourceEventTs: recentTrades[0]?.ts ?? now,
        serverRecvTs: now,
        message: `Recent trades refreshed for ${targetSlug}.`
      });

      this.state = {
        ...this.state,
        recentTrades,
        delta,
        volume,
        status: this.deriveStatus(now)
      };
      this.emit();
    } catch (error) {
      if (this.state.currentMarket?.slug !== targetMarket.slug) {
        return;
      }
      this.updateHealthComponent("trades", this.componentFailureState("trades"), {
        serverRecvTs: Date.now(),
        message: error instanceof Error ? error.message : "Failed to refresh Polymarket trades."
      });
      this.state = {
        ...this.state,
        status: this.deriveStatus()
      };
      this.emit();
    }
  }

  private ensureMarketWs(market?: PolymarketMarketDetail) {
    const assets = [market?.upTokenId, market?.downTokenId].filter(Boolean) as string[];
    const assetKey = assets.join(":");
    if (!market || assets.length !== 2) {
      this.closeMarketWs();
      return;
    }
    if (
      this.subscribedAssetKey === assetKey &&
      this.marketWs &&
      (this.marketWs.readyState === WebSocket.CONNECTING || this.marketWs.readyState === WebSocket.OPEN)
    ) {
      return;
    }

    this.closeMarketWs(false);
    this.subscribedAssetKey = assetKey;
    this.lastMarketWsMessageAt = Date.now();
    const socket = new WebSocket(
      "wss://ws-subscriptions-clob.polymarket.com/ws/market",
      this.proxyWsAgent ? { agent: this.proxyWsAgent as Agent } : undefined
    );
    this.marketWs = socket;
    socket.on("open", () => {
      if (this.marketWs !== socket) {
        return;
      }
      const now = Date.now();
      this.updateHealthComponent("marketWs", "reconnecting", {
        sourceEventTs: now,
        serverRecvTs: now,
        message: `Connected to Polymarket market WebSocket for ${market.slug}; waiting for messages.`
      });
      this.refreshStatus(now);
      this.emit();
      socket.send(
        JSON.stringify({
          assets_ids: assets,
          type: "market",
          custom_feature_enabled: true
        })
      );
    });
    socket.on("message", (buffer) => {
      if (this.marketWs !== socket) {
        return;
      }
      this.lastMarketWsMessageAt = Date.now();
      try {
        const decoded = JSON.parse(buffer.toString()) as unknown;
        const messages = Array.isArray(decoded) ? decoded : [decoded];
        for (const message of messages) {
          this.handleMarketWsMessage(message as Record<string, unknown>, market);
        }
      } catch (error) {
        this.updateHealthComponent("marketWs", "degraded", {
          serverRecvTs: Date.now(),
          message: error instanceof Error ? error.message : "Failed to parse Polymarket market WebSocket message."
        });
        this.state = {
          ...this.state,
          status: this.deriveStatus()
        };
        this.emit();
      }
    });
    socket.on("error", (error) => {
      if (this.marketWs !== socket) {
        return;
      }
      this.reconnectCount += 1;
      this.updateHealthComponent("marketWs", this.componentFailureState("marketWs"), {
        serverRecvTs: Date.now(),
        message: error.message || "Polymarket market WebSocket error."
      });
      this.state = {
        ...this.state,
        status: this.deriveStatus()
      };
      this.emit();
    });
    socket.on("close", () => {
      if (this.marketWs === socket) {
        this.marketWs = undefined;
        this.subscribedAssetKey = "";
        this.scheduleMarketWsReconnect(market, "Polymarket market WebSocket closed.");
      }
    });
  }

  private checkMarketWsStale() {
    const market = this.state.currentMarket;
    if (!market) {
      return;
    }
    const socket = this.marketWs;
    const ageMs = this.lastMarketWsMessageAt ? Date.now() - this.lastMarketWsMessageAt : Number.POSITIVE_INFINITY;
    if (!socket || socket.readyState === WebSocket.CLOSED || socket.readyState === WebSocket.CLOSING) {
      this.scheduleMarketWsReconnect(market, "Polymarket market WebSocket is not open.");
      return;
    }
    if (socket.readyState === WebSocket.OPEN && ageMs > MARKET_WS_STALE_MS) {
      this.reconnectCount += 1;
      const now = Date.now();
      this.updateHealthComponent("marketWs", this.componentFailureState("marketWs"), {
        serverRecvTs: now,
        message: `Polymarket market WebSocket stale for ${Math.round(ageMs)}ms; reconnecting.`
      });
      this.state = {
        ...this.state,
        status: this.deriveStatus(now)
      };
      this.emit();
      socket.terminate();
      this.marketWs = undefined;
      this.subscribedAssetKey = "";
      this.scheduleMarketWsReconnect(market, "Polymarket market WebSocket stale.");
    }
  }

  private scheduleMarketWsReconnect(market: PolymarketMarketDetail, message: string) {
    if (this.marketWsReconnectTimer || this.state.currentMarket?.slug !== market.slug) {
      return;
    }
    this.marketWsReconnectTimer = setTimeout(() => {
      this.marketWsReconnectTimer = undefined;
      if (this.state.currentMarket?.slug === market.slug) {
        this.ensureMarketWs(market);
      }
    }, MARKET_WS_RECONNECT_MS);
    const now = Date.now();
    this.updateHealthComponent("marketWs", this.componentFailureState("marketWs"), {
      serverRecvTs: now,
      message
    });
    this.state = {
      ...this.state,
      status: this.deriveStatus(now)
    };
    this.emit();
  }

  private closeMarketWs(resetAssetKey = true) {
    const socket = this.marketWs;
    if (socket) {
      socket.once("error", swallowWsCloseError);
      if (socket.readyState === WebSocket.CONNECTING) {
        socket.terminate();
      } else if (socket.readyState === WebSocket.OPEN) {
        socket.close();
      }
      if (this.marketWs === socket) {
        this.marketWs = undefined;
      }
    }
    if (resetAssetKey) {
      this.subscribedAssetKey = "";
    }
  }

  private handleMarketWsMessage(message: Record<string, unknown>, market: PolymarketMarketDetail) {
    const eventType = String(message.event_type ?? "");
    const now = Date.now();
    if (eventType === "book") {
      if (this.state.currentMarket?.slug !== market.slug) {
        return;
      }
      const assetId = String(message.asset_id ?? "");
      const side = this.sideForAsset(assetId, market);
      if (!side) {
        return;
      }
      const nextBook = this.bookFromWsMessage(message, side);
      this.updateHealthComponent("marketWs", "healthy", {
        sourceEventTs: nextBook.snapshotTs,
        serverRecvTs: now,
        message: `Streaming order book for ${market.slug}.`
      });
      this.updateHealthComponent("orderBook", "healthy", {
        sourceEventTs: nextBook.snapshotTs,
        serverRecvTs: now,
        message: `Streaming order book for ${market.slug}.`
      });
      this.state = {
        ...this.state,
        orderBooks: {
          ...this.state.orderBooks,
          [side]: nextBook
        },
        status: this.deriveStatus(now)
      };
      this.emit();
      return;
    }

    if (eventType === "best_bid_ask") {
      if (this.state.currentMarket?.slug !== market.slug) {
        return;
      }
      const assetId = String(message.asset_id ?? message.assetId ?? message.asset ?? "");
      const side = this.sideForAsset(assetId, market);
      if (!side) {
        return;
      }
      const snapshotTs = normalizeWsTimestamp(message.timestamp ?? message.ts, now);
      const existingBook = this.state.orderBooks[side];
      const nextBook = applyTopToBook(existingBook, {
        bestBid: numberFromUnknown(message.best_bid ?? message.bestBid ?? message.bid),
        bestAsk: numberFromUnknown(message.best_ask ?? message.bestAsk ?? message.ask),
        snapshotTs,
        snapshotId: String(message.hash ?? `ws_best_${side}_${snapshotTs}`)
      });
      if (!nextBook) {
        return;
      }
      this.updateHealthComponent("marketWs", "healthy", {
        sourceEventTs: snapshotTs,
        serverRecvTs: now,
        message: `Streaming best bid/ask for ${market.slug}.`
      });
      this.updateHealthComponent("orderBook", "healthy", {
        sourceEventTs: snapshotTs,
        serverRecvTs: now,
        message: `Streaming best bid/ask for ${market.slug}.`
      });
      this.state = {
        ...this.state,
        orderBooks: {
          ...this.state.orderBooks,
          [side]: nextBook
        },
        status: this.deriveStatus(now)
      };
      this.emit();
      return;
    }

    if (eventType === "price_change") {
      if (this.state.currentMarket?.slug !== market.slug) {
        return;
      }
      const changes = Array.isArray(message.price_changes)
        ? message.price_changes
        : Array.isArray(message.priceChanges)
          ? message.priceChanges
          : [];
      let nextOrderBooks = this.state.orderBooks;
      let latestTs = 0;
      let applied = false;
      for (const rawChange of changes) {
        const change = rawChange as Record<string, unknown>;
        const assetId = String(change.asset_id ?? change.assetId ?? message.asset_id ?? "");
        const side = this.sideForAsset(assetId, market);
        const bookSide = String(change.side ?? "").toUpperCase();
        const price = numberFromUnknown(change.price);
        const qty = numberFromUnknown(change.size ?? change.qty);
        if (!side || (bookSide !== "BUY" && bookSide !== "SELL") || typeof price !== "number" || typeof qty !== "number") {
          continue;
        }
        const snapshotTs = normalizeWsTimestamp(change.timestamp ?? message.timestamp ?? message.ts, now);
        const nextBook = applyPriceChangeToBook(nextOrderBooks[side], {
          side: bookSide,
          price,
          qty,
          snapshotTs,
          snapshotId: String(change.hash ?? message.hash ?? `ws_delta_${side}_${snapshotTs}`)
        });
        if (!nextBook) {
          continue;
        }
        const bestBook = applyTopToBook(nextBook, {
          bestBid: numberFromUnknown(change.best_bid ?? change.bestBid),
          bestAsk: numberFromUnknown(change.best_ask ?? change.bestAsk),
          snapshotTs,
          snapshotId: nextBook.snapshotId
        }) ?? nextBook;
        nextOrderBooks = {
          ...nextOrderBooks,
          [side]: bestBook
        };
        latestTs = Math.max(latestTs, snapshotTs);
        applied = true;
      }
      if (!applied) {
        return;
      }
      this.updateHealthComponent("marketWs", "healthy", {
        sourceEventTs: latestTs || now,
        serverRecvTs: now,
        message: `Streaming price changes for ${market.slug}.`
      });
      this.updateHealthComponent("orderBook", "healthy", {
        sourceEventTs: latestTs || now,
        serverRecvTs: now,
        message: `Streaming price changes for ${market.slug}.`
      });
      this.state = {
        ...this.state,
        orderBooks: nextOrderBooks,
        status: this.deriveStatus(now)
      };
      this.emit();
      return;
    }

    if (eventType === "last_trade_price") {
      if (this.state.currentMarket?.slug !== market.slug) {
        return;
      }
      const side = this.sideForAsset(String(message.asset_id ?? ""), market);
      const trade = {
        id: `ws_trade_${nanoid(10)}`,
        side: side ?? "UP",
        price: Number(message.price ?? 0),
        qty: Number(message.size ?? 0),
        ts: Number(message.timestamp ?? now)
      };
      this.updateHealthComponent("marketWs", "healthy", {
        sourceEventTs: trade.ts,
        serverRecvTs: now,
        message: `Streaming trade prices for ${market.slug}.`
      });
      this.updateHealthComponent("trades", "healthy", {
        sourceEventTs: trade.ts,
        serverRecvTs: now,
        message: `Streaming trade prices for ${market.slug}.`
      });
      this.state = {
        ...this.state,
        recentTrades: [trade, ...this.state.recentTrades].slice(0, 20),
        volume: Number((this.state.volume + trade.qty).toFixed(4)),
        delta: Number((this.state.delta + (trade.side === "UP" ? trade.qty : -trade.qty)).toFixed(4)),
        status: this.deriveStatus(now)
      };
      this.emit();
      return;
    }

    if (eventType === "market_resolved") {
      const winningTokenId = String(message.winning_asset_id ?? "");
      const winningOutcome = String(message.winning_outcome ?? "");
      const settledSide = this.sideForAsset(winningTokenId, market) ?? this.sideForOutcome(winningOutcome, market);
      const settlementPrice = settledSide === "UP" ? 1 : settledSide === "DOWN" ? 0 : undefined;
      const resolvedMarket = {
        ...market,
        closed: true,
        acceptingOrders: false,
        winningTokenId,
        winningOutcome,
        settlementPrice,
        settlementStatus: "resolved" as const,
        settlementReceivedAt: now
      };
      const resolvedEvent = {
        marketId: market.id,
        marketSlug: market.slug,
        conditionId: market.conditionId,
        winningTokenId,
        winningOutcome,
        settledSide,
        settlementPrice,
        receivedAt: now
      };
      this.updateHealthComponent("marketWs", "healthy", {
        sourceEventTs: now,
        serverRecvTs: now,
        message: `Polymarket resolved ${market.slug} as ${winningOutcome || settledSide || "unknown"}.`
      });
      this.state = {
        ...this.state,
        currentMarket: this.state.currentMarket?.slug === market.slug ? resolvedMarket : this.state.currentMarket,
        lastResolvedMarket: resolvedEvent,
        resolvedMarkets: [...(this.state.resolvedMarkets ?? []), resolvedEvent].slice(-20),
        status: this.deriveStatus(now)
      };
      this.emit();
    }
  }

  private bookFromWsMessage(message: Record<string, unknown>, side: TradeSide): OrderBookSnapshot {
    const top = recomputeBookTop({
      bids: this.parseWsLevels(message.bids),
      asks: this.parseWsLevels(message.asks)
    });
    return {
      snapshotId: String(message.hash ?? `ws_book_${nanoid(8)}`),
      snapshotTs: normalizeWsTimestamp(message.timestamp, Date.now()),
      bestBid: top.bestBid,
      bestAsk: top.bestAsk,
      midPrice: top.midPrice,
      bids: top.bids,
      asks: top.asks
    };
  }

  private parseWsLevels(value: unknown) {
    if (!Array.isArray(value)) {
      return [];
    }
    return value
      .map((level) => level as { price?: string; size?: string })
      .map((level) => ({
        price: Number(level.price ?? 0),
        qty: Number(level.size ?? 0)
      }))
      .filter((level) => level.price > 0 && level.qty > 0);
  }

  private sideForAsset(assetId: string, market = this.state.currentMarket): TradeSide | undefined {
    if (!market || !assetId) {
      return undefined;
    }
    if (assetId === market.upTokenId) {
      return "UP";
    }
    if (assetId === market.downTokenId) {
      return "DOWN";
    }
    return undefined;
  }

  private sideForOutcome(outcome: string, market = this.state.currentMarket): TradeSide | undefined {
    const normalized = normalizeText(outcome);
    if (!market || !normalized) {
      return undefined;
    }
    if (normalized === normalizeText(market.upOutcome) || normalized.includes("up") || normalized.includes("above")) {
      return "UP";
    }
    if (
      normalized === normalizeText(market.downOutcome) ||
      normalized.includes("down") ||
      normalized.includes("below")
    ) {
      return "DOWN";
    }
    return undefined;
  }

  private async fetchBook(tokenId: string): Promise<OrderBookSnapshot> {
    const payload = await this.fetchJson<{
      timestamp: string;
      hash: string;
      bids: Array<{ price: string; size: string }>;
      asks: Array<{ price: string; size: string }>;
    }>(`${this.config.clobBaseUrl}/book?token_id=${encodeURIComponent(tokenId)}`);

    const top = recomputeBookTop({
      bids: payload.bids.map((level) => ({
        price: Number(level.price),
        qty: Number(level.size)
      })),
      asks: payload.asks.map((level) => ({
        price: Number(level.price),
        qty: Number(level.size)
      }))
    });
    return {
      snapshotId: payload.hash || `book_${nanoid(8)}`,
      snapshotTs: normalizeWsTimestamp(payload.timestamp),
      bestBid: top.bestBid,
      bestAsk: top.bestAsk,
      midPrice: top.midPrice,
      bids: top.bids,
      asks: top.asks
    };
  }

  private buildSearchQueryWithDate() {
    const today = new Intl.DateTimeFormat("en-US", {
      month: "long",
      day: "numeric",
      timeZone: "UTC"
    }).format(new Date());
    return `${this.config.searchQuery} - ${today}`;
  }

  private async fetchJson<T>(url: string): Promise<T> {
    try {
      return await fetchJsonWithTimeout<T>(url, this.config.discoveryTimeoutMs, this.proxyDispatcher);
    } catch (error) {
      throw new Error(`Request failed for ${url}: ${error instanceof Error ? error.message : "unknown error"}`);
    }
  }

  private toRound(detail: PolymarketMarketDetail): RoundRecord {
    return {
      id: detail.slug,
      marketId: detail.id,
      symbol: this.config.symbol,
      eventId: detail.eventId,
      marketSlug: detail.slug,
      eventSlug: detail.eventSlug,
      conditionId: detail.conditionId,
      seriesSlug: detail.seriesSlug,
      upTokenId: detail.upTokenId,
      downTokenId: detail.downTokenId,
      title: detail.title,
      resolutionSource: detail.resolutionSource,
      startAt: detail.startAt,
      endAt: detail.endAt,
      priceToBeat: 0,
      status: "Trading",
      pollCount: 0,
      marketInfo: detail.marketInfo,
      acceptingOrders: detail.acceptingOrders
    };
  }

  private toMarketDetail(payload: DetailedMarketPayload): PolymarketMarketDetail {
    const normalizedOutcomes = normalizeMarketOutcomes(payload);
    const event = payload.events?.[0];
    const slugStartAt = parseBtcFiveMinuteSlugStart(payload.slug) ?? parseBtcFiveMinuteSlugStart(event?.slug);
    const parsedEventStartAt = Date.parse(payload.eventStartTime ?? "");
    const parsedStartAt = Date.parse(event?.startTime ?? event?.startDate ?? payload.endDate);
    const parsedEndAt = Date.parse(payload.endDate);
    const startAt = Number.isFinite(parsedEventStartAt) ? parsedEventStartAt : slugStartAt ?? parsedStartAt;
    const endAt = Number.isFinite(parsedEndAt) ? parsedEndAt : startAt + FIVE_MINUTE_MS;
    const raw = payload as unknown as Record<string, unknown>;
    const winningOutcome = String(
      payload.winningOutcome ?? payload.winner ?? payload.resolutionOutcome ?? raw.resolvedOutcome ?? ""
    );
    const winningTokenId = String(payload.winningTokenId ?? raw.winningAssetId ?? raw.winning_asset_id ?? "");
    const automaticallyResolved = Boolean(raw.automaticallyResolved);
    const settlementStatus = payload.closed
      ? ("resolved" as const)
      : ("pending" as const);
    return {
      id: payload.id,
      conditionId: payload.conditionId,
      slug: payload.slug,
      title: payload.question,
      startAt,
      endAt,
      eventId: event?.id ?? payload.id,
      eventSlug: event?.slug ?? payload.slug,
      seriesSlug: event?.seriesSlug,
      upTokenId: normalizedOutcomes.upTokenId,
      downTokenId: normalizedOutcomes.downTokenId,
      upOutcome: normalizedOutcomes.upOutcome,
      downOutcome: normalizedOutcomes.downOutcome,
      outcomePrices: [normalizedOutcomes.upPrice, normalizedOutcomes.downPrice],
      winningTokenId: winningTokenId || undefined,
      winningOutcome: winningOutcome || undefined,
      settlementPrice: payload.closed ? Math.max(normalizedOutcomes.upPrice, normalizedOutcomes.downPrice) : undefined,
      settlementStatus,
      automaticallyResolved: automaticallyResolved || undefined,
      bestBid: toFloat(payload.bestBid),
      bestAsk: toFloat(payload.bestAsk),
      lastTradePrice: toFloat(payload.lastTradePrice),
      acceptingOrders: Boolean(payload.acceptingOrders),
      closed: Boolean(payload.closed),
      resolutionSource: payload.resolutionSource,
      marketInfo: normalizeMarketInfo(raw, payload.conditionId, "gamma")
    };
  }

  private nextMarketSlugFor(endAt: number) {
    return marketSlugForStart(endAt);
  }

  private async prefetchNextMarket(markets: PolymarketMarketDetail[]) {
    const now = Date.now();
    const anchorMarket =
      markets.find((detail) => detail.startAt <= now && detail.endAt > now && !detail.closed) ??
      this.state.currentMarket;
    if (!anchorMarket) {
      return undefined;
    }
    const existing = markets.find((detail) => detail.startAt === anchorMarket.endAt && !detail.closed);
    if (existing) {
      return existing;
    }
    const derivedSlug = this.nextMarketSlugFor(anchorMarket.endAt);
    try {
      const detail = await this.fetchMarketBySlug(derivedSlug);
      return this.matchesDetail(detail) && detail.startAt === anchorMarket.endAt ? detail : undefined;
    } catch {
      return undefined;
    }
  }
}

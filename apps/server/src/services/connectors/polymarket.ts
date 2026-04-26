import { nanoid } from "nanoid";
import WebSocket from "ws";
import type { Agent } from "node:http";
import { createProxyDispatcher, createProxyWsAgent, fetchJsonWithTimeout } from "./network";
import type {
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
  resolutionSource?: string;
  bestBid?: number;
  bestAsk?: number;
  lastTradePrice?: number;
  acceptingOrders?: boolean;
  closed?: boolean;
  winner?: string;
  winningOutcome?: string;
  winningTokenId?: string;
  resolutionOutcome?: string;
  outcomes: string;
  outcomePrices: string;
  clobTokenIds: string;
  events?: Array<{
    id: string;
    slug: string;
    title: string;
    seriesSlug?: string;
    startTime?: string;
    startDate?: string;
  }>;
}

function emptyStatus(symbol: string): SourceHealth {
  const now = Date.now();
  return {
    source: "CLOB",
    symbol,
    state: "reconnecting",
    reconnectCount: 0,
    sourceEventTs: now,
    serverRecvTs: now,
    normalizedTs: now,
    serverPublishTs: now,
    acquireLatencyMs: 0,
    publishLatencyMs: 0,
    frontendLatencyMs: 0,
    message: "Waiting for Polymarket market discovery."
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

function isBtcPrice(value: number) {
  return Number.isFinite(value) && value > 1000 && value < 1_000_000;
}

function collectBtcPriceCandidates(value: unknown, path: string[] = []): Array<{ price: number; source: string }> {
  if (typeof value === "number" || typeof value === "string") {
    const price = Number(value);
    const source = path.join(".");
    const normalized = source.toLowerCase();
    const excluded =
      normalized.includes("outcomeprices") ||
      normalized.includes("outcome_prices") ||
      normalized.includes("bestbid") ||
      normalized.includes("bestask") ||
      normalized.includes("lasttrade") ||
      normalized.includes("timestamp") ||
      normalized.includes("time") ||
      normalized.includes("date") ||
      normalized.includes("token") ||
      normalized.includes("id");
    const priceLike =
      normalized.includes("price") ||
      normalized.includes("reference") ||
      normalized.includes("target") ||
      normalized.includes("beat") ||
      normalized.includes("resolution") ||
      normalized.includes("settle") ||
      normalized.includes("close") ||
      normalized.includes("open");
    return isBtcPrice(price) && priceLike && !excluded ? [{ price, source }] : [];
  }

  if (Array.isArray(value)) {
    return value.flatMap((item, index) => collectBtcPriceCandidates(item, [...path, String(index)]));
  }

  if (value && typeof value === "object") {
    return Object.entries(value as Record<string, unknown>).flatMap(([key, nested]) =>
      collectBtcPriceCandidates(nested, [...path, key])
    );
  }

  return [];
}

function pickBtcPriceCandidate(
  candidates: Array<{ price: number; source: string }>,
  mode: "open" | "close" | "reference"
) {
  const preferred =
    mode === "open"
      ? ["pricetobeat", "price_to_beat", "open", "start", "initial", "reference"]
      : mode === "close"
        ? ["close", "final", "settlement", "settle", "resolution", "resolved"]
        : ["pricetobeat", "price_to_beat", "reference", "target", "resolution", "open"];
  return (
    candidates.find((candidate) => preferred.some((token) => candidate.source.toLowerCase().includes(token))) ??
    (mode === "reference" ? candidates[0] : undefined)
  );
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
  private marketWs?: WebSocket;
  private subscribedAssetKey = "";
  private reconnectCount = 0;
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
      status: emptyStatus(config.symbol)
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

  async focusMarket(round?: RoundRecord | Pick<RoundRecord, "marketSlug" | "marketId">) {
    if (!round?.marketSlug && !round?.marketId) {
      return;
    }

    const detail =
      this.detailFromRound(round) ??
      (round.marketSlug ? await this.fetchMarketBySlug(round.marketSlug) : await this.fetchMarketById(String(round.marketId)));
    const switched = this.state.currentMarket?.slug !== detail.slug;
    const now = Date.now();

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
      status: {
        ...this.state.status,
        state: "healthy",
        reconnectCount: this.reconnectCount,
        sourceEventTs: detail.startAt || now,
        serverRecvTs: now,
        normalizedTs: now,
        serverPublishTs: now,
        acquireLatencyMs: 0,
        publishLatencyMs: 0,
        message: `Tracking ${detail.slug}.`
      }
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
      resolutionSource: round.resolutionSource
    } satisfies PolymarketMarketDetail;
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

      this.state = {
        ...this.state,
        currentMarket,
        nextMarket,
        discoveredRounds,
        status: {
          ...this.state.status,
          state: currentMarket ? "healthy" : "degraded",
          reconnectCount: this.reconnectCount,
          sourceEventTs: currentMarket?.startAt ?? now,
          serverRecvTs: now,
          normalizedTs: now,
          serverPublishTs: now,
          acquireLatencyMs: 0,
          publishLatencyMs: 0,
          message: currentMarket
            ? `Tracking ${currentMarket.slug}.`
            : "No active Polymarket BTC 5m market was discovered."
        }
      };
      this.emit();
      this.ensureMarketWs(currentMarket);
      await this.refreshBooks(currentMarket);
      await this.refreshTrades(currentMarket);
    } catch (error) {
      this.reconnectCount += 1;
      this.state = {
        ...this.state,
        status: {
          ...this.state.status,
          state: "degraded",
          reconnectCount: this.reconnectCount,
          message: error instanceof Error ? error.message : "Polymarket discovery failed."
        }
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
      const [upBookPayload, downBookPayload, marketDetail] = await Promise.all([
        this.fetchBook(targetMarket.upTokenId),
        this.fetchBook(targetMarket.downTokenId),
        this.fetchMarketBySlug(targetSlug)
      ]);
      if (this.state.currentMarket?.slug !== targetSlug) {
        return;
      }
      const sourceEventTs = Math.max(upBookPayload.snapshotTs, downBookPayload.snapshotTs);
      const now = Date.now();

      this.state = {
        ...this.state,
        currentMarket: marketDetail,
        orderBooks: {
          UP: upBookPayload,
          DOWN: downBookPayload
        },
        status: {
          source: "CLOB",
          symbol: this.config.symbol,
          state: "healthy",
          reconnectCount: this.reconnectCount,
          sourceEventTs,
          serverRecvTs: now,
          normalizedTs: now,
          serverPublishTs: now,
          acquireLatencyMs: Math.max(now - sourceEventTs, 0),
          publishLatencyMs: 0,
          frontendLatencyMs: 0,
          message: `Reading order books for ${marketDetail.slug}.`
        }
      };
      this.emit();
      this.ensureMarketWs(marketDetail);
    } catch (error) {
      if (this.state.currentMarket?.slug !== targetMarket.slug) {
        return;
      }
      this.reconnectCount += 1;
      this.state = {
        ...this.state,
        status: {
          ...this.state.status,
          state: this.state.orderBooks.UP.snapshotTs > 0 ? "degraded" : "reconnecting",
          reconnectCount: this.reconnectCount,
          message: error instanceof Error ? error.message : "Failed to refresh Polymarket order books."
        }
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

      this.state = {
        ...this.state,
        recentTrades,
        delta,
        volume
      };
      this.emit();
    } catch (error) {
      if (this.state.currentMarket?.slug !== targetMarket.slug) {
        return;
      }
      this.state = {
        ...this.state,
        status: {
          ...this.state.status,
          state: "degraded",
          message: error instanceof Error ? error.message : "Failed to refresh Polymarket trades."
        }
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
    const socket = new WebSocket(
      "wss://ws-subscriptions-clob.polymarket.com/ws/market",
      this.proxyWsAgent ? { agent: this.proxyWsAgent as Agent } : undefined
    );
    this.marketWs = socket;
    socket.on("open", () => {
      if (this.marketWs !== socket) {
        return;
      }
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
      try {
        const decoded = JSON.parse(buffer.toString()) as unknown;
        const messages = Array.isArray(decoded) ? decoded : [decoded];
        for (const message of messages) {
          this.handleMarketWsMessage(message as Record<string, unknown>, market);
        }
      } catch (error) {
        this.state = {
          ...this.state,
          status: {
            ...this.state.status,
            state: "degraded",
            message: error instanceof Error ? error.message : "Failed to parse Polymarket market WebSocket message."
          }
        };
        this.emit();
      }
    });
    socket.on("error", (error) => {
      if (this.marketWs !== socket) {
        return;
      }
      this.reconnectCount += 1;
      this.state = {
        ...this.state,
        status: {
          ...this.state.status,
          state: "degraded",
          reconnectCount: this.reconnectCount,
          message: error.message
        }
      };
      this.emit();
    });
    socket.on("close", () => {
      if (this.marketWs === socket) {
        this.marketWs = undefined;
        this.subscribedAssetKey = "";
      }
    });
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
      this.state = {
        ...this.state,
        orderBooks: {
          ...this.state.orderBooks,
          [side]: nextBook
        },
        status: {
          source: "CLOB",
          symbol: this.config.symbol,
          state: "healthy",
          reconnectCount: this.reconnectCount,
          sourceEventTs: nextBook.snapshotTs,
          serverRecvTs: now,
          normalizedTs: now,
          serverPublishTs: now,
          acquireLatencyMs: Math.max(now - nextBook.snapshotTs, 0),
          publishLatencyMs: 0,
          frontendLatencyMs: 0,
          message: `Streaming order book for ${market.slug}.`
        }
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
      this.state = {
        ...this.state,
        recentTrades: [trade, ...this.state.recentTrades].slice(0, 20),
        volume: Number((this.state.volume + trade.qty).toFixed(4)),
        delta: Number((this.state.delta + (trade.side === "UP" ? trade.qty : -trade.qty)).toFixed(4))
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
      this.state = {
        ...this.state,
        currentMarket: this.state.currentMarket?.slug === market.slug ? resolvedMarket : this.state.currentMarket,
        lastResolvedMarket: {
          marketId: market.id,
          marketSlug: market.slug,
          conditionId: market.conditionId,
          winningTokenId,
          winningOutcome,
          settledSide,
          settlementPrice,
          receivedAt: now
        },
        status: {
          ...this.state.status,
          state: "healthy",
          serverRecvTs: now,
          normalizedTs: now,
          serverPublishTs: now,
          message: `Polymarket resolved ${market.slug} as ${winningOutcome || settledSide || "unknown"}.`
        }
      };
      this.emit();
    }
  }

  private bookFromWsMessage(message: Record<string, unknown>, side: TradeSide): OrderBookSnapshot {
    const bids = this.parseWsLevels(message.bids).sort((left, right) => right.price - left.price);
    const asks = this.parseWsLevels(message.asks).sort((left, right) => left.price - right.price);
    const bestBid = bids[0]?.price ?? 0;
    const bestAsk = asks[0]?.price ?? 0;
    return {
      snapshotId: String(message.hash ?? `ws_book_${nanoid(8)}`),
      snapshotTs: Number(message.timestamp ?? Date.now()),
      bestBid,
      bestAsk,
      midPrice: bestBid && bestAsk ? Number(((bestBid + bestAsk) / 2).toFixed(4)) : bestBid || bestAsk,
      bids,
      asks
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

    const bids = payload.bids
      .map((level) => ({
        price: Number(level.price),
        qty: Number(level.size)
      }))
      .sort((left, right) => right.price - left.price);
    const asks = payload.asks
      .map((level) => ({
        price: Number(level.price),
        qty: Number(level.size)
      }))
      .sort((left, right) => left.price - right.price);

    const bestBid = bids[0]?.price ?? 0;
    const bestAsk = asks[0]?.price ?? 0;
    const midPrice = bestBid && bestAsk ? Number(((bestBid + bestAsk) / 2).toFixed(4)) : bestBid || bestAsk;
    return {
      snapshotId: payload.hash || `book_${nanoid(8)}`,
      snapshotTs: Number(payload.timestamp),
      bestBid,
      bestAsk,
      midPrice,
      bids,
      asks
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
      acceptingOrders: detail.acceptingOrders
    };
  }

  private toMarketDetail(payload: DetailedMarketPayload): PolymarketMarketDetail {
    const outcomes = parseJsonArray<string>(payload.outcomes);
    const outcomePrices = parseJsonArray<string>(payload.outcomePrices).map((value) => Number(value));
    const clobTokenIds = parseJsonArray<string>(payload.clobTokenIds);
    const event = payload.events?.[0];
    const slugStartAt = parseBtcFiveMinuteSlugStart(payload.slug) ?? parseBtcFiveMinuteSlugStart(event?.slug);
    const parsedStartAt = Date.parse(event?.startTime ?? event?.startDate ?? payload.endDate);
    const parsedEndAt = Date.parse(payload.endDate);
    const startAt = slugStartAt ?? parsedStartAt;
    const endAt = slugStartAt ? slugStartAt + FIVE_MINUTE_MS : parsedEndAt;
    const raw = payload as unknown as Record<string, unknown>;
    const btcPriceCandidates = collectBtcPriceCandidates(raw);
    const referencePrice = pickBtcPriceCandidate(btcPriceCandidates, "reference");
    const referenceOpenPrice = pickBtcPriceCandidate(btcPriceCandidates, "open");
    const referenceClosePrice = pickBtcPriceCandidate(btcPriceCandidates, "close");
    const winningOutcome = String(
      payload.winningOutcome ?? payload.winner ?? payload.resolutionOutcome ?? raw.resolvedOutcome ?? ""
    );
    const winningTokenId = String(payload.winningTokenId ?? raw.winningAssetId ?? raw.winning_asset_id ?? "");
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
      upTokenId: clobTokenIds[0] ?? "",
      downTokenId: clobTokenIds[1] ?? "",
      upOutcome: outcomes[0] ?? "Up",
      downOutcome: outcomes[1] ?? "Down",
      outcomePrices: [toFloat(outcomePrices[0]), toFloat(outcomePrices[1])],
      referencePrice: referencePrice?.price,
      referencePriceSource: referencePrice?.source,
      referenceOpenPrice: referenceOpenPrice?.price,
      referenceOpenPriceSource: referenceOpenPrice?.source,
      referenceClosePrice: referenceClosePrice?.price,
      referenceClosePriceSource: referenceClosePrice?.source,
      winningTokenId: winningTokenId || undefined,
      winningOutcome: winningOutcome || undefined,
      settlementPrice: payload.closed ? Math.max(toFloat(outcomePrices[0]), toFloat(outcomePrices[1])) : undefined,
      settlementStatus,
      bestBid: toFloat(payload.bestBid),
      bestAsk: toFloat(payload.bestAsk),
      lastTradePrice: toFloat(payload.lastTradePrice),
      acceptingOrders: Boolean(payload.acceptingOrders),
      closed: Boolean(payload.closed),
      resolutionSource: payload.resolutionSource
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

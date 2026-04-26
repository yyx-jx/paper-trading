import type {
  AuditEvent,
  BehaviorActionLog,
  BinanceConnectorState,
  ChainlinkConnectorState,
  Language,
  MatchingBookState,
  MatchingFill,
  MarketSnapshot,
  OrderRecord,
  OrderBookSnapshot,
  OrderAction,
  PaperOrderKind,
  PolymarketConnectorState,
  PolymarketMarketDetail,
  PositionRecord,
  RoundRecord,
  RoundStatus,
  SourceHealth,
  TradeSide,
  UserRecord
} from "../domain/types";
import { BinanceConnector } from "./connectors/binance";
import { ChainlinkConnector } from "./connectors/chainlink";
import { estimateClobExecution, type ClobExecutionEstimate } from "./clob-execution";
import { MatchingServiceClient } from "./matching/client";
import { PolymarketConnector } from "./connectors/polymarket";
import { PolymarketReferenceResolver } from "./connectors/polymarket-reference";
import { AppStore } from "./store";

const LATENCY_LOG_INTERVAL_MS = 15000;
const REDEEM_DELAY_MS = 2000;
const MANUAL_SETTLEMENT_RETRY_MS = 60_000;
const QTY_EPSILON = 0.0001;
const FIVE_MINUTE_MS = 5 * 60_000;

function isBtcReferencePrice(value?: number): value is number {
  return typeof value === "number" && Number.isFinite(value) && value > 1000;
}

const roundNumber = (value: number, digits = 2) => Number(value.toFixed(digits));

function pad2(value: number) {
  return String(value).padStart(2, "0");
}

function utcRangeText(startAt: number, endAt: number) {
  const start = new Date(startAt);
  const end = new Date(endAt);
  return `${pad2(start.getUTCHours())}:${pad2(start.getUTCMinutes())}-${pad2(end.getUTCHours())}:${pad2(end.getUTCMinutes())} UTC`;
}

function createDisabledChainlinkState(symbol: string): ChainlinkConnectorState {
  const now = Date.now();
  return {
    price: 0,
    updatedAt: 0,
    status: {
      source: "Chainlink",
      symbol,
      state: "disabled",
      reconnectCount: 0,
      sourceEventTs: now,
      serverRecvTs: now,
      normalizedTs: now,
      serverPublishTs: now,
      acquireLatencyMs: 0,
      publishLatencyMs: 0,
      frontendLatencyMs: 0,
      message: "Chainlink is disabled in local testing mode."
    }
  };
}

export class SimulationEngine {
  private readonly binanceConnector: BinanceConnector;
  private readonly chainlinkConnector: ChainlinkConnector;
  private readonly polymarketConnector: PolymarketConnector;
  private readonly polymarketReferenceResolver: PolymarketReferenceResolver;
  private binanceState: BinanceConnectorState;
  private chainlinkState: ChainlinkConnectorState;
  private polymarketState: PolymarketConnectorState;
  private readonly unsubscribers: Array<() => void> = [];
  private reconcileTimer?: NodeJS.Timeout;
  private reconcileRunning = false;
  private reconcileQueued = false;
  private readonly pollLocks = new Set<string>();
  private readonly lastLatencyLogAt = new Map<string, number>();
  private readonly lastLatencyState = new Map<string, string>();
  private readonly matchingBooks = new Map<string, MatchingBookState>();
  private readonly currentBookKeys = new Map<TradeSide, string>();
  private readonly lastSyncedSnapshotIds = new Map<string, string>();
  private marketSyncSlug?: string;
  private pendingOrdersRunning = false;

  constructor(
    private readonly store: AppStore,
    private readonly matchingClient: MatchingServiceClient,
    private readonly config: {
      symbol: string;
      marketId: string;
      freezeWindowMs: number;
      pollDelayMs: number;
      gammaPollIntervalMs: number;
      gammaMaxPolls: number;
      binanceRestUrl: string;
      binanceWsUrl: string;
      binanceRequestTimeoutMs: number;
      binanceRestPollMs: number;
      binanceWsStaleMs: number;
      upstreamProxyUrl?: string;
      chainlinkEnabled: boolean;
      chainlinkRpcUrl: string;
      chainlinkFallbackRpcUrls: string[];
      chainlinkRequestTimeoutMs: number;
      chainlinkBtcUsdProxyAddress: `0x${string}`;
      chainlinkPollMs: number;
      gammaBaseUrl: string;
      clobBaseUrl: string;
      dataApiBaseUrl: string;
      polymarketMarketId?: string;
      polymarketMarketSlug?: string;
      polymarketSearchQuery: string;
      polymarketSeriesSlug: string;
      polymarketDiscoveryTimeoutMs: number;
      polymarketDiscoveryKeywords: string[];
      marketDiscoveryIntervalMs: number;
      polymarketBookPollMs: number;
      polymarketTradesPollMs: number;
    }
  ) {
    this.binanceConnector = new BinanceConnector({
      symbol: config.symbol,
      wsUrl: config.binanceWsUrl,
      restUrl: config.binanceRestUrl,
      requestTimeoutMs: config.binanceRequestTimeoutMs,
      restPollMs: config.binanceRestPollMs,
      wsStaleMs: config.binanceWsStaleMs,
      upstreamProxyUrl: config.upstreamProxyUrl
    });
    this.chainlinkConnector = new ChainlinkConnector({
      symbol: config.symbol,
      rpcUrl: config.chainlinkRpcUrl,
      fallbackRpcUrls: config.chainlinkFallbackRpcUrls,
      proxyAddress: config.chainlinkBtcUsdProxyAddress,
      pollMs: config.chainlinkPollMs,
      requestTimeoutMs: config.chainlinkRequestTimeoutMs
    });
    this.polymarketConnector = new PolymarketConnector({
      symbol: config.symbol,
      gammaBaseUrl: config.gammaBaseUrl,
      clobBaseUrl: config.clobBaseUrl,
      dataApiBaseUrl: config.dataApiBaseUrl,
      marketId: config.polymarketMarketId,
      marketSlug: config.polymarketMarketSlug,
      searchQuery: config.polymarketSearchQuery,
      seriesSlug: config.polymarketSeriesSlug,
      discoveryKeywords: config.polymarketDiscoveryKeywords,
      discoveryTimeoutMs: config.polymarketDiscoveryTimeoutMs,
      discoveryIntervalMs: config.marketDiscoveryIntervalMs,
      bookPollMs: config.polymarketBookPollMs,
      tradesPollMs: config.polymarketTradesPollMs,
      upstreamProxyUrl: config.upstreamProxyUrl
    });
    this.polymarketReferenceResolver = new PolymarketReferenceResolver({
      requestTimeoutMs: config.chainlinkRequestTimeoutMs,
      upstreamProxyUrl: config.upstreamProxyUrl
    });
    this.binanceState = this.binanceConnector.getState();
    this.chainlinkState = config.chainlinkEnabled
      ? this.chainlinkConnector.getState()
      : createDisabledChainlinkState(config.symbol);
    this.polymarketState = this.polymarketConnector.getState();
  }

  async start() {
    this.unsubscribers.push(
      this.binanceConnector.subscribe((state) => {
        this.binanceState = state;
        this.scheduleReconcile();
      }),
      this.polymarketConnector.subscribe((state) => {
        this.polymarketState = state;
        this.scheduleReconcile();
      })
    );
    if (this.config.chainlinkEnabled) {
      this.unsubscribers.push(
        this.chainlinkConnector.subscribe((state) => {
          this.chainlinkState = state;
          this.scheduleReconcile();
        })
      );
    }

    this.binanceConnector.start();
    if (this.config.chainlinkEnabled) {
      this.chainlinkConnector.start();
    }
    this.polymarketConnector.start();
    this.reconcileTimer = setInterval(() => this.scheduleReconcile(), 1000);
    this.scheduleReconcile();
  }

  async stop() {
    if (this.reconcileTimer) {
      clearInterval(this.reconcileTimer);
      this.reconcileTimer = undefined;
    }
    while (this.unsubscribers.length > 0) {
      this.unsubscribers.pop()?.();
    }
    this.binanceConnector.stop();
    if (this.config.chainlinkEnabled) {
      this.chainlinkConnector.stop();
    }
    this.polymarketConnector.stop();
  }

  async placeOrder(
    user: UserRecord,
    payload: {
      action?: OrderAction;
      side: TradeSide;
      amount?: number;
      qty?: number;
      orderKind?: PaperOrderKind;
      limitPrice?: number;
      clientSendTs?: number;
      positionIds?: string[];
    }
  ): Promise<{ order: OrderRecord }> {
    const snapshot = this.captureActionSnapshot();
    const traceId = this.store.newTraceId();
    const now = Date.now();
    const currentRound = this.getActiveRound(now);
    const action = payload.action ?? "buy";
    const orderKind = payload.orderKind ?? "market";
    try {
      this.assertCanCreateNewOrder(currentRound, now);
      if (orderKind === "limit" && (!payload.limitPrice || payload.limitPrice <= 0)) {
        throw new Error("Limit orders require a positive limit price.");
      }
      if (action === "buy" && (!payload.amount || payload.amount <= 0)) {
        throw new Error("Buy orders require a positive USDC amount.");
      }
      if (action === "sell" && (!payload.qty || payload.qty <= 0)) {
        throw new Error("Sell orders require a positive quantity.");
      }
      if (action === "buy" && user.availableUsdc < (payload.amount ?? 0)) {
        throw new Error("Insufficient virtual balance.");
      }
      if (action === "sell") {
        const availableQty = this.availableSellQty(user.id, currentRound.id, payload.side, payload.positionIds);
        if (availableQty + QTY_EPSILON < (payload.qty ?? 0)) {
          throw new Error("Insufficient unlocked position quantity.");
        }
      }

      const serverRecvTs = Date.now();
      const orderId = this.store.newId("ord");
      const engineStartTs = Date.now();
      const book = await this.fetchExecutionBook(payload.side, currentRound);
      const tokenId = this.resolveTokenId(payload.side, currentRound);
      const { bookKey, marketId } = this.resolveBookContext(payload.side, currentRound);
      const expectedQty =
        action === "buy"
          ? roundNumber((payload.amount ?? 0) / Math.max(payload.limitPrice ?? book.bestAsk, 0.0001), 4)
          : roundNumber(payload.qty ?? 0, 4);
      const estimate = estimateClobExecution({
        action,
        book,
        orderId,
        notional: action === "buy" ? payload.amount : undefined,
        qty: action === "sell" ? payload.qty : undefined,
        limitPrice: payload.limitPrice,
        executedAt: engineStartTs
      });
      const engineFinishTs = Date.now();
      const sourceLatencyMs = Math.max(engineFinishTs - book.snapshotTs, 0);
      const shouldRest = orderKind === "limit" && !estimate.fullyMatched;
      const status = shouldRest ? "pending" : estimate.fullyMatched ? "filled" : "failed";
      const order: OrderRecord = {
        id: orderId,
        traceId,
        userId: user.id,
        roundId: currentRound.id,
        symbol: this.config.symbol,
        marketId,
        action,
        side: payload.side,
        status,
        orderKind,
        timeInForce: orderKind === "limit" ? "GTC" : "FOK",
        limitPrice: payload.limitPrice,
        lifecycleStatus: status,
        resultType: status === "pending" ? "pending" : status === "filled" ? "all_filled" : "all_failed",
        tokenId,
        bookKey,
        bookHash: book.snapshotId,
        requestedAmountUsdc: action === "buy" ? roundNumber(payload.amount ?? 0, 2) : undefined,
        requestedQty: action === "sell" ? roundNumber(payload.qty ?? 0, 4) : expectedQty,
        frozenUsdc: 0,
        frozenQty: 0,
        fills: estimate.fills,
        sourceLatencyMs,
        marketSlug: currentRound.marketSlug,
        notionalUsdc: roundNumber(
          action === "buy"
            ? (status === "filled" ? estimate.matchedNotional : payload.amount ?? 0)
            : status === "filled"
              ? estimate.matchedNotional
              : (payload.qty ?? 0) * (payload.limitPrice ?? book.bestBid),
          2
        ),
        expectedQty,
        filledQty: status === "filled" ? roundNumber(estimate.filledQty, 4) : 0,
        unfilledQty: status === "filled" ? 0 : expectedQty,
        avgFillPrice: status === "filled" && estimate.avgPrice ? roundNumber(estimate.avgPrice, 4) : undefined,
        bestBid: book.bestBid,
        bestAsk: book.bestAsk,
        midPrice: book.midPrice,
        bookSnapshotTs: book.snapshotTs,
        partialFilled: false,
        slippageBps:
          status === "filled" && estimate.avgPrice && book.midPrice > 0
            ? roundNumber(((estimate.avgPrice - book.midPrice) / book.midPrice) * 10000, 2)
            : undefined,
        matchLatencyMs: Math.max(engineFinishTs - engineStartTs, 1),
        failureReason: status === "failed" ? estimate.failureReason : undefined,
        clientSendTs: payload.clientSendTs,
        serverRecvTs,
        serverPublishTs: Date.now(),
        createdAt: Date.now()
      };

      if (status === "pending") {
        if (action === "buy") {
          const frozen = roundNumber(payload.amount ?? 0, 2);
          user.availableUsdc = roundNumber(user.availableUsdc - frozen, 2);
          order.frozenUsdc = frozen;
          await this.store.persistUser(user);
        } else {
          const frozenQty = roundNumber(payload.qty ?? 0, 4);
          await this.lockSellQty(user.id, currentRound.id, payload.side, frozenQty, payload.positionIds);
          order.frozenQty = frozenQty;
        }
        await this.store.persistOrder(order);
      } else if (status === "filled" && estimate.avgPrice) {
        await this.applyFilledOrder(user, currentRound, order, estimate, payload.positionIds);
      } else {
        await this.store.persistOrder(order);
      }

      if (status === "filled" || status === "pending") {
        await this.store.persistUser(user);
      }

      await this.writeAuditLog({
        eventId: this.store.newId("evt"),
        traceId,
        category: "matching",
        actionType: "place_order",
        actionStatus: status === "failed" ? "failed" : "success",
        userId: user.id,
        role: user.role,
        pageName: "trade.main",
        moduleName: "order.panel",
        symbol: this.config.symbol,
        roundId: currentRound.id,
        clientSendTs: payload.clientSendTs,
        serverRecvTs,
        engineStartTs,
        engineFinishTs,
        serverPublishTs: order.serverPublishTs,
        backendLatencyMs: order.matchLatencyMs,
        resultCode: status === "failed" ? "ORDER_FAILED" : status === "pending" ? "ORDER_PENDING" : "ORDER_FILLED",
        resultMessage:
          status === "failed"
            ? estimate.failureReason ?? "Polymarket CLOB depth was insufficient."
            : status === "pending"
              ? "Limit order is pending against future Polymarket CLOB depth."
              : "Order fully matched against the current Polymarket CLOB snapshot.",
        details: {
          traceId,
          roundId: currentRound.id,
          marketId,
          marketSlug: currentRound.marketSlug,
          orderId: order.id,
          positionId: payload.positionIds?.[0],
          bookKey,
          bookHash: book.snapshotId,
          bookSnapshotId: book.snapshotId,
          matchingSequence: undefined,
          tokenId,
          action,
          side: payload.side,
          orderKind,
          timeInForce: order.timeInForce,
          limitPrice: payload.limitPrice,
          notionalUsdc: payload.amount,
          requestedQty: payload.qty,
          filledQty: order.filledQty,
          unfilledQty: order.unfilledQty,
          avgFillPrice: order.avgFillPrice,
          sourceLatencyMs,
          slippageBps: order.slippageBps,
          failureReason: order.failureReason
        }
      });

      await this.writeBehaviorLog(
        this.createBehaviorLog({
          user,
          actionType: "place_order",
          actionStatus: status === "failed" ? "failed" : "success",
          traceId,
          orderId: order.id,
          round: currentRound,
          snapshot,
          direction: payload.side,
          entryOdds: snapshot[payload.side === "UP" ? "upPrice" : "downPrice"],
          positionNotional: order.notionalUsdc,
          bookSnapshot: book,
          order,
          actualFillPrice: order.avgFillPrice,
          slippageBps: order.slippageBps,
          partialFilled: order.partialFilled,
          unfilledQty: order.unfilledQty,
          executionLatencyMs: order.matchLatencyMs,
          failureReason: order.failureReason,
          contextJson: {
            roundStatus: currentRound.status,
            acceptingOrders: currentRound.acceptingOrders,
            requestAction: action,
            requestSide: payload.side,
            requestAmount: payload.amount,
            requestQty: payload.qty,
            orderType: orderKind,
            isAccepted: status !== "failed",
            bookSnapshotId: book.snapshotId,
            bookKey
          }
        })
      );

      this.store.emitUserPayload(user.id);
      return { order };
    } catch (error) {
      const message = error instanceof Error ? error.message : "Order failed.";
      const serverNow = Date.now();
      await this.writeAuditLog({
        eventId: this.store.newId("evt"),
        traceId,
        category: "matching",
        actionType: "place_order",
        actionStatus: "failed",
        userId: user.id,
        role: user.role,
        pageName: "trade.main",
        moduleName: "order.panel",
        symbol: this.config.symbol,
        roundId: currentRound?.id,
        clientSendTs: payload.clientSendTs,
        serverRecvTs: serverNow,
        serverPublishTs: serverNow,
        backendLatencyMs: 0,
        resultCode: "ORDER_FAILED",
        resultMessage: message,
        details: {
          traceId,
          roundId: currentRound?.id,
          marketId: snapshot.marketId,
          marketSlug: currentRound?.marketSlug ?? snapshot.marketSlug,
          action,
          side: payload.side,
          orderKind: payload.orderKind ?? "market",
          limitPrice: payload.limitPrice,
          notionalUsdc: payload.amount,
          requestedQty: payload.qty,
          failureReason: message
        }
      });
      await this.writeBehaviorLog(
        this.createBehaviorLog({
          user,
          actionType: "place_order",
          actionStatus: "failed",
          traceId,
          round: currentRound,
          snapshot,
          direction: payload.side,
          entryOdds: snapshot[payload.side === "UP" ? "upPrice" : "downPrice"],
          positionNotional: payload.amount,
          failureReason: message,
          contextJson: {
            requestAction: action,
            requestSide: payload.side,
            requestAmount: payload.amount,
            requestQty: payload.qty,
            orderType: payload.orderKind ?? "market",
            limitPrice: payload.limitPrice,
            failureReason: message
          }
        })
      );
      throw error;
    }
  }

  async cancelOrder(user: UserRecord, orderId: string) {
    const snapshot = this.captureActionSnapshot();
    const traceId = this.store.newTraceId();
    const order = this.store.orders.find((item) => item.id === orderId && item.userId === user.id);
    try {
      if (!order) {
        throw new Error("Order not found.");
      }
      if (order.status !== "pending") {
        throw new Error("Only pending limit orders can be cancelled.");
      }

      const releasedFrozenUsdc = order.frozenUsdc ?? 0;
      const releasedFrozenQty = order.frozenQty ?? 0;
      order.status = "cancelled";
      order.lifecycleStatus = "cancelled";
      order.resultType = "cancelled";
      if (order.frozenUsdc && order.frozenUsdc > 0) {
        user.availableUsdc = roundNumber(user.availableUsdc + order.frozenUsdc, 2);
        order.frozenUsdc = 0;
        await this.store.persistUser(user);
      }
      if (order.frozenQty && order.frozenQty > 0) {
        await this.unlockSellQty(user.id, order.roundId, order.side, order.frozenQty);
        order.frozenQty = 0;
      }
      order.serverPublishTs = Date.now();
      await this.store.persistOrder(order);
      await this.writeAuditLog({
        eventId: this.store.newId("evt"),
        traceId: order.traceId,
        category: "operation",
        actionType: "cancel_order",
        actionStatus: "success",
        userId: user.id,
        role: user.role,
        pageName: "trade.main",
        moduleName: "order.panel",
        symbol: order.symbol,
        roundId: order.roundId,
        serverRecvTs: Date.now(),
        serverPublishTs: Date.now(),
        backendLatencyMs: 1,
        resultCode: "ORDER_CANCELLED",
        resultMessage: "Pending paper limit order was cancelled and frozen assets were released.",
        details: {
          traceId: order.traceId,
          roundId: order.roundId,
          marketId: order.marketId,
          marketSlug: order.marketSlug,
          orderId,
          orderKind: order.orderKind,
          releasedFrozenUsdc,
          releasedFrozenQty,
          bookKey: order.bookKey,
          bookSnapshotId: order.bookHash
        }
      });
      await this.writeBehaviorLog(
        this.createBehaviorLog({
          user,
          actionType: "cancel_order",
          actionStatus: "success",
          traceId: order.traceId,
          orderId: order.id,
          round: this.store.getRoundById(order.roundId),
          snapshot,
          direction: order.side,
          entryOdds: snapshot[order.side === "UP" ? "upPrice" : "downPrice"],
          positionNotional: order.notionalUsdc,
          bookSnapshot: snapshot.orderBooks[order.side],
          order,
          frozenAssetRelease: {
            releasedFrozenUsdc,
            releasedFrozenQty
          },
          partialFilled: order.partialFilled,
          unfilledQty: order.unfilledQty,
          contextJson: {
            cancelledRemainingQty: order.unfilledQty,
            bookKey: order.bookKey,
            bookSnapshotId: order.bookHash,
            marketSlug: order.marketSlug
          }
        })
      );
      this.store.emitUserPayload(user.id);
      return order;
    } catch (error) {
      const message = error instanceof Error ? error.message : "Cancel order failed.";
      const serverNow = Date.now();
      await this.writeAuditLog({
        eventId: this.store.newId("evt"),
        traceId: order?.traceId ?? traceId,
        category: "operation",
        actionType: "cancel_order",
        actionStatus: "failed",
        userId: user.id,
        role: user.role,
        pageName: "trade.main",
        moduleName: "order.panel",
        symbol: order?.symbol ?? this.config.symbol,
        roundId: order?.roundId,
        serverRecvTs: serverNow,
        serverPublishTs: serverNow,
        backendLatencyMs: 0,
        resultCode: "ORDER_CANCEL_FAILED",
        resultMessage: message,
        details: {
          traceId: order?.traceId ?? traceId,
          roundId: order?.roundId,
          marketId: order?.marketId,
          marketSlug: order?.marketSlug,
          orderId,
          failureReason: message
        }
      });
      await this.writeBehaviorLog(
        this.createBehaviorLog({
          user,
          actionType: "cancel_order",
          actionStatus: "failed",
          traceId: order?.traceId ?? traceId,
          orderId,
          round: order ? this.store.getRoundById(order.roundId) : this.getActiveRound(serverNow),
          snapshot,
          direction: order?.side,
          positionNotional: order?.notionalUsdc,
          order,
          failureReason: message,
          contextJson: {
            failureReason: message
          }
        })
      );
      throw error;
    }
  }

  async sellPosition(user: UserRecord, positionId: string) {
    const snapshot = this.captureActionSnapshot();
    const traceId = this.store.newTraceId();
    const position = this.store.positions.find((item) => item.id === positionId && item.userId === user.id);
    try {
      if (!position) {
        throw new Error("Position not found.");
      }
      if (position.status !== "open") {
        throw new Error("Position is already closed.");
      }

      const currentRound = this.store.getRoundById(position.roundId);
      this.assertCanSellPosition(position, currentRound, Date.now());

      const availableQty = roundNumber(Math.max(position.qty - (position.lockedQty ?? 0), 0), 4);
      if (availableQty <= QTY_EPSILON) {
        throw new Error("Position has no unlocked quantity available to sell.");
      }
      const { order } = await this.placeOrder(user, {
        action: "sell",
        side: position.side,
        qty: availableQty,
        orderKind: "market",
        clientSendTs: undefined,
        positionIds: [positionId]
      });
      await this.writeAuditLog({
        eventId: this.store.newId("evt"),
        traceId: order.traceId,
        category: "matching",
        actionType: "sell_position",
        actionStatus: "success",
        userId: user.id,
        role: user.role,
        pageName: "profile.main",
        moduleName: "position.table",
        symbol: order.symbol,
        roundId: order.roundId,
        serverRecvTs: order.serverRecvTs,
        serverPublishTs: Date.now(),
        backendLatencyMs: order.matchLatencyMs,
        resultCode: "SELL_POSITION_FILLED",
        resultMessage: "Position was sold against the current Polymarket CLOB snapshot.",
        details: {
          traceId: order.traceId,
          roundId: order.roundId,
          marketId: order.marketId,
          marketSlug: order.marketSlug,
          orderId: order.id,
          positionId,
          bookKey: order.bookKey,
          bookSnapshotId: order.bookHash,
          avgFillPrice: order.avgFillPrice,
          filledQty: order.filledQty,
          slippageBps: order.slippageBps
        }
      });
      await this.writeBehaviorLog(
        this.createBehaviorLog({
          user,
          actionType: "sell_position",
          actionStatus: "success",
          traceId: order.traceId,
          orderId: order.id,
          round: currentRound,
          snapshot,
          direction: position.side,
          positionNotional: order.notionalUsdc,
          exitType: "manual_sell",
          exitOdds: order.avgFillPrice,
          settlementResult: position.settlementResult,
          bookSnapshot: snapshot.orderBooks[position.side],
          order,
          actualFillPrice: order.avgFillPrice,
          slippageBps: order.slippageBps,
          partialFilled: order.partialFilled,
          unfilledQty: order.unfilledQty,
          executionLatencyMs: order.matchLatencyMs,
          contextJson: {
            positionId
          }
        })
      );
      return order;
    } catch (error) {
      const message = error instanceof Error ? error.message : "Sell position failed.";
      const currentRound = position ? this.store.getRoundById(position.roundId) : this.getActiveRound(Date.now());
      const serverNow = Date.now();
      await this.writeAuditLog({
        eventId: this.store.newId("evt"),
        traceId,
        category: "matching",
        actionType: "sell_position",
        actionStatus: "failed",
        userId: user.id,
        role: user.role,
        pageName: "profile.main",
        moduleName: "position.table",
        symbol: this.config.symbol,
        roundId: currentRound?.id,
        serverRecvTs: serverNow,
        serverPublishTs: serverNow,
        backendLatencyMs: 0,
        resultCode: "SELL_FAILED",
        resultMessage: message,
        details: {
          traceId,
          roundId: currentRound?.id,
          marketId: currentRound?.marketId ?? snapshot.marketId,
          marketSlug: currentRound?.marketSlug ?? snapshot.marketSlug,
          positionId,
          failureReason: message
        }
      });
      await this.writeBehaviorLog(
        this.createBehaviorLog({
          user,
          actionType: "sell_position",
          actionStatus: "failed",
          traceId,
          round: currentRound,
          snapshot,
          direction: position?.side,
          exitType: "manual_sell",
          settlementResult: position?.settlementResult,
          positionNotional: position?.notionalSpent,
          failureReason: message,
          contextJson: {
            positionId,
            failureReason: message
          }
        })
      );
      throw error;
    }
  }

  async updateLanguage(user: UserRecord, language: Language) {
    await this.store.setUserLanguage(user.id, language);
    await this.writeAuditLog({
      eventId: this.store.newId("evt"),
      traceId: this.store.newTraceId(),
      category: "operation",
      actionType: "switch_language",
      actionStatus: "success",
      userId: user.id,
      role: user.role,
      pageName: "shell.topbar",
      moduleName: "language.switch",
      serverRecvTs: Date.now(),
      serverPublishTs: Date.now(),
      backendLatencyMs: 1,
      resultCode: "LANGUAGE_UPDATED",
      resultMessage: "Language preference updated.",
      details: {
        language
      }
    });
  }

  async getMatchingHealth() {
    return this.matchingClient.health();
  }

  async getCurrentMatchingBookState(input: {
    side?: TradeSide;
    bookKey?: string;
    roundId?: string;
    marketId?: string;
  }) {
    const bookKey = this.resolveReplayBookKey(input);
    const response = await this.matchingClient.getCurrentBook(bookKey);
    if (response.book) {
      this.cacheMatchingBook(response.book);
    }
    return {
      bookKey,
      book: response.book
    };
  }

  async getMatchingReplay(input: {
    side?: TradeSide;
    bookKey?: string;
    roundId?: string;
    marketId?: string;
    fromSequence?: number;
    toSequence?: number;
    limit?: number;
  }) {
    const bookKey = this.resolveReplayBookKey(input);
    return this.matchingClient.replay(bookKey, {
      fromSequence: input.fromSequence,
      toSequence: input.toSequence,
      limit: input.limit
    });
  }

  async closeSide(user: UserRecord, payload: { side: TradeSide; clientSendTs?: number }) {
    const traceId = this.store.newTraceId();
    const snapshot = this.captureActionSnapshot();
    const now = Date.now();
    const currentRound = this.getActiveRound(now);
    try {
      this.assertCanCreateNewOrder(currentRound, now);
      const positions = this.store.positions
        .filter(
          (position) =>
            position.userId === user.id &&
            position.roundId === currentRound.id &&
            position.side === payload.side &&
            position.status === "open"
        )
        .sort((left, right) => left.openedAt - right.openedAt);
      if (positions.length === 0) {
        throw new Error(`No open ${payload.side} positions were found in the current round.`);
      }
      const requestedQty = roundNumber(positions.reduce((sum, position) => sum + position.qty, 0), 4);

      const failures: Array<{ positionId: string; message: string }> = [];
      const orders: OrderRecord[] = [];
      for (const position of positions) {
        try {
          orders.push(await this.sellPosition(user, position.id));
        } catch (error) {
          failures.push({
            positionId: position.id,
            message: error instanceof Error ? error.message : "Position close failed."
          });
        }
      }

      const totalQty = roundNumber(orders.reduce((sum, order) => sum + order.filledQty, 0), 4);
      const totalProceeds = roundNumber(orders.reduce((sum, order) => sum + order.notionalUsdc, 0), 2);
      if (orders.length === 0) {
        throw new Error(failures[0]?.message ?? "No positions were closed.");
      }

      const avgFillPrice = totalQty > 0 ? roundNumber(totalProceeds / totalQty, 4) : undefined;
      const closedPositionsCount = orders.length;
      const serverNow = Date.now();

      await this.writeAuditLog({
        eventId: this.store.newId("evt"),
        traceId,
        category: "operation",
        actionType: "close_side",
        actionStatus: failures.length > 0 ? "timeout" : "success",
        userId: user.id,
        role: user.role,
        pageName: "trade.main",
        moduleName: "quick.actions",
        symbol: this.config.symbol,
        roundId: currentRound.id,
        clientSendTs: payload.clientSendTs,
        serverRecvTs: serverNow,
        serverPublishTs: serverNow,
        backendLatencyMs: 1,
        resultCode: failures.length > 0 ? "CLOSE_SIDE_PARTIAL" : "CLOSE_SIDE_COMPLETED",
        resultMessage:
          failures.length > 0
            ? "Close side completed with partial failures."
            : "All positions on the selected side were closed.",
        details: {
          side: payload.side,
          closedPositionsCount,
          totalQty,
          totalProceeds,
          avgFillPrice,
          failures
        }
      });

      await this.writeBehaviorLog(
        this.createBehaviorLog({
          user,
          actionType: "close_side",
          actionStatus: failures.length > 0 ? "timeout" : "success",
          traceId,
          round: currentRound,
          snapshot,
          direction: payload.side,
          positionNotional: totalProceeds,
          exitType: "close_side",
          exitOdds: avgFillPrice,
          actualFillPrice: avgFillPrice,
          executionLatencyMs: orders.reduce((sum, order) => sum + order.matchLatencyMs, 0),
          partialFilled: failures.length > 0,
          unfilledQty: roundNumber(Math.max(requestedQty - totalQty, 0), 4),
          contextJson: {
            failures,
            closedPositionsCount
          }
        })
      );

      return {
        closedPositionsCount,
        totalQty,
        totalProceeds,
        avgFillPrice,
        failures
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : "Close side failed.";
      const serverNow = Date.now();
      await this.writeAuditLog({
        eventId: this.store.newId("evt"),
        traceId,
        category: "operation",
        actionType: "close_side",
        actionStatus: "failed",
        userId: user.id,
        role: user.role,
        pageName: "trade.main",
        moduleName: "quick.actions",
        symbol: this.config.symbol,
        roundId: currentRound?.id,
        clientSendTs: payload.clientSendTs,
        serverRecvTs: serverNow,
        serverPublishTs: serverNow,
        backendLatencyMs: 0,
        resultCode: "CLOSE_SIDE_FAILED",
        resultMessage: message,
        details: {
          side: payload.side
        }
      });
      await this.writeBehaviorLog(
        this.createBehaviorLog({
          user,
          actionType: "close_side",
          actionStatus: "failed",
          traceId,
          round: currentRound,
          snapshot,
          direction: payload.side,
          exitType: "close_side",
          contextJson: {
            failureReason: message
          }
        })
      );
      throw error;
    }
  }

  async reverseSide(user: UserRecord, payload: { side: TradeSide; clientSendTs?: number }) {
    const traceId = this.store.newTraceId();
    const snapshot = this.captureActionSnapshot();
    const now = Date.now();
    const currentRound = this.getActiveRound(now);
    try {
      this.assertCanCreateNewOrder(currentRound, now);
      const closeResult = await this.closeSide(user, payload);
      if (closeResult.totalProceeds <= 0) {
        throw new Error("Reverse side requires positive proceeds from the close action.");
      }
      const reverseSide = payload.side === "UP" ? "DOWN" : "UP";
      const result = await this.placeOrder(user, {
        side: reverseSide,
        amount: closeResult.totalProceeds,
        clientSendTs: payload.clientSendTs
      });
      const serverNow = Date.now();
      await this.writeAuditLog({
        eventId: this.store.newId("evt"),
        traceId,
        category: "operation",
        actionType: "reverse_side",
        actionStatus: "success",
        userId: user.id,
        role: user.role,
        pageName: "trade.main",
        moduleName: "quick.actions",
        symbol: this.config.symbol,
        roundId: currentRound?.id,
        clientSendTs: payload.clientSendTs,
        serverRecvTs: serverNow,
        serverPublishTs: serverNow,
        backendLatencyMs: result.order.matchLatencyMs,
        resultCode: "REVERSE_SIDE_COMPLETED",
        resultMessage: "Side was closed and the opposite side was bought.",
        details: {
          requestedSide: payload.side,
          reverseSide,
          closeResult,
          reverseOrderId: result.order.id
        }
      });
      await this.writeBehaviorLog(
        this.createBehaviorLog({
          user,
          actionType: "reverse_side",
          actionStatus: "success",
          traceId,
          round: currentRound,
          snapshot,
          direction: reverseSide,
          positionNotional: closeResult.totalProceeds,
          entryOdds: snapshot[reverseSide === "UP" ? "upPrice" : "downPrice"],
          actualFillPrice: result.order.avgFillPrice,
          slippageBps: result.order.slippageBps,
          partialFilled: result.order.partialFilled,
          unfilledQty: result.order.unfilledQty,
          executionLatencyMs: result.order.matchLatencyMs,
          contextJson: {
            requestedSide: payload.side,
            reverseSide,
            closeResult,
            reverseOrderId: result.order.id
          }
        })
      );

      return {
        closeResult,
        reverseSide,
        reverseOrder: result.order
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : "Reverse side failed.";
      const serverNow = Date.now();
      await this.writeAuditLog({
        eventId: this.store.newId("evt"),
        traceId,
        category: "operation",
        actionType: "reverse_side",
        actionStatus: "failed",
        userId: user.id,
        role: user.role,
        pageName: "trade.main",
        moduleName: "quick.actions",
        symbol: this.config.symbol,
        roundId: currentRound?.id,
        clientSendTs: payload.clientSendTs,
        serverRecvTs: serverNow,
        serverPublishTs: serverNow,
        backendLatencyMs: 0,
        resultCode: "REVERSE_SIDE_FAILED",
        resultMessage: message,
        details: {
          side: payload.side
        }
      });
      await this.writeBehaviorLog(
        this.createBehaviorLog({
          user,
          actionType: "reverse_side",
          actionStatus: "failed",
          traceId,
          round: currentRound,
          snapshot,
          direction: payload.side === "UP" ? "DOWN" : "UP",
          exitType: "reverse_side",
          contextJson: {
            failureReason: message
          }
        })
      );
      throw error;
    }
  }

  private captureActionSnapshot() {
    const snapshot = this.store.marketSnapshot;
    if (snapshot.marketId) {
      return snapshot;
    }
    return this.buildSnapshot();
  }

  private createBehaviorLog(input: {
    user: UserRecord;
    actionType: string;
    actionStatus: BehaviorActionLog["actionStatus"];
    traceId?: string;
    orderId?: string;
    round?: RoundRecord;
    snapshot: MarketSnapshot;
    direction?: TradeSide;
    entryOdds?: number;
    positionNotional?: number;
    exitType?: string;
    exitOdds?: number;
    settlementResult?: PositionRecord["settlementResult"];
    bookSnapshot?: OrderBookSnapshot;
    actualFillPrice?: number;
    slippageBps?: number;
    partialFilled?: boolean;
    unfilledQty?: number;
    executionLatencyMs?: number;
    settlementDirection?: TradeSide;
    settlementTimeMs?: number;
    gammaPollCount?: number;
    redeemFinishTimeMs?: number;
    order?: OrderRecord;
    failureReason?: string;
    frozenAssetRelease?: Record<string, unknown>;
    contextJson?: Record<string, unknown>;
  }): BehaviorActionLog {
    const direction = input.direction;
    const round = input.round;
    const snapshot = input.snapshot;
    const bookSnapshot =
      input.bookSnapshot ??
      (direction ? snapshot.orderBooks[direction] : snapshot.orderBooks.UP);
    const candles = snapshot.binance.candlesByInterval;
    const contextJson = {
      ...(input.order
        ? {
            requestAction: input.order.action,
            requestedAmountUsdc: input.order.requestedAmountUsdc,
            requestedQty: input.order.requestedQty,
            orderKind: input.order.orderKind,
            timeInForce: input.order.timeInForce,
            limitPrice: input.order.limitPrice,
            lifecycleStatus: input.order.lifecycleStatus,
            resultType: input.order.resultType,
            bookKey: input.order.bookKey,
            bookSnapshotId: input.order.bookHash,
            marketId: input.order.marketId,
            marketSlug: input.order.marketSlug,
            fills: input.order.fills,
            failureReason: input.order.failureReason
          }
        : {}),
      ...(input.frozenAssetRelease ? { frozenAssetRelease: input.frozenAssetRelease } : {}),
      ...(input.failureReason ? { failureReason: input.failureReason } : {}),
      ...(input.contextJson ?? {})
    };
    return {
      logId: this.store.newId("blog"),
      timestampMs: Date.now(),
      assetClass: "BTC_5M_UPDOWN",
      actionType: input.actionType,
      actionStatus: input.actionStatus,
      roundId: round?.id,
      direction,
      entryOdds: input.entryOdds,
      deltaClob: snapshot.clob.delta,
      volumeClob: snapshot.clob.volume,
      positionNotional: input.positionNotional,
      exitType: input.exitType,
      exitOdds: input.exitOdds,
      settlementResult: input.settlementResult,
      testerIdAnon: this.store.anonymizeUserId(input.user.id),
      traceId: input.traceId,
      orderId: input.orderId,
      marketId: snapshot.marketId,
      marketSlug: snapshot.marketSlug,
      roundStatus: round?.status,
      countdownMs: snapshot.uiMeta.countdownMs,
      binanceSpotPrice: snapshot.binance.spotPrice,
      binance1mLastClose: candles["1m"].at(-1)?.close ?? 0,
      binance5mLastClose: candles["5m"].at(-1)?.close ?? 0,
      binance1dLastClose: candles["1d"].at(-1)?.close ?? 0,
      chainlinkPrice: snapshot.chainlink.referencePrice,
      priceToBeat: snapshot.priceToBeat,
      upPrice: snapshot.upPrice,
      downPrice: snapshot.downPrice,
      upBookTop5: snapshot.orderBooks.UP.bids.slice(0, 5),
      downBookTop5: snapshot.orderBooks.DOWN.bids.slice(0, 5),
      recentTradesTop20: snapshot.recentTrades.slice(0, 20),
      bookSnapshotEntry: {
        snapshotId: bookSnapshot.snapshotId,
        snapshotTs: bookSnapshot.snapshotTs,
        topBids: bookSnapshot.bids.slice(0, 5),
        topAsks: bookSnapshot.asks.slice(0, 5)
      },
      actualFillPrice: input.actualFillPrice,
      slippageBps: input.slippageBps,
      partialFilled: input.partialFilled,
      unfilledQty: input.unfilledQty,
      executionLatencyMs: input.executionLatencyMs,
      settlementDirection: input.settlementDirection,
      settlementTimeMs: input.settlementTimeMs,
      gammaPollCount: input.gammaPollCount,
      redeemFinishTimeMs: input.redeemFinishTimeMs,
      sourceStates: {
        binance: this.pickSourceState(snapshot.sources.binance),
        chainlink: this.pickSourceState(snapshot.sources.chainlink),
        clob: this.pickSourceState(snapshot.sources.clob)
      },
      contextJson
    };
  }

  private pickSourceState(source: SourceHealth) {
    return {
      source: source.source,
      state: source.state,
      sourceEventTs: source.sourceEventTs,
      serverRecvTs: source.serverRecvTs,
      serverPublishTs: source.serverPublishTs
    };
  }

  private async writeBehaviorLog(log: BehaviorActionLog) {
    await this.store.recordBehaviorLog(log);
  }

  private scheduleReconcile() {
    if (this.reconcileRunning) {
      this.reconcileQueued = true;
      return;
    }

    this.reconcileRunning = true;
    void this.reconcileLoop();
  }

  private async reconcileLoop() {
    try {
      do {
        this.reconcileQueued = false;
        await this.reconcileOnce();
      } while (this.reconcileQueued);
    } finally {
      this.reconcileRunning = false;
    }
  }

  private getActiveRound(now = Date.now()) {
    return this.store.rounds
      .filter((round) => round.startAt <= now && round.endAt > now)
      .sort((left, right) => right.startAt - left.startAt)[0];
  }

  private canCreateNewOrders(round: RoundRecord | undefined, now = Date.now()) {
    if (!round) {
      return false;
    }
    if (now < round.startAt || now >= round.endAt) {
      return false;
    }
    if (round.acceptingOrders === false || round.status !== "Trading") {
      return false;
    }
    return round.endAt - now > this.config.freezeWindowMs;
  }

  private assertCanCreateNewOrder(round: RoundRecord | undefined, now = Date.now()) {
    if (!round || now < round.startAt || now >= round.endAt) {
      throw new Error("No active round is available.");
    }
    if (round.endAt - now <= this.config.freezeWindowMs) {
      throw new Error("Round entered final 10-second order freeze window.");
    }
    if (round.acceptingOrders === false) {
      throw new Error("Round is not accepting new orders.");
    }
    if (round.status !== "Trading") {
      throw new Error(`Round is ${round.status}.`);
    }
  }

  private assertCanSellPosition(position: PositionRecord, round: RoundRecord | undefined, now = Date.now()) {
    if (!round) {
      throw new Error("Position round is unavailable.");
    }
    if (position.status !== "open") {
      throw new Error("Position is already closed.");
    }
    const activeRound = this.getActiveRound(now);
    if (!activeRound || activeRound.id !== round.id || now < round.startAt || now >= round.endAt) {
      throw new Error("This position does not belong to the current tradable round.");
    }
    if (round.endAt - now <= this.config.freezeWindowMs) {
      throw new Error("Current round entered the final 10-second sell freeze window.");
    }
    if (round.acceptingOrders === false) {
      throw new Error("Current round is not accepting sell orders.");
    }
    if (round.status !== "Trading") {
      throw new Error("Current round is frozen and can no longer sell positions.");
    }
  }

  private resolveBookContext(side: TradeSide, round?: RoundRecord, marketId?: string) {
    const resolvedMarketId = marketId ?? this.polymarketState.currentMarket?.id ?? round?.marketId ?? this.config.marketId;
    const resolvedRound = round ?? this.getActiveRound() ?? this.store.getCurrentRound();
    return {
      bookKey: `${resolvedMarketId}:${side}`,
      marketId: resolvedMarketId,
      roundId: resolvedRound?.id
    };
  }

  private resolveReplayBookKey(input: {
    side?: TradeSide;
    bookKey?: string;
    roundId?: string;
    marketId?: string;
  }) {
    if (input.bookKey) {
      return input.bookKey;
    }
    const round = input.roundId ? this.store.getRoundById(input.roundId) : undefined;
    return this.resolveBookContext(input.side ?? "UP", round, input.marketId).bookKey;
  }

  private async syncMatchingBooks() {
    const activeRound = this.getActiveRound();
    await Promise.all([this.syncMatchingBook("UP", activeRound), this.syncMatchingBook("DOWN", activeRound)]);
  }

  private async ensureCurrentMatchingBook(side: TradeSide, round?: RoundRecord, forceSync = false) {
    const state = await this.syncMatchingBook(side, round, forceSync);
    return state?.snapshot ?? this.polymarketState.orderBooks[side];
  }

  private async syncMatchingBook(side: TradeSide, round?: RoundRecord, forceSync = false) {
    const sourceBook = this.polymarketState.orderBooks[side];
    const context = this.resolveBookContext(side, round);
    const cached = this.matchingBooks.get(context.bookKey);
    this.currentBookKeys.set(side, context.bookKey);

    if (!forceSync && cached && this.lastSyncedSnapshotIds.get(context.bookKey) === sourceBook.snapshotId) {
      return cached;
    }

    try {
      const response = await this.matchingClient.syncBook({
        bookKey: context.bookKey,
        roundId: context.roundId,
        marketId: context.marketId,
        bookSide: side,
        source: "Polymarket",
        sourceSnapshot: sourceBook,
        syncedAt: Date.now()
      });
      this.cacheMatchingBook(response.book);
      this.lastSyncedSnapshotIds.set(context.bookKey, sourceBook.snapshotId);
      return response.book;
    } catch {
      if (cached) {
        return cached;
      }

      const current = await this.matchingClient.getCurrentBook(context.bookKey).catch(() => ({ book: undefined }));
      if (current.book) {
        this.cacheMatchingBook(current.book);
        return current.book;
      }
      return undefined;
    }
  }

  private async refreshMatchingBook(bookKey: string) {
    const response = await this.matchingClient.getCurrentBook(bookKey);
    if (response.book) {
      this.cacheMatchingBook(response.book);
    }
    return response.book;
  }

  private cacheMatchingBook(book: MatchingBookState) {
    this.matchingBooks.set(book.bookKey, book);
    this.currentBookKeys.set(book.bookSide, book.bookKey);
  }

  private getDisplayedBook(side: TradeSide): OrderBookSnapshot {
    return this.polymarketState.orderBooks[side];
  }

  private async fetchExecutionBook(side: TradeSide, round: RoundRecord) {
    const tokenId = this.resolveTokenId(side, round);
    let book: OrderBookSnapshot;
    if (tokenId) {
      book = await this.polymarketConnector.fetchBookByToken(tokenId);
    } else {
      book = await this.polymarketConnector.fetchBookForSide(side);
    }
    if ((side === "UP" || side === "DOWN") && book.bids.length === 0 && book.asks.length === 0) {
      const fallback = this.polymarketState.orderBooks[side];
      if (fallback.bids.length > 0 || fallback.asks.length > 0) {
        return fallback;
      }
      throw new Error("Polymarket CLOB depth is unavailable.");
    }
    return book;
  }

  private resolveTokenId(side: TradeSide, round?: RoundRecord) {
    return side === "UP"
      ? round?.upTokenId ?? this.polymarketState.currentMarket?.upTokenId
      : round?.downTokenId ?? this.polymarketState.currentMarket?.downTokenId;
  }

  private scopedPositions(userId: string, roundId: string, side: TradeSide, positionIds?: string[]) {
    const allowed = positionIds ? new Set(positionIds) : undefined;
    return this.store.positions
      .filter(
        (position) =>
          position.userId === userId &&
          position.roundId === roundId &&
          position.side === side &&
          position.status === "open" &&
          (!allowed || allowed.has(position.id))
      )
      .sort((left, right) => left.openedAt - right.openedAt);
  }

  private availableSellQty(userId: string, roundId: string, side: TradeSide, positionIds?: string[]) {
    return roundNumber(
      this.scopedPositions(userId, roundId, side, positionIds).reduce(
        (sum, position) => sum + Math.max(position.qty - (position.lockedQty ?? 0), 0),
        0
      ),
      4
    );
  }

  private async lockSellQty(
    userId: string,
    roundId: string,
    side: TradeSide,
    qty: number,
    positionIds?: string[]
  ) {
    let remaining = qty;
    for (const position of this.scopedPositions(userId, roundId, side, positionIds)) {
      if (remaining <= QTY_EPSILON) {
        break;
      }
      const available = Math.max(position.qty - (position.lockedQty ?? 0), 0);
      const take = roundNumber(Math.min(available, remaining), 4);
      if (take <= QTY_EPSILON) {
        continue;
      }
      position.lockedQty = roundNumber((position.lockedQty ?? 0) + take, 4);
      remaining = roundNumber(Math.max(remaining - take, 0), 4);
      await this.store.persistPosition(position);
    }
    if (remaining > QTY_EPSILON) {
      throw new Error("Insufficient unlocked position quantity.");
    }
  }

  private async unlockSellQty(userId: string, roundId: string, side: TradeSide, qty: number) {
    let remaining = qty;
    for (const position of this.scopedPositions(userId, roundId, side)) {
      if (remaining <= QTY_EPSILON) {
        break;
      }
      const locked = position.lockedQty ?? 0;
      const release = roundNumber(Math.min(locked, remaining), 4);
      if (release <= QTY_EPSILON) {
        continue;
      }
      position.lockedQty = roundNumber(Math.max(locked - release, 0), 4);
      remaining = roundNumber(Math.max(remaining - release, 0), 4);
      await this.store.persistPosition(position);
    }
  }

  private async applyFilledOrder(
    user: UserRecord,
    round: RoundRecord,
    order: OrderRecord,
    estimate: ClobExecutionEstimate,
    positionIds?: string[]
  ) {
    order.status = "filled";
    order.lifecycleStatus = "filled";
    order.resultType = "all_filled";
    order.fills = estimate.fills;
    order.filledQty = roundNumber(estimate.filledQty, 4);
    order.unfilledQty = 0;
    order.notionalUsdc = roundNumber(estimate.matchedNotional, 2);
    order.avgFillPrice = estimate.avgPrice ? roundNumber(estimate.avgPrice, 4) : undefined;
    order.failureReason = undefined;
    order.partialFilled = false;
    order.serverPublishTs = Date.now();

    if (order.action === "buy") {
      const spend = roundNumber(estimate.matchedNotional, 2);
      if (order.frozenUsdc && order.frozenUsdc > 0) {
        user.availableUsdc = roundNumber(user.availableUsdc + Math.max(order.frozenUsdc - spend, 0), 2);
        order.frozenUsdc = 0;
      } else {
        user.availableUsdc = roundNumber(user.availableUsdc - spend, 2);
      }
      const position = this.upsertBuyPosition(
        user.id,
        round.id,
        order.side,
        order.filledQty,
        estimate.matchedNotional,
        order.midPrice || order.avgFillPrice || 0
      );
      await this.store.persistPosition(position);
      await this.store.persistUser(user);
      await this.store.persistOrder(order);
      this.store.emitUserPayload(user.id);
      return;
    }

    let remainingQty = order.filledQty;
    const proceedsPerQty = estimate.matchedNotional / Math.max(order.filledQty, QTY_EPSILON);
    const positions = this.scopedPositions(user.id, round.id, order.side, positionIds);
    for (const position of positions) {
      if (remainingQty <= QTY_EPSILON) {
        break;
      }
      const available = order.frozenQty && order.frozenQty > 0
        ? position.lockedQty ?? 0
        : Math.max(position.qty - (position.lockedQty ?? 0), 0);
      const take = roundNumber(Math.min(available, remainingQty), 4);
      if (take <= QTY_EPSILON) {
        continue;
      }
      const proceeds = roundNumber(take * proceedsPerQty, 8);
      const releasedCost = roundNumber(position.averageEntry * take, 8);
      const realizedPnl = proceeds - releasedCost;
      position.qty = roundNumber(Math.max(position.qty - take, 0), 4);
      position.lockedQty = roundNumber(Math.max((position.lockedQty ?? 0) - take, 0), 4);
      position.notionalSpent = roundNumber(Math.max(position.notionalSpent - releasedCost, 0), 4);
      position.realizedPnl = roundNumber(position.realizedPnl + realizedPnl, 2);
      position.currentMark = roundNumber(order.midPrice || order.avgFillPrice || 0, 4);
      position.unrealizedPnl = roundNumber(position.qty * position.currentMark - position.notionalSpent, 2);
      if (position.qty <= QTY_EPSILON) {
        position.qty = 0;
        position.lockedQty = 0;
        position.notionalSpent = 0;
        position.status = "closed";
        position.closedAt = Date.now();
        position.currentMark = roundNumber(order.avgFillPrice ?? 0, 4);
        position.currentBid = undefined;
        position.currentAsk = undefined;
        position.currentMid = undefined;
        position.currentValue = 0;
        position.sourceLatencyMs = undefined;
        position.unrealizedPnl = 0;
        position.settlementResult = "sold";
      }
      remainingQty = roundNumber(Math.max(remainingQty - take, 0), 4);
      await this.store.persistPosition(position);
    }
    if (remainingQty > QTY_EPSILON) {
      throw new Error("Filled sell order could not be applied to local positions.");
    }
    user.availableUsdc = roundNumber(user.availableUsdc + estimate.matchedNotional, 2);
    order.frozenQty = 0;
    await this.store.persistUser(user);
    await this.store.persistOrder(order);
    this.store.emitUserPayload(user.id);
  }

  private async processPendingOrders() {
    const pendingOrders = this.store.orders.filter((order) => order.status === "pending");
    if (pendingOrders.length === 0) {
      return;
    }

    const books: Record<TradeSide, OrderBookSnapshot> = {
      UP: this.polymarketState.orderBooks.UP,
      DOWN: this.polymarketState.orderBooks.DOWN
    };

    for (const order of pendingOrders) {
      const round = this.store.getRoundById(order.roundId);
      const user = this.store.getUserById(order.userId);
      if (!round || !user) {
        continue;
      }

      if (!this.canCreateNewOrders(round)) {
        await this.failPendingOrder(
          user,
          order,
          "Round entered the final order freeze window before the limit order fully matched."
        );
        continue;
      }

      const book = books[order.side];
      if (!book || (book.bids.length === 0 && book.asks.length === 0)) {
        continue;
      }

      const estimate = estimateClobExecution({
        action: order.action,
        book,
        orderId: order.id,
        notional: order.action === "buy" ? order.frozenUsdc || order.requestedAmountUsdc : undefined,
        qty: order.action === "sell" ? order.frozenQty || order.requestedQty : undefined,
        limitPrice: order.limitPrice,
        executedAt: Date.now()
      });

      if (!estimate.fullyMatched) {
        continue;
      }

      order.bookHash = book.snapshotId;
      order.bestBid = book.bestBid;
      order.bestAsk = book.bestAsk;
      order.midPrice = book.midPrice;
      order.bookSnapshotTs = book.snapshotTs;
      order.sourceLatencyMs = Math.max(Date.now() - book.snapshotTs, 0);
      order.matchLatencyMs = Math.max(Date.now() - order.createdAt, 1);
      await this.applyFilledOrder(user, round, order, estimate);
      await this.writeAuditLog({
        eventId: this.store.newId("evt"),
        traceId: order.traceId,
        category: "matching",
        actionType: "limit_order_triggered",
        actionStatus: "success",
        userId: user.id,
        role: user.role,
        pageName: "trade.main",
        moduleName: "pending.orders",
        symbol: order.symbol,
        roundId: order.roundId,
        serverRecvTs: Date.now(),
        serverPublishTs: Date.now(),
        backendLatencyMs: order.matchLatencyMs,
        resultCode: "LIMIT_ORDER_FILLED",
        resultMessage: "Pending paper limit order fully matched against live Polymarket CLOB depth.",
        details: {
          traceId: order.traceId,
          roundId: order.roundId,
          marketId: order.marketId,
          marketSlug: order.marketSlug,
          orderId: order.id,
          bookKey: order.bookKey,
          bookHash: order.bookHash,
          bookSnapshotId: order.bookHash,
          sourceLatencyMs: order.sourceLatencyMs,
          avgFillPrice: order.avgFillPrice,
          filledQty: order.filledQty,
          slippageBps: order.slippageBps
        }
      });
      await this.writeBehaviorLog(
        this.createBehaviorLog({
          user,
          actionType: "limit_order_triggered",
          actionStatus: "success",
          traceId: order.traceId,
          orderId: order.id,
          round,
          snapshot: this.captureActionSnapshot(),
          direction: order.side,
          positionNotional: order.notionalUsdc,
          bookSnapshot: book,
          order,
          actualFillPrice: order.avgFillPrice,
          slippageBps: order.slippageBps,
          partialFilled: order.partialFilled,
          unfilledQty: order.unfilledQty,
          executionLatencyMs: order.matchLatencyMs
        })
      );
    }
  }

  private async failPendingOrder(user: UserRecord, order: OrderRecord, reason: string) {
    const releasedFrozenUsdc = order.frozenUsdc ?? 0;
    const releasedFrozenQty = order.frozenQty ?? 0;
    if (order.frozenUsdc && order.frozenUsdc > 0) {
      user.availableUsdc = roundNumber(user.availableUsdc + order.frozenUsdc, 2);
      order.frozenUsdc = 0;
      await this.store.persistUser(user);
    }
    if (order.frozenQty && order.frozenQty > 0) {
      await this.unlockSellQty(user.id, order.roundId, order.side, order.frozenQty);
      order.frozenQty = 0;
    }
    order.status = "failed";
    order.lifecycleStatus = "failed";
    order.resultType = "all_failed";
    order.failureReason = reason;
    order.serverPublishTs = Date.now();
    await this.store.persistOrder(order);
    const now = Date.now();
    const snapshot = this.captureActionSnapshot();
    const round = this.store.getRoundById(order.roundId);
    await this.writeAuditLog({
      eventId: this.store.newId("evt"),
      traceId: order.traceId,
      category: "matching",
      actionType: "limit_order_failed",
      actionStatus: "failed",
      userId: user.id,
      role: user.role,
      pageName: "trade.main",
      moduleName: "pending.orders",
      symbol: order.symbol,
      roundId: order.roundId,
      serverRecvTs: now,
      serverPublishTs: now,
      backendLatencyMs: order.matchLatencyMs,
      resultCode: "LIMIT_ORDER_FAILED",
      resultMessage: reason,
      details: {
        traceId: order.traceId,
        roundId: order.roundId,
        marketId: order.marketId,
        marketSlug: order.marketSlug,
        orderId: order.id,
        bookKey: order.bookKey,
        bookSnapshotId: order.bookHash,
        releasedFrozenUsdc,
        releasedFrozenQty,
        failureReason: reason
      }
    });
    await this.writeBehaviorLog(
      this.createBehaviorLog({
        user,
        actionType: "limit_order_failed",
        actionStatus: "failed",
        traceId: order.traceId,
        orderId: order.id,
        round,
        snapshot,
        direction: order.side,
        positionNotional: order.notionalUsdc,
        bookSnapshot: snapshot.orderBooks[order.side],
        order,
        partialFilled: order.partialFilled,
        unfilledQty: order.unfilledQty,
        failureReason: reason,
        frozenAssetRelease: {
          releasedFrozenUsdc,
          releasedFrozenQty
        }
      })
    );
    this.store.emitUserPayload(user.id);
  }

  private schedulePendingOrderProcessing() {
    if (this.pendingOrdersRunning) {
      return;
    }
    this.pendingOrdersRunning = true;
    void this.processPendingOrders()
      .catch((error) => {
        console.warn("[simulation] Pending order processing failed:", error);
      })
      .finally(() => {
        this.pendingOrdersRunning = false;
      });
  }

  private async reconcileOnce() {
    await this.syncDiscoveredRounds();
    await this.syncCurrentRoundMarket();
    await this.capturePriceToBeat();
    const roundChangedUsers = await this.processRounds();
    const snapshot = this.buildSnapshot();
    const changedUsers = this.refreshOpenPositions(snapshot);
    await this.store.setMarketSnapshot(snapshot);
    await this.emitLatencyLogs(snapshot);
    for (const userId of new Set([...roundChangedUsers, ...changedUsers])) {
      this.store.emitUserPayload(userId);
    }
    this.schedulePendingOrderProcessing();
  }

  private async syncDiscoveredRounds() {
    const now = Date.now();
    for (const discovered of this.polymarketState.discoveredRounds) {
      const currentMarket = this.polymarketState.currentMarket;
      const isCurrentMarket = currentMarket?.slug === discovered.marketSlug;
      const existing = this.store.getRoundById(discovered.id);
      const merged: RoundRecord = {
        ...discovered,
        marketId: discovered.marketId || existing?.marketId || this.config.marketId,
        eventId: discovered.eventId ?? existing?.eventId,
        marketSlug: discovered.marketSlug ?? existing?.marketSlug,
        eventSlug: discovered.eventSlug ?? existing?.eventSlug,
        conditionId: discovered.conditionId ?? existing?.conditionId,
        seriesSlug: discovered.seriesSlug ?? existing?.seriesSlug,
        upTokenId: discovered.upTokenId ?? existing?.upTokenId,
        downTokenId: discovered.downTokenId ?? existing?.downTokenId,
        title: discovered.title ?? existing?.title,
        resolutionSource: discovered.resolutionSource ?? existing?.resolutionSource,
        priceToBeat:
          existing?.priceToBeat && existing.priceToBeat > 0
            ? existing.priceToBeat
            : discovered.startAt <= now && discovered.endAt > now
              ? this.captureReferencePrice().price
              : 0,
        status: existing?.status ?? discovered.status,
        pollCount: existing?.pollCount ?? discovered.pollCount,
        pollStartAt: existing?.pollStartAt,
        lastPollAt: existing?.lastPollAt,
        closingSpotPrice: existing?.closingSpotPrice,
        settledSide: existing?.settledSide,
        settlementPrice: existing?.settlementPrice,
        settlementTs: existing?.settlementTs,
        settlementSource: existing?.settlementSource,
        polymarketSettlementPrice: existing?.polymarketSettlementPrice,
        polymarketSettlementStatus: existing?.polymarketSettlementStatus,
        polymarketOpenPrice: isBtcReferencePrice(existing?.polymarketOpenPrice) ? existing.polymarketOpenPrice : undefined,
        polymarketClosePrice: isBtcReferencePrice(existing?.polymarketClosePrice) ? existing.polymarketClosePrice : undefined,
        polymarketOpenPriceSource: isBtcReferencePrice(existing?.polymarketOpenPrice) ? existing?.polymarketOpenPriceSource : undefined,
        polymarketClosePriceSource: isBtcReferencePrice(existing?.polymarketClosePrice) ? existing?.polymarketClosePriceSource : undefined,
        settlementReceivedAt: existing?.settlementReceivedAt,
        redeemScheduledAt: existing?.redeemScheduledAt,
        binanceOpenPrice: existing?.binanceOpenPrice,
        binanceClosePrice: existing?.binanceClosePrice,
        redeemStartTs: existing?.redeemStartTs,
        redeemFinishTs: existing?.redeemFinishTs,
        manualReason: existing?.manualReason,
        acceptingOrders:
          isCurrentMarket && currentMarket
            ? currentMarket.acceptingOrders
            : discovered.acceptingOrders ?? existing?.acceptingOrders,
        closingPriceSource: existing?.closingPriceSource
      };
      if (this.roundChanged(existing, merged)) {
        await this.store.upsertRound(merged);
      }
    }
  }

  private async capturePriceToBeat() {
    const now = Date.now();
    const activeRound = this.store.rounds.find((round) => round.startAt <= now && round.endAt > now);
    if (!activeRound || activeRound.priceToBeat > 0) {
      return;
    }

    const reference = this.captureReferencePrice();
    if (reference.price <= 0) {
      return;
    }

    activeRound.priceToBeat = reference.price;
    await this.store.upsertRound(activeRound);
    await this.writeAuditLog({
      eventId: this.store.newId("evt"),
      traceId: this.store.newTraceId(),
      category: "operation",
      actionType: "capture_price_to_beat",
      actionStatus: "success",
      pageName: "trade.main",
      moduleName: "round.lifecycle",
      symbol: activeRound.symbol,
      roundId: activeRound.id,
      serverRecvTs: now,
      serverPublishTs: now,
      backendLatencyMs: 0,
      resultCode: "PRICE_TO_BEAT_CAPTURED",
      resultMessage: `Captured round reference price from ${reference.source}.`,
      details: {
        marketSlug: activeRound.marketSlug,
        priceToBeat: activeRound.priceToBeat,
        source: reference.source
      }
    });
  }

  private async processRounds() {
    const now = Date.now();
    const changedUsers = new Set<string>();
    const rounds = [...this.store.rounds]
      .filter((round) => round.endAt >= now - 2 * 60 * 60 * 1000)
      .sort((left, right) => left.startAt - right.startAt);

    for (const round of rounds) {
      const before = this.roundSignature(round);
      const liveDetail =
        this.polymarketState.currentMarket?.slug === round.marketSlug ? this.polymarketState.currentMarket : undefined;

      if (liveDetail) {
        this.applyMarketMetadata(round, liveDetail);
      }

      if (!round.priceToBeat && round.startAt <= now && round.endAt > now) {
        const reference = this.captureReferencePrice();
        if (reference.price > 0) {
          round.priceToBeat = reference.price;
        }
      }

      if (!round.binanceOpenPrice && round.startAt <= now && this.binanceState.price > 0) {
        round.binanceOpenPrice = roundNumber(round.priceToBeat || this.binanceState.price, 2);
      }

      if (!round.binanceClosePrice && now >= round.endAt && this.binanceState.price > 0) {
        round.binanceClosePrice = roundNumber(this.binanceState.price, 2);
      }

      if (!round.closingSpotPrice && now >= round.endAt && this.binanceState.price > 0) {
        round.closingSpotPrice = roundNumber(this.binanceState.price, 2);
        round.closingPriceSource = "Gamma";
      }
      await this.hydrateRoundPolymarketReferencePrices(round, now);

      if (
        round.status === "Manual" &&
        !round.settledSide &&
        !round.redeemFinishTs &&
        now >= round.endAt &&
        (round.marketId || round.marketSlug)
      ) {
        const canRetryManual = !round.lastPollAt || now - round.lastPollAt >= MANUAL_SETTLEMENT_RETRY_MS;
        if (canRetryManual) {
          round.status = "Polling";
          round.pollCount = 0;
          round.pollStartAt = undefined;
          round.manualReason = undefined;
        }
      }

      const resolved = this.polymarketState.lastResolvedMarket;
      if (
        resolved &&
        !round.settledSide &&
        (resolved.marketSlug === round.marketSlug || resolved.conditionId === round.conditionId)
      ) {
        if (now >= round.endAt && round.status !== "Closed") {
          this.finalizeSettlementFromResolved(round, resolved);
          await this.writeSettlementLog(round, "success", "Polymarket market_resolved event confirmed settlement.");
        }
      }

      const nextStatus = this.computeRoundStatus(round, now);
      if (nextStatus !== round.status) {
        round.status = nextStatus;
      }

      if (round.status === "Polling") {
        this.scheduleSettlementPoll(round, now);
      }

      if (round.status === "Settled") {
        round.redeemStartTs = round.redeemStartTs ?? round.settlementReceivedAt ?? round.settlementTs ?? now;
        round.redeemScheduledAt = round.redeemScheduledAt ?? round.redeemStartTs + REDEEM_DELAY_MS;
        round.status = "Redeeming";
      }

      if (round.status === "Redeeming" && round.redeemStartTs && !round.redeemScheduledAt) {
        round.redeemScheduledAt = round.redeemStartTs + REDEEM_DELAY_MS;
      }

      if (
        round.status === "Redeeming" &&
        !round.redeemFinishTs &&
        round.redeemScheduledAt &&
        now >= round.redeemScheduledAt
      ) {
        await this.applyRedeem(round);
      }

      if (this.roundSignature(round) !== before) {
        await this.store.upsertRound(round);
        for (const userId of this.collectRoundPositionUsers(round.id)) {
          changedUsers.add(userId);
        }
      }
    }

    return changedUsers;
  }

  private scheduleSettlementPoll(round: RoundRecord, now: number) {
    if (this.pollLocks.has(round.id)) {
      return;
    }
    const before = this.roundSignature(round);
    void this.pollSettlement(round, now)
      .then(async () => {
        if (this.roundSignature(round) !== before) {
          await this.store.upsertRound(round);
          for (const userId of this.collectRoundPositionUsers(round.id)) {
            this.store.emitUserPayload(userId);
          }
          this.scheduleReconcile();
        }
      })
      .catch((error) => {
        console.warn(`[simulation] Settlement polling failed for ${round.id}:`, error);
      });
  }

  private buildSnapshot(): MarketSnapshot {
    const now = Date.now();
    const currentRound = this.store.getCurrentRound(now);
    const currentMarket = this.polymarketState.currentMarket;
    const matchedMarket = this.roundMatchesMarket(currentRound, currentMarket) ? currentMarket : undefined;
    const upBook = matchedMarket ? this.getDisplayedBook("UP") : this.createEmptyOrderBook("UP");
    const downBook = matchedMarket ? this.getDisplayedBook("DOWN") : this.createEmptyOrderBook("DOWN");
    const upPrice = upBook.midPrice || matchedMarket?.outcomePrices[0] || 0;
    const downPrice = downBook.midPrice || matchedMarket?.outcomePrices[1] || 0;
    const chainlinkPrice =
      this.config.chainlinkEnabled && this.chainlinkState.price > 0 ? roundNumber(this.chainlinkState.price, 2) : 0;
    const binancePrice = this.binanceState.price > 0 ? roundNumber(this.binanceState.price, 2) : 0;
    const countdownTargetTs = currentRound
      ? currentRound.startAt > now
        ? currentRound.startAt
        : currentRound.endAt
      : undefined;
    const countdownMs = countdownTargetTs ? Math.max(Math.min(countdownTargetTs - now, FIVE_MINUTE_MS), 0) : 0;
    const marketTitle = currentRound
      ? `${this.config.symbol} 5-Min Round ${utcRangeText(currentRound.startAt, currentRound.endAt)}`
      : matchedMarket
        ? `${this.config.symbol} 5-Min Round ${utcRangeText(matchedMarket.startAt, matchedMarket.endAt)}`
        : `${this.config.symbol} 5-Min Round UTC`;
    const candlesByInterval = this.binanceState.candlesByInterval;
    const recentTrades = matchedMarket ? [...this.polymarketState.recentTrades] : [];
    const clobDelta = matchedMarket ? roundNumber(this.polymarketState.delta, 4) : 0;
    const clobVolume = matchedMarket ? roundNumber(this.polymarketState.volume, 4) : 0;

    return {
      symbol: this.config.symbol,
      marketId: currentRound?.marketId ?? matchedMarket?.id ?? this.config.marketId,
      marketSlug: currentRound?.marketSlug ?? matchedMarket?.slug,
      eventId: currentRound?.eventId ?? matchedMarket?.eventId,
      eventSlug: currentRound?.eventSlug ?? matchedMarket?.eventSlug,
      conditionId: currentRound?.conditionId ?? matchedMarket?.conditionId,
      seriesSlug: currentRound?.seriesSlug ?? matchedMarket?.seriesSlug,
      serverNow: now,
      binancePrice,
      chainlinkPrice,
      currentPrice: binancePrice || chainlinkPrice,
      priceToBeat: currentRound?.priceToBeat ?? 0,
      upPrice: roundNumber(upPrice, 4),
      downPrice: roundNumber(downPrice, 4),
      sources: {
        binance: this.normalizeSourceHealth(this.binanceState.status, now),
        chainlink: this.normalizeSourceHealth(this.chainlinkState.status, now),
        clob: this.normalizeSourceHealth(this.polymarketState.status, now)
      },
      orderBooks: {
        UP: upBook,
        DOWN: downBook
      },
      recentTrades,
      candles: [...this.binanceState.candles],
      binance: {
        spotPrice: binancePrice,
        latestTick: this.binanceState.latestTick,
        candlesByInterval
      },
      chainlink: {
        referencePrice: chainlinkPrice,
        settlementReference: currentRound?.settlementPrice ?? chainlinkPrice
      },
      clob: {
        delta: clobDelta,
        volume: clobVolume,
        upBook,
        downBook,
        recentTrades,
        bestBidAskSummary: {
          UP: {
            bestBid: upBook.bestBid,
            bestAsk: upBook.bestAsk
          },
          DOWN: {
            bestBid: downBook.bestBid,
            bestAsk: downBook.bestAsk
          }
        }
      },
      uiMeta: {
        marketTitle,
        marketSubtitle: currentRound ? utcRangeText(currentRound.startAt, currentRound.endAt) : matchedMarket?.slug,
        countdownMs,
        acceptingOrders:
          this.canCreateNewOrders(currentRound, now) &&
          (matchedMarket?.acceptingOrders ?? currentRound?.acceptingOrders ?? false),
        marketSwitchState: this.getMarketSwitchState(currentRound, matchedMarket, now),
        sourceStatusSummary: [
          { source: "Binance", state: this.binanceState.status.state },
          { source: "Chainlink", state: this.chainlinkState.status.state },
          { source: "CLOB", state: this.polymarketState.status.state }
        ]
      }
    };
  }

  private getMarketSwitchState(
    currentRound: RoundRecord | undefined,
    matchedMarket: PolymarketMarketDetail | undefined,
    now: number
  ) {
    if (!currentRound) {
      return "market_not_ready" as const;
    }
    const nextMarket = this.polymarketState.nextMarket;
    if (currentRound.startAt > now) {
      return this.roundMatchesMarket(currentRound, this.polymarketState.currentMarket) ||
        this.roundMatchesMarket(currentRound, nextMarket)
        ? ("next_ready" as const)
        : ("prefetching_next" as const);
    }
    if (!matchedMarket) {
      return "prefetching_next" as const;
    }
    return nextMarket && nextMarket.startAt === currentRound.endAt ? ("next_ready" as const) : ("active" as const);
  }

  private refreshOpenPositions(snapshot: MarketSnapshot) {
    const changedUsers = new Set<string>();
    const activeRound = this.getActiveRound(snapshot.serverNow);
    for (const position of this.store.positions) {
      if (position.status !== "open") {
        continue;
      }
      if (!activeRound || position.roundId !== activeRound.id) {
        continue;
      }
      const nextMark = position.side === "UP" ? snapshot.upPrice : snapshot.downPrice;
      if (nextMark <= 0) {
        continue;
      }
      const mark = roundNumber(nextMark, 4);
      const unrealizedPnl = roundNumber(position.qty * mark - position.notionalSpent, 2);
      const book = snapshot.orderBooks[position.side];
      const currentValue = roundNumber(position.qty * mark, 2);
      if (
        position.currentMark !== mark ||
        position.unrealizedPnl !== unrealizedPnl ||
        position.currentValue !== currentValue ||
        position.currentBid !== book.bestBid ||
        position.currentAsk !== book.bestAsk
      ) {
        position.currentMark = mark;
        position.currentBid = book.bestBid;
        position.currentAsk = book.bestAsk;
        position.currentMid = book.midPrice;
        position.currentValue = currentValue;
        position.sourceLatencyMs = Math.max(snapshot.serverNow - book.snapshotTs, 0);
        position.unrealizedPnl = unrealizedPnl;
        changedUsers.add(position.userId);
      }
    }
    return changedUsers;
  }

  private normalizeSourceHealth(source: SourceHealth, publishedAt: number): SourceHealth {
    if (source.state === "disabled") {
      return {
        ...source,
        normalizedTs: publishedAt,
        serverPublishTs: publishedAt,
        acquireLatencyMs: 0,
        publishLatencyMs: 0,
        frontendLatencyMs: 0
      };
    }
    return {
      ...source,
      normalizedTs: source.normalizedTs || publishedAt,
      serverPublishTs: publishedAt,
      publishLatencyMs: Math.max(publishedAt - (source.normalizedTs || source.serverRecvTs), 0)
    };
  }

  private computeRoundStatus(round: RoundRecord, now: number): RoundStatus {
    if (round.status === "Closed" || round.status === "Manual") {
      return round.status;
    }
    if (round.redeemFinishTs) {
      return "Closed";
    }
    if (round.redeemStartTs) {
      return "Redeeming";
    }
    if (round.settledSide) {
      return "Settled";
    }

    const freezeStart = Math.max(round.endAt - this.config.freezeWindowMs, round.startAt);
    if (now < round.startAt) {
      return round.acceptingOrders === false ? "Frozen" : "Trading";
    }
    if (now < freezeStart && round.acceptingOrders !== false) {
      return "Trading";
    }
    if (now < round.endAt) {
      return "Frozen";
    }
    if (now < round.endAt + this.config.pollDelayMs) {
      return "Settling";
    }
    return "Polling";
  }

  private async pollSettlement(round: RoundRecord, now: number) {
    if ((!round.marketSlug && !round.marketId) || this.pollLocks.has(round.id)) {
      return;
    }
    if (round.lastPollAt && now - round.lastPollAt < this.config.gammaPollIntervalMs) {
      return;
    }
    if (round.pollCount >= this.config.gammaMaxPolls) {
      round.status = "Manual";
      round.manualReason = "Gamma polling timed out after maximum retries.";
      await this.writeSettlementLog(round, "timeout", "Gamma polling exceeded retry limit.");
      return;
    }

    this.pollLocks.add(round.id);
    round.pollCount += 1;
    round.lastPollAt = now;
    round.pollStartAt = round.pollStartAt ?? now;

    try {
      const detail =
        round.marketId && String(round.marketId).trim()
          ? await this.polymarketConnector.fetchMarketById(String(round.marketId))
          : await this.polymarketConnector.fetchMarketBySlug(String(round.marketSlug));
      this.applyMarketMetadata(round, detail);
      const settledSide = this.resolveSettledSide(detail);
      if (settledSide) {
        this.finalizeSettlement(round, detail, settledSide, now);
        await this.writeSettlementLog(round, "success", "Gamma market closed and settlement was confirmed.");
      } else if (round.pollCount >= this.config.gammaMaxPolls) {
        round.status = "Manual";
        round.manualReason = "Gamma market did not publish a result before retry exhaustion.";
        await this.writeSettlementLog(round, "timeout", "Gamma market did not return a final outcome in time.");
      }
    } catch (error) {
      if (round.pollCount >= this.config.gammaMaxPolls) {
        round.status = "Manual";
        round.manualReason = "Gamma market lookup failed repeatedly.";
        await this.writeSettlementLog(round, "failed", "Gamma market lookup failed repeatedly.");
      } else {
        await this.writeAuditLog({
          eventId: this.store.newId("evt"),
          traceId: this.store.newTraceId(),
          category: "settlement",
          actionType: "poll_settlement",
          actionStatus: "failed",
          pageName: "trade.main",
          moduleName: "settlement.engine",
          symbol: round.symbol,
          roundId: round.id,
          serverRecvTs: now,
          serverPublishTs: now,
          backendLatencyMs: 0,
          resultCode: "POLL_FAILED",
          resultMessage: error instanceof Error ? error.message : "Gamma market lookup failed.",
          details: {
            roundId: round.id,
            marketId: round.marketId,
            marketSlug: round.marketSlug,
            pollCount: round.pollCount,
            failureReason: error instanceof Error ? error.message : "Gamma market lookup failed."
          }
        });
      }
    } finally {
      this.pollLocks.delete(round.id);
    }
  }

  private finalizeSettlement(
    round: RoundRecord,
    detail: PolymarketMarketDetail | undefined,
    settledSide: TradeSide | undefined,
    now: number
  ) {
    if (!settledSide || !detail) {
      return;
    }
    if (!round.closingSpotPrice && this.binanceState.price > 0) {
      round.closingSpotPrice = roundNumber(this.binanceState.price, 2);
      round.closingPriceSource = "Gamma";
    }
    round.settledSide = settledSide;
    round.polymarketSettlementPrice = detail.settlementPrice ?? (settledSide === "UP" ? 1 : 0);
    round.polymarketSettlementStatus = detail.settlementStatus ?? "resolved";
    round.settlementPrice = round.polymarketSettlementPrice;
    round.settlementTs = now;
    round.settlementReceivedAt = round.settlementReceivedAt ?? now;
    round.settlementSource = "Gamma";
    round.status = "Settled";
    round.acceptingOrders = false;
    round.manualReason = undefined;
    this.applyMarketMetadata(round, detail);
  }

  private finalizeSettlementFromResolved(
    round: RoundRecord,
    resolved: NonNullable<PolymarketConnectorState["lastResolvedMarket"]>
  ) {
    if (!resolved.settledSide) {
      return;
    }
    const receivedAt = resolved.receivedAt;
    if (!round.closingSpotPrice && this.binanceState.price > 0) {
      round.closingSpotPrice = roundNumber(this.binanceState.price, 2);
      round.closingPriceSource = "Gamma";
    }
    round.settledSide = resolved.settledSide;
    round.polymarketSettlementPrice =
      resolved.settlementPrice ?? (resolved.settledSide === "UP" ? 1 : 0);
    round.polymarketSettlementStatus = "resolved";
    round.settlementPrice = round.polymarketSettlementPrice;
    round.settlementTs = receivedAt;
    round.settlementReceivedAt = receivedAt;
    round.settlementSource = "Polymarket";
    round.status = "Settled";
    round.acceptingOrders = false;
    round.manualReason = undefined;
    round.lastPollAt = undefined;
  }

  private async applyRedeem(round: RoundRecord) {
    if (!round.settledSide || round.redeemFinishTs) {
      return;
    }
    const closedAt = Date.now();
    const userIds = new Set<string>();
    const positions = this.store.positions.filter(
      (position) => position.roundId === round.id && position.status === "open"
    );

    for (const position of positions) {
      const user = this.store.getUserById(position.userId);
      if (!user) {
        continue;
      }
      const snapshot = this.captureActionSnapshot();
      const isWinner = position.side === round.settledSide;
      const redeemAmount = isWinner ? position.qty : 0;
      const realizedPnl = redeemAmount - position.notionalSpent;
      user.availableUsdc = roundNumber(user.availableUsdc + redeemAmount, 2);
      position.realizedPnl = roundNumber(position.realizedPnl + realizedPnl, 2);
      position.unrealizedPnl = 0;
      position.status = "closed";
      position.closedAt = closedAt;
      position.currentMark = isWinner ? 1 : 0;
      position.currentBid = undefined;
      position.currentAsk = undefined;
      position.currentMid = undefined;
      position.sourceLatencyMs = undefined;
      position.lockedQty = 0;
      position.currentValue = redeemAmount;
      position.settlementResult = isWinner ? "win" : "loss";
      await this.store.persistUser(user);
      await this.store.persistPosition(position);
      await this.writeAuditLog({
        eventId: this.store.newId("evt"),
        traceId: this.store.newTraceId(),
        category: "settlement",
        actionType: "redeem_position",
        actionStatus: "success",
        userId: user.id,
        role: user.role,
        pageName: "profile.main",
        moduleName: "position.table",
        symbol: round.symbol,
        roundId: round.id,
        serverRecvTs: closedAt,
        serverPublishTs: closedAt,
        backendLatencyMs: 0,
        resultCode: "POSITION_SETTLED",
        resultMessage: `Position ${position.id} settled as ${position.settlementResult}.`,
        details: {
          roundId: round.id,
          marketId: round.marketId,
          marketSlug: round.marketSlug,
          positionId: position.id,
          side: position.side,
          settlementResult: position.settlementResult,
          redeemAmount,
          realizedPnl: position.realizedPnl
        }
      });
      await this.writeBehaviorLog(
        this.createBehaviorLog({
          user,
          actionType: "redeem_position",
          actionStatus: "success",
          traceId: this.store.newTraceId(),
          round,
          snapshot,
          direction: position.side,
          entryOdds: position.averageEntry,
          positionNotional: position.notionalSpent,
          exitType: "settlement",
          exitOdds: position.currentMark,
          settlementResult: position.settlementResult,
          settlementDirection: round.settledSide,
          settlementTimeMs: round.settlementTs ?? closedAt,
          gammaPollCount: round.pollCount,
          redeemFinishTimeMs: closedAt,
          contextJson: {
            positionId: position.id,
            roundId: round.id,
            marketId: round.marketId,
            marketSlug: round.marketSlug,
            redeemAmount
          }
        })
      );
      userIds.add(user.id);
    }

    round.redeemFinishTs = closedAt;
    round.redeemScheduledAt = round.redeemScheduledAt ?? closedAt;
    round.status = "Closed";
    await this.writeAuditLog({
      eventId: this.store.newId("evt"),
      traceId: this.store.newTraceId(),
      category: "settlement",
      actionType: "round_closed",
      actionStatus: "success",
      pageName: "trade.main",
      moduleName: "settlement.engine",
      symbol: round.symbol,
      roundId: round.id,
      serverRecvTs: closedAt,
      serverPublishTs: closedAt,
      backendLatencyMs: 0,
      resultCode: "ROUND_CLOSED",
      resultMessage: "Round was closed after redeem processing completed.",
      details: {
        roundId: round.id,
        marketId: round.marketId,
        marketSlug: round.marketSlug,
        settledSide: round.settledSide,
        settlementPrice: round.settlementPrice,
        redeemFinishTs: round.redeemFinishTs,
        redeemedPositionCount: positions.length
      }
    });
    for (const userId of userIds) {
      this.store.emitUserPayload(userId);
    }
  }

  private upsertBuyPosition(
    userId: string,
    roundId: string,
    side: TradeSide,
    filledQty: number,
    spent: number,
    mark: number
  ) {
    let position = this.store.positions.find(
      (item) => item.userId === userId && item.roundId === roundId && item.side === side && item.status === "open"
    );
    if (!position) {
      position = {
        id: this.store.newId("pos"),
        userId,
        roundId,
        side,
        qty: 0,
        lockedQty: 0,
        averageEntry: 0,
        notionalSpent: 0,
        currentMark: mark,
        unrealizedPnl: 0,
        realizedPnl: 0,
        status: "open",
        openedAt: Date.now()
      };
    }

    const totalCost = position.notionalSpent + spent;
    const totalQty = position.qty + filledQty;
    position.averageEntry = roundNumber(totalCost / Math.max(totalQty, QTY_EPSILON), 4);
    position.qty = roundNumber(totalQty, 4);
    position.notionalSpent = roundNumber(totalCost, 4);
    position.currentMark = roundNumber(mark, 4);
    position.currentValue = roundNumber(position.qty * position.currentMark, 2);
    position.unrealizedPnl = roundNumber(position.qty * position.currentMark - position.notionalSpent, 2);
    return position;
  }

  private captureReferencePrice() {
    if (this.config.chainlinkEnabled && this.chainlinkState.price > 0) {
      return {
        price: roundNumber(this.chainlinkState.price, 2),
        source: "Chainlink" as const
      };
    }
    if (this.binanceState.price > 0) {
      return {
        price: roundNumber(this.binanceState.price, 2),
        source: "Binance" as const
      };
    }
    return {
      price: 0,
      source: "Unavailable" as const
    };
  }

  private resolveSettledSide(detail: PolymarketMarketDetail) {
    if (detail.winningTokenId) {
      if (detail.winningTokenId === detail.upTokenId) {
        return "UP";
      }
      if (detail.winningTokenId === detail.downTokenId) {
        return "DOWN";
      }
    }
    if (detail.winningOutcome) {
      const normalized = detail.winningOutcome.toLowerCase();
      if (normalized === detail.upOutcome.toLowerCase() || normalized.includes("up") || normalized.includes("above")) {
        return "UP";
      }
      if (
        normalized === detail.downOutcome.toLowerCase() ||
        normalized.includes("down") ||
        normalized.includes("below")
      ) {
        return "DOWN";
      }
    }
    if (!detail.closed) {
      return undefined;
    }
    const [upPrice, downPrice] = detail.outcomePrices;
    if (upPrice >= 0.99 && downPrice <= 0.01) {
      return "UP";
    }
    if (downPrice >= 0.99 && upPrice <= 0.01) {
      return "DOWN";
    }
    if (Math.abs(upPrice - downPrice) < 0.01) {
      return undefined;
    }
    if (upPrice === 0 && downPrice === 0) {
      return undefined;
    }
    return upPrice >= downPrice ? "UP" : "DOWN";
  }

  private applyMarketMetadata(round: RoundRecord, detail: PolymarketMarketDetail) {
    round.marketId = detail.id;
    round.eventId = detail.eventId;
    round.marketSlug = detail.slug;
    round.eventSlug = detail.eventSlug;
    round.conditionId = detail.conditionId;
    round.seriesSlug = detail.seriesSlug;
    round.upTokenId = detail.upTokenId;
    round.downTokenId = detail.downTokenId;
    round.title = detail.title;
    round.resolutionSource = detail.resolutionSource;
    round.acceptingOrders = detail.acceptingOrders;
    round.polymarketSettlementPrice = detail.settlementPrice ?? round.polymarketSettlementPrice;
    round.polymarketSettlementStatus = detail.settlementStatus ?? round.polymarketSettlementStatus;
    round.settlementReceivedAt = detail.settlementReceivedAt ?? round.settlementReceivedAt;
    if (!isBtcReferencePrice(round.polymarketOpenPrice)) {
      round.polymarketOpenPrice = undefined;
      round.polymarketOpenPriceSource = undefined;
    }
    if (!isBtcReferencePrice(round.polymarketClosePrice)) {
      round.polymarketClosePrice = undefined;
      round.polymarketClosePriceSource = undefined;
    }
  }

  private async syncCurrentRoundMarket() {
    const currentRound = this.store.getCurrentRound(Date.now());
    if (!currentRound?.marketSlug) {
      return;
    }
    if (this.roundMatchesMarket(currentRound, this.polymarketState.currentMarket)) {
      return;
    }
    if (this.marketSyncSlug === currentRound.marketSlug) {
      return;
    }

    this.marketSyncSlug = currentRound.marketSlug;
    try {
      await this.polymarketConnector.focusMarket(currentRound);
      this.polymarketState = this.polymarketConnector.getState();
    } catch {
      // Keep the previous market snapshot until the next reconcile retries the switch.
    } finally {
      if (this.marketSyncSlug === currentRound.marketSlug) {
        this.marketSyncSlug = undefined;
      }
    }
  }

  private roundMatchesMarket(round?: Pick<RoundRecord, "marketId" | "marketSlug" | "conditionId">, market?: Pick<PolymarketMarketDetail, "id" | "slug" | "conditionId">) {
    if (!round || !market) {
      return false;
    }
    return Boolean(
      (round.marketSlug && market.slug === round.marketSlug) ||
        (round.marketId && market.id === round.marketId) ||
        (round.conditionId && market.conditionId === round.conditionId)
    );
  }

  private createEmptyOrderBook(side: TradeSide): OrderBookSnapshot {
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

  private async hydrateRoundPolymarketReferencePrices(round: RoundRecord, now: number) {
    if (!isBtcReferencePrice(round.polymarketOpenPrice)) {
      round.polymarketOpenPrice = undefined;
      round.polymarketOpenPriceSource = undefined;
    }
    if (!isBtcReferencePrice(round.polymarketClosePrice)) {
      round.polymarketClosePrice = undefined;
      round.polymarketClosePriceSource = undefined;
    }
    const resolutionSource = round.resolutionSource ?? this.polymarketState.currentMarket?.resolutionSource;
    if (!resolutionSource) {
      return;
    }
    round.resolutionSource = resolutionSource;
    if (!round.polymarketOpenPrice && round.startAt <= now) {
      try {
        const openReference = await this.polymarketReferenceResolver.resolveBoundaryPrice(round.startAt, {
          resolutionSource
        });
        if (openReference) {
          round.polymarketOpenPrice = roundNumber(openReference.price, 2);
          round.polymarketOpenPriceSource = openReference.source;
        }
      } catch {
        // Keep the previous round state; the next reconcile will retry.
      }
    }
    if (!round.polymarketClosePrice && now >= round.endAt) {
      try {
        const closeReference = await this.polymarketReferenceResolver.resolveBoundaryPrice(round.endAt, {
          resolutionSource
        });
        if (closeReference) {
          round.polymarketClosePrice = roundNumber(closeReference.price, 2);
          round.polymarketClosePriceSource = closeReference.source;
        }
      } catch {
        // Keep the previous round state; the next reconcile will retry.
      }
    }
  }

  private collectRoundPositionUsers(roundId: string) {
    return new Set(
      this.store.positions
        .filter((position) => position.roundId === roundId)
        .map((position) => position.userId)
    );
  }

  private roundSignature(round: RoundRecord) {
    return JSON.stringify([
      round.id,
      round.marketId,
      round.symbol,
      round.eventId,
      round.marketSlug,
      round.eventSlug,
      round.conditionId,
      round.seriesSlug,
      round.upTokenId,
      round.downTokenId,
      round.title,
      round.resolutionSource,
      round.startAt,
      round.endAt,
      round.priceToBeat,
      round.status,
      round.pollCount,
      round.pollStartAt,
      round.lastPollAt,
      round.closingSpotPrice,
      round.settledSide,
      round.settlementPrice,
      round.settlementTs,
      round.settlementSource,
      round.polymarketSettlementPrice,
      round.polymarketSettlementStatus,
      round.polymarketOpenPrice,
      round.polymarketClosePrice,
      round.polymarketOpenPriceSource,
      round.polymarketClosePriceSource,
      round.settlementReceivedAt,
      round.redeemScheduledAt,
      round.binanceOpenPrice,
      round.binanceClosePrice,
      round.redeemStartTs,
      round.redeemFinishTs,
      round.manualReason,
      round.acceptingOrders,
      round.closingPriceSource
    ]);
  }

  private roundChanged(left: RoundRecord | undefined, right: RoundRecord) {
    return !left || this.roundSignature(left) !== this.roundSignature(right);
  }

  private async emitLatencyLogs(snapshot: MarketSnapshot) {
    const now = Date.now();
    for (const source of Object.values(snapshot.sources)) {
      const lastLoggedAt = this.lastLatencyLogAt.get(source.source) ?? 0;
      const lastState = this.lastLatencyState.get(source.source);
      if (now - lastLoggedAt < LATENCY_LOG_INTERVAL_MS && lastState === source.state) {
        continue;
      }
      this.lastLatencyLogAt.set(source.source, now);
      this.lastLatencyState.set(source.source, source.state);
      await this.writeAuditLog({
        eventId: this.store.newId("evt"),
        traceId: this.store.newTraceId(),
        category: "latency",
        actionType: "market_latency",
        actionStatus: "success",
        pageName: "trade.main",
        moduleName: source.source.toLowerCase(),
        symbol: snapshot.symbol,
        serverRecvTs: source.serverRecvTs,
        serverPublishTs: source.serverPublishTs,
        backendLatencyMs: source.acquireLatencyMs + source.publishLatencyMs,
        resultCode: "OK",
        resultMessage: `${source.source} ${source.state}`,
        details: {
          acquireLatencyMs: source.acquireLatencyMs,
          publishLatencyMs: source.publishLatencyMs,
          frontendLatencyMs: source.frontendLatencyMs,
          connectionState: source.state,
          reconnectCount: source.reconnectCount,
          message: source.message
        }
      });
    }
  }

  private async writeAuditLog(event: AuditEvent) {
    await this.store.recordLog(event);
  }

  private async writeSettlementLog(
    round: RoundRecord,
    status: "success" | "failed" | "timeout",
    message: string
  ) {
    const now = Date.now();
    await this.writeAuditLog({
      eventId: this.store.newId("evt"),
      traceId: this.store.newTraceId(),
      category: "settlement",
      actionType: status === "success" ? "settlement_confirmed" : "poll_settlement",
      actionStatus: status === "success" ? "success" : status === "timeout" ? "timeout" : "failed",
      pageName: "trade.main",
      moduleName: "settlement.engine",
      symbol: round.symbol,
      roundId: round.id,
      serverRecvTs: now,
      serverPublishTs: now,
      backendLatencyMs: 0,
      resultCode: status === "success" ? "SETTLED" : "MANUAL",
      resultMessage: message,
      details: {
        roundId: round.id,
        marketId: round.marketId,
        marketSlug: round.marketSlug,
        pollCount: round.pollCount,
        settlementPrice: round.settlementPrice,
        settledSide: round.settledSide,
        manualReason: round.manualReason
      }
    });
  }
}

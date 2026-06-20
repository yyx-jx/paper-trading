import type { ConnectionState, SourceComponentHealth, SourceHealth } from "../../domain/types";

export const POLYMARKET_HEALTH_COMPONENTS = ["discovery", "orderBook", "marketWs", "trades"] as const;

export type PolymarketHealthComponent = (typeof POLYMARKET_HEALTH_COMPONENTS)[number];
export type PolymarketHealthComponents = Partial<Record<PolymarketHealthComponent, SourceComponentHealth>>;

export const POLYMARKET_COMPONENT_LABELS: Record<PolymarketHealthComponent, string> = {
  discovery: "Market discovery",
  orderBook: "Order book",
  marketWs: "Market WebSocket",
  trades: "Recent trades"
};

export function createPolymarketComponent(input: {
  name: PolymarketHealthComponent;
  state: ConnectionState;
  sourceEventTs: number;
  serverRecvTs: number;
  message?: string;
}): SourceComponentHealth {
  return {
    name: input.name,
    label: POLYMARKET_COMPONENT_LABELS[input.name],
    state: input.state,
    sourceEventTs: input.sourceEventTs,
    serverRecvTs: input.serverRecvTs,
    message: input.message
  };
}

function latestComponent(components: SourceComponentHealth[]) {
  return components
    .filter((component) => component.sourceEventTs > 0)
    .sort((left, right) => right.sourceEventTs - left.sourceEventTs)[0];
}

function componentSummary(component: SourceComponentHealth | undefined, label: string) {
  if (!component) {
    return `${label} unavailable`;
  }
  if (component.state === "healthy") {
    return `${label} healthy`;
  }
  return `${label} ${component.state}${component.message ? `: ${component.message}` : ""}`;
}

export function derivePolymarketSourceHealth(input: {
  symbol: string;
  reconnectCount: number;
  now: number;
  orderBookFreshMs: number;
  components: PolymarketHealthComponents;
}): SourceHealth {
  const allComponents = POLYMARKET_HEALTH_COMPONENTS
    .map((name) => input.components[name])
    .filter((component): component is SourceComponentHealth => Boolean(component));
  const discovery = input.components.discovery;
  const orderBook = input.components.orderBook;
  const latest = latestComponent(allComponents);
  const orderBookAgeMs = orderBook ? Math.max(input.now - orderBook.sourceEventTs, 0) : Number.POSITIVE_INFINITY;
  const orderBookFresh = orderBook?.state === "healthy" && orderBookAgeMs <= input.orderBookFreshMs;
  const orderBookKnownButUnusable =
    orderBook?.state === "degraded" ||
    orderBook?.state === "stale" ||
    (orderBook?.state === "healthy" && !orderBookFresh);
  const state: ConnectionState = orderBookFresh
    ? "healthy"
    : orderBookKnownButUnusable
      ? "degraded"
      : discovery?.state === "degraded"
        ? "degraded"
        : "reconnecting";
  const anchor = orderBookFresh ? orderBook : latest;
  const sourceEventTs = anchor?.sourceEventTs ?? input.now;
  const serverRecvTs = anchor?.serverRecvTs ?? input.now;
  const primaryMessage = orderBookFresh
    ? "Order book healthy"
    : orderBookKnownButUnusable
      ? `Order book stale for ${Math.round(orderBookAgeMs)}ms`
      : "Order book unavailable";
  const componentMessages = [
    componentSummary(input.components.marketWs, "Market WebSocket"),
    componentSummary(input.components.trades, "Recent trades")
  ].filter((message) => !message.endsWith(" healthy"));

  return {
    source: "CLOB",
    symbol: input.symbol,
    state,
    reconnectCount: input.reconnectCount,
    sourceEventTs,
    serverRecvTs,
    normalizedTs: serverRecvTs,
    serverPublishTs: serverRecvTs,
    acquireLatencyMs: Math.max(serverRecvTs - sourceEventTs, 0),
    publishLatencyMs: 0,
    frontendLatencyMs: 0,
    message: [primaryMessage, ...componentMessages].join("; "),
    components: Object.fromEntries(
      POLYMARKET_HEALTH_COMPONENTS.flatMap((name) => {
        const component = input.components[name];
        return component ? [[name, component]] : [];
      })
    )
  };
}

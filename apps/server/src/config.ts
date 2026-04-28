import "dotenv/config";

export type PersistenceMode = "external" | "memory";

export const DEFAULT_CHAINLINK_RPC_URL = "https://eth.llamarpc.com";
export const DEFAULT_CHAINLINK_FALLBACK_RPC_URLS = [
  "https://ethereum-rpc.publicnode.com",
  "https://mainnet.infura.io/v3/b6bf7d3508c941499b10025c0776eaf8"
];

function textEnv(value: string | undefined, fallback: string) {
  const trimmed = value?.trim();
  return trimmed ? trimmed : fallback;
}

function optionalTextEnv(value: string | undefined) {
  const trimmed = value?.trim();
  return trimmed || undefined;
}

function csvEnv(value: string | undefined, fallback: string[] = []) {
  const parsed = (value ?? "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  return parsed.length > 0 ? parsed : fallback;
}

function persistenceModeEnv(value: string | undefined): PersistenceMode {
  return value === "memory" ? "memory" : "external";
}

export function buildServerConfig(env: NodeJS.ProcessEnv = process.env) {
  return {
    port: Number(env.PORT ?? 8787),
    matchingServicePort: Number(env.MATCHING_SERVICE_PORT ?? 8788),
    matchingServiceUrl: textEnv(env.MATCHING_SERVICE_URL, "http://127.0.0.1:8788"),
    matchingServiceTimeoutMs: Number(env.MATCHING_SERVICE_TIMEOUT_MS ?? 4000),
    embeddedMatchingService: env.EMBEDDED_MATCHING_SERVICE !== "false",
    chainlinkEnabled: env.CHAINLINK_ENABLED !== "false",
    upstreamProxyUrl: optionalTextEnv(env.UPSTREAM_PROXY_URL),
    jwtSecret: env.JWT_SECRET ?? "btc-paper-trading-secret",
    symbol: textEnv(env.SYMBOL, "BTC"),
    marketId: textEnv(env.MARKET_ID, "btc-5m-live"),
    initialBalance: Number(env.INITIAL_BALANCE ?? 10000),
    freezeWindowMs: Number(env.FREEZE_WINDOW_MS ?? 10000),
    pollDelayMs: Number(env.POLL_DELAY_MS ?? 120000),
    gammaPollIntervalMs: Number(env.GAMMA_POLL_INTERVAL_MS ?? 5000),
    gammaMaxPolls: Number(env.GAMMA_MAX_POLLS ?? 60),
    logRetentionMs: Number(env.LOG_RETENTION_MS ?? 300000),
    snapshotRetentionSeconds: Number(env.REDIS_SNAPSHOT_TTL_SECONDS ?? 300),
    persistenceMode: persistenceModeEnv(env.PERSISTENCE_MODE),
    databaseUrl: textEnv(env.DATABASE_URL, "postgresql://postgres:postgres@127.0.0.1:5432/paper_trading"),
    redisUrl: textEnv(env.REDIS_URL, "redis://127.0.0.1:6379"),
    binanceRestUrl: textEnv(env.BINANCE_REST_URL, "https://api.binance.com"),
    binanceRequestTimeoutMs: Number(env.BINANCE_REQUEST_TIMEOUT_MS ?? 8000),
    binanceWsUrl: textEnv(env.BINANCE_WS_URL, "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@kline_1m"),
    binanceRestPollMs: Number(env.BINANCE_REST_POLL_MS ?? 3000),
    binanceWsStaleMs: Number(env.BINANCE_WS_STALE_MS ?? 15000),
    chainlinkRpcUrl: textEnv(env.CHAINLINK_RPC_URL, DEFAULT_CHAINLINK_RPC_URL),
    chainlinkFallbackRpcUrls: csvEnv(env.CHAINLINK_FALLBACK_RPC_URLS, DEFAULT_CHAINLINK_FALLBACK_RPC_URLS),
    chainlinkRequestTimeoutMs: Number(env.CHAINLINK_REQUEST_TIMEOUT_MS ?? 8000),
    chainlinkBtcUsdProxyAddress: textEnv(env.CHAINLINK_BTC_USD_PROXY_ADDRESS, "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c"),
    chainlinkPollMs: Number(env.CHAINLINK_POLL_MS ?? 1500),
    gammaBaseUrl: textEnv(env.POLYMARKET_GAMMA_BASE_URL, "https://gamma-api.polymarket.com"),
    clobBaseUrl: textEnv(env.POLYMARKET_CLOB_BASE_URL, "https://clob.polymarket.com"),
    dataApiBaseUrl: textEnv(env.POLYMARKET_DATA_BASE_URL, "https://data-api.polymarket.com"),
    polymarketMarketId: optionalTextEnv(env.POLYMARKET_MARKET_ID),
    polymarketMarketSlug: optionalTextEnv(env.POLYMARKET_MARKET_SLUG),
    polymarketSearchQuery: textEnv(env.POLYMARKET_SEARCH_QUERY, "Bitcoin Up or Down"),
    polymarketSeriesSlug: textEnv(env.POLYMARKET_SERIES_SLUG, "btc-up-or-down-5m"),
    polymarketDiscoveryTimeoutMs: Number(env.POLYMARKET_DISCOVERY_TIMEOUT_MS ?? 10000),
    polymarketDiscoveryKeywords: csvEnv(env.POLYMARKET_DISCOVERY_KEYWORDS ?? "bitcoin,btc,5m,5-minute,up,down"),
    marketDiscoveryIntervalMs: Number(env.MARKET_DISCOVERY_INTERVAL_MS ?? 30000),
    polymarketBookPollMs: Number(env.POLYMARKET_BOOK_POLL_MS ?? 1000),
    polymarketTradesPollMs: Number(env.POLYMARKET_TRADES_POLL_MS ?? 2000)
  };
}

export const serverConfig = buildServerConfig();

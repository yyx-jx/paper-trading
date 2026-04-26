import "dotenv/config";

export const serverConfig = {
  port: Number(process.env.PORT ?? 8787),
  matchingServicePort: Number(process.env.MATCHING_SERVICE_PORT ?? 8788),
  matchingServiceUrl: process.env.MATCHING_SERVICE_URL ?? "http://127.0.0.1:8788",
  matchingServiceTimeoutMs: Number(process.env.MATCHING_SERVICE_TIMEOUT_MS ?? 4000),
  embeddedMatchingService: process.env.EMBEDDED_MATCHING_SERVICE !== "false",
  chainlinkEnabled: process.env.CHAINLINK_ENABLED !== "false",
  upstreamProxyUrl: process.env.UPSTREAM_PROXY_URL?.trim() || undefined,
  jwtSecret: process.env.JWT_SECRET ?? "btc-paper-trading-secret",
  symbol: process.env.SYMBOL ?? "BTC",
  marketId: process.env.MARKET_ID ?? "btc-5m-live",
  initialBalance: Number(process.env.INITIAL_BALANCE ?? 10000),
  freezeWindowMs: Number(process.env.FREEZE_WINDOW_MS ?? 10000),
  pollDelayMs: Number(process.env.POLL_DELAY_MS ?? 0),
  gammaPollIntervalMs: Number(process.env.GAMMA_POLL_INTERVAL_MS ?? 5000),
  gammaMaxPolls: Number(process.env.GAMMA_MAX_POLLS ?? 60),
  logRetentionMs: Number(process.env.LOG_RETENTION_MS ?? 300000),
  snapshotRetentionSeconds: Number(process.env.REDIS_SNAPSHOT_TTL_SECONDS ?? 300),
  databaseUrl:
    process.env.DATABASE_URL ?? "postgresql://postgres:postgres@127.0.0.1:5432/paper_trading",
  redisUrl: process.env.REDIS_URL ?? "redis://127.0.0.1:6379",
  binanceRestUrl: process.env.BINANCE_REST_URL ?? "https://api.binance.com",
  binanceRequestTimeoutMs: Number(process.env.BINANCE_REQUEST_TIMEOUT_MS ?? 8000),
  binanceWsUrl:
    process.env.BINANCE_WS_URL ??
    "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@kline_1m",
  binanceRestPollMs: Number(process.env.BINANCE_REST_POLL_MS ?? 3000),
  binanceWsStaleMs: Number(process.env.BINANCE_WS_STALE_MS ?? 15000),
  chainlinkRpcUrl:
    process.env.CHAINLINK_RPC_URL ??
    "https://mainnet.infura.io/v3/b6bf7d3508c941499b10025c0776eaf8",
  chainlinkFallbackRpcUrls: (process.env.CHAINLINK_FALLBACK_RPC_URLS ?? "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean),
  chainlinkRequestTimeoutMs: Number(process.env.CHAINLINK_REQUEST_TIMEOUT_MS ?? 8000),
  chainlinkBtcUsdProxyAddress:
    process.env.CHAINLINK_BTC_USD_PROXY_ADDRESS ?? "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c",
  chainlinkPollMs: Number(process.env.CHAINLINK_POLL_MS ?? 1500),
  gammaBaseUrl: process.env.POLYMARKET_GAMMA_BASE_URL ?? "https://gamma-api.polymarket.com",
  clobBaseUrl: process.env.POLYMARKET_CLOB_BASE_URL ?? "https://clob.polymarket.com",
  dataApiBaseUrl: process.env.POLYMARKET_DATA_BASE_URL ?? "https://data-api.polymarket.com",
  polymarketMarketId: process.env.POLYMARKET_MARKET_ID,
  polymarketMarketSlug: process.env.POLYMARKET_MARKET_SLUG,
  polymarketSearchQuery: process.env.POLYMARKET_SEARCH_QUERY ?? "Bitcoin Up or Down",
  polymarketSeriesSlug: process.env.POLYMARKET_SERIES_SLUG ?? "btc-up-or-down-5m",
  polymarketDiscoveryTimeoutMs: Number(process.env.POLYMARKET_DISCOVERY_TIMEOUT_MS ?? 10000),
  polymarketDiscoveryKeywords: (process.env.POLYMARKET_DISCOVERY_KEYWORDS ?? "bitcoin,btc,5m,5-minute,up,down")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean),
  marketDiscoveryIntervalMs: Number(process.env.MARKET_DISCOVERY_INTERVAL_MS ?? 30000),
  polymarketBookPollMs: Number(process.env.POLYMARKET_BOOK_POLL_MS ?? 1000),
  polymarketTradesPollMs: Number(process.env.POLYMARKET_TRADES_POLL_MS ?? 2000)
};

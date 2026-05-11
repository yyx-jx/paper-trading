import "dotenv/config";

export type PersistenceMode = "external" | "memory";

export const DEFAULT_CHAINLINK_RPC_URL = "https://eth.llamarpc.com";
export const DEFAULT_CHAINLINK_FALLBACK_RPC_URLS = [
  "https://ethereum-rpc.publicnode.com",
  "https://mainnet.infura.io/v3/b6bf7d3508c941499b10025c0776eaf8"
];
export const DEFAULT_CHAINLINK_RTDS_WS_URL = "wss://ws-live-data.polymarket.com";
export const DEFAULT_CHAINLINK_HISTORY_URL = "https://data.chain.link/api/historical-data-engine-stream-data";
export const DEFAULT_CHAINLINK_BTC_USD_STREAM_FEED_ID =
  "0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8";

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
  const jwtSecret = env.JWT_SECRET ?? "btc-paper-trading-secret";
  const isProduction = env.NODE_ENV === "production" || env.DEPLOY_ENV === "production";
  if (isProduction && jwtSecret === "btc-paper-trading-secret") {
    throw new Error("JWT_SECRET must be set to a non-default value in production.");
  }
  const corsOrigins = csvEnv(env.CORS_ORIGINS);
  if (isProduction && corsOrigins.length === 0) {
    throw new Error("CORS_ORIGINS must be set in production.");
  }
  const exportAnonymizationSecret = textEnv(env.EXPORT_ANONYMIZATION_SECRET, jwtSecret);
  if (isProduction && exportAnonymizationSecret === jwtSecret) {
    throw new Error("EXPORT_ANONYMIZATION_SECRET must be set to a dedicated value in production.");
  }
  return {
    isProduction,
    port: Number(env.PORT ?? 8787),
    matchingServicePort: Number(env.MATCHING_SERVICE_PORT ?? 8788),
    matchingServiceUrl: textEnv(env.MATCHING_SERVICE_URL, "http://127.0.0.1:8788"),
    matchingServiceTimeoutMs: Number(env.MATCHING_SERVICE_TIMEOUT_MS ?? 4000),
    embeddedMatchingService: env.EMBEDDED_MATCHING_SERVICE !== "false",
    chainlinkEnabled: env.CHAINLINK_ENABLED !== "false",
    upstreamProxyUrl: optionalTextEnv(env.UPSTREAM_PROXY_URL),
    publicDomain: optionalTextEnv(env.PUBLIC_DOMAIN),
    corsOrigins,
    trustProxy: env.TRUST_PROXY === "true",
    requestTimeoutMs: Number(env.REQUEST_TIMEOUT_MS ?? 30000),
    metricsEnabled: env.METRICS_ENABLED !== "false",
    metricsBasicAuthUser: optionalTextEnv(env.METRICS_BASIC_AUTH_USER),
    metricsBasicAuthPassword: optionalTextEnv(env.METRICS_BASIC_AUTH_PASSWORD),
    exportAnonymizationSecret,
    jwtSecret,
    symbol: textEnv(env.SYMBOL, "BTC"),
    marketId: textEnv(env.MARKET_ID, "btc-5m-live"),
    initialBalance: Number(env.INITIAL_BALANCE ?? 10000),
    freezeWindowMs: Number(env.FREEZE_WINDOW_MS ?? 10000),
    pollDelayMs: Number(env.POLL_DELAY_MS ?? 0),
    marketWsMinIntervalMs: Number(env.MARKET_WS_MIN_INTERVAL_MS ?? 50),
    marketSnapshotIntervalMs: Number(env.MARKET_SNAPSHOT_INTERVAL_MS ?? 500),
    marketHistoryCacheMaxUsers: Number(env.MARKET_HISTORY_CACHE_MAX_USERS ?? 200),
    strictPersistence: env.SERVER_STRICT_PERSISTENCE !== "false",
    requireSchemaMigrations: isProduction ? env.SERVER_REQUIRE_MIGRATIONS !== "false" : env.SERVER_REQUIRE_MIGRATIONS === "true",
    allowDevSchemaBootstrap: !isProduction && env.SERVER_ALLOW_DEV_SCHEMA_BOOTSTRAP !== "false",
    expectedSchemaMigrationId: textEnv(env.EXPECTED_SCHEMA_MIGRATION_ID, "000004"),
    loginRateLimitWindowMs: Number(env.LOGIN_RATE_LIMIT_WINDOW_MS ?? 60_000),
    loginRateLimitMax: Number(env.LOGIN_RATE_LIMIT_MAX ?? 30),
    writeRateLimitWindowMs: Number(env.WRITE_RATE_LIMIT_WINDOW_MS ?? 60_000),
    orderRateLimitMax: Number(env.ORDER_RATE_LIMIT_MAX ?? 120),
    exportRateLimitMax: Number(env.EXPORT_RATE_LIMIT_MAX ?? 20),
    bulkImportRateLimitMax: Number(env.BULK_IMPORT_RATE_LIMIT_MAX ?? 20),
    pgConnectionTimeoutMs: Number(env.PG_CONNECTION_TIMEOUT_MS ?? 8000),
    pgIdleTimeoutMs: Number(env.PG_IDLE_TIMEOUT_MS ?? 30000),
    pgMaxConnections: Number(env.PG_MAX_CONNECTIONS ?? 10),
    pgKeepAlive: env.PG_KEEPALIVE !== "false",
    pgReconnectIntervalMs: Number(env.PG_RECONNECT_INTERVAL_MS ?? 2000),
    pgReconnectMaxIntervalMs: Number(env.PG_RECONNECT_MAX_INTERVAL_MS ?? 15000),
    matchingEventsMemoryMax: Number(env.MATCHING_EVENTS_MEMORY_MAX ?? 5000),
    matchingEventsMemoryMaxAgeMs: Number(env.MATCHING_EVENTS_MEMORY_MAX_AGE_MS ?? 30 * 60_000),
    matchingBooksMemoryMax: Number(env.MATCHING_BOOKS_MEMORY_MAX ?? 200),
    orderBookSnapshotsMemoryMax: Number(env.ORDER_BOOK_SNAPSHOTS_MEMORY_MAX ?? 4000),
    orderBookSnapshotsMemoryMaxAgeMs: Number(env.ORDER_BOOK_SNAPSHOTS_MEMORY_MAX_AGE_MS ?? 2 * 60 * 60_000),
    ordersMemoryMax: Number(env.ORDERS_MEMORY_MAX ?? 4000),
    positionsMemoryMax: Number(env.POSITIONS_MEMORY_MAX ?? 2000),
    auditLogsMemoryMax: Number(env.AUDIT_LOGS_MEMORY_MAX ?? 3000),
    behaviorLogsMemoryMax: Number(env.BEHAVIOR_LOGS_MEMORY_MAX ?? 4000),
    orderLifecycleMemoryMax: Number(env.ORDER_LIFECYCLE_MEMORY_MAX ?? 5000),
    roundsMemoryMax: Number(env.ROUNDS_MEMORY_MAX ?? 400),
    serverHeapWarnMb: Number(env.SERVER_HEAP_WARN_MB ?? 768),
    serverHeapProtectMb: Number(env.SERVER_HEAP_PROTECT_MB ?? 1024),
    gammaPollIntervalMs: Number(env.GAMMA_POLL_INTERVAL_MS ?? 1000),
    logRetentionMs: Number(env.LOG_RETENTION_MS ?? 300000),
    snapshotRetentionSeconds: Number(env.REDIS_SNAPSHOT_TTL_SECONDS ?? 300),
    persistenceMode: persistenceModeEnv(env.PERSISTENCE_MODE),
    seedDefaultUsers: isProduction ? env.SEED_DEFAULT_USERS === "true" : env.SEED_DEFAULT_USERS !== "false",
    databaseUrl: textEnv(env.DATABASE_URL, "postgresql://postgres:postgres@127.0.0.1:5432/paper_trading"),
    redisUrl: textEnv(env.REDIS_URL, "redis://127.0.0.1:6379"),
    binanceRestUrl: textEnv(env.BINANCE_REST_URL, "https://api.binance.com"),
    binanceFallbackRestUrl: textEnv(env.BINANCE_FALLBACK_REST_URL, "https://www.okx.com"),
    binanceFallbackRestPollMs: Number(env.BINANCE_FALLBACK_REST_POLL_MS ?? 1000),
    binanceRequestTimeoutMs: Number(env.BINANCE_REQUEST_TIMEOUT_MS ?? 8000),
    binanceWsUrl: textEnv(
      env.BINANCE_WS_URL,
      "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@kline_1m/btcusdt@kline_5m/btcusdt@kline_15m/btcusdt@kline_1h"
    ),
    binanceRestPollMs: Number(env.BINANCE_REST_POLL_MS ?? 3000),
    binanceWsStaleMs: Number(env.BINANCE_WS_STALE_MS ?? 15000),
    chainlinkRpcUrl: textEnv(env.CHAINLINK_RPC_URL, DEFAULT_CHAINLINK_RPC_URL),
    chainlinkFallbackRpcUrls: csvEnv(env.CHAINLINK_FALLBACK_RPC_URLS, DEFAULT_CHAINLINK_FALLBACK_RPC_URLS),
    chainlinkRequestTimeoutMs: Number(env.CHAINLINK_REQUEST_TIMEOUT_MS ?? 8000),
    chainlinkBtcUsdProxyAddress: textEnv(env.CHAINLINK_BTC_USD_PROXY_ADDRESS, "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c"),
    chainlinkPollMs: Number(env.CHAINLINK_POLL_MS ?? 1500),
    chainlinkRtdsWsUrl: textEnv(env.CHAINLINK_RTDS_WS_URL, DEFAULT_CHAINLINK_RTDS_WS_URL),
    chainlinkRtdsSymbol: textEnv(env.CHAINLINK_RTDS_SYMBOL, "btc/usd"),
    chainlinkRtdsPingMs: Number(env.CHAINLINK_RTDS_PING_MS ?? 5000),
    chainlinkHistoryUrl: textEnv(env.CHAINLINK_HISTORY_URL, DEFAULT_CHAINLINK_HISTORY_URL),
    chainlinkHistoryFeedId: textEnv(env.CHAINLINK_HISTORY_FEED_ID, DEFAULT_CHAINLINK_BTC_USD_STREAM_FEED_ID),
    chainlinkHistoryPollMs: Number(env.CHAINLINK_HISTORY_POLL_MS ?? 10000),
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
    polymarketBookCalibrationMs: Number(env.POLYMARKET_BOOK_CALIBRATION_MS ?? env.POLYMARKET_BOOK_POLL_MS ?? 5000),
    polymarketTradesPollMs: Number(env.POLYMARKET_TRADES_POLL_MS ?? 2000)
  };
}

export const serverConfig = buildServerConfig();

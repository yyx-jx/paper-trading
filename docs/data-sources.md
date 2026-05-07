# 项目数据源字段映射

本文档列出 BTC 5 分钟涨跌纸面交易系统所使用的全部外部数据源、API 端点及具体字段。

---

## 1. Binance（BTC 行情数据）

### 1.1 REST API

**Base URL**: `https://api.binance.com`

#### GET `/api/v3/klines` — K 线数据

| 用途 | 获取 1m / 5m / 15m / 1h / 1d K 线历史，用于前端主图渲染和轮次开/收盘价记录 |
|------|------|
| 参数 | `symbol=BTCUSDT`, `interval={1m\|5m\|15m\|1h\|1d}`, `limit=N` |

**响应数组元素（按索引）**:

| 索引 | 字段名 | 类型 | 本项目使用 | 说明 |
|:----:|--------|------|:----------:|------|
| 0 | `openTime` | number | ✅ | K 线起始时间 (ms) |
| 1 | `open` | string | ✅ | 开盘价 |
| 2 | `high` | string | ✅ | 最高价 |
| 3 | `low` | string | ✅ | 最低价 |
| 4 | `close` | string | ✅ | 收盘价 |
| 5 | `volume` | string | ✅ | 成交量 |
| 6 | `closeTime` | number | ✅ | K 线结束时间 (ms) |
| 7 | `quoteAssetVolume` | string | ❌ | 报价资产成交量 |
| 8 | `numberOfTrades` | number | ❌ | 成交笔数 |
| 9 | `takerBuyBaseVolume` | string | ❌ | 主动买入基础资产量 |
| 10 | `takerBuyQuoteVolume` | string | ❌ | 主动买入报价资产量 |
| 11 | `ignore` | string | ❌ | 忽略字段 |

#### GET `/api/v3/ticker/price` — 当前价格

| 用途 | 获取 BTCUSDT 实时现价 |
|------|------|
| 参数 | `symbol=BTCUSDT` |

| 字段 | 类型 | 本项目使用 | 说明 |
|------|------|:----------:|------|
| `price` | string | ✅ | BTC 当前价格（转 number） |
| `symbol` | string | ❌ | 交易对（隐含于请求参数） |

### 1.2 WebSocket

**URL**: `wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@kline_1m/btcusdt@kline_5m/btcusdt@kline_15m/btcusdt@kline_1h`

#### Stream: `btcusdt@aggTrade` — 聚合成交

| 用途 | 实时价格 tick，驱动 30s/1m 实时 K 线更新 |
|------|------|

外层包裹: `{ data: { e, E, p, q, ... } }`

| 字段 | 路径 | 类型 | 本项目使用 | 说明 |
|------|------|------|:----------:|------|
| `e` | `data.e` | string | ✅ | 事件类型 (`"aggTrade"`) |
| `E` | `data.E` | number | ✅ | 事件时间 (ms)，作为 sourceEventTs |
| `p` | `data.p` | string | ✅ | 成交价格（转 number） |
| `q` | `data.q` | string | ✅ | 成交数量（转 number） |

#### Stream: `btcusdt@kline_{interval}` — K 线更新

| 用途 | 1m / 5m / 15m / 1h 实时 K 线更新 |
|------|------|

| 字段 | 路径 | 类型 | 本项目使用 | 说明 |
|------|------|------|:----------:|------|
| `e` | `data.e` | string | ✅ | 事件类型 (`"kline"`) |
| `E` | `data.E` | number | ✅ | 事件时间 |
| `k.t` | `data.k.t` | number | ✅ | K 线起始时间 |
| `k.T` | `data.k.T` | number | ✅ | K 线结束时间 |
| `k.o` | `data.k.o` | string | ✅ | 开盘价 |
| `k.h` | `data.k.h` | string | ✅ | 最高价 |
| `k.l` | `data.k.l` | string | ✅ | 最低价 |
| `k.c` | `data.k.c` | string | ✅ | 收盘价（也用于更新当前 price） |
| `k.v` | `data.k.v` | string | ✅ | 成交量 |
| `k.i` | `data.k.i` | string | ✅ | K 线周期（`1m`/`5m`/`15m`/`1h`） |

---

## 2. Polymarket Gamma API（市场发现 & 结算）

**Base URL**: `https://gamma-api.polymarket.com`

### 2.1 GET `/markets?slug={slug}` — 按 slug 查询市场

| 用途 | 获取指定轮次市场详情，同时用于历史结算回溯 |
|------|------|
| slug 示例 | `btc-updown-5m-{timestamp_seconds}` |

### 2.2 GET `/markets/{id}` — 按 ID 查询市场

| 用途 | slug 查不到时，直接用 market ID 查历史已关闭市场 |
|------|------|

### 2.3 GET `/markets?id={id}` — 按 ID 查询（列表形式）

| 用途 | `/markets/{id}` 直查失败时的备选路径 |
|------|------|

### 2.4 GET `/public-search` — 公开搜索

| 用途 | 市场发现：按关键词搜索 BTC 5m 市场 |
|------|------|
| 参数 | `q={query}`, `limit_per_type=50`, `optimized=true` |

**响应字段**:

| 字段 | 类型 | 本项目使用 | 说明 |
|------|------|:----------:|------|
| `events[].id` | string | ✅ | 事件 ID |
| `events[].slug` | string | ✅ | 事件 slug，用于市场发现匹配 |
| `events[].title` | string | ✅ | 事件标题，匹配 `"Bitcoin Up or Down"` |
| `events[].endDate` | string | ❌ | 结束日期 |
| `events[].markets[].slug` | string | ✅ | 市场 slug，BTC-5m 格式匹配 |

### 2.5 市场详情响应字段（`DetailedMarketPayload`）

以上 2.1-2.3 端点均返回以下结构，本项目实际使用的字段：

| 字段 | 类型 | 本项目使用 | 存储位置 | 说明 |
|------|------|:----------:|------|------|
| `id` | string | ✅ | `RoundRecord.marketId` | 市场唯一 ID |
| `conditionId` | string | ✅ | `RoundRecord.conditionId` | CLOB 条件 ID |
| `slug` | string | ✅ | `RoundRecord.marketSlug` | 市场 slug |
| `question` | string | ✅ | `RoundRecord.title` | 市场标题 |
| `endDate` | string | ✅ | 推算 `endAt`（与 slug 交叉验证） | 结束时间 |
| `resolutionSource` | string | ✅ | `RoundRecord.resolutionSource` | 结算数据源声明 |
| `outcomes` | string (JSON 数组) | ✅ | 解析出 UP/DOWN outcome 文本 | 结果名称列表 |
| `outcomePrices` | string (JSON 数组) | ✅ | 解析出 UP/DOWN 价格 | 结果价格列表 |
| `clobTokenIds` | string (JSON 数组) | ✅ | `RoundRecord.upTokenId` / `downTokenId` | CLOB token ID 列表 |
| `tokens[].token_id` / `tokenId` / `id` | string | ✅ | 覆盖 clobTokenIds 中的 token ID | Token ID（多种命名兼容） |
| `tokens[].outcome` / `title` | string | ✅ | 覆盖 outcome 文本 | Token 对应的 outcome |
| `tokens[].price` | number/string | ✅ | UP/DOWN 价格覆盖 | Token 价格 |
| `bestBid` | number | ✅ | 盘口最佳买价 | |
| `bestAsk` | number | ✅ | 盘口最佳卖价 | |
| `lastTradePrice` | number | ✅ | 最近成交价（display price 来源之一） | |
| `closed` | boolean | ✅ | 结算状态判断 | 市场是否已关闭 |
| `acceptingOrders` | boolean | ✅ | `RoundRecord.acceptingOrders` | 是否接受下单 |
| `winner` | string | ❌ | 已被 `winningOutcome` 替代 | |
| `winningOutcome` | string | ✅ | 判断结算方向（UP/DOWN） | 胜方 outcome |
| `winningTokenId` | string | ✅ | 判断结算方向（按 token ID 匹配） | 胜方 token ID |
| `resolutionOutcome` | string | ✅ | 备用结算方向判断 | |
| `automaticallyResolved` | boolean | ✅ | 标记自动结算 | |
| `events[].id` | string | ✅ | `RoundRecord.eventId` | 所属事件 ID |
| `events[].slug` | string | ✅ | `RoundRecord.eventSlug` | 所属事件 slug |
| `events[].seriesSlug` | string | ✅ | `RoundRecord.seriesSlug` | 系列 slug |

### 2.6 Gamma 市场详情中提取的 BTC 参考价字段

通过递归扫描 `DetailedMarketPayload` 整个 JSON 对象中所有包含 `price`/`reference`/`target`/`beat`/`resolution`/`settle`/`close`/`open` 路径且值在 1000~1,000,000 之间的数值字段：

| 用途 | 提取字段 | 存储位置 | 说明 |
|------|------|------|------|
| 开盘参考价 | 路径含 `pricetobeat`/`open`/`start`/`initial` | `RoundRecord.polymarketOpenPrice` | 轮次开始时 Polymarket 标注的 BTC 参考价 |
| 收盘参考价 | 路径含 `close`/`final`/`settlement`/`settle`/`resolution` | `RoundRecord.polymarketClosePrice` | 轮次结束时 Polymarket 标注的 BTC 参考价 |
| 通用参考价 | 路径含 `pricetobeat`/`reference`/`target` | `RoundRecord.priceToBeat` | BTC 价格阈值 |

**排除路径**: 含 `outcomePrices`/`bestBid`/`bestAsk`/`lastTrade`/`timestamp`/`time`/`date`/`token`/`id` 的字段不会作为 BTC 价格候选。

---

## 3. Polymarket CLOB API（订单簿 & 市场参数）

**Base URL**: `https://clob.polymarket.com`

### 3.1 GET `/book?token_id={tokenId}` — 订单簿深度

| 用途 | 获取 UP 或 DOWN token 的完整订单簿，用于交易执行、盘口展示 |
|------|------|

| 字段 | 类型 | 本项目使用 | 说明 |
|------|------|:----------:|------|
| `timestamp` | string | ✅ | 订单簿时间戳 (ms)，作为 `snapshotTs` |
| `hash` | string | ✅ | 订单簿哈希，作为 `snapshotId` |
| `bids[].price` | string | ✅ | 买单价格（转 number） |
| `bids[].size` | string | ✅ | 买单数量（转 number，映射为 `qty`） |
| `asks[].price` | string | ✅ | 卖单价格（转 number） |
| `asks[].size` | string | ✅ | 卖单数量（转 number，映射为 `qty`） |

### 3.2 GET `/markets/{conditionId}` 或 `/market?condition_id={conditionId}` — CLOB 市场参数

| 用途 | 获取最小 tick、最小订单量、手续费率等市场参数 |
|------|------|

| 字段 | 类型 | 本项目使用 | 存储位置 | 说明 |
|------|------|:----------:|------|------|
| `condition_id` / `conditionId` | string | ✅ | `ClobMarketInfo.conditionId` | 条件 ID |
| `minimum_tick_size` / `minimumTickSize` / `mts` | number/string | ✅ | `ClobMarketInfo.minimumTickSize` | 最小价格变动单位 |
| `minimum_order_size` / `minimumOrderSize` / `mos` | number/string | ✅ | `ClobMarketInfo.minimumOrderSize` | 最小下单量 |
| `fee_rate_bps` / `feeRateBps` | number/string | ✅ | `ClobMarketInfo.feeRateBps` | 费率（bps） |
| `maker_fee_rate` / `makerFeeRate` | number/string | ✅ | `ClobMarketInfo.makerFeeRate` | Maker 费率（十进制） |
| `taker_fee_rate` / `takerFeeRate` | number/string | ✅ | `ClobMarketInfo.takerFeeRate` | Taker 费率（十进制） |
| `fee_details` / `feeDetails` / `fd` | object | ✅ | `ClobMarketInfo.feeDetails` | 费用明细 |
| `fee_details.r` | number | ✅ | `ClobMarketInfo.platformFeeRate` | 平台费率 |
| `fee_details.e` | number | ✅ | `ClobMarketInfo.platformFeeExponent` | 费率指数 |
| `fee_details.to` | boolean | ✅ | `ClobMarketInfo.platformFeeTakerOnly` | 是否仅 Taker 收费 |
| `platform_fee_rate` / `platformFeeRate` | number/string | ✅ | 平台费率（备选路径） | |
| `rfq_enabled` / `rfqEnabled` / `rfqe` | boolean | ✅ | `ClobMarketInfo.rfqEnabled` | 是否启用 RFQ |
| `tokens[].token_id` / `tokenId` / `id` / `t` | string | ✅ | `ClobMarketInfo.tokens` | Token 级别参数 |
| `tokens[].minimum_tick_size` / `minimumTickSize` | number | ✅ | Token 级最小 tick | |
| `tokens[].minimum_order_size` / `minimumOrderSize` | number | ✅ | Token 级最小订单量 | |

> 以上所有 CLOB 参数不可用时，回退到保守默认值：`minimumTickSize=0.01`, `minimumOrderSize=1`, 费率全为 0。

---

## 4. Polymarket CLOB Market WebSocket（实时盘口 & 结算事件）

**URL**: `wss://ws-subscriptions-clob.polymarket.com/ws/market`

| 用途 | 订阅 UP/DOWN 两个 token 的实时盘口、成交和结算事件 |
|------|------|
| 订阅消息 | `{ assets_ids: [upTokenId, downTokenId], type: "market", custom_feature_enabled: true }` |

### 4.1 事件类型: `book` — 全量订单簿快照

| 字段 | 类型 | 本项目使用 | 说明 |
|------|------|:----------:|------|
| `event_type` | string | ✅ | 固定值 `"book"` |
| `asset_id` | string | ✅ | 资产 ID，匹配 UP/DOWN token |
| `bids[].price` | string | ✅ | 买单价格 |
| `bids[].size` | string | ✅ | 买单数量 |
| `asks[].price` | string | ✅ | 卖单价格 |
| `asks[].size` | string | ✅ | 卖单数量 |
| `timestamp` | number/string | ✅ | 快照时间戳（ms），自动检测秒/毫秒 |
| `hash` | string | ✅ | 快照哈希，作为 `snapshotId` |

### 4.2 事件类型: `best_bid_ask` — 最优买卖价更新

| 字段 | 类型 | 本项目使用 | 说明 |
|------|------|:----------:|------|
| `event_type` | string | ✅ | 固定值 `"best_bid_ask"` |
| `asset_id` / `assetId` / `asset` | string | ✅ | 资产 ID |
| `best_bid` / `bestBid` / `bid` | number | ✅ | 最佳买价 |
| `best_ask` / `bestAsk` / `ask` | number | ✅ | 最佳卖价 |
| `timestamp` / `ts` | number/string | ✅ | 时间戳 |
| `hash` | string | ✅ | 消息哈希 |

### 4.3 事件类型: `price_change` — 盘口增量变更

| 字段 | 类型 | 本项目使用 | 说明 |
|------|------|:----------:|------|
| `event_type` | string | ✅ | 固定值 `"price_change"` |
| `asset_id` | string | ✅ | 资产 ID（顶层或变更内） |
| `price_changes[]` / `priceChanges[]` | array | ✅ | 价格变更列表 |
| `price_changes[].asset_id` / `assetId` | string | ✅ | 变更对应的资产 ID |
| `price_changes[].side` | string | ✅ | `"BUY"` / `"SELL"` → 对应 bids / asks |
| `price_changes[].price` | number | ✅ | 变更价格 |
| `price_changes[].size` / `qty` | number | ✅ | 变更数量（0 表示删除该档位） |
| `price_changes[].best_bid` / `bestBid` | number | ✅ | 变更后的最佳买价 |
| `price_changes[].best_ask` / `bestAsk` | number | ✅ | 变更后的最佳卖价 |
| `price_changes[].timestamp` | number/string | ✅ | 变更时间戳 |
| `price_changes[].hash` | string | ✅ | 变更哈希 |

### 4.4 事件类型: `last_trade_price` — 最新成交

| 字段 | 类型 | 本项目使用 | 说明 |
|------|------|:----------:|------|
| `event_type` | string | ✅ | 固定值 `"last_trade_price"` |
| `asset_id` | string | ✅ | 资产 ID |
| `price` | number | ✅ | 成交价格 |
| `size` | number | ✅ | 成交数量 |
| `timestamp` | number | ✅ | 成交时间 |

### 4.5 事件类型: `market_resolved` — 市场结算

| 字段 | 类型 | 本项目使用 | 说明 |
|------|------|:----------:|------|
| `event_type` | string | ✅ | 固定值 `"market_resolved"` |
| `winning_asset_id` | string | ✅ | 胜方 token ID，用于判断 UP/DOWN |
| `winning_outcome` | string | ✅ | 胜方 outcome 文本（备用判断） |

> 注：CLOB `market_resolved` 仅作结算触发器。最终 `settledSide`、`settlementPrice`、`settlementSource` 仍以 Gamma API 为准。

---

## 5. Polymarket Data API（公开成交记录）

**Base URL**: `https://data-api.polymarket.com`

### 5.1 GET `/trades` — 最近成交

| 用途 | 拉取市场最近成交记录，用于前端盘口区的成交展示和 delta/volume 统计 |
|------|------|

| 字段 | 类型 | 本项目使用 | 说明 |
|------|------|:----------:|------|
| `slug` | string | ✅ | 市场 slug，用于过滤当前市场 |
| `outcome` | string | ✅ | 成交方向（`"UP"` / `"DOWN"`） |
| `price` | number | ✅ | 成交价格 |
| `size` | number | ✅ | 成交数量（映射为 `qty`） |
| `timestamp` | number | ✅ | 成交时间（秒），转毫秒 `*1000` |
| `transactionHash` | string | ✅ | 交易哈希，作为 trade ID |

---

## 6. Chainlink RTDS WebSocket（BTC/USD 实时价格）

**URL**: `wss://ws-live-data.polymarket.com`

| 用途 | Chainlink 实时数据流，获取 BTC/USD 参考价格 |
|------|------|
| 订阅消息 | `{ action: "subscribe", subscriptions: [{ topic: "crypto_prices_chainlink", type: "*", filters: JSON.stringify({ symbol: "btc/usd" }) }] }` |

### 6.1 价格提取逻辑

RTDS WebSocket 返回的 JSON 消息没有固定的 schema，系统采用**递归扫描**方式提取数据：

**价格字段提取规则**:
- 扫描整个 JSON 对象中所有路径包含 `price` / `value` / `answer` / `close` / `mid` 的字段
- 值必须为 number 且在 1,000 ~ 1,000,000 之间（BTC 价格区间）
- **排除**路径含 `timestamp` / `time` / `id` 的字段
- 取第一个匹配的值

**时间戳字段提取规则**:
- 扫描整个 JSON 对象中所有路径包含 `time` / `timestamp` / `ts` 的字段
- 值为 number
- 若 < 10,000,000,000 则视为秒，`*1000` 转毫秒
- 取第一个匹配的值

### 6.2 状态存储

| 提取值 | 存储位置 | 说明 |
|------|------|------|
| 价格 | `ChainlinkConnectorState.price` | BTC/USD 实时价 |
| 时间戳 | `ChainlinkConnectorState.updatedAt` | 价格更新时间 |
| 健康状态 | `ChainlinkConnectorState.status` | >10s 未收到消息 → `degraded`；>15s → `reconnecting` |

---

## 7. 内部存储（PostgreSQL / Redis / JSONL 文件）

### 7.1 PostgreSQL

**连接**: `DATABASE_URL` 环境变量（默认 `postgresql://postgres:postgres@127.0.0.1:5432/paper_trading`）

持久化以下核心实体：用户、轮次、订单、持仓、审计日志、行为日志、订单簿快照。

### 7.2 Redis

**连接**: `REDIS_URL` 环境变量（默认 `redis://127.0.0.1:6379`）

用途：市场快照缓存、WebSocket 事件 Pub/Sub。

### 7.3 JSONL 日志文件（`data/logs/`）

| 文件 | 内容 |
|------|------|
| `audit-events.jsonl` | 审计日志：交易、撤单、结算、redeem、延迟事件 |
| `behavior-action-logs.jsonl` | 训练日志：用户动作、市场上下文、成交/结算结果 |
| `matching-events.jsonl` | 独立 matching 服务事件流（调试/回放用） |
| `matching-snapshots.jsonl` | 独立 matching 服务订单簿快照 |

---

## 数据源汇总

| 序号 | 数据源 | 接入方式 | 用途 |
|:----:|--------|------|------|
| 1 | Binance REST | HTTP GET | K 线历史、当前价格 |
| 2 | Binance WebSocket | WebSocket | BTC 实时价格、K 线实时更新 |
| 3 | Polymarket Gamma API | HTTP GET | 市场发现、市场详情、结算状态 |
| 4 | Polymarket CLOB REST | HTTP GET | 订单簿深度、市场参数（费率/tick） |
| 5 | Polymarket CLOB WebSocket | WebSocket | 实时盘口、最优价、成交、结算事件 |
| 6 | Polymarket Data API | HTTP GET | 市场公开成交记录 |
| 7 | Chainlink RTDS WebSocket | WebSocket | BTC/USD 参考价格 |
| 8 | PostgreSQL | TCP | 用户/轮次/订单/持仓/日志持久化 |
| 9 | Redis | TCP | 快照缓存、WebSocket Pub/Sub |
| 10 | JSONL 文件 | 本地磁盘 | 审计日志、训练日志落盘 |

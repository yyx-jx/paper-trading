# 代码文件说明

这份文档用于快速定位仓库中主要文件的职责。当前项目已经调整为对齐 Polymarket BTC 5 分钟市场：交易执行以 Polymarket CLOB 深度为准，本地只做纸面账户、冻结资金、持仓和延迟结算模拟，不再把用户之间的本地订单簿作为主交易逻辑。

推荐阅读顺序：

1. 先看根目录配置，确认如何启动和测试。
2. 再看 `apps/server`，理解市场发现、下单、撤单、持仓和结算主链路。
3. 再看 `apps/client`，理解交易页、最近 10 轮、个人页和延迟标注的展示方式。
4. 最后看 `scripts`、`docs` 和 `data`，理解自检、压测、文档和运行产物。

## 根目录文件

### [package.json](/D:/P_T/package.json)

项目脚本和依赖入口。常用脚本包括：

- `npm run dev:server`：启动主服务开发模式。
- `npm run dev:renderer`：启动前端 Vite 开发服务。
- `npm run dev`：启动前端渲染进程和 Electron。
- `npm run dev:stack`：同时启动主服务、前端和 Electron。
- `npm run typecheck`：服务端和前端 TypeScript 类型检查。
- `npm run test:trading`：执行交易深度估算、失败、挂单和压力测试脚本。
- `npm run build`：构建服务端、独立 matching 入口和前端渲染产物。

### [README.md](/D:/P_T/README.md)

面向首次接手者的总览文档，说明项目定位、启动方式和本地调试入口。它适合在进入代码前先浏览。

### [.env.example](/D:/P_T/.env.example)

环境变量模板，包含端口、数据库、Redis、代理、Binance、Polymarket、Chainlink 和市场发现参数。真实运行时复制为 `.env` 后再调整。

### [.env](/D:/P_T/.env)

本机真实运行配置。服务端会直接读取它，所以本地启动失败、外部数据源不通、代理或数据库连接异常时应优先检查这里。该文件通常不进入版本控制。

### [Dockerfile.server](/D:/P_T/Dockerfile.server)

服务端镜像构建文件，支持主服务和独立 matching 服务两个 target。用于容器部署或 CI 构建。

### [docker-compose.local.yml](/D:/P_T/docker-compose.local.yml)

本地联调用 Compose 文件，主要启动 PostgreSQL、Redis，也可启动 app-server 和 matching-service。

### [docker-compose.deploy.yml](/D:/P_T/docker-compose.deploy.yml)

服务器部署用 Compose 文件，比本地版本更偏向长期运行和容器化部署。

## scripts 目录

### [scripts/setup-env.cjs](/D:/P_T/scripts/setup-env.cjs)

从 `.env.example` 初始化本地 `.env`，降低首次配置成本。

### [scripts/local-doctor.cjs](/D:/P_T/scripts/local-doctor.cjs)

本地环境自检脚本，检查 Docker、数据库、Redis、代理、Chainlink 等配置是否满足当前运行模式。

### [scripts/trading-engine-check.mjs](/D:/P_T/scripts/trading-engine-check.mjs)

交易逻辑测试和压力测试脚本。它独立验证 Polymarket CLOB 深度估算逻辑，包括买入、卖出、深度不足失败、限价挂单和 500 笔挂单估算压力场景。

## apps/server 目录

`apps/server` 是主服务，使用 `TypeScript + Fastify`。它负责接入 Binance 和 Polymarket，维护 5 分钟轮次，执行纸面交易，处理冻结、撤单、持仓估值、结算和 WebSocket 推送。

### [apps/server/src/config.ts](/D:/P_T/apps/server/src/config.ts)

服务端配置中心，从环境变量读取端口、数据库、Redis、外部数据源、代理、市场发现、轮询和结算参数。

### [apps/server/src/domain/types.ts](/D:/P_T/apps/server/src/domain/types.ts)

核心类型定义。当前重点类型包括：

- `RoundRecord`：轮次信息，包含 Binance 开收盘价、Polymarket 结算价、结算状态和 redeem 计划时间。
- `OrderRecord`：订单信息，包含买卖方向、盘口轮次、订单类型、生命周期状态、成交明细、冻结金额和数据源延迟。
- `PositionRecord`：持仓信息，包含盘口轮次、均价、锁定数量、当前盘口估值和总价值。
- `PolymarketConnectorState`：Polymarket 市场发现、盘口、成交、WebSocket 和结算事件状态。

### [apps/server/src/index.ts](/D:/P_T/apps/server/src/index.ts)

Fastify HTTP/WebSocket 入口，负责初始化 `AppStore`、`SimulationEngine`，注册登录、市场、下单、撤单、持仓、日志和系统调试接口。

当前交易相关接口主要有：

- `POST /api/orders`：下单，支持买入/卖出、market/limit、金额/数量和限价。
- `DELETE /api/orders/:id`：撤销 pending 限价单并释放冻结资产。
- `POST /api/positions/close-side`：一键平仓。
- `GET /api/rounds/recent`：最近轮次，前端用来展示最近 10 轮。
- `/ws/market`、`/ws/user`：推送市场和用户资产变化。

### [apps/server/src/services/simulation.ts](/D:/P_T/apps/server/src/services/simulation.ts)

主业务引擎，当前最关键的文件。职责包括：

- 启动 Binance 和 Polymarket 连接器。
- 根据 Polymarket BTC 5 分钟市场发现结果创建和推进轮次。
- 使用 Polymarket CLOB 盘口执行买入、卖出、限价挂单、撤单和一键平仓。
- 对 market 单执行 FOK 逻辑，深度不足直接失败。
- 对 limit 单执行 GTC 逻辑，未完全成交则进入 pending，并冻结 USDC 或持仓数量。
- 定期用最新盘口处理 pending 订单。
- 使用 Polymarket 实时结算状态确定胜负方向和结算价。
- 在读取到结算状态后等待 3 秒，模拟 redeem 延迟，再把胜方持仓余额返还钱包。
- 刷新持仓当前 bid/ask/mid、总价值和信息源延迟。

### [apps/server/src/services/clob-execution.ts](/D:/P_T/apps/server/src/services/clob-execution.ts)

Polymarket CLOB 深度执行估算的纯函数模块。它按真实订单簿方向逐档吃单：

- 买入使用 asks，按投入 USDC 估算得到的合约数量。
- 卖出使用 bids，按卖出数量估算得到的 USDC。
- 支持 `limitPrice` 限制。
- 返回是否完全成交、成交明细、均价、最差成交价、成交数量和失败原因。

这个模块不依赖数据库或外部网络，适合被测试脚本和后续单元测试复用。

### [apps/server/src/services/store.ts](/D:/P_T/apps/server/src/services/store.ts)

服务端状态和持久化中心，负责：

- 内存态用户、轮次、订单、持仓、审计日志和训练日志。
- PostgreSQL 表初始化和兼容性字段迁移。
- Redis 快照缓存和 WebSocket 事件发布。
- JSONL 审计/训练日志落盘。

本次调整后，它持久化了订单生命周期、成交明细、冻结金额、持仓锁定数量、当前盘口估值、Polymarket 结算状态和 Binance 开收盘价等字段。

## apps/server/src/services/connectors 目录

### [apps/server/src/services/connectors/binance.ts](/D:/P_T/apps/server/src/services/connectors/binance.ts)

Binance 行情接入器，负责 REST 预热、WebSocket 实时价格、K 线缓存和数据源健康状态。当前前端主图使用 `1m` 和 `5m` K 线，服务端缓存窗口控制在约 1 小时。

### [apps/server/src/services/connectors/polymarket.ts](/D:/P_T/apps/server/src/services/connectors/polymarket.ts)

Polymarket 市场接入器，负责：

- 发现当前 BTC 5 分钟市场。
- 拉取市场详情、UP/DOWN token、盘口和最近成交。
- 建立 market WebSocket，订阅 CLOB book、last trade 和 market resolved 事件。
- 记录最新结算市场、胜方 token/outcome 和结算状态。
- 提供 `fetchBookForSide` / `fetchBookByToken` 供交易引擎下单时获取最新盘口。

如果当前轮次找不到、盘口为空、结算状态没有实时更新，优先检查这个文件和外部网络/代理配置。

### [apps/server/src/services/connectors/network.ts](/D:/P_T/apps/server/src/services/connectors/network.ts)

统一 HTTP/WebSocket 代理和超时处理。外部数据源走本地代理时主要依赖这里。

### [apps/server/src/services/connectors/chainlink.ts](/D:/P_T/apps/server/src/services/connectors/chainlink.ts)

Chainlink BTC/USD Feed 接入器。当前 Polymarket 对齐逻辑优先使用 Polymarket 结算状态，Chainlink 更适合作为全真实源模式下的外部参考。

## apps/server/src/services/matching 目录

该目录保留独立 matching 服务实现和调试能力，但当前主交易链路已经改为镜像 Polymarket CLOB，不再依赖本地用户订单互相撮合来决定主订单成交。

### [apps/server/src/services/matching/order-book.ts](/D:/P_T/apps/server/src/services/matching/order-book.ts)

价格时间优先订单簿实现，仍可用于独立 matching 服务测试、回放或后续扩展。

### [apps/server/src/services/matching/service.ts](/D:/P_T/apps/server/src/services/matching/service.ts)

独立 matching 服务业务层，负责创建订单簿、提交订单、撤单和组织回放结果。

### [apps/server/src/services/matching/client.ts](/D:/P_T/apps/server/src/services/matching/client.ts)

主服务访问独立 matching 服务的客户端封装。当前主下单逻辑不以它作为成交依据。

### [apps/server/src/services/matching/store.ts](/D:/P_T/apps/server/src/services/matching/store.ts)

独立 matching 服务持久化层，保存 matching events 和 book snapshots。

### [apps/server/src/matching-index.ts](/D:/P_T/apps/server/src/matching-index.ts)

独立 matching 服务启动入口。

## apps/client 目录

`apps/client` 是 `Electron + React + Vite` 桌面客户端。它负责展示行情、交易模块、最近 10 轮、持仓、订单、个人页、日志和数据源延迟。

### [apps/client/src/utils/api.ts](/D:/P_T/apps/client/src/utils/api.ts)

前端 API 类型和请求封装。当前下单请求支持：

- `action`：`buy` 或 `sell`。
- `side`：`UP` 或 `DOWN`。
- `orderKind`：`market` 或 `limit`。
- `amount`：买入金额。
- `qty`：卖出数量。
- `limitPrice`：限价。
- `clientSendTs`：用于延迟标注。

它也定义了前端使用的轮次、订单、持仓和市场快照类型。

### [apps/client/src/App.tsx](/D:/P_T/apps/client/src/App.tsx)

前端主业务文件，包含交易页、个人页、K 线图和数据源状态展示。当前重点能力：

- 交易模块支持买入/卖出、UP/DOWN、market/limit、金额/数量和限价输入。
- pending 订单可在主交易页和个人页撤单。
- 最近 10 轮展示 Binance 开盘/收盘价格、Polymarket 结算状态、结算价和来源延迟。
- K 线图显示 Binance BTC K 线、当前价、Polymarket price to beat、轮次开始/结束标记。
- 持仓展示盘口轮次、均价、锁定数量、当前盘口状态、总价值和数据源延迟。
- 订单日志展示类型、盘口轮次、方向、金额、生命周期状态和数据源延迟。

### [apps/client/src/i18n/index.ts](/D:/P_T/apps/client/src/i18n/index.ts)

中英文文案资源。交易模块、轮次、订单、持仓、结算状态和延迟标注相关文案都在这里维护。

### [apps/client/src/styles.css](/D:/P_T/apps/client/src/styles.css)

全局样式。当前新增了交易分段控件、K 线辅助线、表格注释和响应式布局样式。

### [apps/client/src/store/useAppStore.ts](/D:/P_T/apps/client/src/store/useAppStore.ts)

Zustand 全局状态，保存登录 token、用户信息、语言和权限。

### [apps/client/electron/main.cjs](/D:/P_T/apps/client/electron/main.cjs)

Electron 主进程入口，创建桌面窗口并加载开发地址或构建后的静态文件。

### [apps/client/electron/preload.cjs](/D:/P_T/apps/client/electron/preload.cjs)

Electron preload 脚本，用于在渲染进程和桌面能力之间建立受控桥接。

### [apps/client/vite.config.ts](/D:/P_T/apps/client/vite.config.ts)

Vite 构建配置，定义前端源码根目录、构建输出和开发端口。

## docs 目录

### [docs/architecture.md](/D:/P_T/docs/architecture.md)

架构说明，重点解释 Polymarket BTC 5 分钟对齐、外部数据源、交易执行、结算延迟、前端展示和验证命令。

### [docs/file-guide.md](/D:/P_T/docs/file-guide.md)

当前文件，用于定位主要代码文件和职责。

## data 目录

`data` 是本地运行产物目录，不是源码目录。

### [data/logs/audit-events.jsonl](/D:/P_T/data/logs/audit-events.jsonl)

审计日志，记录交易、撤单、结算、redeem 和延迟事件。

### [data/logs/behavior-action-logs.jsonl](/D:/P_T/data/logs/behavior-action-logs.jsonl)

训练日志，记录可用于策略训练的用户动作、市场上下文、成交结果和结算结果。

### [data/logs/matching-events.jsonl](/D:/P_T/data/logs/matching-events.jsonl)

独立 matching 服务事件流，当前主要用于调试或回放。

### [data/logs/matching-snapshots.jsonl](/D:/P_T/data/logs/matching-snapshots.jsonl)

独立 matching 服务订单簿快照。

### [data/postgres](/D:/P_T/data/postgres)

本地 PostgreSQL 数据卷目录，不建议手动修改。

### [data/redis](/D:/P_T/data/redis)

本地 Redis 数据卷目录，不建议手动修改。

## 推荐代码阅读路径

1. [README.md](/D:/P_T/README.md)
2. [apps/server/src/config.ts](/D:/P_T/apps/server/src/config.ts)
3. [apps/server/src/domain/types.ts](/D:/P_T/apps/server/src/domain/types.ts)
4. [apps/server/src/services/connectors/polymarket.ts](/D:/P_T/apps/server/src/services/connectors/polymarket.ts)
5. [apps/server/src/services/clob-execution.ts](/D:/P_T/apps/server/src/services/clob-execution.ts)
6. [apps/server/src/services/simulation.ts](/D:/P_T/apps/server/src/services/simulation.ts)
7. [apps/server/src/services/store.ts](/D:/P_T/apps/server/src/services/store.ts)
8. [apps/server/src/index.ts](/D:/P_T/apps/server/src/index.ts)
9. [apps/client/src/utils/api.ts](/D:/P_T/apps/client/src/utils/api.ts)
10. [apps/client/src/App.tsx](/D:/P_T/apps/client/src/App.tsx)

## 2026-04-25 Update

- [apps/server/src/services/simulation.ts](/D:/P_T/apps/server/src/services/simulation.ts)
  - splits the round reconcile loop into a market snapshot fast path and background Gamma settlement polling
  - starts Gamma polling immediately at round end because `POLL_DELAY_MS` defaults to `0`
  - persists Polymarket UP open/close prices for the recent 10 round market result view
- [apps/server/src/services/connectors/polymarket.ts](/D:/P_T/apps/server/src/services/connectors/polymarket.ts)
  - lets current-round market focus update from existing round metadata without blocking the snapshot fast path on REST book/trade refreshes
- [apps/server/src/services/store.ts](/D:/P_T/apps/server/src/services/store.ts)
  - persists `polymarketOpenPrice` and `polymarketClosePrice` on rounds
- [apps/client/src/App.tsx](/D:/P_T/apps/client/src/App.tsx)
  - displays realized `Total PnL`
  - shows recent 10 rounds as Polymarket/Binance open-close market results
  - uses websocket receipt timestamps for backend-to-frontend latency and shows data age separately
- [scripts/flow-regression-check.ts](/D:/P_T/scripts/flow-regression-check.ts)
  - regression coverage for market open/close history fields, background settlement polling, receipt-based latency, pending settlement isolation, active-round valuation, and redeem wallet return

- [apps/server/src/services/simulation.ts](/D:/P_T/apps/server/src/services/simulation.ts)
  - retries historical Gamma settlement using market id lookup
  - reactivates rounds that were stuck in `Manual`
  - redeems unresolved round positions after a 3 second delay and writes settlement audit logs
  - limits live mark-to-market to the current active round only
- [apps/server/src/services/connectors/polymarket.ts](/D:/P_T/apps/server/src/services/connectors/polymarket.ts)
  - adds direct Gamma `GET /markets/{id}` lookup for closed historical markets
- [apps/server/src/services/store.ts](/D:/P_T/apps/server/src/services/store.ts)
  - derives `displayStatus` for positions
  - excludes `Pending Settlement` positions from live profile valuation
  - sanitizes non-live position book fields for profile rendering
- [apps/client/src/App.tsx](/D:/P_T/apps/client/src/App.tsx)
  - keeps homepage countdown ticking locally between pushes
  - renders `Open`, `Pending Settlement`, `Settled`, and `Sold` position states
  - hides live CLOB book values for non-open positions
- [scripts/flow-regression-check.ts](/D:/P_T/scripts/flow-regression-check.ts)
  - regression coverage for pending settlement isolation, active-round valuation, and redeem wallet return

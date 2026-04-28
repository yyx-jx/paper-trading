



# BTC 5 分钟涨跌模拟盘

面向 `Polymarket BTC 5 分钟涨跌盘口` 的 `C/S` 架构模拟盘工程。这个项目的目标不是做一个通用交易终端，而是做一个尽可能接近 Polymarket 交易体验的比特币五分钟涨跌模拟盘，用于：

- 复刻交易员在盘中看盘、下单、平仓、反手的真实操作路径
- 记录交易行为、盘口快照、撮合结果、结算过程
- 导出结构化训练日志，供后续策略模型训练使用

当前仓库已经具备本地测试闭环能力：

- 客户端：`Electron + React`
- 主服务：`Fastify + TypeScript`
- 独立撮合服务：价格时间优先订单簿
- 数据源：`Binance + Polymarket` 已可本地联通；`Chainlink` 在本地测试模式默认禁用
- 持久化：`PostgreSQL + Redis + JSONL`
- 本地开发代理：通过 `UPSTREAM_PROXY_URL=http://127.0.0.1:7897` 显式代理 Binance / Polymarket

更多实现细节请看 [docs/architecture.md](./docs/architecture.md) 和 [docs/file-guide.md](./docs/file-guide.md)。

## 核心能力

- 单市场交易页，聚焦 `BTC 5 分钟涨跌`
- Binance 实时价格与 `1m / 5m / 1d` K 线展示
- Polymarket 市场发现、盘口深度、最近成交、Gamma 轮询
- 服务端撮合、持仓更新、轮次状态机、结算与 Redeem
- 一键平仓、一键反手买入
- 审计日志与训练日志双轨记录
- 本地测试模式与 Docker 部署模式

## 系统架构概览

```mermaid
flowchart LR
  Client["Electron + React 客户端"] --> App["Fastify 主服务"]
  App --> Match["独立撮合服务"]
  App --> PG["PostgreSQL"]
  App --> Redis["Redis"]
  App --> Binance["Binance REST / WebSocket"]
  App --> Poly["Polymarket Gamma / CLOB / Data API"]
  App -. 可选 .-> Chainlink["Chainlink Feed"]
```

职责划分：

- 客户端负责登录、交易页展示、实时状态刷新、用户持仓与日志展示
- 主服务负责认证、权限、行情聚合、轮次管理、交易业务、日志写入、WebSocket 推送
- 独立撮合服务负责价格时间优先订单簿、外部流动性同步、订单执行、回放
- PostgreSQL 负责结构化持久化
- Redis 负责快照缓存
- JSONL 负责审计与训练日志文件追加

## 仓库目录说明

| 路径 | 说明 |
| --- | --- |
| `apps/client` | Electron + React 客户端源码 |
| `apps/client/src/App.tsx` | 主交易页面与页面状态编排 |
| `apps/client/src/utils/api.ts` | 前端接口类型与请求封装 |
| `apps/client/electron` | Electron 主进程与 preload |
| `apps/server` | 主服务与撮合服务源码 |
| `apps/server/src/index.ts` | 主服务入口，注册 REST / WebSocket 接口 |
| `apps/server/src/matching-index.ts` | 独立撮合服务入口 |
| `apps/server/src/services/simulation.ts` | 轮次、交易、结算、行为日志的主业务引擎 |
| `apps/server/src/services/store.ts` | 主业务持久化、快照、审计日志、训练日志 |
| `apps/server/src/services/connectors` | Binance / Polymarket / Chainlink 数据源连接器 |
| `apps/server/src/services/matching` | 撮合订单簿、回放与撮合持久化实现 |
| `docs/architecture.md` | 更详细的技术架构说明 |
| `docs/file-guide.md` | 逐个文件说明主要代码和配置文件的职责 |
| `scripts/setup-env.cjs` | 首次复制 `.env.example` 为 `.env` |
| `scripts/local-doctor.cjs` | 本地开发自检脚本 |
| `docker-compose.local.yml` | 本地开发与本地集成测试依赖 |
| `docker-compose.deploy.yml` | 服务端 Docker 部署编排 |
| `Dockerfile.server` | 主服务与撮合服务镜像构建文件 |
| `data/logs` | JSONL 日志目录 |
| `data/postgres` | 本地 PostgreSQL 数据卷 |
| `data/redis` | 本地 Redis 数据卷 |
| `dist` | 构建产物输出目录 |

## 撮合系统实现思路


盘口来源
后端通过 Polymarket 连接器维护 BTC 5 分钟涨跌市场和 UP/DOWN token 的盘口数据。相关逻辑在 polymarket.ts。

用户下单
用户下单进入 simulation.ts 的 placeOrder()。它会先确认当前轮次，然后根据方向、side、token 去拿最新可执行盘口。

撮合估算
真正的成交计算在 clob-execution.ts：

买入走 asks，从最低卖价往上吃。
卖出走 bids，从最高买价往下吃。
限价单会检查价格是否满足 limit。
成交对手被标记为 external:polymarket。
这里只计算能不能成交、成交均价、成交数量和成交明细，不直接改账户。
市价单
当前更接近 FOK：如果当前 Polymarket 盘口能完全吃满，就立即成交；如果吃不满，就失败，不会留下部分挂单。

限价单
限价单如果当前盘口已经满足价格并且能完全成交，就立即成交。否则会变成本地 pending 单：

买单冻结 USDC。
卖单冻结对应持仓数量。
后台 processPendingOrders() 会持续用最新 Polymarket 盘口检查，等能完全成交时再成交。
如果过期、轮次结束或进入限制窗口，会释放冻结资产并关闭挂单。
本地账户记账
成交后才会进入本地模拟账户：

买入增加本地 position。
卖出减少或关闭 position。
结算时根据官方结果关闭剩余持仓并返还收益。

## 本地启动

### 推荐模式：本地测试模式

这是当前最稳定、最容易复现的开发方式。特点是：

- `Chainlink` 默认禁用
- `Binance / Polymarket` 通过本机代理访问
- 主服务以内嵌模式启动撮合服务
- `PostgreSQL / Redis` 通过 Docker 启动

本地 `.env` 关键配置如下：

```env
CHAINLINK_ENABLED=false
UPSTREAM_PROXY_URL=http://127.0.0.1:7897
EMBEDDED_MATCHING_SERVICE=true
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/paper_trading
REDIS_URL=redis://127.0.0.1:6379
```

### 启动顺序

首次初始化：

```powershell
npm run setup:env
```

启动本地依赖：

```powershell
docker compose -f docker-compose.local.yml up -d
```

运行本地自检：

```powershell
npm run doctor:local
```

启动主服务：

```powershell
npm run dev:server
```

启动客户端：

```powershell
npm run dev
```

说明：

- `npm run dev` 只启动 `renderer + electron`
- `npm run dev:server` 只启动主服务
- `npm run dev:stack` 会把主服务、renderer、electron 一起起起来
- 本地默认不需要单独执行 `npm run dev:matching`，因为 `EMBEDDED_MATCHING_SERVICE=true`

## 健康检查与验收

服务启动后，先检查：

```powershell
Invoke-RestMethod http://127.0.0.1:8787/health
```

本地测试模式下，理想结果应至少满足：

- `persistence.postgres = true`
- `persistence.redis = true`
- `sources.chainlink.state = "disabled"`
- `sources.binance.state = "healthy"`
- `sources.clob.state = "healthy"` 或至少可用
- `currentRoundPresent = true`

如果这些条件满足，前端一般就能看到：

- Binance 当前价格
- `1m / 5m / 1d` K 线
- Polymarket 当前盘口
- 最近成交
- 主页面轮次与交易信息

## 环境变量说明

### 本地测试模式

| 变量 | 用途 |
| --- | --- |
| `CHAINLINK_ENABLED=false` | 禁用 Chainlink，避免本地被 RPC 阻塞 |
| `UPSTREAM_PROXY_URL` | 让 Node 显式走本机代理访问 Binance / Polymarket |
| `EMBEDDED_MATCHING_SERVICE=true` | 主服务启动时内嵌拉起撮合服务 |
| `DATABASE_URL` | 本地 PostgreSQL 地址 |
| `REDIS_URL` | 本地 Redis 地址 |

### 全真实源模式

只有在你要把 Chainlink 也接入时才需要：

| 变量 | 用途 |
| --- | --- |
| `CHAINLINK_ENABLED=true` | 启用 Chainlink 连接器 |
| `CHAINLINK_RPC_URL` | 主 RPC |
| `CHAINLINK_FALLBACK_RPC_URLS` | 备选 RPC 列表 |
| `CHAINLINK_POLL_MS` | Chainlink 轮询间隔 |

### Docker 部署模式

服务端部署时常用：

| 变量 | 用途 |
| --- | --- |
| `MATCHING_SERVICE_URL` | 主服务访问独立撮合服务的地址 |
| `EMBEDDED_MATCHING_SERVICE=false` | Docker 部署时分离撮合服务 |
| `POLYMARKET_MARKET_ID` / `POLYMARKET_MARKET_SLUG` | Polymarket 自动发现失败时的手工指定 |

## 常见问题

### 1. `ECONNREFUSED 127.0.0.1:5432/6379`

说明本地 `PostgreSQL / Redis` 没启动，先执行：

```powershell
docker compose -f docker-compose.local.yml up -d
docker compose -f docker-compose.local.yml ps
```

### 2. `/health` 里 `Binance` 或 `CLOB` 还是超时

当前项目在 Node 里不会自动继承 Windows 系统代理，所以本地开发要显式配置：

```env
UPSTREAM_PROXY_URL=http://127.0.0.1:7897
```

如果这个值缺失或代理没开，Node 运行时会出现：

- `fetch failed`
- `ETIMEDOUT`
- `currentRoundPresent=false`

### 3. `currentRoundPresent=false`

优先检查：

- `sources.binance` 是否 healthy
- `sources.clob` 是否 healthy
- `UPSTREAM_PROXY_URL` 是否正确
- Polymarket 自动发现是否成功

如果 Binance 和 CLOB 都通了仍然没有当前轮次，再考虑补：

- `POLYMARKET_MARKET_ID`
- `POLYMARKET_MARKET_SLUG`

### 4. 为什么本地测试默认禁用 Chainlink

因为本地开发当前优先目标是先跑通 `Binance + Polymarket + 撮合 + 日志 + 页面` 的可测试闭环，而不是先解决链上 RPC 成本和稳定性。测试模式下：

- `priceToBeat` 会回退到 Binance
- 结算继续以 Polymarket / Gamma 胜负结果为准
- `sources.chainlink.state = disabled`

### 5. 如何进入 Docker 集成测试模式

如果你想把后端也容器化跑起来：

```powershell
docker compose -f docker-compose.local.yml --profile app up -d --build
```

然后客户端仍在本机运行：

```powershell
npm run dev
```

## Docker 部署

服务端部署入口：

```bash
docker compose -f docker-compose.deploy.yml up -d --build
```

常用检查命令：

```bash
docker compose -f docker-compose.deploy.yml ps
docker compose -f docker-compose.deploy.yml logs -f app-server
curl http://127.0.0.1:8787/health
```

说明：

- 部署模式下 `matching-service` 与 `app-server` 分开运行
- `postgres`、`redis`、`matching-service`、`app-server` 都有健康检查

## 文档导航

- [架构说明](./docs/architecture.md)
- [代码文件说明](./docs/file-guide.md)
- [本地环境示例](./.env.example)
- [本地 Docker 编排](./docker-compose.local.yml)
- [部署 Docker 编排](./docker-compose.deploy.yml)

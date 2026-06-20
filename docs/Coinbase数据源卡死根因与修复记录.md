# Coinbase 数据源卡死根因与修复记录

## 背景

- 环境：新生产服务器 `103.179.242.161:10001`
- 现象：前端看到 Coinbase 数据源像“没有了”或长时间不刷新。
- 首次确认时间：北京时间 `2026-06-11 20:34` 左右。

## 现场现象

- `/api/health/ready` 中仍然存在 `Coinbase` source。
- `Coinbase.state` 被返回为 `healthy`。
- 但 `sourceEventTs` 长时间停留在旧时间点，和 `serverNow` 相差约 `35-43` 分钟。
- `publishLatencyMs` 持续达到数百万毫秒。
- Binance 与 CLOB 同时正常，说明不是整机服务冻结。

## 排查结论

- 不是配置关闭：
  - `.env.production` 中 `COINBASE_ENABLED=true`
  - `COINBASE_WS_URL=wss://advanced-trade-ws.coinbase.com`
  - `COINBASE_REST_URL=https://api.exchange.coinbase.com`
- 不是新服务器完全连不上 Coinbase：
  - 从新机直接请求 Coinbase REST 成功，返回 `200`
  - 在 `app-server` 容器内临时创建 Coinbase WebSocket 探针，`20s` 内可收到大量 `ticker` 与 `heartbeats`
- 不是前端把 Coinbase 整体隐藏：
  - 前端仍会读取 `snapshot.sources.coinbase` 与 `snapshot.coinbase`
  - 真正的问题是后端给前端的 Coinbase 最新行情时间戳已经停住

## 根因

根因位于服务端 [coinbase.ts](/D:/P_T/apps/server/src/services/connectors/coinbase.ts)。

旧逻辑只有一个 `lastWsMessageAt`：

- 只要收到任意 WebSocket 消息，就更新 `lastWsMessageAt`
- `heartbeat` 也会更新这个时间
- `pollRestTicker()` 用它判断 WebSocket 是否仍然“活着”
- `checkWsStale()` 也用它判断是否需要重连

这会导致一个半失效状态：

- WebSocket 连接本身还开着
- `heartbeat` 还在持续收到
- 但真正的 `ticker` 行情已经停止

因为 `heartbeat` 持续刷新 `lastWsMessageAt`，系统误以为 Coinbase WebSocket 仍健康：

- 不触发 REST fallback
- 不触发重连
- 健康检查仍显示 `healthy`
- 前端持续拿到一份越来越旧的 Coinbase 价格

## 修复方案

本次修复只改服务端 Coinbase 连接器，不改前端展示逻辑，不改高低频推送结构。

### 修复点 1：拆分“连接活跃”和“ticker 活跃”

新增两个独立时间戳：

- `lastWsActivityAt`
  - 记录任意 WS 消息到达时间
  - 用于判断 socket 是否还有活动
- `lastTickerMessageAt`
  - 只在收到 `ticker` 行情时更新
  - 用于判断真实行情是否仍在流动

### 修复点 2：REST fallback 只看 ticker 是否过期

`pollRestTicker()` 改为只依据 `lastTickerMessageAt` 判断是否需要启用 Coinbase REST fallback。

效果：

- 如果只有 heartbeat，没有 ticker
- 后端会明确进入 `degraded`
- 并返回 `Coinbase WebSocket stale; serving REST fallback data.`

### 修复点 3：stale 重连只看 ticker 是否过期

`checkWsStale()` 改为：

- 如果 ticker 长时间未更新，则主动重连
- 不再被 heartbeat 掩盖

### 修复点 4：补上“连上后一直没收到 ticker”的超时重连

新增 `lastConnectAt`：

- WebSocket 建立后开始计时
- 如果超过 `COINBASE_WS_STALE_MS` 仍没有任何 ticker 到达
- 主动重连，并避免长期卡在 REST fallback

## 修复后的预期行为

- 正常实时流：
  - `state=healthy`
  - `message=Receiving Coinbase live ticker data.`
- WebSocket 开着但 ticker 停住：
  - `state=degraded`
  - 使用 REST fallback
  - 并在超时后自动重连
- 完全不可用：
  - 根据现有连接/请求结果进入 `degraded` 或 `reconnecting`

## 生产验证结果

修复部署到 `103.179.242.161` 后，现场验证结果如下：

- `app-server`、`matching-service`、`caddy`、`postgres`、`redis` 均为 `healthy`
- `/api/health/ready` 返回 `ok=true`
- `schemaMigration=000008`
- Coinbase 恢复为实时状态：
  - `state=healthy`
  - `message=Receiving Coinbase live ticker data.`
  - `publishLatencyMs` 回落到毫秒级

## 运维结论

这次问题不是：

- 服务器 10MB/S 带宽瓶颈
- 新服务器无法访问 Coinbase
- 前端静态隐藏 Coinbase
- `COINBASE_ENABLED=false`

这次问题本质上是：

- Coinbase WebSocket 健康判断粒度过粗
- `heartbeat` 掩盖了 `ticker` 停止

后续如果再次看到类似现象，应优先检查：

1. `/api/health/ready` 中 Coinbase 的 `sourceEventTs`
2. `serverNow - sourceEventTs` 是否持续扩大
3. `message` 是否为 live ticker 还是 REST fallback
4. Coinbase 是否处于 `healthy` 但数据年龄异常偏大

如果出现“状态 healthy 但数据年龄很大”，应首先怀疑：

- ticker 活跃性判断逻辑
- 上游返回结构变化
- 单连接半失效而非整机网络故障

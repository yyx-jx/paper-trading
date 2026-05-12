# WebSocket 协议说明

## 连接方式

客户端优先调用：

```text
POST /api/ws/tickets
```

请求体：

```json
{ "channel": "market" }
```

或：

```json
{ "channel": "user" }
```

返回 ticket 后连接：

```text
/ws/market?ticket=<WS_TICKET>
/ws/user?ticket=<WS_TICKET>
```

兼容模式仍支持：

```text
/ws/market?token=<TOKEN>
/ws/user?token=<TOKEN>
```

## `/ws/market`

当前保持既有协议，不引入前端 layered stream。

### `market`

完整市场快照。连接建立后立即发送，之后按低频 full snapshot 周期补齐。

主要字段：

- `snapshot`：完整 `MarketSnapshot`
- `currentRound`：当前轮次
- `history`：当前用户可见历史轮次
- `settlementPreview`：结算预览
- `transportMeta`：推送时间、序号、队列时间、发送时间

### `market:tick`

高频市场更新。用于价格、倒计时、盘口和源状态实时刷新。

主要字段：

- `tick`：`MarketRealtimeTick`
- `currentRound`：当前轮次轻量更新
- `settlementPreview`：当前轮次结算预览
- `transportMeta`：推送时间、序号、队列时间、发送时间

## `/ws/user`

### `user`

用户完整负载。连接建立后立即发送，或用户管理/日志等完整状态变化时发送。

主要字段：

- `profile`
- `operatedHistory`
- `positions`
- `orders`
- `logs`

### `user:trade`

交易快速负载。用于下单、撤单、卖出、平仓、反手后的轻量刷新，避免每次交易都发送完整日志和历史。

主要字段：

- `profile`
- `positions`
- `orders`：最近交易相关订单

## 心跳与断连

服务端沿用现有 WS 心跳机制。客户端断开后需要重新申请 ticket 或使用 token 重新连接。

## 重构保持项

- `market` / `market:tick` 输出结构保持不变。
- `user` / `user:trade` 输出结构保持不变。
- 本轮只改变后端代码组织，不改变前端需要消费的协议。

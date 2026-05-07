# 系统数据存储规则

本文档说明 BTC 5 分钟涨跌纸面交易系统中各类数据的存储层级、写入时机和持久化策略。

---

## 1. 存储层级总览

系统采用 **内存 → PostgreSQL → JSONL → Redis** 四层存储架构：

| 层级 | 技术 | 定位 | 持久化 |
|------|------|------|:------:|
| **L1 内存** | JavaScript Map/Array | 主工作区，所有读写操作的起点 | ❌ 进程重启丢失 |
| **L2 PostgreSQL** | pg Pool | 权威持久化存储，系统恢复的数据源 | ✅ 硬盘 |
| **L3 JSONL 文件** | `data/logs/*.jsonl` | 日志类数据的追加备份、审计追溯 | ✅ 硬盘 |
| **L4 Redis** | ioredis | 市场快照缓存 + WebSocket Pub/Sub | ❌ 仅缓存，可丢失 |

> 可通过环境变量 `PERSISTENCE_MODE=memory` 完全跳过 L2/L3/L4，纯内存运行。

---

## 2. 各类数据的存储规则

### 2.1 用户数据（UserRecord）

| 维度 | 说明 |
|------|------|
| 内存结构 | `Map<string, UserRecord>` — 以 `userId` 为键 |
| PostgreSQL | `users` 表，每次变更调用 `persistUser()` → `INSERT ... ON CONFLICT DO UPDATE` |
| JSONL | ❌ 不写 JSONL |
| Redis | ❌ 不写 Redis |
| 写入时机 | 创建用户、修改语言、修改密码、禁用/启用、修改余额 — **每次变更立即同步写 PostgreSQL** |
| 启动加载 | PostgreSQL `SELECT * FROM users ORDER BY created_at ASC` 全量加载到内存 |
| 内存上限 | 无硬限制（用户量小） |

### 2.2 轮次数据（RoundRecord）

| 维度 | 说明 |
|------|------|
| 内存结构 | `RoundRecord[]` — 按 `startAt` 降序排列 |
| PostgreSQL | `rounds` 表，每次变更调用 `upsertRound()` → `INSERT ... ON CONFLICT DO UPDATE` |
| JSONL | ❌ 不写 JSONL |
| Redis | ❌ 不写 Redis |
| 写入时机 | 市场发现新轮次、轮次状态变更（Trading → Polling → Settled → Redeeming → Closed）、结算信息更新 — **每次变更立即同步写 PostgreSQL** |
| 启动加载 | PostgreSQL `SELECT * FROM rounds ORDER BY start_at DESC LIMIT 80` |
| 内存上限 | `roundsMemoryMax`（默认 400），超出时按 `startAt` 裁剪，丢弃最早的轮次 |

### 2.3 订单数据（OrderRecord）

| 维度 | 说明 |
|------|------|
| 内存结构 | `OrderRecord[]`，同时维护 `orderIndexById: Map<string, number>` 索引 |
| PostgreSQL | `orders` 表，每次变更调用 `persistOrder()` → `INSERT ... ON CONFLICT DO UPDATE` |
| JSONL | ❌ 不写 JSONL |
| Redis | ❌ 不写 Redis |
| 写入时机 | 下单、成交、撤单、冻结/释放 — **每次变更立即同步写 PostgreSQL** |
| 特殊处理 | 写入前先调用 `persistOrderBookSnapshot()` 将关联的订单簿快照持久化，然后清除 `order.orderBookSnapshot` 对象（仅保留 `orderBookSnapshotRef` 引用） |
| 启动加载 | PostgreSQL `SELECT * FROM orders ORDER BY created_at DESC LIMIT 2000` |
| 内存上限 | `ordersMemoryMax`（默认 4000），超出时按 `createdAt` 裁剪 |

### 2.4 持仓数据（PositionRecord）

| 维度 | 说明 |
|------|------|
| 内存结构 | `PositionRecord[]`，同时维护 `positionIndexById: Map<string, number>` 索引 |
| PostgreSQL | `positions` 表，每次变更调用 `persistPosition()` → `INSERT ... ON CONFLICT DO UPDATE` |
| JSONL | ❌ 不写 JSONL |
| Redis | ❌ 不写 Redis |
| 写入时机 | 开仓、逐 tick 估值更新、平仓、结算 — **每次变更立即同步写 PostgreSQL** |
| 更新策略 | `ON CONFLICT (id) DO UPDATE` 更新 qty / lockedQty / currentMark / currentBid / currentAsk / currentValue / unrealizedPnl / realizedPnl / status / closedAt / settlementResult |
| 启动加载 | PostgreSQL `SELECT * FROM positions ORDER BY opened_at DESC LIMIT 2000` |
| 内存上限 | `positionsMemoryMax`（默认 2000），超出时按 `openedAt` 裁剪 |

### 2.5 订单生命周期日志（OrderLifecycleRecord）

| 维度 | 说明 |
|------|------|
| 内存结构 | `OrderLifecycleRecord[]`，同时维护 `orderLifecycleIndexById` 索引 |
| PostgreSQL | `order_lifecycle_logs` 表，每次变更调用 `persistOrderLifecycle()` |
| JSONL | ❌ 不写 JSONL |
| Redis | ❌ 不写 Redis |
| 写入时机 | 订单买入成交时创建，后续卖出/结算时更新 — **每次变更立即同步写 PostgreSQL** |
| 更新策略 | `ON CONFLICT (id) DO UPDATE` 更新 remaining/closed qty / exit 字段 / settlement 字段 |
| 启动加载 | PostgreSQL `SELECT * FROM order_lifecycle_logs ORDER BY order_timestamp_ms DESC LIMIT 5000` |
| 内存上限 | `orderLifecycleMemoryMax`（默认 5000） |

### 2.6 订单簿快照（OrderBookSnapshotRecord）

| 维度 | 说明 |
|------|------|
| 内存结构 | `Map<string, OrderBookSnapshotRecord>` — 以 SHA256 哈希 `ref` 为键 |
| PostgreSQL | `order_book_snapshots` 表，`persistOrderBookSnapshot()` → `INSERT ... ON CONFLICT DO NOTHING` |
| JSONL | ❌ 不写 JSONL |
| Redis | ❌ 不写 Redis |
| 写入时机 | 订单持久化时附带写入（通过 `persistOrder()` 触发）。**相同内容的快照只存一次**（通过哈希去重），多个订单/生命周期日志共享引用 |
| 去重机制 | 将快照 JSON 序列化后 SHA256 哈希作为主键，`ON CONFLICT DO NOTHING` |
| 启动加载 | PostgreSQL `SELECT * FROM order_book_snapshots ORDER BY snapshot_ts DESC LIMIT 5000` |
| 内存上限 | `orderBookSnapshotsMemoryMax`（默认 4000），额外有时间限制 `orderBookSnapshotsMemoryMaxAgeMs`（默认 2 小时）。裁剪时保留仍在被订单/生命周期日志引用的快照 |

### 2.7 审计日志（AuditEvent） ⭐ 写 JSONL

| 维度 | 说明 |
|------|------|
| 内存结构 | `AuditEvent[]` — 按 `serverRecvTs` 降序（新日志在前） |
| PostgreSQL | `audit_events` 表，`recordLog()` → `INSERT ... ON CONFLICT (event_id) DO NOTHING` |
| JSONL | ✅ **`data/logs/audit-events.jsonl`** — **同步追加写入**（`appendFileSync`） |
| Redis | ❌ 不写 Redis |
| 写入时机 | 每次业务操作（下单、撤单、平仓、结算等）产生审计事件时，**同时写内存 + PostgreSQL（异步） + JSONL（同步追加）** |
| PostgreSQL 写入策略 | `ON CONFLICT DO NOTHING`（eventId 唯一），写入失败**不阻塞业务**（catch 后仅 warn） |
| 启动加载 | PostgreSQL `SELECT * FROM audit_events WHERE server_recv_ts >= $1 ORDER BY server_recv_ts DESC LIMIT 2000`（受 `logRetentionMs` 约束） |
| 内存上限 | `auditLogsMemoryMax`（默认 3000），超出时按 `serverRecvTs` 裁剪 |
| 保留策略 | 超过 `logRetentionMs`（默认 300s = 5 分钟）的日志在 PostgreSQL 中删除，JSONL 文件也会被裁剪重写 |

### 2.8 行为训练日志（BehaviorActionLog） ⭐ 写 JSONL

| 维度 | 说明 |
|------|------|
| 内存结构 | `BehaviorActionLog[]` — 按 `timestampMs` 降序 |
| PostgreSQL | `behavior_action_logs` 表，`recordBehaviorLog()` → `INSERT ... ON CONFLICT (log_id) DO NOTHING` |
| JSONL | ✅ **`data/logs/behavior-action-logs.jsonl`** — **同步追加写入**（`appendFileSync`） |
| Redis | ❌ 不写 Redis |
| 写入时机 | 每次用户交易动作产生行为日志时，**同时写内存 + PostgreSQL（异步） + JSONL（同步追加）** |
| PostgreSQL 写入策略 | `ON CONFLICT DO NOTHING`，写入失败**不阻塞业务**（catch 后仅 warn） |
| 启动加载 | PostgreSQL `SELECT * FROM behavior_action_logs ORDER BY timestamp_ms DESC LIMIT 5000` |
| 内存上限 | `behaviorLogsMemoryMax`（默认 4000），超出时按 `timestampMs` 裁剪 |
| 保留策略 | 仅受 `logRetentionMs` 影响（通过 `cleanupRetentionIfDue` 触发） |

### 2.9 市场快照（MarketSnapshot） ⭐ 写 Redis

| 维度 | 说明 |
|------|------|
| 内存结构 | 单例 `marketSnapshot: MarketSnapshot`，每次覆盖 |
| PostgreSQL | ❌ **不写 PostgreSQL** |
| JSONL | ❌ 不写 JSONL |
| Redis | ✅ **写入 Redis 缓存**：<br>• `market:snapshot:{symbol}` — 完整快照 JSON，TTL 默认 300s<br>• `market:sources:{symbol}` — 数据源状态 JSON，TTL 默认 300s<br>• **Pub/Sub** `market:update:{symbol}` — 发布快照用于跨进程通知 |
| 写入时机 | `SimulationEngine` 每秒调用 `setMarketSnapshot()` |
| 写入方式 | `setImmediate` 异步批量写入（连续写入时只保留最新一份，避免堆积） |
| 启动加载 | 如果 Redis 可用，从 `market:snapshot:{symbol}` 读取上次快照恢复 |
| 降级策略 | Redis 不可用时：跳过写入，仅保留内存中；不影响任何业务功能 |

---

## 3. 写入时机对照表

| 触发事件 | 内存 | PostgreSQL | JSONL | Redis |
|------|:---:|:---:|:---:|:---:|
| 创建/修改用户 | ✅ | ✅ users | — | — |
| 市场发现新轮次 | ✅ | ✅ rounds | — | — |
| 轮次状态变更 | ✅ | ✅ rounds | — | — |
| 轮次结算 | ✅ | ✅ rounds | — | — |
| 下单（市价/限价） | ✅ | ✅ orders + order_book_snapshots | — | — |
| 订单成交/失败 | ✅ | ✅ orders | — | — |
| 撤单 | ✅ | ✅ orders | — | — |
| 冻结/释放资金 | ✅ | ✅ orders | — | — |
| 开仓 | ✅ | ✅ positions | — | — |
| 持仓估值更新（每秒） | ✅ | ✅ positions | — | — |
| 平仓 | ✅ | ✅ positions | — | — |
| 结算 redeem | ✅ | ✅ positions | — | — |
| 订单生命周期更新 | ✅ | ✅ order_lifecycle_logs | — | — |
| 审计事件（所有操作） | ✅ | ✅ audit_events | ✅ audit-events.jsonl | — |
| 行为训练日志 | ✅ | ✅ behavior_action_logs | ✅ behavior-action-logs.jsonl | — |
| 市场快照更新（每秒） | ✅ | — | — | ✅ 缓存 + Pub/Sub |

---

## 4. 数据流示意图

```
┌──────────────────────────────────────────────────────────┐
│                    SimulationEngine                       │
│                                                          │
│  下单/成交/撤单/结算/估值/审计/行为日志                      │
└───────┬──────────────────────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────┐
│                     AppStore (内存)                        │
│                                                          │
│  users: Map        rounds: []         orders: []          │
│  positions: []     orderLifecycles: []                    │
│  logs: []          behaviorLogs: []                       │
│  orderBookSnapshots: Map    marketSnapshot: {}            │
│                                                          │
│  ★ 所有数据先在内存中更新，再向外同步                         │
└───┬────────────┬──────────────────┬──────────────────────┘
    │            │                  │
    │ await      │ appendFileSync   │ setImmediate
    ▼            ▼                  ▼
┌─────────┐ ┌──────────┐    ┌──────────────┐
│PostgreSQL│ │ JSONL文件 │    │    Redis     │
│(权威存储)│ │(日志备份) │    │  (快照缓存)   │
│         │ │          │    │              │
│ users   │ │audit-    │    │ market:      │
│ rounds  │ │events.   │    │ snapshot:    │
│ orders  │ │jsonl     │    │ {symbol}     │
│ positions│ │          │    │              │
│ order_  │ │behavior- │    │ market:      │
│ lifecycles│ │action-  │    │ sources:     │
│ order_  │ │logs.jsonl│    │ {symbol}     │
│ book_   │ │          │    │              │
│ snapshots│ │          │    │ Pub/Sub      │
│ audit_  │ │          │    │ market:      │
│ events  │ │          │    │ update:      │
│ behavior│ │          │    │ {symbol}     │
│ _logs   │ │          │    │              │
└─────────┘ └──────────┘    └──────────────┘
```

---

## 5. PostgreSQL vs JSONL 的区别

| 特性 | PostgreSQL | JSONL 文件 |
|------|------------|------------|
| 数据 | 全部业务实体 | **仅**审计日志 + 行为训练日志 |
| 写入方式 | `await pool.query()`（异步） | `appendFileSync()`（同步阻塞） |
| 写入失败 | strict 模式下抛异常阻塞操作 | 无法失败（同步写磁盘），异常由进程捕获 |
| 用途 | 系统恢复、查询、统计 | 审计追溯、离线分析、训练数据导出 |
| 查询能力 | 支持 SQL 筛选、分页、聚合 | 无索引，仅支持尾部读取 |
| 保留策略 | `logRetentionMs` 到期 DELETE | 文件裁剪重写（读尾部 512KB，过滤后写回） |
| 可禁用 | `PERSISTENCE_MODE=memory` | 始终写入（只要 `LOG_DIR` 可写） |

---

## 6. 内存保护机制

系统每 15 秒检查一次堆内存使用量：

| 状态 | 阈值 | 行为 |
|------|------|------|
| `normal` | < `serverHeapWarnMb`（默认 768MB） | 正常运行 |
| `warning` | >= 768MB | 触发 `pruneMemoryCaches()` 强制裁剪 |
| `protect` | >= `serverHeapProtectMb`（默认 1024MB） | 强制裁剪 + 拒绝新的大内存分配 |

裁剪按各数据类型的 `*MemoryMax` 上限执行，保留下限内的最新数据。

---

## 7. 启动恢复流程

```
AppStore.init()
  │
  ├─ 1. connectPostgres()
  │     ├─ PERSISTENCE_MODE=memory → 跳过
  │     ├─ 尝试连接 (最多 10 次, 每次间隔 2s)
  │     ├─ 执行 SCHEMA_SQL 建表
  │     └─ 失败 → strict 模式抛异常 / 非 strict 降级纯内存
  │
  ├─ 2. seedUsers()
  │     ├─ PostgreSQL 可用 → 检查并插入 4 个种子用户 (tester/senior/engineer/admin)
  │     └─ PostgreSQL 不可用 → 内存中创建种子用户
  │
  ├─ 3. connectRedis()
  │     ├─ PERSISTENCE_MODE=memory → 跳过
  │     ├─ 尝试连接 (最多 10 次)
  │     └─ 失败 → 跳过缓存，不影响系统运行
  │
  └─ 4. loadStateFromPersistence()
        ├─ PostgreSQL 可用 → 加载所有表数据到内存 (带 LIMIT)
        └─ Redis 可用 → 恢复上次 marketSnapshot
```

---

## 8. 总结：哪些数据进硬盘，哪些不進

| 数据 | 进 PostgreSQL | 进 JSONL | 只存内存 |
|------|:---:|:---:|:---:|
| 用户 (UserRecord) | ✅ | — | — |
| 轮次 (RoundRecord) | ✅ | — | — |
| 订单 (OrderRecord) | ✅ | — | — |
| 持仓 (PositionRecord) | ✅ | — | — |
| 订单生命周期 (OrderLifecycleRecord) | ✅ | — | — |
| 订单簿快照 (OrderBookSnapshotRecord) | ✅ | — | — |
| 审计日志 (AuditEvent) | ✅ | ✅ | — |
| 行为训练日志 (BehaviorActionLog) | ✅ | ✅ | — |
| 市场快照 (MarketSnapshot) | — | — | ✅ (Redis 仅缓存) |
| 数据源健康状态 (SourceHealth) | — | — | ✅ (含在快照中进 Redis) |
| WebSocket 连接状态 | — | — | ✅ |
| 内存索引 (Map<id, index>) | — | — | ✅ (启动时重建) |

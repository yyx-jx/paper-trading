# 生产环境数据存取优化建议

> 当前系统为单进程架构，面向数十位并发用户。以下建议按优先级排序。

---

## P0 — 必须修

### 1. `appendFileSync` 改异步写入

**现状**（`store.ts:2689, 2742`）：

```ts
// 每条审计日志和行为日志都做同步磁盘写入
appendFileSync(LOG_FILE, `${JSON.stringify(event)}\n`, "utf-8");
```

**问题**：每次调用阻塞事件循环 0.1-1ms。50 个用户活跃交易时，每秒可能产生 20-50 条审计事件，累积阻塞 5-50ms/秒，直接表现为 WebSocket 推送延迟抖动。

**建议**：

```ts
// 方案 A（最小改动）：改用 WriteStream
private auditStream = createWriteStream(LOG_FILE, { flags: "a" });

async recordLog(event: AuditEvent) {
  this.logs.unshift(event);
  this.auditStream.write(`${JSON.stringify(event)}\n`);
  void this.runDb(...)  // PostgreSQL 已经是异步的
}
```

```ts
// 方案 B（更健壮）：内存缓冲 + 批量刷盘
private auditBuffer: string[] = [];
private auditFlushTimer: NodeJS.Timeout;

recordLog(event: AuditEvent) {
  this.logs.unshift(event);
  this.auditBuffer.push(JSON.stringify(event));
  if (this.auditBuffer.length >= 50) this.flushAuditBuffer();
}

private flushAuditBuffer() {
  const lines = this.auditBuffer.splice(0);
  if (lines.length) {
    appendFileSync(LOG_FILE, lines.join("\n") + "\n", "utf-8"); // 仍然同步，但频率很低
  }
}
```

方案 A 足够应对数十用户规模，方案 B 在百用户以上更合适。

---

### 2. 持仓估值变更不再逐 tick 写 PostgreSQL

**现状**：持仓的 `currentMark` / `currentBid` / `currentAsk` / `currentValue` / `unrealizedPnl` 在 `refreshOpenPositions()` 中每秒更新，但当前代码**并未**将这些变更写入 PostgreSQL（`persistPosition` 仅在开仓、平仓、结算时调用）。

**问题**：当前实现实际上是正确的——每秒估值只更新内存并通过 WebSocket 推送，**不写 PostgreSQL**。这是一个隐式的良好设计，但如果后续有人"补上"逐 tick 写 PostgreSQL，会立即造成写入风暴（50 用户 × 3 持仓 × 1次/秒 = 150 UPSERT/秒）。

**建议**：**保持现状不变**。同时在 `persistPosition` 上加注释，明确标注估值字段的写入策略，防止未来误改。

---

## P1 — 建议修

### 3. 日志批量写入 PostgreSQL

**现状**：每条审计日志和行为日志独立执行 `INSERT`，各自开启一个事务。

```ts
// store.ts recordLog() — 每个 event 一个 INSERT
void this.runDb(`INSERT INTO audit_events (...) VALUES (...)`, [...])
```

**问题**：高负载时大量小事务竞争 PG 连接池（默认 max=10）。

**建议**：累积批量插入。

```ts
private pendingAuditRows: AuditEvent[] = [];
private auditBatchTimer?: NodeJS.Timeout;

async recordLog(event: AuditEvent) {
  this.logs.unshift(event);
  this.pendingAuditRows.push(event);
  this.auditStream.write(`${JSON.stringify(event)}\n`); // JSONL 也异步化
  if (this.pendingAuditRows.length >= 100) this.flushAuditBatch();
  if (!this.auditBatchTimer) {
    this.auditBatchTimer = setTimeout(() => this.flushAuditBatch(), 1000);
  }
  // ...
}

private async flushAuditBatch() {
  const batch = this.pendingAuditRows.splice(0);
  if (!batch.length) return;
  clearTimeout(this.auditBatchTimer);
  this.auditBatchTimer = undefined;
  // 使用 pg unnest 或 multi-row INSERT
  await this.runDb(
    `INSERT INTO audit_events (...) VALUES ${batch.map((_, i) => `(${/*...*/})`).join(",")} 
     ON CONFLICT (event_id) DO NOTHING`,
    batch.flatMap(e => [/* params */])
  );
}
```

权衡：日志写入延迟从 0ms 变为最多 1 秒 / 100 条阈值。对审计场景可接受。

---

### 4. WebSocket 推送增加节流

**现状**：`emitUserPayload` 用 `setImmediate` 合并同一轮事件循环内的多次调用，但不限制推送频率。如果用户在一秒内下 3 单，会收到 3 次完整 UserPayload 推送。

**建议**：加一个最小推送间隔（如 200ms）。

```ts
private lastUserPayloadPush = new Map<string, number>();
private readonly USER_PAYLOAD_MIN_INTERVAL_MS = 200;

private flushUserPayloads() {
  const now = Date.now();
  for (const userId of userIds) {
    const lastPush = this.lastUserPayloadPush.get(userId) ?? 0;
    if (now - lastPush < this.USER_PAYLOAD_MIN_INTERVAL_MS) {
      // 推迟到下一批次
      this.pendingUserPayloadIds.add(userId);
      continue;
    }
    this.lastUserPayloadPush.set(userId, now);
    // 推送...
  }
}
```

---

### 5. PostgreSQL 连接池扩容 + 读写分离准备

**现状**：`pgMaxConnections` 默认 10。

**建议**：

```
PG_MAX_CONNECTIONS=25  # 50 用户场景
```

如果后续要扩展到百人以上，考虑：
- 用 PgBouncer 做连接池中间件
- 将日志类查询（审计日志搜索、行为日志回溯）路由到只读副本
- 主库只承担写入和实时查询

---

## P2 — 优化建议

### 6. round / order / position 内存查询统一加 PostgreSQL fallback

**现状**：API 查询直接从内存数组读取。一旦内存被裁剪（prune），历史数据对 API 不可见。

**建议**：关键查询接口增加数据库回退。

```ts
getOrderById(orderId: string) {
  const mem = this.orders[this.orderIndexById.get(orderId)];
  if (mem) return mem;
  // fallback to PostgreSQL
  if (this.postgresEnabled) {
    const result = await this.pool.query("SELECT * FROM orders WHERE id = $1", [orderId]);
    return result.rowCount ? this.rowToOrder(result.rows[0]) : undefined;
  }
  return undefined;
}
```

不需要每个接口都加——优先覆盖：订单详情、持仓历史、最近的轮次列表。

---

### 7. JSONL 文件按日切割

**现状**：所有日志写入同一个 `audit-events.jsonl`，无限增长。`pruneLogFile` 每次裁剪都读尾部 512KB 再全量重写。

**问题**：文件变大后，裁剪操作开销增加；排查问题时难以定位特定日期的日志。

**建议**：

```ts
const LOG_FILE = path.join(LOG_DIR, `audit-events-${today()}.jsonl`);
```

每天午夜切换新文件，保留最近 N 天（如 7 天），定期删除过期文件。裁剪变成直接删旧文件，不再需要读写重写。

---

### 8. 内存裁剪策略改为 LRU 按需加载

**现状**：裁剪是硬截断——超过 `*MemoryMax` 就丢弃最旧的数据。

**建议**：长期看，考虑改为 SQLite 或 Redis 做热数据缓存层，PostgreSQL 做冷数据存储。现在阶段可以不急着做，但不要在新功能中依赖"内存中一定有全量数据"。

---

## 不需要急着做的事情

以下看起来是问题但实际上当前不需要动：

| 你以为的问题 | 实际状况 |
|------|------|
| "每秒持仓估值写爆 PostgreSQL" | 当前估值**不写 PostgreSQL**，只更新内存 + WebSocket 推送。保持现状即可 |
| "MarketSnapshot 无持久化" | 不需要。这是实时快照，重启后一秒内就能重建 |
| "Chainlink K 线不持久化" | 不需要。重启后从第一个 tick 重新积累，前端几秒内恢复显示 |
| "订单簿快照太多撑爆 PG" | 已用 SHA256 去重，相同快照只存一份 |

---

## 建议实施顺序

```
第一周（上线前必须）:
  ├─ P0-1: appendFileSync → WriteStream 异步化
  └─ P0-2: 在 persistPosition 加注释防误改

第二周（上线后迭代）:
  ├─ P1-3: 日志批量写入 PG
  ├─ P1-4: 用户推送节流
  └─ P1-5: PG 连接池扩容至 25

后续（按需）:
  ├─ P2-6: 关键查询 PG fallback
  ├─ P2-7: JSONL 按日切割
  └─ P2-8: LRU 内存策略
```

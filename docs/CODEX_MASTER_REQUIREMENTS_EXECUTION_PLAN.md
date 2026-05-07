# Hyper Terminal / BTC Paper Trading：Codex 总体需求与执行计划书

> 本文件供 Codex 直接阅读、拆分任务、修改代码和编写测试使用。  
> 范围覆盖本次对话中确认过的全部新增/修正需求：
>
> 1. C/S 长期部署时的稳定性、性能和容错机制。
> 2. 数据库结构后续版本变更时，旧数据兼容迁移到新数据库结构。
> 3. 用户系统：身份、权限管理、个人信息管理、密码与账号安全。
> 4. 用户/客户数据导出内容、格式、匿名化、权限范围和审计。
> 5. 盈亏 PnL 与手续费口径：后端是否纳入手续费、前端如何显示。
> 6. 不影响现有功能前提下的安全代码整理与冗余清理方案。

---

## 0. Codex 执行原则

### 0.1 首要原则

本项目的核心价值是向客户交付可审计的人工 Paper Trade 数据。任何修改都必须优先保证：

```text
交易数据可信
权限边界可信
数据库升级可回滚
历史数据不丢失
盈亏/手续费口径可解释
部署后长期运行稳定
```

不要为了快速实现新 UI 或新字段而破坏已有交易、结算、订单、持仓、日志、导出、权限逻辑。

---

### 0.2 本轮禁止事项

Codex 修改时必须遵守：

```text
1. 不要删除现有 /api/logs/export。
2. 不要删除 seniorTesterId 字段；只能新增 managerUserId 并兼容旧字段。
3. 不要删除旧 PnL 字段：realizedPnl、unrealizedPnl、notionalSpent、averageEntry。
4. 不要删除现有 CSV/TSV 批量创建能力；Excel 支持只能作为增强。
5. 不要删除现有 JSONL 审计日志能力；只能改为异步队列/轮转/结构化增强。
6. 不要在生产环境启动时静默 ALTER TABLE 或重建数据库。
7. 不要让 Electron 客户端执行数据库 migration。
8. 不要把真实 user_id、username、displayName 导出给客户数据集。
9. 不要只在前端隐藏按钮来实现权限；所有 API 必须后端鉴权。
10. 不要重写整个 App.tsx；先抽取、兼容、测试，再逐步清理。
```

---

### 0.3 推荐执行顺序

本文件的推荐顺序是：

```text
阶段 0：现状核验，避免重复改已经完成的功能
阶段 1：数据库迁移、备份、历史数据兼容底座
阶段 2：用户身份安全、统一权限内核、个人信息管理
阶段 3：用户管理功能补全与 Excel 批量创建
阶段 4：盈亏/手续费口径核验、标准化和前端展示
阶段 5：客户数据集导出、匿名化、格式和审计
阶段 6：部署稳定性、性能、容错、监控和长期运行优化
阶段 7：测试体系、回归脚本、浸泡测试
阶段 8：安全代码整理与冗余清理
```

解释：数据库 migration 是所有后续字段变更的保护伞；权限内核是用户管理、日志、导出范围的保护伞；PnL/手续费口径必须在客户数据导出前定清楚；部署性能优化应在核心数据结构稳定后系统化执行。

---

## 1. 项目当前已知结构

### 1.1 主要技术栈

```text
前端：Electron + React + Vite + Zustand + react-i18next
后端：Fastify + TypeScript + pg + redis + WebSocket
数据：PostgreSQL + Redis + JSONL 文件日志 + 内存热状态
外部数据：Polymarket Gamma/CLOB/Data/WS、Binance、Chainlink
```

### 1.2 重点代码位置

Codex 开始前应重点阅读：

```text
apps/server/src/domain/types.ts
apps/server/src/config.ts
apps/server/src/index.ts
apps/server/src/services/store.ts
apps/server/src/services/simulation.ts
apps/server/src/services/clob-execution.ts
apps/server/src/services/clob-fees.ts
apps/server/src/services/csv-zip-export.ts
apps/server/src/services/bulk-users.ts
apps/server/src/services/connectors/polymarket.ts
apps/client/src/App.tsx
apps/client/src/utils/api.ts
apps/client/src/store/useAppStore.ts
Dockerfile.server
docker-compose.deploy.yml
docker-compose.local.yml
package.json
scripts/*.ts
```

### 1.3 当前代码里与本计划相关的已知事实

基于已读源码，当前项目已经有若干能力，Codex 不应重复实现，而应核验后补强：

```text
1. Role 已定义：Tester、Senior Tester、Test Engineer、Admin。
2. PermissionCode 已有基础权限码，但不够表达导出、解锁、质量审核、范围管理等功能。
3. UserRecord 当前有 password、role、language、permissionCodes、availableUsdc、seniorTesterId 等字段。
4. OrderRecord 已有 estimatedFee、actualFee、feeBreakdown、feeCurrency 等手续费相关字段。
5. clob-execution.ts 已调用 calculateClobFees() 估算 CLOB 手续费。
6. simulation.ts 的买入路径会将 estimate.matchedNotional + actualFee 计入 position.notionalSpent。
7. simulation.ts 的卖出路径会从 proceeds 中扣除 actualFee，并用 position.averageEntry 释放成本。
8. PositionRecord 当前缺少 entryFeeUsdc、exitFeeUsdc、totalFeeUsdc、costBasisUsdc、markPnl、executablePnl 等显式口径字段。
9. 前端持仓摘要主要显示 positionDisplayedPnl(position)，即 open 显示 unrealizedPnl，closed 显示 realizedPnl。
10. 前端已有估算手续费展示片段，但持仓/总资产/盈亏处缺少“是否含手续费”的清晰说明和双口径 PnL 展示。
```

因此，PnL/手续费部分不是从零开始，而是要做“核验 + 标准化 + 显式字段 + 前端说明 + 测试”。

---

## 2. 阶段 0：现状核验任务

### 2.1 目的

在正式改代码前，Codex 必须先输出当前实现核验表，避免重复改已经完成的功能。

### 2.2 需要核验的功能项

Codex 先读代码并输出下表：

```text
功能项 | 当前是否已实现 | 实现文件 | 是否符合需求 | 是否需要修改 | 修改风险 | 建议动作
```

至少核验以下项目：

```text
数据库 migration 是否已存在
备份/恢复脚本是否已存在
store.ts 是否仍在生产环境自动建表/改表
JWT_SECRET 生产强校验是否已存在
密码是否仍明文存储
旧明文密码是否能迁移为 hash
CORS 是否白名单化
登录是否限流
WebSocket 是否有 heartbeat 和鉴权续期机制
角色权限是否集中在统一模块
Test Engineer 是否还能查看 Admin 日志
Tester 是否能在个人页修改密码
managerUserId 是否存在
permissionLevel Initial/Standard 是否存在
权限解锁/降级是否存在
权限变更是否写审计日志
Excel 批量创建是否支持
客户数据集导出接口是否与内部日志导出分离
客户数据导出是否匿名化
客户数据导出是否默认过滤 D 级
导出是否写 export audit log
PnL 是否包含买入实际手续费
卖出 PnL 是否扣除卖出手续费
redeem PnL 是否以含手续费成本为基准
前端 PnL 是否明确标注含手续费
前端是否区分 mid mark PnL 与 best bid executable PnL
JSONL 写入是否仍有同步 appendFileSync / writeFileSync
API 列表是否分页
WS 是否全量高频推送大 payload
健康检查/metrics 是否存在
```

### 2.3 阶段 0 交付物

Codex 应新增或输出：

```text
docs/codex-current-state-audit.md
```

内容包括：

```text
1. 已实现功能清单。
2. 未实现功能清单。
3. 已实现但不符合需求的功能。
4. 可以安全直接改的文件。
5. 高风险修改点。
6. 本计划后续阶段是否需要调整。
```

如果发现某项功能已经完整实现并有测试覆盖，则不要重复开发，只补测试或文档。

---

## 3. 阶段 1：数据库迁移、备份与旧数据兼容

### 3.1 目标

后续版本中数据库结构会持续变化。必须建立版本化 migration 体系，保证：

```text
旧数据库可以升级到新结构
旧数据可以被新代码读取
新字段可以逐步回填
生产升级前有备份
数据库版本不匹配时服务端拒绝启动
任何环境的 schema 变化都可复现
```

### 3.2 推荐技术方案

使用：

```text
node-pg-migrate + PostgreSQL + pg_dump/pg_restore
```

原因：当前项目是 Node/TypeScript + pg，node-pg-migrate 更容易与 package.json、Docker、CI、部署脚本整合。

### 3.3 新增目录结构

Codex 新增：

```text
db/
  migrations/
    000001_initial_schema.ts
    000002_add_user_manager_and_permission_level.ts
    000003_add_pnl_fee_fields.ts
    000004_add_export_audit_and_dataset_schema.ts
  seeds/
    .gitkeep
  README.md

scripts/
  db-backup.sh
  db-restore.sh
  db-migrate-check.ts
  migration-smoke-test.ts
```

也可以用 node-pg-migrate 自动生成的时间戳文件名，但命名必须可读。

### 3.4 package.json 脚本

新增：

```json
{
  "scripts": {
    "db:migrate": "node-pg-migrate up -m db/migrations",
    "db:rollback": "node-pg-migrate down -m db/migrations -n 1",
    "db:status": "node-pg-migrate status -m db/migrations",
    "db:create-migration": "node-pg-migrate create -m db/migrations",
    "db:backup": "bash scripts/db-backup.sh",
    "db:restore": "bash scripts/db-restore.sh",
    "test:migrations": "tsx scripts/migration-smoke-test.ts"
  }
}
```

### 3.5 store.ts 改造原则

当前 `apps/server/src/services/store.ts` 负责很多事情：内存状态、PG schema/read/write、Redis、JSONL、权限查询等。

本阶段必须把 schema 管理从 store.ts 中剥离出来：

```text
store.ts：只负责数据读写和状态聚合
migration：负责建表、改表、加索引、回填数据
schema-check：负责启动时检查数据库版本
```

生产环境禁止在 store.ts 中静默执行大量：

```sql
CREATE TABLE IF NOT EXISTS ...
ALTER TABLE ... ADD COLUMN ...
```

允许开发环境保留最小初始化兜底，但生产环境必须通过 migration。

### 3.6 启动前 schema 检查

新增：

```text
apps/server/src/db/schema-check.ts
apps/server/src/db/pool.ts
```

服务端启动时：

```ts
await assertDatabaseMigrated(pool, EXPECTED_SCHEMA_VERSION);
```

如果数据库没有迁移到当前版本，直接启动失败：

```text
Database schema is outdated. Please run npm run db:migrate before starting server.
```

### 3.7 历史数据兼容策略：Expand → Migrate → Contract

所有数据库字段改动必须分阶段：

```text
Expand：新增字段/新表/新索引，不删除旧字段
Migrate：回填旧数据，代码双读/双写
Contract：多个版本后确认旧字段无人使用，再删除
```

示例：字段 `side` 改为 `direction` 时，禁止直接重命名或删除：

```sql
ALTER TABLE orders ADD COLUMN IF NOT EXISTS direction text;
UPDATE orders SET direction = side WHERE direction IS NULL;
```

代码读取时：

```ts
const direction = row.direction ?? row.side;
```

至少一个版本内双写：

```ts
side: direction,
direction
```

### 3.8 核心表新增 data_version

为后续兼容读取，给核心业务表增加版本字段：

```sql
ALTER TABLE users ADD COLUMN IF NOT EXISTS data_version integer DEFAULT 1;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS data_version integer DEFAULT 1;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS data_version integer DEFAULT 1;
ALTER TABLE rounds ADD COLUMN IF NOT EXISTS data_version integer DEFAULT 1;
ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS data_version integer DEFAULT 1;
ALTER TABLE behavior_logs ADD COLUMN IF NOT EXISTS data_version integer DEFAULT 1;
```

如果表名与当前实际 schema 不一致，Codex 以当前 schema 为准调整。

### 3.9 新增 mapper/normalizer 层

新增：

```text
apps/server/src/mappers/user.mapper.ts
apps/server/src/mappers/order.mapper.ts
apps/server/src/mappers/position.mapper.ts
apps/server/src/mappers/round.mapper.ts
apps/server/src/mappers/log.mapper.ts
```

职责：把不同版本 DB row 转成当前领域对象。

示例：

```ts
export function mapPositionRow(row: DbPositionRow): PositionRecord {
  const dataVersion = Number(row.data_version ?? 1);

  return {
    id: row.id,
    userId: row.user_id,
    roundId: row.round_id,
    side: row.side,
    qty: Number(row.qty),
    averageEntry: Number(row.average_entry),
    notionalSpent: Number(row.notional_spent),
    costBasisUsdc: Number(row.cost_basis_usdc ?? row.notional_spent),
    entryFeeUsdc: Number(row.entry_fee_usdc ?? 0),
    exitFeeUsdc: Number(row.exit_fee_usdc ?? 0),
    totalFeeUsdc: Number(row.total_fee_usdc ?? 0),
    unrealizedPnl: Number(row.unrealized_pnl ?? 0),
    realizedPnl: Number(row.realized_pnl ?? 0),
    dataVersion
  };
}
```

### 3.10 生产升级流程

正式服务器升级必须按这个顺序：

```text
1. 进入维护模式或暂停新订单。
2. 停止 app-server 或关闭交易入口。
3. 执行 npm run db:backup。
4. 执行 npm run db:migrate。
5. 执行 npm run build。
6. 启动新 app-server。
7. 验证登录、行情、下单、撤单、平仓、结算、导出、日志。
8. 恢复用户访问。
```

### 3.11 db-backup.sh 示例要求

`scripts/db-backup.sh`：

```bash
#!/usr/bin/env bash
set -euo pipefail

: "${DATABASE_URL:?DATABASE_URL is required}"

mkdir -p backups
STAMP="$(date +%Y%m%d_%H%M%S)"
OUT="backups/hyper_terminal_${STAMP}.dump"

pg_dump -Fc "$DATABASE_URL" > "$OUT"
sha256sum "$OUT" > "$OUT.sha256"
echo "Backup created: $OUT"
```

### 3.12 db-restore.sh 示例要求

`scripts/db-restore.sh`：

```bash
#!/usr/bin/env bash
set -euo pipefail

: "${DATABASE_URL:?DATABASE_URL is required}"
: "${BACKUP_FILE:?BACKUP_FILE is required}"

pg_restore --clean --if-exists --dbname "$DATABASE_URL" "$BACKUP_FILE"
```

### 3.13 Redis 和 JSONL 也要版本化

数据库 migration 只覆盖 PostgreSQL，但当前项目还有 Redis 和 JSONL。

Redis key 应加版本前缀：

```text
ht:v1:market:snapshot
ht:v1:user:{userId}:state
ht:v1:round:{roundId}
```

升级时如果 Redis 只是缓存，可清理旧 key 后从 PostgreSQL 重建。

JSONL 每条日志应加：

```json
{
  "schemaVersion": 1,
  "eventType": "ORDER_PLACED",
  "payload": {}
}
```

读取旧 JSONL 时用 parser 兼容：

```text
parseAuditLogV1()
parseAuditLogV2()
```

### 3.14 阶段 1 验收标准

必须通过：

```bash
npm ci
npm run typecheck
npm run db:migrate
npm run db:status
npm run test:migrations
npm run test:config
npm run build
```

补充验收：

```text
1. 空数据库可迁移到最新版。
2. 旧 schema 数据库可迁移到最新版。
3. migration 前可以生成 pg_dump 备份。
4. 服务端启动时能识别未迁移数据库并拒绝启动。
5. 旧用户、旧订单、旧持仓、旧轮次、旧日志能被 mapper 正常读取。
6. 不允许生产 store.ts 静默改表。
```

---

## 4. 阶段 2：用户身份安全、统一权限内核、个人信息管理

### 4.1 目标

实现真正可靠的用户身份与权限系统：

```text
统一 RBAC 权限定义
统一数据可见范围计算
所有 API 后端鉴权
个人资料和密码自助修改
密码安全存储
权限变更完整留痕
避免 Test Engineer 越权查看 Admin 数据
```

### 4.2 新增后端模块

建议新增：

```text
apps/server/src/auth/permissions.ts
apps/server/src/auth/scope.ts
apps/server/src/auth/password.ts
apps/server/src/auth/authz.ts
apps/server/src/auth/audit-auth.ts
```

职责：

```text
permissions.ts：角色权限矩阵、权限码、功能点定义
scope.ts：当前用户能访问哪些 userId / roundId / log 范围
authz.ts：assertCan()、assertCanAccessUser()、assertCanExportDataset()
password.ts：hash、verify、旧明文迁移
audit-auth.ts：越权访问、权限变更、密码变更审计
```

### 4.3 扩展 PermissionCode

当前权限码不够。建议扩展为：

```ts
export type PermissionCode =
  | "trade:view"
  | "trade:order"
  | "trade:cancel"
  | "trade:sell"
  | "profile:view"
  | "profile:update"
  | "profile:password:change"
  | "system:status:view"
  | "users:list"
  | "users:create"
  | "users:bulk-create"
  | "users:disable"
  | "users:reset-password"
  | "users:balance:set"
  | "users:role:set"
  | "users:manager:set"
  | "users:permission-level:unlock"
  | "users:permission-level:downgrade"
  | "logs:view:self"
  | "logs:view:team"
  | "logs:view:managed"
  | "logs:view:all"
  | "logs:export:internal"
  | "dataset:export:managed"
  | "dataset:export:all"
  | "dataset:export:include-d"
  | "quality:review"
  | "quality:override";
```

保留现有权限码，避免旧逻辑断裂。

### 4.4 推荐角色默认权限矩阵

一期先使用代码预设矩阵，不必立即做后台可编辑权限矩阵。

```text
Tester:
  trade:view
  trade:order
  trade:cancel
  trade:sell
  profile:view
  profile:update
  profile:password:change
  logs:view:self

Senior Tester:
  Tester 全部
  logs:view:team
  quality:review

Test Engineer:
  trade:view
  profile:view
  profile:update
  profile:password:change
  system:status:view
  users:list
  users:create
  users:bulk-create
  users:disable
  users:reset-password
  users:manager:set
  users:permission-level:unlock
  logs:view:managed
  logs:export:internal
  dataset:export:managed
  quality:review
  quality:override

Admin:
  所有权限
```

重要修正：

```text
Test Engineer 不应默认拥有 logs:view:all。
只有 Admin 可查看 Admin 日志和全局日志。
```

### 4.5 数据范围规则

新增核心函数：

```ts
export function getVisibleUserIdsForActor(actor: UserRecord, users: UserRecord[]): string[];
export function canAccessUser(actor: UserRecord, target: UserRecord, users: UserRecord[]): boolean;
export function assertCan(actor: UserRecord, permission: PermissionCode): void;
export function assertCanAccessUser(actor: UserRecord, targetUserId: string): void;
```

规则：

```text
Admin：可见所有用户，包括其他 Admin。
Test Engineer：可见自己 + 自己管理范围内的 Senior Tester / Tester；不可见 Admin，除非 target 是自己。
Senior Tester：可见自己 + 同组或直属 Tester；不可见 Test Engineer / Admin。
Tester：只能看自己。
```

管理范围优先使用 `managerUserId`，旧数据兼容使用 `seniorTesterId`。

### 4.6 UserRecord 字段扩展

通过 migration 新增字段：

```ts
export type PermissionLevel = "Initial" | "Standard";

export interface UserRecord {
  id: string;
  username: string;
  password: string;               // 兼容旧字段，后续可改名 passwordHash
  passwordHash?: string;           // 推荐新增
  passwordAlgo?: "plain" | "bcrypt";
  passwordChangedAt?: number;
  mustChangePassword?: boolean;
  displayName: string;
  role: Role;
  language: Language;
  permissionCodes: PermissionCode[];
  permissionLevel?: PermissionLevel;
  managerUserId?: string;
  seniorTesterId?: string;         // 旧字段保留
  availableUsdc: number;
  isActive: boolean;
  lastLoginAt?: number;
  failedLoginCount?: number;
  lockedUntil?: number;
  disabledAt?: number;
  disabledBy?: string;
  createdAt: number;
  updatedAt: number;
  dataVersion?: number;
}
```

### 4.7 密码安全

当前如仍是明文密码，必须改为 bcrypt hash。

实现要求：

```text
1. 新用户密码只存 hash，不存明文。
2. 老用户如果 passwordAlgo = plain 或 passwordHash 为空，则登录成功后自动迁移为 bcrypt。
3. 修改密码必须校验当前密码。
4. 管理员重置密码后，可设置 mustChangePassword = true。
5. 登录失败次数写入 failedLoginCount，可选 lockedUntil。
6. 密码变更写审计日志，但不得记录明文密码。
```

新增模块示例：

```ts
export async function hashPassword(raw: string): Promise<string>;
export async function verifyPassword(raw: string, user: UserRecord): Promise<boolean>;
export async function maybeUpgradePasswordHash(user: UserRecord, raw: string): Promise<UserRecord | undefined>;
```

### 4.8 个人信息管理 API

新增或确认这些接口：

```text
GET    /api/me
PATCH  /api/me/profile
POST   /api/me/password
GET    /api/me/security-events
```

`PATCH /api/me/profile` 允许普通用户修改：

```text
displayName
language
```

不允许普通用户自行修改：

```text
role
permissionCodes
permissionLevel
managerUserId
availableUsdc
isActive
```

### 4.9 Profile 前端改造

`apps/client/src/App.tsx` 中个人页必须对所有角色显示：

```text
个人信息
  用户名：只读
  显示名称：可编辑
  语言：可编辑
  角色：只读
  权限等级：只读
  上级/团队：只读

修改密码
  当前密码
  新密码
  确认新密码
  保存
```

解决当前 Tester 找不到修改密码入口的问题。

### 4.10 JWT、CORS、限流

Codex 需要核验当前 `apps/server/src/config.ts` 是否已有生产 JWT_SECRET 默认值阻断。如果已有，只补测试；如果没有，添加。

生产安全要求：

```text
1. NODE_ENV=production 或 DEPLOY_ENV=production 时，JWT_SECRET 不允许默认值。
2. CORS 使用白名单，不允许生产环境 *。
3. 登录接口加限流。
4. 重置密码、创建用户、导出接口加限流。
5. JWT 过期时间、刷新策略写入配置。
6. WebSocket 不应长期使用 URL query token；至少加入短期 WS ticket 或连接后 auth 消息。
```

### 4.11 阶段 2 验收标准

必须通过：

```bash
npm run typecheck
npm run test:config
npm run test:permissions
npm run build
```

新增测试：

```text
1. 旧明文用户登录成功后自动迁移 bcrypt。
2. 新用户创建后 passwordHash 非空且 password 不可包含明文。
3. Tester 可以在 Profile 修改密码。
4. Tester 不能访问用户管理 API。
5. Senior Tester 只能看自己团队。
6. Test Engineer 不能查看 Admin 日志。
7. Admin 可以查看全局日志。
8. 直接调用越权 API 返回 403。
9. 越权尝试写 audit log。
10. 生产默认 JWT_SECRET 启动失败。
```

---

## 5. 阶段 3：用户管理功能补全与 Excel 批量创建

### 5.1 目标

补齐用户系统管理功能：

```text
组织关系
权限等级
解锁/降级
批量创建
换组/解绑
用户状态管理
用户操作审计
```

### 5.2 managerUserId 与 seniorTesterId 兼容

新增 `managerUserId`，但保留 `seniorTesterId`。

兼容规则：

```text
1. 新代码优先读取 managerUserId。
2. 如果 managerUserId 为空，且 seniorTesterId 有值，则把 seniorTesterId 当作旧上级关系。
3. migration 中可回填 managerUserId = seniorTesterId。
4. 创建/编辑用户时双写一段时间：managerUserId 与 seniorTesterId。
5. 未来多个版本后再考虑废弃 seniorTesterId，本轮禁止删除。
```

### 5.3 组织关系规则

```text
Admin：可创建/管理任意非自身或所有角色，具体以业务配置为准。
Test Engineer：可管理自己范围内的 Senior Tester / Tester，不可管理 Admin。
Senior Tester：原则上无配置权限；可查看同组 Tester，可做质量初审。
Tester：无管理权限。
```

### 5.4 权限等级 Initial / Standard

新增：

```ts
export type PermissionLevel = "Initial" | "Standard";
```

默认：

```text
新账号 permissionLevel = Initial
```

业务规则：

```text
Initial：
  仅可使用 STD / CON 策略簇
  仓位上限为标准值 50%
  禁用 ADV / DC / HV 策略簇
  禁用反向对冲/买 DOWN 操作，若业务最终确认需要则启用

Standard：
  完整交易权限
  标准仓位上限
```

注意：如果当前项目还没有策略簇和 Initial 限制对应 UI/字段，先实现用户等级、审计和 API 限制，策略簇限制可预留接口。

### 5.5 权限解锁/降级接口

新增：

```text
POST /api/users/:userId/permission-level
```

请求体：

```json
{
  "permissionLevel": "Standard",
  "reason": "已完成培训并通过考核"
}
```

规则：

```text
Test Engineer：只能把自己管理范围内的 Initial 提升为 Standard。
Admin：可以提升和降级。
降级 Standard -> Initial 只有 Admin 可以执行。
所有变更必须填写 reason，长度 1-100。
所有变更必须写 audit log。
```

### 5.6 用户换组/解绑

新增接口：

```text
PATCH /api/users/:userId/manager
```

请求体：

```json
{
  "managerUserId": "usr_xxx",
  "reason": "团队调整"
}
```

规则：

```text
1. 不允许形成循环管理关系。
2. 不允许把 Admin 设为普通 Test Engineer 的下级。
3. Test Engineer 只能调整自己管理范围内的人。
4. Admin 可全局调整。
5. 所有变更写 audit log。
```

### 5.7 Excel 批量创建

当前已有 CSV/TSV 批量创建，需增强为同时支持 `.xlsx`。

新增：

```text
GET  /api/users/bulk-template.xlsx
POST /api/users/bulk-preview
POST /api/users/bulk-create
```

模板字段建议：

```text
username
displayName
role
initialPassword
managerUsername
permissionLevel
language
initialBalance
note
```

要求：

```text
1. 支持 .csv / .tsv / .xlsx。
2. 后端用 SheetJS/xlsx 解析 Excel。
3. 前端提供“下载 Excel 模板”。
4. 上传后先预览，不直接创建。
5. 返回每一行校验结果。
6. 支持重复用户名检测。
7. 支持 managerUsername 解析成 managerUserId。
8. 允许跳过错误行或要求全部无错误才能提交，以业务配置为准。
9. 创建成功写 audit log。
```

### 5.8 阶段 3 验收标准

```bash
npm run typecheck
npm run test:permissions
npm run test:bulk-users
npm run build
```

新增测试：

```text
1. Excel 模板可下载。
2. Excel 上传可解析。
3. CSV/TSV 旧能力不受影响。
4. 错误行返回明确错误。
5. 重复用户名被阻止。
6. managerUsername 可解析。
7. Test Engineer 只能创建/管理自己范围内用户。
8. 权限解锁必须填写理由。
9. Test Engineer 不能降级用户。
10. Admin 可以降级。
11. 解锁、降级、换组、解绑均有 audit log。
```

---

## 6. 阶段 4：盈亏 PnL 与手续费口径标准化

### 6.1 本阶段核心问题

用户明确询问：

```text
现在的盈亏以及前端显示盈亏的是否包含买单手续费？
```

当前源码显示：

```text
OrderRecord 已有 estimatedFee / actualFee / feeBreakdown。
clob-execution.ts 已计算 fee。
simulation.ts 买入时会用 matchedNotional + actualFee 更新 position.notionalSpent。
simulation.ts 卖出时会从 proceeds 扣除 actualFee。
前端 compact position 显示 realizedPnl/unrealizedPnl，但没有明确标注“已含手续费”。
```

因此，本阶段不是简单“加手续费”，而是要完成：

```text
1. 核验当前真实公式。
2. 定义统一 PnL 口径。
3. 避免重复扣费。
4. 给 Position 增加显式费用字段。
5. 前端明确展示是否含手续费。
6. 同时展示 mid mark PnL 与 best bid executable PnL。
7. 导出字段也带上费用口径。
```

### 6.2 费用模型定义

新增配置：

```text
PAPER_TRADE_FEE_MODEL=clob_formula|embedded_in_price|explicit_bps|none
PAPER_TRADE_BUY_FEE_BPS=0
PAPER_TRADE_SELL_FEE_BPS=0
```

含义：

```text
clob_formula：使用 Polymarket/CLOB marketInfo 里的 feeRate，并采用当前 calculateClobFees 公式。
embedded_in_price：不额外计算显式手续费，认为成本已隐含在真实 ask/bid/双边成本中，explicit fee = 0。
explicit_bps：按配置 bps 手动模拟手续费。
none：不计算任何手续费。
```

默认推荐：

```text
如果 CLOB marketInfo.feeRateAvailable !== false 且 feeRate > 0：使用 clob_formula。
否则使用 embedded_in_price，fee = 0，避免凭空模拟错误费用。
```

注意：不要同时把 CLOB ask/bid overround 和 explicit fee 重复当作两笔手续费。Codex 必须在文档和 UI tooltip 中说明费率来源。

### 6.3 OrderRecord 字段标准化

保留已有：

```ts
estimatedFee?: number;
actualFee?: number;
feeBreakdown?: FeeBreakdown;
feeCurrency?: FeeCurrency;
notionalUsdc: number;
avgFillPrice?: number;
```

新增或规范命名：

```ts
feeModel?: "clob_formula" | "embedded_in_price" | "explicit_bps" | "none";
grossNotionalUsdc?: number;   // 成交额，不含手续费
feeUsdc?: number;             // 当前订单实际手续费；与 actualFee 保持兼容
feeBps?: number;
netNotionalUsdc?: number;     // 买入时 gross + fee；卖出时 gross - fee
```

兼容规则：

```text
feeUsdc = actualFee ?? estimatedFee ?? 0
actualFee 保留
estimatedFee 保留
```

### 6.4 PositionRecord 字段标准化

保留已有：

```ts
notionalSpent: number;     // 当前持仓剩余成本
averageEntry: number;      // 当前实现下通常已包含买入 fee 分摊
unrealizedPnl: number;
realizedPnl: number;
currentMark: number;
currentBid?: number;
currentAsk?: number;
currentMid?: number;
currentValue?: number;
```

新增：

```ts
entryFeeUsdc?: number;         // 已分摊到当前/历史持仓的买入手续费
exitFeeUsdc?: number;          // 已发生卖出手续费
totalFeeUsdc?: number;         // entry + exit
costBasisUsdc?: number;        // 当前剩余成本，推荐等同 notionalSpent，但语义更清楚
markValueUsdc?: number;        // qty * markPrice
executableValueUsdc?: number;  // qty * bestBid，表示立即平仓估值
markPnl?: number;              // markValue - costBasis
executablePnl?: number;        // executableValue - estimatedExitFee - costBasis
netRealizedPnl?: number;       // 已实现净盈亏
netUnrealizedPnl?: number;     // 当前浮动净盈亏
feeModel?: string;
dataVersion?: number;
```

### 6.5 推荐公式

买入成交：

```text
grossEntryNotional = estimate.matchedNotional
entryFee = calculateFee(...)
entryCostBasis = grossEntryNotional + entryFee
user.availableUsdc -= entryCostBasis
position.notionalSpent += entryCostBasis
position.costBasisUsdc = position.notionalSpent
position.averageEntry = position.costBasisUsdc / position.qty
```

卖出成交：

```text
grossExitProceeds = estimate.matchedNotional
exitFee = calculateFee(...)
netExitProceeds = grossExitProceeds - exitFee
releasedCostBasis = position.averageEntry * soldQty
realizedPnl = netExitProceeds - releasedCostBasis
user.availableUsdc += netExitProceeds
position.notionalSpent -= releasedCostBasis
position.exitFeeUsdc += exitFee
position.netRealizedPnl += realizedPnl
```

Redeem：

```text
redeemAmount = winningQty * 1.00
releasedCostBasis = position.notionalSpent
realizedPnl = redeemAmount - releasedCostBasis
user.availableUsdc += redeemAmount
position.notionalSpent = 0
```

浮动盈亏：

```text
markValueUsdc = qty * markPrice
markPnl = markValueUsdc - costBasisUsdc
```

可成交盈亏：

```text
executableValueUsdc = qty * bestBid
estimatedExitFee = calculateFeeForSell(executableValueUsdc)
executablePnl = executableValueUsdc - estimatedExitFee - costBasisUsdc
```

### 6.6 前端展示要求

前端持仓摘要仍可保留简洁形式，但必须补充明确口径：

```text
持仓摘要：
▲ UP  $50.00  +$1.23 净浮盈  [持仓中]

展开详情：
份额：94.12
成交均价：0.5312
买入成交额：$49.82
买入手续费：$0.18
成本合计：$50.00
中间价浮盈：+$1.23
立即平仓盈亏：+$0.86
费用模型：CLOB fee formula / embedded in price
```

顶部总资产区：

```text
总资产 = 可用余额 + Σ 持仓 markValue
浮动盈亏 = Σ markPnl
可平仓盈亏 = Σ executablePnl
```

如果空间有限，至少加 tooltip：

```text
当前浮动盈亏按中间价/展示价估算，已包含买入成本和买入手续费；立即平仓盈亏按 best bid 估算，可能更接近真实退出结果。
```

### 6.7 前端字段兼容

`apps/client/src/utils/api.ts` 中同步新增字段：

```ts
feeModel?: string;
entryFeeUsdc?: number;
exitFeeUsdc?: number;
totalFeeUsdc?: number;
costBasisUsdc?: number;
markValueUsdc?: number;
executableValueUsdc?: number;
markPnl?: number;
executablePnl?: number;
netRealizedPnl?: number;
netUnrealizedPnl?: number;
```

如果后端暂时没返回，前端 fallback：

```ts
const costBasis = position.costBasisUsdc ?? position.notionalSpent;
const markPnl = position.markPnl ?? position.unrealizedPnl;
const realized = position.netRealizedPnl ?? position.realizedPnl;
```

### 6.8 导出字段同步

内部日志导出和客户数据导出均应加入：

```text
fee_model
gross_notional_usdc
fee_usdc
fee_bps
net_notional_usdc
entry_fee_usdc
exit_fee_usdc
total_fee_usdc
cost_basis_usdc
mark_value_usdc
executable_value_usdc
mark_pnl
executable_pnl
net_realized_pnl
net_unrealized_pnl
pnl_price_source
```

### 6.9 阶段 4 验收标准

```bash
npm run typecheck
npm run test:trading
npm run test:export
npm run build
```

新增测试：

```text
1. feeModel=clob_formula 时，买入实际扣款 = matchedNotional + actualFee。
2. 买入后 position.notionalSpent 包含 actualFee。
3. averageEntry 与 costBasisUsdc / qty 一致。
4. 卖出 realizedPnl = grossProceeds - exitFee - releasedCostBasis。
5. redeem realizedPnl = redeemAmount - remainingCostBasis。
6. feeModel=embedded_in_price 时 explicit fee = 0，不能重复扣费。
7. markPnl 使用 mid/display mark。
8. executablePnl 使用 bestBid 并预估退出手续费。
9. 前端持仓详情显示买入手续费、成本合计、费用模型。
10. 前端 PnL 文案明确“是否含手续费”。
11. 导出文件包含费用与 PnL 口径字段。
```

---

## 7. 阶段 5：客户数据集导出、匿名化、格式与审计

### 7.1 目标

当前 `/api/logs/export` 更适合作为内部日志 ZIP 导出。客户交付数据集必须独立，不得泄露真实用户身份。

新增：

```text
/api/datasets/export
```

保留：

```text
/api/logs/export
```

### 7.2 导出权限

```text
Tester：不能导出客户数据集。
Senior Tester：默认不能导出客户数据集。
Test Engineer：只能导出自己管理范围内数据。
Admin：可以导出全局数据。
includeD：只有 Admin 可以启用。
```

权限码：

```text
dataset:export:managed
dataset:export:all
dataset:export:include-d
```

所有导出必须调用：

```ts
assertCanExportDataset(actor, filters)
getVisibleUserIdsForActor(actor)
```

### 7.3 匿名化规则

客户数据集中禁止出现：

```text
真实 user_id
username
displayName
email，如未来存在
真实 trace 到用户的内部标识
```

使用稳定匿名 ID：

```text
tester_anon_id = HMAC_SHA256(EXPORT_ANONYMIZATION_SECRET, userId).slice(0, 16)
```

同一客户/同一批次中匿名 ID 应稳定，便于客户分析；不同部署或不同密钥不可反推真实用户。

### 7.4 导出必填字段

客户数据集至少包含：

```text
timestamp_ms
symbol
asset_class
round_id
market_id
side
entry_odds
entry_price_source
entry_delta
entry_volume
position_amount_usdc
exit_type
exit_odds
settlement_result
settlement_direction
settlement_time_ms
gamma_poll_count
redeem_completed_at_ms
tester_anon_id
strategy_cluster_label
market_regime_label
quality_grade
order_book_snapshot_ref
actual_fill_price
slippage_bps
partial_filled
unfilled_qty
match_latency_ms
book_acquire_latency_ms
total_order_latency_ms
fee_model
entry_fee_usdc
exit_fee_usdc
total_fee_usdc
cost_basis_usdc
net_realized_pnl
net_unrealized_pnl
```

### 7.5 D 级过滤

规则：

```text
quality_grade = D 的记录默认不导出。
Admin 可以 includeD=true。
includeD=true 时，导出行必须带 is_filtered_or_low_quality 标记。
Test Engineer 不可 includeD。
```

如果当前 quality_grade 规则未完成，不允许静默把所有记录标为 A。应使用：

```text
quality_grade = C 或 UNKNOWN
quality_grade_source = "pending_rules"
```

并在 manifest 中说明。

### 7.6 导出格式

第一版必须支持：

```text
CSV
JSON 或 JSONL
manifest.json
schema.json
sha256 校验
```

Parquet 可以第二版实现。如果未实现，接口应明确返回：

```text
501 Not Implemented
```

不要假装支持 Parquet。

### 7.7 导出包结构

建议 ZIP：

```text
dataset_export_20260507_120000.zip
  manifest.json
  schema.json
  trades.csv
  trades.jsonl
  quality_summary.csv
  export_audit.json
  checksums.sha256
```

### 7.8 export audit log

每次导出必须记录：

```text
export_id
exported_by_user_id
exported_by_role
requested_filters
resolved_user_scope
format
include_d
record_count
filtered_d_count
unknown_quality_count
started_at
completed_at
duration_ms
file_sha256
status
failure_reason
```

客户导出 audit log 不能写入客户包里的真实 user_id；内部 audit log 可以保存真实操作者。

### 7.9 阶段 5 验收标准

```bash
npm run typecheck
npm run test:export
npm run test:permissions
npm run build
```

新增测试：

```text
1. Tester 访问 /api/datasets/export 返回 403。
2. Senior Tester 默认返回 403。
3. Test Engineer 只能导出 managed 用户数据。
4. Test Engineer 不能 includeD。
5. Admin 可以 includeD。
6. 导出文件不包含真实 user_id、username、displayName。
7. 同一 userId 在同一密钥下匿名 ID 稳定。
8. D 级默认过滤。
9. manifest 记录范围、数量、过滤数量。
10. schema.json 与 CSV/JSON 字段一致。
11. 导出失败也写 audit log。
12. CSV 字段对逗号、引号、换行正确转义。
```

---

## 8. 阶段 6：C/S 长期部署稳定性、性能与容错机制

### 8.1 目标

项目需要在 C/S 形态下长期部署，并支持数十人同时使用。目标不是立刻多实例扩容，而是先把单实例生产环境做稳：

```text
关键交易操作事务化
WebSocket 不高频全量大包
JSONL 不阻塞 event loop
上游 API 故障可降级
服务有健康检查和监控
数据库可备份恢复
客户端断线可恢复
导出和日志查询不拖垮交易
```

### 8.2 推荐生产拓扑

```text
Electron Client(s)
  ↓ HTTPS / WSS
Nginx or Caddy TLS Reverse Proxy
  ↓ internal HTTP / WS
Fastify app-server
  ↓
PostgreSQL
Redis
JSONL rotated logs
External APIs: Polymarket / Binance / Chainlink / Gamma
```

要求：

```text
1. 不直接暴露 PostgreSQL、Redis、matching-service。
2. 只暴露 HTTPS/WSS 入口。
3. docker-compose.deploy.yml 中使用内网服务名通信。
4. PG/Redis 使用强密码和持久化 volume。
5. 生产使用 Docker secrets 或安全 env 文件。
```

### 8.3 生产配置强校验

`apps/server/src/config.ts` 增加或核验：

```text
JWT_SECRET 生产非默认
DATABASE_URL 生产必填
REDIS_URL 生产必填
CORS_ORIGINS 生产必填
EXPORT_ANONYMIZATION_SECRET 生产必填
SERVER_STRICT_PERSISTENCE=true
PERSISTENCE_MODE=external
LOG_DIR 可写
TLS 由反代处理时，服务知道 TRUST_PROXY
```

如果生产配置不合格，服务启动失败。

### 8.4 HTTP API 稳定性

新增或增强：

```text
1. 全局 request id / trace id。
2. 请求超时。
3. 登录、下单、导出、批量创建限流。
4. 统一错误响应格式。
5. API 分页：users/orders/logs/audit/export list。
6. 大导出使用流式生成或后台任务，避免一次性拉全量进内存。
7. 幂等键：订单提交、redeem、手动结算、导出请求可选。
```

统一错误格式：

```json
{
  "error": {
    "code": "PERMISSION_DENIED",
    "message": "Permission denied.",
    "traceId": "trace_xxx"
  }
}
```

### 8.5 交易和结算事务化

关键写入必须使用 PostgreSQL transaction：

```text
买入：order + position + user balance + lifecycle + audit
卖出：order + position + user balance + lifecycle + audit
redeem：round settlement + position close + user balance + audit
用户权限变更：user + audit
导出：export_audit + file manifest
```

建议新增 repository 层：

```text
apps/server/src/repositories/users.repository.ts
apps/server/src/repositories/orders.repository.ts
apps/server/src/repositories/positions.repository.ts
apps/server/src/repositories/rounds.repository.ts
apps/server/src/repositories/logs.repository.ts
```

并支持：

```ts
await withTransaction(pool, async (tx) => {
  await usersRepo.updateBalance(tx, ...);
  await ordersRepo.insert(tx, ...);
  await positionsRepo.upsert(tx, ...);
});
```

幂等要求：

```text
1. redeem 同一个 position/round 不能重复加余额。
2. 手动结算同一个 round 重复提交不能反复结算。
3. order traceId 或 idempotencyKey 重复提交不能生成重复订单。
4. export_id 重复不能重复写互相冲突的审计记录。
```

### 8.6 PostgreSQL 性能

为高频查询加索引，具体以实际表名为准：

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_user_created_at ON orders(user_id, created_at DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_round_id ON orders(round_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_positions_user_status ON positions(user_id, status);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_positions_round_side ON positions(round_id, side);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_audit_logs_user_ts ON audit_logs(user_id, server_recv_ts DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_audit_logs_round_ts ON audit_logs(round_id, server_recv_ts DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_behavior_logs_user_ts ON behavior_logs(user_id, server_recv_ts DESC);
```

注意：在 node-pg-migrate 事务中使用 `CREATE INDEX CONCURRENTLY` 需要禁用 migration transaction 或单独 SQL 文件处理。

### 8.7 WebSocket 稳定性

当前 market/user WS 如果全量高频推送，会在几十人同时在线时造成压力。

目标：

```text
1. market WS 使用 coalescing：例如 100-250ms 合并一次。
2. user WS 只推送该用户相关 diff。
3. 大字段如 orderBookSnapshot 不要每 tick 全量推给所有人。
4. WS 增加 heartbeat/ping/pong。
5. 连接断开后客户端自动重连并请求 init snapshot。
6. 每条 WS payload 带 seq 和 serverPublishTs。
7. 客户端丢 seq 时主动请求 /api/state/init 或相关快照。
```

建议 payload 拆分：

```text
market:summary
market:book-lite
market:health
round:update
user:profile
user:orders-diff
user:positions-diff
user:notifications
```

### 8.8 JSONL 日志异步队列与轮转

如果当前存在 `appendFileSync` / `writeFileSync`，需要改造为异步队列：

```text
1. 内存队列收集日志。
2. 定时 batch flush。
3. 服务 graceful shutdown 时 flush。
4. 文件按日期/大小轮转。
5. 写入失败告警，但不能悄悄吞掉。
6. 审计日志以 PostgreSQL 为主，JSONL 为不可变备份。
```

新增：

```text
apps/server/src/services/log-writer.ts
apps/server/src/services/log-rotation.ts
```

### 8.9 上游 API 容错和降级

外部源：

```text
Polymarket CLOB
Polymarket Gamma
Binance
Chainlink
```

要求：

```text
1. 所有上游请求有 timeout。
2. 所有上游重连使用 exponential backoff + jitter。
3. 上游错误进入 health state，不要无限刷日志。
4. CLOB stale 超过阈值时禁止新下单，UI 显示“盘口过期”。
5. Binance stale 时图表降级，但不应影响已有持仓查询。
6. Chainlink stale 时显示预言机延迟，不要误写结算。
7. Gamma 超时进入 Manual 状态，并提供人工录入出口。
8. Redis 宕机时如果只作为缓存，不应破坏 PostgreSQL 主写；但如果需要严格缓存一致，应降级提示。
9. PostgreSQL 宕机时，严格模式下禁止交易写入。
```

### 8.10 健康检查和 metrics

新增接口：

```text
GET /api/health/live
GET /api/health/ready
GET /api/metrics
```

live：进程活着即可。

ready：必须检查：

```text
PostgreSQL 可查询
Redis 可连接，若为必需
schema 版本正确
外部源状态，不一定要求全部 healthy，但要返回 degraded
日志目录可写
内存未超过保护阈值
```

metrics 至少输出 JSON：

```json
{
  "uptimeSec": 12345,
  "memoryMb": 512,
  "wsClients": 18,
  "marketPayloadPerSec": 8,
  "ordersPerMin": 12,
  "orderLatencyP95Ms": 72,
  "dbQueryP95Ms": 18,
  "jsonlQueueDepth": 42,
  "sourceHealth": {
    "clob": "healthy",
    "binance": "healthy",
    "chainlink": "degraded",
    "gamma": "healthy"
  }
}
```

### 8.11 graceful shutdown

服务收到 SIGTERM/SIGINT：

```text
1. 标记 server shuttingDown。
2. readiness 返回 false。
3. 停止接受新订单和新导出。
4. 通知 WS 客户端 server_shutdown。
5. flush JSONL 队列。
6. 完成或回滚进行中的 transaction。
7. 关闭 PG/Redis/WS。
8. 退出进程。
```

### 8.12 Docker/反代部署

`docker-compose.deploy.yml` 改造方向：

```text
1. 增加 reverse-proxy 服务，使用 Caddy 或 Nginx。
2. app-server 只暴露内网端口。
3. matching-service 如仍使用，只暴露内网端口。
4. PostgreSQL/Redis 不映射公网端口。
5. 使用 volumes 持久化 pgdata、redisdata、logs、backups。
6. 使用 restart: unless-stopped。
7. 加 healthcheck。
8. 加 resource limits，至少文档说明。
9. 加 backup service 或 cron 说明。
```

### 8.13 阶段 6 验收标准

新增脚本建议：

```text
scripts/deployment-readiness-check.ts
scripts/ws-load-check.ts
scripts/fault-injection-check.ts
scripts/export-load-check.ts
```

命令：

```bash
npm run typecheck
npm run test:config
npm run test:trading
npm run test:regression
npm run build
```

验收：

```text
1. 30 个模拟客户端同时连接 30 分钟，WS 不断连，服务内存不持续上涨。
2. 5 个用户同时下单，订单延迟 P95 < 200ms，目标 < 100ms。
3. CLOB stale 后新下单被禁止并提示。
4. PostgreSQL 临时断开时，严格模式禁止交易写入，不产生半写数据。
5. Redis 断开时系统按设计降级。
6. Gamma 超时进入 Manual，并能人工恢复。
7. 导出 10 万行数据时，不阻塞下单和 WS 心跳。
8. JSONL 队列积压可观测，shutdown 可 flush。
9. Docker 生产环境不暴露 PG/Redis。
10. readiness 在 schema 未迁移时返回失败。
```

---

## 9. 阶段 7：测试体系与回归脚本

### 9.1 必须保留并运行的现有脚本

当前 package.json 已有：

```bash
npm run typecheck
npm run test:config
npm run test:permissions
npm run test:export
npm run test:logs
npm run test:bulk-users
npm run test:trading
npm run test:regression
npm run build
```

Codex 每阶段修改后至少运行与修改相关的测试；最终合并前运行全部。

### 9.2 新增测试脚本

建议新增：

```text
npm run test:migrations
npm run test:pnl-fees
npm run test:deployment
npm run test:ws-load
npm run test:fault-tolerance
```

如果不修改 package.json，也要在 scripts/ 下新增对应脚本并在文档中说明运行方法。

### 9.3 回归测试矩阵

每次发布前必须覆盖：

```text
登录：旧密码、新 hash、错误密码、停用用户
权限：四角色接口访问矩阵
个人信息：修改 displayName/language/password
用户管理：创建、批量创建、停用、重置密码、换组、权限解锁/降级
交易：买入、撤单、卖出、limit pending、limit fill
PnL：买入手续费、卖出手续费、redeem、mark/executable PnL
结算：UP/DOWN win/loss、Manual、redeem 幂等
日志：查询范围、轮次筛选、汉化、tooltip 字段
导出：内部导出、客户导出、匿名化、D 级过滤、schema/manifest
部署：health、ready、metrics、graceful shutdown
性能：多客户端 WS、导出大数据、订单延迟
容错：CLOB/Gamma/Binance/Chainlink/PG/Redis 异常
```

---

## 10. 阶段 8：安全代码整理与冗余清理

### 10.1 原则

冗余清理必须放在测试覆盖之后。做不到确认安全，就先不删。

```text
先抽取模块
先保留兼容入口
先写测试
后删除死代码
```

### 10.2 建议安全抽取

可以优先做：

```text
1. 把 App.tsx 中的 UserManagementPage 抽到 apps/client/src/features/users/UserManagementPage.tsx。
2. 把批量创建弹窗抽到 apps/client/src/features/users/BulkUserDialog.tsx。
3. 把 Profile 修改密码抽到 apps/client/src/features/profile/ProfileSecurityPanel.tsx。
4. 把日志搜索页抽到 apps/client/src/features/logs/LogSearchPage.tsx。
5. 把导出弹窗抽到 apps/client/src/features/export/DatasetExportDialog.tsx。
6. 把权限判断从 store.ts/index.ts 抽到 auth/permissions.ts、auth/scope.ts。
7. 把 bulk-users.ts 的 CSV/TSV/XLSX 解析拆为 parsers。
8. 把 csv-zip-export.ts 拆为 internal-log-export.ts 与 customer-dataset-export.ts。
```

### 10.3 暂时不要删除

```text
seniorTesterId
旧 permissionCodes
/api/logs/export
CSV/TSV 批量创建
已有 JSONL 日志
旧 realizedPnl/unrealizedPnl/notionalSpent/averageEntry
旧导出字段
旧前端 API 类型，直到新旧字段都有兼容
```

### 10.4 可删除条件

只有满足以下条件才删除：

```text
1. grep 确认没有引用。
2. 类型检查通过。
3. 对应回归测试通过。
4. 已有新接口替代旧接口。
5. 文档中说明删除原因。
6. 不影响历史数据 mapper。
```

---

## 11. Codex 最终执行任务卡

### 任务卡 A：现状核验

```text
目标：生成 docs/codex-current-state-audit.md。
动作：阅读代码，核验 migration、权限、用户、PnL、导出、部署稳定性现状。
禁止：不要修改业务代码。
验收：输出完整核验表和调整建议。
```

### 任务卡 B：数据库 migration 与备份

```text
目标：建立后续版本可升级、可回滚、可兼容旧数据的数据库机制。
修改：db/migrations、scripts/db-backup.sh、scripts/db-restore.sh、schema-check。
验收：空库/旧库可迁移，启动能检查 schema，备份可生成。
```

### 任务卡 C：统一权限与个人信息

```text
目标：后端统一 RBAC 和 scope，修复 Test Engineer 越权，Profile 支持所有角色修改密码。
修改：auth/*、index.ts、store.ts、App.tsx、api.ts。
验收：四角色权限矩阵测试通过，越权返回 403 并写日志。
```

### 任务卡 D：用户管理增强

```text
目标：managerUserId、permissionLevel、解锁/降级、Excel 批量创建。
修改：types.ts、migrations、users API、bulk-users、UserManagement UI。
验收：Excel 预览/创建、解锁/降级/换组全部有审计。
```

### 任务卡 E：PnL/手续费口径

```text
目标：核验并标准化手续费是否计入 PnL，前端明确展示。
修改：clob-fees、clob-execution、simulation、types、store、api.ts、App.tsx、export。
验收：买入费计入成本，卖出费扣收益，redeem 按成本，前端 tooltip 清楚。
```

### 任务卡 F：客户数据集导出

```text
目标：新增客户导出接口，匿名化、格式、D 级过滤、导出审计。
修改：customer-dataset-export.ts、index.ts、permissions、schema、manifest、UI。
验收：不同角色范围正确，导出无真实身份字段，D 级默认过滤。
```

### 任务卡 G：部署稳定性/性能/容错

```text
目标：让系统支持数十人长期 C/S 使用。
修改：config、ws、log-writer、health、metrics、transactions、docker-compose.deploy.yml。
验收：30 客户端浸泡、故障注入、导出压力、订单延迟测试通过。
```

### 任务卡 H：安全整理

```text
目标：降低维护成本，不破坏现有功能。
修改：前端组件拆分、权限模块抽取、导出模块拆分。
验收：不删除核心兼容字段，全部测试通过。
```

---

## 12. 最终上线前检查清单

上线前必须确认：

```text
[ ] npm ci 成功
[ ] npm run typecheck 成功
[ ] npm run test:config 成功
[ ] npm run test:permissions 成功
[ ] npm run test:bulk-users 成功
[ ] npm run test:logs 成功
[ ] npm run test:export 成功
[ ] npm run test:trading 成功
[ ] npm run test:regression 成功
[ ] npm run build 成功
[ ] npm run db:backup 成功
[ ] npm run db:migrate 成功
[ ] 旧数据迁移测试成功
[ ] Test Engineer 不能查看 Admin 日志
[ ] Tester 可以在 Profile 修改密码
[ ] 客户导出不含真实身份字段
[ ] 客户导出默认过滤 D 级
[ ] PnL 页面明确显示手续费口径
[ ] redeem 幂等
[ ] 下单/卖出/结算事务化
[ ] JSONL 异步写入和轮转可用
[ ] health/live、health/ready、metrics 可用
[ ] Docker 生产部署不暴露 PG/Redis
[ ] 30 客户端浸泡测试通过
[ ] 备份文件可恢复
```

---

## 13. 给 Codex 的一句话执行指令

```text
请先阅读本文件，并严格按阶段执行。先做现状核验，再做数据库 migration/备份，再做统一权限和个人信息管理，然后做用户管理增强、PnL/手续费口径、客户数据导出，最后做部署稳定性/性能/容错和安全整理。所有新增数据库字段必须通过 migration，所有权限必须后端校验，所有旧字段必须兼容读取，不允许删除现有接口或破坏现有交易功能。每阶段完成后运行对应测试并在 docs/codex-current-state-audit.md 或 docs/codex-implementation-notes.md 中记录修改内容、风险和验收结果。
```

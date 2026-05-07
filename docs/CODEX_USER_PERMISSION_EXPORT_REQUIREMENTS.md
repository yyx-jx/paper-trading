# Hyper Terminal 用户身份/权限管理与数据导出功能差距分析及 Codex 实施方案

生成目的：给 Codex 直接阅读并修改当前项目。  
范围限定：本文件只覆盖应用层新增/修正功能，重点为 **用户系统身份与权限管理**、**用户/客户数据导出内容与格式**、以及不影响现有功能的冗余代码整理建议。  
源码基准：`/mnt/data/P_T_git` 解压/恢复后的当前代码。  
需求依据：`HT 系统需求说明书(2).docx`、`12_hyper_test_analysis_ui_spec.docx`、`08_test_execution_sheet_v3填写完成.xlsx`。

---

## 0. 总结结论

当前项目已经实现了以下基础能力：

1. 四级角色类型：`Tester`、`Senior Tester`、`Test Engineer`、`Admin`。
2. 基础权限码：交易、用户列表、用户创建、批量创建、停用、重置密码、设置余额、查看日志等。
3. 登录、JWT、用户列表、创建用户、停用/启用、重置密码、设置余额。
4. `/api/me/password` 后端接口已经存在。
5. 日志搜索与日志 ZIP 导出已经存在。
6. 导出数据中已经包含部分撮合/订单簿/滑点/延迟字段。
7. 批量创建用户已有 CSV/TSV 文本解析能力。

但距离需求文档和 Excel 测试结果仍有明显差距，最重要的是：

1. **Test Engineer 权限范围错误**：当前后端允许 Test Engineer 查看全局日志和全局用户，已触发 TC-E05。
2. **用户上下级关系过窄**：当前只有 `seniorTesterId`，且只能 Tester 绑定 Senior Tester；无法支持 Test Engineer 管理组、Senior 绑定上级、用户换组/解绑。
3. **RBAC 没有集中化和可配置化**：权限分散在 `store.ts`、`index.ts`、`App.tsx`，没有统一 policy 模块。
4. **Tester 无法在个人页修改密码**：后端接口有，但前端入口放在用户管理页，Tester 看不到，已触发 TC-A08。
5. **密码仍为明文存储/明文比较**：这不是本轮“功能”核心，但用户系统上线前必须修。
6. **测试员权限分级解锁未实现**：需求中的 Initial / Standard、解锁理由、降级限制、审计日志都缺失。
7. **批量创建不支持 Excel 模板/Excel 上传**：已触发 TC-F02。
8. **审计日志 UI 仍有不可读技术字段**：动作分类未汉化、轮次输入不是下拉、traceId/orderId 含义不清，已触发 TC-E08。
9. **客户数据集导出未独立于内部调试导出**：当前 `/api/logs/export` 更像内部 ZIP 包，包含真实 `user_id`、`username`、`display_name` 等，不适合作为客户交付数据集。
10. **导出格式不完整**：当前主要是 ZIP + CSV；需求要求 CSV、JSON、Parquet。
11. **导出默认过滤 D 级记录未实现**：`quality_grade` 等字段存在但填充规则和导出过滤都未完整实现。
12. **导出审计日志未完整实现**：缺少每次导出的导出人、范围、记录数、过滤数、格式、文件校验等留痕。

建议 Codex 按下面的优先级执行：

```text
P0：修正权限范围、统一权限策略、修复 Tester 修改密码入口、修复客户导出匿名化与权限控制。
P1：实现用户组织关系、测试员权限分级解锁、Excel 批量创建、导出格式扩展、导出审计日志。
P2：实现后台角色-功能矩阵预留、Parquet 大批量导出、更多 UI 文案和冗余代码拆分。
```

---

## 1. 当前源码关键发现

### 1.1 类型与角色

文件：`apps/server/src/domain/types.ts`

当前定义：

```ts
export type Role = "Tester" | "Senior Tester" | "Test Engineer" | "Admin";
```

当前 `PermissionCode` 只有：

```ts
trade:view
trade:order
trade:cancel
trade:sell
profile:view
system:status:view
audit:view
users:list
users:create
users:bulk-create
users:disable
users:reset-password
users:balance:set
logs:view:all
logs:view:team
```

问题：权限码不足，不能表达：

```text
导出客户数据集
导出内部审计日志
包含 D 级数据
修改角色
修改上下级关系
测试员权限解锁
质量审核
查看管理范围内数据
查看全局数据
```

当前 `UserRecord` 字段：

```ts
id
username
password
displayName
role
language
permissionCodes
availableUsdc
isActive
seniorTesterId
disabledAt
disabledBy
createdAt
updatedAt
```

问题：

1. `password` 为明文字段。
2. 只有 `seniorTesterId`，没有通用 `managerUserId` / `parentUserId`。
3. 没有 `permissionLevel`，无法实现 Initial / Standard。
4. 没有登录失败锁定字段。
5. 没有 `passwordChangedAt` / `lastLoginAt` / `failedLoginCount`。

---

### 1.2 权限矩阵

文件：`apps/server/src/services/store.ts`

当前 `ROLE_PERMISSIONS`：

```ts
Tester:
  trade:view, trade:order, trade:cancel, trade:sell, profile:view

Senior Tester:
  trade:view, trade:order, trade:cancel, trade:sell, profile:view,
  users:list, users:disable, users:reset-password, users:balance:set,
  logs:view:team

Test Engineer:
  trade:view, trade:order, trade:cancel, trade:sell, profile:view,
  system:status:view, users:list, logs:view:all

Admin:
  all major permissions
```

问题：

1. Test Engineer 被授予 `logs:view:all`，导致可查看 Admin 日志。
2. Senior Tester 当前拥有 `users:disable`、`users:reset-password`、`users:balance:set`，与“高级测试员无配置权限，只能业务操作”的需求不完全一致。
3. Test Engineer 反而没有用户管理、权限解锁、数据导出相关权限。
4. `persistUser()` 每次保存都会直接 `user.permissionCodes = ROLE_PERMISSIONS[user.role]`，无法支持独立功能点配置、用户级覆盖和后台矩阵预留。

---

### 1.3 登录与密码

文件：`apps/server/src/services/store.ts`

当前登录校验：

```ts
if (user.username === username && user.password === password && user.isActive) {
  return user;
}
```

问题：

1. 密码明文存储。
2. 明文比较。
3. 没有失败次数锁定。
4. 没有密码更新时间。
5. 没有管理员强制重置后的“首次登录必须改密码”。

文件：`apps/server/src/index.ts`

`/api/me/password` 后端接口已经存在，前端 API `changeMyPassword()` 也存在。

问题在前端：`apps/client/src/App.tsx` 中“修改个人密码”按钮只出现在 `UserManagementPage`，而 `canOpenUserManagement = me?.role === "Admin" || me?.role === "Senior Tester"`，Tester 没有入口。

---

### 1.4 日志范围控制

文件：`apps/server/src/index.ts`

当前代码：

```ts
function canViewAllLogs(user: UserRecord) {
  return user.role === "Admin" || user.role === "Test Engineer" || user.permissionCodes.includes("logs:view:all");
}
```

问题：Test Engineer 默认全局日志可见，违反需求和 TC-E05。

当前 `teamVisibleUserIds()` 只支持：

```text
Senior Tester -> 直属 Tester
```

不支持：

```text
Test Engineer -> 管理的 Senior Tester + Tester
Senior Tester -> 所属 Test Engineer 下的成员关系
Admin -> 全局
```

---

### 1.5 用户管理前端

文件：`apps/client/src/App.tsx`

当前 `UserManagementPage`：

1. 只允许 Admin 和 Senior Tester 进入。
2. 创建用户表单只对 Admin 显示。
3. `seniorOptions = users.filter(user => user.role === "Senior Tester")`。
4. 直属上级字段只在 `form.role === "Tester"` 时可用。
5. `canManageTarget` 只允许：
   - Admin 管理所有人；
   - Senior Tester 管理直属 Tester。

问题：

1. Test Engineer 无法进入用户管理页，但需求中 Test Engineer 应负责权限解锁、团队管理、数据质量管控。
2. Senior / Tester 无法绑定 Test Engineer。
3. 用户创建后无法换组/解绑。
4. 没有 Initial / Standard 权限级别。
5. 没有权限解锁理由输入。
6. 没有用户角色修改。
7. 没有清晰的后台“权限矩阵”预留页。

---

### 1.6 批量创建用户

文件：`apps/server/src/services/bulk-users.ts`  
文件：`apps/client/src/App.tsx`

当前能力：

1. 前端解析 CSV/TSV 文本。
2. 后端接收 JSON 用户数组。
3. 只支持 Tester 绑定 Senior Tester。
4. 文件上传 accept 主要是 `.csv,.tsv,text/csv,text/tab-separated-values`。

问题：

1. 不支持 `.xlsx`。
2. 没有 Excel 模板下载。
3. 没有后端 Excel 解析。
4. 没有批量预览接口。
5. 没有 500 条上限的自动化测试。
6. 错误提示偏代码化，不够业务可读。

---

### 1.7 当前导出服务

文件：`apps/server/src/services/csv-zip-export.ts`

当前导出包包括：

```text
audit_logs.csv
training_logs.csv
matching_events.csv
matching_actions.csv
orders.csv
positions.csv
operated_rounds.csv
profile.csv
manifest.json
```

当前问题：

1. 这是内部日志导出，不是严格客户数据集导出。
2. `users.csv` / `profile.csv` / `manifest.json` 会暴露真实 `user_id`、`username`、`display_name`。
3. `orders.csv` 使用 `tester_id = row.testerId`，不是匿名 ID。
4. `training_logs.csv` 同时含 `user_id` 和 `tester_id_anon`，客户交付时不应暴露 `user_id`。
5. 没有 `format=csv|json|parquet` 参数。
6. 没有默认过滤 D 级数据。
7. 没有 Admin 才可包含 D 级的权限控制。
8. 没有导出审计日志。
9. 没有导出 schema 文件或字段说明。
10. `resolveExportUsers()` 当前把 Test Engineer 当作 allAllowed，导致导出范围过大。

---

## 2. 文档与测试表对应的未实现功能清单

### 2.1 用户身份与权限管理

| 编号 | 需求/用例 | 当前状态 | 缺口 | 优先级 |
|---|---|---|---|---|
| U-01 | 四级角色体系 | 已有 Role 类型 | 权限矩阵与文档不一致 | P0 |
| U-02 | RBAC 每个功能点可独立配置 | 只有固定 `ROLE_PERMISSIONS` | 无后台矩阵、无用户级覆盖、权限分散 | P1 |
| U-03 | 前端动态渲染菜单/按钮 | 部分实现 | 仍有 role 硬编码，例如 `canOpenUserManagement` | P0 |
| U-04 | 后端所有 API 校验权限 | 部分实现 | 日志/导出范围错误，导出接口权限过宽 | P0 |
| U-05 | 拒绝越权并记录日志 | 部分缺失 | 权限拒绝没有统一 audit event | P1 |
| U-06 | Test Engineer 只能看管理范围内日志 | 未实现 | 当前可看 Admin 日志 | P0 |
| U-07 | Test Engineer 团队管理 | 未实现 | 不能进入用户管理页，不能管理下级 | P1 |
| U-08 | Senior Tester 查看同组/下级数据 | 部分实现 | 只支持直属 Tester，不支持更通用组织树 | P1 |
| U-09 | 用户上下级绑定/修改/解绑 | 部分实现 | 只能创建时指定 Tester 的 Senior，上线后不能改 | P1 |
| U-10 | Tester 修改个人密码 | 后端已有，前端缺入口 | TC-A08 失败 | P0 |
| U-11 | 密码确认不一致 | 前端管理页已有部分校验 | Tester 自己改密码场景无法测试 | P0 |
| U-12 | 登录失败锁定 | 未实现 | Excel 备注建议 3 次失败锁定 | P1 |
| U-13 | bcrypt 密码哈希 | 未实现 | 明文密码 | P0 安全 |
| U-14 | 测试员 Initial / Standard 解锁 | 未实现 | 无字段、无 UI、无审计 | P1 |
| U-15 | 解锁理由与降级限制 | 未实现 | TE 升级，Admin 降级逻辑缺失 | P1 |
| U-16 | 权限变更审计 | 部分实现用户管理日志 | 角色变更、上下级变更、权限级别变更不完整 | P1 |
| U-17 | 批量创建 Excel | 未实现 | 只支持 CSV/TSV | P1 |
| U-18 | 批量创建模板 | 未实现 | 无 Excel 模板下载 | P1 |
| U-19 | 审计日志汉化/字段解释 | 未实现/部分 | TC-E08 失败 | P0 |
| U-20 | 轮次筛选下拉 | 未实现 | TC-E08 失败 | P0 |

---

### 2.2 用户/客户数据导出

| 编号 | 需求/用例 | 当前状态 | 缺口 | 优先级 |
|---|---|---|---|---|
| E-01 | 客户数据集导出 | 部分实现 | 当前更像内部日志导出 | P0 |
| E-02 | 导出字段：时间戳毫秒、品类、轮次、方向等 | 部分已有 | 字段名/完整性未统一，部分字段可能空 | P0 |
| E-03 | 匿名化测试员 ID | 部分已有 | 仍导出真实 user_id/username/display_name | P0 |
| E-04 | 质量等级 `quality_grade` | 字段存在 | 填充规则未开发，TC-G03 N/A | P0/P1 |
| E-05 | 策略簇 `strategy_cluster_label` | 字段存在 | 填充规则未开发 | P1 |
| E-06 | 行情类型 `market_regime_label` | 字段存在 | 填充规则未开发 | P1 |
| E-07 | 撮合字段：订单簿快照、成交价、滑点、部分成交等 | 部分已有 | 需映射到客户 schema 并保证填充 | P0 |
| E-08 | 结算字段：方向、结算时间、Gamma 轮询次数、Redeem 时间 | 部分已有 | 需补空值策略和 schema 校验 | P1 |
| E-09 | CSV 格式 | 已有 | 需客户专用 CSV，不暴露内部字段 | P0 |
| E-10 | JSON 格式 | 未实现 | 需 JSONL 或 JSON array | P1 |
| E-11 | Parquet 格式 | 未实现 | 大批量推荐，需依赖与实现 | P2 |
| E-12 | 默认过滤 D 级 | 未实现 | 当前无 include/exclude 控制 | P0 |
| E-13 | Admin 可选择包含 D 级 | 未实现 | 需权限码与 UI toggle | P0 |
| E-14 | 导出日志 | 不完整 | 缺少导出人、范围、记录数、过滤数、格式、校验 hash | P0 |
| E-15 | 导出权限 | 过宽 | 前端 `profile:view` 即可导出，后端也偏宽 | P0 |
| E-16 | CSV 转义测试 | 未覆盖 | TC-G04 N/A | P1 |
| E-17 | manifest 字段 | 已有 | manifest 暴露真实用户信息，需要区分内部/客户 manifest | P0 |
| E-18 | 查询时区显示 | 有混淆 | UI 查询中国时间但显示 UTC，需要明确 | P1 |

---

## 3. 目标权限模型

### 3.1 新增/调整 PermissionCode

在 `apps/server/src/domain/types.ts` 扩展：

```ts
export type PermissionCode =
  | "trade:view"
  | "trade:order"
  | "trade:cancel"
  | "trade:sell"
  | "profile:view"
  | "profile:password:change"
  | "system:status:view"
  | "audit:view"
  | "audit:export"
  | "users:list"
  | "users:create"
  | "users:bulk-create"
  | "users:disable"
  | "users:enable"
  | "users:reset-password"
  | "users:balance:set"
  | "users:role:update"
  | "users:manager:update"
  | "users:permission-level:update"
  | "logs:view:self"
  | "logs:view:managed"
  | "logs:view:all"
  | "data:export:self"
  | "data:export:managed"
  | "data:export:all"
  | "data:export:include-d"
  | "quality:review"
  | "strategy:config"
  | "market:config";
```

说明：

1. 不要直接删除旧权限码，避免旧代码引用断裂。
2. 新权限先兼容旧权限。例如 `logs:view:team` 可以映射为 `logs:view:managed`。
3. UI 只根据后端返回的 `permissionCodes` 渲染，不再用大量 role 硬编码。

---

### 3.2 推荐角色预设

#### Tester

```text
trade:view
trade:order
trade:cancel
trade:sell
profile:view
profile:password:change
logs:view:self
data:export:self 或不允许导出，由业务决定
```

建议：如果客户数据集只能由 TE/Admin 导出，则不要给 Tester `data:export:self`。若保留现有功能，则保留为“个人内部记录导出”，不要叫“客户数据集导出”。

#### Senior Tester

```text
Tester 全部
logs:view:managed
quality:review
```

注意：需求文档写明高级测试员无配置权限。当前 Senior Tester 拥有停用、重置密码、设置余额权限；为了不破坏现有功能，第一阶段不要直接删除这些能力，而是：

1. 把它们从硬编码角色判断中移入权限矩阵。
2. 默认可以暂时保留现有权限。
3. 在后台矩阵中标注“待业务确认是否移除”。

#### Test Engineer

```text
Tester 全部
system:status:view
users:list
users:balance:set
users:manager:update
users:permission-level:update
logs:view:managed
data:export:managed
quality:review
strategy:config
```

Test Engineer 不应该默认拥有：

```text
logs:view:all
data:export:all
audit:export 全局
users:create Admin 级创建权限，除非业务确认
```

#### Admin

```text
全部权限
```

---

### 3.3 用户组织关系

当前字段 `seniorTesterId` 不够。新增通用字段：

```ts
managerUserId?: string;
```

同时保留旧字段：

```ts
seniorTesterId?: string;
```

兼容规则：

```ts
const managerUserId = user.managerUserId ?? user.seniorTesterId;
```

推荐组织关系：

```text
Admin
  └─ Test Engineer
       └─ Senior Tester
            └─ Tester
```

允许关系：

```text
Tester 可以绑定 Senior Tester 或 Test Engineer
Senior Tester 可以绑定 Test Engineer 或 Admin
Test Engineer 可以绑定 Admin
Admin 不需要上级
```

数据可见范围：

```text
Tester：只能自己
Senior Tester：自己 + 直属 Tester
Test Engineer：自己 + 管理的 Senior Tester + 这些 Senior 下的 Tester + 直属 Tester
Admin：全部
```

必须实现统一函数：

```ts
getVisibleUserIdsForActor(actor: UserRecord, scope: "self" | "managed" | "all"): Set<string>
```

所有地方都用它：

```text
/api/users
/api/logs/search
/api/logs/export
/api/datasets/export
/api/logs/timeline
用户管理目标选择
质量审核
余额设置
权限解锁
```

不要每个接口自己写一套 role 判断。

---

## 4. 后端改造方案

### 4.1 新建权限策略模块

新增：

```text
apps/server/src/auth/permissions.ts
apps/server/src/auth/scope.ts
apps/server/src/auth/audit.ts
```

职责：

```text
permissions.ts
  - ROLE_PRESETS
  - normalizePermissionCodes()
  - hasPermission()
  - assertPermission()
  - canManageUser()

scope.ts
  - getVisibleUserIdsForActor()
  - resolveScopedUserFilter()
  - assertUserInScope()

audit.ts
  - recordPermissionDenied()
  - recordUserChange()
  - recordExportEvent()
```

替换以下分散逻辑：

```text
apps/server/src/services/store.ts 中的 ROLE_PERMISSIONS
apps/server/src/index.ts 中的 canViewAllLogs
apps/server/src/index.ts 中的 canViewTeamLogs
apps/server/src/index.ts 中的 teamVisibleUserIds
apps/server/src/index.ts 中的 getTargetUserForManagement
apps/server/src/services/csv-zip-export.ts 中的 resolveExportUsers
```

验收：所有权限范围都从一个模块得出。

---

### 4.2 修复 Test Engineer 日志越界

当前问题代码：

```ts
return user.role === "Admin" || user.role === "Test Engineer" || user.permissionCodes.includes("logs:view:all");
```

修改为：

```ts
function canViewAllLogs(user: UserRecord) {
  return user.role === "Admin" || hasPermission(user, "logs:view:all");
}

function canViewManagedLogs(user: UserRecord) {
  return hasPermission(user, "logs:view:managed") || hasPermission(user, "logs:view:team");
}
```

`resolveLogSearchFilters()` 使用：

```ts
const visibleUserIds = getVisibleUserIdsForActor(actor);

if (query.userId && !visibleUserIds.has(query.userId)) {
  recordPermissionDenied(...);
  throw forbidden;
}

query.scopedUserIds = [...visibleUserIds];
```

验收：

```text
Test Engineer 查询 Admin 日志 -> 403 或返回空，不能返回 Admin 日志。
Admin 查询全局 -> 正常。
Senior 查询直属 Tester -> 正常。
Tester 查询自己 -> 正常。
```

---

### 4.3 用户字段扩展

在当前未引入 migration 的情况下，可以先在 `SCHEMA_SQL` 和 schema repair 中追加兼容字段。未来再迁移到正式 migration。

建议新增字段：

```sql
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_hash TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS manager_user_id TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS permission_level TEXT NOT NULL DEFAULT 'Initial';
ALTER TABLE users ADD COLUMN IF NOT EXISTS failed_login_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE users ADD COLUMN IF NOT EXISTS locked_until_ms BIGINT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_login_at BIGINT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_changed_at BIGINT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS must_change_password BOOLEAN NOT NULL DEFAULT FALSE;
```

TypeScript：

```ts
export type TesterPermissionLevel = "Initial" | "Standard";

export interface UserRecord {
  ...
  password?: string;          // legacy only, do not expose
  passwordHash?: string;
  managerUserId?: string;
  seniorTesterId?: string;    // legacy alias
  permissionLevel: TesterPermissionLevel;
  failedLoginCount?: number;
  lockedUntilMs?: number;
  lastLoginAt?: number;
  passwordChangedAt?: number;
  mustChangePassword?: boolean;
}
```

兼容旧数据：

```ts
function getUserManagerId(user: UserRecord) {
  return user.managerUserId ?? user.seniorTesterId;
}
```

---

### 4.4 密码哈希

新增依赖：

```bash
npm install bcryptjs
npm install -D @types/bcryptjs
```

或者使用 `bcrypt`。Electron/Node 环境为了编译稳定，建议先用 `bcryptjs`。

实现：

```text
createUser：写 passwordHash，不再写明文 password。
login：先检查 passwordHash；如果旧用户只有 password 明文且校验成功，则自动迁移成 passwordHash 并清空 password。
resetPassword：写 passwordHash，设置 mustChangePassword 可选。
changeMyPassword：校验旧密码 hash 后写新 hash。
```

不要直接删除 `password` 字段，先保留兼容。

登录失败锁定：

```text
同一 username 连续失败 3 次：lockedUntilMs = now + 10 minutes。
成功登录：failedLoginCount = 0，lockedUntilMs = null，lastLoginAt = now。
```

审计：

```text
auth.login.success
auth.login.failed
auth.login.locked
user.password.changed
user.password.reset
```

---

### 4.5 用户管理接口新增/调整

保留现有接口，新增以下接口：

```text
GET    /api/users
POST   /api/users
POST   /api/users/bulk
GET    /api/users/bulk/template.xlsx
POST   /api/users/bulk/preview
POST   /api/users/bulk/upload
PATCH  /api/users/:id/role
PATCH  /api/users/:id/manager
PATCH  /api/users/:id/permission-level
PATCH  /api/users/:id/balance
POST   /api/users/:id/reset-password
POST   /api/me/password
```

权限：

```text
GET /api/users
  Admin: all
  Test Engineer: managed users + self
  Senior Tester: managed users + self
  Tester: self only 或不开放

PATCH /api/users/:id/manager
  Admin: all
  Test Engineer: managed subtree only

PATCH /api/users/:id/permission-level
  Test Engineer: Initial -> Standard for managed Tester/Senior, must provide reason
  Admin: any direction, including Standard -> Initial, must provide reason

POST /api/users/:id/reset-password
  Admin: all
  Test Engineer: managed users, if业务允许
  Senior: 暂按现有权限兼容，待业务确认

PATCH /api/users/:id/balance
  Admin: all
  Test Engineer: managed users if permission exists
  Senior: 保留现有兼容或等待业务确认
```

所有用户变更必须记录审计日志：

```text
user.created
user.bulk.created
user.disabled
user.enabled
user.password.reset
user.password.changed
user.balance.set
user.role.changed
user.manager.changed
user.permission_level.changed
permission.denied
```

审计字段至少：

```ts
actorUserId
actorRole
targetUserId
targetRole
actionType
beforeJson
afterJson
reason
timestampMs
traceId
```

---

### 4.6 测试员权限分级解锁

新增类型：

```ts
export type TesterPermissionLevel = "Initial" | "Standard";
```

默认值：

```text
Tester 新账号默认 Initial。
Senior Tester 和 Test Engineer 默认 Standard，除非业务另行要求。
Admin 默认 Standard。
```

Initial 限制：

```text
仅可使用 STD / CON 策略簇。
仓位上限为标准值 50%。
禁用 ADV / DC / HV。
禁用反向对冲，即买 DOWN。
```

当前系统如果还没有策略簇选择 UI，则第一阶段只实现：

```text
字段存储
后台显示
解锁/降级接口
审计日志
导出字段
```

交易约束可以先落地最明确的一条：

```text
Initial 用户不可买 DOWN。
```

如果担心影响现有交易功能，可以先做“警告 + 审计”，不开启硬拦截，增加配置：

```env
ENFORCE_PERMISSION_LEVEL_RESTRICTIONS=false
```

上线前再改成 true。

---

## 5. 前端改造方案

### 5.1 Profile 页增加修改密码

当前 `api.changeMyPassword()` 已存在。需要把修改密码组件移到所有用户都能访问的 Profile 页。

做法：

```text
1. 新建 ChangePasswordPanel 组件。
2. ProfilePage 永远显示 ChangePasswordPanel。
3. UserManagementPage 保留“修改个人密码”按钮也可以，但推荐复用同一组件。
4. 校验：旧密码、新密码、确认密码、密码长度、两次确认一致。
5. i18n：中文/英文错误信息。
```

验收：Tester 登录后在个人页能修改密码。

---

### 5.2 用户管理页权限化

当前：

```ts
canOpenUserManagement = me?.role === "Admin" || me?.role === "Senior Tester";
```

改为：

```ts
canOpenUserManagement = hasAnyPermission(me, [
  "users:list",
  "users:create",
  "users:bulk-create",
  "users:manager:update",
  "users:permission-level:update",
  "users:reset-password",
  "users:balance:set"
]);
```

用户管理页按权限显示按钮：

```text
创建账号：users:create
批量创建：users:bulk-create
停用/启用：users:disable / users:enable
重置密码：users:reset-password
设置余额：users:balance:set
修改角色：users:role:update
修改上级：users:manager:update
权限解锁/降级：users:permission-level:update
```

不要写：

```ts
if (me.role === "Admin") ...
```

除非是 Admin-only 的强业务规则，例如包含 D 级导出。

---

### 5.3 用户创建/编辑表单

新增字段：

```text
账号 username
初始密码 password
姓名 displayName，可选或必填按业务决定
角色 role
语言 language
直属上级 managerUserId
初始余额 availableUsdc
权限级别 permissionLevel
是否首次登录必须改密码 mustChangePassword
```

直属上级选项：

```text
根据被创建用户 role 动态筛选合法上级。
Tester：Senior Tester / Test Engineer / Admin
Senior Tester：Test Engineer / Admin
Test Engineer：Admin
Admin：无
```

新增操作：

```text
修改上级
修改角色
权限解锁/降级
填写原因
```

---

### 5.4 批量创建 Excel

推荐两层实现：

1. 前端可读取 `.xlsx` 生成预览。
2. 后端也支持 `.xlsx` 上传，保证安全和一致性。

新增依赖：

```bash
npm install xlsx @fastify/multipart
```

前端：

```text
下载 Excel 模板
上传 CSV/TSV/XLSX
预览有效行、错误行
确认创建
导出错误行 Excel/CSV
```

Excel 模板列：

```text
username
password
displayName
role
language
managerUsername
availableUsdc
permissionLevel
mustChangePassword
```

兼容旧列：

```text
seniorTesterUsername
seniorTesterId
```

后端验证：

```text
username 必填且唯一
password 必填且长度合规
role 必须是四级角色之一
language 必须 zh-CN/en-US
managerUsername 必须存在且角色合法
availableUsdc >= 0
permissionLevel 必须 Initial/Standard
最多 500 条
```

---

### 5.5 审计日志 UI 修复

修复 TC-E08：

1. 操作分类汉化：新增 `eventTypeLabelMap`。
2. 轮次筛选：文本输入改为下拉，数据源为 `GET /api/rounds/recent?limit=200`。
3. 字段 tooltip：
   - `order_id` 显示“订单编号”。
   - `trace_id` 显示“系统追踪号，用于内部排查”。
   - `trade_id` 显示“成交编号”。
4. 隐藏不适合普通用户看的内部字段，详情展开时再显示。
5. Test Engineer 和 Senior Tester 只能选择自己可见范围内用户。

---

## 6. 客户数据集导出方案

### 6.1 保留内部导出，新增客户导出

不要直接改坏现有 `/api/logs/export`。建议区分两个导出：

```text
内部日志导出：/api/logs/export
客户数据集导出：/api/datasets/export
```

内部日志导出可以保留：

```text
真实用户 ID
username
displayName
traceId
debug 字段
```

客户数据集导出必须：

```text
匿名化用户 ID
不导出 username/displayName/user_id
字段严格符合 S8
默认过滤 D 级
生成导出审计日志
支持 CSV/JSON/Parquet
```

---

### 6.2 新增导出权限

```text
data:export:self
  仅导出自己的客户格式数据，如果业务允许。

data:export:managed
  Test Engineer / Senior Tester 导出管理范围内数据。

data:export:all
  Admin 全局导出。

data:export:include-d
  允许包含 D 级数据，只给 Admin。
```

建议业务默认：

```text
Tester：不允许客户数据集导出，只允许个人内部记录下载，或只读。
Senior Tester：可导出 managed 内部复盘数据，不一定能导出客户数据集。
Test Engineer：可导出 managed 客户数据集。
Admin：可导出全部，且可包含 D 级。
```

---

### 6.3 导出接口

新增：

```http
POST /api/datasets/export
```

请求：

```ts
interface DatasetExportRequest {
  format: "csv" | "json" | "jsonl" | "parquet";
  startTimeMs?: number;
  endTimeMs?: number;
  userIds?: string[];
  roundIds?: string[];
  symbols?: string[];
  qualityGrades?: Array<"A" | "B" | "C" | "D">;
  includeDGrade?: boolean;
  includeManifest?: boolean;
  includeSchema?: boolean;
  timezone?: "UTC" | "Asia/Shanghai";
}
```

响应：

```text
application/zip
```

ZIP 内容：

```text
manifest.json
schema.json
customer_dataset.csv        // format=csv
customer_dataset.jsonl      // format=jsonl
customer_dataset.parquet    // format=parquet
export_audit.json
```

---

### 6.4 客户数据集字段 schema

推荐字段名使用稳定英文 snake_case：

```text
timestamp_ms
asset
round_id
direction
entry_odds
delta_at_entry
volume_at_entry
position_amount
exit_type
exit_odds
settlement_result
tester_id_anon
strategy_cluster_label
market_regime_label
quality_grade
order_book_snapshot_entry
actual_fill_price
slippage_bps
is_partial_fill
unfilled_qty
execution_latency_ms
settlement_direction
settlement_time_ms
gamma_poll_count
redeem_finish_time_ms
source_binance_state
source_chainlink_state
source_clob_state
trace_id_hash
order_id_hash
export_schema_version
```

注意：

1. `tester_id_anon` 必须使用稳定 hash，不暴露真实 ID。
2. `trace_id_hash` 和 `order_id_hash` 如客户不需要，可不导出；若导出也应 hash 化。
3. 不导出：`username`、`displayName`、`user_id`、真实 `order_id`、真实 `trace_id`。
4. `quality_grade` 为空时不能静默通过；需要导出前校验并显示填充率。

---

### 6.5 默认过滤 D 级

导出逻辑：

```ts
if (!request.includeDGrade) {
  rows = rows.filter(row => row.quality_grade !== "D");
}

if (request.includeDGrade && !hasPermission(actor, "data:export:include-d")) {
  throw forbidden;
}
```

如果 `quality_grade` 缺失：

第一阶段建议：

```text
缺失 quality_grade 的记录标为 C（待审核），并在 manifest 记录 fallback_count。
```

但如果甲方把 TC-G03 定为 P0，则应先补质量规则，不要用 C 掩盖问题。

---

### 6.6 质量字段填充方案

新增服务：

```text
apps/server/src/services/data-quality.ts
```

输出：

```ts
interface QualityClassification {
  qualityGrade: "A" | "B" | "C" | "D";
  strategyClusterLabel?: "ADV" | "STD" | "HV" | "DC" | "CON" | "SKP";
  marketRegimeLabel?: string;
  reasons: string[];
}
```

第一版规则建议：

```text
D：封盘后操作、超仓、止损未执行、标注严重延迟、系统异常未上报、订单失败但未补日志。
C：核心标签缺失、系统检测到异常但待人工审核。
B：正常完成，字段完整，但无高级策略/日志补充。
A：字段完整、策略标签明确、无异常、执行延迟正常。
```

`strategyClusterLabel` 来源优先级：

```text
用户填写/策略面板选择 > 后端规则推断 > 空值/UNKNOWN
```

`marketRegimeLabel` 来源优先级：

```text
用户日志标签 > 时段风险规则 > 波动/Delta 自动分类 > UNKNOWN
```

导出前校验：

```text
quality_grade 填充率
strategy_cluster_label 填充率
market_regime_label 填充率
```

如果填充率低于阈值，前端给出警告，Admin/TE 根据权限决定是否继续。

---

### 6.7 导出审计日志

每次导出记录：

```ts
interface DatasetExportAuditEvent {
  exportId: string;
  actorUserId: string;
  actorRole: Role;
  format: "csv" | "json" | "jsonl" | "parquet";
  startTimeMs?: number;
  endTimeMs?: number;
  requestedUserIds?: string[];
  effectiveUserIds: string[];
  recordCount: number;
  filteredDGradeCount: number;
  missingQualityCount: number;
  missingStrategyClusterCount: number;
  missingMarketRegimeCount: number;
  includeDGrade: boolean;
  fileSha256: string;
  createdAtMs: number;
}
```

写入：

```text
AuditEvent
JSONL 审计日志
可选 PostgreSQL export_audit_logs 表
```

---

### 6.8 前端导出 UI

在 LogSearch / Analysis / Dataset Export 页新增导出向导：

```text
导出类型：内部日志 / 客户数据集
数据范围：日期时间、用户、轮次、品类、质量等级
格式：CSV / JSONL / Parquet
是否包含 D 级：仅 Admin 可见
预览统计：预计记录数、D 级过滤数、缺失字段数量、可见用户范围
下载按钮
导出历史表
```

不要让普通用户通过 `profile:view` 直接导出客户数据集。

---

## 7. 测试与验收

### 7.1 后端权限测试

新增/修改脚本：

```text
scripts/user-permission-check.ts
scripts/log-search-check.ts
scripts/export-check.ts
scripts/bulk-user-xlsx-check.ts
```

覆盖：

```text
Admin 可查看全局日志。
Test Engineer 不能查看 Admin 日志。
Test Engineer 可查看自己管理的 Tester 日志。
Senior Tester 可查看直属 Tester 日志。
Tester 只能查看自己日志。
越权访问返回 403 或空结果，并记录 permission.denied。
```

---

### 7.2 用户功能测试

```text
Tester 可在个人页修改密码。
修改密码时旧密码错误失败。
两次新密码不一致失败。
新密码登录成功，旧密码登录失败。
连续 3 次登录失败后锁定。
Admin 可重置目标用户密码。
Test Engineer 可解锁 managed Tester 的 permissionLevel。
Test Engineer 不能降级 Standard -> Initial。
Admin 可以降级，必须填写理由。
用户换组后，日志/导出可见范围即时变化。
```

---

### 7.3 Excel 批量创建测试

```text
下载模板成功。
上传 .xlsx 成功。
上传 .csv 成功。
上传 .tsv 成功。
501 条被拒绝。
重复 username 被识别。
非法 role 被识别。
非法 manager 被识别。
错误行可导出。
```

---

### 7.4 客户数据集导出测试

```text
导出 CSV 成功。
导出 JSONL 成功。
导出 Parquet 成功，若暂未实现则接口明确返回 501，不允许假装成功。
普通用户不能导出客户数据集。
Test Engineer 只能导出 managed 数据。
Admin 可导出全部。
默认过滤 D 级。
非 Admin 请求 includeDGrade 返回 403。
导出结果不包含真实 user_id、username、displayName。
CSV 中逗号、引号、换行正确转义。
manifest 包含 schemaVersion、recordCount、filteredDGradeCount、fileSha256。
导出审计日志可查询。
```

---

## 8. 冗余代码整理建议

原则：**不影响现有功能；做不到就不做。**

### 8.1 可以做的安全整理

#### R-01：集中权限判断

把以下函数/逻辑迁到统一模块：

```text
ROLE_PERMISSIONS
canViewAllLogs
canViewTeamLogs
teamVisibleUserIds
getTargetUserForManagement
resolveExportUsers
前端 has role 判断
```

替换为：

```text
hasPermission()
getVisibleUserIdsForActor()
canManageUser()
```

这属于“集中复用”，不是删除功能。

---

#### R-02：App.tsx 拆组件

当前 `apps/client/src/App.tsx` 过大。可以先只拆和本轮相关的组件：

```text
apps/client/src/features/profile/ChangePasswordPanel.tsx
apps/client/src/features/users/UserManagementPage.tsx
apps/client/src/features/users/BulkUserDialog.tsx
apps/client/src/features/logs/LogSearchPage.tsx
apps/client/src/features/export/DatasetExportDialog.tsx
```

保持 props 和行为不变。

---

#### R-03：批量创建解析工具独立

把 `parseBulkUserText()` 从 `App.tsx` 移到：

```text
apps/client/src/features/users/bulkImport.ts
```

新增：

```text
parseCsvBulkUsers()
parseTsvBulkUsers()
parseXlsxBulkUsers()
```

---

#### R-04：导出 builder 分层

保留旧文件 `csv-zip-export.ts`，新增：

```text
apps/server/src/services/export/internal-log-export.ts
apps/server/src/services/export/customer-dataset-export.ts
apps/server/src/services/export/export-manifest.ts
apps/server/src/services/export/csv.ts
apps/server/src/services/export/jsonl.ts
apps/server/src/services/export/parquet.ts
```

旧 `/api/logs/export` 调用 `internal-log-export.ts`。  
新 `/api/datasets/export` 调用 `customer-dataset-export.ts`。

---

### 8.2 暂时不要删除的内容

这些不要动，避免破坏现有功能：

```text
seniorTesterId 字段：先作为 legacy alias 保留。
旧 permissionCodes：先兼容，不要删除。
/api/logs/export：保留，作为内部导出。
JSONL 审计日志：保留。
custom ZIP builder：保留，除非确认所有环境都可用第三方 zip 库。
现有用户管理接口：保留并兼容，只新增能力。
现有 CSV/TSV 批量创建：保留，新增 Excel。
```

---

## 9. Codex 执行顺序

### Phase 0：保护现有功能

```bash
npm ci
npm run typecheck
npm run test:permissions
npm run test:export
npm run test:logs
npm run test:bulk-users
npm run build
```

如果当前已有测试失败，先记录，不要误判为新改动导致。

---

### Phase 1：权限策略与 TE 越界修复

修改：

```text
apps/server/src/domain/types.ts
apps/server/src/auth/permissions.ts
apps/server/src/auth/scope.ts
apps/server/src/index.ts
apps/server/src/services/store.ts
apps/server/src/services/csv-zip-export.ts
```

目标：

```text
新增权限码。
统一权限策略。
修复 Test Engineer 全局日志可见问题。
导出范围使用同一 scope 函数。
所有越权访问记录 permission.denied。
```

验收：TC-E05 PASS。

---

### Phase 2：Profile 修改密码 + bcrypt

修改：

```text
apps/server/src/services/store.ts
apps/server/src/index.ts
apps/client/src/App.tsx
apps/client/src/features/profile/ChangePasswordPanel.tsx
apps/client/src/utils/api.ts
```

目标：

```text
所有角色都能在 Profile 修改个人密码。
密码 hash 化。
旧明文密码可自动迁移。
失败锁定。
```

验收：TC-A08 / TC-A09 PASS。

---

### Phase 3：用户组织关系与权限解锁

修改：

```text
apps/server/src/domain/types.ts
apps/server/src/services/store.ts
apps/server/src/auth/scope.ts
apps/server/src/index.ts
apps/client/src/features/users/UserManagementPage.tsx
```

目标：

```text
新增 managerUserId。
保留 seniorTesterId alias。
支持换组/解绑。
支持 Initial / Standard。
支持解锁理由。
支持审计日志。
```

验收：

```text
TE 管理下级。
TE 解锁 managed Tester。
Admin 可降级。
越权失败。
```

---

### Phase 4：Excel 批量创建

修改：

```text
package.json
apps/server/src/services/bulk-users.ts
apps/server/src/index.ts
apps/client/src/features/users/BulkUserDialog.tsx
apps/client/src/features/users/bulkImport.ts
```

目标：

```text
Excel 模板下载。
CSV/TSV/XLSX 上传。
批量预览。
错误行报告。
500 条上限。
```

验收：TC-F02 PASS，TC-F03 可测。

---

### Phase 5：客户数据集导出

修改：

```text
apps/server/src/services/export/customer-dataset-export.ts
apps/server/src/services/export/internal-log-export.ts
apps/server/src/services/data-quality.ts
apps/server/src/index.ts
apps/client/src/features/export/DatasetExportDialog.tsx
apps/client/src/App.tsx
apps/client/src/utils/api.ts
```

目标：

```text
新增 /api/datasets/export。
CSV/JSONL/Parquet 格式。
默认过滤 D 级。
Admin 才能 includeD。
不暴露真实用户信息。
生成 manifest/schema/export_audit。
```

验收：TC-G01 / TC-G02 / TC-G03 / TC-G04 全部可测。

---

### Phase 6：审计日志 UI

修改：

```text
apps/server/src/index.ts
apps/client/src/features/logs/LogSearchPage.tsx
apps/client/src/i18n/index.ts
```

目标：

```text
操作分类汉化。
轮次筛选下拉。
字段 tooltip。
隐藏/解释 traceId。
```

验收：TC-E08 PASS。

---

## 10. 给 Codex 的直接任务描述

可以直接把下面这段给 Codex：

```text
请按 CODEX_USER_PERMISSION_EXPORT_REQUIREMENTS.md 修改当前项目，范围只包括用户身份/权限管理、批量创建用户、审计日志可读性、客户数据集导出，不要改撮合核心逻辑，不要删除现有接口，不要破坏现有日志导出。

必须完成：
1. 新增统一权限策略模块，修复 Test Engineer 可查看 Admin 日志的问题。
2. 所有日志搜索、用户列表、用户管理、导出接口都必须使用同一个 getVisibleUserIdsForActor 范围函数。
3. Profile 页面增加所有角色可见的修改个人密码入口。
4. 密码改为 hash 存储，兼容旧明文用户首次成功登录后自动迁移。
5. 新增 managerUserId，保留 seniorTesterId 兼容；支持用户换组/解绑。
6. 新增 permissionLevel: Initial/Standard，支持 Test Engineer 对管理范围内用户解锁，Admin 可降级，必须填写理由并记录审计日志。
7. 批量创建支持 Excel 模板下载和 .xlsx 上传，同时保留 CSV/TSV。
8. 保留 /api/logs/export 作为内部导出，新增 /api/datasets/export 作为客户数据集导出。
9. 客户数据集导出不得包含真实 user_id、username、displayName；必须使用 tester_id_anon。
10. 客户数据集导出支持 CSV、JSONL，Parquet 如果不能稳定实现则先返回 501 并在 UI 标注暂不可用，不允许假成功。
11. 客户数据集默认过滤 quality_grade=D；只有 Admin 可 includeD。
12. 每次导出必须生成导出审计日志，包含导出人、范围、格式、记录数、过滤数、文件 sha256。
13. 修复审计日志 UI：操作分类汉化，轮次筛选改为最近 200 轮下拉，order_id/trace_id/trade_id 加 tooltip。
14. 增加自动化测试，至少覆盖 TC-A08、TC-A09、TC-E05、TC-E08、TC-F02、TC-F03、TC-G01、TC-G02、TC-G03、TC-G04。
15. 冗余代码只做安全抽取：可以抽权限模块、导出模块、批量创建解析模块；不要删除 seniorTesterId、旧权限码、/api/logs/export、JSONL 日志、现有 CSV/TSV 功能。
```

---

## 11. 最低上线验收清单

上线前至少满足：

```text
[ ] Tester 能在 Profile 修改密码。
[ ] 密码不再明文写入新用户记录。
[ ] Test Engineer 查询 Admin 日志失败。
[ ] Test Engineer 只能导出管理范围内数据。
[ ] 客户导出不包含真实 user_id / username / displayName。
[ ] 客户导出默认过滤 D 级。
[ ] 非 Admin 不能 includeD。
[ ] 导出 manifest 记录 recordCount / filteredDGradeCount / sha256。
[ ] Excel 批量创建模板可下载、上传可预览、错误行可识别。
[ ] 审计日志动作分类已汉化。
[ ] 轮次筛选为下拉。
[ ] 旧 CSV/TSV 批量创建仍可用。
[ ] 旧 /api/logs/export 仍可用。
[ ] npm run typecheck 通过。
[ ] npm run build 通过。
```

# Codex 执行任务卡：用户权限与客户数据导出

## 执行前命令

```bash
npm ci
npm run typecheck
npm run test:permissions
npm run test:export
npm run test:logs
npm run test:bulk-users
npm run build
```

记录当前失败项，后续只把新增失败算作回归。

---

## T1｜统一权限策略并修复 Test Engineer 越界

涉及文件：

```text
apps/server/src/domain/types.ts
apps/server/src/auth/permissions.ts
apps/server/src/auth/scope.ts
apps/server/src/index.ts
apps/server/src/services/store.ts
apps/server/src/services/csv-zip-export.ts
```

任务：

1. 新增权限码：`logs:view:self`、`logs:view:managed`、`data:export:managed`、`data:export:all`、`data:export:include-d`、`users:manager:update`、`users:permission-level:update`、`quality:review`。
2. 新建 `hasPermission()`、`getVisibleUserIdsForActor()`、`canManageUser()`。
3. 替换 `canViewAllLogs()` 中 `Test Engineer` 全局可见逻辑。
4. 日志搜索、导出、用户列表都使用同一 scope 函数。
5. 越权访问写 `permission.denied` 审计日志。

验收：

```text
Test Engineer 不能查看 Admin 日志。
Admin 可查看全局。
Senior 只能查看直属/managed 数据。
Tester 只能查看自己。
```

---

## T2｜所有角色可修改个人密码 + 密码哈希

涉及文件：

```text
apps/server/src/services/store.ts
apps/server/src/index.ts
apps/client/src/App.tsx
apps/client/src/features/profile/ChangePasswordPanel.tsx
apps/client/src/utils/api.ts
```

任务：

1. Profile 页增加“修改密码”组件，所有角色可见。
2. 安装并使用 `bcryptjs`。
3. 新用户写 `passwordHash`。
4. 旧明文用户首次登录成功后自动迁移到 hash。
5. 登录失败 3 次锁定 10 分钟。
6. 修改/重置密码写审计日志。

验收：

```text
Tester 可修改个人密码。
两次新密码不一致会失败。
旧密码不能登录，新密码可登录。
新用户数据库不写明文密码。
```

---

## T3｜用户组织关系与权限分级解锁

涉及文件：

```text
apps/server/src/domain/types.ts
apps/server/src/services/store.ts
apps/server/src/auth/scope.ts
apps/server/src/index.ts
apps/client/src/features/users/UserManagementPage.tsx
```

任务：

1. 新增 `managerUserId`，保留 `seniorTesterId` 兼容。
2. 支持合法上级关系：Tester -> Senior/TE/Admin；Senior -> TE/Admin；TE -> Admin。
3. 新增 `permissionLevel: Initial | Standard`。
4. Test Engineer 可将 managed 用户 Initial -> Standard，必须填写理由。
5. Admin 可升级/降级，必须填写理由。
6. 记录 `user.manager.changed`、`user.permission_level.changed` 审计日志。

验收：

```text
用户创建后可换组/解绑。
TE 只能解锁自己管理范围内用户。
TE 不能降级 Standard -> Initial。
Admin 可以降级。
```

---

## T4｜Excel 批量创建用户（调整，不要使用excel来批量创建用户）

修改需求建议：只能使用CSV文件来创建用户，暂时不提供excel表格来批量创建的功能，提供CSV批量创建用户的格式，你要保证 managerUsername 一定可以找到对应的用户，找不到的话就创建失败，permissionLevel是什么需要这个字段吗？

涉及文件：

```text
package.json
apps/server/src/services/bulk-users.ts
apps/server/src/index.ts
apps/client/src/features/users/BulkUserDialog.tsx
apps/client/src/features/users/bulkImport.ts
```

任务：

1. 安装 `xlsx` 和 `@fastify/multipart`。(去除)
2. 新增 Excel 模板下载接口。（去除，只保留CSV模板下载接口，并且下载的模板的第一行要有对每个字段的中英文描述，后端读取模板时记得规避这个描述）
3. 支持 `.csv`、`.tsv`、`.xlsx` 上传。（只保.CSV）
4. 支持批量预览、错误行报告。
5. 支持最多 100 条限制。
6. 错误提示业务化。

模板列：

```text
username,password,displayName,role,language,managerUsername,availableUsdc,permissionLevel,mustChangePassword
```

验收：

```text
Excel 模板可下载。(去除，CSV模板可下载)
.xlsx 上传可预览和创建。（.csv上传可预览和创建）
101 条被拒绝。
CSV/TSV 旧功能仍可用。
```

---

## T5｜新增用户数据集导出，不破坏内部日志导出

涉及文件：

```text
apps/server/src/services/export/customer-dataset-export.ts
apps/server/src/services/export/internal-log-export.ts
apps/server/src/services/export/export-manifest.ts
apps/server/src/services/data-quality.ts
apps/server/src/index.ts
apps/client/src/features/export/DatasetExportDialog.tsx
apps/client/src/utils/api.ts
```

任务：

1. 保留 `/api/logs/export` 为内部导出。
2. 新增 `/api/datasets/export` 为客户数据集导出。
3. 客户导出不得包含真实 `user_id`、`username`、`displayName`。
4. 客户导出必须使用 `tester_id_anon`。
5. 默认过滤 `quality_grade = D`。
6. 只有 Admin 可 `includeDGrade=true`。
7. 支持 CSV、JSONL。Parquet 可 P2，实现不了则返回 501，不可假成功。
8. ZIP 内包含 `manifest.json`、`schema.json`、`customer_dataset.*`、`export_audit.json`。
9. 记录导出审计日志：导出人、范围、格式、记录数、D 级过滤数、sha256。

客户字段：

```text
timestamp_ms,asset,round_id,direction,entry_odds,delta_at_entry,volume_at_entry,position_amount,exit_type,exit_odds,settlement_result,tester_id_anon,strategy_cluster_label,market_regime_label,quality_grade,order_book_snapshot_entry,actual_fill_price,slippage_bps,is_partial_fill,unfilled_qty,execution_latency_ms,settlement_direction,settlement_time_ms,gamma_poll_count,redeem_finish_time_ms
```

验收：

```text
TE 只能导出 managed 数据。
Admin 可导出全局。
非 Admin includeD 返回 403。
导出文件不包含真实用户信息。
manifest 记录过滤数量和 sha256。
```

---

## T6｜质量字段填充与导出校验

修改需求建议：现在数据库有这三个字段吗？有的话先留空即可，不需要管这个回填需求。


涉及文件：

```text
apps/server/src/services/data-quality.ts
apps/server/src/services/store.ts
apps/server/src/services/export/customer-dataset-export.ts
```

任务：

1. 实现 `qualityGrade`、`strategyClusterLabel`、`marketRegimeLabel` 的填充/回填策略。
2. 导出前统计三字段填充率。
3. `quality_grade` 缺失时按规则标 C 或阻断，按业务配置决定。
4. D 级条件至少覆盖：封盘后操作、超仓、止损未执行、标注延迟、异常未上报。

验收：

```text
TC-G03 可执行。
导出 manifest 有填充率统计。
D 级默认被过滤。
```

---

## T7｜审计日志 UI 可读性

涉及文件：

```text
apps/server/src/index.ts
apps/client/src/features/logs/LogSearchPage.tsx
apps/client/src/i18n/index.ts
```

任务：

1. 操作分类汉化。
2. 轮次筛选从文本框改为最近 200 轮下拉。
3. `order_id` tooltip：订单编号。
4. `trace_id` tooltip：系统追踪号，用于内部排查。
5. `trade_id` tooltip：成交编号。

验收：TC-E08 PASS。

---

## 安全整理范围

可以做：

```text
抽 auth/permissions.ts
抽 auth/scope.ts
抽 ChangePasswordPanel
抽 BulkUserDialog
抽 DatasetExportDialog
抽 export service
```

不要做：

```text
不要删除 seniorTesterId。
不要删除旧 permission code。
不要删除 /api/logs/export。
不要删除 CSV/TSV 批量创建。
不要删除 JSONL 日志。
不要改撮合核心逻辑。
```

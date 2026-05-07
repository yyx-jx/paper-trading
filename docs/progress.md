# 需求实现进度报告

> 对照文档：`需求文档.docx` v1.0 (2026/5/2) · 代码库 `D:\P_T` · 最后更新 2026/5/4

## 总览

| | 数量 |
|---|---|
| ✅ 已实现 | 50 |
| ⚠️ 部分实现 | 4 |
| ❌ 未实现 | 3 |
| **合计** | **57** |

---

## P0 致命缺陷

| 编号 | 问题 | 状态 | 说明 |
|------|------|:----:|------|
| TC-B06 | 盘口赔率归一化 | ✅ | `resolveDisplayPrice()` 按 mid > last_trade > best_ask 优先级取 CLOB 原始价  |
| TC-D01 | 结算方向错误 + Manual Review 卡死 | ✅ | 三重判断 + 手动结算 UI + POLL_DELAY_MS=0 |
| TC-E08 | 日志搜索字段混乱 | ⚠️ | 枚举→下拉框，ID→文本；actionType 待统一 |

TC-B06 本次进一步改进：新增 `displayPriceSource` 标签标注价格来源（mid/末价/best_ask），新增 `displayPriceSpread` 显示买卖价差，前端 K 线区显示价格来源标签。

---

## P1 高优先级

| 编号 | 问题 | 状态 | 说明 |
|------|------|:----:|------|
| TC-D04 | 0.97 预测信号 | ✅ | upPrice/downPrice > 0.97 → pre_settle 蓝标 |
| TC-E01 | 持仓估值缺失 | ✅ | totalEquity = 余额 + 持仓市值，逐 tick mark |
| TC-E03 | 日志无筛选分页 | ✅ | 方向/状态/时间 + limit/offset |
| TC-E05 | Engineer 权限越界 | ✅ | `logs:view:team` 仅本人+下属 |
| TC-A08 | 无修改密码入口 | ✅ | Profile 自改 + Admin 重置 |
| TC-H01 | 中文模式英文字段 | ⚠️ | 125 key 完整，`localLabel()` 内联翻译待逐条审计 |
| TC-F02 | 批量导入不支持 Excel | ❌ | 仅 CSV/TSV |

---

## UI 改造（需求文档 §3）

| 章节 | 功能 | 状态 | 说明 |
|------|------|:----:|------|
| §3.1 | 1440×900 单屏不滚动 | ✅ | |
| §3.4 | ETH 面板全移除 | ✅ | 代码库自始 BTC-only |
| §3.5 | Chainlink K 线图 | ✅ | 5s OHLC + 1m/5m/15m/1h |
| §3.6 | 新下单面板 Market/Limit | ✅ | FOK 市价 + GTC 限价，冻结/释放/挂单撮合 |
| §3.7 | 系统状态面板 4 源 | ✅ | CLOB/BNB/CL/GAMMA + 稳定性 1-5 级 |
| §3.8 | 动态告警 7 种 | ✅ | 冻结/赔率异动/结算预判/数据中断/延迟高/结算卡死 |
| §3.9 | 决策辅助 | ⚠️ | 动量+成本+时间✅，情绪+入场建议+偏向❌ |
| §3.10 | 持仓/记录简化 | ✅ | 紧凑摘要(8条) + 完整表格(分页) 双模式 |

---

## 安全加固

| 项目 | 状态 |
|------|:----:|
| bcrypt (cost=10) + 旧格式自动升级 | ✅ |
| JWT + 环境变量密钥 | ✅ |
| Electron 窗口缩放锁定 (Ctrl+/- 禁用) | ✅ |

---

## 本次代码更新内容 (2026/5/4)

### 手续费系统重构

- 新增 `apps/server/src/services/clob-fees.ts` — 独立手续费计算模块，支持 platform fee、builder fee
- `PolymarketConnector` 从 Polymarket API 拉取真实 CLOB 市场参数（最小 tick、最小订单量、费率），不可用时回退到保守值
- `FeeBreakdown` 类型扩展：新增 `platformFee`、`builderFee`、`totalFee`
- `ClobMarketInfo` 类型扩展：新增 `minimumTickSize`、`minimumOrderSize`、`platformFeeRate`、`builderFeeRate` 等
- 下单前校验 CLOB tick 对齐和最小订单量
- 挂单(pending)也计算预估手续费

### 价格显示系统

- `resolveDisplayPrice()` — 统一的价格显示决策，带来源标签（mid/last_trade/best_ask/outcome_price）
- `displayPrices` / `displayPriceSource` / `displayPriceSpread` 推送到前端
- 前端价格旁显示来源 tag，spread > 0.1 时优先用 last_trade

### UI 改进

- 新增 `AppErrorBoundary` 错误边界，渲染崩溃时显示恢复界面
- K 线交互提取到 `apps/client/src/utils/chartWheel.ts`：滚轮调数量、Shift+滚轮调 Y 轴、双击复位
- 两个 K 线图（Binance + Chainlink）共享 Y 轴缩放状态
- 新增 `RoundUpPriceMiniChart` — 终端数据带中本轮 UP 价格走势迷你图（面积+折线+当前值）
- 下单面板视觉升级：UP/DOWN 卡片渐变背景、BUY/SELL 彩色分段、MARKET/LIMIT 差异化样式
- 金额输入加大加粗（32px 高、14px 字重 700）
- 标题简化："BTC 5-Min Round" → "B5"
- 终端 monitor 新增延迟列
- 最近轮次圆点改用 `up`/`down`/`pending` class + 彩色高亮

### Electron

- 禁止 Ctrl+/-/0 缩放渲染进程，锁定 zoom level

---

## 未完成 / 待修改

### 1. Excel 批量导入 — TC-F02

```
npm install xlsx
apps/client/src/App.tsx L5808 accept 加 .xlsx
新增 parseBulkUserExcel() → XLSX.read() 解析
加 "下载模板(Excel)" 按钮
后端无需改动
```

### 2. 数据标注字段填充

```
类型就绪: apps/server/src/domain/types.ts:789-791
在 simulation.ts 行为日志写入处补充:
  qualityGrade: 滑点/成交率/延迟 → A/B/C/D
  strategyClusterLabel: 交易模式 → scalping/swing/momentum
  marketRegimeLabel: 波动率+深度 → ranging/trending/volatile
```

### 3. 决策辅助面板补齐

```
apps/client/src/App.tsx buildStrategyHints() 加:
  sentiment: bid/ask 深度比 → 多空情绪
  bias: UP/DOWN 价差 → 市场偏向
  entry_suggestion: 综合建议 (远期接 Claude API)
```

---

## 验收核对

| 条件 (需求文档 §6.1) | |
|----------|:----:|
| TC-D01: 5×UP + 5×DOWN 结算方向全部正确 | ✅ |
| 1440×900 单屏不滚动，所有按钮可点 | ✅ |
| Limit Order 挂单→触发→成交链路完整 | ✅ |
| Chainlink K 线每 5s 更新 | ✅ |
| 系统状态面板断 Binance WS 3s 红色告警 | ✅ |
| 盘口延迟告警 inline badge 不挤压按钮 | ✅ |

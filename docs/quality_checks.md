# Data Quality Checks

## 1. Purpose

本文档说明 Agent 1 当前已经落地的数据质量检查体系，以及如何在本地重复运行。

目标不是做重型数据治理平台，而是在展示型项目中仍保持工业级严谨：

- 有固定入口
- 有清晰检查项
- 有结构化输出
- 能在重建核心表后重复执行

## 2. Command

运行全部核心表检查：

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli validate all
```

也可以单独检查某一张表：

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli validate calendar_table
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli validate adjusted_price_panel
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli validate monthly_universe
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli validate monthly_snapshot_base
```

## 3. Current Coverage

### 3.1 `calendar_table`

检查项：

- 必需字段存在
- `trade_date` 主键唯一
- `month` 与 `trade_date` 一致
- 每个自然月恰有一个 `is_month_end`
- `prev_trade_date / next_trade_date` 链接一致

输出指标：

- `month_count`
- `month_end_count`

### 3.2 `adjusted_price_panel`

检查项：

- 必需字段存在
- `ts_code + trade_date` 主键唯一
- `year/month` 与 `trade_date` 一致
- `adj_open = open * adj_factor`
- `adj_close = close * adj_factor`
- 分区 schema 一致

输出指标：

- `unique_ts_code_count`
- `trade_date_count`
- `adj_factor_non_null_ratio`
- `adj_close_non_null_ratio`

### 3.3 `monthly_universe`

检查项：

- 必需字段存在
- `rebalance_date + ts_code` 主键唯一
- `trade_execution_date > rebalance_date`
- `year/month` 与 `rebalance_date` 一致
- 非纳入样本缺失 `exclude_reason` 记 warning
- 分区 schema 一致

输出指标：

- `eligible_row_count`
- `eligible_ratio`
- `rebalance_month_count`
- `unique_ts_code_count`

### 3.4 `monthly_snapshot_base`

检查项：

- 必需字段存在
- `rebalance_date + ts_code` 主键唯一
- `year/month` 与 `rebalance_date` 一致
- `price_trade_date <= rebalance_date`
- `daily_basic_trade_date <= rebalance_date`
- 全部 `*_tradable_date <= trade_execution_date`
- 与 `monthly_universe` 的主键完全对齐
- `is_eligible / exclude_reason` 与 `monthly_universe` 一致
- 分区 schema 一致

输出指标：

- `price_snapshot_coverage_ratio`
- `daily_basic_snapshot_coverage_ratio`
- `fi_coverage_ratio`
- `inc_coverage_ratio`
- `bs_coverage_ratio`
- `cf_coverage_ratio`
- `fc_coverage_ratio`
- `ex_coverage_ratio`

覆盖率阈值告警：

- 市场层覆盖率
  - warning: `< 0.75`
  - error: `< 0.60`
- 财务层覆盖率
  - warning: `< 0.75`
  - error: `< 0.60`
- 公告事件层覆盖率
  - warning: `< 0.50`
  - error: `< 0.30`

当前默认阈值可通过环境变量覆盖：

- `PIT_QUALITY_MARKET_COVERAGE_WARN_RATIO`
- `PIT_QUALITY_MARKET_COVERAGE_ERROR_RATIO`
- `PIT_QUALITY_FINANCIAL_COVERAGE_WARN_RATIO`
- `PIT_QUALITY_FINANCIAL_COVERAGE_ERROR_RATIO`
- `PIT_QUALITY_EVENT_COVERAGE_WARN_RATIO`
- `PIT_QUALITY_EVENT_COVERAGE_ERROR_RATIO`

## 4. Severity Convention

- `error`: 阻断性问题，不应继续把该产物当作可信底座使用
- `warning`: 不阻断，但提示需要进一步关注的审计问题

## 5. Current Positioning

这套检查属于 Agent 1 的“收尾型质量基础设施”，当前已经足以支撑：

- 重建后快速回归检查
- 面试与项目展示中的质量控制说明
- 后续因子层和标签层的稳定输入保障

后续可继续增强的方向：

- 跨时间分区覆盖率对比
- 原始层到交付层的行数桥接审计
- 检查结果自动写入 `data/reports/` 或 markdown 质量报告

# `monthly_snapshot_base`

## 1. Role

`monthly_snapshot_base` 是 Agent 1 对 Agent 2 最核心的交付表。

它将同一 `rebalance_date` 下每只股票当时可用的：

- 市场价格
- 日频估值与流动性
- 财务报表
- 财务指标
- 业绩预告
- 业绩快报

整合成一条月频宽表记录，并严格遵守 point-in-time 约束。

## 2. Grain and Primary Key

- 行粒度：一行一个 `ts_code × rebalance_date`
- 主键：`rebalance_date + ts_code`

## 3. Upstream Lineage

- `data/lake/monthly_universe`
- `data/lake/adjusted_price_panel`
- `data/raw/daily_basic`
- `data/lake/calendar_table`
- `data/raw/fina_indicator`
- `data/raw/income`
- `data/raw/balancesheet`
- `data/raw/cashflow`
- `data/raw/forecast`
- `data/raw/express`

## 4. Core Identity and Audit Columns

| Field | Type | Nullable | Description |
| --- | --- | --- | --- |
| `rebalance_date` | `Date` | No | 月末再平衡日 |
| `trade_execution_date` | `Date` | No | 下一开市交易日 |
| `ts_code` | `String` | No | 股票代码 |
| `exchange` | `String` | Yes | 交易所 |
| `market` | `String` | Yes | 市场板块 |
| `list_date` | `Date` | Yes | 上市日期 |
| `delist_date` | `Date` | Yes | 退市日期 |
| `is_eligible` | `Boolean` | No | 是否来自最终研究池 |
| `exclude_reason` | `String` | Yes | 剔除原因 |
| `days_since_list` | `Int64` | Yes | 上市交易日龄 |
| `valid_trade_days_20d` | `Int64` | Yes | 最近 20 交易日有效天数 |
| `median_amount_20d` | `Float64` | Yes | 最近 20 交易日成交额中位数 |
| `has_price_coverage` | `Boolean` | Yes | 是否存在价格覆盖 |
| `is_st_flag` | `Boolean` | Yes | ST 标记 |
| `is_suspended_flag` | `Boolean` | Yes | 停牌标记 |

## 5. Market Snapshot Columns

| Field | Type | Nullable | Description |
| --- | --- | --- | --- |
| `price_trade_date` | `Date` | Yes | 本期实际使用的行情快照日，必须 `<= rebalance_date` |
| `daily_basic_trade_date` | `Date` | Yes | 本期实际使用的日频估值快照日，必须 `<= rebalance_date` |
| `close` | `Float64` | Yes | 原始收盘价 |
| `adj_close` | `Float64` | Yes | 调整收盘价 |
| `total_mv` | `Float64` | Yes | 总市值 |
| `circ_mv` | `Float64` | Yes | 流通市值 |
| `pb` | `Float64` | Yes | 市净率 |
| `pe_ttm` | `Float64` | Yes | 市盈率 TTM |
| `ps_ttm` | `Float64` | Yes | 市销率 TTM |
| `dv_ttm` | `Float64` | Yes | 股息率 TTM |
| `turnover_rate` | `Float64` | Yes | 换手率 |
| `turnover_rate_f` | `Float64` | Yes | 自由流通换手率 |
| `volume_ratio` | `Float64` | Yes | 量比 |
| `amount` | `Float64` | Yes | 成交额 |
| `vol` | `Float64` | Yes | 成交量 |

## 6. Financial and Event Column Families

本表采用前缀分族管理，避免字段冲突，同时保留来源语义。

### 6.1 `fi_*` : `fina_indicator`

保证至少包含：

- `fi_report_period`
- `fi_ann_date`
- `fi_availability_date`
- `fi_tradable_date`

示例字段：

- `fi_roe`
- `fi_roa`
- `fi_gross_margin`
- `fi_assets_yoy`

### 6.2 `inc_*` : `income`

保证至少包含：

- `inc_report_period`
- `inc_ann_date`
- `inc_f_ann_date`
- `inc_availability_date`
- `inc_tradable_date`

示例字段：

- `inc_revenue`
- `inc_operate_profit`
- `inc_n_income_attr_p`

### 6.3 `bs_*` : `balancesheet`

保证至少包含：

- `bs_report_period`
- `bs_ann_date`
- `bs_f_ann_date`
- `bs_availability_date`
- `bs_tradable_date`

示例字段：

- `bs_total_assets`
- `bs_total_liab`
- `bs_total_hldr_eqy_exc_min_int`

### 6.4 `cf_*` : `cashflow`

保证至少包含：

- `cf_report_period`
- `cf_ann_date`
- `cf_f_ann_date`
- `cf_availability_date`
- `cf_tradable_date`

示例字段：

- `cf_n_cashflow_act`
- `cf_n_cash_flows_fnc_act`
- `cf_c_pay_for_taxes`

### 6.5 `fc_*` : `forecast`

保证至少包含：

- `fc_report_period`
- `fc_ann_date`
- `fc_availability_date`
- `fc_tradable_date`
- `fc_first_ann_date`

示例字段：

- `fc_type`
- `fc_p_change_min`
- `fc_p_change_max`
- `fc_net_profit_min`
- `fc_net_profit_max`

### 6.6 `ex_*` : `express`

保证至少包含：

- `ex_report_period`
- `ex_ann_date`
- `ex_availability_date`
- `ex_tradable_date`

示例字段：

- `ex_revenue`
- `ex_n_income`
- `ex_diluted_eps`
- `ex_yoy_net_profit`

## 7. Join Rules

### 7.1 Market Data

- `adjusted_price_panel` 与 `daily_basic` 使用 `trade_date <= rebalance_date` 的 backward as-of join

### 7.2 Financial and Event Data

- `fina_indicator / income / balancesheet / cashflow / forecast / express`
- 统一使用 `<prefix>_tradable_date <= trade_execution_date` 的 backward as-of join

这意味着：

- 市场类字段以月末观察时点对齐
- 财务与公告事件字段以保守可交易时点对齐

## 8. Downstream Usage

- Agent 2 的月频因子输入宽表
- 估值、质量、投资、杠杆、事件类特征输入
- PIT 口径审计和覆盖率统计基表

## 9. Validation Checks

- `rebalance_date + ts_code` 唯一
- `price_trade_date <= rebalance_date`
- `daily_basic_trade_date <= rebalance_date`
- 所有 `fi/inc/bs/cf/fc/ex` 家族的 `tradable_date <= trade_execution_date`
- 任一主键重复或前视违规都应视为构建失败

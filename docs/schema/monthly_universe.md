# `monthly_universe`

## 1. Role

`monthly_universe` 定义每个再平衡日的研究样本空间。

它不是最终持仓表，而是：

- 月频横截面候选股票池
- 样本纳入/剔除原因审计表
- `monthly_snapshot_base` 的主索引上游

## 2. Grain and Primary Key

- 行粒度：一行一个 `ts_code × rebalance_date`
- 主键：`rebalance_date + ts_code`

## 3. Upstream Lineage

- 来源：
  - `data/raw/stock_basic`
  - `data/lake/calendar_table`
  - `data/lake/adjusted_price_panel`
- 构建逻辑：
  - 先生成每月 `rebalance_date`
  - 再对股票主数据、上市状态、近期行情覆盖和流动性进行过滤

## 4. Required Columns

| Field | Type | Nullable | Description |
| --- | --- | --- | --- |
| `rebalance_date` | `Date` | No | 月末再平衡日 |
| `trade_execution_date` | `Date` | No | 下一开市交易日 |
| `ts_code` | `String` | No | 股票代码 |
| `exchange` | `String` | Yes | 交易所 |
| `market` | `String` | Yes | 主板 / 创业板 / 科创板等 |
| `list_date` | `Date` | Yes | 上市日期 |
| `delist_date` | `Date` | Yes | 退市日期 |
| `days_since_list` | `Int64` | Yes | 截至 `rebalance_date` 的上市交易日龄 |
| `valid_trade_days_20d` | `Int64` | Yes | 最近 20 个交易日有效行情天数 |
| `median_amount_20d` | `Float64` | Yes | 最近 20 个交易日成交额中位数 |
| `has_price_coverage` | `Boolean` | Yes | 是否具有基础价格覆盖 |
| `is_st_flag` | `Boolean` | Yes | ST 标记，V1 可为空 |
| `is_suspended_flag` | `Boolean` | Yes | 停牌标记，V1 可为空 |
| `is_eligible` | `Boolean` | No | 是否进入最终研究池 |
| `exclude_reason` | `String` | Yes | 剔除原因代码 |
| `year` | `Int64` | No | 分区辅助字段 |
| `month` | `Int64` | No | 分区辅助字段 |

## 5. V1 Eligibility Logic

当前 V1 至少覆盖：

- 普通 A 股过滤
- 已上市且未退市过滤
- `days_since_list >= 120`
- 最近 20 交易日基础价格覆盖
- 最近 20 交易日流动性下限

## 6. `exclude_reason` Semantics

`exclude_reason` 是审计字段，不是面向最终模型的特征字段。

典型代码包括：

- `NOT_COMMON_A`
- `NOT_LISTED_YET`
- `DELISTED`
- `IPO_LT_120D`
- `NO_PRICE_HISTORY`
- `LOW_LIQUIDITY_20D`
- `MISSING_ADJ_FACTOR`

若 `is_eligible = True`，`exclude_reason` 应为空字符串或空值。

## 7. Downstream Usage

- 作为 Agent 2 的基础研究样本过滤表
- 作为 `monthly_snapshot_base` 的主键底座
- 作为样本覆盖率、纳入率与剔除原因统计的依据

## 8. Validation Checks

- `rebalance_date + ts_code` 唯一
- `trade_execution_date > rebalance_date`
- `is_eligible = False` 时建议存在 `exclude_reason`
- `days_since_list` 不得因为交易日历截断而出现系统性低估

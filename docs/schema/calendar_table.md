# `calendar_table`

## 1. Role

`calendar_table` 是全平台统一交易时钟。

它负责：

- 识别开市交易日
- 标记月末再平衡日
- 提供前后交易日映射
- 支持 `tradable_date` 与 `trade_execution_date` 的计算

## 2. Grain and Primary Key

- 行粒度：一行一个开市交易日
- 主键：`trade_date`

## 3. Upstream Lineage

- 来源：`data/raw/trade_cal`
- 构建逻辑：基于开市日记录补充 `prev_trade_date`、`next_trade_date`、`is_month_end`

## 4. Required Columns

| Field | Type | Nullable | Description |
| --- | --- | --- | --- |
| `trade_date` | `Date` | No | 当前开市交易日 |
| `prev_trade_date` | `Date` | Yes | 前一个开市交易日；首条记录可为空 |
| `next_trade_date` | `Date` | Yes | 后一个开市交易日；末条记录可为空 |
| `is_month_end` | `Boolean` | No | 是否为该自然月最后一个开市交易日 |
| `month` | `String` | No | 月份标签，格式为 `YYYY-MM` |

## 5. Business Rules

- 表中仅保留开市交易日，不保留休市日
- 每个自然月恰有一个 `is_month_end = True`
- `prev_trade_date < trade_date < next_trade_date` 在非首末行应成立

## 6. Downstream Usage

- 生成 `rebalance_date`
- 生成 `trade_execution_date`
- 计算财务/公告类 `tradable_date`
- 用作 forward-return 区间定位基表

## 7. Validation Checks

- `trade_date` 唯一
- `month` 非空
- 每月 `is_month_end` 数量等于 1

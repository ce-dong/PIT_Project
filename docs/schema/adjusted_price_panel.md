# `adjusted_price_panel`

## 1. Role

`adjusted_price_panel` 是日频复权价格底座。

它负责向下游提供：

- 可重复计算的调整价格序列
- forward return 标签输入
- 动量、反转、波动率等价格路径特征输入

## 2. Grain and Primary Key

- 行粒度：一行一个 `ts_code × trade_date`
- 主键：`ts_code + trade_date`

## 3. Upstream Lineage

- 来源：
  - `data/raw/daily`
  - `data/raw/adj_factor`
- 构建逻辑：
  - 先对齐 `daily` 与 `adj_factor`
  - 再生成统一的调整价格列

## 4. Required Columns

| Field | Type | Nullable | Description |
| --- | --- | --- | --- |
| `ts_code` | `String` | No | 股票代码 |
| `trade_date` | `Date` | No | 交易日 |
| `open` | `Float64` | Yes | 原始开盘价 |
| `high` | `Float64` | Yes | 原始最高价 |
| `low` | `Float64` | Yes | 原始最低价 |
| `close` | `Float64` | Yes | 原始收盘价 |
| `pre_close` | `Float64` | Yes | 前收盘价 |
| `change` | `Float64` | Yes | 涨跌额 |
| `pct_chg` | `Float64` | Yes | 涨跌幅 |
| `vol` | `Float64` | Yes | 成交量 |
| `amount` | `Float64` | Yes | 成交额，沿用源端单位 |
| `adj_factor` | `Float64` | Yes | 复权因子 |
| `adj_open` | `Float64` | Yes | `open * adj_factor` |
| `adj_high` | `Float64` | Yes | `high * adj_factor` |
| `adj_low` | `Float64` | Yes | `low * adj_factor` |
| `adj_close` | `Float64` | Yes | `close * adj_factor` |
| `year` | `Int64` | No | 分区辅助字段 |
| `month` | `Int64` | No | 分区辅助字段 |

## 5. Business Rules

- 不在本表中做停牌剔除，缺失交由下游处理
- 调整价格采用统一乘法定义，保证跨时间收益率可重复计算
- 同一主键只允许保留一条记录

## 6. Downstream Usage

- forward return 标签
- 动量 / 反转因子
- 波动率与流动性相关价格输入
- `monthly_universe` 近期价格覆盖与流动性过滤

## 7. Validation Checks

- `ts_code + trade_date` 唯一
- `adj_close` 与 `close` 同时缺失时应视为无价格覆盖
- 分区重建前后主键集合应一致

# Agent 1 -> Agent 2 Data Contract V1

## 1. 文档目的

本文档定义 Agent 1 交付给 Agent 2 的稳定中间表接口。

目标不是一次性穷尽所有字段，而是先明确：

- 哪些表必须稳定存在
- 每张表的主键和时间语义是什么
- 哪些列是必须列
- 哪些列允许在后续版本增补但不破坏兼容

## 2. 统一约定

### 2.1 字段命名

- 日期字段统一使用 `Date`
- 股票代码统一使用 `ts_code`
- 财务报告期统一命名为 `report_period`
- 布尔字段统一使用 `is_*`
- 审计/时间裁决字段统一显式命名，不隐含在备注里

### 2.2 数据类型

推荐采用 Polars / Arrow 兼容类型：

- 字符串：`Utf8`
- 日期：`Date`
- 布尔：`Boolean`
- 数值：`Float64` / `Int64`

### 2.3 排序与唯一性

- 所有表默认按主键升序排序后落地
- 每张交付表必须能定义清晰主键
- 若源表存在天然重复，必须在交付层完成去重或保留版本字段

### 2.4 分区建议

V1 建议按以下方式落地为 Parquet：

- 日频表：按 `trade_date` 所在年月分区
- 月频表：按 `rebalance_date` 所在年月分区

## 3. calendar_table

### 3.1 表用途

作为全平台统一交易时钟，供：

- 计算 `rebalance_date`
- 查询 `next_trade_date`
- 计算 `tradable_date`
- 标签层定位未来区间

### 3.2 主键

主键：`trade_date`

### 3.3 必须列

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `trade_date` | `Date` | 开市交易日 |
| `prev_trade_date` | `Date` | 上一个开市交易日 |
| `next_trade_date` | `Date` | 下一个开市交易日 |
| `is_month_end` | `Boolean` | 是否为当月最后一个开市交易日 |
| `month` | `Utf8` | 月份标签，格式建议 `YYYY-MM` |

### 3.4 约束

- 仅保留开市日
- `prev_trade_date < trade_date < next_trade_date`
- 每个自然月恰有一个 `is_month_end = true`

## 4. adjusted_price_panel

### 4.1 表用途

提供统一的复权价格底座，供：

- Agent 2 计算 forward return
- 动量 / 反转因子
- 基础收益校验

### 4.2 主键

主键：`ts_code + trade_date`

### 4.3 必须列

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `ts_code` | `Utf8` | 股票代码 |
| `trade_date` | `Date` | 交易日 |
| `open` | `Float64` | 原始开盘价 |
| `close` | `Float64` | 原始收盘价 |
| `pre_close` | `Float64` | 前收盘价 |
| `vol` | `Float64` | 成交量 |
| `amount` | `Float64` | 成交额，沿用源表单位 |
| `adj_factor` | `Float64` | 复权因子 |
| `adj_open` | `Float64` | 调整开盘价，定义为 `open * adj_factor` |
| `adj_close` | `Float64` | 调整收盘价，定义为 `close * adj_factor` |

### 4.4 可选列

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `high` | `Float64` | 原始最高价 |
| `low` | `Float64` | 原始最低价 |
| `pct_chg` | `Float64` | 涨跌幅 |
| `is_valid_price` | `Boolean` | 该日价格字段是否完整 |

### 4.5 约束

- 每个 `ts_code + trade_date` 唯一
- 若缺少 `adj_factor`，对应记录应显式标记，不允许静默填补

## 5. monthly_universe

### 5.1 表用途

定义每个再平衡日的股票池及其纳入/剔除原因，是 snapshot 的上游候选集，也是研究可审计性的核心表。

### 5.2 主键

主键：`rebalance_date + ts_code`

### 5.3 必须列

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `rebalance_date` | `Date` | 月末再平衡日 |
| `trade_execution_date` | `Date` | 下一开市交易日 |
| `ts_code` | `Utf8` | 股票代码 |
| `exchange` | `Utf8` | 交易所，建议由 `ts_code` 解析 |
| `market` | `Utf8` | 市场板块，如主板/创业板/科创板 |
| `list_date` | `Date` | 上市日期 |
| `delist_date` | `Date` | 退市日期，可为空 |
| `days_since_list` | `Int64` | 截至 `rebalance_date` 的上市交易日天数 |
| `is_eligible` | `Boolean` | 是否进入最终 universe |
| `exclude_reason` | `Utf8` | 剔除原因代码，多个原因用 `|` 连接；若纳入则为空 |

### 5.4 建议列

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `valid_trade_days_20d` | `Int64` | 最近 20 交易日有效行情天数 |
| `median_amount_20d` | `Float64` | 最近 20 交易日成交额中位数 |
| `has_price_coverage` | `Boolean` | 是否具备基础行情覆盖 |
| `is_st_flag` | `Boolean` | 是否命中特殊处理标记，V1 可为空 |
| `is_suspended_flag` | `Boolean` | 是否命中停牌相关标记，V1 可为空 |

### 5.5 `exclude_reason` 代码建议

- `NOT_COMMON_A`
- `NOT_LISTED_YET`
- `DELISTED`
- `IPO_LT_120D`
- `NO_PRICE_HISTORY`
- `LOW_LIQUIDITY_20D`
- `MISSING_ADJ_FACTOR`
- `FLAGGED_ST`
- `FLAGGED_SUSPEND`

### 5.6 约束

- 交付表既要保留纳入股票，也要保留被剔除股票
- Agent 2 默认使用 `is_eligible = true` 的记录做研究

## 6. monthly_snapshot_base

### 6.1 表用途

这是 Agent 1 对 Agent 2 最关键的交付表。它把某个 `rebalance_date` 下每只股票可用的市场、估值、财务、公告输入整合到一条宽表记录中。

### 6.2 主键

主键：`rebalance_date + ts_code`

### 6.3 必须列

#### 6.3.1 索引与审计列

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `rebalance_date` | `Date` | 月末再平衡日 |
| `trade_execution_date` | `Date` | 下一开市交易日 |
| `ts_code` | `Utf8` | 股票代码 |
| `is_eligible` | `Boolean` | 是否来自最终 universe |
| `price_trade_date` | `Date` | 本期使用的行情快照日期，必须 `<= rebalance_date` |

#### 6.3.2 市场基础列

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `close` | `Float64` | `price_trade_date` 对应原始收盘价 |
| `adj_close` | `Float64` | `price_trade_date` 对应调整收盘价 |
| `total_mv` | `Float64` | 总市值 |
| `circ_mv` | `Float64` | 流通市值 |
| `pb` | `Float64` | 市净率 |
| `pe_ttm` | `Float64` | 滚动市盈率 |
| `ps_ttm` | `Float64` | 滚动市销率 |
| `dv_ttm` | `Float64` | 股息率 TTM |
| `turnover_rate` | `Float64` | 换手率 |
| `turnover_rate_f` | `Float64` | 自由流通换手率 |
| `volume_ratio` | `Float64` | 量比 |
| `amount` | `Float64` | 成交额 |
| `vol` | `Float64` | 成交量 |

### 6.4 财务与公告列组织方式

V1 不要求在 data contract 文档中枚举所有财务字段，但要求采用稳定前缀规则，避免字段冲突并保留来源语义。

#### 6.4.1 income 前缀

所有利润表字段统一使用 `inc_` 前缀，且必须至少包含：

- `inc_report_period`
- `inc_ann_date`
- `inc_f_ann_date`
- `inc_availability_date`
- `inc_tradable_date`

其余原始字段按 `inc_<tushare_field>` 命名，例如：

- `inc_revenue`
- `inc_n_income_attr_p`

#### 6.4.2 balancesheet 前缀

资产负债表字段统一使用 `bs_` 前缀，且必须至少包含：

- `bs_report_period`
- `bs_ann_date`
- `bs_f_ann_date`
- `bs_availability_date`
- `bs_tradable_date`

#### 6.4.3 cashflow 前缀

现金流量表字段统一使用 `cf_` 前缀，且必须至少包含：

- `cf_report_period`
- `cf_ann_date`
- `cf_f_ann_date`
- `cf_availability_date`
- `cf_tradable_date`

#### 6.4.4 fina_indicator 前缀

财务指标表字段统一使用 `fi_` 前缀，且必须至少包含：

- `fi_report_period`
- `fi_ann_date`
- `fi_availability_date`
- `fi_tradable_date`

#### 6.4.5 forecast / express 前缀

- 业绩预告使用 `fc_` 前缀
- 业绩快报使用 `exp_` 前缀

至少保留：

- `*_report_period`
- `*_ann_date`
- `*_availability_date`
- `*_tradable_date`

### 6.5 约束

- 每个 `rebalance_date + ts_code` 只能有一行
- 所有财务/公告来源都必须满足对应 `*_tradable_date <= trade_execution_date`
- 若某来源本期无可用记录，对应前缀字段允许为空，但不能引用未来记录

## 7. 向后兼容规则

以下改动不视为破坏兼容：

- 新增可选字段
- 新增新的前缀字段族
- 新增质量标记列

以下改动视为破坏兼容，需同步升级版本：

- 修改主键定义
- 修改已有字段语义
- 修改日期裁决规则
- 删除必须列

## 8. Agent 2 的使用约定

Agent 2 默认只消费以下表：

- `calendar_table`
- `adjusted_price_panel`
- `monthly_universe`
- `monthly_snapshot_base`

Agent 2 不直接读取 raw 层，也不自行绕过 PIT 规则拼财务表。

## 9. 当前版本下的务实说明

### 9.1 Universe 的 ST / 停牌字段先允许为空

若 V1 尚未完成稳定的状态层，这些字段可以为空，但表结构先预留，避免后续改表成本过高。

### 9.2 财务字段采用“前缀族 + 审计列”优先

这比一开始就拍脑袋设计大而全的统一财务字段字典更稳，也更适合先把底座打通。


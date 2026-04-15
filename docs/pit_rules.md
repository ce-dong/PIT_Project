# Point-in-Time Rules

## 1. Scope

本文档定义 Agent 1 交付层采用的时间裁决规则，适用于：

- `calendar_table`
- `monthly_universe`
- `monthly_snapshot_base`
- 所有财务与公告类字段的 as-of join

## 2. Core Time Fields

### 2.1 `trade_date`

日频表的自然时间索引，表示某一开市交易日。

### 2.2 `rebalance_date`

定义为每个自然月最后一个开市交易日。

这是月频横截面形成时点，不是实际执行成交时点。

### 2.3 `trade_execution_date`

定义为 `rebalance_date` 之后的第一个开市交易日。

这是保守意义上的“组合执行日”，也是财务/公告信息可进入当期组合的最终截止时点。

### 2.4 `report_period`

统一对应源端 `end_date`，表示财务或公告事件所对应的报告期。

### 2.5 `ann_date`

源端公告日期。若没有更细时间戳，不能假设其在当日盘前可交易。

### 2.6 `f_ann_date`

若源表提供，则表示更贴近真实披露时点的公告日期。若存在，优先用作 `availability_date` 的来源。

### 2.7 `availability_date`

表示信息第一次进入公开可见范围的日期。

V1 规则：

- 行情类：`availability_date = trade_date`
- 财务类：`availability_date = coalesce(f_ann_date, ann_date)`
- 公告事件类：`availability_date = ann_date`

### 2.8 `tradable_date`

表示信息在保守口径下第一次可以进入交易决策的开市日。

V1 统一规则：

- `tradable_date = availability_date` 之后的第一个开市交易日
- 即严格使用“大于 availability_date 的下一交易日”

这条规则的核心目的是避免把“同日盘后公告”误当作“同日盘前已知”。

## 3. Market Data Join Rule

市场类输入包括：

- `adjusted_price_panel`
- `daily_basic`

Join 规则：

- 左表时间键：`rebalance_date`
- 右表时间键：`trade_date`
- 匹配方式：`trade_date <= rebalance_date` 的 backward as-of join

因此，市场快照反映的是：

- 月末收盘后已经观察到的价格、成交量、估值与流动性状态

## 4. Financial and Event Join Rule

财务与公告事件输入包括：

- `fina_indicator`
- `income`
- `balancesheet`
- `cashflow`
- `forecast`
- `express`

Join 规则：

- 左表时间键：`trade_execution_date`
- 右表时间键：对应表的 `<prefix>_tradable_date`
- 匹配方式：`tradable_date <= trade_execution_date` 的 backward as-of join

因此，这些字段进入当期 snapshot 的条件不是“公告日期早于月末”，而是：

- 在执行日前已经保守可交易

## 5. Version Selection Rule

当同一 `ts_code + report_period` 在源端存在多版本记录时：

1. 先筛选 `tradable_date <= trade_execution_date`
2. 再在可见记录中选择最新版本
3. 冲突时按以下顺序稳定裁决：
   - 更晚的 `report_period`
   - 更晚的 `ann_date`
   - 若存在则更晚的 `f_ann_date`
   - 若存在则 `update_flag` 更高优先

规则目标是：

- 保留历史修订轨迹于 raw 层
- 保证交付层在任一时间截面只有一条唯一记录

## 6. Universe Rule

`monthly_universe` 的时点规则是：

- 以 `rebalance_date` 为研究截面时点
- 以 `trade_execution_date` 作为当期组合的保守执行日

V1 当前包含：

- 普通 A 股过滤
- 上市不足 120 个交易日过滤
- 20 日行情覆盖过滤
- 20 日成交额中位数过滤
- 缺失价格/复权基础过滤

## 7. Non-Negotiable Checks

以下约束必须始终成立：

- `price_trade_date <= rebalance_date`
- `daily_basic_trade_date <= rebalance_date`
- `<financial/event>_tradable_date <= trade_execution_date`
- 同一 `rebalance_date + ts_code` 在 `monthly_snapshot_base` 中唯一

如果任一约束被破坏，应视为 PIT 口径失败，而不是普通数据缺失。

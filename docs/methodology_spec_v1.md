# A 股 PIT 横截面研究平台 Methodology Spec V1

## 1. 文档定位

本文档定义本项目 V1 的研究口径、时间规则和数据使用原则，作为 Agent 1 落地 `point-in-time` 月频底座的执行依据。

这是一版可落地的工程化草案，不追求把所有理想规则一次性写满，而是优先保证：

- 无前视偏差
- 可重复生成
- 规则可解释
- 接口可稳定交付给 Agent 2

如后续发现 Tushare 实际字段、覆盖率或更新行为与草案存在冲突，应以“研究可信 + 规则清晰 + 可复现”为优先原则做有限调整，而不是机械照抄大纲。

## 2. V1 研究范围

- 市场范围：沪深 A 股普通股
- 研究频率：月频
- 再平衡方式：每月一次
- 主数据源：Tushare Pro
- V1 优先表：
  - `stock_basic`
  - `trade_cal`
  - `daily`
  - `daily_basic`
  - `adj_factor`
  - `income`
  - `balancesheet`
  - `cashflow`
  - `fina_indicator`
  - `forecast`
  - `express`

基于 2026-04-12 的实际抽样，已确认上述核心表至少包含以下关键字段：

- `trade_cal`: `cal_date`, `is_open`, `pretrade_date`
- `daily`: `trade_date`, `open`, `high`, `low`, `close`, `pre_close`, `vol`, `amount`
- `daily_basic`: `turnover_rate`, `turnover_rate_f`, `pe_ttm`, `pb`, `ps_ttm`, `dv_ttm`, `total_mv`, `circ_mv`
- `adj_factor`: `trade_date`, `adj_factor`
- 财务表核心时间字段：`ann_date`, `f_ann_date`, `end_date`, `update_flag`
- `forecast`: `ann_date`, `end_date`, `type`, `p_change_min`, `p_change_max`
- `express`: `ann_date`, `end_date`

## 3. 核心原则

### 3.1 Point-in-Time 优先

任意 `rebalance_date` 的横截面，只能使用在该时点之前已经“可知且可交易”的信息。

### 3.2 保守处理优先

当 Tushare 只有日期、没有公告具体时刻时，默认采用保守规则，避免把“同日盘后公告”错误地当成“当日盘前已知”。

### 3.3 原始层保留、标准层约束、PIT 层裁决

- 原始层尽量保留源表信息，不做过强裁剪
- 标准化层统一字段和类型
- PIT 层负责决定某条记录何时进入研究视角

### 3.4 数据契约优先于临时脚本

Agent 1 对 Agent 2 的交付以稳定表为准，Agent 2 不直接消费 raw data。

## 4. 核心时间字段定义

### 4.1 基础字段

- `trade_date`: 交易日，来自行情或交易日历
- `report_period`: 财务报告期，统一对应 Tushare 的 `end_date`
- `ann_date`: 公告日期
- `f_ann_date`: 实际公告日期；若源表提供，则优先作为更贴近真实可得时间的字段
- `rebalance_date`: 每月最后一个开市交易日
- `trade_execution_date`: `rebalance_date` 的下一个开市交易日

### 4.2 availability_date

`availability_date` 表示“信息第一次进入公开可见范围的日期”，V1 定义如下：

- 行情类表 `daily` / `daily_basic` / `adj_factor`：
  - `availability_date = trade_date`
- 财务与公告类表：
  - `availability_date = coalesce(f_ann_date, ann_date)`
- `stock_basic`：
  - 上市生效时间使用 `list_date`
  - 退市生效时间使用 `delist_date`

### 4.3 tradable_date

`tradable_date` 表示“该信息保守起见可以进入交易决策的首个交易日”。

V1 统一采用保守规则：

- 对公告驱动型信息：
  - `tradable_date = availability_date` 之后的第一个开市交易日
  - 即严格取“大于 availability_date 的下一交易日”
- 对行情类表：
  - 行情记录本身不单独维护 `tradable_date`
  - `rebalance_date` 当天收盘后可用于形成组合，真正执行在 `trade_execution_date`

这样处理的原因是：Tushare 多数表只有日期，没有盘中/盘后时间戳。若直接令 `tradable_date = ann_date`，会高估信息可用性。

## 5. 月频再平衡口径

### 5.1 rebalance_date

`rebalance_date` 定义为每个自然月最后一个开市交易日，基于 `trade_cal` 生成。

### 5.2 组合形成与执行

月频研究采用以下节奏：

1. 在 `rebalance_date` 收盘后汇总可用信息
2. 形成下一期持仓
3. 在 `trade_execution_date` 执行

因此，PIT 截面的可用性判断统一以 `trade_execution_date` 为最终交易可用截止点。

### 5.3 snapshot 截止规则

对任意 `rebalance_date`：

- 行情类输入允许使用 `trade_date <= rebalance_date` 的最后一条记录
- 公告/财务类输入允许使用 `tradable_date <= trade_execution_date` 的最新记录

该规则允许“在月末交易日公告、次日可交易”的信息进入当月形成的组合，但不会提前进入更早的组合。

## 6. 数据源口径与 V1 实施规则

### 6.1 stock_basic

V1 不只拉取当前上市股票，必须同时拉取不同 `list_status` 状态的数据并统一落地，避免幸存者偏差。

标准规则：

- 拉取 `list_status in ('L', 'D', 'P')`
- 基础 universe 仅纳入沪深普通股
- V1 默认排除北交所、B 股、基金、指数及其他非普通股资产

建议优先依据以下条件过滤：

- `ts_code` 后缀为 `.SH` 或 `.SZ`
- `market` 属于 `主板`、`创业板`、`科创板`

### 6.2 trade_cal

`trade_cal` 作为交易时钟基表使用。由于 Tushare 原生提供 `pretrade_date` 但不直接提供 `next_trade_date`，平台需要在标准化层自行补充：

- `prev_trade_date`
- `next_trade_date`
- `is_month_end`

`calendar_table` 只保留开市日记录，便于后续 `as-of join` 和 next-trading-day 查询。

### 6.3 daily / daily_basic / adj_factor

V1 对行情相关表的处理规则：

- 同一股票同一 `trade_date` 视为唯一记录
- 若个别字段缺失，不删除整条记录，交由下游在因子层判断覆盖率
- 价格调整使用 `daily` + `adj_factor`

### 6.4 财务表与公告类表

V1 纳入以下表：

- `income`
- `balancesheet`
- `cashflow`
- `fina_indicator`
- `forecast`
- `express`

统一规则：

- `end_date` 标准化为 `report_period`
- `coalesce(f_ann_date, ann_date)` 作为 `availability_date`
- 通过交易日历映射得到 `tradable_date`

## 7. 财务记录去重与版本保留规则

根据实际抽样，`income` / `balancesheet` / `cashflow` 中存在同一 `ts_code + end_date` 多条记录并存的情况，且 `update_flag` 可能不同。

V1 规则不是简单“每报告期只留一条”，而是分两层处理：

### 7.1 原始标准层

原始标准层保留所有源端版本，只做字段统一和类型清洗，不提前裁掉真实修订记录。

### 7.2 PIT 使用层

在给定 cutoff 做 `as-of join` 时：

- 先按 `tradable_date <= cutoff` 过滤
- 再按 `ts_code` 分组，取最近可用的报告记录
- 若同一 `ts_code + report_period + tradable_date` 仍有多条冲突记录，按以下顺序稳定去重：
  - 优先 `f_ann_date` 非空
  - 优先更晚的 `f_ann_date`
  - 再优先更晚的 `ann_date`
  - 再优先 `update_flag = '1'`
  - 最后使用稳定行哈希作为最终 tie-breaker

这条规则的目标是：

- 不丢掉真正的历史修订轨迹
- 同时确保同一时点重复生成结果一致

## 8. Universe 规则

V1 Universe 采用“先尽量做对，再逐步扩展”的思路。

### 8.1 基础纳入条件

某股票在 `rebalance_date` 纳入基础池需满足：

- 属于沪深普通股
- 已上市且未退市
- 在 `rebalance_date` 之前已有可用交易记录

### 8.2 新股过滤

V1 默认过滤上市未满 120 个交易日的股票。

理由：

- 避免 IPO 初期价格行为和流动性异常显著扭曲横截面
- 该规则简单、稳健、研究中常见

### 8.3 流动性过滤

V1 使用最近 20 个交易日的日频行情做基础流动性过滤，默认条件：

- 最近 20 个交易日中，至少有 15 个有效交易日
- 最近 20 个交易日的成交额中位数不低于 20,000 千元

这里沿用 Tushare `daily.amount` 的原始单位。

### 8.4 ST / PT / 停牌处理

这部分在大纲中被列为可选项，V1 采取务实策略：

- `suspend_d`、`namechange` 等表先作为辅助数据源
- 若其 PIT 覆盖与时间口径不足以支持稳定硬过滤，则先输出标记，不直接做硬排除
- 等专门的状态层完成后，再升级为正式 universe 过滤规则

这属于“按大纲方向落地，但不死磕不稳定口径”的典型场景。

## 9. 调整价格与收益底座

V1 使用 `daily` 与 `adj_factor` 生成标准价格底座。

定义：

- `adj_close = close * adj_factor`
- `adj_open = open * adj_factor`

说明：

- 该定义的绝对尺度不重要，关键是同一股票跨时点使用同一调整序列
- 未来收益、动量、反转等研究所需的相对收益率在该定义下可重复计算

## 10. 因子预处理与中性化口径

V1.5 起，Agent 2 在因子横截面预处理中采用统一的顺序化流程：

1. `winsorize`
2. `industry_neutralize`
3. `size_neutralize`
4. `zscore`

该流程的目标是：

- 先抑制极端值
- 再去除横截面的行业公共成分
- 再剔除线性市值暴露
- 最后得到跨期可比较的标准化因子

### 10.1 winsorize

在每个 `rebalance_date` 横截面上，基于 `is_eligible = True` 的样本，按默认分位数：

- 下界 `1%`
- 上界 `99%`

对因子原始值进行截尾。

### 10.2 行业中性化

行业中性化采用组内去均值的方式：

- 在每个 `rebalance_date` 横截面单独执行
- 优先使用 `industry` 字段分组
- 若 `industry` 在当前底座中缺失，则回退使用 `market` 字段分组
- 对每个组分别计算组内因子均值
- 中性化后因子值定义为：
  - `factor_neutral = factor - mean(factor | group)`

这一步移除的是横截面中由行业或板块共同驱动的公共成分，保留组内相对差异。

### 10.3 市值中性化

市值中性化采用横截面线性残差化：

- 在每个 `rebalance_date` 横截面单独执行
- 令：
  - `x = log(total_mv)`
  - `y = 当前因子值`
- 在当期横截面上拟合：
  - `y = alpha + beta * x`
- 市值中性化后的结果定义为：
  - `residual = y - (alpha + beta * x)`

实现层面仅对 `total_mv > 0` 且横截面有效样本足够的记录执行；若当期市值暴露缺失严重，或 `log(total_mv)` 几乎无横截面变化，则跳过该步而不强行回归。

### 10.4 z-score 标准化

在完成截尾和中性化后，再基于当期横截面做标准化：

- `z = (x - mean) / std`

若当期有效样本为空，或标准差为 0，则该期因子标准化结果记为缺失。
- 若后续展示层需要“归一化复权价”，可再增加派生列，但不影响底座一致性

## 10. 月频 PIT Snapshot 生成规则

对每个 `rebalance_date × ts_code`，`monthly_snapshot_base` 采用如下逻辑：

1. 从 `monthly_universe` 拿到该期候选股票
2. 对行情类数据：
   - 取 `trade_date <= rebalance_date` 的最近记录
3. 对财务/公告类数据：
   - 取 `tradable_date <= trade_execution_date` 的最近记录
4. 将不同来源数据拼成一条宽表记录
5. 保留关键审计字段，至少包括：
   - `rebalance_date`
   - `trade_execution_date`
   - `price_trade_date`
   - 各来源的 `report_period`
   - 各来源的 `availability_date`
   - 各来源的 `tradable_date`

## 11. 质量控制与验收检查

Agent 1 至少要保证以下检查可执行：

- `availability_date <= tradable_date`
- `price_trade_date <= rebalance_date`
- 财务输入不允许 `tradable_date > trade_execution_date` 仍进入该期 snapshot
- `calendar_table` 中 `next_trade_date` 正确
- `rebalance_date` 为当月最后一个开市日
- 同一输入数据下重复生成结果一致

## 12. 当前已知限制与后续扩展

### 12.1 Tushare 时间粒度限制

多数公告类表没有盘中时间戳，因此 V1 只能采用“公告日后首个交易日才可交易”的保守规则。

### 12.2 Universe 状态过滤先做稳、后做全

ST/PT、长期停牌、特殊处理股票的 PIT 判定在 V1 先输出标记，不强求一步到位。

### 12.3 财务字段跨行业可比性问题

银行、券商、保险的财务字段与工业企业差异较大。V1 底座先保留原始输入与时间规则，不在 Agent 1 阶段做行业特化重写。

## 13. Agent 1 实施优先级

建议按以下顺序落地：

1. `trade_cal`
2. `stock_basic`
3. `daily`
4. `daily_basic`
5. `adj_factor`
6. `calendar_table`
7. `adjusted_price_panel`
8. `monthly_universe`
9. 财务表标准化与 PIT 规则
10. `monthly_snapshot_base`

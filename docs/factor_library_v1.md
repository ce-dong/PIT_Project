# Factor Library V1

## 1. 文档定位

本文档汇总 Agent 2 当前已经实现的 V1 因子库，用于回答以下问题：

- 当前平台已经支持哪些因子
- 每个因子的经济含义是什么
- 它依赖哪些上游字段
- 它的 point-in-time 使用口径是什么
- 它最终如何进入 `factor_panel`

当前全部因子都通过 [src/features/registry.py](/Users/tong/PIT_Project/src/features/registry.py) 注册，并由统一的因子计算链路写入 `data/panel/<experiment_slug>/factor_panel/`。

## 2. 统一输出约定

- 原始值字段：`<output_field>_raw`
- 预处理后字段：`<output_field>`
- 默认预处理链路：
  - 大多数因子：`winsorize -> industry_neutralize -> size_neutralize -> zscore`
  - `size`：`winsorize -> industry_neutralize -> zscore`

说明：

- `industry_neutralize` 优先按 `industry` 去组内均值；若底座没有该字段，则回退到 `market`
- `size_neutralize` 对 `log(total_mv)` 做横截面残差化

## 3. 因子总览

| Family | Factor | Output Field | Formula Sketch |
| --- | --- | --- | --- |
| valuation | `size` | `factor_size` | `log(total_mv)` |
| valuation | `book_to_market` | `factor_book_to_market` | `1 / pb` |
| valuation | `earnings_to_price` | `factor_earnings_to_price` | `1 / pe_ttm` |
| valuation | `sales_to_price` | `factor_sales_to_price` | `1 / ps_ttm` |
| valuation | `cashflow_to_price` | `factor_cashflow_to_price` | `cf_n_cashflow_act / total_mv` |
| momentum | `momentum_12_1` | `factor_momentum_12_1` | `P(t-1) / P(t-12) - 1` |
| momentum | `momentum_6_1` | `factor_momentum_6_1` | `P(t-1) / P(t-6) - 1` |
| momentum | `momentum_3_1` | `factor_momentum_3_1` | `P(t-1) / P(t-3) - 1` |
| momentum | `reversal_1m` | `factor_reversal_1m` | `-(P(t) / P(t-1) - 1)` |
| risk_liquidity | `beta` | `factor_beta` | `cov(ret_i, ret_mkt) / var(ret_mkt)` |
| risk_liquidity | `volatility` | `factor_volatility` | `std(daily_ret)` |
| risk_liquidity | `turnover` | `factor_turnover` | latest `turnover_rate_f` or `turnover_rate` |
| risk_liquidity | `amihud_illiquidity` | `factor_amihud_illiquidity` | `mean(abs(ret_d) / amount_d)` |
| risk_liquidity | `idiosyncratic_volatility` | `factor_idiosyncratic_volatility` | residual vol from market model |
| quality | `roe` | `factor_roe` | `fi_roe` |
| quality | `roa` | `factor_roa` | `fi_roa` |
| quality | `gross_profitability` | `factor_gross_profitability` | `(inc_revenue - inc_oper_cost) / bs_total_assets` |
| quality | `gross_margin` | `factor_gross_margin` | `fi_gross_margin` |
| quality | `operating_cash_flow_to_assets` | `factor_operating_cash_flow_to_assets` | `cf_n_cashflow_act / bs_total_assets` |
| quality | `accruals` | `factor_accruals` | `(inc_n_income_attr_p - cf_n_cashflow_act) / bs_total_assets` |
| investment | `asset_growth` | `factor_asset_growth` | `bs_total_assets / lag_12m_assets - 1` |
| investment | `inventory_growth` | `factor_inventory_growth` | `bs_inventories / lag_12m_inventories - 1` |
| investment | `leverage` | `factor_leverage` | `bs_total_liab / bs_total_assets` |
| investment | `net_operating_assets` | `factor_net_operating_assets` | `(assets - cash - liabilities) / assets` |

## 4. 分家族说明

### 4.1 Valuation

#### `size`

- 含义：公司规模暴露
- 公式：`log(total_mv)`
- 输入字段：
  - `monthly_snapshot_base.total_mv`
- PIT 规则：
  - 使用 `trade_date <= rebalance_date` 的最新市场快照
- 预处理：
  - `winsorize -> industry_neutralize -> zscore`

#### `book_to_market`

- 含义：账面市值比代理
- 公式：`1 / pb`
- 输入字段：
  - `monthly_snapshot_base.pb`
- PIT 规则：
  - 使用 `trade_date <= rebalance_date` 的最新市场快照

#### `earnings_to_price`

- 含义：盈利收益率代理
- 公式：`1 / pe_ttm`
- 输入字段：
  - `monthly_snapshot_base.pe_ttm`

#### `sales_to_price`

- 含义：销售收益率代理
- 公式：`1 / ps_ttm`
- 输入字段：
  - `monthly_snapshot_base.ps_ttm`

#### `cashflow_to_price`

- 含义：经营现金流收益率
- 公式：`cf_n_cashflow_act / total_mv`
- 输入字段：
  - `monthly_snapshot_base.cf_n_cashflow_act`
  - `monthly_snapshot_base.total_mv`
- PIT 规则：
  - 现金流字段使用 `cf_tradable_date <= trade_execution_date`
  - 市值字段使用 `trade_date <= rebalance_date`

### 4.2 Momentum / Reversal

#### `momentum_12_1`

- 含义：12-1 月动量
- 公式：`P(t-1) / P(t-12) - 1`
- 输入字段：
  - `adjusted_price_panel.adj_close`
  - `calendar_table.trade_date`
- PIT 规则：
  - 只使用 `trade_date <= rebalance_date` 的历史价格
  - 跳过最近 1 个月窗口

#### `momentum_6_1`

- 含义：6-1 月动量
- 公式：`P(t-1) / P(t-6) - 1`

#### `momentum_3_1`

- 含义：3-1 月动量
- 公式：`P(t-1) / P(t-3) - 1`

#### `reversal_1m`

- 含义：1 个月短期反转
- 公式：`-(P(t) / P(t-1) - 1)`

### 4.3 Risk / Liquidity

#### `beta`

- 含义：个股相对市场的滚动 beta
- 估计窗口：
  - 252 个交易日
  - 最少有效观测数 120
- 输入字段：
  - `adjusted_price_panel.adj_close`

#### `volatility`

- 含义：滚动实现波动率
- 估计窗口：
  - 252 个交易日

#### `turnover`

- 含义：换手强度代理
- 实现方式：
  - 优先使用 `turnover_rate_f`
  - 缺失时回退到 `turnover_rate`
- 输入字段：
  - `monthly_snapshot_base.turnover_rate`
  - `monthly_snapshot_base.turnover_rate_f`

#### `amihud_illiquidity`

- 含义：Amihud 非流动性
- 公式：过去窗口内 `mean(abs(ret_d) / amount_d)`
- 输入字段：
  - `adjusted_price_panel.adj_close`
  - `monthly_snapshot_base.amount`

#### `idiosyncratic_volatility`

- 含义：市场模型残差波动率
- 实现方式：
  - 先估滚动 beta
  - 再从总方差中扣除市场解释部分

### 4.4 Quality

#### `roe`

- 含义：净资产收益率
- 输入字段：
  - `monthly_snapshot_base.fi_roe`

#### `roa`

- 含义：总资产收益率
- 输入字段：
  - `monthly_snapshot_base.fi_roa`

#### `gross_profitability`

- 含义：毛利 / 总资产
- 公式：`(inc_revenue - inc_oper_cost) / bs_total_assets`

#### `gross_margin`

- 含义：毛利率
- 输入字段：
  - `monthly_snapshot_base.fi_gross_margin`

#### `operating_cash_flow_to_assets`

- 含义：经营现金流 / 总资产
- 公式：`cf_n_cashflow_act / bs_total_assets`

#### `accruals`

- 含义：应计项代理
- 公式：`(inc_n_income_attr_p - cf_n_cashflow_act) / bs_total_assets`

### 4.5 Investment / Leverage

#### `asset_growth`

- 含义：资产增长率
- 公式：`bs_total_assets / lag_12m_assets - 1`

#### `inventory_growth`

- 含义：存货增长率
- 公式：`bs_inventories / lag_12m_inventories - 1`

#### `leverage`

- 含义：杠杆率
- 公式：`bs_total_liab / bs_total_assets`

#### `net_operating_assets`

- 含义：净经营资产占比
- 公式：`(bs_total_assets - bs_money_cap - bs_total_liab) / bs_total_assets`

## 5. 当前实现边界

- 行业中性化目前优先依赖 `industry`，缺失时回退到 `market`
- `turnover` 当前使用月末最新 snapshot，而不是完整滚动均值
- 风险类因子使用简单市场平均收益构造市场因子，尚未引入更完整的风险模型
- V1 因子以经典横截面研究因子为主，暂未扩展到分析师预期和事件驱动因子

## 6. 使用建议

- 若要查看机器可读定义，优先使用：
  - `python -m src.cli research factors`
- 若要查看真实实验结果，配合阅读：
  - [docs/sample_experiment_agent2_baseline.md](sample_experiment_agent2_baseline.md)
  - [data/experiments/agent2_baseline/reports/research_report.md](/Users/tong/PIT_Project/data/experiments/agent2_baseline/reports/research_report.md)

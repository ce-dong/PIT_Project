# A股 Point-in-Time 横截面因子研究平台

[English Version](READMEen.md)

## 项目简介

这是一个面向 A 股中低频量化研究的月频横截面研究平台，目标是在每个再平衡时点，严格基于当时可获得的信息构建股票横截面特征，并对未来收益进行标准化评估。

项目核心不是“多算几个因子”，而是把下面这条链路做成可复现、可扩展、可审计的研究系统：

- 多源数据接入与本地落地
- Point-in-Time 时点还原
- 月频 universe 与 snapshot 构建
- 因子计算与统一预处理
- 未来收益标签生成
- 标准化评估与稳健性分析
- 自动研究报告输出

这个项目不以实盘交易、高频回测或超大因子 zoo 为目标，而是聚焦“研究口径正确、流程统一、结果可复现”。

## 当前状态

当前版本已经完成 Agent 1 与 Agent 2 的 V1 主线能力，形成了从 PIT 底座到研究报告的一键实验闭环。

已完成的核心模块包括：

- 原始数据接入与 Parquet 落地
- `calendar_table`
- `adjusted_price_panel`
- `monthly_universe`
- `monthly_snapshot_base`
- 因子注册中心与 24 个核心因子实现
- `fwd_ret_1m` / `fwd_ret_3m` / `fwd_ret_6m` 标签生成
- `Rank IC`
- `quantile portfolio`
- `top-bottom spread`
- 单调性分析
- `Fama-MacBeth`
- 因子相关矩阵
- 冗余性分析
- 分时期稳健性分析
- Markdown 研究报告与图表输出

## 已实现的因子库

当前 V1 因子共 24 个，分为 5 个家族：

- 估值类：`size`、`book_to_market`、`earnings_to_price`、`sales_to_price`、`cashflow_to_price`
- 动量/反转类：`momentum_12_1`、`momentum_6_1`、`momentum_3_1`、`reversal_1m`
- 风险/流动性类：`beta`、`volatility`、`turnover`、`amihud_illiquidity`、`idiosyncratic_volatility`
- 盈利能力/质量类：`roe`、`roa`、`gross_profitability`、`gross_margin`、`operating_cash_flow_to_assets`、`accruals`
- 投资/杠杆类：`asset_growth`、`inventory_growth`、`leverage`、`net_operating_assets`

统一预处理口径当前支持：

- 横截面 winsorize
- 横截面 z-score
- 覆盖率统计

## Point-in-Time 研究口径

平台当前采用以下关键时点规则：

- `rebalance_date`：每个自然月最后一个开市交易日
- `trade_execution_date`：`rebalance_date` 之后的下一个开市交易日
- 行情类数据：使用 `trade_date <= rebalance_date` 的最新可见记录
- 财务/事件类数据：使用 `<prefix>_tradable_date <= trade_execution_date` 的最新可交易记录
- 保守可交易规则：`tradable_date` 为 `availability_date` 之后的第一个开市日

这个口径的目标是避免未来信息泄漏，特别是避免把“当天公告但盘中是否可交易不明确”的信息过早纳入横截面。

## 核心数据契约

Agent 2 只消费 Agent 1 交付的标准化表，不直接依赖 raw data。

### `calendar_table`

- `trade_date`
- `prev_trade_date`
- `next_trade_date`
- `is_month_end`

### `adjusted_price_panel`

- `ts_code`
- `trade_date`
- `adj_open`
- `adj_close`
- `adj_factor`

### `monthly_universe`

- `rebalance_date`
- `trade_execution_date`
- `ts_code`
- `is_eligible`
- `exclude_reason`

### `monthly_snapshot_base`

- `rebalance_date`
- `ts_code`
- 行情、估值、财务、事件等 PIT 输入字段

## 研究输出

当前实验会产出如下研究结果：

- `factor_panel`
- `label_panel`
- `rank_ic_timeseries`
- `rank_ic_summary`
- `quantile_returns`
- `top_bottom_spread_timeseries`
- `monotonicity_summary`
- `evaluation_summary`
- `fama_macbeth_summary`
- `factor_correlation_matrix`
- `redundancy_summary`
- `robustness_summary`
- `research_report.md`
- 报告图表 PNG

一个真实实验样例目录位于：

- `data/experiments/agent2_baseline/`

## 仓库结构

```text
configs/
docs/
  data_contract.md
  pit_rules.md
  quality_checks.md
  schema/
src/
  adapters/tushare/
  builders/
  core/
  evaluation/
  features/
  labels/
  reports/
  research/
  storage/
  updaters/
  validators/
tests/
data/
  raw/           # ignored
  lake/          # ignored
  panel/         # ignored
  experiments/   # ignored
  reports/       # ignored
```

## 快速上手

### 1. 安装依赖

建议使用项目对应的 Conda 环境，并安装 `requirements.txt`：

```bash
/Users/tong/miniconda3/envs/pit_env/bin/pip install -r requirements.txt
```

### 2. 配置环境变量

项目默认通过 `.env` 读取 `TUSHARE_TOKEN`。

### 3. 拉取原始数据

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli ingest all --start 20150101 --full-refresh
```

### 4. 构建 PIT 底座

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli build all
```

### 5. 运行质量校验

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli validate all --write-report
```

### 6. 初始化研究实验

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research init --name "Agent2 Baseline"
```

### 7. 构建因子、标签、评估和报告

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research build-factors --name "Agent2 Baseline"
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research build-labels --name "Agent2 Baseline"
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research build-evaluation --name "Agent2 Baseline"
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research build-report --name "Agent2 Baseline"
```

## 常用研究命令

### 查看已注册因子

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research factors
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research factors --family momentum
```

### 查看已注册标签

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research labels
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research labels --stage forward_return
```

### 只运行部分因子或标签

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research build-factors --name "Agent2 Baseline" --factor momentum_12_1 --factor size
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research build-labels --name "Agent2 Baseline" --label fwd_ret_1m
```

## 质量控制

当前质量校验覆盖：

- 主键唯一性
- 必需字段检查
- 分区 schema 一致性
- 交易日历关联一致性
- 复权价格公式一致性
- universe 与 snapshot 主键对齐
- PIT 截断规则校验
- 覆盖率阈值预警

相关文档位于：

- [docs/quality_checks.md](docs/quality_checks.md)
- [docs/pit_rules.md](docs/pit_rules.md)
- [docs/data_contract.md](docs/data_contract.md)

## 测试

运行完整测试：

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m pytest
```

## 方法与展示价值

这个项目主要展示以下能力：

- 金融数据工程与本地数据湖构建
- Point-in-Time 研究口径设计
- 横截面因子建模与标签生成
- 标准化因子评估框架
- 自动化研究报告与实验管理

如果需要进一步扩展，后续方向可以包括：

- 行业中性与市值中性增强
- 更丰富的事件因子与预期因子
- 更完整的 README 图表展示
- 面向简历和作品集的样例实验精修

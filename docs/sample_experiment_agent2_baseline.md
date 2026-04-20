# Sample Experiment: `agent2_baseline`

## 1. 文档定位

本文档用于说明当前仓库中最完整的一套样例实验 `agent2_baseline`：

- 它用了什么输入
- 它输出了什么产物
- 应该按什么顺序阅读
- 当前样例可以得出哪些高层结论

对应实验目录：

- [data/experiments/agent2_baseline](/Users/tong/PIT_Project/data/experiments/agent2_baseline)

## 2. 实验目的

`agent2_baseline` 不是一个“最佳策略”，而是一个标准化样例，用来展示这套平台已经具备的完整研究能力：

- 从 PIT 月频底座读取输入
- 生成标准 `factor_panel`
- 生成 `label_panel`
- 运行 Rank IC、分组收益、Fama-MacBeth、稳健性与冗余分析
- 自动输出 markdown 报告和图表

它更像一套“研究样板间”。

## 3. 实验输入

### 3.1 底层表

`agent2_baseline` 只使用 Agent 1 交付的四张标准表：

- `calendar_table`
- `adjusted_price_panel`
- `monthly_universe`
- `monthly_snapshot_base`

### 3.2 因子与标签

- 因子数量：24
- 标签数量：3
- 标签集合：
  - `fwd_ret_1m`
  - `fwd_ret_3m`
  - `fwd_ret_6m`

### 3.3 预处理

当前样例实验的预处理口径为：

- 大多数因子：`winsorize -> industry_neutralize -> size_neutralize -> zscore`
- `size`：`winsorize -> industry_neutralize -> zscore`

## 4. 主要产物

### 4.1 面板层

- `data/panel/agent2_baseline/factor_panel/`
- `data/panel/agent2_baseline/label_panel/`

### 4.2 评估层

- `data/experiments/agent2_baseline/evaluation/evaluation_summary.parquet`
- `data/experiments/agent2_baseline/evaluation/fama_macbeth_summary.parquet`
- `data/experiments/agent2_baseline/evaluation/factor_correlation_matrix.parquet`
- `data/experiments/agent2_baseline/evaluation/redundancy_summary.parquet`
- `data/experiments/agent2_baseline/evaluation/robustness_summary.parquet`

### 4.3 报告层

- [research_report.md](/Users/tong/PIT_Project/data/experiments/agent2_baseline/reports/research_report.md)
- [ic_leaderboard.png](/Users/tong/PIT_Project/data/experiments/agent2_baseline/reports/ic_leaderboard.png)
- [spread_leaderboard.png](/Users/tong/PIT_Project/data/experiments/agent2_baseline/reports/spread_leaderboard.png)
- [factor_correlation_heatmap.png](/Users/tong/PIT_Project/data/experiments/agent2_baseline/reports/factor_correlation_heatmap.png)
- [robustness_consistency.png](/Users/tong/PIT_Project/data/experiments/agent2_baseline/reports/robustness_consistency.png)

## 5. 推荐阅读顺序

如果是第一次看这个项目，推荐按下面顺序阅读：

1. 先看 [research_report.md](/Users/tong/PIT_Project/data/experiments/agent2_baseline/reports/research_report.md)
2. 再看 `evaluation_summary.parquet` 和 `fama_macbeth_summary.parquet`
3. 再看相关矩阵与冗余分析
4. 最后回到因子定义文档确认口径

推荐搭配阅读：

- [docs/factor_library_v1.md](factor_library_v1.md)
- [docs/methodology_spec_v1.md](methodology_spec_v1.md)

## 6. 当前样例的高层结果

以下结论来自当前真实生成的研究报告，主要用于展示平台能力，而不是作为投资建议：

- 最强 IC 信号：
  - `book_to_market` 对 `fwd_ret_6m`
  - `ic_mean = 0.0962`
  - `ICIR = 0.6203`
- 最强 spread：
  - `sales_to_price` 对 `fwd_ret_6m`
  - `spread_mean = 0.0282`
  - `spread_ir = 0.3366`
- 最稳定因子：
  - `accruals` 对 `fwd_ret_1m`
  - `IC sign consistency = 1.0000`
- 最强 Fama-MacBeth 结果：
  - `momentum_12_1` 对 `fwd_ret_6m`
  - `t-stat = 4.1231`
- 最高相关的一对因子：
  - `idiosyncratic_volatility` vs `volatility`
  - `|corr| = 0.9470`

这些结果说明当前平台已经不仅能“算因子”，还可以把横截面研究里常见的几类证据链一起给出来：

- 排序相关性
- 分组收益强度
- 横截面定价能力
- 因子重合程度
- 分时期稳健性

## 7. 作为项目展示时怎么讲

如果把这个样例用于简历、面试或作品集，建议突出三点：

- 这不是单一因子 demo，而是一套可复现的研究平台
- 整个流程严格消费 point-in-time 月频底座，避免直接碰 raw data
- 样例实验已经能自动产出因子、标签、评估和报告，具备一键研究闭环

一个适合口头描述的版本可以是：

“我做了一套 A 股 point-in-time 横截面因子研究平台。上游先把 Tushare 数据整理成月频 PIT 面板，下游通过 registry 统一管理因子和标签，然后自动完成 Rank IC、分组收益、Fama-MacBeth、冗余和稳健性分析，并生成研究报告。现在仓库里已经有一套 `agent2_baseline` 的真实样例输出。”

## 8. 后续怎么扩展

这个样例实验后续最自然的增强方向包括：

- 增加更细的行业分类口径
- 增加事件类与分析师预期因子
- 增加更丰富的图表和展示页
- 固化一份更适合作品集展示的样例输出快照

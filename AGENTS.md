一、项目总览
项目名称
A 股 Point-in-Time 横截面因子研究平台
项目定位
这是一个面向量化研究的 中低频横截面研究平台，目标是在每个再平衡时点，严格基于当时可获得的信息构建股票横截面特征，并对未来收益进行标准化评估。
这个项目不是：
纯数据清洗工程
单个因子 demo
策略实盘系统
高频 tick / L2 项目延伸版
它要解决的问题是：
如何在 A 股市场里，以严格的 point-in-time 口径，构建一个可复现、可扩展、可评估的横截面因子研究平台。
项目价值
这个项目用于补齐你现有 L2 微观结构项目之外的另一条能力线：
L2 项目证明你会做高频与微观结构研究
这个项目证明你会做中低频、财务口径、横截面资产定价和 point-in-time 研究
二者组合起来，简历会很完整。
二、项目目标
核心目标
构建一个可复现的月频横截面研究平台，支持：
多源数据接入
point-in-time 数据对齐
月频横截面面板构建
经典因子库计算
未来收益标签生成
IC / 分组收益 / Fama-MacBeth / 稳健性分析
自动化研究报告输出
V1 范围
V1 先聚焦于最核心、最能写进简历的能力，不做过度扩张。
V1 要做
以 Tushare 为主数据源
构建 月频 横截面研究平台
支持 A 股普通股 universe
实现 15–25 个经典因子
实现 1M / 3M / 6M 未来收益标签
输出标准研究报告与图表
全流程可复现
V1 不做
不接入 L2 / tick 数据
不做日内或高频调仓
不做复杂深度学习模型
不做订单级回测
不追求超大因子 zoo
V1.5 / V2 扩展方向
接入 Wind / CSMAR 增强字段
加入分析师预期和事件因子
加入更细的行业中性 / 风险模型
尝试简单的因子组合模型
三、完整项目流程
整个项目流程可以分为 8 个阶段。
阶段 1：研究口径定义
先统一整个项目的研究规则，否则后面所有实现都会漂。
这一阶段要明确：
研究频率：月频
再平衡日定义
universe 过滤规则
point-in-time 使用规则
收益标签口径
行业分类口径
异常值处理口径
因子标准化口径
评估指标口径
这一步的产出不是代码，而是一份 methodology spec。
它是整个项目的“法律”。
阶段 2：数据接入与原始层落地
从 Tushare 拉取所需数据，保存到本地。
涉及的数据大类：
股票基础信息
交易日历
日频行情
日频估值与流动性数据
复权因子
财务报表
财务指标
业绩预告 / 快报
这一层要做到：
可批量拉取
可增量更新
可重试
有 schema
有日志
本地 Parquet 落地
阶段 3：标准化与 point-in-time 基础层
把不同来源、不同表结构的数据，统一成研究平台可用的标准形式。
这里最关键的是定义：
availability_date
tradable_date
report_period
rebalance_date
然后基于这些时间字段，构建 as-of join 逻辑。
这是项目最核心的地方。
如果这里做不好，后面的因子研究都会有泄漏风险。
阶段 4：universe 构建
在每个再平衡时点，构建可交易股票池。
要做的过滤通常包括：
A 股普通股
排除上市时间太短的新股
排除异常股票
排除不满足流动性要求的股票
可选：排除 ST / PT / 长期停牌
输出应该是：
每个 rebalance_date 对应的股票列表
每只股票被纳入或剔除的原因标签
阶段 5：因子计算层
在每个再平衡时点，基于当时可见数据计算横截面因子。
V1 因子建议分成五类：
估值类
size
book-to-market
earnings-to-price
sales-to-price
cashflow-to-price
动量 / 反转类
12_1 momentum
6_1 momentum
3_1 momentum
1M reversal
风险 / 流动性类
beta
volatility
turnover
Amihud illiquidity
idiosyncratic volatility
盈利能力 / 质量类
ROE
ROA
gross profitability
gross margin
operating cash flow to assets
accruals
投资 / 杠杆类
asset growth
inventory growth
leverage
net operating assets
这一层还要做：
winsorize
standardize
行业中性 / 市值中性
缺失值覆盖率统计
阶段 6：标签生成层
为每个 rebalance_date × stock 生成未来收益标签。
建议 V1 至少做：
fwd_ret_1m
fwd_ret_3m
fwd_ret_6m
标签需要统一口径：
用调整价格计算
明确从哪个交易日开始算未来收益
明确是否扣除停牌影响
明确 horizon 的闭区间 / 开区间规则
阶段 7：评估与研究分析
用标准框架评估每个因子。
建议的标准输出：
Rank IC mean / std / ICIR
IC hit rate
quantile spread
分组收益单调性
Fama-MacBeth 回归
因子覆盖率
因子相关矩阵
冗余性分析
分时期稳健性分析
这是“研究平台”与“算几个因子”之间的关键区别。
阶段 8：自动报告与项目包装
把结果自动整理成适合阅读和简历展示的产物。
输出形式建议包括：
markdown 研究报告
因子总表
自动图表
方法说明文档
README
字段词典
一份用于简历项目展示的 summary
四、项目的整体技术结构
技术栈建议
Python
Polars
DuckDB
PyArrow / Parquet
Typer 或 Click
Pydantic
YAML 配置
Matplotlib
推荐目录结构
ashare-pit-platform/
  configs/
    data_sources.yaml
    universe.yaml
    factors.yaml
    labels.yaml
    evaluation.yaml

  src/
    adapters/
      tushare/
      wind/
      csmar/
    core/
      calendar.py
      universe.py
      pit.py
      adjustments.py
      preprocessing.py
    features/
      valuation/
      momentum/
      liquidity/
      quality/
      investment/
      registry.py
    labels/
    evaluation/
    reports/
    cli/

  data/
    raw/
    lake/
    pit/
    panel/
    reports/

  tests/
  notebooks/
  README.md
五、两个 agent 的任务拆分原则
为什么这样拆
两个 agent 最合理的拆法，不是按“因子种类”拆，也不是按“文件夹数量”拆，而是按上游基础设施和下游研究系统拆。
原因是：
上游部分更偏工程与数据口径
下游部分更偏研究与评估逻辑
二者之间有天然的数据接口
可以并行推进
可以明确验收边界
因此项目拆分为：
Agent 1
负责把“数据变成可研究的 point-in-time 月频面板底座”。
Agent 2
负责把“面板底座变成完整的因子研究系统与自动报告”。
六、Agent 1 负责的部分
角色定位
Data & PIT Infrastructure Agent
这是上游 agent。
它的职责不是“做研究结论”，而是保证整个研究平台的数据基础正确、稳定、可复现。
负责范围
1. 数据源接入
实现 Tushare adapter，支持：
股票基础信息
交易日历
日频行情
日频基本面
复权因子
财务表
财务指标
业绩预告 / 快报
要求：
批量拉取
增量更新
错误重试
日志记录
Parquet 落地
2. 标准化 schema
把不同表统一成规范字段体系。
必须统一的关键字段包括：
ts_code
trade_date
report_period
ann_date
f_ann_date
availability_date
tradable_date
rebalance_date
3. point-in-time 引擎
实现：
availability_date 生成规则
tradable_date 生成规则
as-of join
月频 snapshot 逻辑
数据时间截面还原
这是 Agent 1 的核心任务。
4. 调整价格与收益底层支持
实现：
原始价格落地
复权因子落地
统一调整价格生成函数
为标签层提供可复现的价格底座
5. universe 构建
在每个再平衡时点输出股票池。
包括：
新股过滤
基础可交易性过滤
流动性过滤
可选的 ST / 停牌过滤
6. 中间层与接口交付
向 Agent 2 提供干净、稳定的数据产物：
月频 universe 表
月频 PIT snapshot 表
调整价格表
研究面板基础表
Agent 1 的执行方向
Agent 1 的方向是：
先把“数据源 → 标准化 → point-in-time → 月频底座”这条链路彻底打通。
它不需要关心最终哪个因子最强，也不需要关心报告长什么样。
它的任务是建立一个 研究可信的底座。
Agent 1 的交付物
最终应该交出：
adapters/tushare/*
core/pit.py
core/calendar.py
core/universe.py
core/adjustments.py
data/lake/*
data/pit/*
一套数据 schema 文档
一套 point-in-time 规则文档
若干基础测试
Agent 1 的验收标准
Agent 1 完成后，应该能回答这几个问题：
任意一个再平衡日，平台能否还原当时可用的数据？
财务字段是否严格按公告可得时间进入横截面？
同一个研究面板是否可以重复生成且结果一致？
是否能稳定输出月频 universe 和 snapshot？
如果这些答不上来，说明 Agent 1 没完成。
七、Agent 2 负责的部分
角色定位
Factor Research & Evaluation Agent
这是下游 agent。
它的职责不是“整理原始数据”，而是基于 Agent 1 提供的 PIT 面板，构建因子、标签、评估和报告系统。
负责范围
1. 因子 registry 与因子库
建立因子注册机制，让每个因子都有：
名称
所属因子族
公式说明
输入字段
滞后规则
预处理方式
输出字段名
然后实现 V1 因子库。
2. 特征预处理
实现统一的：
winsorize
z-score
行业中性化
市值中性化
缺失值覆盖率监控
3. 标签生成
基于 Agent 1 提供的调整价格和交易日历，生成：
1M forward return
3M forward return
6M forward return
同时明确：
标签生成起点
horizon 定义
数据缺失处理规则
4. 评估框架
实现标准研究评估流程：
Rank IC
IC 时间序列
ICIR
quantile portfolio
top-bottom spread
Fama-MacBeth
稳健性分析
冗余分析
5. 自动报告系统
自动输出：
因子 summary 表
图表
markdown 研究报告
一键实验结果目录
6. 项目展示层
把结果整理成：
README 中的实验说明
方法论总结
简历项目描述素材
Agent 2 的执行方向
Agent 2 的方向是：
基于一个可信的 point-in-time 月频底座，搭建一个标准化的横截面因子研究系统。
它的核心不在于“写最多的公式”，而在于：
因子定义是否规范
评估框架是否标准
结果输出是否统一
新因子接入是否容易
Agent 2 的交付物
最终应该交出：
features/*
features/registry.py
labels/*
evaluation/*
reports/*
因子说明文档
研究报告模板
实验输出样例
因子评估测试
Agent 2 的验收标准
Agent 2 完成后，应该能做到：
对任意一个因子集，一键跑完整评估
自动输出 IC、分组收益和回归结果
自动产出标准研究报告
新增一个因子时，不需要改很多底层代码
八、两个 agent 的接口边界
这是最关键的一部分。
两个 agent 必须通过固定的数据契约协作，而不是彼此乱改代码。
Agent 1 → Agent 2 的输入接口
Agent 1 需要提供这些稳定产物：
1. monthly_universe
字段示意：
rebalance_date
ts_code
is_eligible
exclude_reason
2. monthly_snapshot_base
字段示意：
rebalance_date
ts_code
各类原始输入字段
availability_date
tradable_date
3. adjusted_price_panel
字段示意：
ts_code
trade_date
adj_close
adj_open
adj_factor
4. calendar_table
字段示意：
trade_date
is_month_end
next_trade_date
Agent 2 不应该直接碰 raw data。
Agent 2 只消费这些标准化后的数据。
Agent 2 → Agent 1 的反馈接口
Agent 2 在研究过程中发现问题时，只能通过明确 issue 反馈给 Agent 1，例如：
某字段在 snapshot 中存在时点错误
某类股票在 universe 构建中处理异常
某些财务表的公告时间逻辑不一致
不能直接绕过 Agent 1 自己修底层数据逻辑。
否则项目会很快失控。
九、建议的并行推进顺序
两个 agent 不需要完全串行，可以半并行推进。
第一阶段
Agent 1 先完成：
trade calendar
stock basic
daily / daily_basic / adj_factor
基础 Parquet 落地
月频再平衡日定义
与此同时，Agent 2 可以先完成：
因子 registry 框架
标签接口框架
评估模块框架
报告模板框架
第二阶段
Agent 1 完成：
财务表 point-in-time 逻辑
universe 构建
snapshot 产出
Agent 2 接着完成：
基于行情数据的动量 / 流动性 / 风险因子
forward return 标签
IC / 分组收益评估
第三阶段
Agent 1 补：
质量检查
数据一致性测试
schema 文档
Agent 2 补：
财务因子
Fama-MacBeth
稳健性 / 冗余分析
自动化报告
十、项目里程碑
M1：平台底座可用
完成后应该可以：
拉取原始数据
落地本地 Parquet
生成月频再平衡日
生成基础调整价格
M2：PIT 月频面板可用
完成后应该可以：
输出每月 universe
输出每月 snapshot
保证 no-lookahead
M3：基础因子研究可跑通
完成后应该可以：
计算第一批经典因子
生成 1M / 3M / 6M 标签
输出 IC 和分组收益
M4：研究平台成型
完成后应该可以：
支持完整的标准评估
自动生成报告
可直接作为简历项目展示
M5：增强层
完成后可以：
接入 Wind / CSMAR
加分析师预期 / 事件因子
做更丰富的研究拓展
注意，这只是暂定的大纲，要按着大纲的方向落地项目，但如果在实际落地时有一些与大纲描述不符或者大纲要求难以实现的情况，还是要具体情况具体分析，不死磕大纲
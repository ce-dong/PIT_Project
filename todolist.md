Data / PIT Infrastructure
总目标
把 “Tushare 原始数据 → 本地数据湖 → point-in-time 月频底座” 这条链路打通，并向 Agent 2 提供稳定、统一、无前视偏差的数据接口。
核心任务
A. 数据接入
写 Tushare client 封装
实现批量拉取和增量更新
给每张表定义 schema
把原始数据落到 Parquet
做日志、重试、断点续传
优先表：
stock_basic
trade_cal
daily
daily_basic
adj_factor
income
balancesheet
cashflow
fina_indicator
forecast
express
B. 标准化层
统一股票代码字段
统一日期类型
统一财务报告期字段
统一公告日期字段
建立 table-level validator
C. PIT 时间规则
定义 availability_date
定义 tradable_date
定义月频 rebalance_date
实现 as-of join
处理“一只股票某报告期多条财务记录”的去重与保留规则
D. Universe 构建
A 股普通股基础池
上市天数过滤
流动性过滤
停牌 / 异常样本过滤
输出每期纳入与剔除原因
E. 调整价格底座
用 daily + adj_factor 统一生成复权价格
生成标准价格面板
为 Agent 2 提供标签计算基础
F. 中间表交付
输出以下稳定表：
calendar_table
monthly_universe
adjusted_price_panel
monthly_snapshot_base
G. 测试
至少写这些测试：
交易日历正确性
next_trading_day 正确性
availability_date <= tradable_date
rebalance_date 生成正确
as-of join 不前视
同一时点重复生成结果一致
Agent 1 交付标准
完成后，别人应该能直接拿这些表去研究，而不用碰 raw 数据。
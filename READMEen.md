# A-Share Point-in-Time Cross-Sectional Research Platform

[中文版本](README.md)

## Overview

This project is a monthly cross-sectional research platform for A-share equities. Its goal is to reconstruct the information set that was actually available at each rebalance date, build a clean point-in-time research panel, and evaluate factor signals with a standardized workflow.

The project is designed around a full research pipeline rather than a single-factor demo:

- raw data ingestion and local storage
- point-in-time data alignment
- monthly universe and snapshot construction
- factor computation and preprocessing
- forward-return label generation
- standardized evaluation and robustness analysis
- automated research report generation

It is intentionally not a live trading system, an order-level backtester, or a high-frequency/L2 project.

## Current Status

The current version has completed the main V1 workflow for both Agent 1 and Agent 2, which means the project now supports a full path from PIT infrastructure to reproducible research outputs.

Implemented modules include:

- raw ingestion and Parquet persistence
- `calendar_table`
- `adjusted_price_panel`
- `monthly_universe`
- `monthly_snapshot_base`
- factor registry and 24 core factor implementations
- `fwd_ret_1m` / `fwd_ret_3m` / `fwd_ret_6m` labels
- `Rank IC`
- `quantile portfolio`
- `top-bottom spread`
- monotonicity analysis
- `Fama-MacBeth`
- factor correlation matrix
- redundancy analysis
- subperiod robustness analysis
- markdown research reports with charts

## Implemented Factor Library

The current V1 factor library contains 24 factors across 5 families:

- Valuation: `size`, `book_to_market`, `earnings_to_price`, `sales_to_price`, `cashflow_to_price`
- Momentum / reversal: `momentum_12_1`, `momentum_6_1`, `momentum_3_1`, `reversal_1m`
- Risk / liquidity: `beta`, `volatility`, `turnover`, `amihud_illiquidity`, `idiosyncratic_volatility`
- Profitability / quality: `roe`, `roa`, `gross_profitability`, `gross_margin`, `operating_cash_flow_to_assets`, `accruals`
- Investment / leverage: `asset_growth`, `inventory_growth`, `leverage`, `net_operating_assets`

Current preprocessing support includes:

- cross-sectional winsorization
- industry/board neutralization
- size neutralization
- cross-sectional z-score standardization
- coverage monitoring

The default preprocessing order is:

- `winsorize`
- `industry_neutralize`
- `size_neutralize`
- `zscore`

The neutralization logic is currently implemented as:

- Industry neutralization: within each `rebalance_date`, de-mean the factor cross section by `industry`; if `industry` is not available in the base panel, fall back to `market`
- Size neutralization: within each `rebalance_date`, regress the factor on `log(total_mv)` and keep the cross-sectional residual

This setup removes broad industry components and linear size exposure before the final standardization step.

## Point-in-Time Methodology

The platform currently uses the following timing rules:

- `rebalance_date`: the last open trading day of each calendar month
- `trade_execution_date`: the next open trading day after `rebalance_date`
- market data: latest record with `trade_date <= rebalance_date`
- financial/event data: latest tradable record with `<prefix>_tradable_date <= trade_execution_date`
- conservative tradability rule: `tradable_date` is the first open trading day strictly after `availability_date`

This is meant to reduce look-ahead bias and avoid treating same-day announcements as intraday tradable when precise timestamps are unavailable.

## Core Data Contract

Agent 2 only consumes standardized outputs produced by Agent 1 instead of reading raw data directly.

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
- PIT-aligned market, valuation, financial, and event inputs

## Research Outputs

A standard experiment currently produces:

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
- PNG charts for the report

A real sample experiment is available under:

- `data/experiments/agent2_baseline/`

## Repository Structure

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

## Quick Start

### 1. Install dependencies

Use the project environment and install `requirements.txt`:

```bash
/Users/tong/miniconda3/envs/pit_env/bin/pip install -r requirements.txt
```

### 2. Configure environment variables

The project expects `TUSHARE_TOKEN` in `.env`.

### 3. Ingest raw data

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli ingest all --start 20150101 --full-refresh
```

### 4. Build PIT base tables

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli build all
```

### 5. Run validations

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli validate all --write-report
```

### 6. Initialize a research experiment

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research init --name "Agent2 Baseline"
```

### 7. Build factors, labels, evaluation outputs, and report

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research build-factors --name "Agent2 Baseline"
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research build-labels --name "Agent2 Baseline"
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research build-evaluation --name "Agent2 Baseline"
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research build-report --name "Agent2 Baseline"
```

## Common Research Commands

### Inspect registered factors

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research factors
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research factors --family momentum
```

### Inspect registered labels

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research labels
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research labels --stage forward_return
```

### Run a subset of factors or labels

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research build-factors --name "Agent2 Baseline" --factor momentum_12_1 --factor size
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli research build-labels --name "Agent2 Baseline" --label fwd_ret_1m
```

## Quality Controls

Current validation checks cover:

- primary-key uniqueness
- required-column presence
- partition schema consistency
- calendar linkage consistency
- adjusted-price formula consistency
- universe/snapshot key alignment
- PIT cutoff enforcement
- coverage-ratio warnings

Related documents:

- [docs/quality_checks.md](docs/quality_checks.md)
- [docs/pit_rules.md](docs/pit_rules.md)
- [docs/data_contract.md](docs/data_contract.md)
- [docs/methodology_spec_v1.md](docs/methodology_spec_v1.md)
- [docs/factor_library_v1.md](docs/factor_library_v1.md)
- [docs/sample_experiment_agent2_baseline.md](docs/sample_experiment_agent2_baseline.md)

## Tests

Run the full test suite with:

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m pytest
```

## Why This Project Matters

This project demonstrates:

- financial data engineering and local data-lake construction
- point-in-time research design
- cross-sectional factor modeling and label generation
- standardized factor evaluation workflows
- automated report generation and experiment management

Natural next extensions include:

- richer industry-neutral and size-neutral preprocessing
- analyst expectation and event-driven factors
- stronger README/report visuals
- a polished sample experiment package for portfolio and resume use

# A-Share Point-in-Time Cross-Sectional Research Platform

## Overview

This project builds a reproducible monthly cross-sectional research platform for A-share equities with a strict point-in-time methodology.

It is designed to answer one question well:

How do we reconstruct the information set that was actually available at each monthly rebalance date, then turn that into a clean research panel for factor testing?

The platform focuses on:

- Tushare-based raw ingestion
- local Parquet storage
- monthly rebalance calendar construction
- point-in-time market, financial, and event snapshots
- auditable universe construction
- reproducible data-quality validation

It does not aim to be:

- a live trading system
- an order-level backtester
- a high-frequency or L2 platform
- a factor zoo with weak data controls

## Current Status

Agent 1 is effectively complete through the PIT infrastructure milestone.

Completed:

- raw ingestion for:
  - `trade_cal`
  - `stock_basic`
  - `daily`
  - `daily_basic`
  - `adj_factor`
  - `fina_indicator`
  - `income`
  - `balancesheet`
  - `cashflow`
  - `forecast`
  - `express`
- derived lake tables:
  - `calendar_table`
  - `adjusted_price_panel`
  - `monthly_universe`
  - `monthly_snapshot_base`
- PIT joins for:
  - market data
  - financial statements
  - financial indicators
  - forecast events
  - express events
- quality validation and markdown quality reporting

## Core Deliverables

### `calendar_table`

Unified trading clock with:

- `trade_date`
- `prev_trade_date`
- `next_trade_date`
- `is_month_end`

### `adjusted_price_panel`

Adjusted daily price base with:

- raw OHLCV-style fields
- `adj_factor`
- adjusted OHLC fields

### `monthly_universe`

Monthly research sample table with:

- `rebalance_date`
- `trade_execution_date`
- `is_eligible`
- `exclude_reason`
- liquidity and listing-age audit fields

### `monthly_snapshot_base`

Monthly PIT-wide table keyed by `rebalance_date + ts_code`, containing:

- market snapshot fields
- `fi_*`
- `inc_*`
- `bs_*`
- `cf_*`
- `fc_*`
- `ex_*`

## Point-in-Time Rules

The platform uses the following timing logic:

- `rebalance_date`: last open trading day of each calendar month
- `trade_execution_date`: next open trading day after `rebalance_date`
- market data join:
  - use the latest `trade_date <= rebalance_date`
- financial and event data join:
  - use the latest `<prefix>_tradable_date <= trade_execution_date`
- conservative tradability rule:
  - `tradable_date` is the first open trading day strictly after `availability_date`

This prevents the platform from treating same-day announcements as if they were tradable intraday when no reliable timestamp exists.

## Repository Structure

```text
configs/                 # reserved for future config expansion
docs/
  data_contract.md
  pit_rules.md
  quality_checks.md
  schema/
src/
  adapters/tushare/
  builders/
  core/
  storage/
  updaters/
  validators/
tests/
data/
  raw/                   # ignored
  lake/                  # ignored
  reports/               # ignored
```

## Main Commands

### Raw ingestion

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli ingest all --start 20150101 --full-refresh
```

### Derived lake builds

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli build calendar_table
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli build adjusted_price_panel
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli build monthly_universe
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli build monthly_snapshot_base
```

### Data-quality validation

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli validate all
/Users/tong/miniconda3/envs/pit_env/bin/python -m src.cli validate all --write-report
```

The second command writes markdown reports under `data/reports/`.

## Quality Controls

Current validation checks cover:

- primary-key uniqueness
- required-column presence
- partition schema consistency
- calendar linkage consistency
- adjusted-price formula consistency
- universe and snapshot key alignment
- PIT cutoff enforcement
- coverage-ratio threshold warnings

The latest rules are documented in:

- [docs/quality_checks.md](docs/quality_checks.md)
- [docs/pit_rules.md](docs/pit_rules.md)
- [docs/data_contract.md](docs/data_contract.md)

## Tests

Run the current core test suite with:

```bash
/Users/tong/miniconda3/envs/pit_env/bin/python -m pytest tests/test_quality.py tests/test_pit.py tests/test_calendar.py tests/test_adjustments.py tests/test_universe.py tests/test_state_store.py
```

## Next Step

With Agent 1 infrastructure in place, the natural next stage is Agent 2 work:

- factor computation
- forward-return labels
- cross-sectional evaluation
- report automation

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from dotenv import dotenv_values


@dataclass(frozen=True)
class AppConfig:
    project_root: Path
    data_root: Path
    raw_data_root: Path
    lake_data_root: Path
    metadata_root: Path
    log_root: Path
    reports_root: Path
    env_file: Path
    tushare_token: str
    initial_history_start: str
    calendar_start_date: str
    calendar_future_days: int
    daily_lookback_trade_days: int
    adj_factor_lookback_trade_days: int
    financial_lookback_days: int
    universe_ipo_min_trade_days: int
    universe_liquidity_window: int
    universe_min_valid_trade_days: int
    universe_min_median_amount: float
    quality_market_coverage_warn_ratio: float
    quality_market_coverage_error_ratio: float
    quality_financial_coverage_warn_ratio: float
    quality_financial_coverage_error_ratio: float
    quality_event_coverage_warn_ratio: float
    quality_event_coverage_error_ratio: float
    request_sleep_seconds: float
    retry_attempts: int
    calendar_exchange: str

    @classmethod
    def load(cls, project_root: Path | None = None) -> "AppConfig":
        root = (project_root or Path.cwd()).resolve()
        env_file = root / ".env"
        env_values = dotenv_values(env_file) if env_file.exists() else {}
        token = env_values.get("TUSHARE_TOKEN") or os.getenv("TUSHARE_TOKEN")
        if not token:
            raise RuntimeError("Missing TUSHARE_TOKEN. Add it to .env or the environment.")

        return cls(
            project_root=root,
            data_root=root / "data",
            raw_data_root=root / "data" / "raw",
            lake_data_root=root / "data" / "lake",
            metadata_root=root / "data" / "metadata",
            log_root=root / "data" / "logs",
            reports_root=root / "data" / "reports",
            env_file=env_file,
            tushare_token=token,
            initial_history_start=os.getenv("PIT_HISTORY_START", "20150101"),
            calendar_start_date=os.getenv("PIT_CALENDAR_START", "19900101"),
            calendar_future_days=int(os.getenv("PIT_CALENDAR_FUTURE_DAYS", "366")),
            daily_lookback_trade_days=int(os.getenv("PIT_DAILY_LOOKBACK_TRADE_DAYS", "20")),
            adj_factor_lookback_trade_days=int(os.getenv("PIT_ADJ_FACTOR_LOOKBACK_TRADE_DAYS", "60")),
            financial_lookback_days=int(os.getenv("PIT_FINANCIAL_LOOKBACK_DAYS", "120")),
            universe_ipo_min_trade_days=int(os.getenv("PIT_UNIVERSE_IPO_MIN_TRADE_DAYS", "120")),
            universe_liquidity_window=int(os.getenv("PIT_UNIVERSE_LIQUIDITY_WINDOW", "20")),
            universe_min_valid_trade_days=int(os.getenv("PIT_UNIVERSE_MIN_VALID_TRADE_DAYS", "15")),
            universe_min_median_amount=float(os.getenv("PIT_UNIVERSE_MIN_MEDIAN_AMOUNT", "20000")),
            quality_market_coverage_warn_ratio=float(os.getenv("PIT_QUALITY_MARKET_COVERAGE_WARN_RATIO", "0.75")),
            quality_market_coverage_error_ratio=float(os.getenv("PIT_QUALITY_MARKET_COVERAGE_ERROR_RATIO", "0.60")),
            quality_financial_coverage_warn_ratio=float(os.getenv("PIT_QUALITY_FINANCIAL_COVERAGE_WARN_RATIO", "0.75")),
            quality_financial_coverage_error_ratio=float(os.getenv("PIT_QUALITY_FINANCIAL_COVERAGE_ERROR_RATIO", "0.60")),
            quality_event_coverage_warn_ratio=float(os.getenv("PIT_QUALITY_EVENT_COVERAGE_WARN_RATIO", "0.50")),
            quality_event_coverage_error_ratio=float(os.getenv("PIT_QUALITY_EVENT_COVERAGE_ERROR_RATIO", "0.30")),
            request_sleep_seconds=float(os.getenv("PIT_REQUEST_SLEEP_SECONDS", "0.3")),
            retry_attempts=int(os.getenv("PIT_RETRY_ATTEMPTS", "3")),
            calendar_exchange=os.getenv("PIT_CALENDAR_EXCHANGE", "SSE"),
        )

    def ensure_directories(self) -> None:
        for path in (self.data_root, self.raw_data_root, self.lake_data_root, self.metadata_root, self.log_root, self.reports_root):
            path.mkdir(parents=True, exist_ok=True)

    def calendar_end_date(self) -> str:
        return (date.today() + timedelta(days=self.calendar_future_days)).strftime("%Y%m%d")

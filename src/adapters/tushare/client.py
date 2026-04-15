from __future__ import annotations

import time
from typing import Any

import pandas as pd
import tushare as ts


STOCK_BASIC_FIELDS = ",".join(
    [
        "ts_code",
        "symbol",
        "name",
        "area",
        "industry",
        "fullname",
        "enname",
        "cnspell",
        "market",
        "exchange",
        "curr_type",
        "list_status",
        "list_date",
        "delist_date",
        "is_hs",
        "act_name",
        "act_ent_type",
    ]
)


class TushareClient:
    def __init__(self, token: str, retry_attempts: int = 3, sleep_seconds: float = 0.3) -> None:
        # Avoid ts.set_token(), which writes a token cache file under the home directory.
        self.pro = ts.pro_api(token)
        self.retry_attempts = retry_attempts
        self.sleep_seconds = sleep_seconds

    def _call(self, endpoint: str, **params: Any) -> pd.DataFrame:
        method = getattr(self.pro, endpoint)
        last_error: Exception | None = None
        for attempt in range(1, self.retry_attempts + 1):
            try:
                result = method(**params)
                if self.sleep_seconds > 0:
                    time.sleep(self.sleep_seconds)
                return result
            except Exception as exc:  # pragma: no cover - network dependent
                last_error = exc
                if attempt == self.retry_attempts:
                    break
                time.sleep(self.sleep_seconds * attempt)
        raise RuntimeError(f"Tushare endpoint '{endpoint}' failed after retries: {last_error}") from last_error

    def fetch_trade_cal(self, exchange: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self._call("trade_cal", exchange=exchange, start_date=start_date, end_date=end_date)

    def fetch_stock_basic(self, list_status: str) -> pd.DataFrame:
        return self._call("stock_basic", exchange="", list_status=list_status, fields=STOCK_BASIC_FIELDS)

    def fetch_daily(self, trade_date: str) -> pd.DataFrame:
        return self._call("daily", trade_date=trade_date)

    def fetch_daily_basic(self, trade_date: str) -> pd.DataFrame:
        return self._call("daily_basic", trade_date=trade_date)

    def fetch_adj_factor(self, trade_date: str) -> pd.DataFrame:
        return self._call("adj_factor", trade_date=trade_date)

    def fetch_fina_indicator(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self._call("fina_indicator", ts_code=ts_code, start_date=start_date, end_date=end_date)

    def fetch_income(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self._call("income", ts_code=ts_code, start_date=start_date, end_date=end_date)

    def fetch_balancesheet(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self._call("balancesheet", ts_code=ts_code, start_date=start_date, end_date=end_date)

    def fetch_cashflow(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self._call("cashflow", ts_code=ts_code, start_date=start_date, end_date=end_date)

    def fetch_forecast(self, ann_date: str) -> pd.DataFrame:
        return self._call("forecast", ann_date=ann_date)

    def fetch_express(self, ann_date: str) -> pd.DataFrame:
        return self._call("express", ann_date=ann_date)

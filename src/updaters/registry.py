from __future__ import annotations

from src.updaters.adj_factor import AdjFactorUpdater
from src.updaters.daily import DailyUpdater
from src.updaters.daily_basic import DailyBasicUpdater
from src.updaters.stock_basic import StockBasicUpdater
from src.updaters.trade_cal import TradeCalUpdater


UPDATER_REGISTRY = {
    "trade_cal": TradeCalUpdater,
    "stock_basic": StockBasicUpdater,
    "daily": DailyUpdater,
    "daily_basic": DailyBasicUpdater,
    "adj_factor": AdjFactorUpdater,
}

CORE_TABLE_ORDER = ["trade_cal", "stock_basic", "daily", "daily_basic", "adj_factor"]


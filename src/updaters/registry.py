from __future__ import annotations

from src.updaters.adj_factor import AdjFactorUpdater
from src.updaters.balancesheet import BalancesheetUpdater
from src.updaters.cashflow import CashflowUpdater
from src.updaters.daily import DailyUpdater
from src.updaters.daily_basic import DailyBasicUpdater
from src.updaters.fina_indicator import FinaIndicatorUpdater
from src.updaters.income import IncomeUpdater
from src.updaters.stock_basic import StockBasicUpdater
from src.updaters.trade_cal import TradeCalUpdater


UPDATER_REGISTRY = {
    "trade_cal": TradeCalUpdater,
    "stock_basic": StockBasicUpdater,
    "daily": DailyUpdater,
    "daily_basic": DailyBasicUpdater,
    "adj_factor": AdjFactorUpdater,
    "fina_indicator": FinaIndicatorUpdater,
    "income": IncomeUpdater,
    "balancesheet": BalancesheetUpdater,
    "cashflow": CashflowUpdater,
}

CORE_TABLE_ORDER = [
    "trade_cal",
    "stock_basic",
    "daily",
    "daily_basic",
    "adj_factor",
    "fina_indicator",
    "income",
    "balancesheet",
    "cashflow",
]

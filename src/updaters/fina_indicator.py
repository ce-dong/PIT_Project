from __future__ import annotations

from src.updaters.financial_statement_base import FinancialStatementUpdater


class FinaIndicatorUpdater(FinancialStatementUpdater):
    endpoint_name = "fina_indicator"
    table_name = "fina_indicator"

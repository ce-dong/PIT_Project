from __future__ import annotations

from src.updaters.financial_statement_base import FinancialStatementUpdater


class BalancesheetUpdater(FinancialStatementUpdater):
    endpoint_name = "balancesheet"
    table_name = "balancesheet"

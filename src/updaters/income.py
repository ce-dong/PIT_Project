from __future__ import annotations

from src.updaters.financial_statement_base import FinancialStatementUpdater


class IncomeUpdater(FinancialStatementUpdater):
    endpoint_name = "income"
    table_name = "income"

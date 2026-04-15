from __future__ import annotations

from src.updaters.financial_statement_base import FinancialStatementUpdater


class CashflowUpdater(FinancialStatementUpdater):
    endpoint_name = "cashflow"
    table_name = "cashflow"

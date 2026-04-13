from __future__ import annotations

from src.builders.adjusted_price_panel import AdjustedPricePanelBuilder
from src.builders.calendar_table import CalendarTableBuilder
from src.builders.monthly_universe import MonthlyUniverseBuilder


BUILDER_REGISTRY = {
    "calendar_table": CalendarTableBuilder,
    "adjusted_price_panel": AdjustedPricePanelBuilder,
    "monthly_universe": MonthlyUniverseBuilder,
}

BUILD_ORDER = ["calendar_table", "adjusted_price_panel", "monthly_universe"]

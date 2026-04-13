from __future__ import annotations

from src.builders.adjusted_price_panel import AdjustedPricePanelBuilder
from src.builders.calendar_table import CalendarTableBuilder


BUILDER_REGISTRY = {
    "calendar_table": CalendarTableBuilder,
    "adjusted_price_panel": AdjustedPricePanelBuilder,
}

BUILD_ORDER = ["calendar_table", "adjusted_price_panel"]


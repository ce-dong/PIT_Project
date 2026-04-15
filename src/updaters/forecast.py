from __future__ import annotations

from src.updaters.announcement_event_base import AnnouncementEventUpdater


class ForecastUpdater(AnnouncementEventUpdater):
    endpoint_name = "forecast"
    table_name = "forecast"
    extra_date_columns = ["first_ann_date"]

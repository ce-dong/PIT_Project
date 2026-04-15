from __future__ import annotations

from src.updaters.announcement_event_base import AnnouncementEventUpdater


class ExpressUpdater(AnnouncementEventUpdater):
    endpoint_name = "express"
    table_name = "express"

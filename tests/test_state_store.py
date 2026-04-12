from __future__ import annotations

import json

from src.storage.state import IngestionStateStore


def test_state_store_round_trip(tmp_path):
    store = IngestionStateStore(tmp_path / "ingestion_state.json")
    store.mark_success(
        "daily",
        started_at="2026-04-12T00:00:00+00:00",
        finished_at="2026-04-12T00:01:00+00:00",
        rows_written=12,
        updated_partitions=["daily/year=2026/month=04/data.parquet"],
        mode="incremental",
        last_success_trade_date="20260410",
    )

    payload = json.loads((tmp_path / "ingestion_state.json").read_text(encoding="utf-8"))
    assert payload["daily"]["last_success_trade_date"] == "20260410"
    assert store.get("daily")["rows_written"] == 12

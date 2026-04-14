from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class IngestionStateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text(encoding="utf-8"))

    def get(self, table_name: str) -> dict[str, Any]:
        return self.load().get(table_name, {})

    def mark_success(
        self,
        table_name: str,
        *,
        started_at: str,
        finished_at: str,
        rows_written: int,
        updated_partitions: list[str],
        mode: str,
        last_success_trade_date: str | None = None,
        extra_state: dict[str, Any] | None = None,
    ) -> None:
        state = self.load()
        payload = {
            "table_name": table_name,
            "status": "success",
            "started_at": started_at,
            "finished_at": finished_at,
            "rows_written": rows_written,
            "updated_partitions": updated_partitions,
            "mode": mode,
            "last_success_trade_date": last_success_trade_date,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }
        if extra_state:
            payload.update(extra_state)
        state[table_name] = payload
        tmp_path = self.path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(state, indent=2, ensure_ascii=True), encoding="utf-8")
        tmp_path.replace(self.path)

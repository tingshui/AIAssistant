"""Append one JSON line to ahacatcher/data/background_monitor.jsonl (repo root = parents[2] of this file)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def append_event(event: str, **fields: Any) -> None:
    root = Path(__file__).resolve().parents[2]
    path = root / "ahacatcher" / "data" / "background_monitor.jsonl"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        row: dict[str, Any] = {"event": event, "ts": datetime.now(timezone.utc).isoformat()}
        for k, v in fields.items():
            if v is not None:
                row[k] = v
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except OSError:
        pass

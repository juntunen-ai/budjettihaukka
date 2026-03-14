from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import settings

logger = logging.getLogger(__name__)


def _resolve_log_path() -> Path:
    path = Path(settings.observability_log_path)
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[1] / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def log_query_event(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        return

    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        **payload,
    }
    path = _resolve_log_path()
    try:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception as exc:
        logger.warning("Observability logging failed: %s", exc)


def read_query_events(limit: int | None = None) -> list[dict[str, Any]]:
    path = _resolve_log_path()
    if not path.exists():
        return []

    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
            if isinstance(parsed, dict):
                rows.append(parsed)
        except Exception:
            continue

    if limit and limit > 0:
        return rows[-limit:]
    return rows


def summarize_slo(events: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(events)
    if total == 0:
        return {
            "total": 0,
            "query_success": 0.0,
            "chart_render_success": 0.0,
            "clarification_rate": 0.0,
        }

    query_success = sum(1 for e in events if bool(e.get("query_success")))
    chart_success = sum(1 for e in events if bool(e.get("chart_render_success")))
    clarification_required = sum(1 for e in events if bool(e.get("clarification_required")))

    return {
        "total": total,
        "query_success": query_success / total,
        "chart_render_success": chart_success / total,
        "clarification_rate": clarification_required / total,
    }

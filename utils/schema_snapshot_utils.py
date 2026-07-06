"""Schema snapshots and drift detection for the Valtiokonttori source data.

The snapshot file records the *accepted* source schema (original CSV header
names and their normalized forms). Ingest compares each run's discovered
schema against it, and the DQ checks compare the live BigQuery raw table
against it. A new, removed, or renamed column then surfaces as an explicit
alert instead of a silent breakage downstream.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_SNAPSHOT_PATH = "data/schema_snapshots/valtiokonttori_source_columns.json"


@dataclass(frozen=True)
class SchemaDrift:
    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)

    @property
    def has_drift(self) -> bool:
        return bool(self.added or self.removed)

    @property
    def possible_renames(self) -> list[tuple[str, str]]:
        # A rename shows up as one removed + one added column; pair them by
        # loose similarity so the alert can hint at what likely happened.
        pairs: list[tuple[str, str]] = []
        for removed_col in self.removed:
            removed_key = _loose_key(removed_col)
            for added_col in self.added:
                added_key = _loose_key(added_col)
                if removed_key and added_key and (
                    removed_key in added_key or added_key in removed_key
                ):
                    pairs.append((removed_col, added_col))
        return pairs


def _loose_key(name: str) -> str:
    return "".join(ch for ch in name.lower() if ch.isalnum())


def build_snapshot(columns: dict[str, str], source: str) -> dict:
    return {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "column_count": len(columns),
        "columns": dict(sorted(columns.items())),
    }


def load_snapshot(path: str | Path) -> dict | None:
    snapshot_path = Path(path)
    if not snapshot_path.exists():
        return None
    return json.loads(snapshot_path.read_text(encoding="utf-8"))


def save_snapshot(path: str | Path, snapshot: dict) -> None:
    snapshot_path = Path(path)
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(
        json.dumps(snapshot, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def diff_columns(previous_columns: set[str], current_columns: set[str]) -> SchemaDrift:
    return SchemaDrift(
        added=sorted(current_columns - previous_columns),
        removed=sorted(previous_columns - current_columns),
    )


def diff_snapshot(snapshot: dict, current_columns: dict[str, str] | set[str]) -> SchemaDrift:
    previous = set(snapshot.get("columns", {}))
    current = set(current_columns)
    return diff_columns(previous, current)


def format_drift_alert(drift: SchemaDrift, context: str) -> str:
    lines = [f"SCHEMA DRIFT DETECTED ({context}):"]
    if drift.added:
        lines.append(f"  added columns:   {', '.join(drift.added)}")
    if drift.removed:
        lines.append(f"  removed columns: {', '.join(drift.removed)}")
    for removed_col, added_col in drift.possible_renames:
        lines.append(f"  possible rename: {removed_col!r} -> {added_col!r}")
    return "\n".join(lines)

#!/usr/bin/env python3
"""Offline tests for schema snapshot / drift detection (no BigQuery needed)."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils import schema_snapshot_utils as ssu

FAILURES: list[str] = []


def check(name: str, condition: bool) -> None:
    status = "OK" if condition else "FAIL"
    print(f"[{status}] {name}")
    if not condition:
        FAILURES.append(name)


def main() -> int:
    baseline = {"Vuosi": "vuosi", "Kk": "kk", "Nettokertymä": "nettokertyma"}

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "snap.json"

        check("missing snapshot loads as None", ssu.load_snapshot(path) is None)

        snapshot = ssu.build_snapshot(baseline, source="test")
        ssu.save_snapshot(path, snapshot)
        loaded = ssu.load_snapshot(path)
        check("roundtrip keeps columns", loaded is not None and loaded["columns"] == dict(sorted(baseline.items())))
        check("snapshot records source", loaded["source"] == "test")

        no_drift = ssu.diff_snapshot(loaded, baseline)
        check("identical schema -> no drift", not no_drift.has_drift)

        added = dict(baseline)
        added["Uusi_Sarake"] = "uusi_sarake"
        drift_added = ssu.diff_snapshot(loaded, added)
        check("new column detected", drift_added.added == ["Uusi_Sarake"] and not drift_added.removed)

        removed = {"Vuosi": "vuosi", "Kk": "kk"}
        drift_removed = ssu.diff_snapshot(loaded, removed)
        check("removed column detected", drift_removed.removed == ["Nettokertymä"] and not drift_removed.added)

        renamed = {"Vuosi": "vuosi", "Kk": "kk", "Nettokertymä_EUR": "nettokertyma_eur"}
        drift_renamed = ssu.diff_snapshot(loaded, renamed)
        check(
            "rename hinted via add+remove pairing",
            ("Nettokertymä", "Nettokertymä_EUR") in drift_renamed.possible_renames,
        )

        alert = ssu.format_drift_alert(drift_renamed, context="test")
        check(
            "alert mentions added, removed and rename",
            "Nettokertymä_EUR" in alert and "Nettokertymä" in alert and "possible rename" in alert,
        )

    real_snapshot = ssu.load_snapshot(ROOT / ssu.DEFAULT_SNAPSHOT_PATH)
    check(
        "repo snapshot exists with expected valtiokonttori columns",
        real_snapshot is not None
        and {"Vuosi", "Kk", "Hallinnonala", "Momentti_TunnusP"} <= set(real_snapshot.get("columns", {})),
    )

    if FAILURES:
        print(f"\nFAILED: {len(FAILURES)} checks")
        return 1
    print("\nAll schema drift checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

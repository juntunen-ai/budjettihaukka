#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audit_bigquery_operational_state import _parse_source_periods, _period_lag_months


def main() -> None:
    periods = _parse_source_periods([
        "https://example.test/budjettitalous/2026/7/file.csv",
        "https://example.test/budjettitalous/2025/12/file.csv",
        "not-a-source-url",
        "https://example.test/budjettitalous/2026/7/duplicate.csv",
    ])
    assert periods == [(2025, 12), (2026, 7)]
    assert _period_lag_months((2026, 5), (2026, 7)) == 2
    assert _period_lag_months((2025, 12), (2026, 2)) == 2
    assert _period_lag_months((2026, 7), (2026, 7)) == 0
    print("BigQuery operational audit tests PASSED")


if __name__ == "__main__":
    main()

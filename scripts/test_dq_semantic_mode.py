#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_bq_data_quality_checks import _build_checks, _detect_table_mode


def main() -> None:
    semantic_columns = {
        "Vuosi",
        "Kk",
        "Hallinnonala",
        "Momentti_TunnusP",
        "Momentti_sNimi",
        "Nettokertymä",
        "period_date",
        "quality_issue_count",
        "row_fingerprint",
        "has_valid_nettokertyma",
    }
    assert _detect_table_mode(semantic_columns) == "semantic"

    checks = _build_checks("project.dataset.semantic_table", "semantic")
    check_names = {check.name for check in checks}
    assert "invalid_nettokertyma_parse" in check_names
    assert "duplicate_row_fingerprint" in check_names

    invalid_nettokertyma_sql = next(
        check.sql for check in checks if check.name == "invalid_nettokertyma_parse"
    )
    assert "has_valid_nettokertyma" in invalid_nettokertyma_sql
    assert "TRIM(`Nettokertymä`)" not in invalid_nettokertyma_sql

    print("DQ semantic mode tests PASSED")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_bq_data_quality_checks import _build_checks, _detect_table_mode, _summary_sql


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
    assert _detect_table_mode({"vuosi", "kk", "hallinnonala", "momentti_tunnusp", "nettokertyma"}) == "normalized_raw"

    checks = _build_checks("project.dataset.semantic_table", "semantic")
    check_names = {check.name for check in checks}
    assert "invalid_nettokertyma_parse" in check_names
    assert "duplicate_row_fingerprint" in check_names

    invalid_nettokertyma_sql = next(
        check.sql for check in checks if check.name == "invalid_nettokertyma_parse"
    )
    assert "has_valid_nettokertyma" in invalid_nettokertyma_sql
    assert "TRIM(`Nettokertymä`)" not in invalid_nettokertyma_sql

    raw_checks = _build_checks("project.dataset.raw_table", "raw")
    raw_sql = "\n".join(check.sql for check in raw_checks)
    assert "TRIM(CAST(`Nettokertymä` AS STRING))" in raw_sql
    assert "TRIM(CAST(`PaaluokkaOsasto_TunnusP` AS STRING))" in raw_sql
    assert "TRIM(`Nettokertymä`)" not in raw_sql
    assert "TRIM(`PaaluokkaOsasto_TunnusP`)" not in raw_sql

    normalized_raw_checks = _build_checks("project.dataset.normalized_raw", "normalized_raw")
    normalized_raw_sql = "\n".join(check.sql for check in normalized_raw_checks)
    assert "SAFE_CAST(`vuosi` AS INT64)" in normalized_raw_sql
    assert "TRIM(CAST(`paaluokkaosasto_tunnusp` AS STRING))" in normalized_raw_sql
    assert "`Vuosi`" not in normalized_raw_sql
    assert "`PaaluokkaOsasto_TunnusP`" not in normalized_raw_sql
    normalized_summary_sql = _summary_sql("project.dataset.normalized_raw", "normalized_raw")
    assert "DATE(vuosi, kk, 1)" in normalized_summary_sql
    assert "DATE(MAX(vuosi), 12, 1)" not in normalized_summary_sql

    print("DQ semantic mode tests PASSED")


if __name__ == "__main__":
    main()

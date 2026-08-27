#!/usr/bin/env python3
"""Offline regression tests for the state-tax growth accounting scenario."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_tax_growth_scenario import (  # noqa: E402
    OUTPUT_PATH,
    build_query,
    validate_snapshot,
)


def main() -> None:
    snapshot = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
    validate_snapshot(snapshot)
    meta = snapshot["meta"]
    rows = snapshot["rows"]
    final = snapshot["summary_2025"]

    assert meta["price_base_year"] == 2025
    assert "ei kuntien veroja" in meta["tax_scope_fi"]
    assert "ei prosenttiyksikkömuutos" in meta["rate_cut_definition_fi"]
    assert "ei arvioi dynaamisesti" in meta["causality_warning_fi"]
    assert len(rows) == 18
    assert all(row["is_complete_year"] is True for row in rows)

    assert 65 < final["actual_tax_revenue_beur"] < 67
    assert 73 < final["unchanged_rate_revenue_beur"] < 75
    assert final["lower_5pct_revenue_beur"] > final["actual_tax_revenue_beur"] + 4
    assert final["lower_10pct_revenue_beur"] > final["actual_tax_revenue_beur"]
    assert 11 < final["break_even_relative_rate_cut_pct"] < 12

    sql = build_query(meta["source_table"])
    assert "fiscal_side = 'revenue'" in sql
    assert "r'^11\\.'" in sql
    assert "-SUM(" in sql

    print(
        "Tax growth scenario OK "
        f"(actual {final['actual_tax_revenue_beur']:.1f} bn; "
        f"10% lower rate {final['lower_10pct_revenue_beur']:.1f} bn; "
        f"break-even {final['break_even_relative_rate_cut_pct']:.1f}%)"
    )


if __name__ == "__main__":
    main()

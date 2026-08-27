#!/usr/bin/env python3
"""Offline regression tests for the reviewed policy budget snapshot."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_policy_budget_evidence import (  # noqa: E402
    OUTPUT_PATH,
    SELECTION_PATH,
    build_history_query,
    build_query,
    validate_selection,
    validate_snapshot,
)


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def by_code(snapshot: dict, code: str) -> dict:
    return next(row for row in snapshot["rows"] if row["momentti_tunnusp"] == code)


def main() -> None:
    selection = load(SELECTION_PATH)
    snapshot = load(OUTPUT_PATH)
    validate_selection(selection)
    validate_snapshot(snapshot, selection)

    meta = snapshot["meta"]
    rows = snapshot["rows"]
    assert meta["source_table"] == (
        "budjettihaukka-gpt.valtiodata.analytics_fiscal_yearly_core_v1"
    )
    assert meta["official_source_url"].startswith("https://api.tutkihallintoa.fi/")
    assert meta["fiscal_side"] == "expense"
    assert (meta["baseline_year"], meta["comparison_year"]) == (2011, 2016)
    assert meta["real_price_base_year"] == 2025
    assert len(rows) == 11
    assert all(row["baseline"]["is_complete_year"] is True for row in rows)
    assert all(row["comparison"]["is_complete_year"] is True for row in rows)
    assert all(row["baseline"]["has_structural_guardrail"] is False for row in rows)
    assert all(row["comparison"]["has_structural_guardrail"] is False for row in rows)
    assert all(row["history"] == sorted(row["history"], key=lambda item: item["year"]) for row in rows)
    assert all(2008 <= row["history"][0]["year"] <= 2011 for row in rows)
    assert all(2016 <= row["history"][-1]["year"] <= 2025 for row in rows)
    for row in rows:
        by_year = {item["year"]: item for item in row["history"]}
        assert abs(by_year[2011]["real_meur"] - row["baseline"]["real_meur"]) < 0.001
        assert abs(by_year[2016]["real_meur"] - row["comparison"]["real_meur"]) < 0.001

    nominal_cuts = [row for row in rows if row["evidence_class"] == "nominal_and_real_reduction"]
    real_only = [row for row in rows if row["evidence_class"] == "real_value_reduction_only"]
    assert len(nominal_cuts) == 10
    assert [row["momentti_tunnusp"] for row in real_only] == ["29.40.51."]

    tki = by_code(snapshot, "32.20.40.")
    assert tki["nominal_change_meur"] < -100
    assert tki["real_change_meur"] < -170
    assert -30 < tki["real_change_pct"] < -29

    university = by_code(snapshot, "29.40.50.")
    assert university["nominal_change_meur"] < 0
    assert university["real_change_meur"] < -130

    health_research = by_code(snapshot, "33.60.32.")
    assert health_research["real_change_pct"] < -60

    gtk = by_code(snapshot, "32.20.01.")
    assert gtk["history"][-1]["source_codes"] == ["32.01.04."]
    business_finland = by_code(snapshot, "32.20.06.")
    assert business_finland["history"][-1]["source_codes"] == ["32.01.05."]
    preparation = by_code(snapshot, "32.20.45.")
    assert preparation["history"][-1]["year"] == 2016

    excluded = snapshot["excluded_comparisons"]
    assert any(item["concept_id"] == "ammattikorkeakoulut" for item in excluded)

    sql = build_query(meta["source_table"])
    assert "fiscal_side = @fiscal_side" in sql
    assert "year IN UNNEST(@years)" in sql
    assert "momentti_tunnusp IN UNNEST(@codes)" in sql
    assert "LOGICAL_OR(has_structural_guardrail)" in sql

    history_sql = build_history_query(meta["source_table"])
    assert "year BETWEEN @history_from AND @history_to" in history_sql
    assert "momentti_tunnusp IN UNNEST(@codes)" in history_sql
    assert "LOGICAL_OR(has_structural_guardrail)" in history_sql

    print(
        "Policy budget evidence OK "
        f"({len(rows)} moments; {len(nominal_cuts)} nominal+real cuts; "
        f"{len(real_only)} real-only decline)"
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Offline regression tests for municipal-finance snapshots and semantic views."""

from __future__ import annotations

import csv
import json
import sys
from collections import Counter
from pathlib import Path

import sqlglot

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_municipal_finance_mart import build_sql_map
from scripts.load_municipal_finance_reference import (
    METRIC_IDS,
    MUNICIPAL_PACKAGES,
    PLANNING_STAGES,
    _findings,
    _period_semantics,
)

REF = ROOT / "data" / "reference"


def _rows(name: str) -> list[dict[str, str]]:
    with (REF / name).open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_catalog_and_taxonomy() -> None:
    index = _rows("municipal_finance_taxonomy_index_v1.csv")
    assert len(index) >= 30
    by_package = {row["reporting_package"]: row for row in index}
    assert by_package["KTAS"]["snapshot_policy"] == "snapshotted"
    assert int(by_package["KTAS"]["content_length_bytes"]) < 10_000_000
    for package in ("KKNR", "KKTPP"):
        assert by_package[package]["snapshot_policy"] == "too_large_for_default_snapshot"
        assert int(by_package[package]["content_length_bytes"]) > 10_000_000

    catalog = _rows("municipal_finance_catalog_v1.csv")
    assert len(catalog) > 30_000
    packages = {row["reporting_package"] for row in catalog}
    assert packages <= MUNICIPAL_PACKAGES
    assert {"KKLMY", "TOLT", "TOLTA", "TOLTB"} <= packages
    assert any(row["source_url"].endswith("/kklmy-aineistot") for row in catalog)
    assert any(row["source_url"].endswith("/tolt-aineistot") for row in catalog)
    assert {row["accounting_stage"] for row in catalog} <= {
        "budget_plan", "forecast", "cumulative_actual", "reported_annual", "unclassified"
    }
    selected = [row for row in catalog if row["is_selected_document"] == "true"]
    selected_keys = [(row["business_id"], row["reporting_package"], row["reporting_period"]) for row in selected]
    assert not [key for key, count in Counter(selected_keys).items() if count > 1]
    assert any(row["reporting_package"] == "KKNR" and row["reporting_period"] == "2022C03" for row in catalog)
    assert all(row["period_coverage"] != "unknown" for row in catalog)

    taxonomy = _rows("municipal_finance_ktas_taxonomy_v1.csv")
    core = [row for row in taxonomy if row["is_core_metric"] == "true"]
    assert len(core) == len(METRIC_IDS) * len(PLANNING_STAGES) == 32
    assert {row["semantic_metric_id"] for row in core} == set(METRIC_IDS.values())
    assert {row["planning_stage"] for row in core} == {stage for stage, _offset in PLANNING_STAGES.values()}
    assert all(row["unit"] == "EUR" for row in core)
    assert not [key for key, count in Counter(row["indicator_code"] for row in core).items() if count > 1]


def test_ktas_facts() -> None:
    facts = _rows("municipal_finance_ktas_core_v1.csv")
    documents = _rows("municipal_finance_document_manifest_v1.csv")
    metadata = json.loads((REF / "municipal_finance_sources_v1.json").read_text(encoding="utf-8"))
    assert metadata["schema_version"] == "1.0.0"
    assert metadata["is_partial_snapshot"] is False
    assert metadata["semantic_guards"]["ktas_actual_values_allowed"] is False
    assert all(row["fetch_status"] == "ok" for row in documents)
    assert sum(int(row["core_row_count"]) for row in documents) == len(facts)
    expected_documents = {
        (row["business_id"], row["reporting_period"])
        for row in _rows("municipal_finance_catalog_v1.csv")
        if row["reporting_package"] == "KTAS"
        and row["is_selected_document"] == "true"
        and metadata["ktas_year_range"]["start"] <= int(row["period_year"]) <= metadata["ktas_year_range"]["end"]
    }
    assert {(row["business_id"], row["reporting_period"]) for row in documents} == expected_documents
    assert facts
    assert {row["reporting_package"] for row in facts} == {"KTAS"}
    assert {row["accounting_stage"] for row in facts} == {"budget_plan"}
    assert {row["is_actual"] for row in facts} == {"false"}
    assert {row["semantic_metric_id"] for row in facts} == set(METRIC_IDS.values())
    assert {row["planning_stage"] for row in facts} == {stage for stage, _offset in PLANNING_STAGES.values()}
    fact_keys = [
        (row["business_id"], row["reporting_year"], row["semantic_metric_id"], row["planning_stage"])
        for row in facts
    ]
    assert not [key for key, count in Counter(fact_keys).items() if count > 1]
    assert all(row["unit"] == "EUR" for row in facts)
    assert all(row["source_url"].startswith("https://prodkuntarest.") for row in facts)


def test_semantic_guards() -> None:
    assert _period_semantics("KTAS", "2026") == (
        "annual_budget_plan", "full_year_budget_plan", True, "budget_plan"
    )
    assert _period_semantics("KKNR", "2026C03") == (
        "cumulative_quarter", "cumulative_q1", False, "cumulative_actual"
    )
    assert _period_semantics("KKNR", "2026C12") == (
        "cumulative_quarter", "cumulative_q4", True, "cumulative_actual"
    )
    assert _findings([{"havainto": "", "tarkennus": "", "vakavuus": ""}]) == []
    assert len(_findings([{"havainto": "Tarkista", "tarkennus": "", "vakavuus": "VAROITUS"}])) == 1


def test_sql() -> None:
    sql_map = build_sql_map("demo-project", "demo_dataset")
    assert set(sql_map) == {
        "dim_municipal_finance_source_v1",
        "dim_municipal_finance_indicator_v1",
        "analytics_municipal_finance_catalog_v1",
        "analytics_municipal_budget_v1",
        "analytics_municipal_budget_revision_v1",
        "analytics_municipal_finance_coverage_v1",
        "analytics_municipal_finance_quality_v1",
    }
    for name, sql in sql_map.items():
        assert len(sqlglot.parse(sql, read="bigquery")) == 1, name
        assert "TakpMrL" not in sql
    budget = sql_map["analytics_municipal_budget_v1"]
    assert "invalid_ktas_actual_flag" in budget
    assert "approved_budget_not_actual" in budget
    revisions = sql_map["analytics_municipal_budget_revision_v1"]
    assert "not the difference between budget and actual expenditure" in revisions
    catalog = sql_map["analytics_municipal_finance_catalog_v1"]
    assert "quarantine_interest_expense_hierarchy_6200_6299" in catalog
    quality = sql_map["analytics_municipal_finance_quality_v1"]
    assert "ktas_never_presented_as_actual" in quality
    assert "large_taxonomies_guarded" in quality


def main() -> None:
    test_catalog_and_taxonomy()
    test_ktas_facts()
    test_semantic_guards()
    test_sql()
    print("Municipal finance reference tests PASSED")


if __name__ == "__main__":
    main()

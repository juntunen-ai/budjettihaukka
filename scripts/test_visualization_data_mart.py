#!/usr/bin/env python3
"""Offline contract and regression tests for the visualization data mart."""

from __future__ import annotations

import csv
import sys
from collections import Counter, defaultdict
from pathlib import Path

import sqlglot

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_visualization_data_mart import build_sql_map, load_contract
from scripts.load_visualization_reference_series import SERIES
from scripts.run_visualization_data_quality_checks import build_check_sql

SNAPSHOT = ROOT / "data" / "reference" / "official_macro_reference_v1.csv"


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def test_contract() -> None:
    contract = load_contract()
    metrics = {row["metric_id"]: row for row in contract["metrics"]}
    required = {
        "net_accumulation_nominal_eur",
        "net_accumulation_real_cpi_eur",
        "net_accumulation_per_capita_eur",
        "net_accumulation_pct_gdp",
        "original_budget_eur",
        "current_budget_eur",
        "actual_to_budget_ratio",
    }
    _assert(required <= metrics.keys(), "contract is missing required visualization metrics")
    for metric_id, metric in metrics.items():
        for field in (
            "display_name_fi",
            "definition_fi",
            "unit",
            "aggregation",
            "price_basis",
            "source_id",
            "sign_rule",
            "missing_means",
            "supported_dimensions",
            "visualization_status",
        ):
            _assert(metric.get(field), f"{metric_id} missing {field}")
    _assert(metrics["net_accumulation_per_capita_eur"]["aggregation"] == "non_additive", "per-capita must be non-additive")
    _assert(metrics["net_accumulation_pct_gdp"]["aggregation"] == "non_additive", "GDP ratio must be non-additive")
    availability = {row["domain_id"]: row["status"] for row in contract["availability"]}
    _assert(availability["recipients_and_grants"] == "aggregate_pilot", "grant pilot must remain aggregate-only")
    _assert(availability["outputs_and_outcomes"] == "ready_two_sector_pilot", "two sector pilots must be disclosed")
    _assert(availability["audited_final_accounts"] == "ready_with_precision_caveat", "final-account precision must remain explicit")
    interfaces = {row["domain_id"]: row for row in contract["join_interfaces"]}
    for domain_id in ("recipients_and_grants", "procurement", "outputs_and_outcomes", "audited_final_accounts"):
        interface = interfaces[domain_id]
        _assert(interface["status"], f"{domain_id} must disclose integration status")
        _assert(interface["required_keys"] and interface["required_measures"], f"{domain_id} join contract is incomplete")
        _assert(interface["join_rule_fi"] and interface["publication_gate_fi"], f"{domain_id} lacks safety gates")
    _assert(interfaces["recipients_and_grants"]["status"] == "aggregate_pilot_recipient_join_blocked", "recipient joins must fail closed")


def test_snapshot() -> None:
    _assert(SNAPSHOT.exists(), "official macro snapshot is missing")
    with SNAPSHOT.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    expected_series = {spec.series_id for spec in SERIES}
    actual_series = {row["series_id"] for row in rows}
    _assert(actual_series == expected_series, f"unexpected reference series: {actual_series}")
    keys = [(row["series_id"], int(row["year"])) for row in rows]
    duplicates = [key for key, count in Counter(keys).items() if count > 1]
    _assert(not duplicates, f"duplicate series-year values: {duplicates}")

    by_series: dict[str, list[tuple[int, float]]] = defaultdict(list)
    for row in rows:
        by_series[row["series_id"]].append((int(row["year"]), float(row["value"])))
        _assert(row["source_url"].startswith("https://pxdata.stat.fi/"), "reference source must be official StatFin")
    for series_id, values in by_series.items():
        years = sorted(year for year, _value in values)
        _assert(years == list(range(min(years), max(years) + 1)), f"{series_id} has an internal year gap")
    _assert(min(value for _year, value in by_series["population_midyear_persons"]) > 5_000_000, "population magnitude invalid")
    _assert(min(value for _year, value in by_series["gdp_current_prices_meur"]) > 100_000, "GDP magnitude invalid")
    _assert(min(value for _year, value in by_series["central_government_edp_debt_q4_meur"]) > 40_000, "debt magnitude invalid")


def test_sql_contract() -> None:
    sql_map = build_sql_map("demo-project", "demo_dataset")
    expected = {
        "dim_data_source_v1",
        "dim_visualization_metric_v1",
        "dim_data_availability_v1",
        "dim_enrichment_join_contract_v1",
        "visualization_fiscal_source_v1",
        "analytics_macro_yearly_v1",
        "analytics_fiscal_yearly_core_v1",
        "analytics_fiscal_yearly_v1",
        "analytics_metric_series_v1",
        "analytics_budget_vs_actual_v2",
        "analytics_final_accounts_reconciliation_v1",
        "analytics_visualization_quality_v1",
    }
    _assert(set(sql_map) == expected, f"unexpected mart objects: {sorted(sql_map)}")
    for name, sql in sql_map.items():
        parsed = sqlglot.parse(sql, read="bigquery")
        _assert(len(parsed) == 1, f"{name} must contain exactly one statement")
        _assert("TakpMrL" not in sql, f"{name} reintroduces ambiguous source naming")
        _assert("semantic_parser" not in sql and "question" not in sql.lower(), f"{name} depends on natural-language logic")

    fiscal = sql_map["analytics_fiscal_yearly_core_v1"]
    _assert("CREATE OR REPLACE TABLE" in fiscal, "core fact must be materialized for predictable query cost")
    _assert("PARTITION BY RANGE_BUCKET(year" in fiscal, "core fact must be partitioned by year")
    for token in (
        "net_accumulation_nominal_eur",
        "net_accumulation_real_cpi_eur",
        "net_accumulation_per_capita_eur",
        "net_accumulation_pct_gdp",
        "coverage_status",
        "partial_current_year",
        "is_reconciled_to_audited_final_accounts",
        "comparability_status",
        "source_id",
    ):
        _assert(token in fiscal, f"fiscal mart missing {token}")
    _assert("FALSE AS is_reconciled_to_audited_final_accounts" in fiscal, "final accounts must be fail-closed")
    _assert("analytics_fiscal_yearly_core_v1" in sql_map["analytics_fiscal_yearly_v1"], "stable fact view must point to materialized core")

    long_series = sql_map["analytics_metric_series_v1"]
    _assert("dim_visualization_metric_v1" in long_series, "long fact must carry metric definitions")
    _assert("aggregation_rule" in long_series and "missing_means" in long_series, "long fact lacks interpretation metadata")

    budget = sql_map["analytics_budget_vs_actual_v2"]
    _assert("partial_year_not_annual_ratio" in budget, "partial-year budget ratios must be flagged")
    _assert("extreme_ratio_review_required" in budget, "extreme budget ratios must be flagged")

    final_accounts = sql_map["analytics_final_accounts_reconciliation_v1"]
    _assert("official_final_accounts_total_eur" in final_accounts, "final-account interface must expose official total")
    _assert("not_reconciled_official_source_missing" in final_accounts, "missing reconciliation must be explicit")
    _assert("FALSE AS is_reconciled_to_audited_final_accounts" in final_accounts, "final-account interface must fail closed")

    legacy_sql = build_sql_map(
        "demo-project",
        "demo_dataset",
        yearly_source_mode="legacy_mislabeled_maararahalaji",
    )["visualization_fiscal_source_v1"]
    _assert("source.alamomentti_tunnus AS maararahalaji_tunnus" in legacy_sql, "legacy field must be remapped to appropriation type")
    _assert("CAST(NULL AS STRING) AS alamomentti_tunnus" in legacy_sql, "legacy adapter must fail-close alamomentti")
    _assert("FALSE AS talousarviotili_available" in legacy_sql, "legacy adapter must disclose missing budget account")
    _assert("source.alamomentti_tunnus AS alamomentti_tunnus" not in legacy_sql, "appropriation type must never be exposed as alamomentti")

    check_sql = build_check_sql("demo-project", "demo_dataset")
    _assert(len(sqlglot.parse(check_sql, read="bigquery")) == 1, "quality check must be valid single-statement SQL")
    for token in (
        "appropriation_type_published_as_alamomentti_rows",
        "unvalidated_alamomentti_rows",
        "missing_core_macro_years",
        "partial_budget_rows_marked_ready",
    ):
        _assert(token in check_sql, f"quality gate missing {token}")


def main() -> None:
    test_contract()
    test_snapshot()
    test_sql_contract()
    print("Visualization data mart tests PASSED")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Offline regression tests for official enrichment snapshots and SQL views."""

from __future__ import annotations

import csv
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

import sqlglot

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_enrichment_data_mart import build_sql_map
from scripts.load_official_enrichment_reference import _business_id_checksum_valid

REF = ROOT / "data" / "reference"


def _rows(name: str) -> list[dict[str, str]]:
    with (REF / name).open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_snapshots() -> None:
    sectors = _rows("official_sector_indicator_v1.csv")
    assert {row["dashboard_id"] for row in sectors} == {"education", "health"}
    assert {row["indicator_type"] for row in sectors} == {"output", "outcome"}
    assert all(row["is_causal_effect"] == "false" for row in sectors)
    sector_keys = [(row["metric_id"], row["year"], row["region_code"]) for row in sectors]
    assert not [key for key, count in Counter(sector_keys).items() if count > 1]
    assert {row["region_type"] for row in sectors} == {"country", "region"}

    deflators = _rows("official_deflator_reference_v1.csv")
    assert {row["deflator_id"] for row in deflators} == {
        "cpi_general_purchasing_power", "building_cost_investment", "public_service_cost_municipal"
    }
    assert len({row["target_scope"] for row in deflators}) == 3
    assert not [key for key, count in Counter((row["deflator_id"], row["year"]) for row in deflators).items() if count > 1]

    organizations = _rows("organization_master_v1.csv")
    assert len(organizations) >= 3
    assert all(_business_id_checksum_valid(row["business_id"]) for row in organizations)
    assert all(row["business_id_checksum_valid"] == "true" for row in organizations)
    assert all(row["master_scope"] == "reference_organizations_not_grant_joined" for row in organizations)

    grants = _rows("official_grants_okm_pilot_v1.csv")
    assert {row["metric_id"] for row in grants} == {
        "applications_received_count", "positive_decisions_count", "applied_amount_eur", "granted_amount_eur"
    }
    assert all(row["recipient_join_status"] == "blocked_raw_decisions_unavailable" for row in grants)
    assert all(row["geographic_allocation"] == "national_unallocated" for row in grants)

    final_accounts = _rows("official_final_accounts_reference_v1.csv")
    assert len(final_accounts) == 7
    assert {row["fiscal_year"] for row in final_accounts} == {"2025"}
    assert "budget_execution_balance_eur" in {row["metric_id"] for row in final_accounts}

    manifests = _rows("source_vintage_manifest_v1.csv")
    assert len(manifests) == 6
    for row in manifests:
        source_rows = _rows(Path(row["snapshot_path"]).name)
        canonical = [{key: value for key, value in source.items() if key != "vintage_date"} for source in source_rows]
        digest = hashlib.sha256(json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        assert digest == row["content_sha256"]
        assert row["vintage_id"].endswith(row["content_sha256"][:16])


def test_sql() -> None:
    sql_map = build_sql_map("demo-project", "demo_dataset")
    assert set(sql_map) == {
        "source_vintage_current_v1", "source_revision_history_v1", "dim_organization_master_v1",
        "analytics_grants_okm_pilot_v1", "analytics_sector_indicator_v1", "analytics_sector_dashboard_v1",
        "analytics_regional_allocation_v1", "dim_deflator_reference_v1", "analytics_fiscal_multi_deflator_v1",
        "analytics_final_accounts_reconciliation_v2", "analytics_enrichment_quality_v1",
    }
    for name, sql in sql_map.items():
        assert len(sqlglot.parse(sql, read="bigquery")) == 1, name
        assert "TakpMrL" not in sql
    grants = sql_map["analytics_grants_okm_pilot_v1"]
    assert "FALSE AS recipient_level_available" in grants
    assert "FALSE AS budget_moment_join_allowed" in grants
    regional = sql_map["analytics_regional_allocation_v1"]
    assert "FI-UNALLOCATED" in regional
    assert "official_statistical_region" in regional
    deflated = sql_map["analytics_fiscal_multi_deflator_v1"]
    assert "applicability_status" in deflated and "multiplier_to_target_year" in deflated
    reconciled = sql_map["analytics_final_accounts_reconciliation_v2"]
    assert "tolerance_eur" in reconciled and "reconciliation_difference_review_required" in reconciled
    revisions = sql_map["source_revision_history_v1"]
    assert "previous_content_sha256" in revisions and "content_changed" in revisions


def main() -> None:
    test_snapshots()
    test_sql()
    print("Official enrichment reference tests PASSED")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_moment_lineage_layer import build_sql_map  # noqa: E402
from utils.analysis_spec_utils import AnalysisSpec  # noqa: E402
from utils.bigquery_utils import _build_yearly_agg_sql  # noqa: E402


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> None:
    sql_map = build_sql_map("demo-project", "demo_dataset")
    expected_views = {
        "moment_semantic_context_v1",
        "moment_node_catalog_v1",
        "moment_lineage_candidates_v1",
        "moment_lineage_v1",
        "moment_structural_change_guardrails_v1",
        "valtiontalous_yearly_agg_guarded_v1",
    }
    assert_true(set(sql_map) == expected_views, f"unexpected sql map keys: {sorted(sql_map)}")
    lineage_sql = sql_map["moment_lineage_v1"]
    assert_true("'rename'" in lineage_sql, "lineage SQL should contain rename relation")
    assert_true("'moved'" in lineage_sql, "lineage SQL should contain moved relation")
    assert_true("'split'" in lineage_sql, "lineage SQL should contain split relation")
    assert_true("'merge'" in lineage_sql, "lineage SQL should contain merge relation")
    guardrail_sql = sql_map["moment_structural_change_guardrails_v1"]
    assert_true("should_exclude_from_change_rankings" in guardrail_sql, "guardrail SQL should expose ranking exclusion flag")
    guarded_yearly_sql = sql_map["valtiontalous_yearly_agg_guarded_v1"]
    assert_true("has_structural_guardrail" in guarded_yearly_sql, "guarded yearly view should expose structural guardrail flag")

    top_cuts_spec = AnalysisSpec(
        intent="top_cuts",
        metric="nettokertyma",
        fiscal_side="expense",
        entity_level="momentti",
        growth_type="absolute",
        requested_time_from=2008,
        requested_time_to=2020,
        time_from=2008,
        time_to=2020,
        ranking_n=25,
        confidence=0.9,
    )
    top_cuts_sql = _build_yearly_agg_sql("Mitäs momenteista on leikattu prosentuaalisesti eniten 2008-2020?", top_cuts_spec)
    assert_true("has_structural_guardrail_window = 0" in top_cuts_sql, "top_cuts SQL should filter structural guardrails")
    assert_true("alkuvuosi_havaintoja > 0" in top_cuts_sql, "top_cuts SQL should require a start-year observation")
    assert_true("loppuvuosi_havaintoja > 0" in top_cuts_sql, "top_cuts SQL should require an end-year observation")

    top_growth_spec = AnalysisSpec(
        intent="top_growth",
        metric="nettokertyma",
        fiscal_side="expense",
        entity_level="momentti",
        growth_type="absolute",
        requested_time_from=2008,
        requested_time_to=2020,
        time_from=2008,
        time_to=2020,
        ranking_n=25,
        confidence=0.9,
    )
    top_growth_sql = _build_yearly_agg_sql("Mitkä momentit kasvoivat eniten 2008-2020?", top_growth_spec)
    assert_true("has_structural_guardrail_window = 0" in top_growth_sql, "top_growth SQL should filter structural guardrails")
    assert_true("alkuvuosi_havaintoja > 0" in top_growth_sql, "top_growth SQL should require a start-year observation")
    assert_true("loppuvuosi_havaintoja > 0" in top_growth_sql, "top_growth SQL should require an end-year observation")

    revenue_spec = AnalysisSpec(
        intent="revenue_decline",
        metric="nettokertyma",
        fiscal_side="revenue",
        entity_level="momentti",
        growth_type="pct",
        requested_time_from=2008,
        requested_time_to=2020,
        time_from=2008,
        time_to=2020,
        ranking_n=25,
        confidence=0.9,
    )
    revenue_sql = _build_yearly_agg_sql("Mistä verokertymä pieneni eniten 2008-2020?", revenue_spec)
    assert_true("has_structural_guardrail_window = 0" in revenue_sql, "revenue_decline SQL should filter structural guardrails")
    assert_true("alkuvuosi_havaintoja > 0" in revenue_sql, "revenue_decline SQL should require a start-year observation")
    assert_true("loppuvuosi_havaintoja > 0" in revenue_sql, "revenue_decline SQL should require an end-year observation")

    print("Moment lineage layer tests PASSED")


if __name__ == "__main__":
    main()

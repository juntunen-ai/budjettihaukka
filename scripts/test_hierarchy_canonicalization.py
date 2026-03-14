#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_bq_data_quality_layer import (  # noqa: E402
    build_curated_sql,
    build_dimensions_sql,
    build_semantic_view_sql,
)
from utils.analysis_spec_utils import infer_analysis_spec  # noqa: E402
from utils.bigquery_utils import _build_bigquery_fallback_sql  # noqa: E402
from utils.semantic_query_contracts import build_contract_sql  # noqa: E402


def main() -> None:
    curated_sql = build_curated_sql(
        project="demo-project",
        dataset="demo_dataset",
        raw_table="valtiontalous_raw",
        curated_table="valtiontalous_curated_dq_v",
        build_mode="view",
    )
    assert "momentti_display" in curated_sql
    assert "momentti_family_key" in curated_sql
    assert "hallinnonala_display" in curated_sql

    dims_sql = dict(
        build_dimensions_sql(
            project="demo-project",
            dataset="demo_dataset",
            curated_table="valtiontalous_curated_dq_v",
            build_mode="view",
        )
    )
    assert "dim_hierarchy_name_mapping" in dims_sql
    assert "dq_hierarchy_consistency" in dims_sql
    assert "alias_issue_category" in dims_sql["dim_hierarchy_name_mapping"]
    assert "formatting_noise" in dims_sql["dim_hierarchy_name_mapping"]
    assert "family_key_count" in dims_sql["dim_hierarchy_name_mapping"]
    assert "historical_rename" in dims_sql["dq_hierarchy_consistency"]
    assert "same_year_conflict" in dims_sql["dq_hierarchy_consistency"]

    semantic_sql = build_semantic_view_sql(
        project="demo-project",
        dataset="demo_dataset",
        curated_table="valtiontalous_curated_dq_v",
        semantic_view="valtiontalous_semantic_v1",
    )
    assert "dim_hierarchy_name_mapping" in semantic_sql
    assert "hallinnonala_canonical" in semantic_sql
    assert "momentti_canonical" in semantic_sql
    assert "alamomentti_has_same_year_conflict" in semantic_sql

    spec = infer_analysis_spec("Miten korkeakoulujen rahoitus on kehittynyt 2008-2024?")
    contract_sql, contract_name = build_contract_sql(spec, "demo-project.demo_dataset.valtiontalous_semantic_v1")
    assert contract_name in {None, "yoy_change", "trend_by_hallinnonala", "top_growth_moment", "top_growth_alamoment"}
    if contract_sql:
        assert "hallinnonala_canonical" in contract_sql or "momentti_canonical" in contract_sql

    fallback_sql = _build_bigquery_fallback_sql("Miten korkeakoulujen rahoitus on kehittynyt 2008-2024?")
    assert "momentti_canonical" in fallback_sql
    assert "%yliopist%" in fallback_sql
    assert "%ammattikorkeakoul%" in fallback_sql

    print("Hierarchy canonicalization tests PASSED")


if __name__ == "__main__":
    main()

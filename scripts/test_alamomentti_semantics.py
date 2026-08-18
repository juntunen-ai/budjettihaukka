#!/usr/bin/env python3
"""Regression guard for TakpMrL/TakpT semantics and alamomentti fail-closed behavior."""

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
    build_yearly_agg_sql,
)
from services.analysis_orchestrator import analyze_question  # noqa: E402
from utils.analysis_spec_utils import infer_analysis_spec  # noqa: E402
from utils.bigquery_utils import execute_analysis_spec  # noqa: E402
from utils.semantic_query_contracts import (  # noqa: E402
    ALAMOMENTTI_UNAVAILABLE_MESSAGE,
    build_contract_sql,
)


def main() -> None:
    curated_sql = build_curated_sql(
        project="demo-project",
        dataset="demo_dataset",
        raw_table="valtiontalous_raw",
        curated_table="valtiontalous_curated_dq_v",
        build_mode="view",
    )
    assert "`TakpMrL_Tunnus`" in curated_sql and "AS maararahalaji_tunnus" in curated_sql
    assert "`TakpMrL_sNimi`" in curated_sql and "AS maararahalaji_snimi" in curated_sql
    assert "`TakpT_TunnusP`" in curated_sql and "AS talousarviotili_tunnusp" in curated_sql
    assert "`TakpT_sNimi`" in curated_sql and "AS talousarviotili_snimi" in curated_sql
    assert "AS alamomentti_tunnus," not in curated_sql
    assert "AS alamomentti_snimi," not in curated_sql
    assert "STARTS_WITH(talousarviotili_tunnusp, momentti_tunnusp)" in curated_sql
    assert "talousarviotili_tunnusp != momentti_tunnusp" in curated_sql
    assert "AS alamomentti_tunnus_candidate" in curated_sql

    dimensions = dict(
        build_dimensions_sql(
            project="demo-project",
            dataset="demo_dataset",
            curated_table="valtiontalous_curated_dq_v",
            build_mode="view",
        )
    )
    assert "dim_maararahalaji" in dimensions
    assert "dim_talousarviotili" in dimensions
    validated_sql = dimensions["dim_alamomentti"]
    assert "official_code_registry_v1" in validated_sql
    assert "registry.year = source.vuosi" in validated_sql
    assert "registry.level IN ('talousarviotili', 'alamomentti')" in validated_sql
    assert "registry.code_dotted = source.talousarviotili_tunnusp" in validated_sql

    semantic_sql = build_semantic_view_sql(
        project="demo-project",
        dataset="demo_dataset",
        curated_table="valtiontalous_curated_dq_v",
        semantic_view="valtiontalous_semantic_v2",
    )
    assert "source.maararahalaji_tunnus" in semantic_sql
    assert "source.talousarviotili_tunnusp" in semantic_sql
    assert "validated_alamomentti.alamomentti_tunnus" in semantic_sql
    assert "AS alamomentti_is_validated" in semantic_sql
    assert "'not_in_official_chart'" in semantic_sql
    assert "source.alamomentti_tunnus AS `TakpMrL_Tunnus`" not in semantic_sql
    assert "source.alamomentti_snimi AS `TakpMrL_sNimi`" not in semantic_sql

    yearly_sql = build_yearly_agg_sql(
        project="demo-project",
        dataset="demo_dataset",
        semantic_view="valtiontalous_semantic_v2",
        yearly_agg_table="valtiontalous_yearly_agg_v2",
    )
    assert "maararahalaji_tunnus" in yearly_sql
    assert "talousarviotili_tunnusp" in yearly_sql
    assert "IF(alamomentti_is_validated" in yearly_sql
    assert "TakpMrL" not in yearly_sql

    spec = infer_analysis_spec("Mitkä alamomentit kasvoivat eniten 2020-2024 euroissa?")
    assert spec.entity_level in {"alamomentti", "molemmat"}
    sql, contract = build_contract_sql(spec, "demo-project.demo_dataset.semantic")
    assert sql is None and contract is None

    execution = execute_analysis_spec("Mitkä alamomentit kasvoivat eniten 2020-2024 euroissa?", spec)
    assert execution["error_class"] == "unsupported_entity_level"
    assert execution["sql_query"] == ""
    assert ALAMOMENTTI_UNAVAILABLE_MESSAGE in execution["error"]

    result = analyze_question("Mitkä alamomentit kasvoivat eniten 2020-2024 euroissa?")
    assert result.status == "unsupported"
    assert result.error_class == "unsupported_entity_level"
    assert result.sql_query is None

    spaced_result = analyze_question("Mitkä budjetin ala momentit kasvoivat eniten 2020-2024?")
    assert spaced_result.status == "unsupported"
    assert spaced_result.error_class == "unsupported_entity_level"
    assert spaced_result.sql_query is None

    generic = infer_analysis_spec("Mitkä momentit kasvoivat eniten 2020-2024?")
    level_prompts = [item for item in generic.clarifications if item.field == "entity_level"]
    assert all("Alamomentti" not in item.options and "Molemmat" not in item.options for item in level_prompts)

    print("Alamomentti semantic regression tests PASSED")


if __name__ == "__main__":
    main()

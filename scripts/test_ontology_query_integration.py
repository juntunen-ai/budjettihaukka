#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.analysis_spec_utils import infer_analysis_spec  # noqa: E402
import utils.bigquery_utils as bq_utils  # noqa: E402
from utils.semantic_query_contracts import build_contract_sql  # noqa: E402


def main() -> None:
    university_spec = infer_analysis_spec("Miten yliopistojen rahoitus on kehittynyt 2008-2024?")
    assert university_spec.resolved_concept_id == "yliopistot", university_spec
    assert university_spec.resolved_concept_label == "Yliopistot", university_spec
    assert university_spec.entity_level == "momentti", university_spec
    assert any("Ontologinen tulkinta" in assumption for assumption in university_spec.assumptions)
    assert "yliopistot" in university_spec.resolved_concept_ids

    higher_ed_spec = infer_analysis_spec("Miten korkeakoulujen rahoitus on kehittynyt 2008-2024?")
    assert higher_ed_spec.resolved_concept_id == "korkeakoulutus", higher_ed_spec
    assert any(field.field == "ontology_scope" for field in higher_ed_spec.clarifications), higher_ed_spec
    assert higher_ed_spec.ontology_must_clarify is True, higher_ed_spec

    original_fetch = bq_utils._fetch_ontology_membership_rules
    try:
        bq_utils._fetch_ontology_membership_rules = lambda concept_id: (
            {
                "rule_scope": "include",
                "hierarchy_level": "momentti",
                "match_type": "canonical_name_pattern",
                "value": "%yliopist%",
                "valid_from_year": None,
                "valid_to_year": None,
                "confidence": 0.95,
                "rule_id": "yliopistot_include_01",
            },
            {
                "rule_scope": "exclude",
                "hierarchy_level": "momentti",
                "match_type": "canonical_name_pattern",
                "value": "%ammattikorkeakoul%",
                "valid_from_year": None,
                "valid_to_year": None,
                "confidence": 0.99,
                "rule_id": "yliopistot_exclude_01",
            },
        )

        scope_clause = bq_utils._ontology_scope_clause(university_spec, "bigquery")
        assert scope_clause is not None
        assert "momentti_canonical" in scope_clause
        assert "%yliopist%" in scope_clause
        assert "%ammattikorkeakoul%" in scope_clause

        fallback_sql = bq_utils._build_bigquery_fallback_sql(
            "Miten yliopistojen rahoitus on kehittynyt 2008-2024?",
            analysis_spec=university_spec,
        )
        assert "%yliopist%" in fallback_sql
        assert "%ammattikorkeakoul%" in fallback_sql

        contract_sql, contract_name = build_contract_sql(
            university_spec,
            "demo-project.demo_dataset.valtiontalous_semantic_v1",
            extra_where=scope_clause,
        )
        assert contract_name is None
        assert contract_sql is None
    finally:
        bq_utils._fetch_ontology_membership_rules = original_fetch

    print("Ontology query integration tests PASSED")


if __name__ == "__main__":
    main()

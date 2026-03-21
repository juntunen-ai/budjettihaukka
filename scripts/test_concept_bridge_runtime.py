#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.analysis_spec_utils import infer_analysis_spec
import utils.bigquery_utils as bq_utils


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> None:
    spec = infer_analysis_spec("Miten puolustusmenot kehittyivät 2018-2024?")
    assert_true(spec.resolved_concept_id == "puolustus", f"unexpected concept: {spec.resolved_concept_id}")

    original_bridge = bq_utils._fetch_concept_bridge_rules
    original_rules = bq_utils._fetch_ontology_membership_rules
    try:
        bq_utils._fetch_concept_bridge_rules = lambda analysis_spec, dialect: (
            {
                "rule_scope": "include",
                "hierarchy_level": "momentti",
                "match_type": "exact_code",
                "value": "27.10.01.",
                "valid_from_year": 2008,
                "valid_to_year": None,
                "confidence": 0.99,
                "rule_id": "puolustus_bridge_001",
            },
        )
        bq_utils._fetch_ontology_membership_rules = lambda concept_id: (
            {
                "rule_scope": "include",
                "hierarchy_level": "momentti",
                "match_type": "code_prefix",
                "value": "27.",
                "valid_from_year": None,
                "valid_to_year": None,
                "confidence": 0.99,
                "rule_id": "puolustus_include_01",
            },
            {
                "rule_scope": "exclude",
                "hierarchy_level": "momentti",
                "match_type": "canonical_name_pattern",
                "value": "%arvonlisävero%",
                "valid_from_year": None,
                "valid_to_year": None,
                "confidence": 0.95,
                "rule_id": "puolustus_exclude_01",
            },
        )

        scope_clause = bq_utils._ontology_scope_clause(spec, "bigquery")
        assert_true(scope_clause is not None, "scope clause missing")
        assert_true("27.10.01." in scope_clause, f"bridge exact code missing: {scope_clause}")
        assert_true("27.%" not in scope_clause, f"should prefer bridge exact codes over broad prefix: {scope_clause}")
        assert_true("%arvonlisävero%" in scope_clause, f"ontology exclude should still remain: {scope_clause}")
    finally:
        bq_utils._fetch_concept_bridge_rules = original_bridge
        bq_utils._fetch_ontology_membership_rules = original_rules

    spec2 = infer_analysis_spec("Miten yliopistojen rahoitus on kehittynyt 2008-2024?")
    original_bridge = bq_utils._fetch_concept_bridge_rules
    original_rules = bq_utils._fetch_ontology_membership_rules
    try:
        bq_utils._fetch_concept_bridge_rules = lambda analysis_spec, dialect: tuple()
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
        )
        scope_clause = bq_utils._ontology_scope_clause(spec2, "yearly_agg")
        assert_true(scope_clause is not None, "fallback scope clause missing")
        assert_true("%yliopist%" in scope_clause, f"expected ontology fallback pattern: {scope_clause}")
    finally:
        bq_utils._fetch_concept_bridge_rules = original_bridge
        bq_utils._fetch_ontology_membership_rules = original_rules

    print("Concept bridge runtime tests PASSED")


if __name__ == "__main__":
    main()

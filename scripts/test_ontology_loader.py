#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.ontology_utils import (  # noqa: E402
    default_ontology_path,
    flatten_budget_ontology,
    load_budget_ontology,
    resolve_concepts_for_question,
    validate_budget_ontology,
)


def main() -> None:
    ontology = load_budget_ontology(default_ontology_path())
    issues = validate_budget_ontology(ontology)
    assert not issues, issues
    assert ontology.ontology_id == "budjettihaukka"
    assert len(ontology.concepts) == 30

    concept_ids = {concept.concept_id for concept in ontology.concepts}
    assert "yliopistot" in concept_ids
    assert "ammatillinen_koulutus" in concept_ids
    assert "puolustus" in concept_ids
    assert "asumistuki" in concept_ids

    flattened = flatten_budget_ontology(ontology)
    assert len(flattened["ontology_concept"]) == 30
    assert len(flattened["ontology_alias"]) >= 500
    assert len(flattened["ontology_membership_rule"]) >= 60
    assert len(flattened["ontology_viz_recipe"]) >= 30

    concept_rows = {row["concept_id"]: row for row in flattened["ontology_concept"]}
    assert concept_rows["asumistuki"]["default_fiscal_side"] == "expense"
    assert concept_rows["verotulot"]["default_fiscal_side"] == "revenue"

    matches = resolve_concepts_for_question("Miten yliopistojen rahoitus on kehittynyt 2008-2024?", ontology)
    assert matches, "No matches for yliopistot question"
    assert matches[0].concept_id in {"yliopistot", "korkeakoulutus"}

    matches = resolve_concepts_for_question("Miten puolustusmenot ovat kasvaneet 2010-2024?", ontology)
    assert matches, "No matches for puolustus question"
    assert matches[0].concept_id == "puolustus"

    matches = resolve_concepts_for_question("Miten ammattikoulutuksen rahoitus on muuttunut?", ontology)
    assert matches, "No matches for ammatillinen koulutus question"
    assert matches[0].concept_id == "ammatillinen_koulutus"

    matches = resolve_concepts_for_question("Miten asumistuen menot ovat kehittyneet 2000-2024?", ontology)
    assert matches, "No matches for asumistuki question"
    assert matches[0].concept_id == "asumistuki"
    assert matches[0].default_fiscal_side == "expense"

    print("Ontology loader tests PASSED")


if __name__ == "__main__":
    main()

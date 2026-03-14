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
    assert len(ontology.concepts) == 20

    concept_ids = {concept.concept_id for concept in ontology.concepts}
    assert "yliopistot" in concept_ids
    assert "ammatillinen_koulutus" in concept_ids
    assert "puolustus" in concept_ids

    flattened = flatten_budget_ontology(ontology)
    assert len(flattened["ontology_concept"]) == 20
    assert len(flattened["ontology_alias"]) >= 60
    assert len(flattened["ontology_membership_rule"]) >= 40
    assert len(flattened["ontology_viz_recipe"]) >= 20

    matches = resolve_concepts_for_question("Miten yliopistojen rahoitus on kehittynyt 2008-2024?", ontology)
    assert matches, "No matches for yliopistot question"
    assert matches[0].concept_id in {"yliopistot", "korkeakoulutus"}

    matches = resolve_concepts_for_question("Miten puolustusmenot ovat kasvaneet 2010-2024?", ontology)
    assert matches, "No matches for puolustus question"
    assert matches[0].concept_id == "puolustus"

    matches = resolve_concepts_for_question("Miten ammattikoulutuksen rahoitus on muuttunut?", ontology)
    assert matches, "No matches for ammatillinen koulutus question"
    assert matches[0].concept_id == "ammatillinen_koulutus"

    print("Ontology loader tests PASSED")


if __name__ == "__main__":
    main()

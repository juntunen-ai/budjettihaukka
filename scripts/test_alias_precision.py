#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.ontology_utils import load_budget_ontology, resolve_concepts_for_question  # noqa: E402


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> None:
    ontology = load_budget_ontology()

    bad_question = "Mitäs momenteista on leikattu prosentuaalisesti eniten 2008-2020?"
    bad_matches = resolve_concepts_for_question(bad_question, ontology, limit=5)
    bad_ids = [match.concept_id for match in bad_matches]
    assert_true("oikeuslaitos" not in bad_ids[:1], f"oikeuslaitos should not be top match for false-positive acronym case: {bad_ids}")

    defense_matches = resolve_concepts_for_question("Miten puolustusmenot kehittyivät 2018-2024?", ontology, limit=3)
    assert_true(defense_matches, "defense query should resolve to a concept")
    assert_true(defense_matches[0].concept_id == "puolustus", f"unexpected top defense concept: {defense_matches[0]}")

    university_matches = resolve_concepts_for_question("Miten yliopistojen rahoitus on kehittynyt 2008-2024?", ontology, limit=3)
    assert_true(university_matches, "university query should resolve")
    assert_true(university_matches[0].concept_id == "yliopistot", f"unexpected university concept: {university_matches[0]}")

    print("Alias precision tests PASSED")


if __name__ == "__main__":
    main()

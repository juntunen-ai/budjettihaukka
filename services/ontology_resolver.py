from __future__ import annotations

from functools import lru_cache
from typing import Any

from domain.contracts import ResolvedAnalysis
from utils.analysis_spec_utils import AnalysisSpec
from utils.ontology_utils import BudgetOntology, OntologyConcept, load_budget_ontology


@lru_cache(maxsize=1)
def load_runtime_ontology() -> BudgetOntology | None:
    try:
        return load_budget_ontology()
    except Exception:
        return None


def _rule_to_dict(rule: Any, scope: str) -> dict[str, Any]:
    return {
        "scope": scope,
        "hierarchy_level": rule.hierarchy_level,
        "match_type": rule.match_type,
        "value": rule.value,
        "valid_from_year": rule.valid_from_year,
        "valid_to_year": rule.valid_to_year,
        "confidence": rule.confidence,
    }


def _concept_notes(spec: AnalysisSpec, concept: OntologyConcept | None) -> list[str]:
    notes = list(spec.assumptions)
    if concept is None and spec.resolved_concept_id:
        notes.append(f"Konseptia {spec.resolved_concept_id} ei löytynyt ontologiasta paikallisesta storesta.")
    elif concept is not None:
        notes.append(f"Canonical concept: {concept.label_fi} ({concept.concept_id})")
    return notes


def resolve_analysis(question: str, spec: AnalysisSpec) -> ResolvedAnalysis:
    ontology = load_runtime_ontology()
    concept = ontology.concepts_by_id().get(spec.resolved_concept_id) if ontology and spec.resolved_concept_id else None
    include_rules = [_rule_to_dict(rule, "include") for rule in concept.include_rules] if concept else []
    exclude_rules = [_rule_to_dict(rule, "exclude") for rule in concept.exclude_rules] if concept else []
    return ResolvedAnalysis(
        question=question,
        analysis_spec=spec,
        concept_id=spec.resolved_concept_id,
        concept_label=spec.resolved_concept_label,
        include_rules=include_rules,
        exclude_rules=exclude_rules,
        ambiguity_notes=_concept_notes(spec, concept),
    )

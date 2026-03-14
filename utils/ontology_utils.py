from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from config import settings


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _tokenize(value: str) -> list[str]:
    normalized = _normalize_text(value)
    return [token for token in re.split(r"[^0-9a-zåäö]+", normalized) if token]


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


@dataclass(frozen=True)
class OntologyExternalRef:
    source: str
    uri: str
    label: str | None = None


@dataclass(frozen=True)
class OntologyAlias:
    alias: str
    source: str
    alias_type: str
    lang: str = "fi"


@dataclass(frozen=True)
class OntologyMembershipRule:
    hierarchy_level: str
    match_type: str
    value: str
    valid_from_year: int | None = None
    valid_to_year: int | None = None
    confidence: float = 1.0


@dataclass(frozen=True)
class OntologyVizRecipe:
    intent: str
    primary_chart: str
    secondary_chart: str | None = None


@dataclass(frozen=True)
class OntologyGuardrail:
    ambiguity_reason: str
    clarification_question: str


@dataclass
class OntologyConcept:
    concept_id: str
    label_fi: str
    description_fi: str
    policy_theme: str
    broader_concept_id: str | None
    narrower_concept_ids: list[str]
    default_entity_level: str
    default_metric: str
    default_intents: list[str]
    risk_level: str
    must_clarify: bool
    clarification_question: str | None = None
    aliases: list[OntologyAlias] = field(default_factory=list)
    external_refs: list[OntologyExternalRef] = field(default_factory=list)
    include_rules: list[OntologyMembershipRule] = field(default_factory=list)
    exclude_rules: list[OntologyMembershipRule] = field(default_factory=list)
    visualization_recipes: list[OntologyVizRecipe] = field(default_factory=list)
    guardrails: list[OntologyGuardrail] = field(default_factory=list)

    def all_aliases(self) -> list[str]:
        values = [self.label_fi]
        values.extend(alias.alias for alias in self.aliases)
        seen: set[str] = set()
        deduped: list[str] = []
        for value in values:
            normalized = _normalize_text(value)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(value)
        return deduped


@dataclass
class BudgetOntology:
    ontology_id: str
    version: int
    label_fi: str
    description_fi: str
    language: str
    concepts: list[OntologyConcept]

    def concepts_by_id(self) -> dict[str, OntologyConcept]:
        return {concept.concept_id: concept for concept in self.concepts}


@dataclass(frozen=True)
class ResolvedConcept:
    concept_id: str
    label_fi: str
    score: float
    matched_aliases: tuple[str, ...]
    risk_level: str
    must_clarify: bool


def default_ontology_path() -> Path:
    base = Path(__file__).resolve().parents[1]
    configured = Path(settings.ontology_path)
    if configured.is_absolute():
        return configured
    return (base / configured).resolve()


def _coerce_aliases(raw_aliases: list[dict[str, Any]]) -> list[OntologyAlias]:
    aliases: list[OntologyAlias] = []
    for item in raw_aliases:
        aliases.append(
            OntologyAlias(
                alias=str(item["alias"]).strip(),
                source=str(item.get("source", "manual")).strip(),
                alias_type=str(item.get("alias_type", "alt")).strip(),
                lang=str(item.get("lang", "fi")).strip(),
            )
        )
    return aliases


def _coerce_external_refs(raw_refs: list[dict[str, Any]]) -> list[OntologyExternalRef]:
    refs: list[OntologyExternalRef] = []
    for item in raw_refs:
        refs.append(
            OntologyExternalRef(
                source=str(item["source"]).strip(),
                uri=str(item["uri"]).strip(),
                label=str(item.get("label", "")).strip() or None,
            )
        )
    return refs


def _coerce_rules(raw_rules: list[dict[str, Any]]) -> list[OntologyMembershipRule]:
    rules: list[OntologyMembershipRule] = []
    for item in raw_rules:
        rules.append(
            OntologyMembershipRule(
                hierarchy_level=str(item["hierarchy_level"]).strip(),
                match_type=str(item["match_type"]).strip(),
                value=str(item["value"]).strip(),
                valid_from_year=item.get("valid_from_year"),
                valid_to_year=item.get("valid_to_year"),
                confidence=float(item.get("confidence", 1.0)),
            )
        )
    return rules


def _coerce_viz_recipes(raw_recipes: list[dict[str, Any]]) -> list[OntologyVizRecipe]:
    recipes: list[OntologyVizRecipe] = []
    for item in raw_recipes:
        recipes.append(
            OntologyVizRecipe(
                intent=str(item["intent"]).strip(),
                primary_chart=str(item["primary_chart"]).strip(),
                secondary_chart=str(item.get("secondary_chart", "")).strip() or None,
            )
        )
    return recipes


def _coerce_guardrails(raw_guardrails: list[dict[str, Any]]) -> list[OntologyGuardrail]:
    guardrails: list[OntologyGuardrail] = []
    for item in raw_guardrails:
        guardrails.append(
            OntologyGuardrail(
                ambiguity_reason=str(item["ambiguity_reason"]).strip(),
                clarification_question=str(item["clarification_question"]).strip(),
            )
        )
    return guardrails


def load_budget_ontology(path: str | Path | None = None) -> BudgetOntology:
    ontology_path = Path(path) if path else default_ontology_path()
    raw = yaml.safe_load(ontology_path.read_text(encoding="utf-8"))
    concepts: list[OntologyConcept] = []
    for item in raw.get("concepts", []):
        concepts.append(
            OntologyConcept(
                concept_id=str(item["concept_id"]).strip(),
                label_fi=str(item["label_fi"]).strip(),
                description_fi=str(item.get("description_fi", "")).strip(),
                policy_theme=str(item.get("policy_theme", "")).strip(),
                broader_concept_id=_optional_str(item.get("broader_concept_id")),
                narrower_concept_ids=[str(v).strip() for v in item.get("narrower_concept_ids", []) if str(v).strip()],
                default_entity_level=str(item.get("default_entity_level", "kokonais")).strip(),
                default_metric=str(item.get("default_metric", "nettokertyma")).strip(),
                default_intents=[str(v).strip() for v in item.get("default_intents", []) if str(v).strip()],
                risk_level=str(item.get("risk_level", "medium")).strip(),
                must_clarify=bool(item.get("must_clarify", False)),
                clarification_question=_optional_str(item.get("clarification_question")),
                aliases=_coerce_aliases(item.get("aliases", [])),
                external_refs=_coerce_external_refs(item.get("external_refs", [])),
                include_rules=_coerce_rules(item.get("include_rules", [])),
                exclude_rules=_coerce_rules(item.get("exclude_rules", [])),
                visualization_recipes=_coerce_viz_recipes(item.get("visualization_recipes", [])),
                guardrails=_coerce_guardrails(item.get("guardrails", [])),
            )
        )
    return BudgetOntology(
        ontology_id=str(raw.get("ontology_id", "budjettihaukka")).strip(),
        version=int(raw.get("version", 1)),
        label_fi=str(raw.get("label_fi", "Budjettihaukka Ontologia")).strip(),
        description_fi=str(raw.get("description_fi", "")).strip(),
        language=str(raw.get("language", "fi")).strip(),
        concepts=concepts,
    )


def validate_budget_ontology(ontology: BudgetOntology) -> list[str]:
    issues: list[str] = []
    concept_ids = [concept.concept_id for concept in ontology.concepts]
    if len(concept_ids) != len(set(concept_ids)):
        issues.append("Duplicate concept_id values found.")

    known_levels = {"kokonais", "hallinnonala", "kirjanpitoyksikko", "momentti", "alamomentti"}
    known_risks = {"low", "medium", "high"}
    known_match_types = {
        "canonical_name_pattern",
        "canonical_exact",
        "name_pattern",
        "exact_code",
        "code_prefix",
        "concept_ref",
    }

    concept_set = set(concept_ids)
    for concept in ontology.concepts:
        if concept.broader_concept_id and concept.broader_concept_id not in concept_set:
            issues.append(f"{concept.concept_id}: unknown broader_concept_id={concept.broader_concept_id}")
        for child_id in concept.narrower_concept_ids:
            if child_id not in concept_set:
                issues.append(f"{concept.concept_id}: unknown narrower_concept_id={child_id}")
        if concept.default_entity_level not in known_levels:
            issues.append(f"{concept.concept_id}: invalid default_entity_level={concept.default_entity_level}")
        if concept.risk_level not in known_risks:
            issues.append(f"{concept.concept_id}: invalid risk_level={concept.risk_level}")
        if concept.must_clarify and not concept.clarification_question and not concept.guardrails:
            issues.append(f"{concept.concept_id}: must_clarify requires clarification_question or guardrails")
        if not concept.aliases:
            issues.append(f"{concept.concept_id}: aliases missing")
        for rule_group_name, rules in (("include_rules", concept.include_rules), ("exclude_rules", concept.exclude_rules)):
            if not rules:
                continue
            for rule in rules:
                if rule.hierarchy_level not in known_levels:
                    issues.append(
                        f"{concept.concept_id}: invalid hierarchy_level={rule.hierarchy_level} in {rule_group_name}"
                    )
                if rule.match_type not in known_match_types:
                    issues.append(
                        f"{concept.concept_id}: invalid match_type={rule.match_type} in {rule_group_name}"
                    )
                if not rule.value:
                    issues.append(f"{concept.concept_id}: empty value in {rule_group_name}")
    return issues


def flatten_budget_ontology(ontology: BudgetOntology) -> dict[str, list[dict[str, Any]]]:
    concepts: list[dict[str, Any]] = []
    aliases: list[dict[str, Any]] = []
    rules: list[dict[str, Any]] = []
    viz_recipes: list[dict[str, Any]] = []
    guardrails: list[dict[str, Any]] = []
    external_refs: list[dict[str, Any]] = []

    for concept in ontology.concepts:
        concepts.append(
            {
                "ontology_id": ontology.ontology_id,
                "ontology_version": ontology.version,
                "concept_id": concept.concept_id,
                "label_fi": concept.label_fi,
                "description_fi": concept.description_fi,
                "policy_theme": concept.policy_theme,
                "broader_concept_id": concept.broader_concept_id,
                "narrower_concept_ids": json.dumps(concept.narrower_concept_ids, ensure_ascii=False),
                "default_entity_level": concept.default_entity_level,
                "default_metric": concept.default_metric,
                "default_intents": json.dumps(concept.default_intents, ensure_ascii=False),
                "risk_level": concept.risk_level,
                "must_clarify": concept.must_clarify,
                "clarification_question": concept.clarification_question,
            }
        )
        for alias in concept.aliases:
            aliases.append(
                {
                    "ontology_id": ontology.ontology_id,
                    "ontology_version": ontology.version,
                    "concept_id": concept.concept_id,
                    "alias": alias.alias,
                    "source": alias.source,
                    "alias_type": alias.alias_type,
                    "lang": alias.lang,
                }
            )
        for ref in concept.external_refs:
            external_refs.append(
                {
                    "ontology_id": ontology.ontology_id,
                    "ontology_version": ontology.version,
                    "concept_id": concept.concept_id,
                    "source": ref.source,
                    "uri": ref.uri,
                    "label": ref.label,
                }
            )
        for scope_name, scoped_rules in (("include", concept.include_rules), ("exclude", concept.exclude_rules)):
            for idx, rule in enumerate(scoped_rules, start=1):
                rules.append(
                    {
                        "ontology_id": ontology.ontology_id,
                        "ontology_version": ontology.version,
                        "concept_id": concept.concept_id,
                        "rule_id": f"{concept.concept_id}_{scope_name}_{idx:02d}",
                        "rule_scope": scope_name,
                        "hierarchy_level": rule.hierarchy_level,
                        "match_type": rule.match_type,
                        "value": rule.value,
                        "valid_from_year": rule.valid_from_year,
                        "valid_to_year": rule.valid_to_year,
                        "confidence": rule.confidence,
                    }
                )
        for idx, recipe in enumerate(concept.visualization_recipes, start=1):
            viz_recipes.append(
                {
                    "ontology_id": ontology.ontology_id,
                    "ontology_version": ontology.version,
                    "concept_id": concept.concept_id,
                    "recipe_id": f"{concept.concept_id}_viz_{idx:02d}",
                    "intent": recipe.intent,
                    "primary_chart": recipe.primary_chart,
                    "secondary_chart": recipe.secondary_chart,
                }
            )
        if concept.clarification_question:
            guardrails.append(
                {
                    "ontology_id": ontology.ontology_id,
                    "ontology_version": ontology.version,
                    "concept_id": concept.concept_id,
                    "guardrail_id": f"{concept.concept_id}_base",
                    "ambiguity_reason": "base_clarification",
                    "clarification_question": concept.clarification_question,
                }
            )
        for idx, guardrail in enumerate(concept.guardrails, start=1):
            guardrails.append(
                {
                    "ontology_id": ontology.ontology_id,
                    "ontology_version": ontology.version,
                    "concept_id": concept.concept_id,
                    "guardrail_id": f"{concept.concept_id}_{idx:02d}",
                    "ambiguity_reason": guardrail.ambiguity_reason,
                    "clarification_question": guardrail.clarification_question,
                }
            )

    return {
        "ontology_concept": concepts,
        "ontology_alias": aliases,
        "ontology_external_ref": external_refs,
        "ontology_membership_rule": rules,
        "ontology_viz_recipe": viz_recipes,
        "ontology_guardrail": guardrails,
    }


def resolve_concepts_for_question(
    question: str,
    ontology: BudgetOntology,
    limit: int = 5,
) -> list[ResolvedConcept]:
    question_norm = _normalize_text(question)
    question_tokens = set(_tokenize(question))
    scored: list[ResolvedConcept] = []

    for concept in ontology.concepts:
        matched_aliases: list[str] = []
        score = 0.0
        for alias in concept.all_aliases():
            alias_norm = _normalize_text(alias)
            if not alias_norm:
                continue
            alias_tokens = set(_tokenize(alias))
            if alias_norm in question_norm:
                matched_aliases.append(alias)
                score += 1.0 + min(len(alias_norm) / 40.0, 1.0)
                continue
            if alias_tokens and alias_tokens.issubset(question_tokens):
                matched_aliases.append(alias)
                score += 0.75 + min(len(alias_tokens) * 0.15, 0.6)
        if score <= 0:
            continue
        if concept.risk_level == "high":
            score += 0.05
        scored.append(
            ResolvedConcept(
                concept_id=concept.concept_id,
                label_fi=concept.label_fi,
                score=round(score, 4),
                matched_aliases=tuple(sorted(set(matched_aliases), key=lambda v: (_normalize_text(v), v))),
                risk_level=concept.risk_level,
                must_clarify=concept.must_clarify,
            )
        )

    scored.sort(key=lambda item: (-item.score, item.concept_id))
    return scored[: max(1, limit)]

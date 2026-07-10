from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from config import settings


TOKEN_RE = re.compile(r"[0-9A-Za-zÅÄÖåäö]+(?:-[0-9A-Za-zÅÄÖåäö]+)*")

ALIAS_REVIEW_STATUSES = {"gold", "reviewed", "candidate", "blocked"}
OBSERVABILITY_CLASSES = {"exact", "composite", "proxy", "unsupported"}

CONCEPT_OBSERVABILITY_DEFAULTS: dict[str, tuple[str, str]] = {
    "asumistuki": (
        "exact",
        "Asumistuki voidaan rajata suoraan tunnistettuihin asumistukimomentteihin koko nykyisen analyysipolun kannalta käyttökelpoisella ajanjaksolla.",
    ),
    "puolustus": (
        "composite",
        "Puolustusmenot koostuvat useista puolustusministeriön ja puolustusvoimien momenteista, joten vastaus perustuu koottuun konseptirajaukseen.",
    ),
    "yliopistot": (
        "composite",
        "Yliopistorahoitus muodostuu useista yliopistoihin liittyvistä budjettimomenteista ja voi muuttua rakenteellisesti vuosien välillä.",
    ),
    "ammatillinen_koulutus": (
        "composite",
        "Ammatillisen koulutuksen rahoitus koostuu useista koulutus- ja valtionosuusmomenteista.",
    ),
    "varhaiskasvatus": (
        "proxy",
        "Varhaiskasvatus ei näy kaikissa vuosissa yhtenä puhtaana budjettikäsitteenä, vaan osin laajemmissa valtionosuus- tai koulutusmomenteissa.",
    ),
    "verotulot": (
        "composite",
        "Verotulot ovat usean verolajin kokonaisuus, joka kannattaa tarvittaessa tarkentaa yksittäiseen verolajiin.",
    ),
    "velka_ja_korkomenot": (
        "composite",
        "Velka ja korkomenot yhdistävät useita velanhoitoon liittyviä eriä, joiden semantiikka vaihtelee.",
    ),
    "kuntien_valtionosuudet": (
        "composite",
        "Kuntien valtionosuudet muodostuvat useista siirto- ja rahoitusmomenteista.",
    ),
    "hyvinvointialueiden_rahoitus": (
        "composite",
        "Hyvinvointialueiden rahoitus on koottu useista sote-rahoituksen budjettieristä.",
    ),
    "yritystuet": (
        "proxy",
        "Yritystuet ovat käsitteenä laaja ja osin tulkinnanvarainen; analyysi perustuu konseptirajaukseen, ei yksiselitteiseen viralliseen momenttiluokkaan.",
    ),
}

ALIAS_TYPE_DEFAULTS: dict[str, dict[str, Any]] = {
    "label": {"precision_score": 0.99, "review_status": "gold"},
    "pref": {"precision_score": 0.98, "review_status": "gold"},
    "canonical": {"precision_score": 0.9, "review_status": "reviewed"},
    "inflected": {"precision_score": 0.9, "review_status": "gold"},
    "alt": {"precision_score": 0.82, "review_status": "reviewed"},
    "english": {"precision_score": 0.72, "review_status": "reviewed"},
    "colloquial": {"precision_score": 0.7, "review_status": "reviewed"},
    "vm_phrase": {"precision_score": 0.62, "review_status": "candidate"},
    "vm_token": {"precision_score": 0.18, "review_status": "blocked"},
    # Finto/YSO: altLabel on sanaston virallinen synonyymi (luotettava);
    # narrower on alakäsite — hyvä vihje muttei sama käsite.
    "finto_alt": {"precision_score": 0.8, "review_status": "reviewed"},
    "finto_narrower": {"precision_score": 0.55, "review_status": "candidate"},
    "abbreviation": {"precision_score": 0.58, "review_status": "reviewed", "is_acronym": True, "requires_token_boundary": True},
}


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _tokenize(value: str) -> list[str]:
    normalized = _normalize_text(value)
    return [token for token in re.split(r"[^0-9a-zåäö]+", normalized) if token]


def _tokenize_with_original(value: str) -> list[tuple[str, str]]:
    tokens: list[tuple[str, str]] = []
    for match in TOKEN_RE.finditer(value or ""):
        original = match.group(0)
        normalized = _normalize_text(original)
        if normalized:
            tokens.append((original, normalized))
    return tokens


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
    precision_score: float = 0.7
    requires_token_boundary: bool = False
    is_acronym: bool = False
    review_status: str = "reviewed"
    valid_from_year: int | None = None
    valid_to_year: int | None = None


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
    default_fiscal_side: str
    default_intents: list[str]
    risk_level: str
    must_clarify: bool
    clarification_question: str | None = None
    observability_class: str = "composite"
    observability_reason: str = ""
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
    default_fiscal_side: str


def _default_observability_for_concept(concept_id: str) -> tuple[str, str]:
    return CONCEPT_OBSERVABILITY_DEFAULTS.get(
        concept_id,
        ("composite", "Käsite tulkitaan koottuna budjettikonseptina, ellei ontologiassa määritellä tarkempaa answerability-luokkaa."),
    )


def _default_alias_metadata(alias: str, source: str, alias_type: str) -> dict[str, Any]:
    alias_text = str(alias or "").strip()
    base = dict(ALIAS_TYPE_DEFAULTS.get(alias_type, {"precision_score": 0.7, "review_status": "reviewed"}))
    is_acronym = bool(base.get("is_acronym")) or (
        len(alias_text) <= 5 and alias_text.isupper() and any(char.isalpha() for char in alias_text)
    )
    requires_token_boundary = bool(base.get("requires_token_boundary")) or len(alias_text) < 4 or " " in alias_text
    if source == "vm_vocabulary" and alias_type == "canonical":
        base["review_status"] = "reviewed"
    if source == "manual" and alias_type in {"pref", "inflected"}:
        base["review_status"] = "gold"
    return {
        "precision_score": float(base.get("precision_score", 0.7)),
        "requires_token_boundary": requires_token_boundary,
        "is_acronym": is_acronym,
        "review_status": str(base.get("review_status", "reviewed")),
    }


def default_ontology_path() -> Path:
    base = Path(__file__).resolve().parents[1]
    configured = Path(settings.ontology_path)
    if configured.is_absolute():
        return configured
    return (base / configured).resolve()


def _coerce_aliases(raw_aliases: list[dict[str, Any]]) -> list[OntologyAlias]:
    aliases: list[OntologyAlias] = []
    for item in raw_aliases:
        alias = str(item["alias"]).strip()
        source = str(item.get("source", "manual")).strip()
        alias_type = str(item.get("alias_type", "alt")).strip()
        defaults = _default_alias_metadata(alias, source, alias_type)
        review_status = str(item.get("review_status", defaults["review_status"])).strip().lower()
        if review_status not in ALIAS_REVIEW_STATUSES:
            review_status = "reviewed"
        aliases.append(
            OntologyAlias(
                alias=alias,
                source=source,
                alias_type=alias_type,
                lang=str(item.get("lang", "fi")).strip(),
                precision_score=float(item.get("precision_score", defaults["precision_score"])),
                requires_token_boundary=bool(item.get("requires_token_boundary", defaults["requires_token_boundary"])),
                is_acronym=bool(item.get("is_acronym", defaults["is_acronym"])),
                review_status=review_status,
                valid_from_year=item.get("valid_from_year"),
                valid_to_year=item.get("valid_to_year"),
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
                default_fiscal_side=str(item.get("default_fiscal_side", "mixed")).strip(),
                default_intents=[str(v).strip() for v in item.get("default_intents", []) if str(v).strip()],
                risk_level=str(item.get("risk_level", "medium")).strip(),
                must_clarify=bool(item.get("must_clarify", False)),
                clarification_question=_optional_str(item.get("clarification_question")),
                observability_class=str(item.get("observability_class") or _default_observability_for_concept(str(item["concept_id"]).strip())[0]).strip(),
                observability_reason=str(item.get("observability_reason") or _default_observability_for_concept(str(item["concept_id"]).strip())[1]).strip(),
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
    known_fiscal_sides = {"expense", "revenue", "financing", "technical", "mixed", "unknown"}
    known_observability_classes = OBSERVABILITY_CLASSES
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
        if concept.default_fiscal_side not in known_fiscal_sides:
            issues.append(f"{concept.concept_id}: invalid default_fiscal_side={concept.default_fiscal_side}")
        if concept.risk_level not in known_risks:
            issues.append(f"{concept.concept_id}: invalid risk_level={concept.risk_level}")
        if concept.observability_class not in known_observability_classes:
            issues.append(f"{concept.concept_id}: invalid observability_class={concept.observability_class}")
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
        for alias in concept.aliases:
            if alias.review_status not in ALIAS_REVIEW_STATUSES:
                issues.append(f"{concept.concept_id}: invalid alias review_status={alias.review_status} for alias {alias.alias}")
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
                "default_fiscal_side": concept.default_fiscal_side,
                "default_intents": json.dumps(concept.default_intents, ensure_ascii=False),
                "risk_level": concept.risk_level,
                "must_clarify": concept.must_clarify,
                "clarification_question": concept.clarification_question,
                "observability_class": concept.observability_class,
                "observability_reason": concept.observability_reason,
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
                    "precision_score": alias.precision_score,
                    "requires_token_boundary": alias.requires_token_boundary,
                    "is_acronym": alias.is_acronym,
                    "review_status": alias.review_status,
                    "valid_from_year": alias.valid_from_year,
                    "valid_to_year": alias.valid_to_year,
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
    question_token_pairs = _tokenize_with_original(question)
    question_tokens = {normalized for _original, normalized in question_token_pairs}
    # Väliviivasanat ("sote-palveluihin") pidetään kysymyksessä kokonaisina,
    # mutta aliakset tokenisoituvat osiin — lisätään osat kysymysjoukkoon,
    # jotta väliviivalliset aliakset voivat osua.
    for token in list(question_tokens):
        if "-" in token:
            question_tokens.update(part for part in token.split("-") if part)
    question_upper_tokens = {original for original, _normalized in question_token_pairs if original.isupper()}
    scored: list[ResolvedConcept] = []

    for concept in ontology.concepts:
        matched_aliases: list[str] = []
        score = 0.0
        synthetic_label_alias = OntologyAlias(
            alias=concept.label_fi,
            source="ontology_label",
            alias_type="label",
            lang="fi",
            **_default_alias_metadata(concept.label_fi, "ontology_label", "label"),
        )
        for alias in [synthetic_label_alias, *concept.aliases]:
            if alias.review_status == "blocked":
                continue
            alias_norm = _normalize_text(alias.alias)
            if not alias_norm:
                continue
            alias_tokens = set(_tokenize(alias.alias))
            matched = False

            if alias.is_acronym:
                matched = alias.alias in question_upper_tokens
            elif " " in alias_norm or alias.requires_token_boundary:
                phrase_pattern = re.compile(rf"(?<![0-9a-zåäö]){re.escape(alias_norm)}(?![0-9a-zåäö])")
                matched = bool(phrase_pattern.search(question_norm))
            elif alias_tokens:
                matched = alias_tokens.issubset(question_tokens)
                if not matched and " " in alias_norm and alias.precision_score >= 0.9 and len(alias_norm) >= 5:
                    matched = alias_norm in question_norm

            if matched:
                matched_aliases.append(alias.alias)
                review_bonus = {"gold": 0.25, "reviewed": 0.08, "candidate": -0.18}.get(alias.review_status, 0.0)
                source_bonus = {"ontology_label": 0.25, "manual": 0.15, "vm_vocabulary": -0.05}.get(alias.source, 0.0)
                token_bonus = min(len(alias_tokens) * 0.08, 0.32)
                score += alias.precision_score + review_bonus + source_bonus + token_bonus
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
                default_fiscal_side=concept.default_fiscal_side,
            )
        )

    scored.sort(key=lambda item: (-item.score, item.concept_id))
    return scored[: max(1, limit)]

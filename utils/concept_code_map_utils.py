"""Runtime access to human-decided concept→budget-code maps.

Maps live as versioned YAML under data/ontology/concept_code_map/ (one file
per concept, decided in review dossiers). At query time the rules become an
inline SQL predicate — literal codes, no extra table joins, so the SQL
security gate's table whitelist is untouched.

Momentti-level rules override luku-level rules by construction: exclusions
are applied as NOT (...) on top of the include set.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

MAP_DIR = Path(__file__).resolve().parents[1] / "data" / "ontology" / "concept_code_map"

_DIALECT_EXPRS = {
    "bigquery": {
        "momentti": "NULLIF(`Momentti_TunnusP`, '')",
        "vuosi": "SAFE_CAST(`Vuosi` AS INT64)",
    },
    "yearly_agg": {"momentti": "momentti_tunnusp", "vuosi": "vuosi"},
    "demo": {"momentti": "momentti_tunnusp", "vuosi": "vuosi"},
}


@lru_cache(maxsize=None)
def _load_concept_doc(concept_id: str) -> dict[str, Any] | None:
    path = MAP_DIR / f"{concept_id}.yaml"
    if not path.exists():
        return None
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(doc, dict) or doc.get("concept") != concept_id:
        return None
    return doc


def has_curated_map(concept_id: str | None) -> bool:
    return bool(concept_id) and _load_concept_doc(concept_id) is not None


def _rule_predicate(rule: dict[str, Any], momentti_expr: str, vuosi_expr: str) -> str | None:
    code = str(rule.get("code", "")).strip()
    if not code:
        return None
    year_from = rule.get("year_from") or 0
    year_to = rule.get("year_to") or 9999
    if rule.get("level") == "momentti":
        code_predicate = f"{momentti_expr} = '{code}'"
    else:
        code_predicate = f"STARTS_WITH({momentti_expr}, '{code}')"
    return f"({code_predicate} AND {vuosi_expr} BETWEEN {year_from} AND {year_to})"


def curated_scope_clause(concept_id: str | None, dialect: str = "bigquery") -> str | None:
    """WHERE-clause fragment from the curated map, or None if no map exists."""
    if not concept_id:
        return None
    doc = _load_concept_doc(concept_id)
    if doc is None:
        return None
    exprs = _DIALECT_EXPRS.get(dialect, _DIALECT_EXPRS["bigquery"])

    includes: list[str] = []
    excludes: list[str] = []
    for rule in doc.get("rules", []):
        predicate = _rule_predicate(rule, exprs["momentti"], exprs["vuosi"])
        if not predicate:
            continue
        role = rule.get("role")
        if role in ("include", "component"):
            includes.append(predicate)
        elif role == "exclude":
            excludes.append(predicate)

    if not includes:
        return None
    clause = "(" + " OR ".join(includes) + ")"
    if excludes:
        clause += " AND NOT (" + " OR ".join(excludes) + ")"
    return "(" + clause + ")"


def definition_meta(concept_id: str | None) -> dict[str, Any] | None:
    """Metadata for the answer's 'näin laskin' / disclosure block."""
    if not concept_id:
        return None
    doc = _load_concept_doc(concept_id)
    if doc is None:
        return None
    components = sorted(
        {
            rule.get("component")
            for rule in doc.get("rules", [])
            if rule.get("role") == "component" and rule.get("component")
        }
    )
    include_count = sum(1 for r in doc.get("rules", []) if r.get("role") in ("include", "component"))
    exclude_count = sum(1 for r in doc.get("rules", []) if r.get("role") == "exclude")
    return {
        "concept_id": concept_id,
        "label": doc.get("label_fi") or concept_id,
        "version": doc.get("version", 1),
        "decided_by": doc.get("decided_by"),
        "decided_on": doc.get("decided_on"),
        "disclosure_fi": (doc.get("disclosure_fi") or "").strip() or None,
        "components": components,
        "include_rule_count": include_count,
        "exclude_rule_count": exclude_count,
    }

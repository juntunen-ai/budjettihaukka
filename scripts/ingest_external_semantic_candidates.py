from __future__ import annotations

import argparse
import html
import json
import re
import sys
import urllib.parse
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.ontology_utils import load_budget_ontology

DEFAULT_SEED_PATH = Path("data/ontology/external_semantic_seed.yaml")
DEFAULT_OUTPUT_PATH = Path("data/ontology/external_semantic_candidates_v1.jsonl")
DEFAULT_REPORT_PATH = Path("docs/reports/external_semantic_candidates_report.md")
USER_AGENT = "Budjettihaukka/1.0"
NEXT_DATA_RE = re.compile(r'__NEXT_DATA__" type="application/json">(.*?)</script>')


def _normalize(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _strip_html(value: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", " ", value or "")).replace("\xa0", " ").strip()


def _fetch_text(url: str) -> str:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8", "ignore")


def _fetch_json(url: str) -> dict[str, Any]:
    return json.loads(_fetch_text(url))


def _is_acronym(alias: str) -> bool:
    return len(alias) <= 5 and alias.isupper() and any(ch.isalpha() for ch in alias)


def _requires_token_boundary(alias: str) -> bool:
    return len(alias) < 4 or " " in alias or _is_acronym(alias)


def _candidate_score(source_kind: str, alias: str) -> float:
    base = {
        "pref": 0.78,
        "recommended": 0.78,
        "hidden": 0.64,
        "alt": 0.64,
        "synonym": 0.58,
        "not_recommended": 0.46,
    }.get(source_kind, 0.52)
    if _is_acronym(alias):
        base -= 0.12
    if len(alias) < 4:
        base -= 0.08
    return max(0.18, min(base, 0.92))


def _finto_vocab_from_source(source: str, uri: str) -> str:
    if source.endswith("_juho"):
        return "juho"
    if source.endswith("_yso"):
        return "yso"
    if "/juho/" in uri:
        return "juho"
    return "yso"


def _label_values(node: dict[str, Any], key: str, lang: str = "fi") -> list[str]:
    values = []
    raw = node.get(key)
    if raw is None:
        return values
    if isinstance(raw, dict):
        raw = [raw]
    for item in raw:
        if isinstance(item, dict) and item.get("lang") == lang and item.get("value"):
            values.append(str(item["value"]).strip())
    return values


def _build_graph_label_index(graph: list[dict[str, Any]], lang: str = "fi") -> dict[str, str]:
    index: dict[str, str] = {}
    for node in graph:
        uri = str(node.get("uri") or "").strip()
        if not uri:
            continue
        labels = _label_values(node, "prefLabel", lang=lang) or _label_values(node, "label", lang=lang)
        if labels:
            index[uri] = labels[0]
    return index


def _extract_finto_rows(
    *,
    concept_id: str,
    concept_label_fi: str,
    source_system: str,
    source_uri: str,
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    graph = payload.get("graph") or []
    node = next((item for item in graph if item.get("uri") == source_uri), None)
    if not node:
        return []
    label_index = _build_graph_label_index(graph)
    broader_labels = []
    narrower_labels = []
    related_labels = []

    broader = node.get("broader")
    if isinstance(broader, dict) and broader.get("uri"):
        broader_labels.append(label_index.get(str(broader["uri"]), str(broader["uri"])))
    elif isinstance(broader, list):
        for item in broader:
            if isinstance(item, dict) and item.get("uri"):
                broader_labels.append(label_index.get(str(item["uri"]), str(item["uri"])))

    for relation_key, bucket in (("narrower", narrower_labels), ("related", related_labels)):
        relation_values = node.get(relation_key) or []
        if isinstance(relation_values, dict):
            relation_values = [relation_values]
        for item in relation_values:
            if isinstance(item, dict) and item.get("uri"):
                bucket.append(label_index.get(str(item["uri"]), str(item["uri"])))

    records: list[dict[str, Any]] = []
    for source_kind, key in (("pref", "prefLabel"), ("hidden", "hiddenLabel"), ("alt", "altLabel")):
        for alias in _label_values(node, key, lang="fi"):
            records.append(
                {
                    "concept_id": concept_id,
                    "concept_label_fi": concept_label_fi,
                    "alias": alias,
                    "lang": "fi",
                    "candidate_alias_type": source_kind,
                    "source_system": source_system,
                    "source_kind": source_kind,
                    "source_uri": source_uri,
                    "source_label": _label_values(node, "prefLabel", lang="fi")[0] if _label_values(node, "prefLabel", lang="fi") else None,
                    "review_status": "candidate",
                    "precision_score": _candidate_score(source_kind, alias),
                    "requires_token_boundary": _requires_token_boundary(alias),
                    "is_acronym": _is_acronym(alias),
                    "valid_from_year": None,
                    "valid_to_year": None,
                    "context_broader_labels": broader_labels,
                    "context_narrower_labels": narrower_labels[:10],
                    "context_related_labels": related_labels[:10],
                    "definition_fi": None,
                    "notes": "Controlled external candidate from Finto. Requires manual review before ontology promotion.",
                }
            )
    return records


def fetch_finto_by_uri(source_uri: str) -> dict[str, Any]:
    url = "https://api.finto.fi/rest/v1/data?" + urllib.parse.urlencode({"uri": source_uri, "format": "application/json"})
    return _fetch_json(url)


def resolve_finto_search(vocab: str, query: str, expected_pref_label_fi: str | None = None) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    url = "https://api.finto.fi/rest/v1/search?" + urllib.parse.urlencode({"vocab": vocab, "query": query, "lang": "fi"})
    payload = _fetch_json(url)
    expected_norm = _normalize(expected_pref_label_fi or "")
    for result in payload.get("results") or []:
        pref = _normalize(str(result.get("prefLabel") or ""))
        hidden = {_normalize(str(item.get("value") if isinstance(item, dict) else item)) for item in result.get("hiddenLabel") or []} if isinstance(result.get("hiddenLabel"), list) else set()
        if expected_norm and pref != expected_norm and expected_norm not in hidden:
            continue
        uri = str(result.get("uri") or "").strip()
        if uri:
            return uri, fetch_finto_by_uri(uri)
    results = payload.get("results") or []
    if results:
        uri = str(results[0].get("uri") or "").strip()
        if uri:
            return uri, fetch_finto_by_uri(uri)
    return None, None


def _extract_sanastot_rows(
    *,
    concept_id: str,
    concept_label_fi: str,
    source_system: str,
    source_url: str,
    html_text: str,
) -> list[dict[str, Any]]:
    match = NEXT_DATA_RE.search(html_text)
    if not match:
        return []
    next_data = json.loads(match.group(1))
    actions = next_data.get("props", {}).get("pageProps", {}).get("reduxWrapperActionsGSSP", [])
    concept_payload = next((item.get("payload") for item in actions if item.get("type") == "conceptAPI/executeQuery/fulfilled"), None)
    terminology_payload = next((item.get("payload") for item in actions if item.get("type") == "terminologyApi/executeQuery/fulfilled"), None)
    if not concept_payload:
        return []

    terminology_label = None
    if isinstance(terminology_payload, dict):
        terminology_label = (terminology_payload.get("label") or {}).get("fi")

    broader_labels = [
        label.get("fi")
        for item in concept_payload.get("broader") or []
        for label in [item.get("label") or {}]
        if isinstance(label, dict) and label.get("fi")
    ]
    narrower_labels = [
        label.get("fi")
        for item in concept_payload.get("narrower") or []
        for label in [item.get("label") or {}]
        if isinstance(label, dict) and label.get("fi")
    ]
    related_labels = [
        label.get("fi")
        for item in concept_payload.get("related") or []
        for label in [item.get("label") or {}]
        if isinstance(label, dict) and label.get("fi")
    ]
    definition_fi = _strip_html((concept_payload.get("definition") or {}).get("fi", "")) or None

    term_groups = [
        ("recommended", concept_payload.get("recommendedTerms") or []),
        ("synonym", concept_payload.get("synonyms") or []),
        ("not_recommended", concept_payload.get("notRecommendedTerms") or []),
    ]
    records: list[dict[str, Any]] = []
    for source_kind, items in term_groups:
        for item in items:
            if str(item.get("language") or "") != "fi":
                continue
            alias = str(item.get("label") or "").strip()
            if not alias:
                continue
            records.append(
                {
                    "concept_id": concept_id,
                    "concept_label_fi": concept_label_fi,
                    "alias": alias,
                    "lang": "fi",
                    "candidate_alias_type": source_kind,
                    "source_system": source_system,
                    "source_kind": source_kind,
                    "source_uri": str(concept_payload.get("uri") or source_url),
                    "source_label": (concept_payload.get("recommendedTerms") or [{"label": None}])[0].get("label"),
                    "review_status": "candidate",
                    "precision_score": _candidate_score(source_kind, alias),
                    "requires_token_boundary": _requires_token_boundary(alias),
                    "is_acronym": _is_acronym(alias),
                    "valid_from_year": None,
                    "valid_to_year": None,
                    "context_broader_labels": broader_labels,
                    "context_narrower_labels": narrower_labels[:10],
                    "context_related_labels": related_labels[:10],
                    "definition_fi": definition_fi,
                    "notes": f"Controlled external candidate from {source_system} ({terminology_label or 'terminology'}). Requires manual review before ontology promotion.",
                }
            )
    return records


def _existing_alias_index() -> dict[str, set[str]]:
    ontology = load_budget_ontology()
    index: dict[str, set[str]] = {}
    for concept in ontology.concepts:
        values = {_normalize(concept.label_fi)}
        values.update(_normalize(alias.alias) for alias in concept.aliases)
        index[concept.concept_id] = {value for value in values if value}
    return index


def _ontology_label_index() -> dict[str, str]:
    ontology = load_budget_ontology()
    return {concept.concept_id: concept.label_fi for concept in ontology.concepts}


def _auto_items_from_ontology() -> list[dict[str, Any]]:
    ontology = load_budget_ontology()
    items: list[dict[str, Any]] = []
    for concept in ontology.concepts:
        for ref in concept.external_refs:
            if not ref.source.startswith("finto_"):
                continue
            items.append(
                {
                    "concept_id": concept.concept_id,
                    "source": "finto_uri",
                    "source_system": ref.source,
                    "vocab": _finto_vocab_from_source(ref.source, ref.uri),
                    "uri": ref.uri,
                    "expected_pref_label_fi": ref.label or concept.label_fi,
                }
            )
    return items


def _seed_items(seed_path: Path) -> list[dict[str, Any]]:
    raw = yaml.safe_load(seed_path.read_text(encoding="utf-8")) if seed_path.exists() else {}
    items: list[dict[str, Any]] = []
    for concept_item in raw.get("concept_sources", []):
        concept_id = str(concept_item.get("concept_id") or "").strip()
        for item in concept_item.get("items", []):
            row = dict(item)
            row["concept_id"] = concept_id
            items.append(row)
    return items


def build_candidates(seed_path: Path) -> list[dict[str, Any]]:
    concept_labels = _ontology_label_index()
    existing_aliases = _existing_alias_index()
    source_items = _auto_items_from_ontology() + _seed_items(seed_path)
    seen_sources: set[tuple[str, str, str]] = set()
    deduped_source_items: list[dict[str, Any]] = []
    for item in source_items:
        key = (str(item.get("concept_id")), str(item.get("source")), str(item.get("uri") or item.get("url") or item.get("query")))
        if key in seen_sources:
            continue
        seen_sources.add(key)
        deduped_source_items.append(item)

    output: list[dict[str, Any]] = []
    seen_aliases: set[tuple[str, str, str, str]] = set()
    for item in deduped_source_items:
        concept_id = str(item["concept_id"])
        concept_label_fi = concept_labels.get(concept_id, concept_id)
        source = str(item.get("source") or "").strip()
        source_system = str(item.get("source_system") or source).strip()
        rows: list[dict[str, Any]] = []
        if source == "finto_uri":
            uri = str(item.get("uri") or "").strip()
            if uri:
                rows = _extract_finto_rows(
                    concept_id=concept_id,
                    concept_label_fi=concept_label_fi,
                    source_system=source_system,
                    source_uri=uri,
                    payload=fetch_finto_by_uri(uri),
                )
        elif source == "finto_search":
            uri, payload = resolve_finto_search(
                str(item.get("vocab") or "yso"),
                str(item.get("query") or concept_label_fi),
                str(item.get("expected_pref_label_fi") or "") or None,
            )
            if uri and payload:
                rows = _extract_finto_rows(
                    concept_id=concept_id,
                    concept_label_fi=concept_label_fi,
                    source_system=source_system,
                    source_uri=uri,
                    payload=payload,
                )
        elif source == "sanastot_concept":
            url = str(item.get("url") or "").strip()
            if url:
                rows = _extract_sanastot_rows(
                    concept_id=concept_id,
                    concept_label_fi=concept_label_fi,
                    source_system=source_system,
                    source_url=url,
                    html_text=_fetch_text(url),
                )

        for row in rows:
            alias_norm = _normalize(str(row.get("alias") or ""))
            if not alias_norm:
                continue
            if alias_norm in existing_aliases.get(concept_id, set()):
                continue
            dedupe_key = (concept_id, alias_norm, str(row.get("source_system")), str(row.get("source_kind")))
            if dedupe_key in seen_aliases:
                continue
            seen_aliases.add(dedupe_key)
            output.append(row)
    output.sort(key=lambda row: (row["concept_id"], -float(row["precision_score"]), row["alias"]))
    return output


def write_jsonl(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_report(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    by_source: dict[str, int] = {}
    by_concept: dict[str, int] = {}
    for row in rows:
        by_source[row["source_system"]] = by_source.get(row["source_system"], 0) + 1
        by_concept[row["concept_id"]] = by_concept.get(row["concept_id"], 0) + 1
    with path.open("w", encoding="utf-8") as handle:
        handle.write("# External Semantic Candidate Report\n\n")
        handle.write("Controlled external enrichment candidates from Finto and Sanastot. These rows are **not** runtime aliases. They are a review queue for Phase 2.\n\n")
        handle.write(f"- Candidate rows: {len(rows)}\n")
        handle.write(f"- Concepts covered: {len(by_concept)}\n")
        handle.write(f"- Sources covered: {len(by_source)}\n\n")
        handle.write("## By source\n\n")
        for source, count in sorted(by_source.items()):
            handle.write(f"- `{source}`: {count}\n")
        handle.write("\n## By concept\n\n")
        for concept_id, count in sorted(by_concept.items()):
            handle.write(f"- `{concept_id}`: {count}\n")
        handle.write("\n## Sample candidates\n\n")
        for row in rows[:20]:
            handle.write(
                f"- `{row['concept_id']}` -> `{row['alias']}` "
                f"({row['source_system']} / {row['source_kind']}, score={row['precision_score']:.2f})\n"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Build controlled external semantic alias candidates from Finto and Sanastot.")
    parser.add_argument("--seed-path", type=Path, default=DEFAULT_SEED_PATH)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--report-path", type=Path, default=DEFAULT_REPORT_PATH)
    args = parser.parse_args()

    candidates = build_candidates(args.seed_path)
    write_jsonl(candidates, args.output_path)
    write_report(candidates, args.report_path)
    print(f"External semantic candidates: {len(candidates)} -> {args.output_path}")
    print(f"Report: {args.report_path}")


if __name__ == "__main__":
    main()

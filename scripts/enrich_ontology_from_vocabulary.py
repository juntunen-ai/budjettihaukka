#!/usr/bin/env python3
"""
Enrich the Budjettihaukka ontology with aliases and vocabulary from VM budget data.

This script:
1. Loads the current ontology (budjettihaukka_ontology.yaml)
2. Loads the extracted VM budget vocabulary (vm_budget_vocabulary.jsonl)
3. For each ontology concept, finds matching VM budget items via include_rules
4. Extracts official Finnish names and name variants as alias candidates
5. Adds new aliases (source: vm_vocabulary) that don't already exist
6. Writes updated ontology back to YAML
7. Generates a report of additions

This enriches the ontology with:
- Official VM budget item names (canonical names used in numerotaulu)
- Historical name variants (names used in earlier years)
- Key domain tokens extracted from official names
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.ontology_utils import load_budget_ontology

DEFAULT_ONTOLOGY_PATH = Path("data/ontology/budjettihaukka_ontology.yaml")
DEFAULT_VOCAB_PATH = Path("data/semantic_enrichment/vm_budget_site/vm_budget_vocabulary.jsonl")
DEFAULT_REPORT_PATH = Path("docs/reports/ontology_enrichment_report.md")

# Common Finnish words to exclude from alias generation
STOP_WORDS = {
    "ja", "tai", "sekä", "muut", "muu", "muita", "eräät", "eräiden",
    "hallinnonala", "hallinnonalan", "toimintamenot", "arvonlisäveromenot",
    "siirtomääräraha", "arviomääräraha", "kiinteä", "määräraha", "menot",
    "tulot", "valtionavut", "valtionavustukset", "avustukset", "avustus",
    "osasto", "pääluokka", "luku", "momentti",
    "yhteensä", "yhteenveto", "selvitysosa",
    "euroa", "eur", "1000",
    # Generic administrative terms that add noise
    "toimintamenot", "palvelukeskuksen", "palvelukeskusten",
    "yhteydessä", "toimivien", "viranomaisten", "toteuttaminen",
    "aiheutuvat", "korvaamisesta", "perusteella",
    "kannettavat", "maksut", "yhteiset", "kehittäminen",
    "erityisviranomaiset",
    # Generic connector/function words that leak into aliases
    "johtuvista", "menoista", "liittyvien", "koskevan",
    "ulkomaisen", "alueellisen", "linjauksen", "laadintaan",
    "tehtävien", "hoitaminen", "eräistä", "eräiden",
    "yleinen", "julkinen", "julkisen", "edistäminen",
    "maksuosuudet", "osallistumismaksut", "jäsenmaksut",
    "korvaukset", "palautukset", "siirtomenot",
    "käyttöä", "käytön", "palvelut",
}

# Words starting/ending with "-" are partial compound words — skip them
PARTIAL_COMPOUND_RE = re.compile(r"^-|^.*-$")

# Minimum alias length
MIN_ALIAS_LEN = 5


def _read_jsonl(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def _match_rule(rule: dict, vocab_entry: dict) -> bool:
    """Check if a vocabulary entry matches an ontology membership rule."""
    match_type = rule.get("match_type", "")
    value = rule.get("value", "")
    rule_level = rule.get("hierarchy_level", "")

    entry_code = vocab_entry["hierarchy_code"]
    entry_name = vocab_entry["canonical_name_fi"].lower()
    entry_level = vocab_entry["hierarchy_level"]

    # Level mapping: ontology uses different names than vocabulary
    LEVEL_COMPAT = {
        "hallinnonala": {"osasto"},
        "kirjanpitoyksikko": {"luku"},
        "momentti": {"momentti", "luku"},  # momentti rules can also match luku
        "alamomentti": {"momentti"},
        "osasto": {"osasto"},
        "luku": {"luku"},
    }
    if rule_level:
        compatible = LEVEL_COMPAT.get(rule_level, {rule_level})
        if entry_level not in compatible:
            return False

    if match_type == "exact_code":
        return entry_code == value
    elif match_type == "code_prefix":
        return entry_code.startswith(value)
    elif match_type in ("canonical_name_pattern", "name_pattern"):
        # SQL LIKE pattern: % = wildcard → convert to regex
        # First replace % with placeholder, then escape, then restore
        lowered = value.lower()
        parts = lowered.split("%")
        pattern = ".*".join(re.escape(p) for p in parts)
        return bool(re.search(pattern, entry_name))
    elif match_type == "canonical_exact":
        return entry_name == value.lower()

    return False


def _clean_token(token: str) -> str:
    """Strip trailing/leading punctuation from a token."""
    return token.strip(".,;:()[]{}\"'")


def _is_valid_token(token: str) -> bool:
    """Check if a token is a valid alias candidate."""
    token = _clean_token(token)
    if len(token) < MIN_ALIAS_LEN:
        return False
    if token in STOP_WORDS:
        return False
    if PARTIAL_COMPOUND_RE.match(token):
        return False
    if not any(c.isalpha() for c in token):
        return False
    return True


def _extract_aliases_from_name(name: str) -> list[str]:
    """Extract meaningful alias candidates from an official budget name."""
    candidates = []
    name_lower = name.lower()

    # The full name itself (if meaningful and not too long/generic)
    if 10 <= len(name_lower) <= 55:
        candidates.append(name_lower)

    tokens = [_clean_token(t) for t in name_lower.split()]
    meaningful = [t for t in tokens if _is_valid_token(t)]

    # Single tokens: only domain-specific words (8+ chars to avoid noise)
    for token in meaningful:
        if len(token) >= 8:
            candidates.append(token)

    # Two-word phrases from meaningful adjacent tokens
    if len(meaningful) >= 2:
        for i in range(len(meaningful) - 1):
            pair = f"{meaningful[i]} {meaningful[i + 1]}"
            if len(pair) >= 14:
                candidates.append(pair)

    return candidates


def find_matching_vocab(concept: dict, vocab: list[dict]) -> list[dict]:
    """Find all vocabulary entries matching a concept's include_rules."""
    include_rules = concept.get("include_rules", [])
    exclude_rules = concept.get("exclude_rules", [])
    matched = []

    for entry in vocab:
        # Check include rules
        included = any(_match_rule(rule, entry) for rule in include_rules)
        if not included:
            continue

        # Check exclude rules
        excluded = any(_match_rule(rule, entry) for rule in exclude_rules)
        if excluded:
            continue

        matched.append(entry)

    return matched


def enrich_concept(concept: dict, vocab: list[dict]) -> list[dict]:
    """
    Generate new alias entries for a concept from matching VM vocabulary.

    Returns a list of new alias dicts to add.
    """
    matched_vocab = find_matching_vocab(concept, vocab)
    if not matched_vocab:
        return []

    # Collect existing aliases
    existing_aliases = {
        alias["alias"].lower()
        for alias in concept.get("aliases", [])
    }

    new_aliases = []
    seen = set()

    for entry in matched_vocab:
        # Extract aliases from canonical name
        canonical_candidates = _extract_aliases_from_name(entry["canonical_name_fi"])

        # Extract from historical name variants
        variant_candidates = []
        for variant in entry.get("name_variants", []):
            variant_candidates.extend(_extract_aliases_from_name(variant["name"]))

        all_candidates = canonical_candidates + variant_candidates

        for candidate in all_candidates:
            candidate_lower = candidate.lower().strip()

            # Skip if already exists or already added
            if candidate_lower in existing_aliases or candidate_lower in seen:
                continue
            if len(candidate_lower) < MIN_ALIAS_LEN:
                continue

            # Determine alias type
            if candidate_lower == entry["canonical_name_fi"].lower():
                alias_type = "canonical"
            elif " " in candidate_lower:
                alias_type = "vm_phrase"
            else:
                alias_type = "vm_token"

            new_aliases.append({
                "alias": candidate_lower,
                "source": "vm_vocabulary",
                "alias_type": alias_type,
                "lang": "fi",
            })
            seen.add(candidate_lower)

    return new_aliases


def load_ontology_raw(path: Path) -> dict:
    """Load ontology YAML preserving structure for modification."""
    import yaml
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def save_ontology_raw(path: Path, data: dict) -> None:
    """Save ontology YAML with clean formatting."""
    import yaml

    class FlowListDumper(yaml.SafeDumper):
        pass

    def represent_list(dumper, data):
        # Use flow style for short lists (narrower_concept_ids, default_intents, years_present)
        if all(isinstance(item, str) and len(str(item)) < 30 for item in data) and len(data) <= 8:
            return dumper.represent_sequence("tag:yaml.org,2002:seq", data, flow_style=True)
        return dumper.represent_sequence("tag:yaml.org,2002:seq", data, flow_style=False)

    FlowListDumper.add_representer(list, represent_list)

    with path.open("w", encoding="utf-8") as f:
        yaml.dump(
            data, f,
            Dumper=FlowListDumper,
            allow_unicode=True,
            default_flow_style=False,
            sort_keys=False,
            width=120,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich ontology with VM budget vocabulary aliases.")
    parser.add_argument("--ontology-path", type=Path, default=DEFAULT_ONTOLOGY_PATH)
    parser.add_argument("--vocab-path", type=Path, default=DEFAULT_VOCAB_PATH)
    parser.add_argument("--report-path", type=Path, default=DEFAULT_REPORT_PATH)
    parser.add_argument("--dry-run", action="store_true", help="Report only, don't modify ontology")
    parser.add_argument("--max-aliases-per-concept", type=int, default=20, help="Max new aliases per concept")
    args = parser.parse_args()

    # Load data
    vocab = _read_jsonl(args.vocab_path)
    ontology_raw = load_ontology_raw(args.ontology_path)
    concepts = ontology_raw.get("concepts", [])

    print(f"Loaded {len(vocab)} vocabulary entries")
    print(f"Loaded {len(concepts)} ontology concepts")

    # Enrich each concept
    enrichment_log: list[dict] = []
    total_added = 0

    for concept in concepts:
        concept_id = concept.get("concept_id", "")
        new_aliases = enrich_concept(concept, vocab)

        # Limit aliases per concept
        new_aliases = new_aliases[:args.max_aliases_per_concept]

        if new_aliases:
            enrichment_log.append({
                "concept_id": concept_id,
                "label_fi": concept.get("label_fi", ""),
                "existing_aliases": len(concept.get("aliases", [])),
                "new_aliases": new_aliases,
                "matched_vocab_count": len(find_matching_vocab(concept, vocab)),
            })

            if not args.dry_run:
                if "aliases" not in concept:
                    concept["aliases"] = []
                concept["aliases"].extend(new_aliases)

            total_added += len(new_aliases)

    print(f"\nEnrichment results:")
    print(f"  Concepts enriched: {len(enrichment_log)}/{len(concepts)}")
    print(f"  New aliases added: {total_added}")

    for entry in enrichment_log:
        print(f"  {entry['concept_id']}: +{len(entry['new_aliases'])} aliases (from {entry['matched_vocab_count']} vocab matches)")

    # Save updated ontology
    if not args.dry_run and total_added > 0:
        save_ontology_raw(args.ontology_path, ontology_raw)
        print(f"\nOntology updated: {args.ontology_path}")

    # Write report
    _write_report(args.report_path, enrichment_log, total_added, len(concepts))
    print(f"Report: {args.report_path}")


def _write_report(path: Path, log: list[dict], total_added: int, total_concepts: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write("# Ontology Enrichment from VM Budget Vocabulary\n\n")
        f.write(f"- Generated: {datetime.now(timezone.utc).isoformat()}\n")
        f.write(f"- Concepts enriched: {len(log)}/{total_concepts}\n")
        f.write(f"- Total new aliases: {total_added}\n\n")

        for entry in log:
            f.write(f"## {entry['concept_id']} ({entry['label_fi']})\n")
            f.write(f"- Existing aliases: {entry['existing_aliases']}\n")
            f.write(f"- Matching vocab items: {entry['matched_vocab_count']}\n")
            f.write(f"- New aliases added:\n")
            for alias in entry["new_aliases"]:
                f.write(f"  - `{alias['alias']}` ({alias['alias_type']})\n")
            f.write("\n")


if __name__ == "__main__":
    main()

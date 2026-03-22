#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.ontology_utils import default_ontology_path, load_budget_ontology  # noqa: E402


DEFAULT_OUTPUT_PATH = Path("data/ontology/ontology_alias_quality_v1.jsonl")
DEFAULT_REPORT_PATH = Path("docs/reports/ontology_alias_quality_report.md")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render ontology alias quality model and report.")
    parser.add_argument("--ontology-path", type=Path, default=default_ontology_path())
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--report-path", type=Path, default=DEFAULT_REPORT_PATH)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    ontology = load_budget_ontology(args.ontology_path)

    rows: list[dict] = []
    review_counter: Counter[str] = Counter()
    type_counter: Counter[str] = Counter()
    source_counter: Counter[str] = Counter()

    for concept in ontology.concepts:
        for alias in concept.aliases:
            row = {
                "concept_id": concept.concept_id,
                "concept_label_fi": concept.label_fi,
                "alias": alias.alias,
                "alias_type": alias.alias_type,
                "source": alias.source,
                "precision_score": alias.precision_score,
                "requires_token_boundary": alias.requires_token_boundary,
                "is_acronym": alias.is_acronym,
                "review_status": alias.review_status,
                "valid_from_year": alias.valid_from_year,
                "valid_to_year": alias.valid_to_year,
            }
            rows.append(row)
            review_counter[alias.review_status] += 1
            type_counter[alias.alias_type] += 1
            source_counter[alias.source] += 1

    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    with args.output_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    args.report_path.parent.mkdir(parents=True, exist_ok=True)
    report_lines = [
        "# Ontology Alias Quality Report",
        "",
        f"- concepts: {len(ontology.concepts)}",
        f"- aliases: {len(rows)}",
        "",
        "## Review statuses",
    ]
    report_lines.extend(f"- `{status}`: {count}" for status, count in sorted(review_counter.items()))
    report_lines.append("")
    report_lines.append("## Alias types")
    report_lines.extend(f"- `{alias_type}`: {count}" for alias_type, count in sorted(type_counter.items()))
    report_lines.append("")
    report_lines.append("## Sources")
    report_lines.extend(f"- `{source}`: {count}" for source, count in sorted(source_counter.items()))
    report_lines.append("")
    report_lines.append("## High-risk short aliases")
    short_aliases = [
        row for row in rows
        if len(row["alias"]) < 4 or row["is_acronym"]
    ]
    for row in sorted(short_aliases[:50], key=lambda item: (item["concept_id"], item["alias"])):
        report_lines.append(
            f"- `{row['concept_id']}` -> `{row['alias']}` "
            f"(type={row['alias_type']}, review={row['review_status']}, boundary={row['requires_token_boundary']})"
        )
    args.report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    print(f"Wrote {len(rows)} alias quality rows -> {args.output_path}")
    print(f"Wrote report -> {args.report_path}")


if __name__ == "__main__":
    main()

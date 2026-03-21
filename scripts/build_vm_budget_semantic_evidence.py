#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

CONTENT_KIND_RULES = [
    ("yksityiskohtaiset perustelut", "detailed_justification"),
    ("yleisperustelut", "general_justification"),
    ("yhteenvetotaulukot", "summary_table"),
    ("numerotaulu", "numeric_table"),
    ("tuloarviot", "revenue_section"),
    ("määrärahat", "allowance_section"),
    ("esipuhe", "preface"),
]

DEFAULT_SEGMENTS_PATH = Path("data/semantic_enrichment/vm_budget_site/segments_2002_2005.jsonl")
DEFAULT_OUTPUT_DIR = Path("data/semantic_enrichment/vm_budget_site")
DEFAULT_REPORT_PATH = Path("docs/reports/vm_budget_semantic_evidence_pilot.md")


def _read_jsonl(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def _write_jsonl(path: Path, rows: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _classify_content_kind(*labels: str) -> str:
    haystack = " ".join(value.lower() for value in labels if value)
    for needle, kind in CONTENT_KIND_RULES:
        if needle in haystack:
            return kind
    return "other"


def _snippet(text: str, length: int = 450) -> str:
    compact = " ".join((text or "").split())
    return compact[:length]


def _confidence(level: str, content_kind: str) -> float:
    base = {
        "momentti": 0.95,
        "luku": 0.85,
        "osasto": 0.75,
    }.get(level, 0.6)
    if content_kind in {"detailed_justification", "numeric_table", "allowance_section", "revenue_section"}:
        return base
    if content_kind in {"summary_table", "general_justification"}:
        return max(base - 0.1, 0.5)
    return max(base - 0.2, 0.4)


def build_evidence_rows(segments: list[dict]) -> list[dict]:
    rows: list[dict] = []
    for segment in segments:
        content_kind = _classify_content_kind(segment.get("heading", ""), segment.get("node_label", ""), segment.get("document_label", ""))
        base = {
            "year": segment["year"],
            "stage_key": segment["stage_key"],
            "stage_label": segment["stage_label"],
            "document_slug": segment["document_slug"],
            "document_label": segment["document_label"],
            "node_label": segment["node_label"],
            "heading": segment["heading"],
            "content_kind": content_kind,
            "text_length": segment["text_length"],
            "content_url": segment["content_url"],
            "content_hash": segment["content_hash"],
            "snippet": _snippet(segment.get("text", "")),
        }
        for level, key in (("momentti", "momentti_codes"), ("luku", "luku_codes"), ("osasto", "osasto_codes")):
            for code in segment.get(key, []):
                rows.append(
                    {
                        **base,
                        "hierarchy_level": level,
                        "hierarchy_code": code,
                        "evidence_confidence": _confidence(level, content_kind),
                    }
                )
    return rows


def _render_report(path: Path, evidence_rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    by_level = Counter(row["hierarchy_level"] for row in evidence_rows)
    by_kind = Counter(row["content_kind"] for row in evidence_rows)
    by_stage = Counter(row["stage_label"] for row in evidence_rows)
    with path.open("w", encoding="utf-8") as handle:
        handle.write("# VM Budjettisivuston semantic evidence -pilotti\n\n")
        handle.write(f"- Aikaleima: {datetime.now(timezone.utc).isoformat()}\n")
        handle.write(f"- Evidenssirivejä: {len(evidence_rows)}\n\n")
        handle.write("## Evidenssi hierarkiatasoittain\n")
        for key, count in by_level.items():
            handle.write(f"- {key}: {count}\n")
        handle.write("\n## Evidenssi sisältötyypeittäin\n")
        for key, count in by_kind.most_common():
            handle.write(f"- {key}: {count}\n")
        handle.write("\n## Evidenssi budjettivaiheittain\n")
        for key, count in by_stage.most_common():
            handle.write(f"- {key}: {count}\n")
        handle.write("\n## Esimerkkirivit\n")
        for row in evidence_rows[:12]:
            handle.write(f"- {row['year']} / {row['hierarchy_level']} / {row['hierarchy_code']} / {row['content_kind']}\n")
            handle.write(f"  - heading: {row['heading']}\n")
            handle.write(f"  - source: {row['content_url']}\n")
            handle.write(f"  - snippet: {row['snippet']}\n")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build semantic evidence rows from VM budget-site pilot segments.")
    parser.add_argument("--segments-path", type=Path, default=DEFAULT_SEGMENTS_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--report-path", type=Path, default=DEFAULT_REPORT_PATH)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    segments = _read_jsonl(args.segments_path)
    evidence_rows = build_evidence_rows(segments)
    label = args.segments_path.stem.replace("segments_", "")
    output_path = args.output_dir / f"semantic_evidence_{label}.jsonl"
    _write_jsonl(output_path, evidence_rows)
    _render_report(args.report_path, evidence_rows)
    print(f"Evidence rows: {len(evidence_rows)} -> {output_path}")
    print(f"Report: {args.report_path}")


if __name__ == "__main__":
    main()

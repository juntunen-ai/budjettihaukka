#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings
from utils.ontology_utils import (
    BudgetOntology,
    default_ontology_path,
    flatten_budget_ontology,
    load_budget_ontology,
    resolve_concepts_for_question,
    validate_budget_ontology,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate and load Budjettihaukka ontology.")
    parser.add_argument("--ontology-path", type=Path, default=default_ontology_path())
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--table-prefix", default=settings.ontology_table_prefix)
    parser.add_argument("--render-json-dir", type=Path, default=None)
    parser.add_argument("--load-bigquery", action="store_true")
    parser.add_argument(
        "--loader",
        choices=("client", "bq"),
        default="bq",
        help="Use BigQuery Python client or bq CLI for loading.",
    )
    parser.add_argument("--match-question", default="", help="Resolve ontology concepts for a sample question.")
    return parser.parse_args()


def _table_name(base_name: str, prefix: str) -> str:
    normalized = prefix.strip("_")
    if not normalized:
        return base_name
    if base_name.startswith(f"{normalized}_"):
        return base_name
    return f"{normalized}_{base_name.removeprefix('ontology_')}" if base_name.startswith("ontology_") else f"{normalized}_{base_name}"


def _print_summary(ontology: BudgetOntology, flattened: dict[str, list[dict[str, Any]]]) -> None:
    print(f"Ontology: {ontology.label_fi} v{ontology.version}")
    print(f"Concepts: {len(ontology.concepts)}")
    print(f"Aliases: {len(flattened['ontology_alias'])}")
    print(f"Rules: {len(flattened['ontology_membership_rule'])}")
    print(f"Viz recipes: {len(flattened['ontology_viz_recipe'])}")
    print(f"Guardrails: {len(flattened['ontology_guardrail'])}")
    print(f"External refs: {len(flattened['ontology_external_ref'])}")


def _write_jsonl_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _render_json_bundle(
    flattened: dict[str, list[dict[str, Any]]],
    out_dir: Path,
    table_prefix: str,
) -> list[tuple[str, str, Path]]:
    out_dir.mkdir(parents=True, exist_ok=True)
    bundle: list[tuple[str, str, Path]] = []
    for base_name, rows in flattened.items():
        table_name = _table_name(base_name, table_prefix)
        out_path = out_dir / f"{table_name}.jsonl"
        _write_jsonl_rows(out_path, rows)
        bundle.append((base_name, table_name, out_path))
    return bundle


def _load_with_client(
    project: str,
    dataset: str,
    table_name: str,
    rows: list[dict[str, Any]],
) -> None:
    client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", encoding="utf-8", delete=False) as handle:
        temp_path = Path(handle.name)
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    try:
        with temp_path.open("rb") as source:
            client.load_table_from_file(source, table_id, job_config=job_config).result()
    finally:
        temp_path.unlink(missing_ok=True)


def _load_with_bq(
    project: str,
    dataset: str,
    table_name: str,
    jsonl_path: Path,
) -> None:
    table_id = f"{project}:{dataset}.{table_name}"
    cmd = [
        "bq",
        "load",
        "--replace",
        "--project_id",
        project,
        "--source_format=NEWLINE_DELIMITED_JSON",
        "--autodetect",
        table_id,
        str(jsonl_path),
    ]
    subprocess.run(cmd, check=True)


def _load_bigquery_bundle(
    project: str,
    dataset: str,
    table_prefix: str,
    flattened: dict[str, list[dict[str, Any]]],
    loader: str,
    render_json_dir: Path | None,
) -> None:
    bundle_dir = render_json_dir
    if loader == "bq" and bundle_dir is None:
        bundle_dir = Path(tempfile.mkdtemp(prefix="budget_ontology_jsonl_"))
    rendered = _render_json_bundle(flattened, bundle_dir or Path(tempfile.mkdtemp()), table_prefix)

    for base_name, table_name, jsonl_path in rendered:
        print(f"Loading {table_name} -> {project}.{dataset}.{table_name}")
        if loader == "client":
            _load_with_client(project, dataset, table_name, flattened[base_name])
        else:
            _load_with_bq(project, dataset, table_name, jsonl_path)


def main() -> None:
    args = _parse_args()
    ontology = load_budget_ontology(args.ontology_path)
    issues = validate_budget_ontology(ontology)
    if issues:
        print("Ontology validation FAILED:")
        for issue in issues:
            print(f"- {issue}")
        raise SystemExit(2)

    flattened = flatten_budget_ontology(ontology)
    _print_summary(ontology, flattened)

    if args.render_json_dir:
        bundle = _render_json_bundle(flattened, args.render_json_dir, args.table_prefix)
        print(f"Rendered JSON bundle to {args.render_json_dir}")
        for _base_name, table_name, path in bundle:
            print(f"- {table_name}: {path}")

    if args.match_question:
        matches = resolve_concepts_for_question(args.match_question, ontology, limit=5)
        print("Question matches:")
        for match in matches:
            print(
                f"- {match.concept_id} ({match.label_fi}) "
                f"score={match.score:.2f} risk={match.risk_level} aliases={list(match.matched_aliases)}"
            )

    if args.load_bigquery:
        _load_bigquery_bundle(
            project=args.project,
            dataset=args.dataset,
            table_prefix=args.table_prefix,
            flattened=flattened,
            loader=args.loader,
            render_json_dir=args.render_json_dir,
        )


if __name__ == "__main__":
    main()

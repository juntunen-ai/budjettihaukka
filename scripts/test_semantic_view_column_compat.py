#!/usr/bin/env python3
"""Offline check: every column the app's generated SQL references must exist
in the semantic layer (semantic view or yearly agg table).

This is the guard for the raw->semantic table switch: SQL contracts and
fallback builders were written against raw `budjettidata` column names, and
the semantic view exposes raw-compatible aliases. A rename on either side
should fail here, not in production BigQuery.

Runs without BigQuery credentials: view/agg schemas are taken from the SQL
rendered by build_bq_data_quality_layer.py, and app SQL is generated from the
robustness golden questions.
"""

from __future__ import annotations

import json
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import sqlglot
from sqlglot import exp

from build_bq_data_quality_layer import build_semantic_view_sql, build_yearly_agg_sql
from utils.analysis_spec_utils import infer_analysis_spec
from utils.bigquery_utils import (
    ONTOLOGY_RULE_LEVEL_MAP,
    ONTOLOGY_RULE_LEVEL_MAP_YEARLY_AGG,
    YEARLY_AGG_TABLE_ID,
    _build_bigquery_fallback_sql,
)
from utils.semantic_query_contracts import build_contract_sql

PROJECT = "p"
DATASET = "d"
SEMANTIC_VIEW = "valtiontalous_semantic_vX"
YEARLY_AGG = "valtiontalous_yearly_agg_vX"
MAIN_TABLE_ID = f"{PROJECT}.{DATASET}.{SEMANTIC_VIEW}"

GOLDENS_PATH = ROOT / "data" / "evals" / "robustness_goldens.json"


def _norm(name: str) -> str:
    # BigQuery column references are case-insensitive.
    return unicodedata.normalize("NFC", name).lower()


def _view_output_columns(create_sql: str) -> set[str]:
    parsed = sqlglot.parse_one(create_sql, read="bigquery")
    select = parsed.find(exp.Select)
    columns: set[str] = set()
    for projection in select.expressions:
        alias = projection.alias_or_name
        if alias and alias != "*":
            columns.add(_norm(alias))
    return columns


def _referenced_columns(sql: str) -> set[str]:
    """Column names referenced in SQL, excluding names derived inside the query."""
    parsed = sqlglot.parse_one(sql, read="bigquery")
    derived: set[str] = set()
    for alias_node in parsed.find_all(exp.Alias):
        derived.add(_norm(alias_node.alias))
    for cte in parsed.find_all(exp.CTE):
        derived.add(_norm(cte.alias))
    referenced = {
        _norm(column.name)
        for column in parsed.find_all(exp.Column)
    }
    return referenced - derived


def _load_golden_questions() -> list[str]:
    raw = json.loads(GOLDENS_PATH.read_text(encoding="utf-8"))
    cases = raw.get("cases", raw) if isinstance(raw, dict) else raw
    return [str(case.get("question", "")).strip() for case in cases if case.get("question")]


def main() -> int:
    semantic_columns = _view_output_columns(
        build_semantic_view_sql(PROJECT, DATASET, "curated", SEMANTIC_VIEW)
    )
    yearly_columns = _view_output_columns(
        build_yearly_agg_sql(PROJECT, DATASET, SEMANTIC_VIEW, YEARLY_AGG)
    )
    yearly_table_name = _norm(YEARLY_AGG_TABLE_ID.rsplit(".", 1)[-1])

    failures: list[str] = []
    checked = 0

    def check_sql(sql: str, origin: str) -> None:
        nonlocal checked
        if not sql:
            return
        checked += 1
        try:
            tables = {
                _norm(table.name)
                for table in sqlglot.parse_one(sql, read="bigquery").find_all(exp.Table)
            }
        except sqlglot.errors.ParseError as error:
            failures.append(f"{origin}: SQL does not parse: {error}")
            return
        allowed = set(semantic_columns)
        if yearly_table_name in tables or _norm(YEARLY_AGG) in tables:
            allowed |= yearly_columns
        unknown = _referenced_columns(sql) - allowed
        if unknown:
            failures.append(f"{origin}: unknown columns {sorted(unknown)}")

    for question in _load_golden_questions():
        spec = infer_analysis_spec(question)
        contract_sql, contract_name = build_contract_sql(spec, MAIN_TABLE_ID)
        if contract_sql:
            check_sql(contract_sql, f"contract={contract_name} q={question[:50]!r}")
        check_sql(
            _build_bigquery_fallback_sql(question, spec),
            f"fallback q={question[:50]!r}",
        )

    for level, exprs in ONTOLOGY_RULE_LEVEL_MAP.items():
        for expr_kind, expr in exprs.items():
            check_sql(
                f"SELECT {expr} AS x FROM `{MAIN_TABLE_ID}`",
                f"ontology_rule_map[{level}][{expr_kind}]",
            )
    for level, exprs in ONTOLOGY_RULE_LEVEL_MAP_YEARLY_AGG.items():
        for expr_kind, expr in exprs.items():
            check_sql(
                f"SELECT {expr} AS x FROM `{YEARLY_AGG_TABLE_ID}`",
                f"ontology_rule_map_yearly[{level}][{expr_kind}]",
            )

    print(f"Semantic view columns: {len(semantic_columns)}")
    print(f"Yearly agg columns: {len(yearly_columns)}")
    print(f"SQL statements checked: {checked}")
    if failures:
        print(f"FAILED: {len(failures)} statements reference unknown columns:")
        seen: set[str] = set()
        for failure in failures:
            key = failure.split(" q=")[0]
            if key in seen:
                continue
            seen.add(key)
            print(f"- {failure}")
        return 1
    print("OK: all generated SQL is column-compatible with the semantic layer.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

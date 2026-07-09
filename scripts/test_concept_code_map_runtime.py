#!/usr/bin/env python3
"""Offline tests: curated concept→code map wiring (no BigQuery needed)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import sqlglot

from utils import concept_code_map_utils as ccm
from utils.analysis_spec_utils import infer_analysis_spec
from utils.bigquery_utils import _ontology_scope_clause

FAILURES: list[str] = []


def check(name: str, condition: bool) -> None:
    print(f"[{'OK' if condition else 'FAIL'}] {name}")
    if not condition:
        FAILURES.append(name)


def main() -> int:
    check("koulutus has curated map", ccm.has_curated_map("koulutus"))
    check("unknown concept has no map", not ccm.has_curated_map("olematon_konsepti"))

    spec = infer_analysis_spec("Miten koulutusmenot ovat kehittyneet 2000-2024?")
    check("question resolves to koulutus", spec.resolved_concept_id == "koulutus")

    for dialect in ("bigquery", "yearly_agg"):
        clause = ccm.curated_scope_clause("koulutus", dialect=dialect)
        check(f"{dialect}: clause exists", bool(clause))
        check(f"{dialect}: includes core teaching luku 29.10", "'29.10.'" in clause)
        check(f"{dialect}: excludes Akatemia momentti (Q2:A)", "'29.40.51.'" in clause and "NOT" in clause)
        check(f"{dialect}: excludes culture luku 29.80", "'29.80.'" in clause)
        check(f"{dialect}: no name matching (no LIKE)", "LIKE" not in clause.upper().replace("STARTS_WITH", ""))
        wrapped = f"SELECT 1 FROM t WHERE {clause}"
        try:
            sqlglot.parse_one(wrapped, read="bigquery")
            check(f"{dialect}: clause parses as SQL", True)
        except Exception:
            check(f"{dialect}: clause parses as SQL", False)

    scope = _ontology_scope_clause(spec, "yearly_agg")
    check("scope path prefers curated map", scope is not None and "'29.10.'" in scope)

    meta = ccm.definition_meta("koulutus")
    check("definition meta has label + version", meta["label"] == "Koulutus" and meta["version"] == 1)
    check("Q1:C — opintotuki listed as component", meta["components"] == ["opintotuki"])
    check("Q3:C — disclosure mentions 28.90.30", "28.90.30" in (meta["disclosure_fi"] or ""))
    check("decided_by recorded", bool(meta["decided_by"]))

    # Era correctness: old-university-era 29.10 rows must still be included
    # (both eras are education), and the map must not leak into other concepts.
    check("no map for terveys yet", ccm.definition_meta("terveys") is None)

    if FAILURES:
        print(f"\nFAILED: {len(FAILURES)} checks")
        return 1
    print("\nAll concept code map runtime checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

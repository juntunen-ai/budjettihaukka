#!/usr/bin/env python3
"""Load human-decided concept→budget-code maps into BigQuery.

Reads data/ontology/concept_code_map/*.yaml (one file per concept, decided in
review dossiers), validates them, loads rows to `concept_code_map_v1`, and
creates `concept_yearly_totals_v1` — per (concept, year, role, component)
sums with momentti-level rules overriding luku-level rules.

Sandbox-safe: uses load jobs and CREATE OR REPLACE VIEW only.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings

MAP_DIR = ROOT / "data" / "ontology" / "concept_code_map"
VALID_ROLES = {"include", "component", "exclude"}
VALID_LEVELS = {"luku", "momentti"}


def load_and_validate() -> list[dict]:
    rows: list[dict] = []
    for path in sorted(MAP_DIR.glob("*.yaml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        concept = doc["concept"]
        for i, rule in enumerate(doc.get("rules", [])):
            if rule.get("level") not in VALID_LEVELS:
                raise ValueError(f"{path.name} rule {i}: bad level {rule.get('level')!r}")
            if rule.get("role") not in VALID_ROLES:
                raise ValueError(f"{path.name} rule {i}: bad role {rule.get('role')!r}")
            if rule["role"] == "component" and not rule.get("component"):
                raise ValueError(f"{path.name} rule {i}: component role needs component name")
            if not str(rule.get("code", "")).endswith("."):
                raise ValueError(f"{path.name} rule {i}: code should end with '.' ({rule.get('code')!r})")
            rows.append(
                {
                    "concept": concept,
                    "map_version": doc.get("version", 1),
                    "decided_by": doc.get("decided_by"),
                    "decided_on": str(doc.get("decided_on", "")),
                    "level": rule["level"],
                    "code": rule["code"],
                    "year_from": rule.get("year_from"),
                    "year_to": rule.get("year_to"),
                    "role": rule["role"],
                    "component": rule.get("component"),
                    "target_concept": rule.get("target_concept"),
                    "note": rule.get("note"),
                }
            )
    if not rows:
        raise ValueError(f"No concept map files found under {MAP_DIR}")
    return rows


def totals_view_sql(project: str, dataset: str, source_view: str) -> str:
    return f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.concept_yearly_totals_v1` AS
WITH source_rows AS (
  SELECT
    vuosi,
    momentti_tunnusp,
    SUBSTR(momentti_tunnusp, 1, INSTR(momentti_tunnusp, '.', 1, 2)) AS luku_code,
    nettokertyma
  FROM `{project}.{dataset}.{source_view}`
  WHERE momentti_tunnusp IS NOT NULL AND nettokertyma IS NOT NULL
),
rules AS (
  SELECT * FROM `{project}.{dataset}.concept_code_map_v1`
),
matched AS (
  SELECT
    COALESCE(m.concept, l.concept) AS concept,
    r.vuosi,
    COALESCE(m.role, l.role) AS role,
    COALESCE(m.component, l.component) AS component,
    COALESCE(m.target_concept, l.target_concept) AS target_concept,
    r.nettokertyma
  FROM source_rows r
  LEFT JOIN rules m
    ON m.level = 'momentti' AND m.code = r.momentti_tunnusp
   AND r.vuosi BETWEEN COALESCE(m.year_from, 0) AND COALESCE(m.year_to, 9999)
  LEFT JOIN rules l
    ON l.level = 'luku' AND l.code = r.luku_code
   AND r.vuosi BETWEEN COALESCE(l.year_from, 0) AND COALESCE(l.year_to, 9999)
   AND (m.concept IS NULL OR l.concept = m.concept)
  WHERE m.concept IS NOT NULL OR l.concept IS NOT NULL
)
SELECT
  concept,
  vuosi,
  role,
  component,
  target_concept,
  ROUND(SUM(nettokertyma) / 1e6, 1) AS total_meur,
  COUNT(*) AS row_count
FROM matched
GROUP BY concept, vuosi, role, component, target_concept
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Load concept code maps to BigQuery.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--source-view", default="valtiontalous_curated_dq_v")
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()

    rows = load_and_validate()
    concepts = sorted({r["concept"] for r in rows})
    print(f"Validated {len(rows)} rules for concepts: {', '.join(concepts)}")
    if args.validate_only:
        return 0

    client = bigquery.Client(project=args.project)
    table_id = f"{args.project}.{args.dataset}.concept_code_map_v1"
    job = client.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("concept", "STRING"),
                bigquery.SchemaField("map_version", "INT64"),
                bigquery.SchemaField("decided_by", "STRING"),
                bigquery.SchemaField("decided_on", "STRING"),
                bigquery.SchemaField("level", "STRING"),
                bigquery.SchemaField("code", "STRING"),
                bigquery.SchemaField("year_from", "INT64"),
                bigquery.SchemaField("year_to", "INT64"),
                bigquery.SchemaField("role", "STRING"),
                bigquery.SchemaField("component", "STRING"),
                bigquery.SchemaField("target_concept", "STRING"),
                bigquery.SchemaField("note", "STRING"),
            ],
        ),
    )
    job.result()
    print(f"Loaded {len(rows)} rules -> {table_id}")

    client.query(totals_view_sql(args.project, args.dataset, args.source_view)).result()
    print(f"View ready: {args.project}.{args.dataset}.concept_yearly_totals_v1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

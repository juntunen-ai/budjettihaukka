#!/usr/bin/env python3
"""Run low-cost acceptance checks against the materialized visualization mart."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings


def build_check_sql(project: str, dataset: str) -> str:
    prefix = f"`{project}.{dataset}"
    return f"""
WITH facts AS (
  SELECT * FROM {prefix}.analytics_fiscal_yearly_core_v1`
),
macro AS (
  SELECT * FROM {prefix}.analytics_macro_yearly_v1`
),
metric_definitions AS (
  SELECT * FROM {prefix}.dim_visualization_metric_v1`
),
source_definitions AS (
  SELECT source_id FROM {prefix}.dim_data_source_v1`
),
budget AS (
  SELECT * FROM {prefix}.analytics_budget_vs_actual_v2`
),
macro_keys AS (
  SELECT series_id, year, COUNT(*) AS row_count
  FROM {prefix}.official_macro_reference_v1`
  GROUP BY series_id, year
),
latest_fact_year AS (
  SELECT MAX(year) AS year FROM facts
),
latest_macro_year AS (
  SELECT MAX(year) AS year
  FROM macro
  WHERE population_midyear_persons IS NOT NULL
    AND gdp_current_prices_meur IS NOT NULL
    AND cost_of_living_index IS NOT NULL
)
SELECT
  COUNT(*) AS fact_rows,
  COUNTIF(alamomentti_tunnus IS NOT NULL AND NOT alamomentti_is_validated) AS unvalidated_alamomentti_rows,
  COUNTIF(
    alamomentti_tunnus IS NOT NULL
    AND alamomentti_tunnus = maararahalaji_tunnus
    AND COALESCE(alamomentti_snimi, '') = COALESCE(maararahalaji_snimi, '')
  ) AS appropriation_type_published_as_alamomentti_rows,
  COUNTIF(is_reconciled_to_audited_final_accounts) AS falsely_reconciled_rows,
  COUNTIF(coverage_status = 'complete' AND observed_months != 12) AS invalid_complete_year_rows,
  COUNTIF(
    facts.year = latest_fact_year.year
    AND observed_months < 12
    AND coverage_status != 'partial_current_year'
  ) AS unflagged_latest_partial_rows,
  (SELECT COUNTIF(row_count != 1) FROM macro_keys) AS duplicate_macro_series_years,
  (
    SELECT COUNT(*)
    FROM macro, latest_macro_year
    WHERE macro.year BETWEEN 1998 AND latest_macro_year.year
      AND (
        population_midyear_persons IS NULL
        OR gdp_current_prices_meur IS NULL
        OR cost_of_living_index IS NULL
      )
  ) AS missing_core_macro_years,
  (
    SELECT COUNT(*)
    FROM metric_definitions
    WHERE metric_id IS NULL
      OR display_name_fi IS NULL
      OR definition_fi IS NULL
      OR unit IS NULL
      OR aggregation_rule IS NULL
      OR price_basis IS NULL
      OR sign_rule IS NULL
      OR missing_means IS NULL
  ) AS incomplete_metric_definitions,
  (
    SELECT COUNT(*)
    FROM metric_definitions
    LEFT JOIN source_definitions USING (source_id)
    WHERE source_definitions.source_id IS NULL
  ) AS metrics_with_unknown_source,
  (
    SELECT COUNT(*)
    FROM budget
    WHERE NOT is_complete_year AND visualization_quality_status = 'ready'
  ) AS partial_budget_rows_marked_ready
FROM facts
CROSS JOIN latest_fact_year
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate visualization mart data quality.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--json-output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = subprocess.run(
        [
            "bq",
            f"--project_id={args.project}",
            "query",
            "--nouse_legacy_sql",
            "--format=prettyjson",
            "--max_rows=1",
        ],
        input=build_check_sql(args.project, args.dataset),
        text=True,
        capture_output=True,
    )
    if result.returncode:
        print(result.stderr or result.stdout, file=sys.stderr)
        return 2
    rows = json.loads(result.stdout)
    if len(rows) != 1:
        print("Expected one quality summary row", file=sys.stderr)
        return 2
    summary = {key: int(value) for key, value in rows[0].items()}
    failures = {
        key: value
        for key, value in summary.items()
        if key != "fact_rows" and value != 0
    }
    report = {
        "status": "PASS" if summary["fact_rows"] > 0 and not failures else "FAIL",
        "summary": summary,
        "failures": failures,
    }
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    print(rendered)
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(rendered + "\n", encoding="utf-8")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

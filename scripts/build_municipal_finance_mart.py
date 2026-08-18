#!/usr/bin/env python3
"""Build semantic BigQuery views for municipal-finance reference snapshots."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings


def build_sql_map(project: str, dataset: str) -> dict[str, str]:
    prefix = f"{project}.{dataset}"
    return {
        "dim_municipal_finance_source_v1": f"""
CREATE OR REPLACE VIEW `{prefix}.dim_municipal_finance_source_v1` AS
SELECT
  reporting_package,
  taxonomy_url,
  SAFE_CAST(content_length_bytes AS INT64) AS content_length_bytes,
  last_modified,
  etag,
  snapshot_policy,
  source_id,
  source_url,
  vintage_date,
  snapshot_policy = 'snapshotted' AS is_semantically_available,
  snapshot_policy = 'too_large_for_default_snapshot' AS requires_large_file_pipeline
FROM `{prefix}.municipal_finance_taxonomy_index_v1`
""",
        "dim_municipal_finance_indicator_v1": f"""
CREATE OR REPLACE VIEW `{prefix}.dim_municipal_finance_indicator_v1` AS
SELECT
  indicator_code,
  NULLIF(semantic_metric_id, '') AS semantic_metric_id,
  indicator_name_fi,
  indicator_name_sv,
  indicator_name_en,
  task_name_fi,
  NULLIF(planning_stage, '') AS planning_stage,
  SAFE_CAST(value_year_offset AS INT64) AS value_year_offset,
  SAFE_CAST(level AS INT64) AS hierarchy_level,
  service_class,
  sector,
  entrypoint,
  taxonomy_version,
  technical_taxonomy_version,
  subpackage,
  mandatory,
  protected,
  SAFE_CAST(is_core_metric AS BOOL) AS is_core_metric,
  NULLIF(unit, '') AS unit,
  source_url
FROM `{prefix}.municipal_finance_ktas_taxonomy_v1`
""",
        "analytics_municipal_finance_catalog_v1": f"""
CREATE OR REPLACE VIEW `{prefix}.analytics_municipal_finance_catalog_v1` AS
SELECT
  *,
  reporting_package = 'KKNR' AND reporting_period = '2022C03' AS known_source_anomaly,
  CASE
    WHEN reporting_package = 'KKNR' AND reporting_period = '2022C03'
      THEN 'quarantine_interest_expense_hierarchy_6200_6299'
    WHEN accounting_stage = 'unclassified' THEN 'semantic_mapping_required'
    ELSE 'catalog_ready'
  END AS semantic_availability,
  'Public reporter identifier; municipality versus joint authority is not inferred without an official organization map' AS reporter_type_caveat
FROM `{prefix}.municipal_finance_catalog_v1`
""",
        "analytics_municipal_budget_v1": f"""
CREATE OR REPLACE VIEW `{prefix}.analytics_municipal_budget_v1` AS
WITH typed AS (
  SELECT
    business_id,
    reporting_package,
    SAFE_CAST(reporting_year AS INT64) AS reporting_year,
    SAFE_CAST(value_year AS INT64) AS value_year,
    planning_stage,
    accounting_stage,
    SAFE_CAST(is_budget AS BOOL) AS is_budget,
    SAFE_CAST(is_forecast AS BOOL) AS is_forecast,
    SAFE_CAST(is_actual AS BOOL) AS is_actual,
    approval_stage,
    approved_at,
    published_at,
    indicator_code,
    semantic_metric_id,
    indicator_name_fi,
    SAFE_CAST(value_numeric AS FLOAT64) AS value_eur,
    unit,
    SAFE_CAST(validation_finding_count AS INT64) AS validation_finding_count,
    SAFE_CAST(validation_max_severity AS INT64) AS validation_max_severity,
    validation_findings,
    source_url,
    vintage_date
  FROM `{prefix}.municipal_finance_ktas_core_v1`
), compared AS (
  SELECT
    *,
    LAG(value_eur) OVER (
      PARTITION BY business_id, semantic_metric_id, planning_stage
      ORDER BY reporting_year
    ) AS previous_report_value_eur,
    LAG(reporting_year) OVER (
      PARTITION BY business_id, semantic_metric_id, planning_stage
      ORDER BY reporting_year
    ) AS previous_reporting_year
  FROM typed
)
SELECT
  *,
  value_eur - previous_report_value_eur AS change_from_previous_report_eur,
  SAFE_DIVIDE(value_eur - previous_report_value_eur, ABS(previous_report_value_eur)) AS change_from_previous_report_ratio,
  CASE
    WHEN is_actual THEN 'invalid_ktas_actual_flag'
    WHEN planning_stage = 'current_budget' THEN 'approved_budget_not_actual'
    WHEN planning_stage = 'prior_year_amended_budget' THEN 'amended_budget_not_actual'
    WHEN is_forecast THEN 'forward_plan_not_actual'
    ELSE 'review_semantics'
  END AS interpretation_status,
  FALSE AS causal_effect_identified
FROM compared
""",
        "analytics_municipal_budget_revision_v1": f"""
CREATE OR REPLACE VIEW `{prefix}.analytics_municipal_budget_revision_v1` AS
WITH current_budget AS (
  SELECT * FROM `{prefix}.analytics_municipal_budget_v1`
  WHERE planning_stage = 'current_budget'
), amended_budget AS (
  SELECT * FROM `{prefix}.analytics_municipal_budget_v1`
  WHERE planning_stage = 'prior_year_amended_budget'
)
SELECT
  current_budget.business_id,
  current_budget.value_year,
  current_budget.semantic_metric_id,
  current_budget.indicator_name_fi,
  current_budget.value_eur AS original_budget_eur,
  amended_budget.value_eur AS amended_budget_eur,
  amended_budget.value_eur - current_budget.value_eur AS revision_eur,
  SAFE_DIVIDE(amended_budget.value_eur - current_budget.value_eur, ABS(current_budget.value_eur)) AS revision_ratio,
  current_budget.reporting_year AS original_budget_report_year,
  amended_budget.reporting_year AS amended_budget_report_year,
  current_budget.source_url AS original_budget_source_url,
  amended_budget.source_url AS amended_budget_source_url,
  amended_budget.value_eur IS NOT NULL AS has_follow_up_amended_budget,
  'Budget revision; not the difference between budget and actual expenditure' AS interpretation_caveat
FROM current_budget
LEFT JOIN amended_budget
  ON amended_budget.business_id = current_budget.business_id
  AND amended_budget.semantic_metric_id = current_budget.semantic_metric_id
  AND amended_budget.value_year = current_budget.value_year
  AND amended_budget.reporting_year = current_budget.reporting_year + 1
""",
        "analytics_municipal_finance_coverage_v1": f"""
CREATE OR REPLACE VIEW `{prefix}.analytics_municipal_finance_coverage_v1` AS
SELECT
  reporting_package,
  reporting_period,
  accounting_stage,
  period_coverage,
  COUNT(*) AS catalog_row_count,
  COUNT(DISTINCT business_id) AS reporter_count,
  COUNTIF(SAFE_CAST(is_selected_document AS BOOL)) AS selected_document_count,
  COUNTIF(approval_stage IN ('lopullinen', 'jalkikorjattu')) AS final_or_corrected_count,
  LOGICAL_OR(reporting_package = 'KKNR' AND reporting_period = '2022C03') AS contains_known_source_anomaly
FROM `{prefix}.municipal_finance_catalog_v1`
GROUP BY reporting_package, reporting_period, accounting_stage, period_coverage
""",
        "analytics_municipal_finance_quality_v1": f"""
CREATE OR REPLACE VIEW `{prefix}.analytics_municipal_finance_quality_v1` AS
SELECT
  'catalog_has_municipal_packages' AS check_id,
  COUNT(DISTINCT reporting_package) >= 10 AS passed,
  COUNT(DISTINCT reporting_package) AS observed,
  10 AS expected_minimum
FROM `{prefix}.municipal_finance_catalog_v1`
UNION ALL
SELECT
  'ktas_taxonomy_core_mapping',
  COUNTIF(SAFE_CAST(is_core_metric AS BOOL)) = 32,
  COUNTIF(SAFE_CAST(is_core_metric AS BOOL)),
  32
FROM `{prefix}.municipal_finance_ktas_taxonomy_v1`
UNION ALL
SELECT
  'ktas_never_presented_as_actual',
  COUNTIF(SAFE_CAST(is_actual AS BOOL) OR accounting_stage != 'budget_plan') = 0,
  COUNTIF(SAFE_CAST(is_actual AS BOOL) OR accounting_stage != 'budget_plan'),
  0
FROM `{prefix}.municipal_finance_ktas_core_v1`
UNION ALL
SELECT
  'ktas_fact_key_unique',
  COUNT(*) = COUNT(DISTINCT CONCAT(business_id, '|', CAST(reporting_year AS STRING), '|', semantic_metric_id, '|', planning_stage)),
  COUNT(*) - COUNT(DISTINCT CONCAT(business_id, '|', CAST(reporting_year AS STRING), '|', semantic_metric_id, '|', planning_stage)),
  0
FROM `{prefix}.municipal_finance_ktas_core_v1`
UNION ALL
SELECT
  'large_taxonomies_guarded',
  COUNTIF(snapshot_policy = 'too_large_for_default_snapshot') >= 2,
  COUNTIF(snapshot_policy = 'too_large_for_default_snapshot'),
  2
FROM `{prefix}.municipal_finance_taxonomy_index_v1`
UNION ALL
SELECT
  'kkNr_2022c03_not_in_semantic_facts',
  COUNTIF(reporting_package = 'KKNR' OR reporting_period = '2022C03') = 0,
  COUNTIF(reporting_package = 'KKNR' OR reporting_period = '2022C03'),
  0
FROM `{prefix}.municipal_finance_ktas_core_v1`
""",
    }


def _run_sql(sql: str, *, project: str, dry_run: bool) -> None:
    command = ["bq", f"--project_id={project}", "query", "--nouse_legacy_sql"]
    if dry_run:
        command.append("--dry_run")
    result = subprocess.run(command, input=sql, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(result.stderr or result.stdout or "bq query failed")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build municipal-finance mart views.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--render-sql-dir", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    sql_map = build_sql_map(args.project, args.dataset)
    if args.render_sql_dir:
        args.render_sql_dir.mkdir(parents=True, exist_ok=True)
        for index, (name, sql) in enumerate(sql_map.items(), start=1):
            path = args.render_sql_dir / f"{index:02d}_{name}.sql"
            path.write_text(sql.strip() + "\n", encoding="utf-8")
            print(f"SQL -> {path}")
        return 0
    for name, sql in sql_map.items():
        print(f"{'Dry-run' if args.dry_run else 'Build'}: {name}")
        _run_sql(sql, project=args.project, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

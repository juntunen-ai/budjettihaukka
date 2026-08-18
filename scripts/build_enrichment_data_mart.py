#!/usr/bin/env python3
"""Build the versioned enrichment layer on top of the fiscal mart."""

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
        "source_vintage_current_v1": f"""
CREATE OR REPLACE VIEW `{prefix}.source_vintage_current_v1` AS
SELECT * EXCEPT(vintage_rank)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY source_id ORDER BY fetched_at DESC, vintage_id DESC) AS vintage_rank
  FROM `{prefix}.source_vintage_manifest_v1`
)
WHERE vintage_rank = 1
""",
        "source_revision_history_v1": f"""
CREATE OR REPLACE VIEW `{prefix}.source_revision_history_v1` AS
WITH vintages AS (
  SELECT * FROM `{prefix}.source_vintage_manifest_v1`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY vintage_id ORDER BY fetched_at DESC) = 1
)
SELECT
  *,
  LAG(content_sha256) OVER (PARTITION BY source_id ORDER BY fetched_at, vintage_id) AS previous_content_sha256,
  content_sha256 != LAG(content_sha256) OVER (PARTITION BY source_id ORDER BY fetched_at, vintage_id) AS content_changed,
  row_count - LAG(row_count) OVER (PARTITION BY source_id ORDER BY fetched_at, vintage_id) AS row_count_change
FROM vintages
""",
        "dim_organization_master_v1": f"""
CREATE OR REPLACE VIEW `{prefix}.dim_organization_master_v1` AS
SELECT
  *,
  REGEXP_CONTAINS(business_id, r'^[0-9]{{7}}-[0-9]$') AS business_id_format_valid,
  municipality_code IS NOT NULL AND CAST(municipality_code AS STRING) != '' AS has_registered_municipality,
  'registered_address_not_service_delivery_location' AS geography_caveat
FROM `{prefix}.organization_master_v1`
""",
        "analytics_grants_okm_pilot_v1": f"""
CREATE OR REPLACE VIEW `{prefix}.analytics_grants_okm_pilot_v1` AS
SELECT
  *,
  metric_id IN ('applications_received_count', 'positive_decisions_count') AS is_count_metric,
  metric_id IN ('applied_amount_eur', 'granted_amount_eur') AS is_currency_metric,
  FALSE AS recipient_level_available,
  FALSE AS budget_moment_join_allowed,
  'Pilot aggregate from the public report; not decision-level open data' AS publication_caveat
FROM `{prefix}.official_grants_okm_pilot_v1`
""",
        "analytics_sector_indicator_v1": f"""
CREATE OR REPLACE VIEW `{prefix}.analytics_sector_indicator_v1` AS
SELECT
  source.*,
  value - LAG(value) OVER (PARTITION BY dashboard_id, metric_id, region_code ORDER BY year) AS change_from_previous_observation,
  CASE
    WHEN is_causal_effect THEN 'invalid_source_flag'
    ELSE 'descriptive_association_only'
  END AS interpretation_status
FROM `{prefix}.official_sector_indicator_v1` AS source
""",
        "analytics_sector_dashboard_v1": f"""
CREATE OR REPLACE VIEW `{prefix}.analytics_sector_dashboard_v1` AS
SELECT
  dashboard_id,
  year,
  region_code,
  region_name_fi,
  region_type,
  ARRAY_AGG(STRUCT(metric_id, metric_name_fi, indicator_type, value, unit, direction, change_from_previous_observation, source_url)
    ORDER BY indicator_type, metric_id) AS indicators,
  COUNTIF(indicator_type = 'output') > 0 AS has_output_indicator,
  COUNTIF(indicator_type = 'outcome') > 0 AS has_outcome_indicator,
  'Indicators are descriptive and do not identify the causal effect of expenditure' AS causal_caveat
FROM `{prefix}.analytics_sector_indicator_v1`
GROUP BY dashboard_id, year, region_code, region_name_fi, region_type
""",
        "analytics_regional_allocation_v1": f"""
CREATE OR REPLACE VIEW `{prefix}.analytics_regional_allocation_v1` AS
SELECT
  'sector_indicator_context' AS allocation_domain,
  dashboard_id AS subject_id,
  metric_id,
  year,
  region_code,
  region_name_fi,
  region_type,
  value,
  unit,
  'official_statistical_region' AS allocation_basis,
  TRUE AS region_validated,
  source_url
FROM `{prefix}.analytics_sector_indicator_v1`
WHERE region_type = 'region'
UNION ALL
SELECT
  'grants' AS allocation_domain,
  pilot_id AS subject_id,
  metric_id,
  EXTRACT(YEAR FROM SAFE_CAST(period_end AS DATE)) AS year,
  'FI-UNALLOCATED' AS region_code,
  'Kohdentamaton / koko maa' AS region_name_fi,
  'national_unallocated' AS region_type,
  value,
  unit,
  allocation_basis,
  FALSE AS region_validated,
  source_url
FROM `{prefix}.analytics_grants_okm_pilot_v1`
""",
        "dim_deflator_reference_v1": f"""
CREATE OR REPLACE VIEW `{prefix}.dim_deflator_reference_v1` AS
WITH base AS (
  SELECT deflator_id, MAX_BY(index_value, year) AS target_index, MAX(year) AS target_year
  FROM `{prefix}.official_deflator_reference_v1`
  GROUP BY deflator_id
)
SELECT
  source.*,
  base.target_year,
  base.target_index,
  SAFE_DIVIDE(base.target_index, source.index_value) AS multiplier_to_target_year
FROM `{prefix}.official_deflator_reference_v1` AS source
JOIN base USING (deflator_id)
""",
        "analytics_fiscal_multi_deflator_v1": f"""
CREATE OR REPLACE VIEW `{prefix}.analytics_fiscal_multi_deflator_v1` AS
SELECT
  fiscal.year,
  fiscal.fiscal_side,
  fiscal.hallinnonala,
  fiscal.kirjanpitoyksikko,
  fiscal.momentti_tunnusp,
  fiscal.momentti_snimi,
  fiscal.net_accumulation_nominal_eur,
  deflator.deflator_id,
  deflator.target_scope,
  deflator.target_year AS price_year,
  deflator.multiplier_to_target_year,
  fiscal.net_accumulation_nominal_eur * deflator.multiplier_to_target_year AS net_accumulation_real_eur,
  deflator.is_preliminary AS deflator_is_preliminary,
  CASE
    WHEN deflator.target_scope = 'generic_nominal_amounts' THEN 'general_purchasing_power_only'
    WHEN deflator.target_scope = 'building_investments' THEN 'use_only_for_verified_building_investment_scope'
    WHEN deflator.target_scope = 'municipal_public_services_only' THEN 'context_only_not_state_budget_deflator'
  END AS applicability_status,
  deflator.source_url AS deflator_source_url
FROM `{prefix}.analytics_fiscal_yearly_v1` AS fiscal
JOIN `{prefix}.dim_deflator_reference_v1` AS deflator USING (year)
""",
        "analytics_final_accounts_reconciliation_v2": f"""
CREATE OR REPLACE VIEW `{prefix}.analytics_final_accounts_reconciliation_v2` AS
WITH mart AS (
  SELECT year AS fiscal_year, SUM(net_accumulation_nominal_eur) AS budget_execution_balance_eur
  FROM `{prefix}.analytics_fiscal_yearly_v1`
  GROUP BY year
)
SELECT
  official.*,
  IF(official.metric_id = 'budget_execution_balance_eur', mart.budget_execution_balance_eur, NULL) AS mart_value,
  IF(official.metric_id = 'budget_execution_balance_eur', mart.budget_execution_balance_eur - official.official_value, NULL) AS difference_eur,
  CASE
    WHEN official.metric_id != 'budget_execution_balance_eur' THEN 'official_benchmark_only'
    WHEN mart.budget_execution_balance_eur IS NULL THEN 'mart_value_missing'
    WHEN ABS(mart.budget_execution_balance_eur - official.official_value) <= official.tolerance_eur THEN 'reconciled_within_source_precision'
    ELSE 'reconciliation_difference_review_required'
  END AS reconciliation_status,
  official.metric_id = 'budget_execution_balance_eur'
    AND mart.budget_execution_balance_eur IS NOT NULL
    AND ABS(mart.budget_execution_balance_eur - official.official_value) <= official.tolerance_eur AS is_reconciled
FROM `{prefix}.official_final_accounts_reference_v1` AS official
LEFT JOIN mart USING (fiscal_year)
""",
        "analytics_enrichment_quality_v1": f"""
CREATE OR REPLACE VIEW `{prefix}.analytics_enrichment_quality_v1` AS
SELECT 'source_vintages' AS check_id, COUNT(*) >= 6 AS passed, COUNT(*) AS observed, 6 AS expected_minimum
FROM `{prefix}.source_vintage_current_v1`
UNION ALL
SELECT 'okm_grants_pilot', COUNT(*) = 4 AND COUNTIF(recipient_level_available) = 0, COUNT(*), 4
FROM `{prefix}.analytics_grants_okm_pilot_v1`
UNION ALL
SELECT 'organization_business_ids', COUNT(*) >= 3 AND COUNTIF(NOT business_id_format_valid OR NOT business_id_checksum_valid) = 0, COUNT(*), 3
FROM `{prefix}.dim_organization_master_v1`
UNION ALL
SELECT 'two_sector_dashboards', COUNT(DISTINCT dashboard_id) = 2 AND COUNTIF(is_causal_effect) = 0, COUNT(DISTINCT dashboard_id), 2
FROM `{prefix}.analytics_sector_indicator_v1`
UNION ALL
SELECT 'regional_context', COUNTIF(region_validated) > 0 AND COUNTIF(allocation_domain = 'grants' AND region_validated) = 0, COUNTIF(region_validated), 1
FROM `{prefix}.analytics_regional_allocation_v1`
UNION ALL
SELECT 'multiple_deflators', COUNT(DISTINCT deflator_id) >= 3, COUNT(DISTINCT deflator_id), 3
FROM `{prefix}.dim_deflator_reference_v1`
UNION ALL
SELECT 'final_accounts_benchmarks', COUNT(*) >= 7, COUNT(*), 7
FROM `{prefix}.analytics_final_accounts_reconciliation_v2`
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
    parser = argparse.ArgumentParser(description="Build official enrichment mart views.")
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

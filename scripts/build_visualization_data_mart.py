#!/usr/bin/env python3
"""Build a visualization-ready BigQuery mart without natural-language logic."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings
from utils.budget_semantics import fiscal_side_case_sql

CONTRACT_PATH = ROOT / "data" / "reference" / "visualization_data_contract.yaml"


def load_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    contract = yaml.safe_load(path.read_text(encoding="utf-8"))
    for key in ("version", "sources", "metrics", "availability", "join_interfaces"):
        if not contract.get(key):
            raise ValueError(f"visualization data contract missing {key}")
    _require_unique(contract["sources"], "source_id")
    _require_unique(contract["metrics"], "metric_id")
    _require_unique(contract["availability"], "domain_id")
    _require_unique(contract["join_interfaces"], "domain_id")
    source_ids = {row["source_id"] for row in contract["sources"]}
    unknown = {row["source_id"] for row in contract["metrics"]} - source_ids
    if unknown:
        raise ValueError(f"metrics reference unknown sources: {sorted(unknown)}")
    return contract


def _require_unique(rows: list[dict[str, Any]], key: str) -> None:
    values = [str(row.get(key) or "") for row in rows]
    if any(not value for value in values):
        raise ValueError(f"empty {key} in visualization data contract")
    duplicates = sorted({value for value in values if values.count(value) > 1})
    if duplicates:
        raise ValueError(f"duplicate {key}: {duplicates}")


def _literal(value: Any) -> str:
    if value is None:
        return "CAST(NULL AS STRING)"
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _array(values: list[str]) -> str:
    return "[" + ", ".join(_literal(value) for value in values) + "]"


def _struct_rows(rows: list[dict[str, Any]], fields: list[tuple[str, str]]) -> str:
    rendered = []
    for row in rows:
        columns = []
        for source_name, target_name in fields:
            value = row.get(source_name)
            expression = _array(value) if isinstance(value, list) else _literal(value)
            columns.append(expression + f" AS {target_name}")
        rendered.append("  STRUCT(" + ", ".join(columns) + ")")
    return ",\n".join(rendered)


def build_sql_map(
    project: str,
    dataset: str,
    *,
    contract: dict[str, Any] | None = None,
    yearly_table: str = "valtiontalous_yearly_agg_v1",
    semantic_view: str = "valtiontalous_semantic_current",
    macro_table: str = "official_macro_reference_v1",
    budget_actual_view: str = "budget_vs_actual_v1",
    yearly_source_mode: str = "corrected",
) -> dict[str, str]:
    doc = contract or load_contract()
    version = str(doc["version"])
    source_structs = _struct_rows(
        doc["sources"],
        [
            ("source_id", "source_id"),
            ("title_fi", "title_fi"),
            ("organization", "organization"),
            ("url", "source_url"),
            ("accounting_basis", "accounting_basis"),
            ("update_frequency", "update_frequency"),
            ("caveat_fi", "caveat_fi"),
        ],
    )
    metric_structs = _struct_rows(
        doc["metrics"],
        [
            ("metric_id", "metric_id"),
            ("display_name_fi", "display_name_fi"),
            ("definition_fi", "definition_fi"),
            ("unit", "unit"),
            ("aggregation", "aggregation_rule"),
            ("price_basis", "price_basis"),
            ("source_id", "source_id"),
            ("sign_rule", "sign_rule"),
            ("missing_means", "missing_means"),
            ("supported_dimensions", "supported_dimensions"),
            ("visualization_status", "visualization_status"),
        ],
    )
    availability_structs = _struct_rows(
        doc["availability"],
        [
            ("domain_id", "domain_id"),
            ("status", "status"),
            ("grain", "grain"),
            ("limitation_fi", "limitation_fi"),
        ],
    )
    join_interface_structs = _struct_rows(
        doc["join_interfaces"],
        [
            ("domain_id", "domain_id"),
            ("target_grain", "target_grain"),
            ("required_keys", "required_keys"),
            ("required_measures", "required_measures"),
            ("preferred_source", "preferred_source"),
            ("join_rule_fi", "join_rule_fi"),
            ("publication_gate_fi", "publication_gate_fi"),
            ("status", "status"),
        ],
    )

    source_sql = f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.dim_data_source_v1` AS
SELECT *, '{version}' AS contract_version
FROM UNNEST([
{source_structs}
])
"""
    metric_sql = f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.dim_visualization_metric_v1` AS
SELECT *, '{version}' AS contract_version
FROM UNNEST([
{metric_structs}
])
"""
    availability_sql = f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.dim_data_availability_v1` AS
SELECT *, '{version}' AS contract_version
FROM UNNEST([
{availability_structs}
])
"""
    join_interface_sql = f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.dim_enrichment_join_contract_v1` AS
SELECT *, '{version}' AS contract_version
FROM UNNEST([
{join_interface_structs}
])
"""

    if yearly_source_mode == "corrected":
        source_projection = """
  source.*,
  'corrected_v2' AS semantic_source_version,
  TRUE AS talousarviotili_available
"""
    elif yearly_source_mode == "legacy_mislabeled_maararahalaji":
        source_projection = """
  source.vuosi,
  source.hallinnonala,
  source.ha_tunnus,
  source.tv_tunnus,
  source.kirjanpitoyksikko,
  source.momentti_tunnusp,
  source.momentti_snimi,
  source.alamomentti_tunnus AS maararahalaji_tunnus,
  source.alamomentti_snimi AS maararahalaji_snimi,
  CAST(NULL AS STRING) AS talousarviotili_tunnusp,
  CAST(NULL AS STRING) AS talousarviotili_snimi,
  CAST(NULL AS STRING) AS alamomentti_tunnus,
  CAST(NULL AS STRING) AS alamomentti_snimi,
  FALSE AS alamomentti_is_validated,
  source.nettokertyma_sum,
  source.source_rows,
  'legacy_columns_remapped_fail_closed' AS semantic_source_version,
  FALSE AS talousarviotili_available
"""
    else:
        raise ValueError(f"unsupported yearly_source_mode: {yearly_source_mode}")

    fiscal_source_sql = f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.visualization_fiscal_source_v1` AS
SELECT
{source_projection.rstrip()}
FROM `{project}.{dataset}.{yearly_table}` AS source
"""

    macro_sql = f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.analytics_macro_yearly_v1` AS
WITH source AS (
  SELECT * FROM `{project}.{dataset}.{macro_table}`
),
price_base AS (
  SELECT
    year AS real_base_year,
    value AS real_base_index
  FROM source
  WHERE series_id = 'cost_of_living_index_1951_10_100'
  QUALIFY ROW_NUMBER() OVER (ORDER BY year DESC) = 1
)
SELECT
  year,
  MAX(IF(series_id = 'population_midyear_persons', value, NULL)) AS population_midyear_persons,
  MAX(IF(series_id = 'cost_of_living_index_1951_10_100', value, NULL)) AS cost_of_living_index,
  MAX(IF(series_id = 'gdp_current_prices_meur', value, NULL)) AS gdp_current_prices_meur,
  MAX(IF(series_id = 'central_government_edp_debt_q4_meur', value, NULL)) AS central_government_edp_debt_q4_meur,
  ANY_VALUE(price_base.real_base_year) AS real_base_year,
  SAFE_DIVIDE(
    ANY_VALUE(price_base.real_base_index),
    MAX(IF(series_id = 'cost_of_living_index_1951_10_100', value, NULL))
  ) AS cpi_multiplier_to_base_year,
  COUNT(DISTINCT series_id) AS available_series_count,
  ARRAY_AGG(DISTINCT series_id ORDER BY series_id) AS available_series
FROM source
CROSS JOIN price_base
GROUP BY year
"""

    fiscal_side = fiscal_side_case_sql(
        code_expr="base.momentti_tunnusp",
        name_expr="base.momentti_snimi",
        hallinnonala_expr="base.hallinnonala",
    )
    fiscal_sql = f"""
CREATE OR REPLACE TABLE `{project}.{dataset}.analytics_fiscal_yearly_core_v1`
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(1990, 2100, 1))
CLUSTER BY fiscal_side, hallinnonala, momentti_tunnusp AS
WITH coverage AS (
  SELECT
    SAFE_CAST(`Vuosi` AS INT64) AS year,
    COUNT(DISTINCT SAFE_CAST(`Kk` AS INT64)) AS observed_months,
    MAX(SAFE_CAST(`Kk` AS INT64)) AS latest_month,
    MAX(period_date) AS data_as_of
  FROM `{project}.{dataset}.{semantic_view}`
  GROUP BY year
),
guardrails AS (
  SELECT
    momentti_tunnusp,
    event_year AS year,
    LOGICAL_OR(should_exclude_from_change_rankings) AS has_structural_guardrail,
    ARRAY_AGG(DISTINCT relation_type IGNORE NULLS ORDER BY relation_type) AS structural_relation_types,
    ARRAY_AGG(DISTINCT guardrail_note IGNORE NULLS ORDER BY guardrail_note LIMIT 3) AS structural_guardrail_notes
  FROM `{project}.{dataset}.moment_structural_change_guardrails_v1`
  GROUP BY momentti_tunnusp, year
),
latest_data_year AS (
  SELECT MAX(year) AS year FROM coverage
)
SELECT
  base.vuosi AS year,
  {fiscal_side} AS fiscal_side,
  base.hallinnonala,
  base.ha_tunnus,
  base.tv_tunnus,
  base.kirjanpitoyksikko,
  base.momentti_tunnusp,
  base.momentti_snimi,
  base.maararahalaji_tunnus,
  base.maararahalaji_snimi,
  base.talousarviotili_tunnusp,
  base.talousarviotili_snimi,
  base.alamomentti_tunnus,
  base.alamomentti_snimi,
  base.alamomentti_is_validated,
  base.semantic_source_version,
  base.talousarviotili_available,
  base.nettokertyma_sum AS net_accumulation_nominal_eur,
  base.nettokertyma_sum * macro.cpi_multiplier_to_base_year AS net_accumulation_real_cpi_eur,
  SAFE_DIVIDE(base.nettokertyma_sum, macro.population_midyear_persons) AS net_accumulation_per_capita_eur,
  100 * SAFE_DIVIDE(base.nettokertyma_sum, macro.gdp_current_prices_meur * 1000000) AS net_accumulation_pct_gdp,
  macro.population_midyear_persons,
  macro.gdp_current_prices_meur,
  macro.cost_of_living_index,
  macro.real_base_year,
  macro.central_government_edp_debt_q4_meur,
  coverage.observed_months,
  coverage.latest_month,
  coverage.data_as_of,
  coverage.observed_months = 12 AS is_complete_year,
  CASE
    WHEN coverage.observed_months = 12 THEN 'complete'
    WHEN base.vuosi = latest_data_year.year THEN 'partial_current_year'
    ELSE 'incomplete_historical'
  END AS coverage_status,
  base.vuosi < latest_data_year.year AND coverage.observed_months = 12 AS is_closed_period,
  FALSE AS is_reconciled_to_audited_final_accounts,
  COALESCE(guardrails.has_structural_guardrail, FALSE) AS has_structural_guardrail,
  guardrails.structural_relation_types,
  guardrails.structural_guardrail_notes,
  CASE
    WHEN COALESCE(guardrails.has_structural_guardrail, FALSE) THEN 'structural_break_review_required'
    ELSE 'comparable_within_accounting_basis'
  END AS comparability_status,
  base.source_rows,
  'valtiokonttori_monthly_central_government_finance' AS source_id,
  CURRENT_TIMESTAMP() AS mart_built_at
FROM `{project}.{dataset}.visualization_fiscal_source_v1` AS base
LEFT JOIN coverage ON coverage.year = base.vuosi
LEFT JOIN `{project}.{dataset}.analytics_macro_yearly_v1` AS macro ON macro.year = base.vuosi
LEFT JOIN guardrails ON guardrails.year = base.vuosi AND guardrails.momentti_tunnusp = base.momentti_tunnusp
CROSS JOIN latest_data_year
"""
    fiscal_alias_sql = f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.analytics_fiscal_yearly_v1` AS
SELECT * FROM `{project}.{dataset}.analytics_fiscal_yearly_core_v1`
"""

    metric_series_sql = f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.analytics_metric_series_v1` AS
WITH fact AS (
  SELECT * FROM `{project}.{dataset}.analytics_fiscal_yearly_v1`
),
long_values AS (
  SELECT
    fact.* EXCEPT(
      net_accumulation_nominal_eur,
      net_accumulation_real_cpi_eur,
      net_accumulation_per_capita_eur,
      net_accumulation_pct_gdp
    ),
    metric.metric_id,
    metric.value
  FROM fact,
  UNNEST([
    STRUCT('net_accumulation_nominal_eur' AS metric_id, CAST(net_accumulation_nominal_eur AS FLOAT64) AS value),
    STRUCT('net_accumulation_real_cpi_eur', CAST(net_accumulation_real_cpi_eur AS FLOAT64)),
    STRUCT('net_accumulation_per_capita_eur', CAST(net_accumulation_per_capita_eur AS FLOAT64)),
    STRUCT('net_accumulation_pct_gdp', CAST(net_accumulation_pct_gdp AS FLOAT64))
  ]) AS metric
)
SELECT
  long_values.*,
  definition.display_name_fi,
  definition.definition_fi,
  definition.unit,
  definition.aggregation_rule,
  definition.price_basis,
  definition.sign_rule,
  definition.missing_means,
  definition.visualization_status,
  definition.contract_version
FROM long_values
JOIN `{project}.{dataset}.dim_visualization_metric_v1` AS definition USING (metric_id)
"""

    budget_sql = f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.analytics_budget_vs_actual_v2` AS
WITH coverage AS (
  SELECT year, observed_months, latest_month, data_as_of, coverage_status, is_complete_year
  FROM `{project}.{dataset}.analytics_fiscal_yearly_v1`
  GROUP BY year, observed_months, latest_month, data_as_of, coverage_status, is_complete_year
)
SELECT
  source.vuosi AS year,
  source.momentti_koodi AS momentti_tunnusp,
  source.momentti_nimi AS momentti_snimi,
  CASE source.puoli WHEN 'meno' THEN 'expense' WHEN 'tulo' THEN 'revenue' ELSE 'unknown' END AS fiscal_side,
  source.tae_eur AS original_budget_eur,
  source.ltae_eur AS supplementary_budget_eur,
  source.budjetoitu_eur AS current_budget_eur,
  source.toteuma_eur AS actual_eur,
  SAFE_DIVIDE(source.toteuma_eur, NULLIF(source.budjetoitu_eur, 0)) AS actual_to_budget_ratio,
  coverage.observed_months,
  coverage.latest_month,
  coverage.data_as_of,
  coverage.coverage_status,
  coverage.is_complete_year,
  FALSE AS is_reconciled_to_audited_final_accounts,
  CASE
    WHEN source.budjetoitu_eur IS NULL THEN 'missing_budget'
    WHEN source.toteuma_eur IS NULL THEN 'missing_actual'
    WHEN source.budjetoitu_eur = 0 THEN 'zero_denominator'
    WHEN NOT coverage.is_complete_year THEN 'partial_year_not_annual_ratio'
    WHEN ABS(SAFE_DIVIDE(source.toteuma_eur, source.budjetoitu_eur)) > 10 THEN 'extreme_ratio_review_required'
    ELSE 'ready'
  END AS visualization_quality_status,
  'vm_budget_documents' AS budget_source_id,
  'valtiokonttori_monthly_central_government_finance' AS actual_source_id
FROM `{project}.{dataset}.{budget_actual_view}` AS source
LEFT JOIN coverage ON coverage.year = source.vuosi
"""

    final_accounts_sql = f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.analytics_final_accounts_reconciliation_v1` AS
SELECT
  year AS fiscal_year,
  fiscal_side,
  'state_budget_accounting_monthly_accumulation' AS accounting_basis,
  SUM(net_accumulation_nominal_eur) AS mart_total_eur,
  CAST(NULL AS NUMERIC) AS official_final_accounts_total_eur,
  CAST(NULL AS NUMERIC) AS reconciliation_difference_eur,
  CAST(NULL AS FLOAT64) AS reconciliation_difference_ratio,
  CAST(NULL AS STRING) AS official_source_url,
  CAST(NULL AS DATE) AS official_publication_date,
  'not_reconciled_official_source_missing' AS reconciliation_status,
  LOGICAL_AND(is_complete_year) AS has_twelve_months,
  FALSE AS is_reconciled_to_audited_final_accounts
FROM `{project}.{dataset}.analytics_fiscal_yearly_v1`
WHERE is_closed_period
GROUP BY fiscal_year, fiscal_side
"""

    quality_sql = f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.analytics_visualization_quality_v1` AS
WITH metric_year AS (
  SELECT
    metric_id,
    year,
    ANY_VALUE(unit) AS unit,
    ANY_VALUE(coverage_status) AS coverage_status,
    ANY_VALUE(comparability_status) AS comparability_status,
    COUNT(*) AS row_count,
    COUNTIF(value IS NULL) AS missing_value_rows,
    COUNTIF(has_structural_guardrail) AS structural_guardrail_rows,
    SUM(source_rows) AS source_rows
  FROM `{project}.{dataset}.analytics_metric_series_v1`
  GROUP BY metric_id, year
)
SELECT
  *,
  SAFE_DIVIDE(missing_value_rows, row_count) AS missing_value_rate,
  GREATEST(
    0,
    100
      - IF(coverage_status = 'complete', 0, 35)
      - ROUND(50 * SAFE_DIVIDE(missing_value_rows, row_count))
      - IF(structural_guardrail_rows > 0, 15, 0)
  ) AS visualization_readiness_score,
  ARRAY_CONCAT(
    IF(coverage_status = 'complete', [], [coverage_status]),
    IF(missing_value_rows = 0, [], ['missing_metric_values']),
    IF(structural_guardrail_rows = 0, [], ['structural_break_review_required'])
  ) AS quality_flags
FROM metric_year
"""

    return {
        "dim_data_source_v1": source_sql,
        "dim_visualization_metric_v1": metric_sql,
        "dim_data_availability_v1": availability_sql,
        "dim_enrichment_join_contract_v1": join_interface_sql,
        "visualization_fiscal_source_v1": fiscal_source_sql,
        "analytics_macro_yearly_v1": macro_sql,
        "analytics_fiscal_yearly_core_v1": fiscal_sql,
        "analytics_fiscal_yearly_v1": fiscal_alias_sql,
        "analytics_metric_series_v1": metric_series_sql,
        "analytics_budget_vs_actual_v2": budget_sql,
        "analytics_final_accounts_reconciliation_v1": final_accounts_sql,
        "analytics_visualization_quality_v1": quality_sql,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the visualization-ready BigQuery mart.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--yearly-table", default="valtiontalous_yearly_agg_v1")
    parser.add_argument("--semantic-view", default="valtiontalous_semantic_current")
    parser.add_argument("--macro-table", default="official_macro_reference_v1")
    parser.add_argument("--budget-actual-view", default="budget_vs_actual_v1")
    parser.add_argument(
        "--yearly-source-mode",
        choices=["corrected", "legacy_mislabeled_maararahalaji"],
        default="corrected",
        help="Use the legacy adapter only until corrected semantic v2 has been deployed.",
    )
    parser.add_argument("--render-sql-dir", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _run_sql(sql: str, *, project: str, dry_run: bool) -> None:
    command = ["bq", f"--project_id={project}", "query", "--nouse_legacy_sql"]
    if dry_run:
        command.append("--dry_run")
    result = subprocess.run(command, input=sql, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(result.stderr or result.stdout or "bq query failed")


def main() -> int:
    args = parse_args()
    sql_map = build_sql_map(
        args.project,
        args.dataset,
        yearly_table=args.yearly_table,
        semantic_view=args.semantic_view,
        macro_table=args.macro_table,
        budget_actual_view=args.budget_actual_view,
        yearly_source_mode=args.yearly_source_mode,
    )
    if args.render_sql_dir:
        args.render_sql_dir.mkdir(parents=True, exist_ok=True)
        expected_paths = {
            args.render_sql_dir / f"{index:02d}_{name}.sql"
            for index, name in enumerate(sql_map, start=1)
        }
        for stale_path in args.render_sql_dir.glob("*.sql"):
            if stale_path not in expected_paths:
                stale_path.unlink()
                print(f"Removed stale SQL -> {stale_path}")
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

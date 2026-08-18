CREATE OR REPLACE TABLE `budjettihaukka-gpt.valtiodata.analytics_fiscal_yearly_core_v1`
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(1990, 2100, 1))
CLUSTER BY fiscal_side, hallinnonala, momentti_tunnusp AS
WITH coverage AS (
  SELECT
    SAFE_CAST(`Vuosi` AS INT64) AS year,
    COUNT(DISTINCT SAFE_CAST(`Kk` AS INT64)) AS observed_months,
    MAX(SAFE_CAST(`Kk` AS INT64)) AS latest_month,
    MAX(period_date) AS data_as_of
  FROM `budjettihaukka-gpt.valtiodata.valtiontalous_semantic_current`
  GROUP BY year
),
guardrails AS (
  SELECT
    momentti_tunnusp,
    event_year AS year,
    LOGICAL_OR(should_exclude_from_change_rankings) AS has_structural_guardrail,
    ARRAY_AGG(DISTINCT relation_type IGNORE NULLS ORDER BY relation_type) AS structural_relation_types,
    ARRAY_AGG(DISTINCT guardrail_note IGNORE NULLS ORDER BY guardrail_note LIMIT 3) AS structural_guardrail_notes
  FROM `budjettihaukka-gpt.valtiodata.moment_structural_change_guardrails_v1`
  GROUP BY momentti_tunnusp, year
),
latest_data_year AS (
  SELECT MAX(year) AS year FROM coverage
)
SELECT
  base.vuosi AS year,
  CASE WHEN COALESCE(CAST(base.momentti_tunnusp AS STRING), '') = '' AND COALESCE(CAST(base.momentti_snimi AS STRING), '') = '' THEN 'technical' WHEN LOWER(COALESCE(CAST(base.momentti_tunnusp AS STRING), '')) = 'tapahtumia' THEN 'technical' WHEN LOWER(COALESCE(CAST(base.momentti_snimi AS STRING), '')) LIKE '%vain liikekirjanpidossa%' THEN 'technical' WHEN LOWER(COALESCE(CAST(base.momentti_snimi AS STRING), '')) LIKE '%siirrettyjen määrärahojen peruutus%' THEN 'technical' WHEN LOWER(COALESCE(CAST(base.momentti_snimi AS STRING), '')) LIKE '%nettolainanotto%' THEN 'financing' WHEN LOWER(COALESCE(CAST(base.momentti_snimi AS STRING), '')) LIKE '%velanhallinta%' THEN 'financing' WHEN CAST(base.momentti_tunnusp AS STRING) LIKE '15.%' THEN 'financing' WHEN LOWER(COALESCE(CAST(base.momentti_snimi AS STRING), '')) LIKE '%arvonlisäveromenot%' THEN 'expense' WHEN REGEXP_CONTAINS(COALESCE(CAST(base.momentti_tunnusp AS STRING), ''), r'^(11|12|13|14)\.') THEN 'revenue' WHEN LOWER(COALESCE(CAST(base.momentti_snimi AS STRING), '')) LIKE '%arvonlisävero%' AND LOWER(COALESCE(CAST(base.momentti_snimi AS STRING), '')) NOT LIKE '%arvonlisäveromenot%' THEN 'revenue' WHEN LOWER(COALESCE(CAST(base.momentti_snimi AS STRING), '')) LIKE '%tulovero%' THEN 'revenue' WHEN LOWER(COALESCE(CAST(base.momentti_snimi AS STRING), '')) LIKE '%yhteisövero%' THEN 'revenue' WHEN LOWER(COALESCE(CAST(base.momentti_snimi AS STRING), '')) LIKE '%energiavero%' THEN 'revenue' WHEN LOWER(COALESCE(CAST(base.momentti_snimi AS STRING), '')) LIKE '%myyntitulot%' THEN 'revenue' WHEN LOWER(COALESCE(CAST(base.momentti_snimi AS STRING), '')) LIKE '%osinkotulot%' THEN 'revenue' WHEN LOWER(COALESCE(CAST(base.momentti_snimi AS STRING), '')) LIKE '%toimintamenot%' THEN 'expense' WHEN LOWER(COALESCE(CAST(base.momentti_snimi AS STRING), '')) LIKE '%rahoitus%' THEN 'expense' WHEN LOWER(COALESCE(CAST(base.momentti_snimi AS STRING), '')) LIKE '%tuki%' THEN 'expense' WHEN LOWER(COALESCE(CAST(base.momentti_snimi AS STRING), '')) LIKE '%avustus%' THEN 'expense' WHEN LOWER(COALESCE(CAST(base.hallinnonala AS STRING), '')) LIKE '%hallinnonala%' THEN 'expense' ELSE 'expense' END AS fiscal_side,
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
FROM `budjettihaukka-gpt.valtiodata.visualization_fiscal_source_v1` AS base
LEFT JOIN coverage ON coverage.year = base.vuosi
LEFT JOIN `budjettihaukka-gpt.valtiodata.analytics_macro_yearly_v1` AS macro ON macro.year = base.vuosi
LEFT JOIN guardrails ON guardrails.year = base.vuosi AND guardrails.momentti_tunnusp = base.momentti_tunnusp
CROSS JOIN latest_data_year

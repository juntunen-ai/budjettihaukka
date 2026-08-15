CREATE OR REPLACE VIEW `budjettihaukka-gpt.valtiodata.analytics_budget_vs_actual_v2` AS
WITH coverage AS (
  SELECT year, observed_months, latest_month, data_as_of, coverage_status, is_complete_year
  FROM `budjettihaukka-gpt.valtiodata.analytics_fiscal_yearly_v1`
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
FROM `budjettihaukka-gpt.valtiodata.budget_vs_actual_v1` AS source
LEFT JOIN coverage ON coverage.year = source.vuosi

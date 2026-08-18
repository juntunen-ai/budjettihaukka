CREATE OR REPLACE VIEW `budjettihaukka-gpt.valtiodata.analytics_final_accounts_reconciliation_v1` AS
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
FROM `budjettihaukka-gpt.valtiodata.analytics_fiscal_yearly_v1`
WHERE is_closed_period
GROUP BY fiscal_year, fiscal_side

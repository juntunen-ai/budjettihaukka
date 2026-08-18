CREATE OR REPLACE VIEW `budjettihaukka-gpt.valtiodata.analytics_metric_series_v1` AS
WITH fact AS (
  SELECT * FROM `budjettihaukka-gpt.valtiodata.analytics_fiscal_yearly_v1`
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
JOIN `budjettihaukka-gpt.valtiodata.dim_visualization_metric_v1` AS definition USING (metric_id)

CREATE OR REPLACE VIEW `budjettihaukka-gpt.valtiodata.analytics_macro_yearly_v1` AS
WITH source AS (
  SELECT * FROM `budjettihaukka-gpt.valtiodata.official_macro_reference_v1`
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

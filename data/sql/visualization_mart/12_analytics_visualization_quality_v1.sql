CREATE OR REPLACE VIEW `budjettihaukka-gpt.valtiodata.analytics_visualization_quality_v1` AS
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
  FROM `budjettihaukka-gpt.valtiodata.analytics_metric_series_v1`
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

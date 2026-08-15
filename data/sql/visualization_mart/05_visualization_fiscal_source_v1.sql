CREATE OR REPLACE VIEW `budjettihaukka-gpt.valtiodata.visualization_fiscal_source_v1` AS
SELECT

  source.*,
  'corrected_v2' AS semantic_source_version,
  TRUE AS talousarviotili_available
FROM `budjettihaukka-gpt.valtiodata.valtiontalous_yearly_agg_v1` AS source

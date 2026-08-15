
-- Fail closed: this dimension remains empty until the official registry
-- contains year-specific talousarviotili/alamomentti rows.
CREATE OR REPLACE VIEW `valtion-budjetti-data.valtiodata.dim_alamomentti` AS
SELECT DISTINCT
  source.vuosi,
  source.momentti_tunnusp,
  source.talousarviotili_tunnusp,
  source.alamomentti_tunnus_candidate AS alamomentti_tunnus,
  COALESCE(registry.name_fi, source.alamomentti_snimi_candidate) AS alamomentti_snimi,
  registry.code_dotted AS official_code_dotted,
  'official_code_registry_v1' AS validation_source
FROM `valtion-budjetti-data.valtiodata.valtiontalous_curated_dq_v` AS source
JOIN `valtion-budjetti-data.valtiodata.official_code_registry_v1` AS registry
  ON registry.year = source.vuosi
 AND registry.level IN ('talousarviotili', 'alamomentti')
 AND registry.code_dotted = source.talousarviotili_tunnusp
WHERE source.alamomentti_tunnus_candidate IS NOT NULL


CREATE OR REPLACE VIEW `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.dq_hierarchy_consistency` AS
WITH hierarchy AS (
  
SELECT
  'hallinnonala' AS level_name,
  vuosi,
  ha_tunnus AS code,
  hallinnonala AS alias_name,
  hallinnonala_display AS alias_display_name,
  hallinnonala_family_key AS alias_family_key
FROM `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.valtiontalous_curated_dq_v`
WHERE ha_tunnus IS NOT NULL
  AND hallinnonala IS NOT NULL

UNION ALL

SELECT
  'kirjanpitoyksikko' AS level_name,
  vuosi,
  tv_tunnus AS code,
  kirjanpitoyksikko AS alias_name,
  kirjanpitoyksikko_display AS alias_display_name,
  kirjanpitoyksikko_family_key AS alias_family_key
FROM `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.valtiontalous_curated_dq_v`
WHERE tv_tunnus IS NOT NULL
  AND kirjanpitoyksikko IS NOT NULL

UNION ALL

SELECT
  'paaluokkaosasto' AS level_name,
  vuosi,
  paaluokkaosasto_tunnusp AS code,
  paaluokkaosasto_snimi AS alias_name,
  paaluokkaosasto_display AS alias_display_name,
  paaluokkaosasto_family_key AS alias_family_key
FROM `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.valtiontalous_curated_dq_v`
WHERE paaluokkaosasto_tunnusp IS NOT NULL
  AND paaluokkaosasto_snimi IS NOT NULL

UNION ALL

SELECT
  'luku' AS level_name,
  vuosi,
  luku_tunnusp AS code,
  luku_snimi AS alias_name,
  luku_display AS alias_display_name,
  luku_family_key AS alias_family_key
FROM `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.valtiontalous_curated_dq_v`
WHERE luku_tunnusp IS NOT NULL
  AND luku_snimi IS NOT NULL

UNION ALL

SELECT
  'momentti' AS level_name,
  vuosi,
  momentti_tunnusp AS code,
  momentti_snimi AS alias_name,
  momentti_display AS alias_display_name,
  momentti_family_key AS alias_family_key
FROM `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.valtiontalous_curated_dq_v`
WHERE momentti_tunnusp IS NOT NULL
  AND momentti_snimi IS NOT NULL

UNION ALL

SELECT
  'alamomentti' AS level_name,
  vuosi,
  alamomentti_tunnus AS code,
  alamomentti_snimi AS alias_name,
  alamomentti_display AS alias_display_name,
  alamomentti_family_key AS alias_family_key
FROM `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.valtiontalous_curated_dq_v`
WHERE alamomentti_tunnus IS NOT NULL
  AND alamomentti_snimi IS NOT NULL

),
normalized AS (
  SELECT
    level_name,
    vuosi,
    code,
    alias_name,
    alias_display_name,
    alias_family_key
  FROM hierarchy
  WHERE code IS NOT NULL
    AND alias_display_name IS NOT NULL
),
mapping AS (
  SELECT
    level_name,
    code,
    canonical_name,
    alias_name,
    alias_display_name,
    alias_family_key,
    valid_from_year,
    valid_to_year,
    alias_issue_category,
    family_key_count,
    family_names
  FROM `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.dim_hierarchy_name_mapping`
),
same_year_conflicts AS (
  SELECT
    normalized.level_name,
    normalized.code,
    canonical.canonical_name,
    'same_year_conflict' AS issue_category,
    normalized.vuosi AS affected_year,
    CAST(NULL AS INT64) AS valid_from_year,
    CAST(NULL AS INT64) AS valid_to_year,
    ARRAY_TO_STRING(
      ARRAY_AGG(DISTINCT normalized.alias_display_name ORDER BY normalized.alias_display_name),
      ' | '
    ) AS alias_name,
    CAST(NULL AS STRING) AS alias_display_name,
    COUNT(DISTINCT normalized.alias_display_name) AS alias_count,
    COUNT(DISTINCT normalized.alias_family_key) AS family_key_count,
    ARRAY_TO_STRING(
      ARRAY_AGG(DISTINCT normalized.alias_name ORDER BY normalized.alias_name),
      ' | '
    ) AS details
  FROM normalized
  JOIN (
    SELECT DISTINCT level_name, code, canonical_name
    FROM mapping
  ) AS canonical
    USING(level_name, code)
  GROUP BY
    normalized.level_name,
    normalized.code,
    canonical.canonical_name,
    normalized.vuosi
  HAVING COUNT(DISTINCT normalized.alias_display_name) > 1
),
alias_issues AS (
  SELECT
    level_name,
    code,
    canonical_name,
    alias_issue_category AS issue_category,
    CAST(NULL AS INT64) AS affected_year,
    valid_from_year,
    valid_to_year,
    alias_name,
    alias_display_name,
    CAST(NULL AS INT64) AS alias_count,
    CAST(NULL AS INT64) AS family_key_count,
    CONCAT('family=', alias_family_key) AS details
  FROM mapping
  WHERE alias_issue_category = 'formatting_noise'
),
historical_renames AS (
  SELECT
    level_name,
    code,
    canonical_name,
    'historical_rename' AS issue_category,
    CAST(NULL AS INT64) AS affected_year,
    MIN(valid_from_year) AS valid_from_year,
    MAX(valid_to_year) AS valid_to_year,
    MAX(alias_name) AS alias_name,
    canonical_name AS alias_display_name,
    CAST(NULL AS INT64) AS alias_count,
    MAX(family_key_count) AS family_key_count,
    MAX(family_names) AS details
  FROM mapping
  WHERE family_key_count > 1
  GROUP BY level_name, code, canonical_name
)
SELECT *
FROM same_year_conflicts
UNION ALL
SELECT *
FROM alias_issues
UNION ALL
SELECT *
FROM historical_renames


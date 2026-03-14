
CREATE OR REPLACE VIEW `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.dim_hierarchy_name_mapping` AS
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
alias_ranges AS (
  SELECT
    level_name,
    code,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT alias_name ORDER BY alias_name), ' | ') AS alias_name,
    alias_display_name,
    alias_family_key,
    MIN(vuosi) AS valid_from_year,
    MAX(vuosi) AS valid_to_year,
    COUNT(*) AS row_count,
    COUNT(DISTINCT vuosi) AS distinct_years
  FROM normalized
  GROUP BY level_name, code, alias_display_name, alias_family_key
),
family_canonical AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      level_name,
      code,
      alias_family_key,
      alias_display_name AS canonical_name,
      ROW_NUMBER() OVER (
        PARTITION BY level_name, code, alias_family_key
        ORDER BY
          SUM(row_count) DESC,
          MAX(LENGTH(alias_display_name)) DESC,
          MAX(valid_to_year) DESC,
          alias_display_name DESC
      ) AS rn
    FROM alias_ranges
    GROUP BY level_name, code, alias_family_key, alias_display_name
  )
  WHERE rn = 1
),
family_summary AS (
  SELECT
    level_name,
    code,
    COUNT(DISTINCT alias_family_key) AS family_key_count,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT canonical_name ORDER BY canonical_name), ' | ') AS family_names
  FROM family_canonical
  GROUP BY level_name, code
),
same_year_conflicts AS (
  SELECT
    level_name,
    code,
    COUNT(*) AS same_year_conflict_years,
    ARRAY_TO_STRING(ARRAY_AGG(CAST(vuosi AS STRING) ORDER BY vuosi), ', ') AS conflict_years
  FROM (
    SELECT
      level_name,
      code,
      vuosi
    FROM normalized
    GROUP BY level_name, code, vuosi
    HAVING COUNT(DISTINCT alias_display_name) > 1
  )
  GROUP BY level_name, code
)
SELECT
  alias_ranges.level_name,
  alias_ranges.code,
  family_canonical.canonical_name,
  alias_ranges.alias_name,
  alias_ranges.alias_display_name,
  alias_ranges.alias_family_key,
  alias_ranges.valid_from_year,
  alias_ranges.valid_to_year,
  alias_ranges.distinct_years,
  alias_ranges.row_count,
  CASE
    WHEN alias_ranges.alias_display_name = family_canonical.canonical_name THEN 'canonical'
    ELSE 'formatting_noise'
  END AS alias_issue_category,
  COALESCE(family_summary.family_key_count, 1) AS family_key_count,
  COALESCE(family_summary.family_names, family_canonical.canonical_name) AS family_names,
  COALESCE(same_year_conflicts.same_year_conflict_years, 0) AS same_year_conflict_years,
  COALESCE(same_year_conflicts.same_year_conflict_years, 0) > 0 AS has_same_year_conflict,
  same_year_conflicts.conflict_years
FROM alias_ranges
JOIN family_canonical
  USING(level_name, code, alias_family_key)
LEFT JOIN family_summary
  USING(level_name, code)
LEFT JOIN same_year_conflicts
  USING(level_name, code)


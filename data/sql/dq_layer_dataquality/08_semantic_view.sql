
CREATE OR REPLACE VIEW `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.valtiontalous_semantic_v1` AS
WITH source AS (
  SELECT *
  FROM `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.valtiontalous_curated_dq_v`
)
SELECT
  -- Raw-compatible names for existing SQL contracts/fallbacks
  source.vuosi AS `Vuosi`,
  source.kk AS `Kk`,
  source.ha_tunnus AS `Ha_Tunnus`,
  source.hallinnonala AS `Hallinnonala`,
  source.tv_tunnus AS `Tv_Tunnus`,
  source.kirjanpitoyksikko AS `Kirjanpitoyksikkö`,
  source.paaluokkaosasto_tunnusp AS `PaaluokkaOsasto_TunnusP`,
  source.paaluokkaosasto_snimi AS `PaaluokkaOsasto_sNimi`,
  source.luku_tunnusp AS `Luku_TunnusP`,
  source.luku_snimi AS `Luku_sNimi`,
  source.momentti_tunnusp AS `Momentti_TunnusP`,
  source.momentti_snimi AS `Momentti_sNimi`,
  source.alamomentti_tunnus AS `TakpMrL_Tunnus`,
  source.alamomentti_snimi AS `TakpMrL_sNimi`,
  source.alkuperainen_talousarvio AS `Alkuperäinen_talousarvio`,
  source.lisatalousarvio AS `Lisätalousarvio`,
  source.voimassaoleva_talousarvio AS `Voimassaoleva_talousarvio`,
  source.kaytettavissa AS `Käytettävissä`,
  source.alkusaldo AS `Alkusaldo`,
  source.nettokertyma_ko_vuodelta AS `Nettokertymä_ko_vuodelta`,
  source.nettokertyma AS `Nettokertymä`,
  source.loppusaldo AS `Loppusaldo`,

  -- Semantic helper columns (named so they do not collide with case-insensitive raw names)
  source.period_date,
  source.kirjanpitoyksikko,
  source.alamomentti_tunnus,
  source.alamomentti_snimi,
  source.nettokertyma,
  source.nettokertyma_ko_vuodelta,
  source.hallinnonala_display AS hallinnonala_display,
  source.hallinnonala_family_key AS hallinnonala_family_key,
  COALESCE(hallinnonala_map.canonical_name, source.hallinnonala_display, source.hallinnonala) AS hallinnonala_canonical,
  COALESCE(hallinnonala_map.alias_issue_category, 'canonical') AS hallinnonala_alias_issue_category,
  COALESCE(hallinnonala_map.has_same_year_conflict, FALSE) AS hallinnonala_has_same_year_conflict,
  source.kirjanpitoyksikko_display AS kirjanpitoyksikko_display,
  source.kirjanpitoyksikko_family_key AS kirjanpitoyksikko_family_key,
  COALESCE(kirjanpitoyksikko_map.canonical_name, source.kirjanpitoyksikko_display, source.kirjanpitoyksikko) AS kirjanpitoyksikko_canonical,
  COALESCE(kirjanpitoyksikko_map.alias_issue_category, 'canonical') AS kirjanpitoyksikko_alias_issue_category,
  COALESCE(kirjanpitoyksikko_map.has_same_year_conflict, FALSE) AS kirjanpitoyksikko_has_same_year_conflict,
  source.paaluokkaosasto_display AS paaluokkaosasto_display,
  source.paaluokkaosasto_family_key AS paaluokkaosasto_family_key,
  COALESCE(paaluokkaosasto_map.canonical_name, source.paaluokkaosasto_display, source.paaluokkaosasto_snimi) AS paaluokkaosasto_canonical,
  COALESCE(paaluokkaosasto_map.alias_issue_category, 'canonical') AS paaluokkaosasto_alias_issue_category,
  COALESCE(paaluokkaosasto_map.has_same_year_conflict, FALSE) AS paaluokkaosasto_has_same_year_conflict,
  source.luku_display AS luku_display,
  source.luku_family_key AS luku_family_key,
  COALESCE(luku_map.canonical_name, source.luku_display, source.luku_snimi) AS luku_canonical,
  COALESCE(luku_map.alias_issue_category, 'canonical') AS luku_alias_issue_category,
  COALESCE(luku_map.has_same_year_conflict, FALSE) AS luku_has_same_year_conflict,
  source.momentti_display AS momentti_display,
  source.momentti_family_key AS momentti_family_key,
  COALESCE(momentti_map.canonical_name, source.momentti_display, source.momentti_snimi) AS momentti_canonical,
  COALESCE(momentti_map.alias_issue_category, 'canonical') AS momentti_alias_issue_category,
  COALESCE(momentti_map.has_same_year_conflict, FALSE) AS momentti_has_same_year_conflict,
  source.alamomentti_display AS alamomentti_display,
  source.alamomentti_family_key AS alamomentti_family_key,
  COALESCE(alamomentti_map.canonical_name, source.alamomentti_display, source.alamomentti_snimi) AS alamomentti_canonical,
  COALESCE(alamomentti_map.alias_issue_category, 'canonical') AS alamomentti_alias_issue_category,
  COALESCE(alamomentti_map.has_same_year_conflict, FALSE) AS alamomentti_has_same_year_conflict,
  CONCAT(COALESCE(source.momentti_tunnusp, '?'), ' ', COALESCE(COALESCE(momentti_map.canonical_name, source.momentti_display, source.momentti_snimi), '')) AS momentti_label,
  CONCAT(COALESCE(source.alamomentti_tunnus, '?'), ' ', COALESCE(COALESCE(alamomentti_map.canonical_name, source.alamomentti_display, source.alamomentti_snimi), '')) AS alamomentti_label,
  source.quality_issue_count,
  source.has_valid_nettokertyma,
  source.row_fingerprint
FROM source
LEFT JOIN `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.dim_hierarchy_name_mapping` AS hallinnonala_map
  ON hallinnonala_map.level_name = 'hallinnonala'
 AND hallinnonala_map.code = source.ha_tunnus
 AND hallinnonala_map.alias_display_name = source.hallinnonala_display
 AND source.vuosi BETWEEN hallinnonala_map.valid_from_year AND hallinnonala_map.valid_to_year
LEFT JOIN `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.dim_hierarchy_name_mapping` AS kirjanpitoyksikko_map
  ON kirjanpitoyksikko_map.level_name = 'kirjanpitoyksikko'
 AND kirjanpitoyksikko_map.code = source.tv_tunnus
 AND kirjanpitoyksikko_map.alias_display_name = source.kirjanpitoyksikko_display
 AND source.vuosi BETWEEN kirjanpitoyksikko_map.valid_from_year AND kirjanpitoyksikko_map.valid_to_year
LEFT JOIN `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.dim_hierarchy_name_mapping` AS paaluokkaosasto_map
  ON paaluokkaosasto_map.level_name = 'paaluokkaosasto'
 AND paaluokkaosasto_map.code = source.paaluokkaosasto_tunnusp
 AND paaluokkaosasto_map.alias_display_name = source.paaluokkaosasto_display
 AND source.vuosi BETWEEN paaluokkaosasto_map.valid_from_year AND paaluokkaosasto_map.valid_to_year
LEFT JOIN `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.dim_hierarchy_name_mapping` AS luku_map
  ON luku_map.level_name = 'luku'
 AND luku_map.code = source.luku_tunnusp
 AND luku_map.alias_display_name = source.luku_display
 AND source.vuosi BETWEEN luku_map.valid_from_year AND luku_map.valid_to_year
LEFT JOIN `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.dim_hierarchy_name_mapping` AS momentti_map
  ON momentti_map.level_name = 'momentti'
 AND momentti_map.code = source.momentti_tunnusp
 AND momentti_map.alias_display_name = source.momentti_display
 AND source.vuosi BETWEEN momentti_map.valid_from_year AND momentti_map.valid_to_year
LEFT JOIN `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.dim_hierarchy_name_mapping` AS alamomentti_map
  ON alamomentti_map.level_name = 'alamomentti'
 AND alamomentti_map.code = source.alamomentti_tunnus
 AND alamomentti_map.alias_display_name = source.alamomentti_display
 AND source.vuosi BETWEEN alamomentti_map.valid_from_year AND alamomentti_map.valid_to_year
WHERE source.is_valid_year
  AND source.is_valid_month


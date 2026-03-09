
CREATE OR REPLACE VIEW `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.valtiontalous_semantic_v1` AS
SELECT
  -- Raw-compatible names for existing SQL contracts/fallbacks
  vuosi AS `Vuosi`,
  kk AS `Kk`,
  ha_tunnus AS `Ha_Tunnus`,
  hallinnonala AS `Hallinnonala`,
  tv_tunnus AS `Tv_Tunnus`,
  kirjanpitoyksikko AS `Kirjanpitoyksikkö`,
  paaluokkaosasto_tunnusp AS `PaaluokkaOsasto_TunnusP`,
  paaluokkaosasto_snimi AS `PaaluokkaOsasto_sNimi`,
  luku_tunnusp AS `Luku_TunnusP`,
  luku_snimi AS `Luku_sNimi`,
  momentti_tunnusp AS `Momentti_TunnusP`,
  momentti_snimi AS `Momentti_sNimi`,
  alamomentti_tunnus AS `TakpMrL_Tunnus`,
  alamomentti_snimi AS `TakpMrL_sNimi`,
  alkuperainen_talousarvio AS `Alkuperäinen_talousarvio`,
  lisatalousarvio AS `Lisätalousarvio`,
  voimassaoleva_talousarvio AS `Voimassaoleva_talousarvio`,
  kaytettavissa AS `Käytettävissä`,
  alkusaldo AS `Alkusaldo`,
  nettokertyma_ko_vuodelta AS `Nettokertymä_ko_vuodelta`,
  nettokertyma AS `Nettokertymä`,
  loppusaldo AS `Loppusaldo`,

  -- Semantic helper columns (named so they do not collide with case-insensitive raw names)
  period_date,
  kirjanpitoyksikko,
  alamomentti_tunnus,
  alamomentti_snimi,
  nettokertyma,
  nettokertyma_ko_vuodelta,
  CONCAT(COALESCE(momentti_tunnusp, '?'), ' ', COALESCE(momentti_snimi, '')) AS momentti_label,
  CONCAT(COALESCE(alamomentti_tunnus, '?'), ' ', COALESCE(alamomentti_snimi, '')) AS alamomentti_label,
  quality_issue_count,
  has_valid_nettokertyma,
  row_fingerprint
FROM `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.valtiontalous_curated_dq_v`
WHERE is_valid_year
  AND is_valid_month


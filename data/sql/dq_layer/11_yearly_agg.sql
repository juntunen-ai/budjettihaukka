
CREATE OR REPLACE TABLE `valtion-budjetti-data.valtiodata.valtiontalous_yearly_agg_v1`
PARTITION BY RANGE_BUCKET(vuosi, GENERATE_ARRAY(1998, 2026, 1))
CLUSTER BY hallinnonala, momentti_tunnusp, talousarviotili_tunnusp AS
SELECT
  SAFE_CAST(`Vuosi` AS INT64) AS vuosi,
  COALESCE(NULLIF(hallinnonala_canonical, ''), `Hallinnonala`) AS hallinnonala,
  NULLIF(`Ha_Tunnus`, '') AS ha_tunnus,
  NULLIF(`Tv_Tunnus`, '') AS tv_tunnus,
  NULLIF(`Kirjanpitoyksikkö`, '') AS kirjanpitoyksikko,
  NULLIF(`Momentti_TunnusP`, '') AS momentti_tunnusp,
  COALESCE(NULLIF(momentti_canonical, ''), NULLIF(`Momentti_sNimi`, '')) AS momentti_snimi,
  NULLIF(maararahalaji_tunnus, '') AS maararahalaji_tunnus,
  NULLIF(maararahalaji_snimi, '') AS maararahalaji_snimi,
  NULLIF(talousarviotili_tunnusp, '') AS talousarviotili_tunnusp,
  NULLIF(talousarviotili_snimi, '') AS talousarviotili_snimi,
  IF(alamomentti_is_validated, NULLIF(alamomentti_tunnus, ''), NULL) AS alamomentti_tunnus,
  IF(alamomentti_is_validated, NULLIF(alamomentti_snimi, ''), NULL) AS alamomentti_snimi,
  LOGICAL_AND(alamomentti_is_validated) AS alamomentti_is_validated,
  SUM(SAFE_CAST(`Nettokertymä` AS NUMERIC)) AS nettokertyma_sum,
  COUNT(*) AS source_rows
FROM `valtion-budjetti-data.valtiodata.valtiontalous_semantic_v1`
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13

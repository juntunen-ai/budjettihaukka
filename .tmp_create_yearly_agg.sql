CREATE OR REPLACE TABLE `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.valtiontalous_yearly_agg_v1`
PARTITION BY RANGE_BUCKET(vuosi, GENERATE_ARRAY(1998, 2026, 1))
CLUSTER BY hallinnonala, momentti_tunnusp, alamomentti_tunnus AS
SELECT
  SAFE_CAST(`Vuosi` AS INT64) AS vuosi,
  COALESCE(NULLIF(hallinnonala_canonical, ''), `Hallinnonala`) AS hallinnonala,
  NULLIF(`Ha_Tunnus`, '') AS ha_tunnus,
  NULLIF(`Tv_Tunnus`, '') AS tv_tunnus,
  NULLIF(`Kirjanpitoyksikkö`, '') AS kirjanpitoyksikko,
  NULLIF(`Momentti_TunnusP`, '') AS momentti_tunnusp,
  COALESCE(NULLIF(momentti_canonical, ''), NULLIF(`Momentti_sNimi`, '')) AS momentti_snimi,
  NULLIF(`TakpMrL_Tunnus`, '') AS alamomentti_tunnus,
  COALESCE(NULLIF(alamomentti_canonical, ''), NULLIF(`TakpMrL_sNimi`, '')) AS alamomentti_snimi,
  SUM(SAFE_CAST(`Nettokertymä` AS NUMERIC)) AS nettokertyma_sum,
  COUNT(*) AS source_rows
FROM `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.valtiontalous_semantic_v1`
GROUP BY 1,2,3,4,5,6,7,8,9

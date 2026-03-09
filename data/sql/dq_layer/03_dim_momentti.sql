
CREATE OR REPLACE VIEW `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.dim_momentti` AS
SELECT
  TO_HEX(MD5(CONCAT(COALESCE(momentti_tunnusp, ''), '|', COALESCE(momentti_snimi, '')))) AS momentti_id,
  momentti_tunnusp,
  momentti_snimi,
  ANY_VALUE(ha_tunnus) AS ha_tunnus,
  ANY_VALUE(hallinnonala) AS hallinnonala,
  MIN(vuosi) AS first_year,
  MAX(vuosi) AS last_year,
  COUNT(*) AS row_count
FROM `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.valtiontalous_curated_dq_v`
WHERE COALESCE(momentti_tunnusp, momentti_snimi) IS NOT NULL
GROUP BY momentti_tunnusp, momentti_snimi


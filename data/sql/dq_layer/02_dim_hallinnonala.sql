
CREATE OR REPLACE VIEW `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.dim_hallinnonala` AS
SELECT
  TO_HEX(MD5(CONCAT(COALESCE(ha_tunnus, ''), '|', COALESCE(hallinnonala, '')))) AS hallinnonala_id,
  ha_tunnus,
  hallinnonala,
  MIN(vuosi) AS first_year,
  MAX(vuosi) AS last_year,
  COUNT(*) AS row_count
FROM `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.valtiontalous_curated_dq_v`
WHERE hallinnonala IS NOT NULL
GROUP BY ha_tunnus, hallinnonala



CREATE OR REPLACE VIEW `valtion-budjetti-data.valtiodata.dim_talousarviotili` AS
SELECT
  TO_HEX(MD5(CONCAT(COALESCE(talousarviotili_tunnusp, ''), '|', COALESCE(talousarviotili_snimi, '')))) AS talousarviotili_id,
  talousarviotili_tunnusp,
  talousarviotili_snimi,
  ANY_VALUE(momentti_tunnusp) AS momentti_tunnusp,
  ANY_VALUE(momentti_snimi) AS momentti_snimi,
  MIN(vuosi) AS first_year,
  MAX(vuosi) AS last_year,
  COUNT(*) AS row_count
FROM `valtion-budjetti-data.valtiodata.valtiontalous_curated_dq_v`
WHERE COALESCE(talousarviotili_tunnusp, talousarviotili_snimi) IS NOT NULL
GROUP BY talousarviotili_tunnusp, talousarviotili_snimi

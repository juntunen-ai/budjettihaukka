
CREATE OR REPLACE VIEW `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.dim_alamomentti` AS
SELECT
  TO_HEX(MD5(CONCAT(COALESCE(alamomentti_tunnus, ''), '|', COALESCE(alamomentti_snimi, '')))) AS alamomentti_id,
  alamomentti_tunnus,
  alamomentti_snimi,
  ANY_VALUE(momentti_tunnusp) AS momentti_tunnusp,
  ANY_VALUE(momentti_snimi) AS momentti_snimi,
  MIN(vuosi) AS first_year,
  MAX(vuosi) AS last_year,
  COUNT(*) AS row_count
FROM `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.valtiontalous_curated_dq_v`
WHERE COALESCE(alamomentti_tunnus, alamomentti_snimi) IS NOT NULL
GROUP BY alamomentti_tunnus, alamomentti_snimi



CREATE OR REPLACE VIEW `valtion-budjetti-data.valtiodata.dim_maararahalaji` AS
SELECT
  TO_HEX(MD5(CONCAT(COALESCE(maararahalaji_tunnus, ''), '|', COALESCE(maararahalaji_snimi, '')))) AS maararahalaji_id,
  maararahalaji_tunnus,
  maararahalaji_snimi,
  MIN(vuosi) AS first_year,
  MAX(vuosi) AS last_year,
  COUNT(*) AS row_count
FROM `valtion-budjetti-data.valtiodata.valtiontalous_curated_dq_v`
WHERE COALESCE(maararahalaji_tunnus, maararahalaji_snimi) IS NOT NULL
GROUP BY maararahalaji_tunnus, maararahalaji_snimi

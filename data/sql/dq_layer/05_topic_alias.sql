
CREATE OR REPLACE VIEW `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.dim_topic_alias` AS
SELECT * FROM UNNEST([
  STRUCT('korkeakoulutus' AS topic, 'korkeakoulu' AS alias, 'momentti_snimi' AS match_field, '%korkeakoul%' AS like_pattern),
  STRUCT('korkeakoulutus', 'yliopisto', 'momentti_snimi', '%yliopist%'),
  STRUCT('korkeakoulutus', 'ammattikorkeakoulu', 'momentti_snimi', '%ammattikorkeakoul%'),
  STRUCT('korkeakoulutus', 'opetus', 'hallinnonala', '%opetus%'),
  STRUCT('puolustus', 'puolustus', 'hallinnonala', '%puolustus%'),
  STRUCT('sosiaali_ja_terveys', 'sosiaali', 'hallinnonala', '%sosiaali%'),
  STRUCT('sosiaali_ja_terveys', 'terveys', 'hallinnonala', '%terveys%'),
  STRUCT('liikenne', 'liikenne', 'hallinnonala', '%liikenne%'),
  STRUCT('ymparisto', 'ymparisto', 'hallinnonala', '%ympäristö%'),
  STRUCT('ymparisto', 'ymparisto', 'hallinnonala', '%ymparisto%')
])


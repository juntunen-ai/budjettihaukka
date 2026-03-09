
CREATE OR REPLACE VIEW `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.valtiontalous_curated_dq_v` AS
WITH normalized AS (
  SELECT
    SAFE_CAST(`Vuosi` AS INT64) AS vuosi,
    SAFE_CAST(`Kk` AS INT64) AS kk,
    DATE(SAFE_CAST(`Vuosi` AS INT64), SAFE_CAST(`Kk` AS INT64), 1) AS period_date,
    NULLIF(TRIM(`Ha_Tunnus`), '') AS ha_tunnus,
    NULLIF(TRIM(`Hallinnonala`), '') AS hallinnonala,
    NULLIF(TRIM(`Tv_Tunnus`), '') AS tv_tunnus,
    NULLIF(TRIM(`Kirjanpitoyksikkö`), '') AS kirjanpitoyksikko,
    NULLIF(TRIM(`PaaluokkaOsasto_TunnusP`), '') AS paaluokkaosasto_tunnusp,
    NULLIF(TRIM(`PaaluokkaOsasto_sNimi`), '') AS paaluokkaosasto_snimi,
    NULLIF(TRIM(`Luku_TunnusP`), '') AS luku_tunnusp,
    NULLIF(TRIM(`Luku_sNimi`), '') AS luku_snimi,
    NULLIF(TRIM(`Momentti_TunnusP`), '') AS momentti_tunnusp,
    NULLIF(TRIM(`Momentti_sNimi`), '') AS momentti_snimi,
    NULLIF(TRIM(`TakpT_TunnusP`), '') AS takpt_tunnusp,
    NULLIF(TRIM(`TakpT_sNimi`), '') AS takpt_snimi,
    NULLIF(TRIM(`TakpTr_sNimi`), '') AS takptr_snimi,
    NULLIF(TRIM(`TakpMrL_Tunnus`), '') AS alamomentti_tunnus,
    NULLIF(TRIM(`TakpMrL_sNimi`), '') AS alamomentti_snimi,
    NULLIF(TRIM(`TakpT_Netto`), '') AS takpt_netto_raw,
    NULLIF(TRIM(`Tililuokka_Tunnus`), '') AS tililuokka_tunnus,
    NULLIF(TRIM(`Tililuokka_sNimi`), '') AS tililuokka_snimi,
    NULLIF(TRIM(`Ylatiliryhma_Tunnus`), '') AS ylatiliryhma_tunnus,
    NULLIF(TRIM(`Ylatiliryhma_sNimi`), '') AS ylatiliryhma_snimi,
    NULLIF(TRIM(`Tiliryhma_Tunnus`), '') AS tiliryhma_tunnus,
    NULLIF(TRIM(`Tiliryhma_sNimi`), '') AS tiliryhma_snimi,
    NULLIF(TRIM(`Tililaji_Tunnus`), '') AS tililaji_tunnus,
    NULLIF(TRIM(`Tililaji_sNimi`), '') AS tililaji_snimi,
    NULLIF(TRIM(`LkpT_Tunnus`), '') AS lkpt_tunnus,
    NULLIF(TRIM(`LkpT_sNimi`), '') AS lkpt_snimi,
    NULLIF(TRIM(`Alkuperäinen_talousarvio`), '') AS alkuperainen_talousarvio_raw,
    NULLIF(TRIM(`Lisätalousarvio`), '') AS lisatalousarvio_raw,
    NULLIF(TRIM(`Voimassaoleva_talousarvio`), '') AS voimassaoleva_talousarvio_raw,
    NULLIF(TRIM(`Käytettävissä`), '') AS kaytettavissa_raw,
    NULLIF(TRIM(`Alkusaldo`), '') AS alkusaldo_raw,
    NULLIF(TRIM(`Nettokertymä_ko_vuodelta`), '') AS nettokertyma_ko_vuodelta_raw,
    NULLIF(TRIM(`NettoKertymaAikVuosSiirrt`), '') AS nettokertymaaikvuossiirrt_raw,
    NULLIF(TRIM(`Nettokertymä`), '') AS nettokertyma_raw,
    NULLIF(TRIM(`Loppusaldo`), '') AS loppusaldo_raw,
    NULLIF(TRIM(`JakamatonDb`), '') AS jakamatondb_raw,
    NULLIF(TRIM(`JakamatonKr`), '') AS jakamatonkr_raw
  FROM `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.valtiontalous_raw`
),
typed AS (
  SELECT
    *,
    SAFE_CAST(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(NULLIF(TRIM(alkuperainen_talousarvio_raw), ''), r'\s+', ''), '−', '-'), ' ', ''), ' ', ''), ',', '.') AS NUMERIC) AS alkuperainen_talousarvio,
    SAFE_CAST(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(NULLIF(TRIM(lisatalousarvio_raw), ''), r'\s+', ''), '−', '-'), ' ', ''), ' ', ''), ',', '.') AS NUMERIC) AS lisatalousarvio,
    SAFE_CAST(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(NULLIF(TRIM(voimassaoleva_talousarvio_raw), ''), r'\s+', ''), '−', '-'), ' ', ''), ' ', ''), ',', '.') AS NUMERIC) AS voimassaoleva_talousarvio,
    SAFE_CAST(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(NULLIF(TRIM(kaytettavissa_raw), ''), r'\s+', ''), '−', '-'), ' ', ''), ' ', ''), ',', '.') AS NUMERIC) AS kaytettavissa,
    SAFE_CAST(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(NULLIF(TRIM(alkusaldo_raw), ''), r'\s+', ''), '−', '-'), ' ', ''), ' ', ''), ',', '.') AS NUMERIC) AS alkusaldo,
    SAFE_CAST(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(NULLIF(TRIM(nettokertyma_ko_vuodelta_raw), ''), r'\s+', ''), '−', '-'), ' ', ''), ' ', ''), ',', '.') AS NUMERIC) AS nettokertyma_ko_vuodelta,
    SAFE_CAST(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(NULLIF(TRIM(nettokertymaaikvuossiirrt_raw), ''), r'\s+', ''), '−', '-'), ' ', ''), ' ', ''), ',', '.') AS NUMERIC) AS nettokertymaaikvuossiirrt,
    SAFE_CAST(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(NULLIF(TRIM(nettokertyma_raw), ''), r'\s+', ''), '−', '-'), ' ', ''), ' ', ''), ',', '.') AS NUMERIC) AS nettokertyma,
    SAFE_CAST(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(NULLIF(TRIM(loppusaldo_raw), ''), r'\s+', ''), '−', '-'), ' ', ''), ' ', ''), ',', '.') AS NUMERIC) AS loppusaldo,
    SAFE_CAST(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(NULLIF(TRIM(jakamatondb_raw), ''), r'\s+', ''), '−', '-'), ' ', ''), ' ', ''), ',', '.') AS NUMERIC) AS jakamatondb,
    SAFE_CAST(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(NULLIF(TRIM(jakamatonkr_raw), ''), r'\s+', ''), '−', '-'), ' ', ''), ' ', ''), ',', '.') AS NUMERIC) AS jakamatonkr
  FROM normalized
)
SELECT
  *,
  vuosi BETWEEN 1900 AND 2100 AS is_valid_year,
  kk BETWEEN 1 AND 12 AS is_valid_month,
  hallinnonala IS NOT NULL AS has_hallinnonala,
  COALESCE(momentti_tunnusp, momentti_snimi) IS NOT NULL AS has_momentti,
  (nettokertyma_raw IS NULL OR nettokertyma IS NOT NULL) AS has_valid_nettokertyma,
  (
    CAST(NOT (vuosi BETWEEN 1900 AND 2100) AS INT64) +
    CAST(NOT (kk BETWEEN 1 AND 12) AS INT64) +
    CAST(hallinnonala IS NULL AS INT64) +
    CAST(COALESCE(momentti_tunnusp, momentti_snimi) IS NULL AS INT64) +
    CAST(NOT (nettokertyma_raw IS NULL OR nettokertyma IS NOT NULL) AS INT64)
  ) AS quality_issue_count,
  TO_HEX(
    MD5(
      CONCAT(
        COALESCE(CAST(vuosi AS STRING), ''), '|',
        COALESCE(CAST(kk AS STRING), ''), '|',
        COALESCE(ha_tunnus, ''), '|',
        COALESCE(hallinnonala, ''), '|',
        COALESCE(tv_tunnus, ''), '|',
        COALESCE(kirjanpitoyksikko, ''), '|',
        COALESCE(momentti_tunnusp, ''), '|',
        COALESCE(momentti_snimi, ''), '|',
        COALESCE(alamomentti_tunnus, ''), '|',
        COALESCE(alamomentti_snimi, ''), '|',
        COALESCE(CAST(nettokertyma AS STRING), '')
      )
    )
  ) AS row_fingerprint
FROM typed
WHERE vuosi IS NOT NULL
  AND kk IS NOT NULL


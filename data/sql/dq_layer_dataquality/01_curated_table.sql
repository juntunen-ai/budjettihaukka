
CREATE OR REPLACE VIEW `valtion-budjetti-data.valtiodata.valtiontalous_curated_dq_v` AS
WITH source_raw AS (
  SELECT * FROM `valtion-budjetti-data.valtiodata.budjettidata`
),
normalized AS (
  SELECT
    SAFE_CAST(`Vuosi` AS INT64) AS vuosi,
    SAFE_CAST(`Kk` AS INT64) AS kk,
    DATE(SAFE_CAST(`Vuosi` AS INT64), SAFE_CAST(`Kk` AS INT64), 1) AS period_date,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Ha_Tunnus` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS ha_tunnus,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Hallinnonala` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS hallinnonala,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Tv_Tunnus` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS tv_tunnus,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Kirjanpitoyksikkö` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS kirjanpitoyksikko,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`PaaluokkaOsasto_TunnusP` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS paaluokkaosasto_tunnusp,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`PaaluokkaOsasto_sNimi` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS paaluokkaosasto_snimi,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Luku_TunnusP` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS luku_tunnusp,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Luku_sNimi` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS luku_snimi,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Momentti_TunnusP` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS momentti_tunnusp,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Momentti_sNimi` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS momentti_snimi,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`TakpT_TunnusP` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS talousarviotili_tunnusp,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`TakpT_sNimi` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS talousarviotili_snimi,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`TakpTr_sNimi` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS takptr_snimi,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`TakpMrL_Tunnus` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS maararahalaji_tunnus,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`TakpMrL_sNimi` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS maararahalaji_snimi,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`TakpT_Netto` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS takpt_netto_raw,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Tililuokka_Tunnus` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS tililuokka_tunnus,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Tililuokka_sNimi` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS tililuokka_snimi,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Ylatiliryhma_Tunnus` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS ylatiliryhma_tunnus,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Ylatiliryhma_sNimi` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS ylatiliryhma_snimi,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Tiliryhma_Tunnus` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS tiliryhma_tunnus,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Tiliryhma_sNimi` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS tiliryhma_snimi,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Tililaji_Tunnus` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS tililaji_tunnus,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Tililaji_sNimi` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS tililaji_snimi,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`LkpT_Tunnus` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS lkpt_tunnus,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`LkpT_sNimi` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS lkpt_snimi,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Alkuperäinen_talousarvio` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS alkuperainen_talousarvio_raw,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Lisätalousarvio` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS lisatalousarvio_raw,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Voimassaoleva_talousarvio` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS voimassaoleva_talousarvio_raw,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Käytettävissä` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS kaytettavissa_raw,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Alkusaldo` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS alkusaldo_raw,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Nettokertymä_ko_vuodelta` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS nettokertyma_ko_vuodelta_raw,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`NettoKertymaAikVuosSiirrt` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS nettokertymaaikvuossiirrt_raw,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Nettokertymä` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS nettokertyma_raw,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`Loppusaldo` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS loppusaldo_raw,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`JakamatonDb` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS jakamatondb_raw,
    NULLIF(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`JakamatonKr` AS STRING), 'ÃĪ', 'ä'), 'Ãķ', 'ö'), 'Ã\u0096', 'Ö'), 'Ã\u0084', 'Ä')), '') AS jakamatonkr_raw
  FROM source_raw
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
  CASE WHEN REGEXP_CONTAINS(momentti_tunnusp, r'^\d{2}\.\d{2}\.\d{2}\.$')  AND REGEXP_CONTAINS(talousarviotili_tunnusp, r'^\d{2}\.\d{2}\.\d{2}\.(?:\d+\.)+$')  AND STARTS_WITH(talousarviotili_tunnusp, momentti_tunnusp)  AND talousarviotili_tunnusp != momentti_tunnusp THEN SUBSTR(talousarviotili_tunnusp, LENGTH(momentti_tunnusp) + 1) ELSE NULL END AS alamomentti_tunnus_candidate,
  CASE
    WHEN CASE WHEN REGEXP_CONTAINS(momentti_tunnusp, r'^\d{2}\.\d{2}\.\d{2}\.$')  AND REGEXP_CONTAINS(talousarviotili_tunnusp, r'^\d{2}\.\d{2}\.\d{2}\.(?:\d+\.)+$')  AND STARTS_WITH(talousarviotili_tunnusp, momentti_tunnusp)  AND talousarviotili_tunnusp != momentti_tunnusp THEN SUBSTR(talousarviotili_tunnusp, LENGTH(momentti_tunnusp) + 1) ELSE NULL END IS NOT NULL THEN talousarviotili_snimi
    ELSE NULL
  END AS alamomentti_snimi_candidate,
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
  -- Fingerprint must cover EVERY content column: the source export has no
  -- transaction id, and rows may legitimately differ only in accounting-code
  -- dimensions (tililuokka/tiliryhma/lkp). A narrower hash flags such rows
  -- as false duplicates.
  TO_HEX(
    MD5(
      CONCAT(
        COALESCE(CAST(vuosi AS STRING), ''), '|',
        COALESCE(CAST(kk AS STRING), ''), '|',
        COALESCE(ha_tunnus, ''), '|',
        COALESCE(hallinnonala, ''), '|',
        COALESCE(tv_tunnus, ''), '|',
        COALESCE(kirjanpitoyksikko, ''), '|',
        COALESCE(paaluokkaosasto_tunnusp, ''), '|',
        COALESCE(paaluokkaosasto_snimi, ''), '|',
        COALESCE(luku_tunnusp, ''), '|',
        COALESCE(luku_snimi, ''), '|',
        COALESCE(momentti_tunnusp, ''), '|',
        COALESCE(momentti_snimi, ''), '|',
        COALESCE(takptr_snimi, ''), '|',
        COALESCE(talousarviotili_tunnusp, ''), '|',
        COALESCE(talousarviotili_snimi, ''), '|',
        COALESCE(maararahalaji_tunnus, ''), '|',
        COALESCE(maararahalaji_snimi, ''), '|',
        COALESCE(takpt_netto_raw, ''), '|',
        COALESCE(tililuokka_tunnus, ''), '|',
        COALESCE(tililuokka_snimi, ''), '|',
        COALESCE(ylatiliryhma_tunnus, ''), '|',
        COALESCE(ylatiliryhma_snimi, ''), '|',
        COALESCE(tiliryhma_tunnus, ''), '|',
        COALESCE(tiliryhma_snimi, ''), '|',
        COALESCE(tililaji_tunnus, ''), '|',
        COALESCE(tililaji_snimi, ''), '|',
        COALESCE(lkpt_tunnus, ''), '|',
        COALESCE(lkpt_snimi, ''), '|',
        COALESCE(alkuperainen_talousarvio_raw, ''), '|',
        COALESCE(lisatalousarvio_raw, ''), '|',
        COALESCE(voimassaoleva_talousarvio_raw, ''), '|',
        COALESCE(kaytettavissa_raw, ''), '|',
        COALESCE(alkusaldo_raw, ''), '|',
        COALESCE(nettokertyma_ko_vuodelta_raw, ''), '|',
        COALESCE(nettokertymaaikvuossiirrt_raw, ''), '|',
        COALESCE(nettokertyma_raw, ''), '|',
        COALESCE(loppusaldo_raw, ''), '|',
        COALESCE(jakamatondb_raw, ''), '|',
        COALESCE(jakamatonkr_raw, '')
      )
    )
  ) AS row_fingerprint,
  CASE WHEN hallinnonala IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' ') END AS hallinnonala_display,
  CASE WHEN CASE WHEN hallinnonala IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' ') END IS NULL THEN NULL WHEN LENGTH(CASE WHEN hallinnonala IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' ') END) >= 35 THEN SUBSTR(LOWER(CASE WHEN hallinnonala IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' ') END), 1, 35) ELSE LOWER(CASE WHEN hallinnonala IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(hallinnonala), r'\s+', ' ') END) END AS hallinnonala_family_key,
  CASE WHEN kirjanpitoyksikko IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' ') END AS kirjanpitoyksikko_display,
  CASE WHEN CASE WHEN kirjanpitoyksikko IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' ') END IS NULL THEN NULL WHEN LENGTH(CASE WHEN kirjanpitoyksikko IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' ') END) >= 35 THEN SUBSTR(LOWER(CASE WHEN kirjanpitoyksikko IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' ') END), 1, 35) ELSE LOWER(CASE WHEN kirjanpitoyksikko IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(kirjanpitoyksikko), r'\s+', ' ') END) END AS kirjanpitoyksikko_family_key,
  CASE WHEN paaluokkaosasto_snimi IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' ') END AS paaluokkaosasto_display,
  CASE WHEN CASE WHEN paaluokkaosasto_snimi IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' ') END IS NULL THEN NULL WHEN LENGTH(CASE WHEN paaluokkaosasto_snimi IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' ') END) >= 35 THEN SUBSTR(LOWER(CASE WHEN paaluokkaosasto_snimi IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' ') END), 1, 35) ELSE LOWER(CASE WHEN paaluokkaosasto_snimi IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(paaluokkaosasto_snimi), r'\s+', ' ') END) END AS paaluokkaosasto_family_key,
  CASE WHEN luku_snimi IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' ') END AS luku_display,
  CASE WHEN CASE WHEN luku_snimi IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' ') END IS NULL THEN NULL WHEN LENGTH(CASE WHEN luku_snimi IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' ') END) >= 35 THEN SUBSTR(LOWER(CASE WHEN luku_snimi IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' ') END), 1, 35) ELSE LOWER(CASE WHEN luku_snimi IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(luku_snimi), r'\s+', ' ') END) END AS luku_family_key,
  CASE WHEN momentti_snimi IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' ') END AS momentti_display,
  CASE WHEN CASE WHEN momentti_snimi IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' ') END IS NULL THEN NULL WHEN LENGTH(CASE WHEN momentti_snimi IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' ') END) >= 35 THEN SUBSTR(LOWER(CASE WHEN momentti_snimi IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' ') END), 1, 35) ELSE LOWER(CASE WHEN momentti_snimi IS NULL THEN NULL WHEN REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' '), r'[A-ZÅÄÖ]') AND REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' ') = UPPER(REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' ')) THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' '))) ELSE REGEXP_REPLACE(TRIM(momentti_snimi), r'\s+', ' ') END) END AS momentti_family_key
FROM typed
WHERE vuosi IS NOT NULL
  AND kk IS NOT NULL

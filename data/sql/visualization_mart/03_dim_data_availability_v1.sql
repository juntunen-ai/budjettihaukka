CREATE OR REPLACE VIEW `budjettihaukka-gpt.valtiodata.dim_data_availability_v1` AS
SELECT *, '1.0.0' AS contract_version
FROM UNNEST([
  STRUCT('fiscal_transactions' AS domain_id, 'ready' AS status, 'month and budget hierarchy' AS grain, 'Ei kerro maksun lopullista saajaa eikä toiminnan tulosta.' AS limitation_fi),
  STRUCT('macro_denominators' AS domain_id, 'ready' AS status, 'national annual' AS grain, 'Vertailusarjat ovat eri laskentakehikosta kuin budjettikirjanpito.' AS limitation_fi),
  STRUCT('budget_vs_actual' AS domain_id, 'ready_with_caveat' AS status, 'moment and year, 2014 onward' AS grain, 'Osavuoden toteuma-astetta ei saa esittää vuositoteumana.' AS limitation_fi),
  STRUCT('recipients_and_grants' AS domain_id, 'not_integrated' AS status, 'recipient and decision' AS grain, 'Lisää Tutkiavustuksia-data ennen saajakohtaisia visualisointeja.' AS limitation_fi),
  STRUCT('procurement' AS domain_id, 'not_integrated' AS status, 'procurement notice or contract' AS grain, 'Lisää Hilma- tai Hansel-data ennen toimittaja- ja hankintavisualisointeja.' AS limitation_fi),
  STRUCT('outputs_and_outcomes' AS domain_id, 'not_integrated' AS status, 'indicator and reporting period' AS grain, 'Rahamäärästä ei saa päätellä tehokkuutta tai vaikuttavuutta ilman tulosindikaattoria.' AS limitation_fi),
  STRUCT('audited_final_accounts' AS domain_id, 'not_integrated' AS status, 'fiscal year' AS grain, 'Täyden 12 kuukauden kertymä ei yksin todista täsmäytystä valtion tilinpäätökseen.' AS limitation_fi)
])

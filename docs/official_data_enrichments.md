# Virallisten rikastusten tietomalli

Versio 1.1 lisää Budjettihaukan visualisointimarttiin kuusi toisiaan täydentävää
rikastusta. Kaikki verkkolähteet ladataan ensin tarkistettaviksi CSV-snapshoteiksi.
Jokaiselle snapshotille lasketaan SHA-256-tiiviste ja `vintage_id`; BigQueryn
`source_vintage_manifest_v1` on append-only ja `source_revision_history_v1`
näyttää muutoksen edelliseen havaittuun versioon.

## Toteutetut rikastukset

| Kokonaisuus | Julkaistu taso | Turvaraja |
|---|---|---|
| Valtion tilinpäätös 2025 | 7 virallista benchmarkia | Pyöristettyihin miljardilukuihin liitetään lähdekohtainen toleranssi. Vain talousarvion toteumalaskelman saldoa verrataan marttiin automaattisesti. |
| Tutkiavustuksia / OKM | 4 raporttiaggregaattia | Saaja-, päätös-, Y-tunnus-, alue- ja momenttiliitokset ovat estettyjä ilman päätöstason raakadataa. |
| Y-tunnusmaster | PRH/YTJ:n todentamat viiteorganisaatiot | Y-tunnuksen muoto ja tarkistusnumero validoidaan. Rekisteriosoite ei ole toiminnan kohdealue. |
| Koulutusmittaristo | Tutkinnot (tuotos) ja työllistyminen (tulos), maa ja maakunnat | Rahamäärän ja mittarin yhteisvaihtelua ei nimetä vaikuttavuudeksi. |
| Terveysmittaristo | Lääkärikäynnit (tuotos) ja koettu terveys (tulos), maa ja maakunnat | Käyntimäärän suunta ei ole normatiivisesti yksiselitteinen; koetun terveyden sarja ei ole vuosittainen. |
| Alueellinen malli | Viralliset maakunta-avaimet sektorimittareille | OKM-avustuspilotti julkaistaan arvolla `FI-UNALLOCATED`; rekisteriosoitteesta ei päätellä avustuksen käyttöpaikkaa. |
| Usean deflaattorin malli | CPI, rakennuskustannusindeksi ja kuntatalouden julkisten menojen hintaindeksi | Rakennusindeksiä saa käyttää vain varmistettuihin rakennusinvestointeihin. Kuntatalouden indeksi on valtiontaloudessa vain konteksti. |

## Lähteet

- Valtiokonttori / Tutkihallintoa: valtion tilinpäätös 2025 ja Tutkiavustuksia.fi.
- Patentti- ja rekisterihallitus: avoin YTJ API v3, päivitys päivittäin, CC BY 4.0.
- Tilastokeskus / StatFin: taulukot 111l, 13g9 ja 11m2 sekä aiempi CPI-snapshot.
- Terveyden ja hyvinvoinnin laitos / Sotkanet: indikaattorit 1080 ja 4333 sekä virallinen alueluettelo.

Täsmälliset URL-osoitteet ja tulkintarajoitteet ovat koneellisesti luettavassa
`data/reference/visualization_data_contract.yaml`-sopimuksessa.

## BigQuery-objektit

- `source_vintage_current_v1`, `source_revision_history_v1`
- `dim_organization_master_v1`, `dim_deflator_reference_v1`
- `analytics_grants_okm_pilot_v1`
- `analytics_sector_indicator_v1`, `analytics_sector_dashboard_v1`
- `analytics_regional_allocation_v1`
- `analytics_fiscal_multi_deflator_v1`
- `analytics_final_accounts_reconciliation_v2`
- `analytics_enrichment_quality_v1`

Ensimmäinen live-ajo jättää talousarvion toteumalaskelman saldon tilaan
`reconciliation_difference_review_required`: kuukausimartin nettokertymä ei ole
samaa rajausta kuin verkkosivun lainanoton sisältävä 3,7 miljoonan euron saldo.
Poikkeama julkaistaan näkyvästi eikä sitä kuitata täsmäytetyksi. Muut kuusi
tilinpäätöslukua ovat lähdebenchmarkeja, kunnes niille on samaa laskentaperustaa
käyttävä mart-mittari.

Laatuportti hyväksyy alueelliset sektorihavainnot mutta edellyttää, että
avustusaggregaatit pysyvät kohdentamattomina. Näin myöhempi saajatason aineisto
voidaan liittää ilman, että nykyinen pilotti antaa liian tarkkaa kuvaa datasta.

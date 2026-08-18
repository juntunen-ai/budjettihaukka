# Kuntien ja kuntayhtymien talouden tietomalli

Versio 1.0 tuo Budjettihaukkaan Valtiokonttorin julkisen kuntatalousrajapinnan
aineistoluettelon ja KTAS-raportointikokonaisuuden ydintunnusluvut. Putki on
snapshot-first: verkkolähteet tallennetaan tarkistettaviksi CSV-tiedostoiksi,
ja BigQueryta muutetaan vain erillisellä `--load-bigquery`-valinnalla.

## Viralliset lähteet

1. `Kuntatalous - v1` -rajapinnan `taksonomia`-kutsu palauttaa virallisten
   raportointikokonaisuuksien taksonomiatiedostojen URL-osoitteet.
2. Valtiokonttorin kuntatalouden REST-rajapinnan `aineistot`,
   `kklmy-aineistot` ja `tolt-aineistot` -kutsut palauttavat raportoidun aineiston
   Y-tunnuksen, kokonaisuuden, kauden, hyväksymisvaiheen, päivämäärät ja
   dokumentin URL-osoitteen. Erilliset luettelot ovat välttämättömiä, jotta myös
   KKLMY- ja TOLT-tyyppiset aineistot tulevat mukaan.
3. Dokumentti-URL palauttaa varsinaiset tunnusluvut, kommentit ja
   validointihavainnot.

Koko aineistoluettelo säilytetään, mutta samaa raportointikokonaisuutta, kautta
ja Y-tunnusta kohti valitaan deterministisesti korkein hyväksymisvaihe ja uusin
hyväksymis- ja julkaisuajankohta. Alkuperäinen hyväksymisvaihe jää aina näkyviin.

## Snapshotit

| Tiedosto | Jyvä | Sisältö |
|---|---|---|
| `municipal_finance_taxonomy_index_v1.csv` | raportointikokonaisuus | Taksonomia-URL, koko, versiotunniste ja latauspolitiikka. |
| `municipal_finance_catalog_v1.csv` | Y-tunnus × kokonaisuus × kausi × julkaisu | Kaikki kuntien ja kuntayhtymien julkiset aineistometatiedot. |
| `municipal_finance_ktas_taxonomy_v1.csv` | KTAS-solutunniste | Virallinen nimi, tehtävä, suunnitteluvaihe ja ydintunnusluvun semanttinen avain. |
| `municipal_finance_ktas_core_v1.csv` | Y-tunnus × raportointivuosi × mittari × suunnitteluvaihe | Kahdeksan euromääräistä ydintunnuslukua neljässä budjetti- tai suunnitteluvaiheessa. |
| `municipal_finance_document_manifest_v1.csv` | lähdedokumentti | Tiiviste, rivimäärät, validointihavainnot ja latauksen tila. |
| `municipal_finance_sources_v1.json` | snapshot-versio | Lähteet, rajaus, taulutiivisteet ja semanttiset turvarajat. |

KTAS-ydintunnusluvut ovat toimintatulot, toimintamenot, henkilöstömenot,
palvelujen ostot, vuosikate, tilikauden tulos, lainakanta 31.12. ja
bruttoinvestointimenot. Kullekin säilytetään neljä erillistä vaihetta:

- `prior_year_amended_budget`
- `current_budget`
- `plan_year_plus_1`
- `plan_year_plus_2`

`reporting_year` kertoo lähdedokumentin vuoden ja `value_year` vuoden, jota arvo
koskee. Näin esimerkiksi vuoden 2026 dokumentin ensimmäinen suunnitelmavuosi
kohdistuu vuoteen 2027 ilman, että sitä nimetään vuoden 2027 hyväksytyksi
talousarvioksi.

## Semanttiset turvarajat

- KTAS-rivien `accounting_stage` on aina `budget_plan` ja `is_actual` aina
  `false`. Talousarviota, suunnitelmaa tai ennustetta ei saa esittää toteumana.
- KKNR:n kumulatiiviset osavuodet erotetaan vuosiraporteista. C03, C06 ja C09
  eivät ole kokonaisia vuosia; C12 merkitään koko vuoden kattavaksi, mutta se ei
  silti muutu automaattisesti tarkastetuksi tilinpäätökseksi.
- Puuttuva arvo säilyy puuttuvana. Sitä ei täytetä nollalla.
- Raportoijan Y-tunnus on kanoninen avain. Kuntaa ja kuntayhtymää ei päätellä
  nimestä tai raportointipaketista ilman virallista organisaatiomasteria.
- Rajapinnan palauttamat validointivaroitukset säilytetään rivillä. Varoitus ei
  automaattisesti poista arvoa, mutta sen tulee olla analyysissa suodatettavissa.
- Lähteen dokumentoitu KKNR 2022C03 -korkomenojen hierarkiapoikkeama
  (tilit 6200–6299) merkitään karanteeniin ennen tulevaa toteumamallia.

## Suurten taksonomioiden suoja

KTAS-taksonomia on riittävän pieni versionoitavaan snapshotiin. KKNR on satojen
megatavujen ja KKTPP kymmenien gigatavujen kokoluokkaa, joten niitä ei ladata
oletuksena. Lataaja tekee HEAD-tarkistuksen, kirjaa koon ja merkitsee yli 10 MB:n
aineiston arvolla `too_large_for_default_snapshot`. Tuleva toteuma- ja
palveluluokkamalli tarvitsee erillisen suoratoistavan putken sekä tarkasti
rajatun virallisen koodiston; koodeja ei päätellä pelkästä numerosta.

## BigQuery-näkymät

- `dim_municipal_finance_source_v1`
- `dim_municipal_finance_indicator_v1`
- `analytics_municipal_finance_catalog_v1`
- `analytics_municipal_budget_v1`
- `analytics_municipal_budget_revision_v1`
- `analytics_municipal_finance_coverage_v1`
- `analytics_municipal_finance_quality_v1`

Budjettirevisionäkymä vertaa saman vuoden alkuperäistä talousarviota seuraavan
KTAS-julkaisun muutettuun talousarvioon. Se ei mittaa budjetin ja toteuman eroa.
Kuntien summia tai paremmuusjärjestystä ei julkaista ennen kuin virallinen
organisaatioavain erottaa kunnat kuntayhtymistä ja mahdollistaa väestö- ja
alueliitokset.

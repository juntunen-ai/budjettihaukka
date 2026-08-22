# Visualisoinnit

Tämä dokumentti kuvaa Budjettihaukan visualisointien toteutustavat,
aineistosopimukset ja julkaisuportit. Visualisointi ei saa tehdä lähdeaineistoa
vahvempaa väitettä: talousarvio ei ole toteuma, yhteys ei ole syy–seuraus ja
puuttuva havainto ei ole nolla.

## Kaksi käyttötapaa

| Käyttötapa | Toteutus | Tarkoitus |
|---|---|---|
| Kysymyksestä muodostettava visualisointi | `streamlit_app.py`, `services/visualization_planner.py` ja `utils/visualization_plan_utils.py` | Käyttäjän kysymyksestä tuotettava rajattu kaavio vakioskeemalla `time`, `entity`, `metric`, `delta`, `pct` |
| Kuratoitu demonstraatio | Staattiset HTML-tiedostot sekä Streamlit-sivu `pages/visualisointidemot.py` | Ennalta tarkistettu, lähteistetty ja selkokielisesti tulkittu kokonaisuus |

Streamlitin kysymysnäkymä saa olla vuorovaikutteinen. Julkaistut staattiset
demot pidetään toistaiseksi ilman suodattimia, valitsimia ja hoverin taakse
piilotettua olennaista tietoa. Kaikki johtopäätöksen kannalta välttämättömät
arvot ja rajaukset näkyvät suoraan sivulla.

## Nykyiset demonstraatiot

| Näkymä | Aineisto ja tila | Regressiotesti |
|---|---|---|
| `visualisointidemot.html` | Viisi staattista esimerkkiä ostovoimasta, koulutuksesta, terveydestä, avustuksista ja valtion tilinpäätöksestä | `scripts/test_enrichment_visualization_demos.py` |
| `tuki-avustus.html` | Nimihaku tuki/avustus-momentteihin 1998–2025; erottaa valtionosuudet, etuudet ja sivuun jäävät erät | `scripts/test_subsidy_name_search.py` |
| `yritystuet-veroluokat.html` | Yrityksille suunnatut suorat tuet, energiaverotuki ja verokertymät; alemmat ALV-kannat merkitään puuttuviksi | `scripts/test_firm_support_tax.py` |
| `valtion-tuet-2025.html` | Vuoden 2025 tuki-, avustus-, valtionosuus- ja rahoitusmomentit; HVA-sote ja verotuet rajattu | `scripts/test_state_supports_2025.py` |
| `hallituskaudet.html` | Kuukausitoteuma hallituskausittain 1998–; kohdennus enemmistösäännöllä, ylijäämä nollaviivan yläpuolella, nimellinen ja reaalinen erikseen | `scripts/test_government_fiscal_eras.py` |
| `puoluevelka.html` | Kumulatiivinen nettovelanotto 2001– pääministeripuolueen mukaan; kokonaissumma ja kuukausivauhti rinnakkain | `scripts/test_party_debt_cumulative.py` |
| `velkavauhti.html` | Kuukausia kutakin 10 mrd € velkalisäystä kohti; askelmat kumulatiivisen kertymän pohjalta 9/2008, lyhennysvaihe erikseen | `scripts/test_debt_speed_steps.py` |
| `/visualisointidemot` (`pages/visualisointidemot.py`) | Samojen virallisten rikastusten kokeellinen vuorovaikutteinen Streamlit-näkymä | `scripts/test_official_enrichment_reference.py` ja UI-smoke |
| `sote-demo.html` | Sote 2008–2025; vuoden 2025 havaintoja käsitellään osittaisina | `scripts/test_sote_demo.py` |
| `perusterveydenhuollon-palveluarvo.html` | HVA-tason palvelujen käyttö, saatavuus, kustannukset ja vastamittarit; ei julkaise tehokkuusindeksiä | `scripts/test_primary_care_value.py` |
| `vantaa-kerava-sote.html` | Vantaa–Keravan rajaus; tunnettu käyntikirjausvaje merkitään | `scripts/test_vantaa_kerava_visualization.py` |
| `lastensuojelu-budjetti-hva.html` | Sijoitetut lapset 2021–2024 ja HVA:n koko talousarvio 2023–2026; vain 2023–2024 ovat yhteisiä vuosia | `scripts/test_child_welfare_budget_reference.py` |
| `docs/ontologia_visualisointi.html` | Ontologian, sääntöjen ja semanttisten yhteyksien tekninen arkkitehtuurikuva | Ontologian loader- ja laatutestit |

Kuntatalouden KTAS-snapshot ja semanttinen mart on toteutettu paikallisesti,
mutta auditoinnissa 18.8.2026 sen viittä lähdetaulua ja seitsemää näkymää ei
vielä löytynyt aktiivisesta BigQuery-datasetistä. Kuntatalouskuvia ei siksi
pidä kytkeä tuotannon BigQuery-kyselyihin ennen latausta ja laatunäkymän
hyväksymistä. Katso [BigQuery- ja järjestelmäauditointi](./reports/bigquery_system_audit_20260818.md).

## Visuaalinen ja kielellinen standardi

Kuratoiduissa demoissa käytetään seuraavaa oletusta:

- luonnonvalkoinen sisältötausta ja keltainen yläosa;
- suuri, mobiilissa luettava typografia;
- sama merkitys käyttää samaa väriä kaikissa kuvissa;
- kielteinen tai riskisignaali ei vaihda sattumanvaraisesti väriä kuvasta toiseen;
- jokaisella kuvalla on yksikkö, aikarajaus, lähde ja aineiston tila;
- jokaisella kuvalla on selkokielinen **Päähavainto**;
- jokaisella kuvalla on **Mitä kuva ei kerro** -rajaus;
- taloustieteellinen tai tilastollinen termi avataan arkikielellä ennen käyttöä;
- johtopäätös ei saa riippua hoverista, animaatiosta tai käyttäjän oletusvalinnasta.

## Datasopimus

Kysymyksestä muodostettavien kaavioiden ensisijainen lähde on
`analytics_metric_series_v1`. Mittari luetaan aina yhdessä kenttien `unit`,
`price_basis`, `aggregation_rule`, `coverage_status` ja
`has_structural_guardrail` kanssa. Tarkemmat objektit ja esimerkkikyselyt on
kuvattu tiedostossa [visualization_data_mart.md](./visualization_data_mart.md),
ja koneellisesti luettava sopimus on tiedostossa
`data/reference/visualization_data_contract.yaml`.

Pakolliset tulkintasäännöt:

1. Täyden vuoden vertailussa käytetään `coverage_status = 'complete'`.
2. Osavuosi merkitään näkyvästi eikä sitä verrata täyteen vuoteen ilman erillistä normalisointia.
3. Henkeä kohti lasketut arvot, prosentit ja suhdeluvut eivät ole yhteenlaskettavia.
4. Nimellinen, CPI-korjattu ja julkisten palvelujen kustannuksilla korjattu euro ovat eri mittareita.
5. `NULL` säilyy puuttuvana arvona.
6. Talousarvio, muutettu talousarvio, suunnitelma ja toteuma pidetään erillään.
7. Alamomentti julkaistaan vain virallista tilikarttaa vasten validoituna.
8. Korrelaatiota tai samanaikaista muutosta ei nimetä vaikuttavuudeksi tai tehokkuudeksi.

## Paikallinen käyttö

```bash
ruby -run -e httpd . -p 8503
```

Staattiset tiedostot avautuvat esimerkiksi osoitteessa
`http://127.0.0.1:8503/sote-demo.html`. Streamlit-näkymät käynnistyvät näin:

```bash
.venv/bin/streamlit run streamlit_app.py
```

## Julkaisuportti

Ennen visualisointimuutoksen julkaisua:

```bash
.venv/bin/python scripts/test_visualization_data_mart.py
.venv/bin/python scripts/run_visualization_data_quality_checks.py \
  --project budjettihaukka-gpt --dataset valtiodata
.venv/bin/python scripts/eval_visualization_pipeline.py
.venv/bin/python scripts/test_ui_no_crash_smoke.py
```

Lisäksi ajetaan muutetun demon oma regressiotesti. BigQuery-tarkistus on
pakollinen tuotantodatan tai datasopimuksen muuttuessa; se ei kuulu
kirjautumista vaativana testinä tavalliseen fork-PR:n offline-CI-ajoon.

Tuotantoympäristön objektit, lähteen ja semantic-kerroksen ajantasaisuus sekä
vanhenemisajat tarkistetaan kirjoittamatta BigQueryyn:

```bash
.venv/bin/python scripts/audit_bigquery_operational_state.py \
  --project budjettihaukka-gpt --dataset valtiodata
```

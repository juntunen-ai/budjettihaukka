# Budjettihaukka – Tekoälypohjainen talouspoliittisen tiedon analysointisovellus

**Nykyinen julkaisu: v2.0.0 (18.8.2026).** Julkaisun sisältö ja tunnetut
operatiiviset rajoitteet on koottu tiedostoon [CHANGELOG.md](./CHANGELOG.md).

Budjettihaukka on avoimen lähdekoodin web-sovellus, jonka tarkoituksena on tuoda **talouspolitiikkaan liittyvä tieto** helposti saataville, analysoitavaksi ja visualisoitavaksi. Sovelluksen käyttäjä voi esittää kysymyksiä **luonnollisella kielellä**, ja tekoälyn avulla saa kansantaloudelliseen optimaalisuuteen ja empiiriseen taloustutkimukseen perustubia analyyseja. Tulokset voidaan näyttää taulukkoina, dynaamisina visualisointeina sekä analyyttisinä raportteina.

Projektin pitkän aikavälin tavoite on tukea kansalaisia, tutkijoita ja toimittajia tarjoamalla **tietopohjainen, helppokäyttöinen työkalu poliittisten ja taloudellisten päätösten arviointiin**.

---

## 🔍 Tärkeimmät ominaisuudet

- 💬 Luonnollisen kielen kyselyt (esim. "Paljonko koulutukseen budjetoitiin vuonna 2023?")
- 🔁 Contract-first NL→QueryPlan→SQL -ketju (deterministinen SQL ilman vapaata LLM-SQL:ää)
- 🛡️ SQL-turvaportti ennen BigQuery-ajoa (`SELECT/WITH`, taulu-whitelist, aikarajaus, LIMIT-katto, `sqlglot`-lint)
- 🔧 Auto-repair-loop BigQuery-virheille (1-2 korjausyritystä + deterministinen fallback-contract)
- 🧭 Contract-pohjainen visualisointi vakioskeemalla (`time`, `entity`, `metric`, `delta`, `pct`)
- ❓ Pakollinen tarkennus matalalla luottamuksella ennen ajoa
- 📈 Observability + SLO-seuranta (`query_success`, `chart_render_success`, `clarification_rate`)
- 📊 Dynaamiset visualisoinnit Streamlit-käyttöliittymässä
- 📚 Datan lähteenä mm. `www.tutkihallintoa.fi`, BigQuery (tässä vaiheessa. Tarkoitus on lisätä **luotettavaa** dataa ajan myötä)

---

## 🧠 Kehityssuunta

Tulevissa vaiheissa:

- Otetaan käyttöön **agenttimainen AI-kehys** (esim. LangChain tai Haystack)
- Kehitetään **kehittyneempiä analyysikyvykkyyksiä** (esim. regressio, klusterointi, trendit)
- Visualisointeja rakennetaan **suoraan luonnollisen kielen kysymysten pohjalta**
- Laajennetaan dataa tutkimusartikkeleihin, tilastoihin ja kansainvälisiin vertailuihin
- Lopullinen tavoite: **tarjota AI:n avulla syvällisiä näkemyksiä talouspolitiikasta**

---

## 🛠️ Teknologiat

- Python 3.12+ (Streamlit, FastAPI, pandas, sqlglot)
- Google Cloud Platform
  - BigQuery (**sandbox/free tier** — rajoitukset ja niiden huomiointi
    dokumentoitu arkkitehtuurikuvauksessa)
  - Vertex AI / AI Studio (Gemini; valinnainen QueryPlan-tuki)
- Docker (Cloud Run -käyttöönotto, tulossa)

📐 **Arkkitehtuuri ja GCP-asetukset:**
[docs/architecture/system_overview.md](./docs/architecture/system_overview.md)
— komponenttikartta, dataputki, BigQuery free tier -rajoitukset,
autentikointi ja tunnetut operatiiviset erikoisuudet.

---

## ⚙️ Ympäristömuuttujat

Sovellus lukee asetukset ensisijaisesti ympäristömuuttujista:

- `BUDJETTIHAUKKA_PROJECT_ID` (oletus ja aktiivinen data-projekti: `budjettihaukka-gpt`)
- `BUDJETTIHAUKKA_LOCATION` (Vertex AI -sijainti; oletus: `us-central1`. Huom: BigQuery-datasetin sijainti on `europe-west1`)
- `BUDJETTIHAUKKA_DATA_SOURCE` (`bigquery` tai `google_sheets`; oletus: `bigquery`)
- `BUDJETTIHAUKKA_DATASET` (oletus: `valtiodata`)
- `BUDJETTIHAUKKA_TABLE` (oletus: `valtiontalous_semantic_current` — promotoitu semantic-kerroksen alias)
- `BUDJETTIHAUKKA_RAW_TABLE` (raakadatataulu ingest-/build-skripteille; oletus: `valtiontalous_raw`; `budjettidata` on legacy-kopio)
- `BUDJETTIHAUKKA_DEMO_SQL_TABLE` (oletus: `budjettidata_demo`)
- `BUDJETTIHAUKKA_DEMO_SHEET_ID_2022`, `BUDJETTIHAUKKA_DEMO_SHEET_ID_2023`, `BUDJETTIHAUKKA_DEMO_SHEET_ID_2024` (Google Sheets -lähde, kun `BUDJETTIHAUKKA_DATA_SOURCE=google_sheets`)
- `BUDJETTIHAUKKA_GEMINI_MODEL` (oletus: `gemini-2.5-pro-preview-03-25`)
- `BUDJETTIHAUKKA_ENABLE_LLM_QUERY_PLAN` (`true`/`false`, käytetäänkö LLM:ää rakenteisen QueryPlan-JSON:n tuottamiseen; oletus `false` jotta palvelu toimii myös quota-tilanteessa)
- `BUDJETTIHAUKKA_MAX_QUERY_BYTES` (BigQuery-kyselyn kustannuskatto tavuina; oletus: `1000000000`)
- `BUDJETTIHAUKKA_SQL_MAX_LIMIT` (SQL-turvaportin enimmäisrivilimit; oletus: `1000`)
- `BUDJETTIHAUKKA_BQ_AUTO_REPAIR_ATTEMPTS` (kuinka monta SQL-korjausyritystä tehdään virheen jälkeen; oletus: `2`)
- `BUDJETTIHAUKKA_CLARIFICATION_REQUIRED_CONFIDENCE` (luottamusraja pakolliselle tarkennukselle; oletus: `0.75`)
- `BUDJETTIHAUKKA_OBSERVABILITY_LOG_PATH` (jsonl-loki kyselymetriikoille; oletus: `agent_data/query_observability.jsonl`)
- `BUDJETTIHAUKKA_FREE_QUERIES_PER_SESSION` (ilmaiskäyttäjän kyselyraja / sessio; oletus: `25`)
- `BUDJETTIHAUKKA_SHOW_ADS` (`true`/`false`, näytetäänkö mainospaikat UI:ssa)
- `BUDJETTIHAUKKA_ADSENSE_CLIENT_ID` (Google AdSense client id, esim. `ca-pub-...`)
- `BUDJETTIHAUKKA_ADSENSE_SLOT_TOP`, `BUDJETTIHAUKKA_ADSENSE_SLOT_BOTTOM` (ylä- ja alapalkin ad slot id:t)
- `BUDJETTIHAUKKA_AD_PLACEHOLDER_TEXT` (placeholder-teksti, jos AdSense ei ole konfiguroitu)
- `GEMINI_API_KEY` (AI Studio / Gemini API key; jos asetettu, sovellus käyttää AI Studio -tilaa)
- `GOOGLE_APPLICATION_CREDENTIALS` (polku GCP service account -avaimeen)
- `TAVILY_API_KEY` (vain jos verkkohaku on käytössä)

Esimerkki:

```bash
export BUDJETTIHAUKKA_PROJECT_ID="budjettihaukka-gpt"
export BUDJETTIHAUKKA_LOCATION="us-central1"
export GOOGLE_APPLICATION_CREDENTIALS="/polku/avaimeen/gcp-creds.json"
export GEMINI_API_KEY="your-gemini-api-key"
```

---

## 🚧 Nykytila

Prototyyppi on toimiva, mutta ei vielä luotettava kaikissa kyselyissä. Kehitys on käynnissä AI-agenttirakenteen suuntaan. Projektia rakentaa kehittäjä, jolla on rajoitettu kokemus koodaamisesta ja pilvipalveluista, mutta vahva ymmärrys ongelmakentästä ja tekoälyn soveltamisesta.

Koodia rakennetaan tekoälyapureiden (esim. ChatGPT) tuella vaihe vaiheelta — tavoite on **helppokäyttöinen ja läpinäkyvä järjestelmä**, jonka rakentaminen on dokumentoitu oppimisprosessina.

---

## 🧪 Data Quality (BigQuery)

Data quality -kerros voidaan rakentaa ja tarkistaa skripteillä:

```bash
cd /Users/harrijuntunen/budjettihaukka
.venv/bin/python scripts/build_bq_data_quality_layer.py
.venv/bin/python scripts/run_bq_data_quality_checks.py
.venv/bin/python scripts/audit_bigquery_operational_state.py
```

Tämä luo:
- `valtiontalous_curated_dq` (tyypitetty/normalisoitu taulu + quality flagit)
- `dim_hallinnonala`, `dim_momentti`, `dim_maararahalaji`, `dim_talousarviotili`, validoitu `dim_alamomentti` ja `dim_topic_alias`
- `valtiontalous_semantic_v{N}` (versioitu analytiikan näkymä, `--semantic-version N`)
- `valtiontalous_yearly_agg_v1` (vuositason aggregaattitaulu contracteille)
- `valtiontalous_semantic_current` (promotoitu alias, jota sovellus lukee)
- raportit hakemistoon `docs/reports/`

Operatiivinen auditointi on kirjoittamaton tarkistus. Se varmistaa kaikkien
objektien kyseltävyyden, vertaa semantic-kerroksen uusinta kuukautta viralliseen
lähdeluetteloon ja varoittaa lähestyvistä BigQuery-vanhenemisajoista.

Sovellus lukee oletuksena promotoitua aliasta (`BUDJETTIHAUKKA_TABLE=valtiontalous_semantic_current`),
ei raakataulua. Versiointi mahdollistaa turvallisen rollbackin:

```bash
# Rakenna ja promotoi uusi versio
.venv/bin/python scripts/build_bq_data_quality_layer.py --semantic-version 2

# Rollback edelliseen versioon ilman uudelleenrakennusta
.venv/bin/python scripts/build_bq_data_quality_layer.py --semantic-version 1 --promote-only
```

Sarakeyhteensopivuus sovelluksen SQL-generoinnin ja semantic-näkymän välillä
tarkistetaan offline-testillä (ei vaadi BigQuery-yhteyttä):

```bash
.venv/bin/python scripts/test_semantic_view_column_compat.py
```

Jos dataset-oikeudet eivät vielä riitä taulujen luontiin, voit generoida SQL-paketin paikallisesti:

```bash
.venv/bin/python scripts/build_bq_data_quality_layer.py --render-sql-dir data/sql/dq_layer
```

Lisätiedot: [docs/data_quality_improvements.md](./docs/data_quality_improvements.md)
Alamomentin fail-closed-malli: [docs/alamomentti_semantic_model.md](./docs/alamomentti_semantic_model.md)

### Visualisointivalmis datamart

Viralliset väestö-, hinta-, BKT- ja EDP-velkavertailut sekä versionoitu
mittari- ja lähderekisteri rakennetaan erilliseen visualisointikerrokseen:

```bash
.venv/bin/python scripts/load_visualization_reference_series.py --year-from 1998 --year-to 2026
.venv/bin/python scripts/build_visualization_data_mart.py --render-sql-dir data/sql/visualization_mart
.venv/bin/python scripts/test_visualization_data_mart.py
.venv/bin/python scripts/run_visualization_data_quality_checks.py --project PROJECT --dataset DATASET
```

Kerros merkitsee osavuodet, puuttuvat nimittäjät ja rakennemuutokset, eikä
väitä kuukausikertymää tarkastettuun tilinpäätökseen täsmäytetyksi.
Käyttö- ja tulkintaohje: [docs/visualization_data_mart.md](./docs/visualization_data_mart.md)
Visualisointien kokonaisuus, tyylisäännöt ja julkaisuportti:
[docs/visualizations.md](./docs/visualizations.md).

### Viralliset rikastukset ja revisiohistoria

Tilinpäätösbenchmarkit, OKM:n Tutkiavustuksia-aggregaatit, PRH:n Y-tunnusmaster,
koulutus- ja terveysmittaristot, aluekonteksti sekä kolme käyttötarkoitukseltaan
erilaista deflaattoria päivitetään snapshot-first-putkella:

```bash
.venv/bin/python scripts/load_official_enrichment_reference.py
.venv/bin/python scripts/test_official_enrichment_reference.py
.venv/bin/python scripts/load_official_enrichment_reference.py --load-bigquery --project PROJECT --dataset DATASET
.venv/bin/python scripts/build_enrichment_data_mart.py --project PROJECT --dataset DATASET
```

Tutkiavustuksia-pilotti on tarkoituksella vain aggregaattitasolla: saaja-, päätös-,
Y-tunnus-, alue- ja momenttiliitokset pysyvät estettyinä, kunnes virallinen
päätöstason aineisto on saatavilla. Tarkemmat määritelmät ja julkaisuportit:
[docs/official_data_enrichments.md](./docs/official_data_enrichments.md).

### Kuntien ja kuntayhtymien talous

Valtiokonttorin avoimista rajapinnoista muodostetaan koko kuntatalouden
aineistoluettelo sekä vuosien 2023–2026 KTAS-taloustietojen ydinsnapshot. KTAS
sisältää hyväksytyn talousarvion, edellisen vuoden muutetun talousarvion ja kaksi
suunnitelmavuotta. Lukuja ei esitetä toteumina.

```bash
.venv/bin/python scripts/load_municipal_finance_reference.py --start-year 2023 --end-year 2026
.venv/bin/python scripts/test_municipal_finance_reference.py
.venv/bin/python scripts/load_municipal_finance_reference.py --start-year 2023 --end-year 2026 --load-bigquery --project PROJECT --dataset DATASET
.venv/bin/python scripts/build_municipal_finance_mart.py --project PROJECT --dataset DATASET
```

Rajapinnan suuret KKNR- ja KKTPP-taksonomiat on estetty oletuslatauksesta.
Niiden URL, koko ja saatavuus säilyvät lähdeindeksissä. Raportoijan Y-tunnusta ei
automaattisesti nimetä kunnaksi tai kuntayhtymäksi ilman virallista
organisaatioavainta. Malli, laatupoikkeamat ja jatkolaajennus on kuvattu
tiedostossa [docs/municipal_finance_enrichment.md](./docs/municipal_finance_enrichment.md).

Tuotannon BigQuery-objektien tila ja 18.8.2026 havaittujen korjaustarpeiden
priorisointi on dokumentoitu
[BigQuery- ja järjestelmäauditointiin](./docs/reports/bigquery_system_audit_20260818.md).
BigQuery-sandboxin ulkopuolisen, eheystarkistetun Parquet-varmistuksen käyttöohje:
[docs/bigquery_huggingface_backup.md](./docs/bigquery_huggingface_backup.md).

### Tuet ja avustukset, nimihaku 1998–2025

Staattinen `tuki-avustus.html` näyttää valtion menomomentit, joiden nimessä on
tuki, tuet, tuen, tukia tai avustus. Viisi kuvaa erottaa valtionosuudet,
kotitalouksien etuudet ja sivuun jäävät erät, kuten opintorahan ja kuntien
peruspalvelujen valtionosuuden. Nimihaku ei ole virallinen tukirekisteri, eikä
verotukia ole mukana.

```bash
.venv/bin/python scripts/load_subsidy_name_search.py
.venv/bin/python scripts/test_subsidy_name_search.py
ruby -run -e httpd . -p 8503
```

Sivu avautuu osoitteessa `http://127.0.0.1:8503/tuki-avustus.html`. Vuoden 2010
pudotus merkitään VOS-uudistukseksi, ei automaattisesti tukien leikkaukseksi.

### Yritystuet ja budjetin veroluokat 2015–2025

`yritystuet-veroluokat.html` erottaa suorat yritystuet, energiaverotuen ja
verojen kertymät. Alemmat ALV-kannat ja yhteisöverokannan “hinta” eivät ole
omia momentejaan.

```bash
.venv/bin/python scripts/load_firm_support_tax.py
.venv/bin/python scripts/test_firm_support_tax.py
```

Sivu: `http://127.0.0.1:8503/yritystuet-veroluokat.html`.

### Valtion tuet 2025

`valtion-tuet-2025.html` kokoaa vuoden 2025 tuki-, avustus-, valtionosuus- ja
rahoitusmomentit. Hyvinvointialueiden sote-rahoitus on rajattu ulos;
energiaverotuki on mukana verotukena.

```bash
.venv/bin/python scripts/load_state_supports_2025.py
.venv/bin/python scripts/test_state_supports_2025.py
```

Sivu: `http://127.0.0.1:8503/valtion-tuet-2025.html`.

### Suomen talouskasvu neljästä lähteestä

`gdp_growth_outlook_v1` kokoaa BKT:n volyymin muutoksen eli sen, mitä
talouskasvulla tarkoitetaan. Nimellinen BKT ei kelpaa kasvun mittariksi, eikä
elinkustannusindeksillä deflatointi tuota samaa lukua: vuoden 2023 volyymi oli
−1,3 %, mutta CPI-deflatoituna −3,5 %.

| Lähde | Rooli | Kattavuus | Haku |
|---|---|---|---|
| Tilastokeskus | kansallinen toteuma | 1976– | StatFin PxWeb |
| Suomen Pankki | ennuste | 2026–2028 | verkkosivu, kirjattu käsin |
| OECD | toteuma ja ennuste | 1961–2027 | SDMX |
| IMF | toteuma ja ennuste | 1980–2031 | DataMapper |

Saman vuoden luvut eroavat lähteittäin, koska aineistoversiot ovat eri
ikäisiä. Vuosina 2024–2025 OECD ja IMF jäävät noin 0,5 prosenttiyksikköä
Tilastokeskuksesta, koska ne eivät ole päivittäneet tarkistettua tilinpitoa.
Eroa ei tasoiteta: jokainen rivi kantaa oman lähteensä, vintagensa ja
osoitteensa.

Suomen Pankilla ei ole avointa data-rajapintaa, joten sen ennusteluvut on
luettu julkaisusta ja kirjattu lataajaan julkaisupäivineen. Mukana on kaksi
vintagea, jotta ennusteen tarkistuminen näkyy.

Lisäksi `official_macro_reference_v1` sai kolme uutta sarjaa samasta
Tilastokeskuksen taulusta: `gdp_volume_change_pct`, `gdp_volume_index_2015_100`
ja `gdp_price_index_2015_100`. Viimeinen on BKT:n oma deflaattori, toisin kuin
aiemmat kolme indeksiä.

```bash
.venv/bin/python scripts/load_visualization_reference_series.py --load-bigquery
.venv/bin/python scripts/load_gdp_growth_outlook.py --load-bigquery
.venv/bin/python scripts/test_gdp_growth_outlook.py
```

### Velkaantumisen nopeus 2001–

`velkavauhti.html` on yhden kuvan sivu: montako kuukautta kului kunkin
10 miljardin euron velkalisäyksen kertymiseen. Lyhyt palkki tarkoittaa nopeaa
velkaantumista. Velan määritelmä tulee sellaisenaan tiedostosta
`scripts/load_party_debt_cumulative.py`, jotta samasta asiasta on yksi sääntö.

Askelmat lasketaan kumulatiivisen kertymän pohjalta 9/2008, ei jakson alusta.
Valtio lyhensi velkaa vuoteen 2008 asti ja kertymä painui 17,8 miljardia
miinukselle. Jakson alusta laskettuna ensimmäinen askelma olisi 127 kuukautta
ja näyttäisi hitaalta velkaantumiselta, vaikka valtio tosiasiassa maksoi velkaa
pois. Lyhennysvaihe esitetään omana rivinään.

```bash
.venv/bin/python scripts/load_debt_speed_steps.py
.venv/bin/python scripts/test_debt_speed_steps.py
```

Sivu: `http://127.0.0.1:8503/velkavauhti.html`.

### Nettovelanotto pääministeripuolueittain 2001–

`puoluevelka.html` on yhden kuvan sivu: kumulatiivinen nettovelanotto, jonka
käyrän väri vaihtuu pääministerin puolueen mukaan. Mittari muodostuu kahdesta
velanhallintamomentista, `15.03.01. Nettolainanotto ja velanhallinta` (2003–)
ja `37.01.94. Nettokuoletukset ja velanhallinta` (2001–2008). Positiivinen luku
kasvattaa velkaa, negatiivinen lyhentää.

Kolme rajoitetta, jotka sivu kertoo näkyvästi:

- Pääministerin puolue ei ole hallitus. Kaikki kaudet olivat koalitioita, ja
  samat puolueet istuivat toistensa hallituksissa.
- Kausien pituudet eroavat rajusti (83–143 kuukautta), joten kokonaissummat
  eivät vertaudu. Sivu esittää kuukausivauhdin niiden rinnalla.
- Sarja alkaa 1/2001, koska sitä ennen velanhallintamomenttia ei ole lainkaan.
  Vuosien 1998–2000 velanlyhennys jää siis pois.

```bash
.venv/bin/python scripts/load_party_debt_cumulative.py
.venv/bin/python scripts/test_party_debt_cumulative.py
```

Sivu: `http://127.0.0.1:8503/puoluevelka.html`.

### Hallituskaudet ja valtion talous 1998–

`hallituskaudet.html` esittää valtion kuukausitoteuman hallituskausittain.
Kohdennussääntö: kuukausi kuuluu sille hallitukselle, joka piti valtaa
suurimman osan kuukauden päivistä. Vaihtoehto olisi jakaa vuosisumma
päiväosuuksilla, mutta se olettaisi menojen jakautuvan tasaisesti vuoden
sisällä.

Saldo esitetään julkisen taloudenpidon tapaan: ylijäämä nollaviivan
yläpuolella, alijäämä alapuolella. Lähdeaineiston nettokertymässä menot ovat
positiivisia, joten merkki käännetään esitystä varten. Jokainen hallitus on
merkitty nimellä ja vuosiluvuilla.

Sivu merkitsee näkyvästi kolme asiaa, joita ilman luvut johtaisivat harhaan:
kausien pituudet eroavat (vertailu tehdään kuukausikeskiarvona), Lipposen
ensimmäisestä kaudesta puuttuu alku ja Orpon kausi on kesken, ja kolmelta
kuukaudelta (3/2017, 3/2019, 3/2023) lähdeaineistossa ei ole yhtään
nettokertymän arvoa — ne on jätetty pois summista eikä korvattu nollalla.

```bash
.venv/bin/python scripts/load_government_fiscal_eras.py
.venv/bin/python scripts/test_government_fiscal_eras.py
```

Sivu: `http://127.0.0.1:8503/hallituskaudet.html`. Huom: kuva kertoo kuka oli
vallassa, ei kuka päätti — talousarvio päätetään edellisenä syksynä.

### Suomen hallitukset 1917–

`government_cabinet_v1` sisältää kaikki 77 hallitusta itsenäisyydestä alkaen:
nimi, pääministerin puolue, hallitustyyppi sekä tarkat alku- ja
loppupäivämäärät. Lähde on valtioneuvoston hallituslistaus, ja ketju on
katkeamaton — jokaisen hallituksen päättymispäivä on seuraavan aloituspäivä.

Näkymä `dim_government_by_year_v1` purkaa vallassaolon vuosiksi. Se käyttää
puoliavointa väliä `[start_date, end_date)`, jotta vallanvaihtopäivää ei
lasketa kahdesti; täysinä vuosina osuudet summautuvat tasan yhteen. Vuosi voi
siis sisältää useamman hallituksen omalla osuudellaan, kuten 2019
(Sipilä 43 %, Rinne 51 %, Marin 6 %).

```bash
.venv/bin/python scripts/load_government_cabinet.py --load-bigquery
.venv/bin/python scripts/test_government_cabinet.py
```

Huom: lähteen `hallituspäivät` laskee molemmat päätepäivät mukaan, joten se on
yhden suurempi kuin `end_date - start_date`. Istuvan hallituksen luku on elävä
laskuri, joka vastaa `vintage_date`-päivää.

### Sote-demonstratio 2008–2025

Staattinen, selkokielinen `sote-demo.html` vertaa verorasitusta ja julkisia
terveysmenoja palvelujen saatavuuteen, henkilöstöön ja terveystuloksiin. Eurostat-
snapshot voidaan päivittää ja tarkistaa näin:

```bash
.venv/bin/python scripts/load_sote_demo_reference.py
.venv/bin/python scripts/test_sote_demo.py
ruby -run -e httpd . -p 8503
```

Vuoden 2025 aineisto on osittainen. Arvio ei oleta kaikkien verojen rahoittavan
sotea eikä tulkitse panosten ja terveystulosten yhteyttä kausaaliseksi.

### Perusterveydenhuollon laatu ja ohjausriski 2020–2025

`perusterveydenhuollon-palveluarvo.html` yhdistää nykyisten hyvinvointialueiden
rajoilla reaaliset kustannukset, henkilöstön, lääkärikäynnit, hoitoonpääsyn,
jatkuvuuden, tyydyttämättömän palvelutarpeen, päivystyskäynnit, vältettävissä
olevat sairaalahoidot ja koetun terveyden. Vuodet 2020–2022 ovat
kuntakauden havaintoja koottuna nykyisille aluerajoille; vuoden 2023 kohdalla
näytetään siksi järjestämisvastuun muutos. Vuosi 2025 pidetään näkyvissä
osittaisena eikä puuttuvia havaintoja täytetä.

`vantaa-kerava-sote.html` rajaa samat mittarit Vantaa–Keravan
hyvinvointialueeseen ja vertaa niitä koko maahan. Näkymä merkitsee tunnetun
käyntikirjausten vajauksen eikä käytä käyntituotoksen muutosta alueen
tehokkuusarviona.

```bash
.venv/bin/python scripts/load_primary_care_value_reference.py
.venv/bin/python scripts/test_primary_care_value.py
.venv/bin/python scripts/test_vantaa_kerava_visualization.py
ruby -run -e httpd . -p 8503
```

Käyntien ja kustannusten suhde esitetään käyntituotoksena, ei tehokkuutena.
Palveluarvoa ei julkaista ennen ratkaistun palvelutarpeen ja koko hoitoketjun
kustannuksen saatavuutta. Aluekohtainen ohjausriski tunnistaa ristiriitaiset
mittarisignaalit, mutta ei ole paremmuusjärjestys tai kausaalinen arvio.
Määritelmät, säännöt, kattavuus ja rajaukset:
[docs/primary_care_service_value.md](./docs/primary_care_service_value.md).

### Lastensuojelun sijoitukset ja HVA-budjetit

`lastensuojelu-budjetti-hva.html` vertaa 22 järjestäjän kodin ulkopuolelle
sijoitettujen 0–17-vuotiaiden määrää ja osuutta hyvinvointialueiden koko
talousarvion toimintamenoihin. Sijoitustiedot kattavat vuodet 2021–2024 ja
nimelliset talousarviot vuodet 2023–2026. Vain vuodet 2023–2024 ovat aineistoissa
päällekkäiset, joten rinnastus on kuvaileva eikä osoita syy-yhteyttä tai
lastensuojelun kustannustehokkuutta.

```bash
.venv/bin/python scripts/load_child_welfare_budget_reference.py
.venv/bin/python scripts/test_child_welfare_budget_reference.py
ruby -run -e httpd . -p 8503
```

---

## 🧪 Robustisuus- ja regressiotestit

```bash
cd /Users/harrijuntunen/budjettihaukka
.venv/bin/python scripts/eval_visualization_pipeline.py
.venv/bin/python scripts/eval_robustness_suite.py --dataset data/evals/robustness_goldens.json
.venv/bin/python scripts/test_semantic_view_column_compat.py
.venv/bin/python scripts/test_schema_drift_detection.py
.venv/bin/python scripts/test_bigquery_integration.py
.venv/bin/python scripts/test_visualization_data_mart.py
.venv/bin/python scripts/test_official_enrichment_reference.py
.venv/bin/python scripts/test_ui_no_crash_smoke.py
# Optional screenshot-smoke (requires Playwright):
# .venv/bin/python scripts/test_ui_no_crash_screenshots.py
```

Robustisuusdatasetti sisältää 320 kysymystä (typoja, puhekieltä, epäselviä aikarajoja, top-kasvu-kysymyksiä), ja arviointi tarkistaa 3 tasoa:
- intentti
- SQL shape
- visualisoinnin primäärityyppi

---

## 📊 SLO-seuranta

Kyselypolusta kirjoitetaan observability-lokiin mm. kentät:
- `query_source`
- `contract`
- `confidence`
- `retries`
- `dry_run_bytes`
- `render_template`
- `error_class`

Raportoi nykytila:

```bash
.venv/bin/python scripts/report_slo_metrics.py
```

Tavoitteet:
- `query_success` > 99%
- `chart_render_success` > 98%
- `clarification_rate` hallitulla tasolla

---

## 📄 Lisenssi

Tämä projekti on lisensoitu **GNU General Public License v3.0 (GPLv3)** mukaisesti.

Lue koko lisenssiteksti tiedostosta [`LICENSE`](./LICENSE).

---

## 🤝 Osallistu

Tämä projekti on avoin ideoille, ehdotuksille ja kontribuutioille.  
Voit tehdä forkkeja, issueita tai PR:itä – tai vain käyttää ja kertoa eteenpäin!

---

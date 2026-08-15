# Budjettihaukka – Tekoälypohjainen talouspoliittisen tiedon analysointisovellus

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

- `BUDJETTIHAUKKA_PROJECT_ID` (oletus: `valtion-budjetti-data`; aktiivinen data-projekti on `budjettihaukka-gpt` — aseta `.env.local`-tiedostossa)
- `BUDJETTIHAUKKA_LOCATION` (Vertex AI -sijainti; oletus: `us-central1`. Huom: BigQuery-datasetin sijainti on `europe-west1`)
- `BUDJETTIHAUKKA_DATA_SOURCE` (`bigquery` tai `google_sheets`; oletus: `bigquery`)
- `BUDJETTIHAUKKA_DATASET` (oletus: `valtiodata`)
- `BUDJETTIHAUKKA_TABLE` (oletus: `valtiontalous_semantic_current` — promotoitu semantic-kerroksen alias)
- `BUDJETTIHAUKKA_RAW_TABLE` (raakadatataulu ingest-/build-skripteille; oletus: `budjettidata`)
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
export BUDJETTIHAUKKA_PROJECT_ID="valtion-budjetti-data"
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
```

Tämä luo:
- `valtiontalous_curated_dq` (tyypitetty/normalisoitu taulu + quality flagit)
- `dim_hallinnonala`, `dim_momentti`, `dim_maararahalaji`, `dim_talousarviotili`, validoitu `dim_alamomentti` ja `dim_topic_alias`
- `valtiontalous_semantic_v{N}` (versioitu analytiikan näkymä, `--semantic-version N`)
- `valtiontalous_yearly_agg_v1` (vuositason aggregaattitaulu contracteille)
- `valtiontalous_semantic_current` (promotoitu alias, jota sovellus lukee)
- raportit hakemistoon `docs/reports/`

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

---

## 🧪 Robustisuus- ja regressiotestit

```bash
cd /Users/harrijuntunen/budjettihaukka
.venv/bin/python scripts/eval_visualization_pipeline.py
.venv/bin/python scripts/eval_robustness_suite.py --dataset data/evals/robustness_goldens.json
.venv/bin/python scripts/test_semantic_view_column_compat.py
.venv/bin/python scripts/test_schema_drift_detection.py
.venv/bin/python scripts/test_bigquery_integration.py
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

# Budjettihaukka – Tekoälypohjainen talouspoliittisen tiedon analysointisovellus

Budjettihaukka on avoimen lähdekoodin web-sovellus, jonka tarkoituksena on tuoda **talouspolitiikkaan liittyvä tieto** helposti saataville, analysoitavaksi ja visualisoitavaksi. Sovelluksen käyttäjä voi esittää kysymyksiä **luonnollisella kielellä**, ja tekoälyn avulla saa kansantaloudelliseen optimaalisuuteen ja empiiriseen taloustutkimukseen perustubia analyyseja. Tulokset voidaan näyttää taulukkoina, dynaamisina visualisointeina sekä analyyttisinä raportteina.

Projektin pitkän aikavälin tavoite on tukea kansalaisia, tutkijoita ja toimittajia tarjoamalla **tietopohjainen, helppokäyttöinen työkalu poliittisten ja taloudellisten päätösten arviointiin**.

---

## 🔍 Tärkeimmät ominaisuudet

- 💬 Luonnollisen kielen kyselyt (esim. "Paljonko koulutukseen budjetoitiin vuonna 2023?")
- 🔁 Automaattinen SQL-generointi Google Vertex AI:n avulla
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

- Python 3.13
- Streamlit
- Google Cloud Platform
  - BigQuery
  - Vertex AI
  - Cloud Storage
  - Firebase Hosting / Studio
- Docker (Cloud Run -käyttöönotto)
- Jupyter Workbench (prototyyppivaiheessa)

---

## ⚙️ Ympäristömuuttujat

Sovellus lukee asetukset ensisijaisesti ympäristömuuttujista:

- `BUDJETTIHAUKKA_PROJECT_ID` (oletus: `valtion-budjetti-data`)
- `BUDJETTIHAUKKA_LOCATION` (oletus: `us-central1`)
- `BUDJETTIHAUKKA_DATA_SOURCE` (`bigquery` tai `google_sheets`; oletus: `bigquery`)
- `BUDJETTIHAUKKA_DATASET` (oletus: `valtiodata`)
- `BUDJETTIHAUKKA_TABLE` (oletus: `budjettidata`)
- `BUDJETTIHAUKKA_DEMO_SQL_TABLE` (oletus: `budjettidata_demo`)
- `BUDJETTIHAUKKA_DEMO_SHEET_ID_2022`, `BUDJETTIHAUKKA_DEMO_SHEET_ID_2023`, `BUDJETTIHAUKKA_DEMO_SHEET_ID_2024` (Google Sheets -lähde, kun `BUDJETTIHAUKKA_DATA_SOURCE=google_sheets`)
- `BUDJETTIHAUKKA_GEMINI_MODEL` (oletus: `gemini-2.5-pro-preview-03-25`)
- `BUDJETTIHAUKKA_MAX_QUERY_BYTES` (BigQuery-kyselyn kustannuskatto tavuina; oletus: `1000000000`)
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
- `dim_hallinnonala`, `dim_momentti`, `dim_alamomentti`, `dim_topic_alias`
- `valtiontalous_semantic_v1` (analytiikan näkymä)
- raportit hakemistoon `docs/reports/`

Jos dataset-oikeudet eivät vielä riitä taulujen luontiin, voit generoida SQL-paketin paikallisesti:

```bash
.venv/bin/python scripts/build_bq_data_quality_layer.py --render-sql-dir data/sql/dq_layer
```

Lisätiedot: [docs/data_quality_improvements.md](./docs/data_quality_improvements.md)

---

## 📄 Lisenssi

Tämä projekti on lisensoitu **GNU General Public License v3.0 (GPLv3)** mukaisesti.

Lue koko lisenssiteksti tiedostosta [`LICENSE`](./LICENSE).

---

## 🤝 Osallistu

Tämä projekti on avoin ideoille, ehdotuksille ja kontribuutioille.  
Voit tehdä forkkeja, issueita tai PR:itä – tai vain käyttää ja kertoa eteenpäin!

---

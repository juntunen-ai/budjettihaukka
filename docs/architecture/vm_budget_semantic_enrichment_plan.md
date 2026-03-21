# VM Budjettisivuston semanttinen rikastuskerros

## Tavoite

Hyödynnetään [budjetti.vm.fi:n historiallista budjettiaineistoa](https://budjetti.vm.fi/tae/etusivu_aiemmat.jsp?lang=fi) Budjettihaukan semanttisen kerroksen rikastamiseen niin, että luonnollisen kielen kysymykset voidaan maadoittaa tarkemmin oikeisiin budjettimomentteihin, niiden historiallisiin merkityksiin ja budjettiprosessin vaiheisiin.

Tämä ei korvaa Valtiokonttorin numeerista master-dataa. Se täydentää sitä.

## Toteutettu pilotti

### 1. Crawler / ingest

Tiedosto:
- `/Users/harrijuntunen/budjettihaukka/scripts/ingest_vm_budget_semantic_enrichment.py`

Mitä tekee:
- hakee vuosikohtaiset dokumenttilistaukset `frame_year.jsp`-sivuilta
- löytää dokumentit kuten:
  - ajantasainen yhdistelmä
  - talousarvioesitys
  - täydentävä esitys
  - lisätalousarvioesitys
  - VM:n ehdotus
- avaa dokumenttien TOC-solmut `sisalto.jsp`-polun kautta
- lataa varsinaisen HTML-sisällön `download.jsp`-reitiltä
- erottaa näkyvän tekstin
- rekonstruoi budjettihierarkian viittaukset vanhoista rivinvaihtoja sisältävistä HTML-sivuista
- kirjoittaa ulos:
  - dokumenttikatalogin
  - tekstisegmentit

Pilotin ajettu otos:
- vuodet `2002-2005`
- dokumentit per vuosi: enintään `6`
- nodeja per dokumentti: enintään `18`

Tuotokset:
- `/Users/harrijuntunen/budjettihaukka/data/semantic_enrichment/vm_budget_site/catalog_2002_2005.jsonl`
- `/Users/harrijuntunen/budjettihaukka/data/semantic_enrichment/vm_budget_site/segments_2002_2005.jsonl`
- `/Users/harrijuntunen/budjettihaukka/docs/reports/vm_budget_semantic_enrichment_pilot.md`

### 2. Semanttinen evidenssikerros

Tiedosto:
- `/Users/harrijuntunen/budjettihaukka/scripts/build_vm_budget_semantic_evidence.py`

Mitä tekee:
- lukee tekstisegmentit
- tunnistaa segmentin sisällön luonteen, esim.
  - `numeric_table`
  - `general_justification`
  - `detailed_justification`
  - `revenue_section`
  - `allowance_section`
- räjäyttää segmentin evidenssiriveiksi hierarkiatasoittain:
  - `osasto`
  - `luku`
  - `momentti`
- lisää joka riville:
  - lähdedokumentin metatiedot
  - sisältötyypin
  - katkelman (`snippet`)
  - evidenssiluottamuksen

Tuotokset:
- `/Users/harrijuntunen/budjettihaukka/data/semantic_enrichment/vm_budget_site/semantic_evidence_2002_2005.jsonl`
- `/Users/harrijuntunen/budjettihaukka/docs/reports/vm_budget_semantic_evidence_pilot.md`

### 3. Regressiotesti hierarkiapoiminnalle

Tiedosto:
- `/Users/harrijuntunen/budjettihaukka/scripts/test_vm_budget_semantic_enrichment.py`

Tarkistaa:
- että vanhan VM-HTML:n hajautettu rakenne kuten
  - `35.`
  - `30.`
  - `54.`
  rekonstruoituu oikein muodoksi `35.30.54.`

## Pilotin löydökset

Vuodet `2002-2005` tuottivat:
- `24` dokumenttia
- `98` tekstisegmenttiä
- `6147` evidenssiriviä

Hierarkiatasoittain:
- `momentti`: `4015`
- `luku`: `1924`
- `osasto`: `208`

Budjettivaiheittain eniten evidenssiä tuli:
- `Ajantasainen yhdistelmä`
- `Talousarvioesitys`
- `Muu budjettiasiakirja`
- `Lisätalousarvioesitys`

Tärkeä havainto:
- `Numerotaulu`- ja `Yksityiskohtaiset perustelut` -sisältö on käyttökelpoista semanttiseen rikastamiseen.
- Vanhoissa dokumenteissa momenttirakenne on usein rivinvaihtojen rikkoma, joten hierarkiaviittaukset on rekonstruoitava parserissa, ei pelkällä regexillä.

## Mihin tätä dataa käytetään

### 1. Concept bridge -rikastus

Tavoite:
- kytkeä canonical conceptit täsmällisempiin momentteihin ja lukuihin historiallisesti

Esimerkki:
- `asumistuki`
  - voidaan ankkuroida momenttievidenssiin eri vuosilta
- `varhaiskasvatus`
  - voidaan tunnistaa, esiintyykö se omana momenttina vai osana laajempaa rahoituskokonaisuutta
- `puolustus`
  - voidaan erottaa budjettiteknisistä tai muista vääristä osumista

### 2. Historiallinen momenttijatkumo

Tavoite:
- rakentaa `moment lineage` -kerros, joka erottaa
  - aidon leikkauksen
  - uudelleennimeämisen
  - momentin siirron
  - split/merge-muutoksen

Tämä on kriittinen parannus esimerkiksi kysymyksiin:
- `Mistä leikattiin eniten?`
- `Mikä kasvoi eniten?`

### 3. Selitysten laadun parantaminen

Tavoite:
- käyttää dokumenttikatkelmia LLM-selitysten taustatietona
- yhdistää numeerinen trendi BigQuerystä ja semanttinen selitys budjettiasiakirjasta

### 4. Budjettiprosessin vaihevertailu

Tavoite:
- vertailla samoja käsitteitä eri vaiheissa:
  - VM:n ehdotus
  - hallituksen esitys
  - täydentävä esitys
  - lisätalousarviot
  - ajantasainen yhdistelmä

Tämä mahdollistaa myöhemmin kysymykset kuten:
- `Miten puolustusmomentit muuttuivat valmistelun aikana?`
- `Mitä lisättiin lisätalousarvioissa?`

## Suositeltu BigQuery-skeema

### `vm_budget_document_catalog`

Kentät:
- `year`
- `document_label`
- `document_slug`
- `stage_key`
- `stage_label`
- `document_url`
- `source_page_url`

### `vm_budget_document_segments`

Kentät:
- `year`
- `document_slug`
- `document_label`
- `stage_key`
- `stage_label`
- `node_label`
- `node_url`
- `heading`
- `text`
- `text_length`
- `content_url`
- `content_hash`
- `osasto_codes`
- `luku_codes`
- `momentti_codes`

### `vm_budget_semantic_evidence`

Kentät:
- `year`
- `stage_key`
- `stage_label`
- `document_slug`
- `document_label`
- `node_label`
- `heading`
- `content_kind`
- `hierarchy_level`
- `hierarchy_code`
- `snippet`
- `content_url`
- `content_hash`
- `evidence_confidence`

### myöhempi taulu: `vm_budget_moment_lineage_candidates`

Kentät:
- `from_code`
- `to_code`
- `relation_type`
- `source_year`
- `target_year`
- `evidence_source_url`
- `evidence_confidence`

## Kytkentä nykyiseen AI-native arkkitehtuuriin

### Nykyinen polku
- `question -> semantic parser -> ontology resolver -> analytics engine -> BigQuery -> visualization -> explanation`

### Uusi rikastus tällä aineistolla
- `question -> semantic parser -> ontology resolver`
- `ontology resolver -> concept rules + VM semantic evidence`
- `analytics engine -> BigQuery numerodata`
- `explanation service -> BigQuery numerodata + VM semantic evidence`

Tämä tarkoittaa käytännössä:
- SQL pysyy edelleen numeerisen datan toteutuskerroksena
- VM-aineisto toimii semanttisen merkityksen, historiallisen jatkuvuuden ja selitysten rikastuskerroksena

## Seuraavat vaiheet

### Phase 1
- Aja crawler koko käyttökelpoiselle aikajaksolle `2002-2025`
- Kirjoita katalogi-, segmentti- ja evidenssiaineisto pysyviksi outputeiksi

### Phase 2
- Lataa aineisto BigQueryyn tauluihin:
  - `vm_budget_document_catalog`
  - `vm_budget_document_segments`
  - `vm_budget_semantic_evidence`

### Phase 3
- Rakenna `concept_moment_bridge_v2`
  - yhdistä nykyinen ontologia + VM-evidenssi
  - lisää historiatieto ja confidence

### Phase 4
- Käytä evidenssiä automaattisissa guardraileissa:
  - jos käsite on epäselvä, tarkenna
  - jos historiallinen jatkuvuus on rikki, varoita käyttäjää

### Phase 5
- Kytke evidenssi LLM explanation -palveluun
  - näytä lähdekatkelma tai lähdelinkki
  - tee selityksestä perusteltu, ei vain tilastollinen

## Mitä tämä ratkaisee juuri nyt

Tämä rikastuskerros auttaa erityisesti näissä ongelmissa:
- `varhaiskasvatus` ei osu vielä oikein nykyiseen dataan
- `yliopistot` vs `korkeakoulutus` tarvitsee tarkemman semanttisen rajauksen
- `leikattu eniten` tarvitsee historiallisen jatkuvuuden, ettei rakennemuutos näytä leikkaukselta
- `puolustusmenot` tarvitsee parempaa evidenssiä siitä, mitkä momentit kuuluvat konseptiin eri vuosina

## Yhteenveto

Tämä pilotin toteutus antaa meille jo toimivan VM-budjettisivuston rikastusputken. Se ei ole vielä koko historian täysi ingest, mutta se on riittävän vahva näyttö siitä, että aineistosta saadaan:
- käyttökelpoista semanttista evidenssiä
- momenttitason historiallista kontekstia
- parempia guardraileja luonnollisen kielen kyselyille
- uskottavampia selityksiä ja visualisointeja

Seuraava järkevä askel on ajaa tämä koko käyttökelpoiselle aikajaksolle ja nostaa evidenssiaineisto BigQueryyn semanttisen kerroksen osaksi.

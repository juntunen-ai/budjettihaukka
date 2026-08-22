# BigQuery-varmistus Hugging Face Storage Bucketiin

## Tavoite

Varmistus suojaa BigQuery-sandboxin 60 päivän vanhenemiselta. Se ei korvaa
BigQueryn SQL-laskentaa eikä pidennä objektien elinikää. BigQuery-objektien TTL
uusitaan ja lähdedata päivitetään erillisellä hallitulla ajolla.

Varmistin on BigQueryyn päin kirjoittamaton:

- fyysiset taulut luetaan erissä ja kirjoitetaan Zstandard-pakatuiksi
  Parquet-shardeiksi;
- näkymien SQL ja kaikkien objektien API-metatiedot tallennetaan;
- manifesti sisältää rivi- ja kokoluvut sekä jokaisen tiedoston SHA-256-tiivisteen;
- keskeneräistä tai muuttunutta snapshotia ei lähetetä;
- Hugging Face -siirto hyväksyy vain yksityisen bucketin;
- snapshotit tallennetaan uusiin aikaleimaprefikseihin eikä vanhoja poisteta.

## Asennus

```bash
.venv/bin/pip install -r requirements-backup.txt
```

Hugging Face -tokenia ei anneta komentoriviparametrina tai tallenneta repoon.
Kirjaudu paikallisesti `hf auth login` -komennolla ja käytä vain bucketille
rajattua write-tokenia. Automaatiota varten token annetaan salaisuutena
`HF_TOKEN`-ympäristömuuttujassa.

Ensimmäinen kertaluonteinen varmistus voidaan tehdä myös Hugging Facen
verkkolatauksella ilman API-tokenia. Tällöin snapshot paketoidaan yhdeksi
`tar.gz`-arkistoksi ja sen rinnalle tallennetaan SHA-256-tarkistussumma.

## Turvallinen käyttöönotto

Tarkista ensin suunnitelma ilman tiedostojen luontia:

```bash
.venv/bin/python scripts/backup_bigquery_to_huggingface.py --dry-run
```

Aja pieni pilotti:

```bash
.venv/bin/python scripts/backup_bigquery_to_huggingface.py \
  --table analytics_fiscal_yearly_core_v1 \
  --snapshot-id 20260818T120000Z
```

Tarkista olemassa oleva paikallinen snapshot:

```bash
.venv/bin/python scripts/backup_bigquery_to_huggingface.py \
  --verify backups/bigquery/20260818T120000Z
```

Luo ensimmäisellä kerralla yksityinen bucket ja lähetä tarkistettu pilotti:

```bash
.venv/bin/python scripts/backup_bigquery_to_huggingface.py \
  --upload backups/bigquery/20260818T120000Z \
  --hf-bucket NAMESPACE/budjettihaukka-bigquery-backups \
  --create-hf-bucket
```

Täysi snapshot:

```bash
.venv/bin/python scripts/backup_bigquery_to_huggingface.py \
  --hf-bucket NAMESPACE/budjettihaukka-bigquery-backups
```

Siirto ei käytä `--delete`-toimintoa. Etäkopio tarkistetaan tiedostopolun ja
koon perusteella ja siitä kirjoitetaan `huggingface_upload_receipt.json`.
Paikallinen eheys varmistetaan aina SHA-256-tiivisteillä.

## Ensimmäinen varmistettu snapshot

Ensimmäinen täysi snapshot luotiin 18.8.2026 ja tallennettiin yksityiseen
buckettiin
[`hjuntunen/budjettihaukka-bigquery-backups`](https://huggingface.co/buckets/hjuntunen/budjettihaukka-bigquery-backups):

- snapshot: `20260818T201635Z`;
- 71 objektia, joista 29 fyysistä taulua;
- 9 129 869 vietyä riviä;
- 5 597 230 472 tavua BigQueryn loogista dataa;
- 192 977 804 tavua snapshot-tiedostoissa;
- arkisto: `budjettihaukka-bigquery-20260818T201635Z.tar.gz`;
- arkiston SHA-256:
  `f5ee4e08e85aec971dfa7d44113a003d06a87a7f07f39f1a1c984fe2ccc8d499`.

Etäarkisto ladattiin siirron jälkeen takaisin paikallisesti. Ladatun tiedoston
SHA-256 täsmäsi lähdearkistoon, joten päästä päähän -eheystarkistus oli PASS.
Bucketissa ovat lisäksi `.sha256`-tiedosto ja varmennuksen `.receipt.json`.

## Säilytys ja palautus

Aluksi säilytetään kuusi viikkoversiota ja kolme kuukausiversiota. Poistot
tehdään vasta, kun uudemman snapshotin etä- ja paikallistarkistus ovat PASS.
Hugging Face -bucket ei tarjoa objektiversiointia, joten samaa snapshot-polun
nimeä ei käytetä uudelleen.

Palautuksessa snapshot ladataan ensin paikallisesti, SHA-256-tiivisteet
tarkistetaan ja Parquet-shardit ladataan BigQueryyn uutena versiona. Näkymät
rakennetaan GitHubissa versionoiduista SQL-määrityksistä ja snapshotiin
tallennettuja view-SQL-tiedostoja käytetään riippumattomana tarkistuskopiona.

Verkkolatauksella tehdyn arkiston paikallinen tarkistus ja purku:

```bash
shasum -a 256 -c budjettihaukka-bigquery-20260818T201635Z.tar.gz.sha256
tar -xzf budjettihaukka-bigquery-20260818T201635Z.tar.gz
.venv/bin/python scripts/backup_bigquery_to_huggingface.py \
  --verify 20260818T201635Z
```

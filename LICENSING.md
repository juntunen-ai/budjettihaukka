# Budjettihaukan julkinen lisenssijako

Versio 3.0.0 on julkisen repositorion ainoa julkaisu. Aiemmat julkaisut
ja niiden historia on poistettu tästä repositoriosta.

## Avoin koodi

Sovelluksen kuori on avointa lähdekoodia lisenssillä **GPL-3.0-only**.
Teksti on tiedostossa [`LICENSES/GPL-3.0.txt`](LICENSES/GPL-3.0.txt).

Avoimia polkuja ovat:

- `frontend/`
- `api/`
- `infra/`
- `.github/`
- `config.py`
- `scripts/deploy_firebase.sh`
- `Dockerfile`
- `requirements.txt`, `requirements-api.txt`, `requirements-backup.txt`
- `firebase.json`, `firestore.rules`, `firestore.indexes.json`, `.firebaserc`
- `.dockerignore`, `.gcloudignore`, `.gitignore`, `.env.example`
- `README.md`, `CHANGELOG.md`, `docs/releases/`
- repositorion juuren HTML-visualisoinnit

Kolmannen osapuolen kirjastot pysyvät omilla lisensseillään.

Julkinen kuori ei sisällä analyysimoottoria. `/v1/analyze` vastaa
501-tilalla. Käyttöönottoskripti kieltäytyy viemästä tätä puuta
tuotantoon, koska se korvaisi toimivan rajapinnan kuorella.

## Julkinen data

Hakemisto `data/` on julkista aineistoa: koottuja viitesarjoja ja
lähteiden skeemakuvauksia. Viranomaisten omat luvut pysyvät lähteen
ehdoissa. Tämä lisenssi ei siirrä niitä Budjettihaukan omaisuudeksi.

## Datan käsittely ei ole julkinen

Seuraavia ei ole tässä repositoriossa eikä sen Git-historiassa:

- analyysimoottori
- lataajat ja niiden testit
- ontologia ja käsittelysäännöt
- SQL-muunnokset
- semanttisen rikastuksen johdannaiset
- Streamlit-käyttöliittymä, koska se on sidottu moottoriin

Niitä koskee [`LICENSES/LicenseRef-Budjettihaukka-Data-1.0.txt`](LICENSES/LicenseRef-Budjettihaukka-Data-1.0.txt).
Se ei ole avoin lisenssi. Aineistoa ei jaeta tästä repositoriosta.
Muusta käytöstä: `harri@juntunen.ai`.

## Kontribuutiot

Tähän julkiseen repositorioon tarjotut muutokset lisensoidaan
GPL-3.0-only-lisenssillä. Muutos ei anna oikeutta datan käsittelyyn.

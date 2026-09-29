# Budjettihaukka

**Nykyinen julkinen julkaisu: v3.0.0 (29.9.2026).**

Tuotantopalvelu: [valtion-budjetti-data.web.app](https://valtion-budjetti-data.web.app)

Budjettihaukka on talouspoliittisen tiedon selaussovellus. Tämä julkinen
repositorio sisältää sovelluksen avoimen koodin ja julkisen datan.
Datan käsittely ei kuulu tähän lähdekoodiin.

Avoin kuori:

- React/ECharts-käyttöliittymä
- FastAPI-rajapinta, terveystarkistus ja Google-kirjautuminen
- Firebase Hosting- ja Cloud Run -infran kuvaus

Julkinen rajapinta käynnistyy, mutta `/v1/analyze` ei suorita analyysiä.
Analyysimoottori, lataajat, ontologia ja SQL eivät ole tässä
repositoriossa. Tuotannossa oleva palvelu on aiemmin käyttöönotettu
sovellus, ei tämä puu. Älä aja `scripts/deploy_firebase.sh` tästä
puusta: skripti kieltäytyy, koska moottori puuttuu.

Julkinen data on hakemistossa `data/`. Viranomaisten omat aineistot
pysyvät lähteiden ehdoissa.

Lisenssijako on tiedostossa [LICENSING.md](./LICENSING.md). Koodi on
GPL-3.0-only. Datan käsittely on erillinen, ei-avoin lisenssi, eikä sen
lähdekoodia jaeta täältä.

## Osallistu

Avoimen kuoren issuet ja muutosehdotukset ovat tervetulleita
GPL-3.0-onlyn ehdoilla. Datan käsittelyyn tämä repositorio ei anna
oikeutta.

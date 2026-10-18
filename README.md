# Budjettihaukka

**Nykyinen julkinen julkaisu: v3.0.0 (29.9.2026).**

Tuotantopalvelu: [valtion-budjetti-data.web.app](https://valtion-budjetti-data.web.app)

Vuoden 2027 vaihtoehtobudjetin kirjanpidollinen vertailu on sivulla
`liberaali-vaihtoehtobudjetti-2027.html`. Sen korjatuilla luvuilla varsinainen
alijäämä on 1 644 645 000 € ja rahoitustarve miljoonan euron
velanlyhennysvarauksen kanssa 1 645 645 000 €. Vuoden 2026 skenaarioarvio
säilyy erillisenä. Vuoden 2027 näkymä ei sisällä uutta makrosimulaatiota.

Julkinen momenttiaineisto on tiedostossa
`data/reference/liberaali_vaihtoehtobudjetti/vaihtoehtobudjetti_2027_v1.json`.
Sen voi muodostaa paikallisesta korjatusta lähteestä komennolla
`python scripts/build_liberaali_2027.py --csv /polku/korjattu.csv`.
Muodostus tarkistaa summat ja julkaisee vain erikseen valitut tunniste- ja
euromääräkentät. Sisäinen työtaulukko ei kuulu tähän repositorioon.

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

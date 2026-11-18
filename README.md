# Budjettihaukka

**Nykyinen julkinen julkaisu: v4.0.0 (marraskuu 2026).** Julkaisutiedot:
[docs/releases/v4.0.0.md](./docs/releases/v4.0.0.md). Muutokset:
[CHANGELOG.md](./CHANGELOG.md).

Tuotantopalvelu: [valtion-budjetti-data.web.app](https://valtion-budjetti-data.web.app)

## Julkinen MCP-palvelin

Budjettihaukan luotettu ydinaineisto on minkä tahansa kielimallin käytössä
julkisen MCP-palvelimen kautta:

`https://budjettihaukka-mcp-524401221130.europe-west1.run.app/mcp`

Palvelin ei vaadi kirjautumista eikä API-avainta. Sen voi lisätä mihin
tahansa Streamable HTTP:tä tukevaan MCP-asiakkaaseen, esimerkiksi Claude
Codessa:

```bash
claude mcp add --transport http budjettihaukka https://budjettihaukka-mcp-524401221130.europe-west1.run.app/mcp
```

Aineisto on Valtiokonttorin kuukausittainen talousarvion toteuma
2006–2026, hyväksytty versio `core_20261116_v3`. Palvelin vain lukee.
Rajat ovat yhteiset kaikille käyttäjille: 60 kutsua minuutissa. Tuloksissa
on pakolliset datan laatua koskevat huomiot (esim. osavuosi, puuttuvat
jaksot, rakennemuutokset). Niitä ei saa jättää pois tuloksen esityksestä.

MCP-palvelimen koodi ei kuulu tähän repositorioon. Sitä ylläpidetään
erikseen samoin kuin analyysimoottoria.

## Vaihtoehtobudjetti 2027

Vuoden 2027 vaihtoehtobudjetin kirjanpidollinen vertailu on sivulla
`frontend/public/liberaali-vaihtoehtobudjetti-2027.html`, ja sovelluksen
ylävalikossa on linkki siihen. Sen korjatuilla luvuilla varsinainen
alijäämä on 1 644 645 000 € ja rahoitustarve miljoonan euron
velanlyhennysvarauksen kanssa 1 645 645 000 €. Vuoden 2026 skenaarioarvio
säilyy erillisenä. Vuoden 2027 näkymä ei sisällä uutta makrosimulaatiota.

Julkinen momenttiaineisto on tiedostossa
`data/reference/liberaali_vaihtoehtobudjetti/vaihtoehtobudjetti_2027_v1.json`.
Sen voi muodostaa paikallisesta korjatusta lähteestä komennolla
`python scripts/build_liberaali_2027.py --csv /polku/korjattu.csv`.
Muodostus tarkistaa summat ja julkaisee vain erikseen valitut tunniste- ja
euromääräkentät. Sisäinen työtaulukko ei kuulu tähän repositorioon.

## Tämä repositorio

Budjettihaukka on talouspoliittisen tiedon selaussovellus. Tämä julkinen
repositorio sisältää sovelluksen avoimen koodin ja julkisen datan.
Datan käsittely ei kuulu tähän lähdekoodiin.

Avoin kuori:

- React/ECharts-käyttöliittymä
- FastAPI-rajapinta, terveystarkistus ja Google-kirjautuminen
- Firebase Hosting- ja Cloud Run -infran kuvaus
- npm audit -portti (`scripts/check_npm_audit.py`) määräaikaisine
  poikkeuksineen (`frontend/npm-audit-exceptions.json`)

Julkinen rajapinta käynnistyy, mutta `/v1/analyze` ei suorita analyysiä.
Analyysimoottori, lataajat, ontologia ja SQL eivät ole tässä
repositoriossa. Tuotanto otetaan käyttöön erillisestä yksityisestä
repositoriosta, ei tästä puusta. Älä aja `scripts/deploy_firebase.sh` tästä
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

# Muutosloki

Tämä tiedosto kuvaa Budjettihaukan julkisen repositorion muutokset.
Versiot noudattavat semanttista versionumerointia.

## [4.0.0] – 2026-11

### Lisätty

- Vaihtoehtobudjetti 2027 -sivu ja sen momenttiaineisto julkaisussa;
  linkki sovelluksen ylävalikossa.
- Sovelluksen versio alapalkissa („Budjettihaukka v4.0.0”).
- Ohjaus käännösaikana asetettuun kanoniseen osoitteeseen
  (`VITE_CANONICAL_ORIGIN`) uudelleenohjauskirjautumista varten.
- npm audit -portti `scripts/check_npm_audit.py` määräaikaisine,
  vain kehitysriippuvuuksia koskevine poikkeuksineen
  (`frontend/npm-audit-exceptions.json`) ja testeineen.
- README: luotettu ydinaineisto julkisen MCP-palvelimen kautta. Palvelimen
  koodi ei kuulu tähän repositorioon.
- Julkaisurajan testit MCP-palvelimelle ja datan käsittelyn kerroksille.

### Muutettu

- Sovelluskuoren versio 4.0.0 (käyttöliittymä ja FastAPI-kuori).
  `/v1/analyze` vastaa edelleen 501.
- 2027-sivun muodostin kirjoittaa vain hakemistoon `frontend/public/`.
- `firebase.json`: juuriosoitetta ei säilötä välimuistiin.
- Lukitustiedosto: re2js 2.10.0 korjaa haavoittuvuuden
  GHSA-gg6h-894w-4m9c (korkea, tuotantoriippuvuus Firebasen kautta).
- `gcp-creds.json` pois Cloud Buildin ja Dockerin konteksteista.

### Poistettu

- GitHub Pages -workflow ja käyttämätön `api/client.py`.
- Politiikan kasvujarru- ja velka tunnissa -sivut sekä niiden viitedata.
- Google Sheets- ja bq CLI -manifestit.
- Vaihtoehtobudjettisivujen juurikopiot; tarjoillut sivut ovat
  hakemistossa `frontend/public/`.
- Streamlit-asetukset sekä `streamlit`- ja `altair`-riippuvuudet.

### Tunnetut rajoitteet

- npm audit -poikkeus GHSA-fc5q-39xc-vjpf (31.12.2026 asti) ei vastaa
  enää mitään löydöstä, koska GitHub veti tiedotteen pois 18.11.2026.
- Julkista kuorta ei viedä tuotantoon; tuotanto otetaan käyttöön
  yksityisestä repositoriosta.

## [3.0.0] – 2026-09-29

### Muutettu

- Julkinen repositorio sisältää vain sovelluksen avoimen koodin ja
  julkisen datan.
- Analyysimoottori, lataajat, ontologia, SQL ja muu datan käsittely
  eivät ole tässä lähdekoodissa.
- Aiemmat julkiset julkaisut, tagit ja historia on poistettu tästä
  GitHub-repositoriosta. Julkaisuhetkellä tämä oli ainoa julkinen julkaisu.
- Julkinen API tarkistaa edelleen Google-kirjautumisen. `/v1/analyze`
  ja kysymyskirjaston luku vastaavat 501, koska moottori ei kuulu
  julkaistuun koodiin.
- Käyttöönottoskripti ei vie tätä puuta tuotantoon.

### Tunnetut rajoitteet

- Tuotantopalvelu osoitteessa valtion-budjetti-data.web.app on aiemmin
  käyttöönotettu sovellus. Sitä ei rakenneta uudelleen tästä puusta.
- Viranomaisten luvut pysyvät lähteiden omissa käyttöehdoissa.

[4.0.0]: https://github.com/juntunen-ai/budjettihaukka/releases/tag/v4.0.0
[3.0.0]: https://github.com/juntunen-ai/budjettihaukka/releases/tag/v3.0.0

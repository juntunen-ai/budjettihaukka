# Muutosloki

Tämä tiedosto kuvaa Budjettihaukan julkisen repositorion muutokset.
Versiot noudattavat semanttista versionumerointia.

## [3.0.0] – 2026-09-29

### Muutettu

- Julkinen repositorio sisältää vain sovelluksen avoimen koodin ja
  julkisen datan.
- Analyysimoottori, lataajat, ontologia, SQL ja muu datan käsittely
  eivät ole tässä lähdekoodissa.
- Aiemmat julkiset julkaisut, tagit ja historia on poistettu tästä
  GitHub-repositoriosta. Tämä on ainoa julkinen julkaisu.
- Julkinen API tarkistaa edelleen Google-kirjautumisen. `/v1/analyze`
  ja kysymyskirjaston luku vastaavat 501, koska moottori ei kuulu
  julkaistuun koodiin.
- Käyttöönottoskripti ei vie tätä puuta tuotantoon.

### Tunnetut rajoitteet

- Tuotantopalvelu osoitteessa valtion-budjetti-data.web.app on aiemmin
  käyttöönotettu sovellus. Sitä ei rakenneta uudelleen tästä puusta.
- Viranomaisten luvut pysyvät lähteiden omissa käyttöehdoissa.

[3.0.0]: https://github.com/juntunen-ai/budjettihaukka/releases/tag/v3.0.0

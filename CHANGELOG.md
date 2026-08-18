# Muutosloki

Tämä tiedosto kuvaa Budjettihaukan käyttäjille ja ylläpitäjille merkittävät
muutokset. Versiot noudattavat semanttista versionumerointia.

## [2.0.0] – 2026-08-18

### Lisätty

- Visualisointivalmis BigQuery-martti, lähde- ja mittarirekisterit sekä
  sopimustestit.
- Virallisiin snapshotteihin perustuvat rikastukset tilinpäätöksille,
  deflaattoreille, alueille, avustuksille ja sote-mittareille.
- Kuntatalouden KTAS-snapshot, semanttinen malli ja regressiotestit.
- Selkokieliset staattiset visualisointidemot sote-palveluista,
  perusterveydenhuollosta, Vantaa–Keravasta ja lastensuojelusta.
- BigQueryn kirjoittamaton operatiivinen auditointi, joka tarkistaa objektien
  kyseltävyyden, ajantasaisuuden ja vanhenemisriskin.
- Visualisointien lähteistys-, tulkinta-, saavutettavuus- ja julkaisuohje.

### Muutettu

- Aktiivisen GCP-projektin oletus on `budjettihaukka-gpt` ja raakadataobjektin
  oletus `valtiontalous_raw`.
- Visualisointievaluointi ja uudet data-aineistojen regressiotestit ovat
  estäviä CI-portteja.
- BigQueryn DQ-tarkistus tukee sekä legacy- että normalisoitua raakadataa ja
  käsittelee numeeriset sarakkeet tyyppiturvallisesti.
- Vertex AI:n valinnainen kirjasto ladataan vasta sitä tarvittaessa.

### Korjattu

- `TakpMrL` tulkitaan yksiselitteisesti määrärahalajiksi eikä alamomentiksi.
- Alamomenttikyselyt epäonnistuvat turvallisesti, kunnes dokumentoitu
  johtamissääntö ja virallisen tilikartan validointi ovat käytössä.
- Visualisointien puolustusmenojen golden-odotukset vastaavat nykyistä
  hallinnonalatrendiä.

### Tunnetut operatiiviset rajoitteet

- Tuotannon semantic-kerros päättyi auditointihetkellä toukokuuhun 2026,
  vaikka virallinen lähde sisälsi heinäkuun. Datan päivitys on P0-jatkotoimi.
- BigQuery-sandboxin 60 päivän oletusvanheneminen uhkaa 38 objektia 21 päivän
  sisällä auditoinnista. Ajastettu uudelleenrakennus tai TTL:n poisto on
  tehtävä ennen määräaikoja.
- Kuntatalouden 5 lähdetaulua ja 7 semanttista näkymää ovat vielä paikallisessa
  snapshotissa, eivät tuotannon BigQueryssä.
- Vuoden 2025 sote-aineisto on osittainen, eikä kuvailevista vertailuista pidä
  tehdä syy–seurauspäätelmiä.

[2.0.0]: https://github.com/juntunen-ai/budjettihaukka/releases/tag/v2.0.0

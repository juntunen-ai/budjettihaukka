# Muutosloki

Tämä tiedosto kuvaa Budjettihaukan käyttäjille ja ylläpitäjille merkittävät
muutokset. Versiot noudattavat semanttista versionumerointia.

## [2.2.0] – 2026-08-27

### Lisätty

- Firebase-tuotanto projektissa `valtion-budjetti-data`: Hosting, Cloud Run,
  Firestore, Artifact Registry, Secret Manager ja vähimmän oikeuden
  palvelutili määritellään Terraformilla.
- Sama-origin Hosting-rewritet FastAPI:n `/v1/**`- ja `/health`-reiteille
  sekä muuttumattomaan image-digestiin perustuva käyttöönottoskripti.
- Firestore-pohjainen kysymyskirjasto, istuntotunniste ja tietosuojahuomautus.
  Selain ei saa suoraa Firestore-luku- tai kirjoitusoikeutta, ja admin-API
  vaatii Secret Managerissa säilytettävän avaimen.
- Historiallisia talous-, velka-, hallituskausi-, tuottavuus- ja
  politiikkaskenaarioiden aineistoja, visualisointeja ja regressiotestejä.
- Liberaalien vaihtoehtobudjetin ja historiallisen vastelaskelman
  dokumentoidut malliaineistot ja visualisoinnit.

### Muutettu

- React/Vite/ECharts-frontend on ensisijainen verkkokäyttöliittymä;
  Streamlit säilyy paikallisena legacy- ja kehityskäyttöliittymänä.
- Frontend käyttää tuotannossa Firebase Hostingin sama-origin API:a ja
  paikallisesti Viten proxyä.
- Apache ECharts päivitettiin versioon 6.1.0 XSS-korjauksen vuoksi.
- API palauttaa varmennustilan myös Pydantic-vastauksessa ja julkaisee
  revision sisältävän terveysvastauksen.

### Turvallisuus ja kustannusten hallinta

- Firestore-säännöt estävät kaikki suorat selainoperaatiot.
- Cloud Run skaalautuu nollaan ja enintään kahteen instanssiin.
- BigQuery-kyselyt dry-run-tarkistetaan ja rajataan oletuksena 1 gigatavuun.
- Frontendin tuotantoriippuvuuksissa ei ole julkaisuhetkellä tunnettuja
  `npm audit` -haavoittuvuuksia.

### Tunnetut rajoitteet

- Tuotannon analyysi-API on julkinen. Instanssi- ja kyselyrajat hillitsevät
  kustannuksia, mutta erillinen käyttäjäkohtainen rate limit tai App Check
  ei vielä kuulu tähän julkaisuun.
- Kysymyskirjastoon tallennetaan käyttäjän kysymys ja analyysin tekninen
  lopputulos. Käyttöliittymä pyytää olemaan kirjoittamatta henkilötietoja.
- Politiikkaskenaariot ja kontrafaktuaalit ovat ehdollisia mallilaskelmia,
  eivät toteutuneesta datasta yksin tunnistettuja kausaalivaikutuksia.

## [2.1.0] – 2026-08-21

### Lisätty

- Eheystarkistettu BigQuery-varmistus Hugging Facen yksityisiin Storage
  Bucketeihin: sharded Zstandard-Parquet, rivimäärä- ja SHA-256-tarkistukset
  sekä muuttumattomat snapshot-prefiksit. Vienti on kirjoittamaton BigQueryä
  vasten ja kieltäytyy julkisista bucketeista.
- Tuki-, avustus- ja veroluokka-aineistot: nimihaku tuki- ja
  avustusmomentteihin 1998–2025, yritystuet veroluokittain sekä vuoden 2025
  valtion tuet, kaikki omilla regressiotesteillään.
- Kolme staattista visualisointisivua uusien aineistojen päälle
  (`tuki-avustus.html`, `yritystuet-veroluokat.html`,
  `valtion-tuet-2025.html`) rajaukset näkyvästi merkittyinä.
- VM-budjettisivuston semanttinen rikastus 2002–2025: katalogi-, segmentti- ja
  evidenssiaineistot laajennetussa ja rekursiivisessa muodossa.
- Vite- ja TypeScript-pohjainen frontend sekä manuaalisesti laukaistava
  GitHub Pages -julkaisutyönkulku.
- Kuntatalouden 5 lähdetaulua ja 7 semanttista näkymää on viety tuotannon
  BigQueryyn, joten operatiivinen auditointi on PASS ilman varoituksia.

### Muutettu

- Semantic-kerros ulottuu heinäkuuhun 2026 ja vastaa virallista lähdettä
  ilman viivettä. Visualisointimartti on rakennettu uudelleen kattaen
  vuodet 1998–2026.
- `.gitignore` estää salaisuuksien varmuuskopiot (`.env*.bak*`), Claude Coden
  työkansion ja Node-riippuvuudet sekä frontendin build-artefaktit.

### Korjattu

- Kuntatalousaineiston `reporting_period` pinnataan latauksessa STRING:iksi.
  CSV-autodetect tyypitti sen INT64:ksi aina kun snapshotissa oli vain
  nelinumeroisia vuosia, jolloin `analytics_municipal_finance_quality_v1`
  ei kääntynyt. Tämän vuoksi kuntatalousmarttia ei aiemmin voinut ottaa
  käyttöön dokumentoidulla komennolla.

### Tunnetut operatiiviset rajoitteet

- BigQuery-sandboxin oletusvanheneminen on edelleen 59 päivää ja koskee
  kaikkia dataset-objekteja. Hugging Face -varmistus lievittää riskiä, mutta
  ei poista sitä: ajastettu uudelleenrakennus tai laskutuksen käyttöönotto on
  yhä tekemättä. Aikaisin vanheneminen on 25.9.2026.
- Vuoden 2025 sote-aineisto on osittainen, eikä kuvailevista vertailuista pidä
  tehdä syy–seurauspäätelmiä.
- Tukimomenttien nimihaku ei ole virallinen tukirekisteri eikä sisällä
  verotukia.

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

[2.1.0]: https://github.com/juntunen-ai/budjettihaukka/releases/tag/v2.1.0
[2.0.0]: https://github.com/juntunen-ai/budjettihaukka/releases/tag/v2.0.0
[2.2.0]: https://github.com/juntunen-ai/budjettihaukka/releases/tag/v2.2.0

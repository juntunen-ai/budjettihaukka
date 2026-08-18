# BigQuery- ja järjestelmäauditointi 18.8.2026

## Yhteenveto

BigQueryn nykyiset 71 objektia ovat rakenteellisesti käytettävissä: jokainen
taulu ja näkymä läpäisi `SELECT * ... LIMIT 0` -kuivaharjoittelun. Visualisointimartin
laatukysely läpäisi kaikki portit 61 425 faktarivillä.

Kokonaisuus ei silti ole tuotannollisesti kunnossa ilman jatkotoimia. Valtion
kuukausidatan semantic-kerros päättyy toukokuuhun 2026, vaikka virallinen
Valtiokonttorin rajapinta julkaisee jo kesä- ja heinäkuun. Datasetin 60 päivän
vanhenemisasetus voi lisäksi poistaa lähdetaulut ja niiden päällä olevat näkymät
ilman ajastettua uudelleenrakennusta.

## Tarkastettu ympäristö

| Kohde | Havainto |
|---|---|
| Projekti | `budjettihaukka-gpt` |
| Dataset | `valtiodata`, `europe-west1` |
| BigQuery-objektit | 71/71 kuivaharjoitteli onnistuneesti |
| Semantic-fakta | 7 537 667 riviä |
| Semantic-aikaväli | 1/1998–5/2026 |
| Virallisen lähteen saatavuus | 1/2026–7/2026 |
| Visualisointimartti | PASS, 61 425 faktariviä |
| Datasetin oletusvanheneminen | 60 päivää tauluille ja partitioille |

Virallisen kuukausilistan lähde:
`https://api.tutkihallintoa.fi/valtiontalous/v1/budjettitalousvuosikuukausi`.

## BigQuery-laatutulokset

### Semantic-kerros

| Tarkistus | Tulos | Havainto |
|---|---|---:|
| Vuosi ja kuukausi | PASS | 0 virhettä |
| Nettokertymän parsinta | PASS | 0 virhettä |
| Puuttuva hallinnonala | PASS kynnyksen puitteissa | 15 203 riviä, 0,20 % |
| Puuttuva momentti | PASS kynnyksen puitteissa | 4 271 riviä, 0,057 % |
| Sama `row_fingerprint` useasti | WARN | 3 600 riviä, 0,048 % |
| Puuttuvat kuukaudet ennen uusinta vuotta | PASS | 0 kuukautta |
| Lähdeskeeman poikkeama | PASS | 0 poikkeamaa |
| Ajantasaisuus | **FAIL** | 109 päivää viimeisimmästä kuukaudesta |

### Normalisoitu raakakerros

Raakadatan tarkistin ei aiemmin tunnistanut aktiivisen `valtiontalous_raw`-taulun
normalisoituja sarakenimiä, ja legacy-taulun numeerinen sarake kaatoi `TRIM`-
kutsun. Tarkistin korjattiin tunnistamaan sekä legacy- että normalisoitu raakamuoto
ja muuntamaan tarkistettavat arvot merkkijonoiksi tyyppiturvallisesti.

Korjauksen jälkeen raakakerroksen rakenne- ja parsintaportit läpäisivät. Laajalla
luonnollisella avaimella havaittiin 6 114 päällekkäistä riviä (0,081 %), mikä jää
nykyisen virherajan alle mutta vaatii lähde- ja ingest-duplikaattien erottelun.

### Visualisointimartti

Laatukyselyn tulos oli PASS:

- ei validoimattomia alamomentteja;
- ei määrärahalajeja alamomentteina;
- ei virheellisesti täsmäytetyiksi merkittyjä rivejä;
- ei valmiiksi merkittyjä osavuoden budjettirivejä;
- ei duplikaatteja makrosarjan vuosiavaimessa;
- ei puuttuvia pakollisia mittari- tai lähdemääritelmiä.

## Puuttuvat tuotanto-objektit

Paikallinen kuntataloussnapshot läpäisee regressiotestinsä, mutta seuraavia
objekteja ei auditointihetkellä ollut BigQueryssä:

**Lähdetaulut (5):**

- `municipal_finance_taxonomy_index_v1`
- `municipal_finance_catalog_v1`
- `municipal_finance_ktas_taxonomy_v1`
- `municipal_finance_ktas_core_v1`
- `municipal_finance_document_manifest_v1`

**Semanttiset näkymät (7):**

- `dim_municipal_finance_source_v1`
- `dim_municipal_finance_indicator_v1`
- `analytics_municipal_finance_catalog_v1`
- `analytics_municipal_budget_v1`
- `analytics_municipal_budget_revision_v1`
- `analytics_municipal_finance_coverage_v1`
- `analytics_municipal_finance_quality_v1`

Tämän vuoksi kuntatalousvisualisointi on toistaiseksi paikalliseen snapshotiin
perustuva demonstraatio, ei tuotannon BigQuery-ominaisuus.

## Vanhenemisriski

Datasetin oletusvanheneminen on 5 184 000 000 ms eli 60 päivää. Auditointihetken
metatiedon perusteella 38 objektia vanhenee 21 päivän kuluessa. Esimerkiksi:

- `valtiontalous_raw` vanhenee 5.9.2026;
- `valtiontalous_semantic_current` vanhenee 7.9.2026;
- `analytics_fiscal_yearly_core_v1` vanhenee 14.10.2026.

Näkymän onnistunut kuivaharjoittelu ei suojaa sitä lähdetaulun vanhenemiselta.
Ilman laskutuksen käyttöönottoa tarvitaan ajastettu ingest, kerrosten
uudelleenrakennus ja hälytys selvästi alle 60 päivän välein.

## Korjaustarpeet

| Prioriteetti | Korjaus | Hyväksymisehto |
|---|---|---|
| P0 | Lataa kesä- ja heinäkuu 2026 sekä rakenna DQ-, semantic- ja visualisointikerrokset uudelleen versiona | Uusin `period_date` vastaa uusinta virallista kuukautta ja molemmat DQ-ajot ovat PASS/WARN ilman uusia FAIL-tuloksia |
| P0 | Estä BigQuery-objektien tahaton vanheneminen | Laskutus poistaa TTL:n tai alle 60 päivän ajastettu putki on käytössä ja hälyttää epäonnistumisesta |
| P1 | Lataa kuntatalouden 5 lähdetaulua ja rakenna 7 näkymää | `analytics_municipal_finance_quality_v1` näyttää kaikille riveille `passed = TRUE` |
| P1 | Erota julkaistut lähdeduplikaatit ingestin tuottamista duplikaateista | Duplikaattiraportti näyttää alkuperän ja estää saman lähde-URL:n lataamisen kahdesti |
| P1 | Lisää uusi `scripts/audit_bigquery_operational_state.py` ajastettuun ylläpitoon | Tarkistus ajetaan vähintään viikoittain ja sen FAIL-tulos hälyttää |
| P2 | Korvaa kovakoodattu sovelluksen ylävuosi (`DATA_MAX_YEAR = 2025`) datasta johdetulla täyden/osittaisen vuoden saatavuudella | Käyttäjä voi pyytää vuoden 2026 osavuotta, ja vastaus kertoo näkyvästi havaittujen kuukausien määrän |
| P2 | Lisää kustannusrajattu kuukausiaggregaatti | Kuukausikyselyn fallback alittaa 1 GB oletusrajan; nykyinen raakakysely kuivaharjoittelee noin 1,77 GB |

## Tässä auditoinnissa tehdyt paikalliset korjaukset

- aktiivisen projektin oletukseksi asetettiin `budjettihaukka-gpt`;
- aktiivisen raakadataobjektin oletukseksi asetettiin `valtiontalous_raw`;
- `.env.example` ja README yhdenmukaistettiin aktiivisen ympäristön kanssa;
- BigQuery DQ -skripti korjattiin tyyppiturvalliseksi;
- normalisoidulle raakadatataululle lisättiin oma tunnistus ja regressiotesti;
- raakakerroksen raportoitu enimmäisjakso korjattiin todelliseksi vuosi–kuukausiarvoksi;
- lisättiin kirjoittamaton operatiivinen auditointikomento objekteille, ajantasaisuudelle ja vanhenemiselle;
- Vertex AI:n valinnainen SDK siirrettiin laiskaksi importiksi, jotta deterministiset testit eivät odota sitä;
- visualisointievaluoinnin kaksi vanhentunutta puolustusmenojen golden-odotusta päivitettiin; 52/52 tapausta ja 23/23 kriittistä tapausta läpäisevät;
- visualisointien toteutus-, tulkinta- ja julkaisuohje koottiin tiedostoon `docs/visualizations.md`.

Auditointi ei muuttanut BigQueryn sisältöä eikä ajanut tuotannon korvaavia load-
tai `CREATE OR REPLACE` -operaatioita. Ajantasaisuuden ja TTL-riskin korjaaminen
vaatii erillisen hallitun tuotantoajon.

# Perusterveydenhuollon laatu ja ohjausriski 2020–2025

## Tarkoitus

Pilotti erottaa perusterveydenhuollon toiminnan määrän, laadun ja mahdolliset
ohjauksen ristiriidat. Aineisto muodostaa 24 maantieteellisen yksikön (koko maa ja 23
hyvinvointialuetta) ja kuuden vuoden paneelin. Puuttuva havainto säilyy
puuttuvana: sitä ei interpoloida, kopioida edelliseltä vuodelta eikä muuteta
nollaksi.

Pilotti ei ole paremmuusjärjestys eikä syy-seuraussuhteen osoittava
vaikuttavuustutkimus. Alueiden ikä-, sairastavuus- ja sosioekonomisia eroja ei
ole vielä vakioitu.

## Mittarit ja lähteet

| Ulottuvuus | Paneelin kenttä | Virallinen lähde | Ajallinen kattavuus |
|---|---|---|---|
| Reaalinen kustannus | `primary_care_cost_real_2024_eur_per_resident` | THL Sotkanet 3764 + 3766; Tilastokeskus 11m2 | 2020–2024 |
| Henkilöstö | `public_health_staff_per_10000` | THL Sotkanet 4604 | 2020–2023 |
| Lääkärikäynnit | `primary_care_doctor_visits_per_1000` | THL Sotkanet 1080 | 2020–2024 |
| Yli 7 päivää odottaneet | `primary_care_wait_over_7d_pct` | THL Sotkanet 6411 | 2021–2025 |
| Hoidon jatkuvuus | `primary_care_doctor_continuity_coci` | THL Sotkanet 5502 | 2020–2025 |
| Koettu nopea saatavuus | `experienced_fast_access_pct` | THL Sotkanet 5186 | 2020, 2022, 2024 |
| Tarpeeseen nähden riittämättömät lääkäripalvelut | `experienced_insufficient_doctor_services_pct` | THL Sotkanet 4909 | 2020, 2022, 2024 |
| Koettu terveys, keskitasoinen tai huonompi | `self_rated_health_mediocre_or_worse_pct` | THL Sotkanet 4333 | 2020, 2022, 2024 |
| Perusterveydenhuollon päivystyskäynnit | `primary_care_emergency_visits_per_1000` | THL Sotkanet 5081 | 2020–2025 |
| Vältettävissä olevat päivystykselliset sairaalahoidot | `avoidable_emergency_hospitalizations_per_100000` | THL Sotkanet 5587 | 2020–2025, ei Ahvenanmaata |

Kaikki Sotkanet-tiedot haetaan palvelun nykyisellä
hyvinvointialuejaolla. Vuosien 2020–2022 luvut kuvaavat kuntien järjestämää
toimintaa koottuna nykyisille hyvinvointialuerajoille. Vuosien 2023–2025 luvut
ovat hyvinvointialueiden järjestämää toimintaa. Vuoden 2023 kohdalla on siten
sekä todellinen uudistus että mahdollinen kirjaamis- ja luokituskatkos.

Henkilöstömittari kuvaa koko julkista terveydenhuoltoa, ei yksinomaan
perusterveydenhuoltoa. Hoitoonpääsymittari kuvaa toteutuneita käyntejä eikä
kaikkia palvelua tarvitsevia. Kyselymittarit ovat saatavilla vain joka toinen
vuosi. Vuoden 2025 aineistossa ei siksi ole laadun tasapainoindeksiä.

## Reaalinen kustannus

Perusterveydenhuollon nimellinen nettokäyttökustannus asukasta kohti on
Sotkanet-indikaattoreiden 3764 ja 3766 summa. Se muunnetaan vuoden 2024 euroiksi:

```text
reaalikustannus(y) = nimelliskustannus(y) × hintaindeksi(2024) / hintaindeksi(y)
```

Deflaattorina käytetään Tilastokeskuksen kuntatalouden julkisten menojen
hintaindeksiä, yhteensä (2015 = 100). Se on läpinäkyvä julkisten palvelujen
kustannuskehityksen vertailuluku, ei perusterveydenhuollolle räätälöity
deflaattori.

## Käyntituotos ei ole tehokkuus

`activity_per_real_cost` kertoo lääkärikäyntien määrän suhteessa
perusterveydenhuollon reaalikustannukseen. Sen indeksi on
`activity_per_real_cost_index_fi_2020_100`. Kenttää ei saa nimetä
tehokkuudeksi, kustannusvaikuttavuudeksi tai palveluarvoksi, koska käynti ei
kerro palvelutarpeen ratkeamisesta ja kustannuksia voi siirtyä muualle
hoitoketjuun.

Aiempi `service_value_index` on poistettu käytöstä. Sen tila on jokaisella
rivillä `retired_requires_whole_chain_value_data`. Palveluarvo julkaistaan vasta,
kun ratkaistun palvelutarpeen, potilasturvallisuuden ja koko hoitoketjun
kustannuksen aineistot ovat käytettävissä.

## Laadun tasapainoindeksi

Kaikki neljä osatekijää suunnataan niin, että suurempi luku on parempi, ja
suhteutetaan koko Suomen vuoden 2020 lähtötasoon 100:

1. lääkärikäyntien jatkuvuus (COCI),
2. riittävän nopeasti yhteyden saaneiden osuus,
3. lääkäripalvelut tarpeeseensa nähden riittäviksi kokeneiden osuus,
4. terveytensä hyväksi tai melko hyväksi kokevien osuus, joka johdetaan
   vähentämällä heikoksi tai keskitasoiseksi koettu terveys sadasta.

Laadun tasapaino on neljän indeksin yhtä painotettu geometrinen keskiarvo:

```text
laadun tasapaino = (jatkuvuus × koettu saatavuus × palvelujen riittävyys × koettu terveys)^(1/4)
```

Indeksi julkaistaan vain, jos kaikki neljä osatekijää ovat samalle alue-vuodelle
saatavilla. Käyntituotos, henkilöstö, odotusaika, päivystyskäynnit ja
vältettävissä olevat sairaalahoidot näytetään vastamittareina tai kontekstina,
ei laatupisteinä.

## Ohjausriskin säännöt

`primary_care_steering_risk_v1` vertaa vuosia 2022 ja 2024 ja nostaa
tarkistettavaksi neljä ristiriitaa:

1. `activity_quality_conflict`: käyntituotos kasvaa olennaisesti samalla kun
   jatkuvuus, koettu saatavuus tai palvelujen riittävyys heikkenee.
2. `wait_denominator_conflict`: toteutuneiden käyntien odotusosuus paranee,
   mutta väestön kokema saatavuus tai palvelujen riittävyys heikkenee.
3. `cost_shift_signal`: perusterveydenhuollon reaalikustannus laskee samalla kun
   päivystyskäynnit tai vältettävissä olevat sairaalahoidot lisääntyvät.
4. `continuity_countermeasure_breach`: jatkuvuus heikkenee olennaisesti.

Säännöt käyttävät lähdemetadatassa julkaistuja olennaisuusrajoja. Vahva signaali
tarkoittaa vähintään kahta yhtäaikaista ristiriitaa, seurattava yhtä. Signaali ei
todista väärää kannustinta, tarkoituksellista toimintaa tai syy-seuraussuhdetta.

Seuraavat tietovajeet säilytetään semanttisessa mallissa tyhjinä kenttinä:
hoitopyyntöjen kokonaismäärä, ilman ajanvarausta tai muuta ratkaisua jääneiden
osuus, keskeytyneet hoitopolut, mediaaniodotusaika ja riskivakioitu koko
hoitoketjun kustannus. Niitä ei korvata synteettisillä arvoilla.

## Tiedostot ja päivitys

- `official_primary_care_value_v1.csv`: lähteestä saadut ja dokumentoidusti
  johdetut havainnot lähdeviitteineen
- `primary_care_value_panel_v1.csv`: alue-vuosi-paneeli analyysiin
- `primary_care_value_panel_v1.json`: sama paneeli selaindemolle
- `primary_care_value_sources_v1.json`: lähteet, vuosikerrat ja laskentasäännöt
- `primary_care_steering_risk_v1.csv` ja `.json`: aluekohtaiset
  ohjausriskisignaalit ja niiden perusteet

Päivitys ja regressiotesti:

```bash
.venv/bin/python scripts/load_primary_care_value_reference.py
.venv/bin/python scripts/test_primary_care_value.py
```

Seuraava tuotantovaihe on tarvevakiointi vähintään iällä, sairastavuudella ja
sosioekonomisella asemalla sekä puuttuvien hoitopyyntö-, hoitopolku- ja
episodikustannustietojen hankinta.

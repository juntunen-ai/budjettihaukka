# Budjettihaukka – Tekoälypohjainen talouspoliittisen tiedon analysointisovellus

Budjettihaukka on avoimen lähdekoodin web-sovellus, jonka tarkoituksena on tuoda **talouspolitiikkaan liittyvä tieto** helposti saataville, analysoitavaksi ja visualisoitavaksi. Sovelluksen käyttäjä voi esittää kysymyksiä **luonnollisella kielellä**, ja tekoälyn avulla saa kansantaloudelliseen optimaalisuuteen ja empiiriseen taloustutkimukseen perustubia analyyseja. Tulokset voidaan näyttää taulukkoina, dynaamisina visualisointeina sekä analyyttisinä raportteina.

Projektin pitkän aikavälin tavoite on tukea kansalaisia, tutkijoita ja toimittajia tarjoamalla **tietopohjainen, helppokäyttöinen työkalu poliittisten ja taloudellisten päätösten arviointiin**.

---

## 🔍 Tärkeimmät ominaisuudet

- 💬 Luonnollisen kielen kyselyt (esim. "Paljonko koulutukseen budjetoitiin vuonna 2023?")
- 🔁 Automaattinen SQL-generointi Google Vertex AI:n avulla
- 📊 Dynaamiset visualisoinnit Streamlit-käyttöliittymässä
- 📚 Datan lähteenä mm. `www.tutkihallintoa.fi`, BigQuery (tässä vaiheessa. Tarkoitus on lisätä **luotettavaa** dataa ajan myötä)

---

## 🧠 Kehityssuunta

Tulevissa vaiheissa:

- Otetaan käyttöön **agenttimainen AI-kehys** (esim. LangChain tai Haystack)
- Kehitetään **kehittyneempiä analyysikyvykkyyksiä** (esim. regressio, klusterointi, trendit)
- Visualisointeja rakennetaan **suoraan luonnollisen kielen kysymysten pohjalta**
- Laajennetaan dataa tutkimusartikkeleihin, tilastoihin ja kansainvälisiin vertailuihin
- Lopullinen tavoite: **tarjota AI:n avulla syvällisiä näkemyksiä talouspolitiikasta**

---

## 🛠️ Teknologiat

- Python 3.13
- Streamlit
- Google Cloud Platform
  - BigQuery
  - Vertex AI
  - Cloud Storage
  - Firebase Hosting / Studio
- Docker (Cloud Run -käyttöönotto)
- Jupyter Workbench (prototyyppivaiheessa)

---

## 🚧 Nykytila

Prototyyppi on toimiva, mutta ei vielä luotettava kaikissa kyselyissä. Kehitys on käynnissä AI-agenttirakenteen suuntaan. Projektia rakentaa kehittäjä, jolla on rajoitettu kokemus koodaamisesta ja pilvipalveluista, mutta vahva ymmärrys ongelmakentästä ja tekoälyn soveltamisesta.

Koodia rakennetaan tekoälyapureiden (esim. ChatGPT) tuella vaihe vaiheelta — tavoite on **helppokäyttöinen ja läpinäkyvä järjestelmä**, jonka rakentaminen on dokumentoitu oppimisprosessina.

---

## 📄 Lisenssi

Tämä projekti on lisensoitu **GNU General Public License v3.0 (GPLv3)** mukaisesti.

Lue koko lisenssiteksti tiedostosta [`LICENSE`](./LICENSE).

---

## 🤝 Osallistu

Tämä projekti on avoin ideoille, ehdotuksille ja kontribuutioille.  
Voit tehdä forkkeja, issueita tai PR:itä – tai vain käyttää ja kertoa eteenpäin!

---

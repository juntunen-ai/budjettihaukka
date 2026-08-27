#!/usr/bin/env python3
"""Build a plain-language public visualization of the 2008-2025 backcast."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INPUT = (
    ROOT
    / "data"
    / "reference"
    / "liberaali_vaihtoehtobudjetti"
    / "historiallinen_vastelaskelma_v1.json"
)
ANALYSIS = (
    ROOT
    / "data"
    / "reference"
    / "liberaali_vaihtoehtobudjetti"
    / "analyysi_v2.json"
)
OUTPUT = ROOT / "liberaali-historiallinen-vastelaskelma.html"


TEMPLATE = r'''<!doctype html>
<html lang="fi">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta name="description" content="Selkokielinen, ehdollinen historiallinen laskelma Liberaalien politiikan julkistaloudellisista vaikutuksista vuosina 2008-2025.">
  <link rel="icon" href="data:,">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Archivo+Black&family=Source+Sans+3:ital,wght@0,400;0,600;0,700;0,800;1,600&display=swap" rel="stylesheet">
  <title>Jos politiikka olisi ollut toisenlaista? | Budjettihaukka</title>
  <style>
    :root {
      --ink: #151512;
      --paper: #f8f3e8;
      --paper-dark: #eae0cd;
      --white: #fffdf7;
      --yellow: #f7d94d;
      --yellow-soft: #fff0a7;
      --blue: #006ca8;
      --blue-soft: #b9def1;
      --green: #18733a;
      --green-soft: #cfe9d1;
      --red: #a92c22;
      --red-soft: #f5cbc5;
      --muted: #625d52;
      --line: #1515122b;
      --shadow: 7px 7px 0 var(--ink);
    }
    * { box-sizing: border-box; }
    html { scroll-behavior: smooth; }
    body { margin: 0; color: var(--ink); background: var(--paper); font: 18px/1.5 "Source Sans 3", sans-serif; }
    a { color: inherit; text-underline-offset: 3px; }
    button { font: inherit; }
    .wrap { width: min(1140px, calc(100% - 40px)); margin-inline: auto; }
    .mast { position: relative; overflow: hidden; border-bottom: 3px solid var(--ink); background: var(--yellow); }
    .mast::after { content: "?"; position: absolute; right: 3vw; bottom: -20vw; color: #fff4; font: clamp(16rem, 40vw, 36rem)/.7 "Archivo Black", sans-serif; }
    .mast-inner { position: relative; z-index: 1; padding: 22px 0 66px; }
    .topline { display: flex; align-items: center; justify-content: space-between; gap: 16px; }
    .brand, .tag { font-size: .86rem; font-weight: 800; letter-spacing: .08em; text-transform: uppercase; }
    .brand { display: inline-flex; align-items: center; gap: 9px; text-decoration: none; }
    .brand b { display: grid; width: 31px; height: 31px; place-items: center; border: 2px solid var(--ink); border-radius: 8px; background: var(--ink); color: var(--yellow); letter-spacing: 0; }
    .tag { padding: 6px 10px; border: 1.5px solid var(--ink); border-radius: 999px; background: var(--white); }
    h1, h2, h3, .big, .metric-number { font-family: "Archivo Black", "Arial Black", sans-serif; letter-spacing: -.045em; }
    h1 { max-width: 940px; margin: 70px 0 21px; font-size: clamp(3.25rem, 8.8vw, 7.4rem); line-height: .88; text-transform: uppercase; }
    h1 .outline { display: block; color: var(--white); -webkit-text-stroke: 2px var(--ink); text-shadow: 4px 4px 0 var(--ink); }
    .hero-text { max-width: 730px; margin: 0; font-size: clamp(1.16rem, 2.3vw, 1.43rem); font-weight: 700; }
    .disclosure { display: grid; grid-template-columns: auto 1fr; gap: 14px; max-width: 860px; margin-top: 29px; padding: 15px 18px; border: 2px solid var(--ink); background: #fff5c8; box-shadow: 4px 4px 0 var(--ink); }
    .disclosure b { font: .92rem/1.05 "Archivo Black", sans-serif; text-transform: uppercase; }
    .disclosure span { font-size: .94rem; line-height: 1.35; }
    .nav { position: sticky; z-index: 10; top: 0; overflow-x: auto; background: var(--ink); color: var(--white); }
    .nav .wrap { display: flex; min-width: max-content; }
    .nav a { padding: 12px 15px; color: inherit; font-size: .86rem; font-weight: 800; text-decoration: none; }
    .nav a:hover, .nav a:focus-visible { color: var(--ink); background: var(--yellow); }
    main { padding-bottom: 76px; }
    section { padding: 70px 0; border-bottom: 2px solid var(--ink); }
    .eyebrow { margin: 0 0 8px; color: var(--muted); font-size: .8rem; font-weight: 800; letter-spacing: .14em; text-transform: uppercase; }
    h2 { max-width: 910px; margin: 0 0 16px; font-size: clamp(2.15rem, 5vw, 4.55rem); line-height: .96; }
    h3 { margin: 0; font-size: 1.18rem; line-height: 1.14; }
    .lede { max-width: 760px; margin: 0; color: var(--muted); font-size: 1.12rem; }
    .case-switches { display: flex; flex-wrap: wrap; gap: 9px; margin: 27px 0 12px; }
    .case-switches button { padding: 9px 15px; border: 2px solid var(--ink); border-radius: 999px; color: var(--ink); background: var(--white); font-weight: 800; cursor: pointer; }
    .case-switches button[aria-pressed="true"] { background: var(--yellow); box-shadow: 3px 3px 0 var(--ink); transform: translate(-1px, -1px); }
    .case-note { margin: 0; color: var(--muted); font-size: .92rem; }
    .headline-grid { display: grid; grid-template-columns: 1.25fr .75fr; gap: 20px; margin-top: 28px; }
    .debt-answer, .growth-answer, .card { border: 2px solid var(--ink); background: var(--white); }
    .debt-answer { padding: clamp(23px, 4vw, 45px); background: var(--blue); color: var(--white); box-shadow: var(--shadow); }
    .debt-answer .small { display: block; max-width: 500px; font-size: 1rem; font-weight: 700; }
    .big { display: block; margin: 16px 0 8px; font-size: clamp(3.6rem, 9vw, 7rem); line-height: .82; }
    .debt-answer .explain { max-width: 540px; margin: 19px 0 0; font-size: 1.1rem; font-weight: 600; }
    .growth-answer { display: flex; flex-direction: column; justify-content: space-between; padding: 25px; background: var(--yellow-soft); }
    .growth-answer .metric-number { display: block; margin-top: 28px; font-size: clamp(3rem, 6vw, 4.6rem); line-height: .86; }
    .growth-answer p { margin: 15px 0 0; font-weight: 600; }
    .truth { margin-top: 20px; padding: 18px 20px; border-left: 7px solid var(--yellow); background: var(--paper-dark); }
    .truth strong { font-weight: 800; }
    .chart-card { margin-top: 27px; padding: 23px; border: 2px solid var(--ink); background: var(--white); }
    .chart-card h3 { font-size: 1.35rem; }
    .legend { display: flex; flex-wrap: wrap; gap: 10px 18px; margin-top: 11px; font-size: .9rem; font-weight: 700; }
    .legend span { display: inline-flex; align-items: center; gap: 7px; }
    .swatch { width: 15px; height: 15px; border: 1px solid var(--ink); }
    .chart { min-height: 360px; margin-top: 15px; }
    .chart svg { display: block; width: 100%; height: auto; overflow: visible; }
    .chart-note { margin: 10px 0 0; color: var(--muted); font-size: .88rem; }
    .micro-banner { display: grid; grid-template-columns: 190px 1fr; gap: 18px; margin-top: 26px; padding: 19px; border: 3px solid var(--ink); background: var(--red-soft); box-shadow: 5px 5px 0 var(--ink); }
    .micro-banner strong { font: 1rem/1.08 "Archivo Black", sans-serif; text-transform: uppercase; }
    .micro-banner p { margin: 0; }
    .micro-metrics { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; margin-top: 24px; }
    .micro-metric { padding: 18px; border: 2px solid var(--ink); background: var(--white); }
    .micro-metric b { display: block; font: clamp(1.65rem, 3vw, 2.7rem)/.95 "Archivo Black", sans-serif; letter-spacing: -.05em; }
    .micro-metric span { display: block; margin-top: 8px; color: var(--muted); font-size: .9rem; }
    .simple-steps { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 18px; margin-top: 29px; }
    .step { position: relative; padding: 22px; border: 2px solid var(--ink); background: var(--white); }
    .step-num { display: grid; width: 43px; height: 43px; place-items: center; margin-bottom: 26px; border: 2px solid var(--ink); border-radius: 50%; background: var(--yellow); font-weight: 800; }
    .step p, .card p { margin: 9px 0 0; color: var(--muted); }
    .balance { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 20px; margin-top: 28px; }
    .card { padding: 24px; }
    .card.good { background: var(--green-soft); }
    .card.caution { background: var(--red-soft); }
    .card ul { margin: 14px 0 0; padding-left: 20px; }
    .card li { margin-bottom: 7px; }
    .sisu { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-top: 27px; }
    .sisu .card:first-child { background: var(--yellow-soft); }
    .compare { display: grid; gap: 11px; margin-top: 18px; }
    .compare div { display: grid; grid-template-columns: 118px 1fr; gap: 10px; padding-top: 10px; border-top: 1px solid var(--line); }
    .compare b { font-size: .86rem; text-transform: uppercase; letter-spacing: .06em; }
    .compare span { color: var(--muted); }
    .source-grid { display: grid; gap: 10px; margin-top: 25px; }
    .source { display: grid; grid-template-columns: minmax(180px, .9fr) 2fr; gap: 18px; padding: 13px 0; border-top: 1px solid var(--line); }
    .source a { font-weight: 800; }
    .source span { color: var(--muted); }
    .footer { padding: 36px 0; color: var(--muted); font-size: .92rem; }
    @media (max-width: 800px) {
      .headline-grid, .sisu { grid-template-columns: 1fr; }
      .simple-steps { grid-template-columns: 1fr; }
      .micro-metrics { grid-template-columns: 1fr; }
    }
    @media (max-width: 620px) {
      body { font-size: 16px; }
      .wrap { width: min(100% - 28px, 1140px); }
      .mast-inner { padding-bottom: 48px; }
      .tag { display: none; }
      h1 { margin-top: 54px; font-size: clamp(2.35rem, 10.5vw, 3.25rem); }
      h1 .outline { -webkit-text-stroke-width: 1.4px; text-shadow: 3px 3px 0 var(--ink); }
      section { padding: 49px 0; }
      .disclosure { grid-template-columns: 1fr; }
      .micro-banner { grid-template-columns: 1fr; }
      .chart { min-height: 250px; overflow-x: auto; }
      .chart svg { width: 720px; max-width: none; }
      .balance { grid-template-columns: 1fr; }
      .source { grid-template-columns: 1fr; gap: 3px; }
      .compare div { grid-template-columns: 1fr; gap: 4px; }
    }
    @media (prefers-reduced-motion: reduce) { html { scroll-behavior: auto; } }
  </style>
</head>
<body>
  <header class="mast">
    <div class="wrap mast-inner">
      <div class="topline">
        <a class="brand" href="liberaali-vaihtoehtobudjetti-2026.html"><b>BH</b> Budjettihaukka</a>
        <span class="tag">Ehdollinen vastelaskelma 2008–2025</span>
      </div>
      <h1>Jos politiikka olisi ollut <span class="outline">toisenlaista?</span></h1>
      <p class="hero-text">Mallissa Liberaalien politiikkarakenteen pitkäjänteinen toteutus olisi jättänyt Suomen valtionhallinnolle vähemmän velkaa. Kasvu olisi voinut olla vahvempaa, mutta se riippuu talousoletuksista.</p>
      <div class="disclosure">
        <b>Tärkeä rajaus</b>
        <span>Tämä on vertailulaskelma, ei menneisyyden ennustus. Toteutunut data on fakta. Vaihtoehtoinen historia on mallin tuottama vastaus kysymykseen: “mitä jos?”</span>
      </div>
    </div>
  </header>

  <nav class="nav" aria-label="Sivun osiot"><div class="wrap"><a href="#tulos">Tulos</a><a href="#miten">Miten laskettu?</a><a href="#rajoitukset">Mitä se ei kerro?</a><a href="#tulonjako">Tulonjako</a><a href="#sisu">SISU-malli</a><a href="#lahteet">Lähteet</a></div></nav>

  <main>
    <section id="tulos"><div class="wrap">
      <p class="eyebrow">Valitse oletus</p>
      <h2>Velkaa vähemmän. Kasvu voisi olla vahvempaa.</h2>
      <p class="lede">Keskiskenaario on vertailupiste. Kokeile myös suotuisaa ja varovaista oletusta. Näin näet, mikä johtopäätös kestää epävarmuutta.</p>
      <div class="case-switches" aria-label="Valitse laskelman oletus">
        <button type="button" data-case="suotuisa" aria-pressed="false">Suotuisa</button>
        <button type="button" data-case="keskinen" aria-pressed="true">Keskinen</button>
        <button type="button" data-case="varovainen" aria-pressed="false">Varovainen</button>
      </div>
      <p id="case-note" class="case-note"></p>

      <div class="headline-grid">
        <article class="debt-answer">
          <span class="small">Mallin arvio: velkaa vähemmän vuonna 2025</span>
          <span id="debt-gap" class="big"></span>
          <span id="debt-explain" class="small"></span>
          <p class="explain">Tämä syntyy suorasta tasapainoparannuksesta, sen vaikutuksesta suhdanteeseen sekä siitä, että pienemmästä velasta kertyy vähemmän korkomenoja.</p>
        </article>
        <article class="growth-answer">
          <div><h3>Kasvun tulos ei ole yhtä varma</h3><span id="growth-total" class="metric-number"></span><p id="growth-explain"></p></div>
          <p><strong id="growth-2025"></strong> on mallin arvio vuoden 2025 BKT-tason erosta toteutuneeseen verrattuna.</p>
        </article>
      </div>

      <div class="truth"><strong>Mikä on vahvin viesti?</strong> Kaikissa kolmessa oletuksessa velkaa kertyy toteutunutta vähemmän. Kasvun etumerkki muuttuu, jos oletukset toimien toteutumisesta ja pitkän aikavälin vaikutuksista muuttuvat.</div>

      <article class="chart-card">
        <h3>Toteutunut velka ja mallin vaihtoehto</h3>
        <div class="legend"><span><i class="swatch" style="background:#151512"></i>Toteutunut valtionhallinnon EDP-velka</span><span><i class="swatch" style="background:#006ca8"></i>Valittu vaihtoehto</span><span><i class="swatch" style="background:#b9def1"></i>Mahdollinen vaihteluväli</span></div>
        <div id="debt-chart" class="chart"></div>
        <p class="chart-note">Velka tarkoittaa valtionhallinnon EDP-velkaa vuoden viimeisellä neljänneksellä. Se ei ole sama asia kuin koko julkisen sektorin velka.</p>
      </article>

      <article class="chart-card">
        <h3>Mitä malli olettaa kasvusta?</h3>
        <div class="legend"><span><i class="swatch" style="background:#a92c22"></i>Lyhyen aikavälin jarru</span><span><i class="swatch" style="background:#18733a"></i>Pidemmän aikavälin hyötyoletus</span><span><i class="swatch" style="background:#006ca8"></i>Yhteisvaikutus</span></div>
        <div id="growth-chart" class="chart"></div>
        <p class="chart-note">Käyrä kertoo poikkeaman saman vuoden toteutuneesta BKT:sta. Vihreä viiva on nimenomaan oletus tarjontavaikutuksesta, ei havaittu tosiasia.</p>
      </article>
    </div></section>

    <section id="miten"><div class="wrap">
      <p class="eyebrow">Kolme yksinkertaista vaihetta</p>
      <h2>Näin “mitä jos?” -laskelma tehtiin</h2>
      <p class="lede">Laskelma ei kirjoita historiaa uusiksi. Se pitää toteutuneet kriisit perusuralla ja arvioi, miten politiikkapaketti olisi muuttanut velan ja BKT:n kulkua.</p>
      <div class="simple-steps">
        <article class="step"><span class="step-num">1</span><h3>Käytetään toteutunutta dataa</h3><p>Budjettihaukan momenttitoteumat, BKT, velka ja korkomenot vuosilta 2008–2025 ovat laskelman lähtökohta.</p></article>
        <article class="step"><span class="step-num">2</span><h3>Lisätään politiikkavaihtoehto</h3><p>Vuoden 2026 politiikkarakennetta sovelletaan neljässä vaiheessa vuosina 2008–2011. Kertaluonteinen Alko-tulo jätetään pois.</p></article>
        <article class="step"><span class="step-num">3</span><h3>Verrataan kahta polkua</h3><p>Velkaa vähentää suora tasapainoparannus, mahdollinen suhdannevaikutus ja pienemmän velan korkosäästö.</p></article>
      </div>
    </div></section>

    <section id="rajoitukset"><div class="wrap">
      <p class="eyebrow">Rehellinen tulkinta</p>
      <h2>Laskelma vastaa yhteen kysymykseen, ei kaikkiin.</h2>
      <div class="balance">
        <article class="card good"><h3>Mitä tästä voi sanoa</h3><ul><li>Politiikkapaketti parantaisi mallissa julkista tasapainoa joka vuosi.</li><li>Pienempi velka pienentäisi myös korkomenoja.</li><li>Velkavaikutuksen suunta säilyy kaikissa kolmessa herkkyystapauksessa.</li></ul></article>
        <article class="card caution"><h3>Mitä tästä ei voi sanoa</h3><ul><li>Ei tarkkaa työllisyysvaikutusta.</li><li>Ei vaikutusta yksittäisen ihmisen tuloihin tai palveluihin.</li><li>Ei varmaa arviota siitä, miten markkinakorot tai poliittinen päätöksenteko olisivat reagoineet.</li></ul></article>
      </div>
    </div></section>

    <section id="tulonjako"><div class="wrap">
      <p class="eyebrow">Tulonjakovaikutus</p>
      <h2>Vuoden 2026 paketti osuisi eri tavoin eri tulotasoihin.</h2>
      <p class="lede">Tämä osio ei kuvaa historiallista vuotta 2008–2025. Se näyttää erillisen arvion siitä, miten vuoden 2026 politiikkapaketti voisi muuttaa asuntokuntien käytettävissä olevia tuloja tulokymmenyksittäin.</p>
      <div class="micro-banner">
        <strong>Suuntaa-antava<br>mikromalli</strong>
        <p><strong>Ei SISU-ajo.</strong> Arvio on kalibroitu Tilastokeskuksen julkisiin tulonjakotilastoihin, kulutustilastoihin ja 90 edustavaan kotitaloussoluun. Se kertoo suunnan, mutta ei ole rekisteriaineistoon perustuva virallinen tulonjakolaskelma.</p>
      </div>
      <div class="case-switches" aria-label="Valitse tulonjakomallin oletus">
        <button type="button" data-micro-case="jakauma_suotuisa" aria-pressed="false">Suotuisa</button>
        <button type="button" data-micro-case="keskinen" aria-pressed="true">Keskinen</button>
        <button type="button" data-micro-case="jakauma_rasittava" aria-pressed="false">Rasittava</button>
      </div>
      <p id="micro-case-note" class="case-note"></p>
      <div class="micro-metrics" aria-live="polite">
        <div class="micro-metric"><b id="micro-average"></b><span>arvioitu keskimääräinen muutos asuntokuntaa kohti vuodessa</span></div>
        <div class="micro-metric"><b id="micro-winners"></b><span>asuntokunnista hyötyisi mallissa</span></div>
        <div class="micro-metric"><b id="micro-bottom-40"></b><span>mallin yhteisvaikutus neljälle pienituloisimmalle tulodesiilille</span></div>
      </div>
      <article class="chart-card">
        <h3>Arvio tulojen muutoksesta tulodesiileittäin</h3>
        <div class="legend"><span><i class="swatch" style="background:#a92c22"></i>pienempi käytettävissä oleva tulo</span><span><i class="swatch" style="background:#18733a"></i>suurempi käytettävissä oleva tulo</span><span><i class="swatch" style="background:#151512"></i>herkkyysalue kolmesta oletuksesta</span></div>
        <div id="decile-chart" class="chart"></div>
        <p class="chart-note">Tulodesiili 1 on pienituloisin kymmenesosa ja tulodesiili 10 suurituloisin. Paksu palkki on valittu oletus. Ohut viiva näyttää kolmen mallioletuksen vaihteluvälin. Luku on arvio keskimääräisestä muutoksesta asuntokuntaa kohti, ei yksittäisen ihmisen tulos.</p>
      </article>
      <article class="chart-card">
        <h3>Käytettävissä olevat tulot vuodessa</h3>
        <div class="legend"><span><i class="swatch" style="background:#e4ded1"></i>mallin tulotaso ennen politiikkamuutosta</span><span><i class="swatch" style="background:#006ca8"></i>mallin tulotaso politiikkamuutoksen jälkeen</span></div>
        <div id="income-chart" class="chart"></div>
        <p class="chart-note">Kyseessä on mallin painotettu keskimääräinen asuntokunta tulodesiilissä. Tulopohja on vuoden 2024 tasossa ja politiikkamuutos vuoden 2026 euroissa, joten kuvaaja ei ole inflaatiokorjattu ostovoimavertailu.</p>
      </article>
      <div id="micro-summary" class="truth"></div>
    </div></section>

    <section id="sisu"><div class="wrap">
      <p class="eyebrow">SISU-malli ja tämä laskelma</p>
      <h2>Sama vertailun idea, eri kysymys ja eri data.</h2>
      <p class="lede">SISU täydentäisi tätä työtä. Se ei kuitenkaan ole sama malli, eikä tätä visualisointia ole laskettu SISUlla.</p>
      <div class="sisu">
        <article class="card"><h3>Tämä historiallinen vastelaskelma</h3><div class="compare"><div><b>Kysymys</b><span>Miten valtion velka ja BKT:n polku voisivat muuttua?</span></div><div><b>Aineisto</b><span>Toteutuneet budjettimomentit, makrotalous, velka ja korkomenot.</span></div><div><b>Tulos</b><span>Julkistalouden ja kansantalouden skenaariopolku.</span></div></div></article>
        <article class="card"><h3>Tilastokeskuksen SISU</h3><div class="compare"><div><b>Kysymys</b><span>Miten vero- ja etuusmuutos vaikuttaa ihmisiin ja kotitalouksiin?</span></div><div><b>Aineisto</b><span>Väestöä edustava henkilötason aineisto luvanvaraisessa etäkäytössä.</span></div><div><b>Tulos</b><span>Tulonjako, käytettävissä olevat tulot, verot ja etuudet.</span></div></div></article>
      </div>
      <div class="truth"><strong>Voimmeko käyttää SISUa?</strong> Kyllä, mallikoodi on avoin BSD-3-Clause-lisenssillä. Edustavaa väestöaineistoa käyttävä aineistosimulointi vaatii kuitenkin Tilastokeskuksen käyttöluvan ja etäkäyttöympäristön. Käytännössä SISU olisi Budjettihaukan seuraava erillinen analyysikerros, jolla arvioidaan esimerkiksi verojen ja etuuksien tulonjakovaikutuksia.</div>
    </div></section>

    <section id="lahteet"><div class="wrap">
      <p class="eyebrow">Lähteet ja tarkistettavuus</p>
      <h2>Kaikki lähtötiedot ovat jäljitettävissä.</h2>
      <div id="sources" class="source-grid"></div>
      <p class="chart-note">Laskelmaan on jäädytetty 11 258 momentti-vuosiriviä ja viralliset makrosarjat. Momenttien vertailukelpoisuus on heikoin historiallisessa alkupäässä, joten momentteja käytetään kattavuusvarmennukseen, ei yksin politiikkavaikutuksen mekaaniseksi laskemiseksi.</p>
    </div></section>
  </main>
  <footer class="footer"><div class="wrap">Budjettihaukka · Historiallinen vastelaskelma 2008–2025 · <a href="liberaali-vaihtoehtobudjetti-2026.html">Avaa laajempi vaihtoehtobudjettiraportti</a></div></footer>

  <script type="application/json" id="visualization-data">__DATA__</script>
  <script>
    const data = JSON.parse(document.getElementById('visualization-data').textContent);
    const colors = { ink: '#151512', blue: '#006ca8', blueSoft: '#b9def1', green: '#18733a', red: '#a92c22', grid: '#1515122b', muted: '#625d52' };
    let selected = 'keskinen';
    let selectedMicro = 'keskinen';
    const cases = {
      suotuisa: { note: 'Suotuisa: kaikki toimet toteutuvat ja pitkän aikavälin kasvuhyöty on suuri.' },
      keskinen: { note: 'Keskinen: 90 % toimista toteutuu ja pitkän aikavälin kasvuhyöty on maltillinen.' },
      varovainen: { note: 'Varovainen: vain 75 % toimista toteutuu eikä pitkän aikavälin kasvuhyötyä oleteta.' }
    };
    const eur = (value, digits = 1) => `${new Intl.NumberFormat('fi-FI', { maximumFractionDigits: digits, minimumFractionDigits: digits }).format(Math.abs(value) / 1e9)} mrd. €`;
    const signedEur = (value, digits = 1) => `${value >= 0 ? '+' : '−'}${eur(value, digits)}`;
    const pct = (value, digits = 2) => `${value >= 0 ? '+' : '−'}${new Intl.NumberFormat('fi-FI', { maximumFractionDigits: digits, minimumFractionDigits: digits }).format(Math.abs(value))} %`;
    const householdEur = (value, digits = 0) => `${value >= 0 ? '+' : '−'}${new Intl.NumberFormat('fi-FI', { maximumFractionDigits: digits, minimumFractionDigits: digits }).format(Math.abs(value))} €`;
    const el = id => document.getElementById(id);
    const rowFor = (caseId, year) => data.cases[caseId].yearly.find(row => row.year === year);
    const xAt = (year, min, max, left, width) => left + (year - min) / (max - min) * width;
    const yAt = (value, min, max, top, height) => top + (max - value) / (max - min) * height;
    const line = (rows, key, yMin, yMax, plot) => rows.map((row, index) => `${index ? 'L' : 'M'}${xAt(row.year, 2008, 2025, plot.left, plot.width).toFixed(1)},${yAt(row[key], yMin, yMax, plot.top, plot.height).toFixed(1)}`).join(' ');
    const svg = (target, label) => { const node = el(target); node.innerHTML = ''; const ns = 'http://www.w3.org/2000/svg'; const output = document.createElementNS(ns, 'svg'); output.setAttribute('viewBox', '0 0 880 420'); output.setAttribute('role', 'img'); output.setAttribute('aria-label', label); node.append(output); return output; };
    const add = (node, name, attrs, text) => { const item = document.createElementNS('http://www.w3.org/2000/svg', name); Object.entries(attrs || {}).forEach(([key, value]) => item.setAttribute(key, value)); if (text !== undefined) item.textContent = text; node.append(item); return item; };
    const text = (node, x, y, value, attrs = {}) => add(node, 'text', { x, y, fill: colors.muted, 'font-family': 'Source Sans 3', 'font-size': 13, ...attrs }, value);
    function debtChart() {
      const chart = svg('debt-chart', 'Toteutuneen ja vaihtoehtoisen valtionhallinnon EDP-velan kehitys vuosina 2008-2025');
      const plot = { left: 66, top: 27, width: 760, height: 280 };
      const actual = data.cases.keskinen.yearly;
      const envelope = data.envelope;
      const maxDebt = Math.ceil(Math.max(...actual.map(row => row.actual_debt_eur), ...envelope.map(row => row.high_alternative_debt_eur)) / 25e9) * 25e9;
      [0, .25, .5, .75, 1].forEach(share => { const value = maxDebt * share; const y = yAt(value, 0, maxDebt, plot.top, plot.height); add(chart, 'line', { x1: plot.left, y1: y, x2: plot.left + plot.width, y2: y, stroke: colors.grid, 'stroke-width': 1 }); text(chart, 5, y + 4, `${Math.round(value / 1e9)} mrd.`); });
      [2008, 2011, 2015, 2020, 2025].forEach(year => text(chart, xAt(year, 2008, 2025, plot.left, plot.width), 336, year, { 'text-anchor': 'middle' }));
      const high = envelope.map((row, index) => `${index ? 'L' : 'M'}${xAt(row.year, 2008, 2025, plot.left, plot.width)},${yAt(row.high_alternative_debt_eur, 0, maxDebt, plot.top, plot.height)}`).join(' ');
      const low = [...envelope].reverse().map(row => `L${xAt(row.year, 2008, 2025, plot.left, plot.width)},${yAt(row.low_alternative_debt_eur, 0, maxDebt, plot.top, plot.height)}`).join(' ');
      add(chart, 'path', { d: `${high} ${low} Z`, fill: colors.blueSoft, opacity: .8 });
      add(chart, 'path', { d: line(actual, 'actual_debt_eur', 0, maxDebt, plot), fill: 'none', stroke: colors.ink, 'stroke-width': 3, 'stroke-dasharray': '7 5' });
      add(chart, 'path', { d: line(data.cases[selected].yearly, 'alternative_debt_eur', 0, maxDebt, plot), fill: 'none', stroke: colors.blue, 'stroke-width': 4 });
      const finalActual = actual.at(-1); const finalAlternative = data.cases[selected].yearly.at(-1);
      add(chart, 'circle', { cx: xAt(2025, 2008, 2025, plot.left, plot.width), cy: yAt(finalActual.actual_debt_eur, 0, maxDebt, plot.top, plot.height), r: 4.5, fill: colors.ink });
      add(chart, 'circle', { cx: xAt(2025, 2008, 2025, plot.left, plot.width), cy: yAt(finalAlternative.alternative_debt_eur, 0, maxDebt, plot.top, plot.height), r: 5, fill: colors.blue });
      text(chart, 835, yAt(finalActual.actual_debt_eur, 0, maxDebt, plot.top, plot.height) + 4, 'Toteutunut', { fill: colors.ink, 'font-weight': 800 });
      text(chart, 835, yAt(finalAlternative.alternative_debt_eur, 0, maxDebt, plot.top, plot.height) + 4, 'Malli', { fill: colors.blue, 'font-weight': 800 });
    }
    function growthChart() {
      const chart = svg('growth-chart', 'Mallin BKT-poikkeaman kysyntä-, tarjonta- ja yhteisvaikutus vuosina 2008-2025');
      const plot = { left: 66, top: 27, width: 760, height: 280 };
      const rows = data.cases[selected].yearly;
      const yMin = -1.6, yMax = 1.8;
      [-1.5, -0.75, 0, .75, 1.5].forEach(value => { const y = yAt(value, yMin, yMax, plot.top, plot.height); add(chart, 'line', { x1: plot.left, y1: y, x2: plot.left + plot.width, y2: y, stroke: value === 0 ? colors.ink : colors.grid, 'stroke-width': value === 0 ? 1.8 : 1 }); text(chart, 9, y + 4, `${value.toFixed(1).replace('.', ',')} %`); });
      [2008, 2011, 2015, 2020, 2025].forEach(year => text(chart, xAt(year, 2008, 2025, plot.left, plot.width), 336, year, { 'text-anchor': 'middle' }));
      const demandRows = rows.map(row => ({ ...row, value: row.demand_effect_pct }));
      const supplyRows = rows.map(row => ({ ...row, value: row.supply_effect_pct }));
      const totalRows = rows.map(row => ({ ...row, value: row.total_output_effect_pct }));
      add(chart, 'path', { d: line(demandRows, 'value', yMin, yMax, plot), fill: 'none', stroke: colors.red, 'stroke-width': 2.7, 'stroke-dasharray': '6 4' });
      add(chart, 'path', { d: line(supplyRows, 'value', yMin, yMax, plot), fill: 'none', stroke: colors.green, 'stroke-width': 2.7, 'stroke-dasharray': '2 4' });
      add(chart, 'path', { d: line(totalRows, 'value', yMin, yMax, plot), fill: 'none', stroke: colors.blue, 'stroke-width': 4 });
      const final = totalRows.at(-1); add(chart, 'circle', { cx: xAt(2025, 2008, 2025, plot.left, plot.width), cy: yAt(final.value, yMin, yMax, plot.top, plot.height), r: 5, fill: colors.blue });
      text(chart, 835, yAt(final.value, yMin, yMax, plot.top, plot.height) + 4, 'Yhteensä', { fill: colors.blue, 'font-weight': 800 });
    }
    function populate() {
      const summary = data.cases[selected].summary;
      el('case-note').textContent = cases[selected].note;
      document.querySelectorAll('[data-case]').forEach(button => button.setAttribute('aria-pressed', button.dataset.case === selected ? 'true' : 'false'));
      el('debt-gap').textContent = signedEur(summary.debt_difference_2025_eur, 0).replace('−', '');
      el('debt-explain').textContent = `Velkaa olisi mallissa ${eur(summary.alternative_debt_2025_eur, 1)} toteutuneen ${eur(summary.actual_debt_2025_eur, 1)} sijasta.`;
      const output = summary.cumulative_output_effect_real_2025_eur;
      el('growth-total').textContent = signedEur(output, 1);
      el('growth-explain').textContent = output >= 0 ? 'Kumulatiivinen tuotanto olisi mallissa suurempi vuosina 2008–2025, vuoden 2025 hinnoin.' : 'Kumulatiivinen tuotanto jäisi mallissa pienemmäksi vuosina 2008–2025, vuoden 2025 hinnoin.';
      el('growth-2025').textContent = pct(summary.output_effect_2025_pct, 2);
      debtChart(); growthChart();
    }
    function decileChart() {
      const chart = svg('decile-chart', 'Arvio vuoden 2026 politiikkapaketin vaikutuksesta asuntokuntien käytettävissä oleviin tuloihin tulodesiileittäin');
      chart.setAttribute('viewBox', '0 0 880 500');
      const rows = data.microsimulation.cases[selectedMicro].by_decile;
      const envelope = data.microsimulation.decile_envelope;
      const plot = { left: 170, top: 58, width: 570, height: 360 };
      const maxAbs = Math.ceil(Math.max(...envelope.flatMap(row => [Math.abs(row.low_change_per_household_eur), Math.abs(row.high_change_per_household_eur)])) / 500) * 500;
      const x = value => plot.left + (value + maxAbs) / (2 * maxAbs) * plot.width;
      [-maxAbs, -maxAbs / 2, 0, maxAbs / 2, maxAbs].forEach(value => {
        const xpos = x(value);
        add(chart, 'line', { x1: xpos, y1: plot.top - 18, x2: xpos, y2: plot.top + plot.height + 10, stroke: value === 0 ? colors.ink : colors.grid, 'stroke-width': value === 0 ? 2 : 1 });
        text(chart, xpos, 34, householdEur(value).replace('+', ''), { 'text-anchor': 'middle' });
      });
      rows.forEach((row, index) => {
        const range = envelope[index];
        const y = plot.top + index * 36;
        const barStart = x(Math.min(0, row.change_per_household_eur));
        const barEnd = x(Math.max(0, row.change_per_household_eur));
        const color = row.change_per_household_eur >= 0 ? colors.green : colors.red;
        text(chart, 151, y + 5, `D${row.group_id}`, { 'text-anchor': 'end', fill: colors.ink, 'font-weight': 800 });
        add(chart, 'line', { x1: x(range.low_change_per_household_eur), y1: y, x2: x(range.high_change_per_household_eur), y2: y, stroke: colors.ink, 'stroke-width': 2 });
        add(chart, 'line', { x1: x(range.low_change_per_household_eur), y1: y - 5, x2: x(range.low_change_per_household_eur), y2: y + 5, stroke: colors.ink, 'stroke-width': 2 });
        add(chart, 'line', { x1: x(range.high_change_per_household_eur), y1: y - 5, x2: x(range.high_change_per_household_eur), y2: y + 5, stroke: colors.ink, 'stroke-width': 2 });
        add(chart, 'rect', { x: barStart, y: y - 8, width: Math.max(2, barEnd - barStart), height: 16, fill: color });
        text(chart, 758, y + 5, `${householdEur(row.change_per_household_eur)} · ${pct(row.change_pct_disposable_income, 1)}`, { fill: color, 'font-weight': 800 });
      });
      text(chart, 170, 462, 'Pienempi käytettävissä oleva tulo', { fill: colors.red, 'font-weight': 800 });
      text(chart, 740, 462, 'Suurempi käytettävissä oleva tulo', { fill: colors.green, 'font-weight': 800, 'text-anchor': 'end' });
    }
    function incomeChart() {
      const chart = svg('income-chart', 'Mallin arvio käytettävissä olevista vuosituloista ennen ja jälkeen vuoden 2026 politiikkamuutoksen tulodesiileittäin');
      chart.setAttribute('viewBox', '0 0 880 500');
      const rows = data.microsimulation.cases[selectedMicro].by_decile;
      const plot = { left: 142, top: 58, width: 540, height: 360 };
      const maxIncome = Math.ceil(Math.max(...rows.map(row => row.post_policy_disposable_income_per_household_eur)) / 10000) * 10000;
      const x = value => plot.left + value / maxIncome * plot.width;
      [0, .25, .5, .75, 1].forEach(share => {
        const value = maxIncome * share;
        const xpos = x(value);
        add(chart, 'line', { x1: xpos, y1: plot.top - 18, x2: xpos, y2: plot.top + plot.height + 8, stroke: colors.grid, 'stroke-width': 1 });
        text(chart, xpos, 34, `${Math.round(value / 1000)} t€`, { 'text-anchor': 'middle' });
      });
      rows.forEach((row, index) => {
        const y = plot.top + index * 36;
        const baseline = row.baseline_disposable_income_per_household_eur;
        const post = row.post_policy_disposable_income_per_household_eur;
        text(chart, 124, y + 5, `D${row.group_id}`, { 'text-anchor': 'end', fill: colors.ink, 'font-weight': 800 });
        add(chart, 'rect', { x: plot.left, y: y - 9, width: x(baseline) - plot.left, height: 18, fill: '#e4ded1' });
        add(chart, 'rect', { x: plot.left, y: y - 6, width: x(post) - plot.left, height: 12, fill: colors.blue });
        text(chart, 706, y + 5, `${Math.round(baseline / 100) / 10} t€ → ${Math.round(post / 100) / 10} t€`, { fill: colors.ink, 'font-weight': 800 });
      });
      text(chart, 142, 462, 'Vuodessa, asuntokuntaa kohti', { 'font-weight': 800 });
    }
    function populateMicro() {
      const microsimulation = data.microsimulation;
      const selectedCase = microsimulation.cases[selectedMicro];
      const summary = selectedCase.summary;
      const notes = {
        jakauma_suotuisa: 'Suotuisa: kompensaatiot kohdistuvat mallissa vahvemmin pienituloisiin kotitalouksiin.',
        keskinen: 'Keskinen: julkiseen aggregaattidataan kalibroitu vertailuoletus.',
        jakauma_rasittava: 'Rasittava: kompensaatiot kohdistuvat mallissa heikommin pienituloisiin kotitalouksiin.'
      };
      el('micro-case-note').textContent = notes[selectedMicro];
      document.querySelectorAll('[data-micro-case]').forEach(button => button.setAttribute('aria-pressed', button.dataset.microCase === selectedMicro ? 'true' : 'false'));
      el('micro-average').textContent = householdEur(summary.average_change_per_household_eur);
      el('micro-winners').textContent = pct(summary.winner_households_pct, 0).replace('+', '');
      el('micro-bottom-40').textContent = signedEur(summary.bottom_40_change_eur, 1);
      const sign = summary.average_change_per_household_eur >= 0 ? 'kasvaisi' : 'pienenisi';
      el('micro-summary').innerHTML = `<strong>Tulkinta.</strong> Tässä ${selectedMicro === 'keskinen' ? 'vertailu' : 'herkkyys'}oletuksessa asuntokunnan käytettävissä oleva tulo ${sign} keskimäärin ${householdEur(summary.average_change_per_household_eur).replace('+', '').replace('−', '')} vuodessa. Tämä ei tarkoita, että jokainen saman tulodesiilin kotitalous saisi saman tuloksen: malli ei sisällä desiilin sisäistä vaihtelua.`;
      decileChart(); incomeChart();
    }
    function sources() {
      const target = el('sources');
      const historicalSources = data.sources.map(source => ({ label: source.label, url: source.url, use: source.use }));
      const microsimulationSources = data.microsimulation.source_audit.sources.map(source => ({ label: `Mikromalli: ${source.description}`, url: source.landing_page, use: `Julkinen lähdetaulukko, noudettu ${source.retrieved_at}.` }));
      target.innerHTML = [...historicalSources, ...microsimulationSources].map(source => `<div class="source"><a href="${source.url}" target="_blank" rel="noreferrer">${source.label}</a><span>${source.use}</span></div>`).join('');
    }
    document.querySelectorAll('[data-case]').forEach(button => button.addEventListener('click', () => { selected = button.dataset.case; populate(); }));
    document.querySelectorAll('[data-micro-case]').forEach(button => button.addEventListener('click', () => { selectedMicro = button.dataset.microCase; populateMicro(); }));
    sources(); populate(); populateMicro();
  </script>
</body>
</html>'''


def build(input_path: Path, analysis_path: Path, output_path: Path) -> None:
    data = json.loads(input_path.read_text(encoding="utf-8"))
    analysis = json.loads(analysis_path.read_text(encoding="utf-8"))
    data["microsimulation"] = analysis["microsimulation"]
    embedded = json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace(
        "</", "<\\/"
    )
    output_path.write_text(TEMPLATE.replace("__DATA__", embedded), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=INPUT)
    parser.add_argument("--analysis", type=Path, default=ANALYSIS)
    parser.add_argument("--out", type=Path, default=OUTPUT)
    args = parser.parse_args()
    build(args.input, args.analysis, args.out)
    try:
        output_label = args.out.relative_to(ROOT)
    except ValueError:
        output_label = args.out
    print(output_label)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Build a self-contained browser report from the v2 scenario model."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ANALYSIS = ROOT / "data" / "reference" / "liberaali_vaihtoehtobudjetti" / "analyysi_v2.json"
OUTPUT = ROOT / "liberaali-vaihtoehtobudjetti-2026.html"


TEMPLATE = r'''<!doctype html>
<html lang="fi">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta name="description" content="Liberaalipuolueen vaihtoehtobudjetin 2026 tutkimusperusteinen skenaarioarvio.">
  <link rel="icon" href="data:,">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Archivo+Black&family=Source+Sans+3:wght@400;600;700;800&display=swap" rel="stylesheet">
  <title>Rohkeutta priorisoida - vaihtoehtobudjetin skenaarioarvio</title>
  <style>
    :root {
      --yellow: #f6d84f;
      --yellow-soft: #fff2a8;
      --paper: #f7f3e9;
      --paper-deep: #ebe3d3;
      --white: #fffdf7;
      --ink: #151512;
      --muted: #615d52;
      --blue: #006ca8;
      --green: #18733a;
      --orange: #d85b18;
      --red: #a92c22;
      --purple: #70437c;
      --grid: #15151222;
      --shadow: 6px 6px 0 var(--ink);
    }
    * { box-sizing: border-box; }
    html { scroll-behavior: smooth; }
    body { margin: 0; background: var(--paper); color: var(--ink); font: 17px/1.56 "Source Sans 3", sans-serif; }
    a { color: inherit; }
    button { font: inherit; }
    .wrap { width: min(1180px, calc(100% - 40px)); margin-inline: auto; }
    .hero { position: relative; overflow: hidden; background: var(--yellow); border-bottom: 3px solid var(--ink); }
    .hero::after { content: ""; position: absolute; width: 440px; height: 440px; right: -140px; top: -170px; border: 80px solid #fff3; border-radius: 50%; }
    .hero-inner { position: relative; z-index: 1; padding: 28px 0 54px; }
    .topline { display: flex; justify-content: space-between; align-items: center; gap: 16px; }
    .back, .method-link { display: inline-flex; align-items: center; gap: 8px; padding: 8px 14px; border: 2px solid var(--ink); border-radius: 999px; background: var(--yellow-soft); font-weight: 800; text-decoration: none; }
    .publication { font-size: .85rem; font-weight: 800; letter-spacing: .08em; text-transform: uppercase; }
    h1, h2, h3, .number { font-family: "Archivo Black", "Arial Black", sans-serif; letter-spacing: -.04em; }
    h1 { max-width: 980px; margin: 46px 0 22px; font-size: clamp(3rem, 8.5vw, 7.2rem); line-height: .88; text-transform: uppercase; }
    h1 span { display: block; color: var(--white); -webkit-text-stroke: 2px var(--ink); text-shadow: 4px 4px 0 var(--ink); }
    .lede { max-width: 840px; margin: 0; font-size: clamp(1.08rem, 2vw, 1.3rem); font-weight: 600; }
    .trust-row { display: flex; flex-wrap: wrap; gap: 9px; margin-top: 24px; }
    .pill { padding: 7px 11px; border: 1.5px solid var(--ink); border-radius: 999px; background: var(--white); font-size: .84rem; font-weight: 800; }
    .pill.good { background: #ccebcf; }
    .pill.warn { background: #ffd9a3; }
    .pill.low { background: #f3c4bf; }
    .hero-stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; margin-top: 34px; }
    .hero-stat { padding-top: 13px; border-top: 3px solid var(--ink); }
    .hero-stat strong { display: block; font: 1.75rem/1 "Archivo Black", sans-serif; letter-spacing: -.05em; }
    .hero-stat span { display: block; margin-top: 7px; font-size: .88rem; line-height: 1.3; }
    .nav { position: sticky; z-index: 20; top: 0; overflow-x: auto; background: #151512f2; color: white; border-bottom: 1px solid #fff3; }
    .nav-inner { display: flex; min-width: max-content; }
    .nav a { padding: 12px 15px; color: white; font-size: .86rem; font-weight: 800; text-decoration: none; }
    .nav a:hover, .nav a:focus-visible { background: var(--yellow); color: var(--ink); }
    main { padding-bottom: 78px; }
    section { padding: 62px 0; border-bottom: 2px solid var(--ink); }
    .eyebrow { margin: 0 0 8px; color: var(--muted); font-size: .79rem; font-weight: 800; letter-spacing: .14em; text-transform: uppercase; }
    h2 { max-width: 980px; margin: 0 0 16px; font-size: clamp(2rem, 4.8vw, 4.2rem); line-height: .98; }
    h3 { margin: 0 0 10px; font-size: 1.08rem; line-height: 1.15; letter-spacing: -.025em; }
    .section-lede { max-width: 840px; margin: 0 0 28px; color: var(--muted); font-size: 1.08rem; }
    .verdict { display: grid; grid-template-columns: 1.5fr 1fr; gap: 20px; align-items: stretch; }
    .verdict-main, .verdict-side, .panel { border: 2px solid var(--ink); background: var(--white); }
    .verdict-main { padding: 30px; box-shadow: var(--shadow); }
    .verdict-main .stamp { display: inline-block; transform: rotate(-2deg); padding: 7px 10px; border: 3px solid var(--green); color: var(--green); font: .9rem/1 "Archivo Black", sans-serif; text-transform: uppercase; }
    .verdict-main p { max-width: 720px; }
    .verdict-side { padding: 24px; background: var(--paper-deep); }
    .verdict-side ol { margin: 14px 0 0; padding-left: 22px; }
    .verdict-side li { margin-bottom: 9px; }
    .two-col { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 20px; }
    .panel { padding: 22px; min-width: 0; }
    .panel.yellow { background: var(--yellow-soft); }
    .finding { margin-top: 18px; padding: 16px 18px; border-left: 6px solid var(--yellow); background: var(--paper-deep); }
    .finding strong { font-weight: 800; }
    .chart { min-height: 290px; margin-top: 12px; }
    .chart svg { display: block; width: 100%; height: auto; overflow: visible; }
    .source { margin: 14px 0 0; color: var(--muted); font-size: .83rem; }
    .source a { text-decoration-thickness: 1px; text-underline-offset: 3px; }
    .metric-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 14px; margin: 26px 0; }
    .metric { padding: 18px; border: 2px solid var(--ink); background: var(--white); }
    .metric .number { display: block; font-size: clamp(1.55rem, 3vw, 2.5rem); line-height: 1; }
    .metric .label { display: block; margin-top: 9px; color: var(--muted); font-size: .9rem; }
    .switches { display: flex; flex-wrap: wrap; gap: 8px; margin: 24px 0 12px; }
    .switches button { padding: 8px 13px; border: 2px solid var(--ink); border-radius: 999px; background: var(--white); color: var(--ink); font-weight: 800; cursor: pointer; }
    .switches button[aria-pressed="true"] { background: var(--yellow); box-shadow: 3px 3px 0 var(--ink); transform: translate(-1px, -1px); }
    .assumption-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 9px; margin: 12px 0 22px; }
    .assumption { padding: 11px 13px; border: 1.5px solid var(--ink); background: var(--white); }
    .assumption strong { display: block; font-size: 1.05rem; }
    .assumption span { display: block; color: var(--muted); font-size: .78rem; line-height: 1.25; }
    .scenario-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; margin-top: 22px; }
    .scenario-card { padding: 18px; border: 2px solid var(--ink); background: var(--white); }
    .scenario-card.recommended { background: #d8eedb; }
    .scenario-card .big { display: block; margin-top: 14px; font: 1.7rem/1 "Archivo Black", sans-serif; }
    .scenario-card dl { display: grid; grid-template-columns: 1fr auto; gap: 5px 12px; margin: 14px 0 0; font-size: .9rem; }
    .scenario-card dt { color: var(--muted); }
    .scenario-card dd { margin: 0; font-weight: 800; text-align: right; }
    .callout { margin-top: 24px; padding: 22px; border: 3px solid var(--ink); background: var(--yellow); box-shadow: var(--shadow); }
    .callout p { max-width: 900px; margin: 7px 0 0; }
    .risk-list { display: grid; gap: 12px; margin-top: 24px; }
    .risk { display: grid; grid-template-columns: 210px 110px 1fr 1fr; gap: 16px; padding: 16px; border: 1.5px solid var(--ink); background: var(--white); align-items: start; }
    .risk-name { font-weight: 800; }
    .risk-level { display: inline-flex; width: fit-content; padding: 4px 8px; border: 1px solid var(--ink); border-radius: 999px; font-size: .77rem; font-weight: 800; text-transform: uppercase; }
    .risk-level.korkea { background: #f3c4bf; }
    .risk-level.keskikorkea { background: #ffd9a3; }
    .risk p { margin: 0; font-size: .91rem; }
    .roadmap { position: relative; display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; margin-top: 26px; }
    .stage { position: relative; padding: 19px; border: 2px solid var(--ink); background: var(--white); }
    .stage::before { content: attr(data-share); display: grid; place-items: center; width: 44px; height: 44px; margin-bottom: 14px; border: 2px solid var(--ink); border-radius: 50%; background: var(--yellow); font-weight: 800; }
    .stage .timing { color: var(--muted); font-size: .82rem; font-weight: 800; text-transform: uppercase; }
    .stage ul { padding-left: 18px; font-size: .9rem; }
    .stop { margin-top: 12px; padding-top: 10px; border-top: 2px solid var(--red); color: var(--red); font-size: .85rem; font-weight: 700; }
    .recommendations { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px; margin-top: 24px; }
    .rec { display: grid; grid-template-columns: 48px 1fr; gap: 14px; padding: 19px; border: 2px solid var(--ink); background: var(--white); }
    .rec-num { display: grid; place-items: center; width: 42px; height: 42px; background: var(--ink); color: var(--yellow); font: 1rem/1 "Archivo Black", sans-serif; }
    .rec p { margin: 6px 0 0; color: var(--muted); font-size: .94rem; }
    .method-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
    .method-grid ul { padding-left: 20px; }
    .sources { display: grid; gap: 9px; margin-top: 16px; }
    .source-row { display: grid; grid-template-columns: 1.1fr 1fr; gap: 18px; padding: 12px 0; border-top: 1px solid var(--grid); }
    .source-row a { font-weight: 800; text-underline-offset: 3px; }
    .source-row span { color: var(--muted); font-size: .9rem; }
    .table-wrap { overflow-x: auto; margin-top: 20px; border: 2px solid var(--ink); }
    table { width: 100%; min-width: 720px; border-collapse: collapse; background: var(--white); font-size: .9rem; }
    th, td { padding: 11px 12px; border-bottom: 1px solid var(--grid); text-align: left; vertical-align: top; }
    th { background: var(--ink); color: white; font-size: .78rem; letter-spacing: .08em; text-transform: uppercase; }
    td.num { white-space: nowrap; font-variant-numeric: tabular-nums; font-weight: 800; }
    .footer-note { margin-top: 36px; padding: 20px 0; border-top: 2px solid var(--ink); color: var(--muted); font-size: .88rem; }
    .axis { stroke: var(--ink); stroke-width: 1.3; }
    .gridline { stroke: var(--grid); stroke-width: 1; }
    .zero { stroke: var(--ink); stroke-width: 1.8; }
    .tick { fill: var(--muted); font: 12px "Source Sans 3", sans-serif; }
    .direct { font: 700 12px "Source Sans 3", sans-serif; }
    .bar-value { font: 800 12px "Source Sans 3", sans-serif; }
    .legend { display: flex; flex-wrap: wrap; gap: 9px 18px; margin-top: 10px; font-size: .86rem; }
    .legend span { display: inline-flex; align-items: center; gap: 7px; }
    .swatch { width: 15px; height: 15px; border: 1px solid var(--ink); }
    .trust-banner { display: grid; grid-template-columns: 180px 1fr; gap: 20px; padding: 20px; border: 3px solid var(--ink); background: #ffd9a3; box-shadow: var(--shadow); }
    .trust-banner strong { font: 1.05rem/1.1 "Archivo Black", sans-serif; text-transform: uppercase; }
    .trust-banner p { margin: 0; }
    .micro-metrics { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin: 22px 0; }
    .micro-metric { padding: 16px; border: 2px solid var(--ink); background: var(--white); }
    .micro-metric strong { display: block; font: clamp(1.35rem, 2.7vw, 2.2rem)/1 "Archivo Black", sans-serif; letter-spacing: -.05em; }
    .micro-metric span { display: block; margin-top: 8px; color: var(--muted); font-size: .84rem; line-height: 1.25; }
    .event-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; margin-top: 18px; }
    .event { padding: 17px; border: 2px solid var(--ink); background: var(--white); }
    .event .event-value { display: block; margin: 13px 0 4px; font: 1.55rem/1 "Archivo Black", sans-serif; }
    .event p { margin: 8px 0 0; color: var(--muted); font-size: .88rem; }
    .coverage-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; margin-top: 20px; }
    .micro-charts { grid-template-columns: 1fr; }
    .coverage { padding: 18px; border: 2px solid var(--ink); background: var(--white); }
    .coverage.excluded { background: var(--paper-deep); }
    .coverage ul { margin-bottom: 0; padding-left: 19px; }
    .coverage li { margin-bottom: 7px; }
    .range-note { display: inline-flex; align-items: center; gap: 9px; color: var(--muted); font-size: .84rem; }
    .range-line { position: relative; width: 34px; height: 2px; background: var(--ink); }
    .range-line::before, .range-line::after { content: ""; position: absolute; top: -4px; width: 2px; height: 10px; background: var(--ink); }
    .range-line::before { left: 0; }
    .range-line::after { right: 0; }
    .history-event-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-top: 20px; }
    .history-event { padding: 16px; border: 2px solid var(--ink); background: var(--paper-deep); }
    .history-event strong { display: block; margin-bottom: 5px; font: 1rem/1.1 "Archivo Black", sans-serif; }
    .history-event span { display: block; color: var(--orange); font-size: .8rem; font-weight: 800; text-transform: uppercase; }
    .history-event p { margin: 9px 0 0; color: var(--muted); font-size: .86rem; }
    .logic-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; margin-top: 22px; }
    .logic-card { padding: 17px; border: 2px solid var(--ink); background: var(--white); }
    .logic-card.assumed { background: var(--yellow-soft); }
    .logic-card.modelled { background: #d8eedb; }
    .logic-card ul { margin-bottom: 0; padding-left: 18px; font-size: .88rem; }
    #historia .panel .logic-grid { grid-template-columns: 1fr; margin-top: 16px; }
    @media (max-width: 900px) {
      .hero-stats, .metric-grid, .assumption-grid, .roadmap, .micro-metrics { grid-template-columns: repeat(2, 1fr); }
      .verdict, .two-col, .method-grid { grid-template-columns: 1fr; }
      .coverage-grid { grid-template-columns: 1fr; }
      .history-event-grid, .logic-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .risk { grid-template-columns: 1fr 110px; }
      .risk p { grid-column: 1 / -1; }
    }
    @media (max-width: 680px) {
      body { font-size: 16px; }
      .wrap { width: min(100% - 26px, 1180px); }
      .topline { align-items: flex-start; flex-direction: column; }
      h1 { margin-top: 34px; }
      h1 span { -webkit-text-stroke-width: 1.5px; text-shadow: 3px 3px 0 var(--ink); }
      .hero-stats, .metric-grid, .assumption-grid, .scenario-grid, .recommendations, .roadmap, .micro-metrics, .event-grid { grid-template-columns: 1fr; }
      .history-event-grid, .logic-grid { grid-template-columns: 1fr; }
      section { padding: 44px 0; }
      .panel, .verdict-main, .verdict-side { padding: 17px; }
      .risk { grid-template-columns: 1fr; }
      .risk p { grid-column: auto; }
      .chart { min-height: 230px; overflow-x: auto; overscroll-behavior-inline: contain; }
      .chart svg { width: 720px; max-width: none; }
      .source-row { grid-template-columns: 1fr; gap: 4px; }
      .trust-banner { grid-template-columns: 1fr; }
    }
    @media (prefers-reduced-motion: reduce) { html { scroll-behavior: auto; } }
  </style>
</head>
<body>
  <header class="hero">
    <div class="wrap hero-inner">
      <div class="topline">
        <a class="back" href="visualisointidemot.html">← Budjettihaukka</a>
        <span class="publication">Politiikkaraportti · 23.8.2026</span>
      </div>
      <h1>Rohkeutta <span>priorisoida</span></h1>
      <p class="lede">Liberaalipuolueen vaihtoehtobudjetti voi kääntää velkasuhteen suunnan ja siirtää verotusta pois työnteosta. Taloudellisesti kestävä toteutus vaatii kuitenkin neljän vuoden vaiheistuksen, korkean tuoton menojen suojan ja täsmällisemmän verokevennyksen määritelmän.</p>
      <div class="trust-row" aria-label="Arvion luottamustasot">
        <span class="pill good">Kirjanpito: korkea luottamus</span>
        <span class="pill warn">Historia 2008–2025: kontrafaktuaali</span>
        <span class="pill warn">Makroskenaariot: keskitaso</span>
        <span class="pill warn">Tulonjako: synteettinen mikromalli</span>
        <span class="pill low">SISU/FIONA-varmennus puuttuu</span>
        <span class="pill">Puolueen oma raportti, oletukset näkyvissä</span>
      </div>
      <div class="hero-stats">
        <div class="hero-stat"><strong id="hero-balance"></strong><span>lähdetaulukon ensimmäisen vuoden staattinen tasapainoparannus; sisältää kertatulon</span></div>
        <div class="hero-stat"><strong id="hero-tax"></strong><span>maksajien verorasituksen nettomuutos, jos aluevero toteutuu täysimääräisenä</span></div>
        <div class="hero-stat"><strong id="hero-output"></strong><span>pahin BKT-poikkeama suositusuralla, keskiskenaario</span></div>
        <div class="hero-stat"><strong id="hero-debt"></strong><span>EDP-velkasuhde 2035 suositusuralla; tekninen perusura <span id="hero-debt-base"></span></span></div>
      </div>
    </div>
  </header>

  <nav class="nav" aria-label="Raportin osiot">
    <div class="wrap nav-inner">
      <a href="#johtopaatos">Johtopäätös</a><a href="#kirjanpito">Kirjanpito</a><a href="#historia">2008–2025</a><a href="#rakenne">Rakenne</a><a href="#makro">Makro</a><a href="#hyvinvointi">Tulonjako</a><a href="#toteutus">Toteutus</a><a href="#suositukset">Suositukset</a><a href="#menetelma">Menetelmä</a>
    </div>
  </nav>

  <main class="wrap">
    <section id="johtopaatos">
      <p class="eyebrow">Tiivistelmä · mitä tästä seuraa</p>
      <h2>Suunnan voi toteuttaa. Aikataulua ja koostumusta pitää muuttaa.</h2>
      <div class="verdict">
        <article class="verdict-main">
          <span class="stamp">Ehdollisesti toteuttamiskelpoinen</span>
          <p><strong>Vahva ydin</strong> on lähdetaulukon 8,66 mrd. euron ensimmäisen vuoden staattinen alijäämän supistaminen, josta 8,05 mrd. euroa on tunnistettu toistuvaksi ennen toteutumisriskin herkkyyttä. Kasvua tukevat työn verotuksen keventäminen, veropohjien laajentaminen, haittaverot ja heikosti perusteltujen tukien karsiminen.</p>
          <p><strong>Heikko kohta</strong> on yhden vuoden mittakaava. Taulukko siirtää tai leikkaa yli 12,5 mrd. euroa samalla, kun Suomen Pankki ennustaa vuodelle 2026 vain 0,7 prosentin kasvua ja 10,4 prosentin työttömyyttä. Kertatoteutus osuisi elpymisen alkuun.</p>
          <p id="history-verdict"><strong>Historiallinen peili</strong> ladataan toteumapohjaisesta vastelaskelmasta.</p>
          <p><strong>Tulonjakoriski</strong> on synteettisessä mikrolaskelmassa olennainen. Keskiskenaariossa suorat mallinnetut kotitalousvirrat heikkenevät keskimäärin noin 690 euroa vuodessa, ja suurin riski kohdistuu pienituloisiin eläkeläistalouksiin. Luku ei ole virallinen SISU-tulos, mutta se estää väittämästä pakettia tulonjaoltaan neutraaliksi ilman uudelleen kohdennettua kompensaatiota ja rekisteriaineistovarmennusta.</p>
          <p><strong>Suositus</strong> on neljän vuoden, 10–20–30–40 prosentin toteutus: ensin varmennetut matalan haitan säästöt ja veropohjat, viimeiseksi alue- ja palvelurakenteet. TKI, infrastruktuuri ja ydintehtävät korvataan muilla säästöillä.</p>
        </article>
        <aside class="verdict-side">
          <h3>Neljä ehtoa hyväksynnälle</h3>
          <ol>
            <li>Synteettinen tulonjakoarvio varmennetaan SISU/FIONA-aineistosimulaatiolla.</li>
            <li>Hyvinvointialueleikkaus seuraa tehtävän poistumista tai todennettua tuottavuutta.</li>
            <li>TKI- ja investointileikkaukset korvataan matalamman kertoimen säästöillä.</li>
            <li>Veronalennus sidotaan toteutuneeseen pysyvään, ei arvioituun säästöön.</li>
          </ol>
        </aside>
      </div>
    </section>

    <section id="kirjanpito">
      <p class="eyebrow">01 · Kirjanpidollinen totuus</p>
      <h2>12,55 miljardin menovähennys ei ole sama asia kuin 12,55 miljardin sopeutus</h2>
      <p class="section-lede">Valtion budjetissa menot laskevat 12,55 mrd. euroa. Tästä 8,66 mrd. vähentää lainanottoa ja 3,89 mrd. korvaa muiden tulojen vähenemää. Jälkimmäinen sisältää veroja, EU-tuloja, omaisuustuloja ja momenttien välisiä siirtoja.</p>
      <div class="two-col">
        <div class="panel yellow">
          <h3>Mihin menovähennys käytetään?</h3>
          <div id="accounting-chart" class="chart"></div>
          <p class="source">Lähde: vaihtoehtobudjetin päätason meno- ja tulosummat. Summat ovat valtion budjettikirjanpitoa.</p>
        </div>
        <div class="panel">
          <h3>Mitä verorasitukselle todella tapahtuu?</h3>
          <div id="tax-bridge-chart" class="chart"></div>
          <p class="source">Päästökaupan 503 milj. euroa siirtyy momentilta toiselle eikä ole uusi maksu. Hyvinvointialueveron 2,714 mrd. euroa oletetaan kerättävän täysimääräisenä.</p>
        </div>
      </div>
      <div class="callout">
        <h3>Raportin tärkein korjaus</h3>
        <p><strong>3,89 mrd. euroa ei ole nettomääräinen veronkevennys.</strong> Kun ei-verotulot, 503 milj. euron luokittelusiirto ja hyvinvointialuevero erotetaan, maksajien verorasitus kevenee noin 0,31 mrd. euroa. Jos alueet kattavat puolet siirrosta säästöillä ja puolet verolla, kevennys olisi noin 1,67 mrd. euroa - mutta tätä säästöä ei ole vielä todennettu. Lisäksi Alkon myyntiin perustuva 614 milj. euroa on kertatulo, ei pysyvän veronalennuksen rahoitus.</p>
      </div>
      <div class="metric-grid">
        <div class="metric"><span class="number" id="metric-state-tax"></span><span class="label">valtion vero- ja veronluonteisten tulojen muutos</span></div>
        <div class="metric"><span class="number" id="metric-nontax"></span><span class="label">oikaistu ei-verotulojen menetys</span></div>
        <div class="metric"><span class="number" id="metric-recurring"></span><span class="label">tunnistettu toistuva tasapainoparannus ennen toteutumisriskin herkkyyttä</span></div>
        <div class="metric"><span class="number">0,7 milj. €</span><span class="label">lähdetaulukon pieni hierarkiaero; makromalli käyttää päätason summaa</span></div>
      </div>
    </section>

    <section id="historia">
      <p class="eyebrow">02 · Historiallinen vastelaskelma 2008–2025</p>
      <h2>Mallissa velkaa olisi kertynyt vähemmän. Tarkka ero ei ole havaittu fakta.</h2>
      <p class="section-lede">Laskelma säilyttää finanssikriisin, eurokriisin, pandemian ja muut toteutuneet sokit perusurassa. Vuoden 2026 suositeltu politiikkarakenne skaalataan kunkin vuoden toteutuneeseen BKT:hen ja valtiontalouden pohjaan sekä otetaan käyttöön neljässä vaiheessa vuosina 2008–2011.</p>
      <div class="trust-banner">
        <strong>Kontrafaktuaali<br>ei toteutunut historia</strong>
        <p><strong>Havaittua:</strong> 11 258 Budjettihaukan momenttivuosiriviä, toteutunut BKT, valtionhallinnon EDP-velka ja korkomenot. <strong>Oletettua:</strong> vuoden 2026 politiikkarakenteen soveltuvuus aiempiin instituutioihin, toimien toteutumisaste, finanssikertoimet ja tarjontavaikutus. Tulos kertoo “jos nämä ehdot olisivat toteutuneet”, ei “näin olisi varmasti käynyt”.</p>
      </div>
      <div class="switches" aria-label="Valitse historiallisen vastelaskelman skenaario">
        <button type="button" data-history-case="suotuisa" aria-pressed="false">Suotuisa</button>
        <button type="button" data-history-case="keskinen" aria-pressed="true">Keskinen</button>
        <button type="button" data-history-case="varovainen" aria-pressed="false">Varovainen</button>
      </div>
      <p class="source">Varovainen skenaario ei oleta lainkaan pitkän aikavälin tarjontahyötyä. Kaikissa tapauksissa muut EDP-velan kanta-virtakorjaukset pidetään toteutuneen uran mukaisina.</p>
      <div class="metric-grid" aria-live="polite">
        <div class="metric"><span class="number" id="history-debt"></span><span class="label">mallinnettu valtionhallinnon EDP-velka 2025; toteutunut 209,1 mrd. €</span></div>
        <div class="metric"><span class="number" id="history-debt-ratio"></span><span class="label">mallinnettu velka/BKT 2025; toteutunut 74,2 %</span></div>
        <div class="metric"><span class="number" id="history-output"></span><span class="label">kumulatiivinen BKT-ero 2008–2025 vuoden 2025 hinnoin</span></div>
        <div class="metric"><span class="number" id="history-worst"></span><span class="label">heikoin vuotuinen BKT-poikkeama ja vuosi</span></div>
      </div>
      <div class="two-col">
        <div class="panel yellow">
          <h3>Toteutunut ja vaihtoehtoinen valtionvelka</h3>
          <div class="legend"><span><i class="swatch" style="background:#151512"></i>Toteutunut</span><span><i class="swatch" style="background:#006ca8"></i>Valittu skenaario</span><span><i class="swatch" style="background:#9bc8df"></i>Skenaariohaarukka</span></div>
          <div id="history-debt-chart" class="chart"></div>
          <p class="source">Valtionhallinnon EDP-velka vuoden viimeisellä neljänneksellä. Ei koko julkisyhteisöjen velka.</p>
        </div>
        <div class="panel">
          <h3>BKT-poikkeaman rakenne</h3>
          <div class="legend"><span><i class="swatch" style="background:#a92c22"></i>Kysyntä</span><span><i class="swatch" style="background:#18733a"></i>Tarjonta</span><span><i class="swatch" style="background:#006ca8"></i>Yhteensä</span></div>
          <div id="history-output-chart" class="chart"></div>
          <p class="source">Prosenttia saman vuoden toteutuneesta BKT:sta. Tarjontavaikutus on näkyvä oletus, ei havainto.</p>
        </div>
      </div>
      <div class="finding" id="history-finding"></div>
      <div class="two-col" style="margin-top:20px">
        <div class="panel">
          <h3>Momenttivarmennuksen kattavuus</h3>
          <div id="history-coverage-chart" class="chart"></div>
          <p class="source">Osuus rajatuista vuoden 2026 momenttimuutoksista, jolle löytyy samana vuonna vastaava toteumamomentti. Vihreä sarja poistaa rakennemurrosvartioidut osumat.</p>
        </div>
        <div class="panel yellow">
          <h3>Fakta, oletus ja mallin tulos</h3>
          <div class="logic-grid">
            <article class="logic-card"><h3>Havaittu</h3><ul id="history-observed"></ul></article>
            <article class="logic-card assumed"><h3>Oletettu</h3><ul id="history-assumed"></ul></article>
            <article class="logic-card modelled"><h3>Mallinnettu</h3><ul id="history-modelled"></ul></article>
          </div>
        </div>
      </div>
      <div class="table-wrap">
        <table aria-label="Historiallisen vastelaskelman valitut vuodet">
          <thead><tr><th>Vuosi</th><th>Toteutuneet menot</th><th>Suora tasapainohyöty</th><th>BKT-ero</th><th>Toteutunut velka</th><th>Vaihtoehtoinen velka</th></tr></thead>
          <tbody id="history-table"></tbody>
        </table>
      </div>
      <div id="history-events" class="history-event-grid"></div>
      <div class="callout">
        <h3>Robusti johtopäätös on suunta, ei pisteluku</h3>
        <p>Kaikissa kolmessa herkkyystapauksessa valtion velkaa kertyy selvästi toteutunutta vähemmän. Kansantalousvaikutuksen etumerkki ei ole yhtä vakaa: varovaisessa tapauksessa kumulatiivinen tuotanto jää toteutunutta pienemmäksi, keskisessä ja suotuisassa tarjontaoletus kääntää pitkän aikavälin eron positiiviseksi. Siksi velkavaikutusta voi pitää mallissa vahvempana johtopäätöksenä kuin tarkkaa BKT- tai työllisyysväitettä.</p>
      </div>
    </section>

    <section id="rakenne">
      <p class="eyebrow">03 · Paketin rakenne</p>
      <h2>Suurin riski ei ole hallinto vaan alueiden palvelurahoitus</h2>
      <p class="section-lede">Leikkaukset painottuvat valtiovarainministeriön hallinnonalalle, jossa ovat hyvinvointialueiden ja kuntien rahoitus. Yritystukien karsiminen on kasvun kannalta perustellumpaa, mutta TKI-tuki on erotettava säilyttävistä tuista.</p>
      <div class="two-col">
        <div class="panel">
          <h3>Menomuutos hallinnonaloittain</h3>
          <div id="department-chart" class="chart"></div>
          <p class="source">Hallinnonalatason summa. Negatiivinen on menon vähennys, positiivinen lisäys.</p>
        </div>
        <div class="panel">
          <h3>Verorakenteen vaihto</h3>
          <div id="tax-instrument-chart" class="chart"></div>
          <p class="source">Konsolidoitu arvio: valtion ansio- ja pääomatuloverot sekä oletettu aluevero on yhdistetty.</p>
        </div>
      </div>
      <div class="finding"><strong>Tulkinta.</strong> Työn verotuksen kevennys on paketin kasvumyönteinen elementti. Samaan aikaan kulutusverot nousevat nettona 5,04 mrd. euroa. Suomen tuore kerrointutkimus arvioi kulutusverojen viiden vuoden tuotantokustannuksen suureksi, joten verorakenteen vaihto pitää vaiheistaa ja kompensoida.</div>
      <div class="table-wrap">
        <table aria-label="Suurimmat menoleikkaukset"><thead><tr><th>Momentti</th><th>Menokohde</th><th>Muutos</th><th>Arvio</th></tr></thead><tbody id="largest-cuts"></tbody></table>
      </div>
    </section>

    <section id="makro">
      <p class="eyebrow">04 · Kansantalousskenaariot 2026-2035</p>
      <h2>Vaiheistus puolittaa pahimman iskun. Parempi koostumus leikkaa sitä vielä kolmanneksen.</h2>
      <p class="section-lede">Skenaariot yhdistävät Suomen 2026 instrumenttikohtaiset finanssikertoimet, automaattisten vakauttajien palautteen ja näkyvän pitkän aikavälin tarjontaherkkyyden. Valitse oletusjoukko nähdäksesi epävarmuuden.</p>
      <div class="switches" aria-label="Valitse oletusjoukko">
        <button type="button" data-case="optimistinen" aria-pressed="false">Optimistinen</button>
        <button type="button" data-case="keskinen" aria-pressed="true">Keskinen</button>
        <button type="button" data-case="varovainen" aria-pressed="false">Varovainen</button>
      </div>
      <div id="assumption-grid" class="assumption-grid" aria-live="polite"></div>
      <p class="source">Oletusjoukot ovat läpinäkyviä stressiskenaarioita, eivät todennäköisyysennusteita. Tarjontavaikutus on politiikkatavoitteen herkkyys; toistuvan tasapainon toteutumisaste leikkaa velkalaskelman pysyvää hyötyä mutta ei muuta lähdetaulukon ensimmäisen vuoden kirjanpitoa.</p>
      <div class="two-col">
        <div class="panel">
          <h3>BKT-poikkeama perusurasta</h3>
          <div class="legend"><span><i class="swatch" style="background:#a92c22"></i>Kerralla</span><span><i class="swatch" style="background:#006ca8"></i>Vaiheistettu</span><span><i class="swatch" style="background:#18733a"></i>Suositus</span></div>
          <div id="output-chart" class="chart"></div>
          <p class="source">Prosenttia saman vuoden nimellisestä perusuran BKT:sta. Kysyntävaikutus hiipuu; tarjontavaikutus kasvaa asteittain.</p>
        </div>
        <div class="panel">
          <h3>Julkinen EDP-velka suhteessa BKT:hen</h3>
          <div class="legend"><span><i class="swatch" style="background:#151512"></i>Tekninen perusura</span><span><i class="swatch" style="background:#a92c22"></i>Kerralla</span><span><i class="swatch" style="background:#006ca8"></i>Vaiheistettu</span><span><i class="swatch" style="background:#18733a"></i>Suositus</span></div>
          <div id="debt-chart" class="chart"></div>
          <p class="source">Suomen Pankin ennuste 2026-2028; vuodet 2029-2035 ovat tekninen jatko 3,2 % nimelliskasvulla ja 4,5 % alijäämällä.</p>
        </div>
      </div>
      <div id="scenario-cards" class="scenario-grid"></div>
      <div class="finding"><strong>Velkajohtopäätös.</strong> Paketti parantaa velkauraa kaikissa herkkyystapauksissa myös silloin, kun vain 75 prosenttia tunnistetusta toistuvasta tasapainohyödystä toteutuu. Tämä ei tee kertatoteutuksesta hyvää: huomattava velkakorjaus voidaan saavuttaa pienemmällä hyvinvointitappiolla suojaamalla korkean kertoimen menot ja hyväksymällä hitaampi alku.</div>
    </section>

    <section id="hyvinvointi">
      <p class="eyebrow">05 · Synteettinen kotitalousmikrosimulaatio</p>
      <h2>Työn verokevennys ei tee paketista automaattisesti tulonjaoltaan neutraalia</h2>
      <p class="section-lede">Julkiseen aggregaattidataan kalibroitu malli jakaa lähdetaulukon suorat vero- ja etuusmuutokset 90 painotetulle kotitaloussolulle. Keskilaskelma osoittaa suunnan ja riskiryhmät, mutta ei ole rekisteripohjainen SISU-tulos.</p>
      <div class="trust-banner">
        <strong>Suuntaa antava<br>ei virallinen</strong>
        <p><strong>Mitä tämä todistaa:</strong> lähdetaulukon omilla euromäärillä eläketulovähennyksen poisto, ALV-uudistus ja etuusmuutokset voivat ylittää työn verokevennyksen monissa pienituloisissa ja eläkeläistalouksissa. <strong>Mitä tämä ei todista:</strong> tarkkaa kotitalouskohtaista nettoa, köyhyysastetta tai Gini-vaikutusta. Ne vaativat SISU/FIONA-aineistosimulaation ja täsmälliset lakiparametrit.</p>
      </div>
      <div class="switches" aria-label="Valitse tulonjaon kohdentumisskenaario">
        <button type="button" data-micro-case="jakauma_suotuisa" aria-pressed="false">Suotuisampi</button>
        <button type="button" data-micro-case="keskinen" aria-pressed="true">Keskinen</button>
        <button type="button" data-micro-case="jakauma_rasittava" aria-pressed="false">Rasittavampi</button>
      </div>
      <p class="source">Skenaariot muuttavat työtulovähennyksen kohdentumista sekä välillisten verojen kotitalouskohtaantoa. Vaihteluväli ei ole tilastollinen luottamusväli.</p>
      <div class="micro-metrics" aria-live="polite">
        <div class="micro-metric"><strong id="micro-average"></strong><span>mallinnettu keskimääräinen muutos / kotitalous / vuosi</span></div>
        <div class="micro-metric"><strong id="micro-winners"></strong><span>voittavien kotitaloussolujen paino; karkea arvio</span></div>
        <div class="micro-metric"><strong id="micro-worst"></strong><span>heikoimman tulokymmenyksen keskimääräinen muutos / kotitalous</span></div>
        <div class="micro-metric"><strong id="micro-total"></strong><span>malliin sisältyvien suorien kotitalousvirtojen nettomuutos</span></div>
      </div>
      <div class="two-col micro-charts">
        <div class="panel yellow">
          <h3>Vaikutus tulokymmenyksittäin</h3>
          <span class="range-note"><i class="range-line"></i>kolmen kohdentumisskenaarion vaihteluväli</span>
          <div id="decile-chart" class="chart"></div>
          <p class="source">Euroa kotitaloutta kohti vuodessa. Tuloerät perustuvat Tilastokeskuksen vuoden 2024 asuntokuntiin; kulutuskohdennus vuoden 2022 kulutustutkimukseen.</p>
        </div>
        <div class="panel">
          <h3>Vaikutus kotitaloustyypeittäin</h3>
          <div id="household-type-chart" class="chart"></div>
          <p class="source">Painotettu keskiarvo saman elinvaiheen kotitalouksille. Solujen sisäinen hajonta ei näy.</p>
        </div>
      </div>
      <div class="finding" id="micro-finding"></div>
      <div class="table-wrap">
        <table aria-label="Synteettisen mikrosimulaation tulokset tulokymmenyksittäin">
          <thead><tr><th>Tulokymmenys</th><th>Muutos / kotitalous</th><th>Skenaarioiden vaihteluväli</th><th>Muutos suhteessa tuloon</th><th>Voittajaosuus</th></tr></thead>
          <tbody id="micro-decile-table"></tbody>
        </table>
      </div>
      <h3 style="margin-top:34px">Asuntotapahtumien vaikutus ei näy vuosikeskiarvossa</h3>
      <p class="section-lede">Varainsiirtoveron poisto hyödyttää ostajaa, mutta oman asunnon nimellisen myyntivoiton verotus voi tehdä muuttamisesta selvästi kalliimpaa. Esimerkit käyttävät vuoden 2026 nykyisiä 1,5/3 prosentin varainsiirtoveroja ja 30/34 prosentin pääomatuloveroa.</p>
      <div id="event-grid" class="event-grid"></div>
      <div class="coverage-grid">
        <article class="coverage"><h3>Mallissa mukana</h3><ul id="micro-included"></ul></article>
        <article class="coverage excluded"><h3>Ei väestökeskiarvossa</h3><ul id="micro-excluded"></ul></article>
      </div>
      <h3 style="margin-top:34px">Jäljelle jäävät politiikkariskit</h3>
      <div id="risk-list" class="risk-list"></div>
      <div class="callout">
        <h3>Johtopäätös: kompensaatio ja eläkeverotus on suunniteltava uudelleen ennen hyväksyntää</h3>
        <p>Budjetin 359,1 milj. euron ALV-kompensaatio on vain 8,8 prosenttia 4,086 mrd. euron bruttokertymästä. Samalla 2,345 mrd. euron eläketulovähennys poistuu ja sen tuotto siirretään työtulovähennykseen. Synteettinen laskelma näyttää tämän rakenteen voimakkaasti sukupolvien ja kotitaloustyyppien välisenä siirtona. Pienituloisten täysimääräinen automaattikompensaatio, eläkeläisten siirtymäsuoja ja SISU-varmennus ovat toteutuksen ennakkoehtoja.</p>
      </div>
    </section>

    <section id="toteutus">
      <p class="eyebrow">06 · Toteutusarkkitehtuuri</p>
      <h2>Ensin todennus, sitten rahat. Ei toisin päin.</h2>
      <p class="section-lede">Toteutusportit muuttavat poliittisen tavoitteen hallittavaksi ohjelmaksi. Jokainen vaihe voi pysähtyä ilman, että koko velkatavoite hylätään.</p>
      <div id="roadmap" class="roadmap"></div>
    </section>

    <section id="suositukset">
      <p class="eyebrow">07 · Kahdeksan politiikkasuositusta</p>
      <h2>Näin hyvinvointitappio pienenee ja kasvuhyöty vahvistuu</h2>
      <div id="recommendations" class="recommendations"></div>
    </section>

    <section id="menetelma">
      <p class="eyebrow">08 · Menetelmä ja lähteet</p>
      <h2>Selkeä ero faktan, mallin ja arvion välillä</h2>
      <div class="method-grid">
        <div class="panel">
          <h3>Mitä malli tekee</h3>
          <ul>
            <li>Täsmäyttää Google Sheets -snapshotin päätason budjettilukuihin.</li>
            <li>Konsolidoi hyvinvointialueiden rahoitus- ja verosiirron.</li>
            <li>Poistaa päästökauppatulon 503 milj. euron momenttisiirron veromuutoksesta.</li>
            <li>Maadoittaa vuosien 2008-2025 vastelaskelman 11 258 toteutuneeseen Budjettihaukan momenttivuosiriviin sekä viralliseen BKT-, velka- ja korkopolkuun.</li>
            <li>Soveltaa instrumenttikohtaisia viiden vuoden kertoimia näkyvillä vasteprofiileilla.</li>
            <li>Laskee velkapolkuun 0,5:n automaattisen vakauttajan ja varovaisen korkosäästön.</li>
            <li>Muodostaa 90 painotettua kotitaloussolua tulokymmenyksen ja elinvaiheen mukaan.</li>
            <li>Kalibroi vero- ja etuuskanavat lähdetaulukon kokonaissummiin sekä näyttää kolme kohdentumisherkkyyttä.</li>
          </ul>
        </div>
        <div class="panel yellow">
          <h3>Mitä malli ei tee</h3>
          <ul id="model-limits"></ul>
        </div>
      </div>
      <div class="sources" id="sources"></div>
      <div class="footer-note">Raportti on Liberaalipuolueen politiikkaraportti ja etsii esitykselle parhaan toteuttamiskelpoisen muodon. Suopea lähtökohta ei tarkoita suopeaa kirjanpitoa: kaikki kriittiset oikaisut, epävarmuudet ja pysäytysehdot näytetään. Historiallinen vastelaskelma, tulevaisuuden makroskenaariot ja synteettiset mikrolaskelmat ovat toistettavissa repon v2-analyysisyötteestä. Historiallinen vaihtoehtoura ei ole kausaalinen arvio, ja kampanjan lopulliset kotitalous-, köyhyys- ja kannustinväitteet edellyttävät erillistä SISU/FIONA-varmennusta.</div>
    </section>
  </main>

  <script type="application/json" id="report-data">__DATA__</script>
  <script>
    const data = JSON.parse(document.getElementById('report-data').textContent);
    const NS = 'http://www.w3.org/2000/svg';
    const colors = { kerralla: '#a92c22', vaiheistettu: '#006ca8', suojattu: '#18733a', ink: '#151512', grid: '#15151222', positive: '#18733a', negative: '#a92c22', neutral: '#766f61' };
    const euro = (value, digits = 2) => `${(value / 1e9).toLocaleString('fi-FI', {minimumFractionDigits: digits, maximumFractionDigits: digits})} mrd. €`;
    const signedEuro = (value, digits = 2) => `${value > 0 ? '+' : ''}${euro(value, digits)}`;
    const pct = (value, digits = 1) => `${value.toLocaleString('fi-FI', {minimumFractionDigits: digits, maximumFractionDigits: digits})} %`;
    const signedPct = (value, digits = 2) => `${value > 0 ? '+' : ''}${pct(value, digits)}`;
    const short = value => Math.abs(value) >= 1e9 ? `${(value / 1e9).toLocaleString('fi-FI', {maximumFractionDigits: 2})} mrd.` : `${(value / 1e6).toLocaleString('fi-FI', {maximumFractionDigits: 0})} milj.`;
    const householdEuro = value => `${value > 0 ? '+' : value < 0 ? '−' : ''}${Math.abs(value).toLocaleString('fi-FI', {maximumFractionDigits: 0})} €`;
    const scale = (d0, d1, r0, r1) => value => r0 + (value - d0) * (r1 - r0) / (d1 - d0 || 1);
    const svg = (id, height, label) => {
      const host = document.getElementById(id); host.innerHTML = '';
      const node = document.createElementNS(NS, 'svg');
      node.setAttribute('viewBox', `0 0 900 ${height}`); node.setAttribute('role', 'img'); node.setAttribute('aria-label', label);
      host.appendChild(node); return node;
    };
    const el = (parent, name, attrs = {}, text = '') => {
      const node = document.createElementNS(NS, name);
      Object.entries(attrs).forEach(([key, value]) => node.setAttribute(key, value));
      if (text) node.textContent = text; parent.appendChild(node); return node;
    };
    const title = (node, text) => el(node, 'title', {}, text);

    function accountingChart() {
      const central = data.accounting.central_budget;
      const rows = [
        { label: 'Lainanoton vähennys', value: -central.borrowing_change_eur, color: colors.positive },
        { label: 'Vero- ja muut tulomenetykset', value: -central.nonborrowing_revenue_change_eur, color: '#70437c' }
      ];
      const node = svg('accounting-chart', 320, 'Menovähennyksen käyttö lainanoton vähennykseen ja muiden tulojen menetykseen');
      const x = scale(0, -central.spending_change_eur, 90, 820);
      let start = 0;
      rows.forEach((row, index) => {
        const x0 = x(start), x1 = x(start + row.value);
        const rect = el(node, 'rect', { x: x0, y: 76, width: x1 - x0, height: 86, fill: row.color, stroke: colors.ink, 'stroke-width': 2 });
        title(rect, `${row.label}: ${euro(row.value)}`);
        el(node, 'text', { x: (x0 + x1) / 2, y: 112, 'text-anchor': 'middle', fill: 'white', class: 'bar-value' }, euro(row.value));
        el(node, 'text', { x: (x0 + x1) / 2, y: 190 + index * 37, 'text-anchor': 'middle', class: 'direct' }, row.label);
        start += row.value;
      });
      el(node, 'line', { x1: 90, y1: 258, x2: 820, y2: 258, class: 'axis' });
      [0, 4, 8, 12].forEach(v => { const xx = x(v * 1e9); el(node, 'line', {x1: xx, y1: 253, x2: xx, y2: 264, class: 'axis'}); el(node, 'text', {x: xx, y: 284, 'text-anchor': 'middle', class: 'tick'}, `${v} mrd.`); });
      el(node, 'text', { x: 90, y: 45, class: 'direct' }, `Menot vähenevät yhteensä ${euro(-central.spending_change_eur)}`);
    }

    function taxBridgeChart() {
      const central = data.accounting.central_budget;
      const pub = data.accounting.consolidated_public_sector;
      const rows = [
        { label: 'Valtion veroluku', value: central.tax_chapter_change_eur },
        { label: '− luokittelusiirto', value: -pub.emissions_reclassification_eur },
        { label: '+ aluevero', value: pub.regional_tax_assumption_eur },
        { label: '= maksajien netto', value: pub.payer_tax_burden_change_eur, result: true }
      ];
      horizontalBars('tax-bridge-chart', rows, 'Verorasituksen oikaisu', { height: 320, domain: [-3.3e9, 3.3e9], resultColor: '#006ca8' });
    }

    function horizontalBars(id, rows, label, options = {}) {
      const height = options.height || Math.max(280, 64 + rows.length * 44);
      const node = svg(id, height, label);
      const left = options.left || 245, right = 840, top = 24, bottom = height - 42;
      const formatValue = options.formatValue || signedEuro;
      const formatTick = options.formatTick || short;
      const values = rows.map(row => row.value);
      const limit = options.domain ? Math.max(Math.abs(options.domain[0]), Math.abs(options.domain[1])) : Math.max(...values.map(Math.abs)) * 1.12;
      const x = scale(-limit, limit, left, right);
      el(node, 'line', { x1: x(0), y1: top, x2: x(0), y2: bottom, class: 'zero' });
      rows.forEach((row, index) => {
        const y = top + index * ((bottom - top) / rows.length) + 5;
        const h = Math.min(27, (bottom - top) / rows.length - 9);
        const xx = x(row.value), zero = x(0);
        const fill = row.result ? (options.resultColor || colors.positive) : (row.value < 0 ? colors.negative : colors.positive);
        const rect = el(node, 'rect', { x: Math.min(xx, zero), y, width: Math.max(Math.abs(xx - zero), 1), height: h, fill, stroke: colors.ink, 'stroke-width': 1.3 });
        title(rect, `${row.label}: ${formatValue(row.value)}`);
        el(node, 'text', { x: left - 12, y: y + h * .7, 'text-anchor': 'end', class: 'direct' }, row.label);
        const anchor = row.value < 0 ? 'end' : 'start';
        const tx = xx + (row.value < 0 ? -8 : 8);
        el(node, 'text', { x: tx, y: y + h * .7, 'text-anchor': anchor, class: 'bar-value' }, formatValue(row.value));
      });
      [-limit, 0, limit].forEach(value => el(node, 'text', { x: x(value), y: height - 14, 'text-anchor': 'middle', class: 'tick' }, formatTick(value)));
    }

    function departmentChart() {
      const rows = data.expenditure_by_department.filter(row => Math.abs(row.change_eur) > 40e6).slice(0, 10).map(row => ({ label: row.label.replace('Ministeriön Hallinnonala', '').replace('Valtioneuvoston Kanslia', 'VNK'), value: row.change_eur }));
      horizontalBars('department-chart', rows, 'Suurimmat menomuutokset hallinnonaloittain', { height: 510 });
    }

    function taxInstrumentChart() {
      const t = data.tax_instruments;
      horizontalBars('tax-instrument-chart', [
        {label: 'Ansio- ja pääomatuloverot + aluevero', value: t.household_income_consolidated_eur},
        {label: 'Kulutusverot', value: t.consumption_taxes_eur},
        {label: 'Perintö- ja varainsiirtovero', value: t.inheritance_and_transfer_taxes_eur},
        {label: 'Yhteisövero', value: t.corporate_income_tax_eur},
        {label: 'Muut aidot veromuutokset', value: t.other_actual_taxes_eur},
        {label: 'Netto', value: t.payer_tax_burden_change_eur, result: true}
      ], 'Veroinstrumenttien muutokset', { height: 360, resultColor: '#006ca8' });
    }

    const historyYears = [2008, 2011, 2015, 2020, 2025];
    function historyAxes(node, x, y, minimum, maximum, valueFormat) {
      for (let index = 0; index <= 4; index += 1) {
        const value = minimum + (maximum - minimum) * index / 4;
        const yy = y(value);
        el(node, 'line', {x1:72, y1:yy, x2:838, y2:yy, class:'gridline'});
        el(node, 'text', {x:62, y:yy + 4, 'text-anchor':'end', class:'tick'}, valueFormat(value));
      }
      historyYears.forEach(year => el(node, 'text', {x:x(year), y:316, 'text-anchor':'middle', class:'tick'}, String(year)));
    }

    function historySeries(node, rows, x, y, valueKey, color, name, dash = '') {
      const path = rows.map((row, index) => `${index ? 'L' : 'M'}${x(row.year)},${y(row[valueKey])}`).join(' ');
      el(node, 'path', {d:path, fill:'none', stroke:color, 'stroke-width':3, 'stroke-dasharray':dash});
      rows.filter(row => historyYears.includes(row.year)).forEach(row => {
        const circle = el(node, 'circle', {cx:x(row.year), cy:y(row[valueKey]), r:3.5, fill:color, stroke:colors.ink, 'stroke-width':.8});
        title(circle, `${name}, ${row.year}: ${valueKey.includes('debt') ? euro(row[valueKey], 1) : signedPct(row[valueKey], 2)}`);
      });
      const last = rows.at(-1);
      el(node, 'text', {x:x(last.year) + 7, y:y(last[valueKey]) + 4, fill:color, class:'direct'}, name);
    }

    function historicalDebtChart(selected) {
      const history = data.historical_counterfactual;
      const envelope = history.envelope;
      const rows = selected.yearly;
      const node = svg('history-debt-chart', 340, 'Toteutunut ja vaihtoehtoinen valtionhallinnon EDP-velka 2008-2025');
      const x = scale(2008, 2025, 72, 805);
      const maximum = Math.max(...rows.map(row => row.actual_debt_eur), ...envelope.map(row => row.high_alternative_debt_eur)) * 1.08;
      const y = scale(0, maximum, 286, 24);
      historyAxes(node, x, y, 0, maximum, value => `${(value / 1e9).toLocaleString('fi-FI', {maximumFractionDigits:0})} mrd.`);
      const upper = envelope.map((row, index) => `${index ? 'L' : 'M'}${x(row.year)},${y(row.high_alternative_debt_eur)}`).join(' ');
      const lower = [...envelope].reverse().map(row => `L${x(row.year)},${y(row.low_alternative_debt_eur)}`).join(' ');
      el(node, 'path', {d:`${upper}${lower}Z`, fill:'#9bc8df88', stroke:'none'});
      historySeries(node, rows, x, y, 'actual_debt_eur', colors.ink, 'Toteutunut', '8 6');
      historySeries(node, rows, x, y, 'alternative_debt_eur', colors.blue || '#006ca8', selected.label, '');
    }

    function historicalOutputChart(selected) {
      const rows = selected.yearly;
      const node = svg('history-output-chart', 340, 'BKT-poikkeaman kysyntä-, tarjonta- ja kokonaisvaikutus 2008-2025');
      const x = scale(2008, 2025, 72, 805);
      const values = rows.flatMap(row => [row.demand_effect_pct, row.supply_effect_pct, row.total_output_effect_pct, 0]);
      let minimum = Math.min(...values), maximum = Math.max(...values);
      const pad = Math.max((maximum - minimum) * .12, .15);
      minimum -= pad; maximum += pad;
      const y = scale(minimum, maximum, 286, 24);
      historyAxes(node, x, y, minimum, maximum, value => pct(value, 1));
      if (minimum < 0 && maximum > 0) el(node, 'line', {x1:72, y1:y(0), x2:838, y2:y(0), class:'zero'});
      historySeries(node, rows, x, y, 'demand_effect_pct', colors.negative, 'Kysyntä', '6 5');
      historySeries(node, rows, x, y, 'supply_effect_pct', colors.positive, 'Tarjonta', '3 4');
      historySeries(node, rows, x, y, 'total_output_effect_pct', '#006ca8', 'Yhteensä', '');
    }

    function historicalCoverageChart() {
      const rows = data.historical_counterfactual.moment_grounding.yearly;
      const node = svg('history-coverage-chart', 340, 'Historiallisen momenttivarmennuksen kattavuus 2008-2025');
      const x = scale(2008, 2025, 72, 805), y = scale(0, 100, 286, 24);
      historyAxes(node, x, y, 0, 100, value => pct(value, 0));
      historySeries(node, rows, x, y, 'mapped_reference_share_pct', '#006ca8', 'Löytyvä momentti', '6 5');
      historySeries(node, rows, x, y, 'stable_reference_share_pct', colors.positive, 'Vertailukelpoinen', '');
    }

    let selectedHistoryCase = 'keskinen';
    function drawHistorical() {
      const history = data.historical_counterfactual;
      const selected = history.cases[selectedHistoryCase];
      const summary = selected.summary;
      document.querySelectorAll('[data-history-case]').forEach(button => button.setAttribute('aria-pressed', button.dataset.historyCase === selectedHistoryCase ? 'true' : 'false'));
      historicalDebtChart(selected);
      historicalOutputChart(selected);
      document.getElementById('history-debt').textContent = euro(summary.alternative_debt_2025_eur, 1);
      document.getElementById('history-debt-ratio').textContent = pct(summary.alternative_debt_ratio_2025_pct, 1);
      document.getElementById('history-output').textContent = signedEuro(summary.cumulative_output_effect_real_2025_eur, 1);
      document.getElementById('history-worst').textContent = `${signedPct(summary.worst_output_effect_pct, 2)} · ${summary.worst_output_year}`;
      document.getElementById('history-finding').innerHTML = `<strong>${selected.label}.</strong> Vuoden 2025 velka olisi mallissa ${euro(summary.alternative_debt_2025_eur, 1)}, eli ${euro(Math.abs(summary.debt_difference_2025_eur), 1)} toteutunutta pienempi. Suoraa tasapainohyötyä kertyy ${euro(summary.cumulative_direct_balance_improvement_eur, 1)} ja korkosäästöä ${euro(summary.cumulative_interest_saving_eur, 1)}. Kumulatiivinen tuotantoero on ${signedEuro(summary.cumulative_output_effect_real_2025_eur, 1)} vuoden 2025 hinnoin; tämä osa riippuu voimakkaasti tarjontaoletuksesta.`;

      const table = document.getElementById('history-table'); table.innerHTML = '';
      selected.yearly.filter(row => [2008, 2009, 2011, 2015, 2020, 2023, 2025].includes(row.year)).forEach(row => {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>${row.year}</td><td class="num">${euro(row.actual_expense_eur, 1)}</td><td class="num">${euro(row.direct_balance_improvement_eur, 1)}</td><td class="num">${signedPct(row.total_output_effect_pct, 2)}</td><td class="num">${euro(row.actual_debt_eur, 1)}</td><td class="num">${euro(row.alternative_debt_eur, 1)}</td>`;
        table.appendChild(tr);
      });
    }

    function populateHistorical() {
      const history = data.historical_counterfactual;
      const central = history.cases.keskinen.summary;
      const finalValues = Object.values(history.cases).map(item => item.summary.alternative_debt_2025_eur);
      document.getElementById('history-verdict').innerHTML = `<strong>Historiallinen peili</strong> viittaa olennaiseen velkaeroon: keskiskenaariossa valtionhallinnon EDP-velka olisi vuonna 2025 noin ${euro(central.alternative_debt_2025_eur, 0)}, herkkyysvälillä ${euro(Math.min(...finalValues), 0)}–${euro(Math.max(...finalValues), 0)}, kun toteutunut kanta oli ${euro(central.actual_debt_2025_eur, 0)}. Tämä on ehdollinen vastelaskelma, ei havaittu tai kausaalisesti tunnistettu historia.`;
      [['history-observed', history.method.observed], ['history-assumed', history.method.assumed], ['history-modelled', history.method.modelled]].forEach(([id, items]) => {
        const host = document.getElementById(id);
        items.forEach(item => { const li = document.createElement('li'); li.textContent = item; host.appendChild(li); });
      });
      const eventHost = document.getElementById('history-events');
      history.structural_events.sort((a, b) => a.year - b.year).forEach(item => {
        const card = document.createElement('article'); card.className = 'history-event';
        card.innerHTML = `<span>${item.year}</span><strong>${item.label_fi}</strong><p>${item.description_fi}</p>`;
        eventHost.appendChild(card);
      });
      historicalCoverageChart();
    }

    function decileChart(rows) {
      const envelope = new Map(data.microsimulation.decile_envelope.map(row => [row.decile, row]));
      const node = svg('decile-chart', 410, 'Kotitalouksien vuosimuutos tulokymmenyksittäin ja skenaarioiden vaihteluväli');
      const left = 64, right = 850, top = 28, bottom = 338;
      const all = rows.flatMap(row => {
        const range = envelope.get(Number(row.group_id));
        return [row.change_per_household_eur, range.low_change_per_household_eur, range.high_change_per_household_eur];
      });
      let minimum = Math.min(0, ...all), maximum = Math.max(0, ...all);
      const pad = Math.max((maximum - minimum) * .13, 300);
      minimum -= pad; maximum += pad;
      const y = scale(minimum, maximum, bottom, top);
      const band = (right - left) / rows.length;
      for (let index = 0; index <= 5; index += 1) {
        const value = minimum + (maximum - minimum) * index / 5;
        const yy = y(value);
        el(node, 'line', {x1:left, y1:yy, x2:right, y2:yy, class:'gridline'});
        el(node, 'text', {x:left - 8, y:yy + 4, 'text-anchor':'end', class:'tick'}, householdEuro(value));
      }
      el(node, 'line', {x1:left, y1:y(0), x2:right, y2:y(0), class:'zero'});
      rows.forEach((row, index) => {
        const decile = Number(row.group_id), range = envelope.get(decile);
        const cx = left + band * (index + .5);
        const rangeTop = y(range.high_change_per_household_eur);
        const rangeBottom = y(range.low_change_per_household_eur);
        el(node, 'line', {x1:cx, y1:rangeTop, x2:cx, y2:rangeBottom, stroke:colors.ink, 'stroke-width':2});
        el(node, 'line', {x1:cx - 7, y1:rangeTop, x2:cx + 7, y2:rangeTop, stroke:colors.ink, 'stroke-width':2});
        el(node, 'line', {x1:cx - 7, y1:rangeBottom, x2:cx + 7, y2:rangeBottom, stroke:colors.ink, 'stroke-width':2});
        const valueY = y(row.change_per_household_eur), zeroY = y(0);
        const rect = el(node, 'rect', {x:cx - 20, y:Math.min(valueY, zeroY), width:40, height:Math.max(Math.abs(valueY - zeroY), 1), fill:row.change_per_household_eur >= 0 ? colors.positive : colors.negative, stroke:colors.ink, 'stroke-width':1.3});
        title(rect, `Tulokymmenys ${decile}: ${householdEuro(row.change_per_household_eur)} vuodessa; vaihteluväli ${householdEuro(range.low_change_per_household_eur)}–${householdEuro(range.high_change_per_household_eur)}`);
        const labelY = row.change_per_household_eur >= 0 ? valueY - 7 : valueY + 16;
        el(node, 'text', {x:cx, y:labelY, 'text-anchor':'middle', class:'bar-value'}, householdEuro(row.change_per_household_eur));
        el(node, 'text', {x:cx, y:bottom + 25, 'text-anchor':'middle', class:'direct'}, `D${decile}`);
      });
    }

    let selectedMicroCase = 'keskinen';
    function drawMicrosimulation() {
      const micro = data.microsimulation;
      const selected = micro.cases[selectedMicroCase];
      document.querySelectorAll('[data-micro-case]').forEach(button => button.setAttribute('aria-pressed', button.dataset.microCase === selectedMicroCase ? 'true' : 'false'));
      decileChart(selected.by_decile);
      horizontalBars(
        'household-type-chart',
        [...selected.by_household_type].sort((a, b) => a.change_per_household_eur - b.change_per_household_eur).map(row => ({label:row.label, value:row.change_per_household_eur})),
        'Kotitalouden käytettävissä olevien resurssien vuosimuutos elinvaiheen mukaan',
        {height:470, left:300, formatValue:householdEuro, formatTick:householdEuro}
      );

      const worstDecile = [...selected.by_decile].sort((a, b) => a.change_per_household_eur - b.change_per_household_eur)[0];
      const worstType = [...selected.by_household_type].sort((a, b) => a.change_per_household_eur - b.change_per_household_eur)[0];
      const bestType = [...selected.by_household_type].sort((a, b) => b.change_per_household_eur - a.change_per_household_eur)[0];
      document.getElementById('micro-average').textContent = householdEuro(selected.summary.average_change_per_household_eur);
      document.getElementById('micro-winners').textContent = pct(selected.summary.winner_households_pct, 0);
      document.getElementById('micro-worst').textContent = `D${worstDecile.group_id}: ${householdEuro(worstDecile.change_per_household_eur)}`;
      document.getElementById('micro-total').textContent = signedEuro(selected.summary.modeled_household_resource_change_eur, 2);
      document.getElementById('micro-finding').innerHTML = `<strong>Valitun skenaarion tulkinta.</strong> Heikoin kotitaloustyyppi on ${worstType.label.toLowerCase()} (${householdEuro(worstType.change_per_household_eur)} vuodessa), vahvin ${bestType.label.toLowerCase()} (${householdEuro(bestType.change_per_household_eur)}). Tulokymmenys D${worstDecile.group_id} menettää keskimäärin ${householdEuro(Math.abs(worstDecile.change_per_household_eur))} vuodessa. Tulos syntyy ennen käyttäytymisvaikutuksia ja julkisten palvelujen luontoismuotoisia vaikutuksia.`;

      const table = document.getElementById('micro-decile-table'); table.innerHTML = '';
      const envelope = new Map(micro.decile_envelope.map(row => [row.decile, row]));
      selected.by_decile.forEach(row => {
        const range = envelope.get(Number(row.group_id));
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>D${row.group_id}</td><td class="num">${householdEuro(row.change_per_household_eur)} / v</td><td class="num">${householdEuro(range.low_change_per_household_eur)} – ${householdEuro(range.high_change_per_household_eur)}</td><td class="num">${signedPct(row.change_pct_disposable_income, 1)}</td><td class="num">${pct(row.winner_households_pct, 0)}</td>`;
        table.appendChild(tr);
      });
    }

    function populateMicrosimulation() {
      const micro = data.microsimulation;
      const eventHost = document.getElementById('event-grid');
      micro.event_examples.forEach(item => {
        const card = document.createElement('article'); card.className = 'event';
        card.innerHTML = `<h3>${item.label}</h3><span class="event-value" style="color:${item.net_household_change_eur >= 0 ? colors.positive : colors.negative}">${householdEuro(item.net_household_change_eur)}</span><p>Poistuva varainsiirtovero ${householdEuro(item.transfer_tax_saved_eur)}. Uusi luovutusvoittovero ${householdEuro(-item.capital_gains_tax_added_eur)}.</p><p class="source">${item.assumption}</p>`;
        eventHost.appendChild(card);
      });
      const included = document.getElementById('micro-included');
      micro.coverage.included.forEach(text => { const li = document.createElement('li'); li.textContent = text; included.appendChild(li); });
      const excluded = document.getElementById('micro-excluded');
      micro.coverage.excluded_from_population_average.forEach(text => { const li = document.createElement('li'); li.textContent = text; excluded.appendChild(li); });
    }

    function lineChart(id, series, valueKey, label, zero = false) {
      const node = svg(id, 330, label);
      const left = 67, right = 832, top = 26, bottom = 275;
      const all = series.flatMap(s => s.rows.map(row => row[valueKey]));
      let min = Math.min(...all), max = Math.max(...all);
      const pad = Math.max((max - min) * .12, .4);
      min -= pad; max += pad;
      if (zero) { min = Math.min(min, 0); max = Math.max(max, 0); }
      const x = scale(2026, 2035, left, right), y = scale(min, max, bottom, top);
      for (let i = 0; i <= 4; i += 1) {
        const value = min + (max - min) * i / 4, yy = y(value);
        el(node, 'line', {x1: left, y1: yy, x2: right, y2: yy, class: 'gridline'});
        el(node, 'text', {x: left - 9, y: yy + 4, 'text-anchor': 'end', class: 'tick'}, pct(value, 1));
      }
      if (min < 0 && max > 0) el(node, 'line', {x1: left, y1: y(0), x2: right, y2: y(0), class: 'zero'});
      [2026, 2028, 2030, 2032, 2035].forEach(year => el(node, 'text', {x: x(year), y: bottom + 25, 'text-anchor': 'middle', class: 'tick'}, String(year)));
      series.forEach(item => {
        const d = item.rows.map((row, index) => `${index ? 'L' : 'M'}${x(row.year)},${y(row[valueKey])}`).join(' ');
        el(node, 'path', {d, fill: 'none', stroke: item.color, 'stroke-width': item.width || 3, 'stroke-dasharray': item.dash || ''});
        item.rows.forEach(row => { const c = el(node, 'circle', {cx: x(row.year), cy: y(row[valueKey]), r: 3.2, fill: item.color, stroke: colors.ink, 'stroke-width': .8}); title(c, `${item.name}, ${row.year}: ${pct(row[valueKey], 2)}`); });
        const last = item.rows[item.rows.length - 1];
        el(node, 'text', {x: x(last.year) + 8, y: y(last[valueKey]) + 4, fill: item.color, class: 'direct'}, item.name);
      });
    }

    let selectedCase = 'keskinen';
    function drawScenarios() {
      document.querySelectorAll('[data-case]').forEach(button => button.setAttribute('aria-pressed', button.dataset.case === selectedCase ? 'true' : 'false'));
      const entries = Object.entries(data.scenarios).map(([id, scenario]) => ({id, scenario, rows: scenario.cases[selectedCase].yearly, summary: scenario.cases[selectedCase].summary, parameters: scenario.cases[selectedCase].parameters}));
      lineChart('output-chart', entries.map(item => ({name: item.scenario.short_label, rows: item.rows, color: colors[item.id]})), 'total_output_effect_pct', 'BKT-poikkeama perusurasta', true);
      const debtSeries = [{name: 'Perusura', rows: entries[0].rows.map(r => ({year:r.year, value:r.baseline_debt_pct})), color:colors.ink, dash:'8 6', width:2.5}, ...entries.map(item => ({name:item.scenario.short_label, rows:item.rows.map(r => ({year:r.year,value:r.alternative_debt_pct})), color:colors[item.id]}))];
      lineChart('debt-chart', debtSeries, 'value', 'EDP-velkasuhde');

      const p = entries[0].parameters;
      const assumptions = [
        [`${p.spending.toLocaleString('fi-FI', {maximumFractionDigits: 2})}`, 'menojen 5 v. kerroin'],
        [`${p.investment.toLocaleString('fi-FI', {maximumFractionDigits: 2})}`, 'investointien 5 v. kerroin'],
        [`${p.consumption_tax.toLocaleString('fi-FI', {maximumFractionDigits: 2})}`, 'kulutusveron 5 v. kerroin'],
        [`${p.household_income_tax.toLocaleString('fi-FI', {maximumFractionDigits: 2})}`, 'kotitalouksien tuloveron 5 v. kerroin'],
        [`${p.corporate_tax.toLocaleString('fi-FI', {maximumFractionDigits: 2})}`, 'yhteisöveron 5 v. kerroin'],
        [pct(100 * p.recurring_balance_delivery, 0), 'toistuvan tasapainohyödyn toteutuminen'],
        [`+${pct(data.scenarios.suojattu.cases[selectedCase].summary.long_run_supply_target_pct, 2)}`, 'tarjontaherkkyys 2035, suositusura']
      ];
      const assumptionHost = document.getElementById('assumption-grid'); assumptionHost.innerHTML = '';
      assumptions.forEach(([value, label]) => {
        const card = document.createElement('div'); card.className = 'assumption';
        card.innerHTML = `<strong>${value}</strong><span>${label}</span>`;
        assumptionHost.appendChild(card);
      });

      const host = document.getElementById('scenario-cards'); host.innerHTML = '';
      entries.forEach(item => {
        const card = document.createElement('article'); card.className = `scenario-card ${item.id === 'suojattu' ? 'recommended' : ''}`;
        card.innerHTML = `<h3>${item.scenario.label}</h3><span class="big">${signedPct(item.summary.worst_output_effect_pct)}</span><span class="source">pahin BKT-poikkeama (${item.summary.worst_output_year})</span><dl><dt>Kumulatiivinen tuotantovaikutus 2026-2030</dt><dd>${signedEuro(item.summary.cumulative_output_effect_2026_2030_eur, 1)}</dd><dt>Toistuva tasapainohyöty</dt><dd>${euro(item.summary.delivered_recurring_balance_improvement_eur, 1)}</dd><dt>Velkasuhde 2035</dt><dd>${pct(item.summary.debt_ratio_2035_pct)}</dd><dt>Ero perusuraan</dt><dd>${signedPct(item.summary.debt_ratio_difference_2035_pp, 1)}-yks.</dd><dt>Tarjontaherkkyys 2035</dt><dd>+${pct(item.summary.long_run_supply_target_pct, 2)}</dd></dl>`;
        host.appendChild(card);
      });
    }

    function populate() {
      const central = data.accounting.central_budget, pub = data.accounting.consolidated_public_sector;
      const recommended = data.scenarios.suojattu.cases.keskinen;
      document.getElementById('hero-balance').textContent = euro(pub.direct_balance_improvement_eur);
      document.getElementById('hero-tax').textContent = signedEuro(pub.payer_tax_burden_change_eur);
      document.getElementById('hero-output').textContent = signedPct(recommended.summary.worst_output_effect_pct);
      document.getElementById('hero-debt').textContent = pct(recommended.summary.debt_ratio_2035_pct);
      document.getElementById('hero-debt-base').textContent = pct(recommended.yearly.at(-1).baseline_debt_pct);
      document.getElementById('metric-state-tax').textContent = signedEuro(central.tax_chapter_change_eur);
      document.getElementById('metric-nontax').textContent = signedEuro(pub.adjusted_non_tax_revenue_change_eur);
      document.getElementById('metric-recurring').textContent = euro(pub.identified_recurring_balance_improvement_eur);

      const cuts = document.getElementById('largest-cuts');
      data.largest_spending_cuts.slice(0, 10).forEach(item => {
        const tr = document.createElement('tr');
        const assessment = item.moment === '32.20.40.' ? 'Suojaa tai kohdista uudelleen: TKI-ulkoisvaikutus.' : item.moment === '28.89.31.' || item.moment === '28.90.30.' ? 'Edellyttää tehtävä- ja rahoitusuudistusta.' : item.moment.endsWith('.77.') ? 'Korkean kertoimen investointiriski.' : 'Momenttikohtainen vaikuttavuustesti.';
        tr.innerHTML = `<td>${item.moment}</td><td>${item.label}</td><td class="num">${signedEuro(item.change_eur)}</td><td>${assessment}</td>`;
        cuts.appendChild(tr);
      });

      const riskHost = document.getElementById('risk-list');
      data.distributional_stress_tests.forEach(item => {
        const row = document.createElement('article'); row.className = 'risk';
        row.innerHTML = `<span class="risk-name">${item.reform}</span><span class="risk-level ${item.risk}">${item.risk}</span><p><strong>Vaikutuskanava:</strong> ${item.incidence}</p><p><strong>Hyväksymisportti:</strong> ${item.gate}</p>`;
        riskHost.appendChild(row);
      });

      const roadmap = document.getElementById('roadmap');
      data.implementation_gates.forEach(item => {
        const stage = document.createElement('article'); stage.className = 'stage'; stage.dataset.share = item.share_pct ? `${item.share_pct} %` : '0';
        stage.innerHTML = `<span class="timing">${item.timing}</span><h3>${item.stage}</h3><ul>${item.actions.map(action => `<li>${action}</li>`).join('')}</ul><p class="stop">Pysäytysehto: ${item.stop_rule}</p>`;
        roadmap.appendChild(stage);
      });

      const recs = document.getElementById('recommendations');
      data.recommendations.forEach(item => {
        const rec = document.createElement('article'); rec.className = 'rec';
        rec.innerHTML = `<span class="rec-num">${item.priority}</span><div><h3>${item.title}</h3><p>${item.text}</p></div>`;
        recs.appendChild(rec);
      });

      const limits = document.getElementById('model-limits');
      data.model_limits.forEach(item => { const li = document.createElement('li'); li.textContent = item; limits.appendChild(li); });
      const sources = document.getElementById('sources');
      data.research_basis.forEach(item => {
        const row = document.createElement('div'); row.className = 'source-row';
        row.innerHTML = `<a href="${item.url}" target="_blank" rel="noopener noreferrer">${item.label}</a><span>${item.use}</span>`;
        sources.appendChild(row);
      });
    }

    populate(); populateHistorical(); populateMicrosimulation(); accountingChart(); taxBridgeChart(); departmentChart(); taxInstrumentChart(); drawHistorical(); drawScenarios(); drawMicrosimulation();
    document.querySelectorAll('[data-history-case]').forEach(button => button.addEventListener('click', () => { selectedHistoryCase = button.dataset.historyCase; drawHistorical(); }));
    document.querySelectorAll('[data-case]').forEach(button => button.addEventListener('click', () => { selectedCase = button.dataset.case; drawScenarios(); }));
    document.querySelectorAll('[data-micro-case]').forEach(button => button.addEventListener('click', () => { selectedMicroCase = button.dataset.microCase; drawMicrosimulation(); }));
  </script>
</body>
</html>
'''


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the alternative-budget browser report.")
    parser.add_argument("--analysis", type=Path, default=ANALYSIS)
    parser.add_argument("--out", type=Path, default=OUTPUT)
    args = parser.parse_args()

    analysis = json.loads(args.analysis.read_text(encoding="utf-8"))
    embedded = json.dumps(analysis, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    rendered = TEMPLATE.replace("__DATA__", embedded)
    if "__DATA__" in rendered:
        raise ValueError("Report data placeholder was not replaced")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(rendered, encoding="utf-8")
    try:
        output_label = args.out.relative_to(ROOT)
    except ValueError:
        output_label = args.out
    print(f"{output_label} ({args.out.stat().st_size // 1024} kt)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

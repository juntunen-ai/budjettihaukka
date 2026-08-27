#!/usr/bin/env python3
"""Build a transparent hourly debt-rate comparison visualization."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PARTY_INPUT = ROOT / "data" / "reference" / "party_debt_cumulative_v1.json"
SCENARIO_INPUT = (
    ROOT
    / "data"
    / "reference"
    / "liberaali_vaihtoehtobudjetti"
    / "historiallinen_vastelaskelma_v1.json"
)
OUTPUT = ROOT / "velka-tunnissa.html"


def next_month_start(month: str) -> date:
    year, number, _ = map(int, month.split("-"))
    return date(year + 1, 1, 1) if number == 12 else date(year, number + 1, 1)


def observed_rate(monthly: list[dict], cabinet_name: str) -> dict:
    rows = [row for row in monthly if row["cabinet_name"] == cabinet_name]
    # The final July 2026 record is a zero placeholder rather than a completed
    # month, so it must not dilute the observed Orpo rate.
    if rows and rows[-1]["debt_change_eur"] == 0:
        rows.pop()
    if not rows:
        raise ValueError(f"No monthly debt observations for {cabinet_name}")
    cabinet_labels = {
        "Marin": "Marinin hallitus",
        "Orpo": "Orpon hallitus",
    }

    start = date.fromisoformat(rows[0]["month"])
    end = next_month_start(rows[-1]["month"])
    hours = (end - start).days * 24
    debt_change = sum(float(row["debt_change_eur"]) for row in rows)
    return {
        "id": cabinet_name.lower(),
        "label": cabinet_labels[cabinet_name],
        "kind": "observed",
        "period": f"{rows[0]['month'][:7]}–{rows[-1]['month'][:7]}",
        "months": len(rows),
        "hours": hours,
        "debt_change_eur": debt_change,
        "rate_eur_per_hour": debt_change / hours,
        "annualized_debt_change_eur": debt_change / hours * (365 * 24),
        "cabinet_name": cabinet_name,
    }


def build_payload() -> dict:
    party = json.loads(PARTY_INPUT.read_text(encoding="utf-8"))
    scenario = json.loads(SCENARIO_INPUT.read_text(encoding="utf-8"))
    marin = observed_rate(party["monthly"], "Marin")
    orpo = observed_rate(party["monthly"], "Orpo")

    central = scenario["cases"]["keskinen"]["summary"]
    annual_effect = float(central["direct_balance_improvement_2025_eur"])
    observed_2025 = [
        row for row in party["monthly"] if row["month"].startswith("2025-")
    ]
    observed_2025_debt_change = sum(
        float(row["debt_change_eur"]) for row in observed_2025
    )
    liberal = {
        "id": "liberaali",
        "label": "Liberaalien ehdotus",
        "kind": "modelled_counterfactual",
        "period": None,
        "hours": 365 * 24,
        "baseline_debt_change_eur": observed_2025_debt_change,
        "direct_balance_improvement_eur": annual_effect,
        "debt_change_eur": observed_2025_debt_change - annual_effect,
        "rate_eur_per_hour": (observed_2025_debt_change - annual_effect) / (365 * 24),
        "annualized_debt_change_eur": observed_2025_debt_change - annual_effect,
        "scenario_case": "Keskinen",
        "implementation": "Vaiheistus 10 %, 30 %, 60 % ja 100 % vuosina 2008–2011",
    }

    return {
        "meta": {
            "dataset_id": "debt_per_hour_v1",
            "observed_measure": party["meta"]["measure"],
            "observed_sign_rule": party["meta"]["sign_rule"],
            "observed_attribution_rule": party["meta"]["attribution_rule"],
            "scenario_trust_class": scenario["meta"]["trust_class"],
            "scenario_causal_claim": scenario["meta"]["causal_claim"],
            "scenario_scope": scenario["meta"]["headline_scope"],
            "sources": [
                {
                    "label": "Valtiokonttori: talousarviotalouden nettokertymät",
                    "url": "https://www.tutkihallintoa.fi/valtio/taloustiedot/talousarviotalous-eli-budjettitalous/",
                },
                {
                    "label": "Valtioneuvosto: hallitukset ja ministerit",
                    "url": "https://valtioneuvosto.fi/hallitukset-ja-ministerit/hallitukset/",
                },
            ],
        },
        "observed": [marin, orpo],
        "modelled_counterfactual": liberal,
    }


TEMPLATE = r'''<!doctype html>
<html lang="fi">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta name="description" content="Havainnollistus Marinin ja Orpon hallitusten toteutuneesta nettovelanotosta tunnissa sekä Liberaalien ehdotuksen laskennallisesta vaikutuksesta.">
  <link rel="icon" href="data:,">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Archivo+Black&family=Source+Sans+3:ital,wght@0,400;0,600;0,700;0,800&display=swap" rel="stylesheet">
  <title>Velkaantuminen tunnissa | Budjettihaukka</title>
  <style>
    :root { --ink:#151512; --paper:#f8f3e8; --white:#fffdf7; --yellow:#FFD500; --orange:#F9B000; --yellow-deep:#9a6700; --red:#c43732; --blue:#006ca8; --green:#148142; --muted:#625d52; --line:#15151233; --shadow:7px 7px 0 var(--ink); }
    * { box-sizing:border-box; }
    html { scroll-behavior:smooth; }
    body { margin:0; color:var(--ink); background:var(--paper); font:18px/1.5 "Source Sans 3", sans-serif; }
    .wrap { width:min(1160px, calc(100% - 40px)); margin-inline:auto; }
    .hero { overflow:hidden; border-bottom:3px solid var(--ink); background:var(--yellow); }
    .hero-inner { position:relative; padding:24px 0 62px; }
    .hero-inner::after { position:absolute; right:-16px; bottom:-155px; color:#fff5; content:"€"; font:18rem/.9 "Archivo Black", sans-serif; }
    .back { position:relative; z-index:1; display:inline-flex; padding:7px 12px; border:2px solid var(--ink); border-radius:999px; background:var(--white); color:var(--ink); font-weight:800; text-decoration:none; }
    h1,h2,h3,.rate { font-family:"Archivo Black","Arial Black",sans-serif; letter-spacing:-.045em; }
    h1 { position:relative; z-index:1; max-width:1000px; margin:62px 0 18px; font-size:clamp(3.1rem,8vw,7rem); line-height:.86; text-transform:uppercase; }
    h1 span { display:block; color:var(--white); -webkit-text-stroke:2px var(--ink); text-shadow:4px 4px 0 var(--ink); }
    .lede { position:relative; z-index:1; max-width:800px; margin:0; font-size:clamp(1.1rem,2.2vw,1.42rem); font-weight:700; }
    .notice { position:relative; z-index:1; max-width:910px; margin-top:25px; padding:15px 18px; border:2px solid var(--ink); background:#fff5c8; box-shadow:4px 4px 0 var(--ink); font-weight:700; }
    main { padding:62px 0 84px; }
    .eyebrow { margin:0 0 8px; color:var(--muted); font-size:.82rem; font-weight:800; letter-spacing:.13em; text-transform:uppercase; }
    h2 { max-width:900px; margin:0; font-size:clamp(2rem,5vw,4.4rem); line-height:.95; }
    .intro { max-width:810px; margin:16px 0 0; color:var(--muted); font-size:1.1rem; }
    .rate-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:20px; margin-top:32px; }
    .rate-card { position:relative; min-height:470px; padding:24px; border:2px solid var(--ink); background:var(--white); box-shadow:var(--shadow); overflow:hidden; container-type:inline-size; }
    .rate-card::after { position:absolute; right:-12px; bottom:-60px; color:#1515120b; content:"+"; font:15rem/1 "Archivo Black",sans-serif; }
    .rate-card.marin { border-top:12px solid var(--red); }
    .rate-card.orpo { border-top:12px solid var(--red); }
    .rate-card.liberaali { border-top:12px solid var(--orange); background:var(--white); }
    .pill { position:relative; z-index:1; display:inline-block; padding:4px 9px; border:1.5px solid var(--ink); border-radius:999px; background:var(--paper); font-size:.76rem; font-weight:800; letter-spacing:.06em; text-transform:uppercase; }
    .liberaali .pill { background:var(--yellow); }
    h3 { position:relative; z-index:1; margin:22px 0 4px; font-size:1.5rem; line-height:1; }
    .period { position:relative; z-index:1; margin:0; color:var(--muted); font-size:.95rem; font-weight:700; }
    .rate { position:relative; z-index:1; margin:38px 0 6px; font-size:clamp(2.1rem,3vw,3.3rem); line-height:.85; white-space:nowrap; }
    .marin .rate { color:var(--red); } .orpo .rate { color:var(--red); } .liberaali .rate { color:var(--green); }
    .rate-caption { position:relative; z-index:1; margin:0; font-size:1.05rem; font-weight:800; }
    .direction { position:relative; z-index:1; display:grid; justify-items:stretch; margin:18px 0 0; text-align:left; }
    .direction-arrow { display:block; justify-self:end; font:clamp(5.2rem,10cqi,8.5rem)/.64 "Archivo Black","Arial Black",sans-serif; letter-spacing:-.12em; }
    .marin .direction { color:var(--red); } .orpo .direction { color:var(--red); } .liberaali .direction { color:var(--green); }
    @container (min-width:560px) { h3,.period,.annual { max-width:52%; } .direction { position:absolute; z-index:2; top:205px; right:40px; width:min(280px,38%); margin:0; } }
    .annual { position:relative; z-index:1; margin:16px 0 0; color:var(--muted); font-size:.92rem; font-weight:700; }
    .annual strong { display:block; margin-top:2px; color:var(--ink); font:clamp(2.15rem,2.75vw,3rem)/.9 "Archivo Black","Arial Black",sans-serif; letter-spacing:-.045em; white-space:nowrap; }
    .marin .annual strong { color:var(--red); } .orpo .annual strong { color:var(--red); } .liberaali .annual strong { color:var(--green); }
    .card-note { position:relative; z-index:1; margin:21px 0 0; padding-top:14px; border-top:1.5px solid var(--line); color:var(--muted); font-size:.86rem; }
    .hour { margin-top:52px; padding:32px; border:2px solid var(--ink); background:var(--ink); color:var(--white); box-shadow:var(--shadow); }
    .hour h2 { max-width:700px; color:var(--yellow); font-size:clamp(1.8rem,4vw,3.35rem); }
    .hour p { max-width:760px; margin:13px 0 0; color:#f8f3e8cc; }
    .bars { display:grid; gap:18px; margin-top:30px; }
    .bar-row { display:grid; grid-template-columns:150px minmax(0,1fr) 150px; align-items:center; gap:14px; }
    .bar-label { font-weight:800; }
    .bar-track { height:36px; overflow:hidden; border:1px solid #fff8; background:#fff2; }
    .bar-fill { width:0; height:100%; transition:width 1.2s cubic-bezier(.2,.8,.2,1); }
    .bar-fill.marin { background:var(--red); } .bar-fill.orpo { background:var(--red); } .bar-fill.liberaali { background:var(--green); }
    .bar-value { font-family:"Archivo Black",sans-serif; text-align:right; }
    .method { display:grid; grid-template-columns:1fr 1fr; gap:20px; margin-top:65px; }
    .method article { padding:24px; border:2px solid var(--ink); background:var(--white); }
    .method h3 { margin:0 0 11px; font-size:1.38rem; }
    .method p { margin:0; }
    .source { margin-top:32px; color:var(--muted); font-size:.9rem; }
    .source a { color:inherit; font-weight:700; }
    @media (max-width:840px) { .rate-grid { grid-template-columns:1fr; } .rate-card { min-height:285px; } .method { grid-template-columns:1fr; } }
    @media (max-width:620px) { body { font-size:16px; } .wrap { width:min(100% - 28px,1160px); } .hero-inner { padding-bottom:42px; } h1 { margin-top:50px; } .bar-row { grid-template-columns:1fr; gap:6px; } .bar-value { text-align:left; } .rate { white-space:normal; } }
  </style>
</head>
<body>
  <header class="hero"><div class="wrap hero-inner">
    <a class="back" href="visualisointidemot.html">&larr; Visualisointidemot</a>
    <h1>Velkaantuminen<span>tunnissa</span></h1>
    <p class="lede">Kuinka paljon valtion nettovelanottoa kertyi keskimäärin yhden tunnin aikana Marinin ja Orpon hallitusten kuukausitoteumissa? Kolmas kortti näyttää samalla mittarilla Liberaalien ehdotuksen mukaisen, ehdollisen vuoden 2025 velkaantumisvauhdin.</p>
    <p class="notice"><strong>Kolmas kortti on mallilaskelma.</strong> Se lähtee vuoden 2025 toteutuneesta budjettitalouden nettovelanotosta ja vähentää siitä vastelaskelman keskisen skenaarion välittömän tasapainovaikutuksen. Se ei ole toteutunut hallituskauden velkaluku eikä ennuste.</p>
  </div></header>
  <main class="wrap">
    <p class="eyebrow">Yksi tunti, kolme eri lukua</p>
    <h2>Velkaa otettiin yli miljoona euroa tunnissa. Mallissa vauhti olisi noin miljoona euroa tunnissa.</h2>
    <p class="intro">Vertailu käyttää koko havaittua kuukausijaksoa. Se ei jaa syy-seurausvastuuta yksittäiselle pääministerille: hallitukset ovat koalitioita, suhdanteet ja kriisit vaikuttavat voimakkaasti ja talousarviot valmistellaan etukäteen.</p>
    <section class="rate-grid" aria-label="Velkaantumisen tuntivertailu" id="rate-cards"></section>
    <section class="hour">
      <p class="eyebrow" style="color:#FFD500">Saman tunnin mittatikku</p>
      <h2>Tämä määrä liikkuu yhdessä tunnissa.</h2>
    <p>Oranssi palkki näyttää mallinnetun vuoden 2025 velkaantumisvauhdin: toteutunut 2025 nettovelanotto miinus mallin välitön tasapainovaikutus. Se on ehdollinen laskelma, ei toteutunut velkaluku.</p>
      <div class="bars" id="bars"></div>
    </section>
    <section class="method">
      <article>
        <h3>Mitä toteutunut luku mittaa?</h3>
        <p>Marin- ja Orpo-korttien nimittäjä on havaittujen, hallitukselle kohdennettujen kuukausien kalenteritunnit. Osoittaja on velanhallintamomenttien nettokertymän vastaluku: positiivinen kasvattaa velkaa ja negatiivinen lyhentää sitä.</p>
      </article>
      <article>
        <h3>Mitä malliluku mittaa?</h3>
        <p>Liberaalien kortti käyttää samaa budjettitalouden nettovelanoton mittaria kuin toteutuneet kortit. Vuoden 2025 toteutuneesta nettovelanotosta vähennetään historiallisen vastelaskelman keskisen skenaarion välitön tasapainovaikutus. Malli on vaiheistettu vuosille 2008–2011. Se ei kuvaa käyttäytymis-, korko- tai poliittisia reaktioita täydellisesti.</p>
      </article>
    </section>
    <p class="source" id="source"></p>
  </main>
  <script type="application/json" id="hour-data">__DATA__</script>
  <script>
    const DATA = JSON.parse(document.getElementById('hour-data').textContent);
    const euro = value => Math.round(Math.abs(value)).toLocaleString('fi-FI') + ' €';
    const signedEuro = value => (value < 0 ? '−' : '+') + euro(value);
    const billion = value => (value / 1e9).toLocaleString('fi-FI', {maximumFractionDigits:1}) + ' mrd €';
    const cards = [
      ...DATA.observed.map(row => ({...row, rateSign: 1})),
      DATA.modelled_counterfactual
    ];
    const cardHost = document.getElementById('rate-cards');
    cardHost.innerHTML = cards.map(row => {
      const observed = row.kind === 'observed';
      const note = observed
        ? `${billion(row.debt_change_eur)} nettovelanottoa ${row.months} kuukauden havaintojaksolla.`
        : 'Liberaalien ehdotuksilla valtion velkaantuminen hidastuisi ja velka jäisi pidemmällä aikavälillä selvästi pienemmäksi.';
      const heading = observed ? row.label : row.label;
      const kind = observed ? 'Toteutunut kuukausidata' : 'Ratkaisut rahapulaan';
      const caption = 'velkaa tunnissa';
      const direction = observed
        ? { arrow: '↑', ariaLabel: 'Nettovelanotto kasvaa.' }
        : { arrow: '↓', ariaLabel: 'Nettovelanotto vähenee.' };
      return `<article class="rate-card ${row.id}">
        <span class="pill">${kind}</span><h3>${heading}</h3>${row.period ? `<p class="period">${row.period}</p>` : ''}
        <div class="rate" data-counter="${row.rate_eur_per_hour}">0 €</div><p class="rate-caption">${caption}</p>
        <div class="direction" aria-label="${direction.ariaLabel}"><span class="direction-arrow" aria-hidden="true">${direction.arrow}</span></div>
        <p class="annual">Sama vauhti vuodessa<strong>+${billion(row.annualized_debt_change_eur)}</strong></p>
        <p class="card-note">${note}</p></article>`;
    }).join('');
    const maxRate = Math.max(...cards.map(row => row.rate_eur_per_hour));
    document.getElementById('bars').innerHTML = cards.map(row => {
      return `<div class="bar-row"><div class="bar-label">${row.label}</div><div class="bar-track"><div class="bar-fill ${row.id}" data-width="${row.rate_eur_per_hour / maxRate * 100}"></div></div><div class="bar-value">+${euro(row.rate_eur_per_hour)}</div></div>`;
    }).join('');
    function animate() {
      const reduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
      document.querySelectorAll('[data-counter]').forEach(node => {
        const target = Number(node.dataset.counter);
        if (reduced) { node.textContent = signedEuro(target); return; }
        const start = performance.now(), duration = 1050;
        const step = now => {
          const share = Math.min(1, (now - start) / duration);
          const eased = 1 - Math.pow(1 - share, 3);
          node.textContent = signedEuro(target * eased);
          if (share < 1) requestAnimationFrame(step);
        };
        requestAnimationFrame(step);
      });
      requestAnimationFrame(() => document.querySelectorAll('.bar-fill').forEach(node => { node.style.width = node.dataset.width + '%'; }));
    }
    document.getElementById('source').innerHTML = `Lähteet: <a href="${DATA.meta.sources[0].url}" target="_blank" rel="noreferrer">${DATA.meta.sources[0].label}</a> ja <a href="${DATA.meta.sources[1].url}" target="_blank" rel="noreferrer">${DATA.meta.sources[1].label}</a>. Toteutunut aineisto päättyy 6/2026. Malliluvun lähteenä on Budjettihaukan ${DATA.modelled_counterfactual.scenario_case.toLowerCase()} historiallinen vastelaskelma; sen luottamusluokka on ${DATA.meta.scenario_trust_class.replaceAll('_',' ')} eikä se esitä kausaaliväitettä.`;
    animate();
  </script>
</body>
</html>'''


def main() -> int:
    payload = build_payload()
    output = TEMPLATE.replace("__DATA__", json.dumps(payload, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/"))
    OUTPUT.write_text(output, encoding="utf-8")
    print(OUTPUT.relative_to(ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

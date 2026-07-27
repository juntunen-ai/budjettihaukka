#!/usr/bin/env python3
"""Rakentaa tietopohjan tilannekuvan (ontologia, relaatiot, semanttiset
piirteet) itsenäiseksi HTML-sivuksi.

Kokoaa datan ontologiasta, käsitekartoista ja BigQuerystä ja kirjoittaa
docs/ontologia_visualisointi.html. Toistettavissa: aja uudelleen kun
kartat, aliakset tai sarjat muuttuvat.

Aja: .venv/bin/python scripts/build_ontology_viz.py --project budjettihaukka-gpt
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

import yaml
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings

OUT_PATH = ROOT / "docs" / "ontologia_visualisointi.html"


def collect_data(project: str, dataset: str) -> dict:
    out: dict = {}

    ontology = yaml.safe_load((ROOT / "data/ontology/budjettihaukka_ontology.yaml").read_text(encoding="utf-8"))
    concepts = []
    for concept in ontology["concepts"]:
        sources = Counter(a.get("source", "?") for a in concept.get("aliases", []))
        concepts.append(
            {
                "id": concept["concept_id"],
                "label": concept.get("label_fi", concept["concept_id"]),
                "aliases_total": len(concept.get("aliases", [])),
                "aliases_by_source": dict(sources),
            }
        )
    out["concepts"] = concepts
    out["alias_types"] = dict(
        Counter(
            a.get("alias_type", "?") for c in ontology["concepts"] for a in c.get("aliases", [])
        ).most_common()
    )

    maps: dict[str, dict] = {}
    flows: list[tuple[str, str]] = []
    for path in sorted((ROOT / "data/ontology/concept_code_map").glob("*.yaml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        cid, rules = doc["concept"], doc.get("rules", [])
        paaluokat: Counter = Counter()
        for rule in rules:
            if rule["role"] in ("include", "component"):
                paaluokat[rule["code"][:2]] += 1
            if rule["role"] == "exclude" and rule.get("target_concept"):
                flows.append((cid, rule["target_concept"]))
        maps[cid] = {
            "label": doc.get("label_fi", cid),
            "version": doc.get("version"),
            "decided_on": str(doc.get("decided_on", "")),
            "rules": len(rules),
            "component": sorted(
                {r.get("component") for r in rules if r["role"] == "component" and r.get("component")}
            ),
            "paaluokat": dict(paaluokat),
        }
    out["maps"] = maps
    out["flows"] = [{"from": a, "to": b, "rules": n} for (a, b), n in Counter(flows).most_common()]

    client = bigquery.Client(project=project)
    series: dict[str, list] = {}
    for row in client.query(
        f"""SELECT concept, vuosi, ROUND(SUM(total_meur_real)) real_meur, ROUND(SUM(total_meur_nominal)) nom_meur
        FROM `{project}.{dataset}.concept_yearly_totals_real_v1`
        WHERE role IN ('include','component') AND vuosi BETWEEN 1998 AND 2025
        GROUP BY 1, 2 ORDER BY 1, 2"""
    ).result():
        series.setdefault(row.concept, []).append(
            {"v": row.vuosi, "real": float(row.real_meur or 0), "nom": float(row.nom_meur or 0)}
        )
    out["series"] = series

    coverage = list(
        client.query(
            f"""WITH menot AS (
              SELECT SUM(nettokertyma_sum)/1e6 total FROM `{project}.{dataset}.valtiontalous_yearly_agg_v1`
              WHERE vuosi = 2024 AND SAFE_CAST(SUBSTR(momentti_tunnusp, 1, 2) AS INT64) BETWEEN 21 AND 36),
            kartoitettu AS (
              SELECT SUM(total_meur) mapped FROM `{project}.{dataset}.concept_yearly_totals_v1`
              WHERE vuosi = 2024 AND role IN ('include','component'))
            SELECT ROUND(k.mapped) m, ROUND(mm.total) t, ROUND(k.mapped/mm.total*100, 1) pct
            FROM kartoitettu k, menot mm"""
        ).result()
    )[0]
    out["coverage"] = {
        "mapped_meur": float(coverage.m),
        "total_meur": float(coverage.t),
        "pct": float(coverage.pct),
    }

    out["bva"] = [
        {"v": r.vuosi, "budjetoitu": float(r.budjetoitu or 0), "toteuma": float(r.toteuma or 0)}
        for r in client.query(
            f"""SELECT vuosi, ROUND(SUM(budjetoitu_eur)/1e6) budjetoitu, ROUND(SUM(toteuma_eur)/1e6) toteuma
            FROM `{project}.{dataset}.budget_vs_actual_v1`
            WHERE puoli = 'meno' AND vuosi BETWEEN 2014 AND 2025 GROUP BY vuosi ORDER BY vuosi"""
        ).result()
    ]

    tae_rows = list(client.query(f"SELECT COUNT(*) n FROM `{project}.{dataset}.talousarvio_v1`").result())[0].n
    codes = list(
        client.query(f"SELECT COUNT(*) n FROM `{project}.{dataset}.official_code_registry_v1`").result()
    )[0].n

    events = yaml.safe_load((ROOT / "data/reference/structural_events.yaml").read_text(encoding="utf-8"))["events"]
    out["events"] = [
        {"year": e["year"], "label": e["label_fi"], "affects": e["affects"]} for e in events
    ]
    out["stats"] = {
        "concepts_total": len(concepts),
        "concepts_mapped": len(maps),
        "map_rules": sum(m["rules"] for m in maps.values()),
        "aliases_total": sum(c["aliases_total"] for c in concepts),
        "finto_aliases": sum(c["aliases_by_source"].get("finto_yso", 0) for c in concepts),
        "official_codes": int(codes),
        "events": len(out["events"]),
        "tae_rows": int(tae_rows),
        "coverage_pct": out["coverage"]["pct"],
    }
    return out


TEMPLATE = r"""<title>Budjettihaukan tietopohja — ontologia ja semantiikka</title>
<style>
.viz-root{
  --surface-1:#fcfcfb; --surface-2:#f4f3ef; --line:#e3e1da;
  --text-primary:#0b0b0b; --text-secondary:#52514e; --text-muted:#7d7b73;
  --s1:#2a78d6; --s2:#1baf7a; --s3:#eda100; --s4:#008300; --s5:#4a3aa7; --s6:#e34948;
  --seq-250:#86b6ef; --seq-400:#3987e5; --seq-550:#1c5cab;
}
@media (prefers-color-scheme: dark){
  .viz-root{
    --surface-1:#1a1a19; --surface-2:#232322; --line:#3a3936;
    --text-primary:#ffffff; --text-secondary:#c3c2b7; --text-muted:#96948a;
    --s1:#3987e5; --s2:#199e70; --s3:#c98500; --s4:#008300; --s5:#9085e9; --s6:#e66767;
    --seq-250:#86b6ef; --seq-400:#3987e5; --seq-550:#1c5cab;
  }
}
:root[data-theme="dark"] .viz-root{
  --surface-1:#1a1a19; --surface-2:#232322; --line:#3a3936;
  --text-primary:#ffffff; --text-secondary:#c3c2b7; --text-muted:#96948a;
  --s1:#3987e5; --s2:#199e70; --s3:#c98500; --s4:#008300; --s5:#9085e9; --s6:#e66767;
}
:root[data-theme="light"] .viz-root{
  --surface-1:#fcfcfb; --surface-2:#f4f3ef; --line:#e3e1da;
  --text-primary:#0b0b0b; --text-secondary:#52514e; --text-muted:#7d7b73;
  --s1:#2a78d6; --s2:#1baf7a; --s3:#eda100; --s4:#008300; --s5:#4a3aa7; --s6:#e34948;
}
body{margin:0;}
.viz-root{background:var(--surface-1);color:var(--text-primary);
  font:15px/1.55 "Avenir Next","Helvetica Neue",system-ui,sans-serif;padding:40px 24px 72px;}
.wrap{max-width:960px;margin:0 auto;}
.kicker{font-size:12px;font-weight:600;letter-spacing:.13em;text-transform:uppercase;color:var(--text-secondary);}
h1{font-size:30px;line-height:1.15;margin:8px 0 4px;text-wrap:balance;}
.sub{color:var(--text-secondary);margin:0 0 26px;max-width:70ch;}
h2{font-size:19px;margin:40px 0 4px;}
.note{font-size:13px;color:var(--text-secondary);margin:0 0 14px;max-width:76ch;}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:20px 0;}
.tile{background:var(--surface-2);border:1px solid var(--line);padding:14px 16px;}
.tile .v{font-size:26px;font-weight:700;font-variant-numeric:tabular-nums;}
.tile .l{font-size:12px;color:var(--text-secondary);margin-top:2px;}
.tile.hero{border-left:3px solid var(--seq-550);}
.panel{background:var(--surface-2);border:1px solid var(--line);padding:18px;margin:12px 0;overflow-x:auto;}
svg text{font:12px "Avenir Next","Helvetica Neue",system-ui,sans-serif;fill:var(--text-secondary);}
svg .lbl{fill:var(--text-primary);font-weight:600;}
svg .num{font-variant-numeric:tabular-nums;}
.legend{display:flex;gap:18px;flex-wrap:wrap;font-size:13px;color:var(--text-secondary);margin:8px 2px 0;}
.legend span{display:inline-flex;align-items:center;gap:6px;}
.legend i{width:12px;height:12px;border-radius:3px;display:inline-block;}
table.dt{border-collapse:collapse;width:100%;font-size:13.5px;}
table.dt th{text-align:left;font-size:11px;letter-spacing:.06em;text-transform:uppercase;color:var(--text-secondary);padding:8px 10px;border-bottom:1px solid var(--line);}
table.dt td{padding:8px 10px;border-bottom:1px solid var(--line);}
table.dt td.num{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap;}
.tooltip{position:fixed;pointer-events:none;background:var(--surface-2);border:1px solid var(--line);
  padding:8px 10px;font-size:12.5px;display:none;z-index:10;box-shadow:0 2px 10px rgba(0,0,0,.14);}
.dot{width:10px;height:10px;border-radius:3px;display:inline-block;margin-right:6px;}
.covbar{height:22px;background:var(--surface-1);border:1px solid var(--line);margin:6px 0 2px;}
.covbar > div{height:100%;background:var(--seq-550);}
</style>
<div class="viz-root"><div class="wrap">
  <div class="kicker">Budjettihaukka · tietopohjan tilannekuva · __DATE__</div>
  <h1>Ontologia, relaatiot ja semanttiset piirteet</h1>
  <p class="sub">Miten valtion budjettidata jäsentyy Budjettihaukassa: ihmisen vahvistamat käsitemääritelmät,
  niiden väliset rahavirrat, sanaston semanttiset lähteet, budjetoitu vs. toteutunut sekä
  reaalihintaiset aikasarjat rakennemurroksineen.</p>

  <div class="tiles" id="tiles"></div>

  <h2>Kartoitettu osuus valtion menoista 2024</h2>
  <p class="note">Kuinka suuri osa menototeumasta kuuluu ihmisen vahvistamien käsitemääritelmien piiriin.
  Loppu odottaa tulevia katselmuksia (puolustus, työllisyys, liikenne…).</p>
  <div class="panel">
    <div style="font-size:22px;font-weight:700;" id="covPct"></div>
    <div class="covbar"><div id="covFill"></div></div>
    <div class="note" id="covDetail" style="margin:4px 0 0;"></div>
  </div>

  <h2>Semanttinen arkkitehtuuri</h2>
  <p class="note">Raakadatasta vastaukseksi: jokainen kerros lisää merkitystä, ja jokainen nuoli on koneellisesti validoitu.</p>
  <div class="panel"><svg id="arch" width="920" height="150" viewBox="0 0 920 150"></svg></div>

  <h2>Käsitteiden väliset relaatiot — mihin poissuljettu raha kuuluu</h2>
  <p class="note">Katselmuksissa tehdyt rajaukset synnyttävät target-relaatioita: kun momentti suljetaan pois käsitteestä,
  kirjataan mihin käsitteeseen se kuuluu. Sosiaaliturva syntyi juuri näin sote-katselmuksen sivutuotteena.
  Viivan paksuus = sääntöjen määrä.</p>
  <div class="panel"><svg id="flows" width="920" height="330" viewBox="0 0 920 330"></svg>
  <div class="legend" id="flowsLegend"></div></div>

  <h2>Budjetoitu vs. toteutunut 2014–2025</h2>
  <p class="note">Budjetoitu-metriikka (talousarvioesitys + lisätalousarviot, budjetti.vm.fi:n avoin data) toteuman
  rinnalla, valtion menot yhteensä. Vie hiiri kaavion päälle nähdäksesi vuosikohtaiset arvot ja toteuma-asteen.</p>
  <div class="panel"><svg id="bva" width="920" height="300" viewBox="0 0 920 300"></svg>
  <div class="legend" id="bvaLegend"></div></div>

  <h2>Sanaston semanttiset lähteet</h2>
  <p class="note">__ALIASES__ aliasta __CONCEPTS__ käsitteelle. Väri = tarkkuuspaino (precision), joka määrää painoarvon
  kyselyjen tulkinnassa — esim. vm_token on estetty (0,18) ja finto_alt luotettava (0,80).</p>
  <div class="panel"><svg id="aliases" width="920" height="360" viewBox="0 0 920 360"></svg>
  <div class="legend" id="aliasLegend"></div></div>

  <h2>Kartoitetut käsitteet reaalihinnoin (2025 hinnoin) — rakennemurrokset merkittyinä</h2>
  <p class="note">Ihmisen vahvistamat käsitekartat. Katkoviivat = structural_events-rekisterin murrokset:
  tasosiirtymä uudistuksen kohdalla ei ole menomuutos. Vie hiiri kaavion päälle nähdäksesi arvot.</p>
  <div class="panel"><svg id="series" width="920" height="380" viewBox="0 0 920 380"></svg>
  <div class="legend" id="seriesLegend"></div></div>

  <h2>Käsitekartat — päätökset datana</h2>
  <div class="panel"><table class="dt" id="mapTable"></table></div>
  <div class="tooltip" id="tt"></div>
</div></div>
<script>
const DATA = __DATA__;
const CSS = getComputedStyle(document.querySelector('.viz-root'));
const C = k => CSS.getPropertyValue(k).trim();
const SER = {sosiaali_ja_terveys:'--s1', sosiaaliturva:'--s2', koulutus:'--s3', tutkimus:'--s4', kulttuuri:'--s5'};
const NIMI = {koulutus:'Koulutus', sosiaali_ja_terveys:'Sote-palvelut', tutkimus:'Tutkimus',
              kulttuuri:'Kulttuuri', sosiaaliturva:'Sosiaaliturva'};
const fi = (x, d = 1) => x.toFixed(d).replace('.', ',');
const tt = document.getElementById('tt');
function placeTooltip(ev){
  tt.style.display = 'block';
  tt.style.left = Math.min(ev.clientX + 14, window.innerWidth - tt.offsetWidth - 12) + 'px';
  tt.style.top = Math.max(8, Math.min(ev.clientY + 12, window.innerHeight - tt.offsetHeight - 8)) + 'px';
}

// Tiles
const T = DATA.stats;
document.getElementById('tiles').innerHTML = [
  [fi(T.coverage_pct) + ' %', 'menoista 2024 kartoitettu', 'hero'],
  [T.concepts_mapped, 'vahvistettua karttaa'],
  [T.map_rules, 'momenttisääntöä'],
  [T.aliases_total, 'aliasta (' + T.finto_aliases + ' YSO:sta)'],
  [T.tae_rows.toLocaleString('fi'), 'TAE+LTAE-riviä 2014–2026'],
  [T.official_codes.toLocaleString('fi'), 'virallista koodia'],
  [T.events, 'rakennemurrosta'],
].map(([v, l, cls]) => `<div class="tile ${cls || ''}"><div class="v">${v}</div><div class="l">${l}</div></div>`).join('');

// Coverage
document.getElementById('covPct').textContent =
  fi(DATA.coverage.pct) + ' % — ' + fi(DATA.coverage.mapped_meur / 1000) + ' / ' + fi(DATA.coverage.total_meur / 1000) + ' mrd €';
document.getElementById('covFill').style.width = DATA.coverage.pct + '%';
document.getElementById('covDetail').textContent =
  'Kartoitettu: ' + Object.values(DATA.maps).map(m => m.label).join(', ') + '. Metriikka: toteuma (nettokertymä), nimellinen.';

// Arkkitehtuuri
(function(){
  const steps = [
    ['Valtiokonttori', '7,5 M riviä'], ['Curated-kerros', 'tyypitys · DQ · merkistökorjaus'],
    ['Semanttinen näkymä', 'kanoniset nimet'], ['Käsitekerros', 'kartat · ontologia · YSO'],
    ['Vastaus', 'määritelmä + epävarmuudet'],
  ];
  const w = 165, gap = 22, y = 40, h = 62;
  let x = 8, parts = '';
  steps.forEach(([a, b], i) => {
    parts += `<rect x="${x}" y="${y}" width="${w}" height="${h}" fill="var(--surface-1)" stroke="var(--line)"/>`;
    parts += `<text class="lbl" x="${x + w / 2}" y="${y + 26}" text-anchor="middle">${a}</text>`;
    parts += `<text x="${x + w / 2}" y="${y + 44}" text-anchor="middle" font-size="10.5">${b}</text>`;
    if (i < steps.length - 1)
      parts += `<path d="M${x + w + 3},${y + h / 2} l${gap - 8},0 m-6,-5 l6,5 l-6,5" stroke="var(--text-muted)" fill="none" stroke-width="1.5"/>`;
    x += w + gap;
  });
  parts += `<text x="8" y="130" font-size="11">Rinnalla: KHI-deflaattori · COFOG-ankkurit · TAE+LTAE-määrärahat · structural_events · virallinen koodirekisteri</text>`;
  document.getElementById('arch').innerHTML = parts;
})();

// Relaatiovirrat
(function(){
  const sources = [...new Set(DATA.flows.map(f => f.from))];
  const targets = [...new Set(DATA.flows.map(f => f.to))];
  const sy = i => 60 + i * 90, ty = i => 26 + i * 30;
  let parts = '';
  DATA.flows.forEach(f => {
    const y1 = sy(sources.indexOf(f.from)), y2 = ty(targets.indexOf(f.to));
    const wpx = Math.max(1.5, Math.min(10, f.rules * 0.9));
    parts += `<path d="M 250,${y1} C 480,${y1} 560,${y2} 700,${y2}" stroke="${C(SER[f.from] || '--s6')}" stroke-width="${wpx}" fill="none" opacity="0.55"><title>${NIMI[f.from] || f.from} → ${NIMI[f.to] || f.to}: ${f.rules} sääntöä</title></path>`;
  });
  sources.forEach((s, i) => {
    parts += `<rect x="60" y="${sy(i) - 16}" width="190" height="32" fill="var(--surface-1)" stroke="${C(SER[s] || '--s6')}" stroke-width="2"/>`;
    parts += `<text class="lbl" x="70" y="${sy(i) + 4}">${NIMI[s] || s}</text>`;
  });
  targets.forEach((tg, i) => {
    parts += `<rect x="700" y="${ty(i) - 12}" width="200" height="24" fill="var(--surface-1)" stroke="var(--line)"/>`;
    parts += `<text x="710" y="${ty(i) + 4}">${NIMI[tg] || tg}</text>`;
  });
  parts += `<text x="60" y="${sy(sources.length - 1) + 52}" font-size="11">Vahvistettu kartta</text>`;
  parts += `<text x="700" y="${ty(targets.length - 1) + 34}" font-size="11">Kohdekäsite (esitäytetty tai valmis)</text>`;
  document.getElementById('flows').innerHTML = parts;
  document.getElementById('flowsLegend').innerHTML = sources.map(s =>
    `<span><i style="background:${C(SER[s] || '--s6')}"></i>${NIMI[s] || s} (lähdekäsite)</span>`).join('');
})();

// Budjetoitu vs toteuma
(function(){
  const svg = document.getElementById('bva');
  const M = {l: 78, r: 24, t: 16, b: 34}, W = 920, H = 300;
  const rows = DATA.bva.filter(d => d.toteuma > 0 || d.budjetoitu > 0);
  const y0 = rows[0].v, y1 = rows[rows.length - 1].v;
  let vmax = 0; rows.forEach(d => vmax = Math.max(vmax, d.budjetoitu, d.toteuma)); vmax *= 1.08;
  const X = v => M.l + (v - y0) / (y1 - y0) * (W - M.l - M.r);
  const Y = v => H - M.b - v / vmax * (H - M.t - M.b);
  // Budjetoitu/toteuma ei ole kategorinen pari vaan suunniteltu vs. toteutunut:
  // omat värit (eivät käsitevärejä) ja katkoviiva toissijaisena erotteluna.
  const MET = {budjetoitu: {col: C('--text-muted'), dash: '6 4'}, toteuma: {col: C('--s6'), dash: ''}};
  let parts = '';
  for (let g = 0; g <= 4; g++){
    const val = vmax / 4 * g, yy = Y(val);
    parts += `<line x1="${M.l}" y1="${yy}" x2="${W - M.r}" y2="${yy}" stroke="var(--line)"/>`;
    parts += `<text class="num" x="${M.l - 8}" y="${yy + 4}" text-anchor="end">${Math.round(val / 1000)} mrd €</text>`;
  }
  rows.forEach(d => { if (d.v % 2 === 0) parts += `<text x="${X(d.v)}" y="${H - 10}" text-anchor="middle">${d.v}</text>`; });
  const mk = key => {
    const {col, dash} = MET[key];
    const pts = rows.filter(d => d[key] > 0);
    let s = `<path d="${pts.map((d, i) => `${i ? 'L' : 'M'}${X(d.v).toFixed(1)},${Y(d[key]).toFixed(1)}`).join(' ')}" stroke="${col}" stroke-width="2" fill="none"${dash ? ` stroke-dasharray="${dash}"` : ''}/>`;
    pts.forEach(d => s += `<circle cx="${X(d.v)}" cy="${Y(d[key])}" r="3" fill="${col}"/>`);
    const last = pts[pts.length - 1], ly = Y(last[key]) + (key === 'budjetoitu' ? -12 : 20);
    const txt = key === 'budjetoitu' ? 'Budjetoitu' : 'Toteuma', tw = txt.length * 7.2;
    s += `<rect x="${X(last.v) - 8 - tw - 14}" y="${ly - 9}" width="9" height="9" rx="2" fill="${col}"/>`;
    s += `<text class="lbl" x="${X(last.v) - 8}" y="${ly}" text-anchor="end">${txt}</text>`;
    return s;
  };
  parts += mk('toteuma') + mk('budjetoitu');
  parts += `<line id="bvaxh" x1="0" y1="${M.t}" x2="0" y2="${H - M.b}" stroke="var(--text-muted)" opacity="0"/>`;
  svg.innerHTML = parts;
  document.getElementById('bvaLegend').innerHTML =
    `<span><i style="background:${MET.toteuma.col}"></i>Toteuma (nettokertymä)</span>
     <span><i style="background:${MET.budjetoitu.col}"></i>Budjetoitu, katkoviiva (TAE + lisätalousarviot)</span>`;
  const xh = svg.querySelector('#bvaxh');
  svg.addEventListener('mousemove', ev => {
    const rect = svg.getBoundingClientRect();
    const yr = Math.round(y0 + ((ev.clientX - rect.left) * (W / rect.width) - M.l) / (W - M.l - M.r) * (y1 - y0));
    const d = rows.find(p => p.v === yr);
    if (!d){ xh.setAttribute('opacity', 0); tt.style.display = 'none'; return; }
    xh.setAttribute('x1', X(yr)); xh.setAttribute('x2', X(yr)); xh.setAttribute('opacity', .8);
    const aste = d.budjetoitu > 0 && d.toteuma > 0 ? fi(d.toteuma / d.budjetoitu * 100) + ' %' : '—';
    tt.innerHTML = `<b>${yr}</b>
      <div><span class="dot" style="background:${MET.budjetoitu.col}"></span>Budjetoitu: <b>${fi(d.budjetoitu / 1000)} mrd €</b></div>
      <div><span class="dot" style="background:${MET.toteuma.col}"></span>Toteuma: <b>${d.toteuma > 0 ? fi(d.toteuma / 1000) + ' mrd €' : 'ei vielä'}</b></div>
      <div style="color:var(--text-secondary)">Toteuma-aste: ${aste}</div>`;
    placeTooltip(ev);
  });
  svg.addEventListener('mouseleave', () => { xh.setAttribute('opacity', 0); tt.style.display = 'none'; });
})();

// Aliastyypit
(function(){
  const svg = document.getElementById('aliases');
  const PREC = {label:.99, pref:.98, canonical:.9, inflected:.9, alt:.82, finto_alt:.8, english:.72,
                colloquial:.7, vm_phrase:.62, abbreviation:.58, finto_narrower:.55, historical:.5, vm_token:.18};
  const entries = Object.entries(DATA.alias_types).sort((a, b) => b[1] - a[1]);
  const max = entries[0][1], bw = 540, x0 = 200, rh = 22, gap = 6;
  let parts = '', y = 14;
  entries.forEach(([type, n]) => {
    const p = PREC[type] ?? .5;
    const col = p >= .8 ? C('--seq-550') : p >= .55 ? C('--seq-400') : C('--seq-250');
    const w = Math.max(2, n / max * bw);
    parts += `<text x="${x0 - 8}" y="${y + 15}" text-anchor="end">${type}</text>`;
    parts += `<rect x="${x0}" y="${y}" width="${w}" height="${rh - 4}" rx="3" fill="${col}"><title>${type}: ${n} aliasta · precision ${String(p).replace('.', ',')}</title></rect>`;
    parts += `<text class="num" x="${x0 + w + 8}" y="${y + 14}">${n}${type === 'vm_token' ? ' (estetty)' : ''}</text>`;
    y += rh + gap;
  });
  svg.setAttribute('height', y + 6); svg.setAttribute('viewBox', `0 0 920 ${y + 6}`);
  document.getElementById('aliasLegend').innerHTML =
    `<span><i style="background:${C('--seq-550')}"></i>precision ≥ 0,80</span>
     <span><i style="background:${C('--seq-400')}"></i>0,55–0,79</span>
     <span><i style="background:${C('--seq-250')}"></i>&lt; 0,55 tai estetty</span>`;
})();

// Reaalisarjat
(function(){
  const svg = document.getElementById('series');
  const M = {l: 78, r: 24, t: 16, b: 34}, W = 920, H = 380;
  const order = ['sosiaali_ja_terveys', 'sosiaaliturva', 'koulutus', 'tutkimus', 'kulttuuri'].filter(k => DATA.series[k]);
  const years = DATA.series[order[0]].map(d => d.v);
  const y0 = Math.min(...years), y1 = Math.max(...years);
  let vmax = 0; order.forEach(k => DATA.series[k].forEach(d => vmax = Math.max(vmax, d.real))); vmax *= 1.06;
  const X = v => M.l + (v - y0) / (y1 - y0) * (W - M.l - M.r);
  const Y = v => H - M.b - v / vmax * (H - M.t - M.b);
  let parts = '';
  for (let g = 0; g <= 4; g++){
    const val = vmax / 4 * g, yy = Y(val);
    parts += `<line x1="${M.l}" y1="${yy}" x2="${W - M.r}" y2="${yy}" stroke="var(--line)"/>`;
    parts += `<text class="num" x="${M.l - 8}" y="${yy + 4}" text-anchor="end">${Math.round(val / 1000)} mrd €</text>`;
  }
  for (let yr = 2000; yr <= y1; yr += 5) parts += `<text x="${X(yr)}" y="${H - 10}" text-anchor="middle">${yr}</text>`;
  [...new Set(DATA.events.map(e => e.year))].forEach(yr => {
    const labels = DATA.events.filter(e => e.year === yr).map(e => e.label).join(' · ');
    parts += `<line x1="${X(yr)}" y1="${M.t}" x2="${X(yr)}" y2="${H - M.b}" stroke="var(--text-muted)" stroke-dasharray="4 4" opacity="0.7"><title>${yr}: ${labels}</title></line>`;
  });
  parts += `<text x="${X(2010) + 4}" y="${M.t + 12}" font-size="10.5">VOS-uudistus</text>`;
  parts += `<text x="${X(2023) + 4}" y="${M.t + 12}" font-size="10.5">Sote 2023</text>`;
  order.forEach(cid => {
    const path = DATA.series[cid].map((d, i) => `${i ? 'L' : 'M'}${X(d.v).toFixed(1)},${Y(d.real).toFixed(1)}`).join(' ');
    parts += `<path d="${path}" stroke="${C(SER[cid])}" stroke-width="2" fill="none"/>`;
  });
  // Suorat labelit: teksti tekstivärillä (osa sarjaväreistä alle 3:1 kontrastin),
  // tunnistus värimerkillä; päällekkäiset labelit työnnetään erilleen.
  const labels = order.map(cid => {
    const last = DATA.series[cid][DATA.series[cid].length - 1];
    return {cid, col: C(SER[cid]), y: Y(last.real) - 8, x: X(last.v) - 4};
  }).sort((a, b) => a.y - b.y);
  for (let i = 1; i < labels.length; i++)
    if (labels[i].y - labels[i - 1].y < 16) labels[i].y = labels[i - 1].y + 16;
  labels.forEach(l => {
    const textW = NIMI[l.cid].length * 7.2;
    parts += `<rect x="${l.x - textW - 22}" y="${l.y - 9}" width="9" height="9" rx="2" fill="${l.col}"/>`;
    parts += `<text class="lbl" x="${l.x - 8}" y="${l.y}" text-anchor="end">${NIMI[l.cid]}</text>`;
  });
  parts += `<line id="xh" x1="0" y1="${M.t}" x2="0" y2="${H - M.b}" stroke="var(--text-muted)" opacity="0"/>`;
  svg.innerHTML = parts;
  document.getElementById('seriesLegend').innerHTML = order.map(cid =>
    `<span><i style="background:${C(SER[cid])}"></i>${NIMI[cid]}</span>`).join('') +
    `<span style="color:var(--text-secondary)">– – rakennemurros</span>`;
  const xh = svg.querySelector('#xh');
  svg.addEventListener('mousemove', ev => {
    const rect = svg.getBoundingClientRect();
    const yr = Math.round(y0 + ((ev.clientX - rect.left) * (W / rect.width) - M.l) / (W - M.l - M.r) * (y1 - y0));
    if (yr < y0 || yr > y1){ xh.setAttribute('opacity', 0); tt.style.display = 'none'; return; }
    xh.setAttribute('x1', X(yr)); xh.setAttribute('x2', X(yr)); xh.setAttribute('opacity', .8);
    const lines = order.map(cid => {
      const d = DATA.series[cid].find(p => p.v === yr);
      return d ? `<div><span class="dot" style="background:${C(SER[cid])}"></span>${NIMI[cid]}: <b>${fi(d.real / 1000)} mrd €</b> <span style="color:var(--text-secondary)">(nim. ${fi(d.nom / 1000)})</span></div>` : '';
    }).join('');
    const evs = DATA.events.filter(e => e.year === yr).map(e => `<div style="color:var(--text-secondary)">⚑ ${e.label}</div>`).join('');
    tt.innerHTML = `<b>${yr}</b> · 2025 hinnoin${evs}${lines}`;
    placeTooltip(ev);
  });
  svg.addEventListener('mouseleave', () => { xh.setAttribute('opacity', 0); tt.style.display = 'none'; });
})();

// Karttataulukko
(function(){
  const rows = Object.entries(DATA.maps).map(([cid, m]) => {
    const last = (DATA.series[cid] || []).find(d => d.v === 2024) || {real: 0};
    return `<tr><td><span class="dot" style="background:${C(SER[cid] || '--s6')}"></span>${m.label}</td>
      <td class="num">v${m.version}</td><td>${m.decided_on}</td>
      <td class="num">${m.rules}</td><td>${m.component.join(', ') || '—'}</td>
      <td class="num">${fi(last.real / 1000)} mrd €</td>
      <td>${Object.keys(m.paaluokat).sort().map(p => 'PL ' + p).join(', ')}</td></tr>`;
  }).join('');
  document.getElementById('mapTable').innerHTML =
    `<tr><th>Käsite</th><th>Versio</th><th>Päätetty</th><th>Sääntöjä</th><th>Komponentit</th><th>2024 (reaali)</th><th>Pääluokat</th></tr>` + rows;
})();
</script>
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Rakenna ontologiavisualisointi.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--date", default="", help="Päiväys otsikkoon (oletus: kartoista uusin)")
    args = parser.parse_args()

    data = collect_data(args.project, args.dataset)
    date = args.date or max((m.get("decided_on") or "") for m in data["maps"].values())
    html = (
        TEMPLATE.replace("__DATA__", json.dumps(data, ensure_ascii=False))
        .replace("__DATE__", date)
        .replace("__ALIASES__", str(data["stats"]["aliases_total"]))
        .replace("__CONCEPTS__", str(data["stats"]["concepts_total"]))
    )
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(html, encoding="utf-8")
    print(f"Kirjoitettu: {OUT_PATH.relative_to(ROOT)} ({len(html)} merkkiä)")
    print(f"  kartat: {data['stats']['concepts_mapped']} · säännöt: {data['stats']['map_rules']} · kattavuus: {data['stats']['coverage_pct']} %")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

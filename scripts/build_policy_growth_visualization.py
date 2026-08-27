#!/usr/bin/env python3
"""Build the first evidence-led view of policy contributions to Finland's growth gap."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
LEDGER_PATH = ROOT / "data" / "reference" / "policy_growth_ledger_v1.json"
PRODUCTIVITY_PATH = ROOT / "data" / "reference" / "productivity_counterfactual_v1.json"
BUDGET_EVIDENCE_PATH = ROOT / "data" / "reference" / "policy_budget_evidence_v1.json"
TAX_SCENARIO_PATH = ROOT / "data" / "reference" / "tax_growth_scenario_v1.json"
OUTPUT_PATH = ROOT / "politiikan-kasvujarrut.html"

REQUIRED_POLICY_FIELDS = {
    "policy_id",
    "priority",
    "title_fi",
    "period_label_fi",
    "policy_form",
    "evidence_grade",
    "channels",
    "decision_fi",
    "observed_evidence_fi",
    "mechanism_fi",
    "counterfactual_fi",
    "claim_boundary_fi",
    "overlap_groups",
    "budget_grounding",
    "quantitative_evidence",
    "sources",
}


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_ledger(ledger: dict[str, Any], productivity: dict[str, Any]) -> None:
    meta = ledger.get("meta", {})
    policies = ledger.get("policies", [])
    if meta.get("dataset_id") != "policy_growth_ledger_v1":
        raise ValueError("Unexpected policy growth ledger dataset id")
    if meta.get("aggregation_policy", {}).get("do_not_sum_policy_estimates") is not True:
        raise ValueError("Policy estimates must be explicitly non-additive")
    if meta.get("aggregation_policy", {}).get("do_not_allocate_total_gap") is not True:
        raise ValueError("Total productivity gap must not be allocated to policies")
    if len(policies) != 6:
        raise ValueError(f"Phase 1 requires exactly six policies, got {len(policies)}")

    policy_ids = [policy.get("policy_id") for policy in policies]
    if len(policy_ids) != len(set(policy_ids)):
        raise ValueError("Policy ids must be unique")
    if sorted(policy.get("priority") for policy in policies) != list(range(1, 7)):
        raise ValueError("Policy priorities must be the complete range 1..6")

    grades = set(meta.get("evidence_grades", {}))
    for policy in policies:
        missing = REQUIRED_POLICY_FIELDS - set(policy)
        if missing:
            raise ValueError(f"Policy {policy.get('policy_id')} missing fields: {sorted(missing)}")
        if policy["evidence_grade"] not in grades:
            raise ValueError(f"Unknown evidence grade for {policy['policy_id']}")
        for field in (
            "title_fi",
            "period_label_fi",
            "decision_fi",
            "observed_evidence_fi",
            "mechanism_fi",
            "counterfactual_fi",
            "claim_boundary_fi",
        ):
            if not str(policy[field]).strip():
                raise ValueError(f"Policy {policy['policy_id']} has empty {field}")
        if not policy["channels"] or not policy["sources"]:
            raise ValueError(f"Policy {policy['policy_id']} lacks channels or sources")
        for source in policy["sources"]:
            if not source.get("url", "").startswith("https://"):
                raise ValueError(f"Policy {policy['policy_id']} has non-HTTPS source")
            if not source.get("supports"):
                raise ValueError(f"Policy {policy['policy_id']} has unscoped source")
        for estimate in policy["quantitative_evidence"]:
            if estimate.get("comparable_to_productivity_gap") is not False:
                raise ValueError(
                    f"Estimate {estimate.get('metric_id')} must not be additive to the gap"
                )

    outcome = meta.get("outcome_contract", {})
    productivity_meta = productivity.get("meta", {})
    if outcome.get("observed_source_dataset") != productivity_meta.get("dataset_id"):
        raise ValueError("Ledger outcome does not point to the productivity snapshot")
    for field in ("base_year", "end_year"):
        if outcome.get(field) != productivity_meta.get(field):
            raise ValueError(f"Outcome {field} differs from productivity snapshot")
    if outcome.get("trend_is_causal_potential") is not False:
        raise ValueError("Trend benchmark must not be labelled causal potential")

    final = next(
        row for row in productivity["years"] if row["year"] == outcome["end_year"]
    )
    if not 100 < final["productivity_index"] < 115:
        raise ValueError("Observed productivity endpoint is outside reviewed range")
    if not final["trend_index"] > 140:
        raise ValueError("Trend endpoint is outside reviewed range")


def validate_budget_evidence(budget_evidence: dict[str, Any], ledger: dict[str, Any]) -> None:
    meta = budget_evidence.get("meta", {})
    rows = budget_evidence.get("rows", [])
    if meta.get("dataset_id") != "policy_budget_evidence_v1":
        raise ValueError("Unexpected policy budget evidence dataset id")
    if meta.get("fiscal_side") != "expense":
        raise ValueError("Budget evidence must contain expense-side rows only")
    if meta.get("baseline_year") != 2011 or meta.get("comparison_year") != 2016:
        raise ValueError("Budget evidence comparison period has changed")
    if meta.get("real_price_base_year") != 2025:
        raise ValueError("Budget evidence must use the reviewed 2025 price base")
    if len(rows) < 10:
        raise ValueError("Budget evidence must contain at least ten reviewed moments")
    policy_ids = {policy["policy_id"] for policy in ledger["policies"]}
    seen_codes: set[str] = set()
    for row in rows:
        code = row.get("momentti_tunnusp")
        if not code or code in seen_codes:
            raise ValueError(f"Missing or duplicate budget evidence code: {code}")
        seen_codes.add(code)
        if row.get("primary_policy_id") not in policy_ids:
            raise ValueError(f"Unknown policy link for budget moment {code}")
        if row.get("claim_status") != "verified_actual_spending_decline":
            raise ValueError(f"Unverified budget evidence row: {code}")
        if row.get("real_change_meur", 0) >= 0:
            raise ValueError(f"Budget evidence row is not a real decline: {code}")
        if row.get("baseline", {}).get("has_structural_guardrail") is True:
            raise ValueError(f"Baseline structural warning for budget moment {code}")
        if row.get("comparison", {}).get("has_structural_guardrail") is True:
            raise ValueError(f"Comparison structural warning for budget moment {code}")
        history = row.get("history", [])
        if not history or history != sorted(history, key=lambda item: item["year"]):
            raise ValueError(f"Invalid historical budget series for {code}")


def validate_tax_scenario(tax_scenario: dict[str, Any]) -> None:
    meta = tax_scenario.get("meta", {})
    rows = tax_scenario.get("rows", [])
    summary = tax_scenario.get("summary_2025", {})
    if meta.get("dataset_id") != "tax_growth_scenario_v1":
        raise ValueError("Unexpected tax growth scenario dataset id")
    if len(rows) != 18 or rows[0].get("year") != 2008 or rows[-1].get("year") != 2025:
        raise ValueError("Tax scenario must cover 2008-2025")
    if not 10 < summary.get("break_even_relative_rate_cut_pct", 0) < 12:
        raise ValueError("Unexpected tax scenario break-even rate")
    if summary.get("lower_10pct_revenue_beur", 0) <= summary.get(
        "actual_tax_revenue_beur", 0
    ):
        raise ValueError("Reviewed ten percent tax scenario no longer exceeds actual revenue")
    if any(item.get("used_in_formula") is not False for item in meta.get("evidence", [])):
        raise ValueError("External tax evidence cannot be an implicit model coefficient")


def _json_for_html(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")


TEMPLATE = r'''<!doctype html>
<html lang="fi">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta name="description" content="Lähteistetty ensimmäinen arvio Suomen tuottavuuskehitykseen vaikuttaneista valtion politiikkatoimista.">
  <link rel="icon" href="data:,">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Archivo+Black&family=Source+Sans+3:ital,wght@0,400;0,600;0,700;0,800&display=swap" rel="stylesheet">
  <title>Politiikan kasvujarrut | Budjettihaukka</title>
  <style>
    :root { --ink:#151512; --paper:#f6f0e3; --white:#fffdf7; --yellow:#FFD500; --orange:#F9B000; --red:#c43732; --blue:#006ca8; --green:#148142; --muted:#625d52; --line:#1515122b; --shadow:7px 7px 0 var(--ink); }
    * { box-sizing:border-box; }
    html { scroll-behavior:smooth; }
    body { margin:0; color:var(--ink); background:var(--paper); font:18px/1.5 "Source Sans 3",sans-serif; }
    a { color:inherit; }
    .wrap { width:min(1180px,calc(100% - 40px)); margin-inline:auto; }
    .hero { border-bottom:3px solid var(--ink); background:var(--yellow); }
    .hero-inner { position:relative; overflow:hidden; padding:24px 0 64px; }
    .hero-inner::after { position:absolute; right:-18px; bottom:-110px; color:#fff5; content:"?"; font:19rem/.8 "Archivo Black",sans-serif; }
    .back { position:relative; z-index:1; display:inline-flex; padding:7px 12px; border:2px solid var(--ink); border-radius:999px; background:var(--white); font-weight:800; text-decoration:none; }
    h1,h2,h3,.index-value,.policy-number { font-family:"Archivo Black","Arial Black",sans-serif; letter-spacing:-.045em; }
    h1 { position:relative; z-index:1; max-width:940px; margin:58px 0 18px; font-size:clamp(3rem,7.8vw,6.8rem); line-height:.88; text-transform:uppercase; }
    h1 span { display:block; color:var(--white); -webkit-text-stroke:2px var(--ink); text-shadow:4px 4px 0 var(--ink); }
    .lede { position:relative; z-index:1; max-width:850px; margin:0; font-size:clamp(1.1rem,2.2vw,1.42rem); font-weight:700; }
    main { padding:62px 0 90px; }
    .eyebrow { margin:0 0 8px; color:var(--muted); font-size:.8rem; font-weight:800; letter-spacing:.13em; text-transform:uppercase; }
    h2 { max-width:950px; margin:0; font-size:clamp(2rem,5vw,4.2rem); line-height:.95; }
    .intro { max-width:840px; margin:16px 0 0; color:var(--muted); font-size:1.08rem; }
    .warning { margin:28px 0 0; padding:16px 18px; border:2px solid var(--ink); background:#fff4b0; box-shadow:4px 4px 0 var(--ink); font-weight:700; }
    .index-strip { display:grid; grid-template-columns:repeat(3,1fr); margin-top:34px; border:2px solid var(--ink); background:var(--white); box-shadow:var(--shadow); }
    .index-item { min-width:0; padding:22px 24px; }
    .index-item + .index-item { border-left:2px solid var(--ink); }
    .index-label { display:block; color:var(--muted); font-size:.78rem; font-weight:800; letter-spacing:.08em; text-transform:uppercase; }
    .index-value { display:block; margin-top:4px; font-size:clamp(2.7rem,6vw,5rem); line-height:.9; }
    .index-item.actual .index-value { color:var(--blue); }
    .index-item.trend .index-value { color:var(--red); }
    .index-item.modest .index-value { color:var(--muted); }
    .index-context { display:block; margin-top:8px; color:var(--muted); font-size:.86rem; }
    .chart-shell { margin-top:42px; }
    .chart-title { display:flex; align-items:end; justify-content:space-between; gap:20px; margin-bottom:12px; }
    .chart-title h3 { margin:0; font-size:1.45rem; }
    .chart-title p { margin:0; color:var(--muted); font-size:.86rem; }
    #productivity-chart { display:block; width:100%; height:auto; overflow:visible; border:2px solid var(--ink); background:var(--white); }
    .chart-grid { stroke:#1515121f; stroke-width:1; }
    .chart-axis { fill:var(--muted); font:700 12px "Source Sans 3",sans-serif; }
    .chart-label { font:800 13px "Source Sans 3",sans-serif; }
    .chart-actual { fill:none; stroke:var(--blue); stroke-width:4; }
    .chart-trend { fill:none; stroke:var(--red); stroke-width:4; stroke-dasharray:10 7; }
    .chart-modest { fill:none; stroke:var(--muted); stroke-width:3; stroke-dasharray:2 7; }
    .chart-gap { fill:#c4373214; }
    .chart-marker { stroke:var(--white); stroke-width:2; }
    .section-break { margin-top:74px; padding-top:32px; border-top:3px solid var(--ink); }
    .policy-list { display:grid; gap:22px; margin-top:34px; }
    .policy { display:grid; grid-template-columns:92px minmax(0,1.1fr) minmax(280px,.9fr); border:2px solid var(--ink); background:var(--white); box-shadow:var(--shadow); }
    .policy-number { display:flex; align-items:flex-start; justify-content:center; padding:24px 12px; border-right:2px solid var(--ink); background:var(--yellow); font-size:3.3rem; line-height:1; }
    .policy-main,.policy-proof { padding:24px; }
    .policy-proof { border-left:2px solid var(--ink); background:#f8f5ed; }
    .policy-head { display:flex; align-items:start; justify-content:space-between; gap:18px; }
    .policy h3 { margin:0; font-size:1.55rem; line-height:1.05; }
    .period { flex:none; color:var(--muted); font-weight:800; }
    .channels { display:flex; flex-wrap:wrap; gap:7px; margin:13px 0 0; padding:0; list-style:none; }
    .channels li { padding:3px 8px; border:1.5px solid var(--ink); border-radius:999px; background:var(--paper); font-size:.74rem; font-weight:800; }
    .policy-main p,.policy-proof p { margin:15px 0 0; }
    .policy-main strong,.policy-proof strong { display:block; margin-bottom:2px; font-size:.75rem; letter-spacing:.08em; text-transform:uppercase; }
    .grade { display:inline-flex; align-items:center; gap:8px; font-weight:800; }
    .grade b { display:grid; width:34px; height:34px; place-items:center; border:2px solid var(--ink); border-radius:50%; background:var(--yellow); }
    .budget-link { padding-top:14px; border-top:1.5px solid var(--line); font-weight:800; }
    .boundary { padding-top:14px; border-top:1.5px solid var(--line); color:var(--muted); }
    .budget-intro { display:grid; grid-template-columns:minmax(0,1fr) 310px; gap:34px; align-items:start; }
    .budget-method { padding:20px; border:2px solid var(--ink); background:var(--yellow); box-shadow:5px 5px 0 var(--ink); font-weight:700; }
    .budget-method p { margin:0; }
    .budget-method p + p { margin-top:12px; }
    .budget-list { margin-top:30px; border:2px solid var(--ink); background:var(--white); box-shadow:var(--shadow); }
    .budget-header,.budget-row { display:grid; grid-template-columns:110px minmax(260px,1fr) 145px 145px 170px; gap:0; align-items:center; }
    .budget-header { border-bottom:2px solid var(--ink); background:var(--ink); color:var(--white); font-size:.72rem; font-weight:800; letter-spacing:.08em; text-transform:uppercase; }
    .budget-header > span,.budget-row > div { padding:12px 14px; }
    .budget-row + .budget-row { border-top:1.5px solid var(--line); }
    .budget-code { font-weight:800; }
    .budget-name { min-width:0; }
    .budget-name strong { display:block; line-height:1.15; }
    .budget-bar-track { height:6px; margin-top:9px; background:#15151217; }
    .budget-bar { display:block; height:100%; background:var(--red); }
    .budget-value { text-align:right; font-variant-numeric:tabular-nums; }
    .budget-delta { color:var(--red); font-weight:800; text-align:right; font-variant-numeric:tabular-nums; }
    .budget-delta small { display:block; color:var(--muted); font-size:.74rem; }
    .budget-kind { display:block; margin-top:5px; color:var(--muted); font-size:.72rem; font-weight:800; }
    .budget-excluded { margin-top:24px; padding:18px 20px; border-left:8px solid var(--orange); background:var(--white); }
    .budget-excluded strong { display:block; }
    .budget-source { margin-top:18px; color:var(--muted); font-size:.86rem; }
    .history-explorer { margin-top:30px; padding:24px; border:2px solid var(--ink); background:var(--white); box-shadow:var(--shadow); }
    .history-head { display:flex; align-items:end; justify-content:space-between; gap:24px; }
    .history-head h3 { margin:0; font-size:1.45rem; }
    .history-head p { margin:5px 0 0; color:var(--muted); }
    .history-picker { min-width:min(360px,100%); }
    .history-picker label { display:block; margin-bottom:5px; font-size:.72rem; font-weight:800; letter-spacing:.08em; text-transform:uppercase; }
    .history-picker select { width:100%; padding:10px 12px; border:2px solid var(--ink); border-radius:0; background:var(--paper); color:var(--ink); font:700 1rem "Source Sans 3",sans-serif; }
    #budget-history-chart,#tax-history-chart { display:block; width:100%; height:auto; margin-top:22px; border:2px solid var(--ink); background:var(--white); }
    .history-axis { fill:var(--muted); font:700 12px "Source Sans 3",sans-serif; }
    .history-grid { stroke:#1515121f; stroke-width:1; }
    .history-line { fill:none; stroke:var(--red); stroke-width:4; }
    .history-area { fill:#c4373214; }
    .history-point { fill:var(--red); stroke:var(--white); stroke-width:2; }
    .history-compare { fill:var(--yellow); stroke:var(--ink); stroke-width:2; }
    .history-guardrail { fill:var(--orange); stroke:var(--ink); stroke-width:1.5; }
    .history-note { margin:14px 0 0; color:var(--muted); font-size:.86rem; }
    .tax-panel { margin-top:34px; padding:28px; border:2px solid var(--ink); background:var(--white); box-shadow:var(--shadow); }
    .tax-controls { display:grid; grid-template-columns:minmax(260px,.8fr) minmax(0,1.2fr); gap:32px; align-items:start; }
    .tax-control { padding:20px; background:var(--yellow); border:2px solid var(--ink); }
    .tax-control label { display:flex; justify-content:space-between; gap:20px; font-weight:800; }
    .tax-control output { font-family:"Archivo Black",sans-serif; font-size:1.6rem; line-height:1; }
    .tax-control input { width:100%; margin:20px 0 5px; accent-color:var(--ink); }
    .tax-control small { color:var(--muted); }
    .tax-answer { margin:0; font-size:1.2rem; font-weight:800; }
    .tax-answer strong { display:block; margin-top:8px; color:var(--green); font:2.2rem/.95 "Archivo Black",sans-serif; }
    .tax-stats { display:grid; grid-template-columns:repeat(3,1fr); margin-top:22px; border:2px solid var(--ink); }
    .tax-stat { padding:18px; }
    .tax-stat + .tax-stat { border-left:2px solid var(--ink); }
    .tax-stat span { display:block; color:var(--muted); font-size:.72rem; font-weight:800; letter-spacing:.08em; text-transform:uppercase; }
    .tax-stat b { display:block; margin-top:5px; font:1.7rem/1 "Archivo Black",sans-serif; }
    .tax-actual-line { fill:none; stroke:var(--blue); stroke-width:4; }
    .tax-scenario-line { fill:none; stroke:var(--green); stroke-width:4; stroke-dasharray:10 6; }
    .tax-actual-point { fill:var(--blue); stroke:var(--white); stroke-width:2; }
    .tax-scenario-point { fill:var(--green); stroke:var(--white); stroke-width:2; }
    .tax-legend { display:flex; flex-wrap:wrap; gap:18px; margin-top:12px; font-weight:700; }
    .tax-legend span::before { display:inline-block; width:22px; height:4px; margin:0 7px 3px 0; content:""; background:var(--blue); }
    .tax-legend .scenario::before { background:var(--green); }
    .tax-break-even { margin-top:22px; padding:18px 20px; border-left:8px solid var(--green); background:#eaf7e9; font-weight:700; }
    .tax-caveat { margin-top:20px; padding-top:18px; border-top:2px solid var(--ink); color:var(--muted); }
    .tax-evidence { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:16px; margin-top:22px; }
    .tax-evidence article { padding:18px; background:var(--paper); border:1.5px solid var(--ink); }
    .tax-evidence strong,.tax-evidence a { display:block; }
    .tax-evidence a { margin-top:8px; font-weight:800; }
    .excluded { display:grid; grid-template-columns:240px 1fr; gap:24px; margin-top:70px; padding:26px; border:2px solid var(--ink); background:var(--ink); color:var(--white); box-shadow:var(--shadow); }
    .excluded h3 { margin:0; color:var(--yellow); font-size:1.45rem; }
    .excluded ul { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:8px 26px; margin:0; padding-left:20px; }
    .sources { margin-top:54px; color:var(--muted); }
    .sources h3 { margin:0 0 12px; color:var(--ink); font-size:1.3rem; }
    .sources ol { margin:0; padding-left:22px; }
    .sources li + li { margin-top:6px; }
    .sources a { font-weight:700; }
    @media (max-width:1040px) { .budget-header,.budget-row { grid-template-columns:100px minmax(220px,1fr) 125px 125px 150px; } }
    @media (max-width:900px) { .policy { grid-template-columns:72px 1fr; } .policy-number { grid-row:1 / span 2; } .policy-proof { grid-column:2; border-top:2px solid var(--ink); border-left:0; } .excluded,.budget-intro,.tax-controls { grid-template-columns:1fr; } .budget-header { display:none; } .budget-row { grid-template-columns:100px 1fr 1fr; padding:14px; } .budget-row > div { padding:6px 8px; } .budget-name { grid-column:2 / -1; } .budget-value { text-align:left; } .budget-value::before { display:block; color:var(--muted); font-size:.68rem; font-weight:800; letter-spacing:.06em; text-transform:uppercase; content:attr(data-label); } .budget-delta { text-align:left; } .history-head { display:block; } .history-picker { margin-top:16px; } }
    @media (max-width:680px) { body { font-size:16px; } .wrap { width:min(100% - 28px,1180px); } .hero-inner { padding-bottom:42px; } h1 { margin-top:46px; } .index-strip { grid-template-columns:1fr; } .index-item + .index-item { border-top:2px solid var(--ink); border-left:0; } .chart-title { display:block; } .chart-title p { margin-top:5px; } .policy { grid-template-columns:1fr; } .policy-number { display:block; grid-row:auto; padding:15px 20px; border-right:0; border-bottom:2px solid var(--ink); font-size:2.2rem; } .policy-proof { grid-column:auto; } .policy-head { display:block; } .period { display:block; margin-top:4px; } .excluded ul { grid-template-columns:1fr; } .budget-row { grid-template-columns:1fr 1fr; } .budget-code,.budget-name { grid-column:1 / -1; } .budget-delta { grid-column:1 / -1; padding-top:10px !important; border-top:1px solid var(--line); } .history-explorer,.tax-panel { padding:15px; } .tax-stats { grid-template-columns:1fr; } .tax-stat + .tax-stat { border-top:2px solid var(--ink); border-left:0; } .tax-evidence { grid-template-columns:1fr; } }
  </style>
</head>
<body>
  <header class="hero"><div class="wrap hero-inner">
    <a class="back" href="visualisointidemot.html">&larr; Visualisointidemot</a>
    <h1>Kasvukuilu<span>ei ole yksi syy</span></h1>
    <p class="lede">Suomen työn tuottavuus jäi vuoden 2008 jälkeen kauas aiemmasta urasta. Tämä ensimmäinen evidenssikerros erottaa valtion politiikan mahdolliset kasvujarrut ulkoisista rakennemuutoksista ja näyttää, mitä voidaan oikeasti väittää.</p>
  </div></header>
  <main class="wrap">
    <p class="eyebrow">Tuottavuus · 2008 = 100</p>
    <h2>Toteutunut taso 105. Vanhan kasvuvauhdin mekaaninen jatke 148.</h2>
    <p class="intro">Mittari on reaalinen BKT tehtyä työtuntia kohti. Vertailu-ura jatkaa vuosien 1995–2008 toteutunutta kasvuvauhtia. Se ei ole virallinen arvio siitä, mihin Suomen olisi varmasti pitänyt päästä.</p>
    <p class="warning"><strong>Keskeinen rajoitus:</strong> alla olevien politiikkatoimien vaikutuksia ei summata eikä 43 indeksipisteen kuilua jaeta niiden kesken. Arviot käyttävät eri mittareita, vaikutukset menevät päällekkäin ja suuri osa muutoksesta johtuu politiikan ulkopuolisista häiriöistä.</p>
    <section class="index-strip" aria-label="Tuottavuusindeksin vertailu" id="index-strip"></section>
    <section class="chart-shell" aria-labelledby="chart-heading">
      <div class="chart-title"><h3 id="chart-heading">Tuottavuuden toteutunut ja vaihtoehtoinen ura</h3><p>Reaalinen BKT / tehty työtunti</p></div>
      <svg id="productivity-chart" viewBox="0 0 1120 430" role="img" aria-labelledby="chart-heading chart-description"><desc id="chart-description">Tuottavuusindeksi vuodesta 2008 vuoteen 2025: toteutunut kehitys, yhden prosentin kasvu-ura ja vuosien 1995–2008 kasvuvauhdin mekaaninen jatke.</desc></svg>
    </section>

    <section class="section-break" aria-labelledby="policy-heading">
      <p class="eyebrow">Ensimmäinen politiikka-auditointi</p>
      <h2 id="policy-heading">Kuusi päätöskokonaisuutta, joista on riittävästi näyttöä jatkoanalyysiin</h2>
      <p class="intro">Järjestys kuvaa tutkimus- ja data-auditoinnin prioriteettia, ei laskettua osuutta kasvukuilusta.</p>
      <div class="policy-list" id="policy-list"></div>
    </section>

    <section class="section-break" aria-labelledby="budget-heading">
      <div class="budget-intro">
        <div>
          <p class="eyebrow">Budjettihaukan toteumadata · 2011–2016</p>
          <h2 id="budget-heading">Näillä momenteilla toteutunut rahankäyttö väheni</h2>
          <p class="intro">Vertailussa käytetään valtion budjettitalouden toteutunutta nettokertymää. Kaikki rivit ovat menoja, kokonaisia vuosia ja samalla momenttikoodilla vertailtavia. Eurot on muunnettu vuoden 2025 rahaksi.</p>
        </div>
        <aside class="budget-method">
          <p><strong id="budget-count"></strong> tarkistettua momenttia</p>
          <p>Nimellinen vähennys ja ostovoiman lasku näytetään eri luokkina.</p>
          <p>Toteuman lasku ei yksin todista vaikutusta talouskasvuun.</p>
        </aside>
      </div>
      <div class="history-explorer">
        <div class="history-head">
          <div><h3 id="budget-history-heading">Momentin historiallinen kehitys</h3><p id="budget-history-context">Toteutunut meno vuoden 2025 rahassa</p></div>
          <div class="history-picker"><label for="budget-moment-select">Näytettävä momentti</label><select id="budget-moment-select"></select></div>
        </div>
        <svg id="budget-history-chart" viewBox="0 0 1120 430" role="img" aria-labelledby="budget-history-heading budget-history-description"><desc id="budget-history-description">Valitun budjettimomentin toteutunut reaalinen nettokertymä vuosina 2008–2025.</desc></svg>
        <p class="history-note" id="budget-history-note"></p>
      </div>
      <div class="budget-list" id="budget-list" aria-label="Momenttikohtaiset toteumamuutokset">
        <div class="budget-header" aria-hidden="true"><span>Momentti</span><span>Kohde</span><span>2011</span><span>2016</span><span>Muutos</span></div>
        <div id="budget-rows"></div>
      </div>
      <div class="budget-excluded" id="budget-excluded"></div>
      <p class="budget-source" id="budget-source"></p>
    </section>

    <section class="section-break" aria-labelledby="tax-heading">
      <p class="eyebrow">Verotus, kasvu ja verokertymä · 2008–2025</p>
      <h2 id="tax-heading">Voiko alempi verotus tuottaa suuremman verokertymän?</h2>
      <p class="intro">Voi kirjanpidollisesti, jos veropohja kasvaa veroasteen laskua enemmän. Alla oleva laskuri näyttää, mitä valtion verokertymälle olisi tapahtunut 1 prosentin tuottavuusuralla eri efektiivisen veroasteen oletuksilla. Se ei oleta, että veronalennus olisi yksin synnyttänyt kasvun.</p>
      <div class="tax-panel">
        <div class="tax-controls">
          <div class="tax-control">
            <label for="tax-cut-range"><span>Efektiivinen veroaste</span><output id="tax-cut-output">−5,0 %</output></label>
            <input id="tax-cut-range" type="range" min="0" max="15" step="0.5" value="5" aria-describedby="tax-cut-definition">
            <small id="tax-cut-definition">Suhteellinen alennus, ei prosenttiyksikköä työn verokiilasta.</small>
          </div>
          <p class="tax-answer" id="tax-answer" aria-live="polite"></p>
        </div>
        <div class="tax-stats">
          <div class="tax-stat"><span>Toteutunut verokertymä 2025</span><b id="tax-actual-stat"></b></div>
          <div class="tax-stat"><span>1 % kasvu-uralla</span><b id="tax-scenario-stat"></b></div>
          <div class="tax-stat"><span>Ero toteutuneeseen</span><b id="tax-difference-stat"></b></div>
        </div>
        <svg id="tax-history-chart" viewBox="0 0 1120 430" role="img" aria-labelledby="tax-heading tax-chart-description"><desc id="tax-chart-description">Valtion toteutunut reaalinen verokertymä ja yhden prosentin tuottavuusuran laskennallinen verokertymä valitulla efektiivisen veroasteen alennuksella vuosina 2008–2025.</desc></svg>
        <div class="tax-legend"><span>Toteutunut verokertymä</span><span class="scenario">1 % kasvu-ura ja alempi veroaste</span></div>
        <div class="tax-break-even" id="tax-break-even"></div>
        <p class="tax-caveat" id="tax-caveat"></p>
        <div class="tax-evidence" id="tax-evidence"></div>
      </div>
    </section>

    <section class="excluded" aria-labelledby="excluded-heading">
      <h3 id="excluded-heading">Ei kirjata politiikan vaikutukseksi</h3>
      <ul id="excluded-list"></ul>
    </section>

    <section class="sources" aria-labelledby="sources-heading">
      <h3 id="sources-heading">Lähteet</h3>
      <ol id="sources-list"></ol>
    </section>
  </main>
  <script type="application/json" id="ledger-data">__LEDGER__</script>
  <script type="application/json" id="productivity-data">__PRODUCTIVITY__</script>
  <script type="application/json" id="budget-evidence-data">__BUDGET_EVIDENCE__</script>
  <script type="application/json" id="tax-scenario-data">__TAX_SCENARIO__</script>
  <script>
    const LEDGER = JSON.parse(document.getElementById('ledger-data').textContent);
    const PRODUCTIVITY = JSON.parse(document.getElementById('productivity-data').textContent);
    const BUDGET = JSON.parse(document.getElementById('budget-evidence-data').textContent);
    const TAX = JSON.parse(document.getElementById('tax-scenario-data').textContent);
    const fi = (value, digits = 0) => Number(value).toLocaleString('fi-FI', {minimumFractionDigits:digits, maximumFractionDigits:digits});
    const baseYear = LEDGER.meta.outcome_contract.base_year;
    const endYear = LEDGER.meta.outcome_contract.end_year;
    const rows = PRODUCTIVITY.years.filter(row => row.year >= baseYear && row.year <= endYear);
    const final = rows.at(-1);
    const values = [
      {className:'actual', label:'Toteutunut', value:final.productivity_index, context:`Kasvu ${fi(PRODUCTIVITY.meta.actual_rate_pct,2)} % vuodessa`},
      {className:'modest', label:'Vaatimaton vertailu', value:final.modest_index, context:`Kasvu ${fi(PRODUCTIVITY.meta.modest_rate_pct,1)} % vuodessa`},
      {className:'trend', label:'Vanhan vauhdin jatke', value:final.trend_index, context:`Kasvu ${fi(PRODUCTIVITY.meta.trend_rate_pct,2)} % vuodessa`}
    ];
    document.getElementById('index-strip').innerHTML = values.map(item => `<article class="index-item ${item.className}"><span class="index-label">${item.label}</span><span class="index-value">${fi(item.value)}</span><span class="index-context">${item.context}</span></article>`).join('');

    const svg = document.getElementById('productivity-chart');
    const ns = 'http://www.w3.org/2000/svg';
    const drawChart = () => {
      svg.querySelectorAll(':scope > :not(desc)').forEach(node => node.remove());
      const width = Math.max(320, Math.round(svg.getBoundingClientRect().width));
      const compact = width < 680;
      svg.setAttribute('viewBox', `0 0 ${width} 430`);
      const add = (name, attrs, text = '') => {
        const node = document.createElementNS(ns, name);
        Object.entries(attrs).forEach(([key,value]) => node.setAttribute(key,value));
        if (text) node.textContent = text;
        svg.appendChild(node);
        return node;
      };
      const frame = {left:compact?48:72,right:width-(compact?18:78),top:36,bottom:365};
      const x = year => frame.left + (year - baseYear) / (endYear - baseYear) * (frame.right - frame.left);
      const y = value => frame.bottom - (value - 95) / (155 - 95) * (frame.bottom - frame.top);
      for (const tick of [100,110,120,130,140,150]) {
        add('line',{x1:frame.left,x2:frame.right,y1:y(tick),y2:y(tick),class:'chart-grid'});
        add('text',{x:frame.left-9,y:y(tick)+4,'text-anchor':'end',class:'chart-axis'},String(tick));
      }
      for (const year of [2008,2012,2016,2020,2025]) {
        add('text',{x:x(year),y:frame.bottom+29,'text-anchor':year===baseYear?'start':year===endYear?'end':'middle',class:'chart-axis'},String(year));
      }
      const pathFor = key => rows.map((row,index) => `${index?'L':'M'} ${x(row.year).toFixed(1)} ${y(row[key]).toFixed(1)}`).join(' ');
      const area = `${rows.map((row,index) => `${index?'L':'M'} ${x(row.year).toFixed(1)} ${y(row.trend_index).toFixed(1)}`).join(' ')} ${[...rows].reverse().map(row => `L ${x(row.year).toFixed(1)} ${y(row.productivity_index).toFixed(1)}`).join(' ')} Z`;
      add('path',{d:area,class:'chart-gap'});
      add('path',{d:pathFor('trend_index'),class:'chart-trend'});
      add('path',{d:pathFor('modest_index'),class:'chart-modest'});
      add('path',{d:pathFor('productivity_index'),class:'chart-actual'});
      for (const item of [
        {key:'trend_index',label:compact?`Jatke ${fi(final.trend_index)}`:`Vanhan vauhdin jatke ${fi(final.trend_index)}`,color:'var(--red)',offset:-9},
        {key:'modest_index',label:`1 % vuodessa ${fi(final.modest_index)}`,color:'var(--muted)',offset:4},
        {key:'productivity_index',label:`Toteutunut ${fi(final.productivity_index)}`,color:'var(--blue)',offset:5}
      ]) {
        add('circle',{cx:x(endYear),cy:y(final[item.key]),r:6,fill:item.color,class:'chart-marker'});
        add('text',{x:x(endYear)-10,y:y(final[item.key])+item.offset,'text-anchor':'end',fill:item.color,class:'chart-label'},item.label);
      }
      add('text',{x:frame.left,y:18,class:'chart-axis'},'Indeksi, 2008 = 100');
    };
    drawChart();
    new ResizeObserver(drawChart).observe(svg);

    const gradeLabels = LEDGER.meta.evidence_grades;
    const budgetCounts = new Map();
    BUDGET.rows.forEach(row => [row.primary_policy_id,...row.related_policy_ids].forEach(policyId => budgetCounts.set(policyId,(budgetCounts.get(policyId)||0)+1)));
    document.getElementById('policy-list').innerHTML = [...LEDGER.policies].sort((a,b) => a.priority-b.priority).map(policy => `
      <article class="policy">
        <div class="policy-number" aria-hidden="true">${String(policy.priority).padStart(2,'0')}</div>
        <div class="policy-main">
          <div class="policy-head"><h3>${policy.title_fi}</h3><span class="period">${policy.period_label_fi}</span></div>
          <ul class="channels">${policy.channels.map(channel => `<li>${channel}</li>`).join('')}</ul>
          <p><strong>Politiikkahavainto</strong>${policy.decision_fi}</p>
          <p><strong>Vaikutuskanava</strong>${policy.mechanism_fi}</p>
          <p><strong>Vertailuvaihtoehto</strong>${policy.counterfactual_fi}</p>
        </div>
        <div class="policy-proof">
          <div class="grade"><b>${policy.evidence_grade}</b><span>${gradeLabels[policy.evidence_grade]}</span></div>
          <p><strong>Mitä data näyttää</strong>${policy.observed_evidence_fi}</p>
          ${budgetCounts.has(policy.policy_id)?`<p class="budget-link"><strong>Budjettihaukan toteumadata</strong>${budgetCounts.get(policy.policy_id)} tarkistettua momenttia vuosilta 2011 ja 2016</p>`:''}
          <p class="boundary"><strong>Mitä ei voi päätellä</strong>${policy.claim_boundary_fi}</p>
        </div>
      </article>`).join('');

    const maxCut = Math.max(...BUDGET.rows.map(row => Math.abs(row.real_change_meur)));
    document.getElementById('budget-count').textContent = BUDGET.rows.length;
    document.getElementById('budget-rows').innerHTML = BUDGET.rows.map(row => {
      const kind = row.evidence_class === 'nominal_and_real_reduction' ? 'Nimellinen ja reaalinen vähennys' : 'Vain reaaliarvo pieneni';
      const width = 10 + 90 * Math.abs(row.real_change_meur) / maxCut;
      return `<article class="budget-row">
        <div class="budget-code">${row.momentti_tunnusp}</div>
        <div class="budget-name"><strong>${row.canonical_label_fi}</strong><span class="budget-kind">${kind}</span><div class="budget-bar-track" aria-hidden="true"><span class="budget-bar" style="width:${width.toFixed(1)}%"></span></div></div>
        <div class="budget-value" data-label="2011">${fi(row.baseline.real_meur,1)} milj. €</div>
        <div class="budget-value" data-label="2016">${fi(row.comparison.real_meur,1)} milj. €</div>
        <div class="budget-delta">${fi(row.real_change_meur,1)} milj. €<small>${fi(row.real_change_pct,1)} %</small></div>
      </article>`;
    }).join('');
    document.getElementById('budget-excluded').innerHTML = BUDGET.excluded_comparisons.map(item => `<strong>Rajattu pois suorasta vertailusta: ${item.concept_id}</strong>${item.reason_fi}`).join('');
    document.getElementById('budget-source').innerHTML = `Lähde: <a href="${BUDGET.meta.official_source_url}" target="_blank" rel="noreferrer">${BUDGET.meta.official_source_name}</a>, jalostettu taulu ${BUDGET.meta.source_table}. Mittari: ${BUDGET.meta.source_metric_fi}. Poimittu ${new Date(BUDGET.meta.generated_at).toLocaleDateString('fi-FI')}.`;

    const budgetSelect = document.getElementById('budget-moment-select');
    const budgetHistorySvg = document.getElementById('budget-history-chart');
    budgetSelect.innerHTML = BUDGET.rows.map((row,index) => `<option value="${index}">${row.momentti_tunnusp} ${row.canonical_label_fi}</option>`).join('');
    const drawBudgetHistory = () => {
      const row = BUDGET.rows[Number(budgetSelect.value || 0)];
      const history = row.history;
      budgetHistorySvg.querySelectorAll(':scope > :not(desc)').forEach(node => node.remove());
      const width = Math.max(320,Math.round(budgetHistorySvg.getBoundingClientRect().width));
      const compact = width < 680;
      budgetHistorySvg.setAttribute('viewBox',`0 0 ${width} 430`);
      const add = (name,attrs,text='') => {
        const node = document.createElementNS(ns,name);
        Object.entries(attrs).forEach(([key,value]) => node.setAttribute(key,value));
        if (text) node.textContent = text;
        budgetHistorySvg.appendChild(node);
        return node;
      };
      const frame = {left:compact?58:76,right:width-(compact?18:42),top:42,bottom:356};
      const x = year => frame.left + (year-2008)/(2025-2008)*(frame.right-frame.left);
      const maxValue = Math.max(...history.map(item => item.real_meur));
      const yMax = Math.ceil(maxValue*1.15/10)*10 || 10;
      const y = value => frame.bottom-value/yMax*(frame.bottom-frame.top);
      for (let step=0;step<=4;step+=1) {
        const value = yMax*step/4;
        add('line',{x1:frame.left,x2:frame.right,y1:y(value),y2:y(value),class:'history-grid'});
        add('text',{x:frame.left-9,y:y(value)+4,'text-anchor':'end',class:'history-axis'},fi(value,0));
      }
      const yearTicks = compact ? [2008,2014,2020,2025] : [2008,2012,2016,2020,2025];
      yearTicks.forEach(year => add('text',{x:x(year),y:frame.bottom+28,'text-anchor':year===2008?'start':year===2025?'end':'middle',class:'history-axis'},String(year)));
      const line = history.map((item,index) => `${index?'L':'M'} ${x(item.year).toFixed(1)} ${y(item.real_meur).toFixed(1)}`).join(' ');
      const area = `${line} L ${x(history.at(-1).year).toFixed(1)} ${frame.bottom} L ${x(history[0].year).toFixed(1)} ${frame.bottom} Z`;
      add('path',{d:area,class:'history-area'});
      add('path',{d:line,class:'history-line'});
      history.forEach(item => {
        const compare = item.year===BUDGET.meta.baseline_year || item.year===BUDGET.meta.comparison_year;
        const className = item.has_structural_guardrail ? 'history-guardrail' : compare ? 'history-compare' : 'history-point';
        add('circle',{cx:x(item.year),cy:y(item.real_meur),r:item.has_structural_guardrail?7:compare?6:4,class:className});
        if (compare) add('text',{x:x(item.year),y:y(item.real_meur)-12,'text-anchor':'middle',class:'chart-label'},`${item.year}: ${fi(item.real_meur,1)}`);
      });
      const first = history[0];
      const last = history.at(-1);
      add('text',{x:frame.left,y:20,class:'history-axis'},'Milj. euroa, vuoden 2025 rahassa');
      add('text',{x:x(last.year)-5,y:y(last.real_meur)-12,'text-anchor':'end',class:'chart-label'},`${last.year}: ${fi(last.real_meur,1)} milj. €`);
      document.getElementById('budget-history-heading').textContent = row.canonical_label_fi;
      document.getElementById('budget-history-context').textContent = `${first.year}–${last.year} · tarkistettu koodijatkumo`;
      const codeTrail = [...new Set(history.flatMap(item => item.source_codes))].join(' → ');
      const guardrail = row.history_has_structural_guardrails
        ? ' Oranssi piste tarkoittaa momenttirakenteen muutosta: muutosta sen yli ei tulkita automaattisesti leikkaukseksi.'
        : '';
      document.getElementById('budget-history-note').textContent = `Käytetyt momenttikoodit: ${codeTrail} Keltaiset pisteet ovat suoran 2011–2016-vertailun vuodet.${guardrail}`;
    };
    budgetSelect.addEventListener('change',drawBudgetHistory);
    drawBudgetHistory();
    new ResizeObserver(drawBudgetHistory).observe(budgetHistorySvg);

    const taxSlider = document.getElementById('tax-cut-range');
    const taxHistorySvg = document.getElementById('tax-history-chart');
    const taxScenarioRows = rateCut => TAX.rows.map(row => ({
      ...row,
      scenario_revenue_beur:row.scenario_revenue_unchanged_rate_beur*(1-rateCut/100)
    }));
    const drawTaxHistory = rateCut => {
      const taxRows = taxScenarioRows(rateCut);
      taxHistorySvg.querySelectorAll(':scope > :not(desc)').forEach(node => node.remove());
      const width = Math.max(320,Math.round(taxHistorySvg.getBoundingClientRect().width));
      const compact = width < 680;
      taxHistorySvg.setAttribute('viewBox',`0 0 ${width} 430`);
      const add = (name,attrs,text='') => {
        const node = document.createElementNS(ns,name);
        Object.entries(attrs).forEach(([key,value]) => node.setAttribute(key,value));
        if (text) node.textContent = text;
        taxHistorySvg.appendChild(node);
        return node;
      };
      const frame = {left:compact?58:76,right:width-(compact?18:52),top:46,bottom:356};
      const x = year => frame.left+(year-2008)/(2025-2008)*(frame.right-frame.left);
      const allValues = taxRows.flatMap(item => [item.real_tax_revenue_beur,item.scenario_revenue_beur]);
      const yMin = Math.floor(Math.min(...allValues)*0.9/5)*5;
      const yMax = Math.ceil(Math.max(...allValues)*1.1/5)*5;
      const y = value => frame.bottom-(value-yMin)/(yMax-yMin)*(frame.bottom-frame.top);
      for (let step=0;step<=4;step+=1) {
        const value = yMin+(yMax-yMin)*step/4;
        add('line',{x1:frame.left,x2:frame.right,y1:y(value),y2:y(value),class:'history-grid'});
        add('text',{x:frame.left-9,y:y(value)+4,'text-anchor':'end',class:'history-axis'},fi(value,0));
      }
      const yearTicks = compact ? [2008,2014,2020,2025] : [2008,2012,2016,2020,2025];
      yearTicks.forEach(year => add('text',{x:x(year),y:frame.bottom+28,'text-anchor':year===2008?'start':year===2025?'end':'middle',class:'history-axis'},String(year)));
      const path = key => taxRows.map((item,index) => `${index?'L':'M'} ${x(item.year).toFixed(1)} ${y(item[key]).toFixed(1)}`).join(' ');
      add('path',{d:path('real_tax_revenue_beur'),class:'tax-actual-line'});
      add('path',{d:path('scenario_revenue_beur'),class:'tax-scenario-line'});
      taxRows.forEach(item => {
        add('circle',{cx:x(item.year),cy:y(item.real_tax_revenue_beur),r:3,class:'tax-actual-point'});
        add('circle',{cx:x(item.year),cy:y(item.scenario_revenue_beur),r:3,class:'tax-scenario-point'});
      });
      const last = taxRows.at(-1);
      add('text',{x:frame.left,y:20,class:'history-axis'},'Mrd. euroa, vuoden 2025 rahassa');
      add('text',{x:x(last.year)-8,y:y(last.real_tax_revenue_beur)+18,'text-anchor':'end',class:'chart-label'},`Toteutunut ${fi(last.real_tax_revenue_beur,1)}`);
      add('text',{x:x(last.year)-8,y:y(last.scenario_revenue_beur)-10,'text-anchor':'end',class:'chart-label'},`Skenaario ${fi(last.scenario_revenue_beur,1)}`);
    };
    const updateTaxScenario = () => {
      const rateCut = Number(taxSlider.value);
      const actual = TAX.summary_2025.actual_tax_revenue_beur;
      const scenario = TAX.summary_2025.unchanged_rate_revenue_beur*(1-rateCut/100);
      const difference = scenario-actual;
      const direction = difference>=0 ? 'enemmän' : 'vähemmän';
      document.getElementById('tax-cut-output').textContent = `−${fi(rateCut,1)} %`;
      document.getElementById('tax-actual-stat').textContent = `${fi(actual,1)} mrd. €`;
      document.getElementById('tax-scenario-stat').textContent = `${fi(scenario,1)} mrd. €`;
      document.getElementById('tax-difference-stat').textContent = `${difference>=0?'+':'−'}${fi(Math.abs(difference),1)} mrd. €`;
      document.getElementById('tax-answer').innerHTML = `Jos tuottavuus olisi kasvanut yhden prosentin vuodessa ja valtion efektiivinen veroaste olisi ollut ${fi(rateCut,1)} % alempi, vuoden 2025 verokertymä olisi ollut laskennallisesti <strong>${fi(Math.abs(difference),1)} mrd. € ${direction} kuin toteutunut</strong>`;
      document.getElementById('tax-break-even').innerHTML = `Vuoden 2025 laskennallinen raja: verokertymä olisi pysynyt toteutunutta suurempana enintään noin <strong>${fi(TAX.summary_2025.break_even_relative_rate_cut_pct,1)} prosentin</strong> suhteellisella efektiivisen veroasteen alennuksella, jos yhden prosentin tuottavuusura olisi toteutunut.`;
      drawTaxHistory(rateCut);
    };
    taxSlider.addEventListener('input',updateTaxScenario);
    updateTaxScenario();
    new ResizeObserver(() => drawTaxHistory(Number(taxSlider.value))).observe(taxHistorySvg);
    document.getElementById('tax-caveat').innerHTML = `<strong>Rajaus:</strong> ${TAX.meta.tax_scope_fi} ${TAX.meta.causality_warning_fi}`;
    document.getElementById('tax-evidence').innerHTML = TAX.meta.evidence.map(item => `<article><strong>${item.publisher}: ${item.title}</strong><span>${item.finding_fi}</span><a href="${item.url}" target="_blank" rel="noreferrer">Avaa lähde</a></article>`).join('');

    document.getElementById('excluded-list').innerHTML = LEDGER.meta.excluded_from_policy_attribution.map(item => `<li>${item}</li>`).join('');
    const uniqueSources = new Map();
    LEDGER.policies.forEach(policy => policy.sources.forEach(source => uniqueSources.set(source.source_id, source)));
    document.getElementById('sources-list').innerHTML = [...uniqueSources.values()].map(source => `<li><a href="${source.url}" target="_blank" rel="noreferrer">${source.publisher}: ${source.title}</a></li>`).join('');
  </script>
</body>
</html>'''


def build_html(
    ledger: dict[str, Any],
    productivity: dict[str, Any],
    budget_evidence: dict[str, Any],
    tax_scenario: dict[str, Any],
) -> str:
    return (
        TEMPLATE.replace("__LEDGER__", _json_for_html(ledger))
        .replace("__PRODUCTIVITY__", _json_for_html(productivity))
        .replace("__BUDGET_EVIDENCE__", _json_for_html(budget_evidence))
        .replace("__TAX_SCENARIO__", _json_for_html(tax_scenario))
    )


def main() -> int:
    ledger = load_json(LEDGER_PATH)
    productivity = load_json(PRODUCTIVITY_PATH)
    budget_evidence = load_json(BUDGET_EVIDENCE_PATH)
    tax_scenario = load_json(TAX_SCENARIO_PATH)
    validate_ledger(ledger, productivity)
    validate_budget_evidence(budget_evidence, ledger)
    validate_tax_scenario(tax_scenario)
    OUTPUT_PATH.write_text(
        build_html(ledger, productivity, budget_evidence, tax_scenario),
        encoding="utf-8",
    )
    print(OUTPUT_PATH.relative_to(ROOT))
    print(f"  {len(ledger['policies'])} politiikkakokonaisuutta")
    print(f"  {len(budget_evidence['rows'])} tarkistettua budjettimomenttia")
    print(
        "  vuoden 2025 valtion verot: "
        f"{tax_scenario['summary_2025']['actual_tax_revenue_beur']:.1f} mrd. euroa"
    )
    print("  politiikkavaikutuksia ei summata tuottavuuskuiluksi")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

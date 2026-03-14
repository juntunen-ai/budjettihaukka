#!/usr/bin/env python3
from __future__ import annotations

import json
import random
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "evals" / "robustness_goldens.json"

random.seed(42)

YEARS = [
    (1998, 2024),
    (2000, 2024),
    (2008, 2024),
    (2010, 2024),
    (2015, 2024),
    (2020, 2024),
    (2022, 2024),
]

TOP_N = [5, 10, 15, 20]

SCENARIOS = [
    {
        "id_prefix": "TG_M",
        "templates": [
            "Mitkä ovat absoluuttisesti eniten kasvaneet budjettimomentit vuosina {y1}-{y2}?",
            "Top {n} eniten kasvaneet momentit {y1}-{y2}",
            "Suurin momenttien kasvu euroissa välillä {y1}-{y2}",
            "Mitkä momentit kasvoivat eniten prosentteina {y1}-{y2}",
            "top {n} momentit kasvun mukaan {y1}-{y2}",
            "mitkä budjettimomentit nousi eniten {y1}-{y2}",
        ],
        "expected": {
            "intent": "top_growth",
            "contract": "top_growth_moment",
            "primary_template": "top_growth",
            "sql_shape": "top_growth_moment",
            "critical": True,
        },
    },
    {
        "id_prefix": "TG_A",
        "templates": [
            "Mitkä alamomentit kasvoivat eniten {y1}-{y2}?",
            "Top {n} eniten kasvaneet alamomentit vuosina {y1}-{y2}",
            "Suurimmat kasvaneet momentit ja alamomentit {y1}-{y2}",
            "mitkä alamomentit nousi eniten euroissa {y1}-{y2}",
            "top {n} alamomentit kasvuprosentin mukaan {y1}-{y2}",
            "mitkä budjetin ala momentit kasvo eniten {y1}-{y2}",
        ],
        "expected": {
            "intent": "top_growth",
            "contract": "top_growth_alamoment",
            "primary_template": "top_growth",
            "sql_shape": "top_growth_alamoment",
            "critical": True,
        },
    },
    {
        "id_prefix": "YOY",
        "templates": [
            "Näytä menojen vuosimuutos {y1}-{y2}",
            "Miten nettokertymä muuttui vuodesta toiseen {y1}-{y2}?",
            "vuosikasvu euroissa {y1}-{y2}",
            "vuosimuutos prosentteina {y1}-{y2}",
            "Miten menot kehittyi vuositasolla {y1}-{y2}",
            "yoy muutos koko budjetille {y1}-{y2}",
        ],
        "expected": {
            "intent": "growth",
            "contract": "yoy_change",
            "primary_template": "trend",
            "sql_shape": "yoy_change",
            "critical": True,
        },
    },
    {
        "id_prefix": "TR_H",
        "templates": [
            "Näytä trendi hallinnonaloittain {y1}-{y2}",
            "Miten eri hallinnonalat kehittyivät {y1}-{y2}?",
            "Aikasarja hallinnonaloittain välillä {y1}-{y2}",
            "hallinnonalojen pitkän aikavälin trendi {y1}-{y2}",
            "trendi hallinnonala tasolla {y1}-{y2}",
            "miten hallinnonalat liikku {y1}-{y2}",
        ],
        "expected": {
            "intent": "trend",
            "contract": "trend_by_hallinnonala",
            "primary_template": "trend",
            "sql_shape": "trend_by_hallinnonala",
            "critical": True,
        },
    },
    {
        "id_prefix": "COMP",
        "templates": [
            "Mikä on menojen jakauma hallinnonaloittain {y1}-{y2}?",
            "Näytä budjetin rakenne hallinnonaloille {y1}-{y2}",
            "osuudet hallinnonaloittain {y1}-{y2}",
            "miten menot jakautuu ministeriöittäin {y1}-{y2}",
            "rakennekuva menojen jakautumisesta {y1}-{y2}",
            "jakauma hallinnonaloittain, pliis {y1}-{y2}",
        ],
        "expected": {
            "intent": "composition",
            "contract": None,
            "primary_template": "composition",
            "sql_shape": "yearly_hallinnonala",
            "critical": False,
        },
    },
    {
        "id_prefix": "SEA",
        "templates": [
            "Onko kuukausissa kausivaihtelua {y1}-{y2}?",
            "Näytä kausivaihtelu kuukausittain {y1}-{y2}",
            "kuukausitrendi hallinnonaloittain {y1}-{y2}",
            "seasonality menodataan {y1}-{y2}",
            "kuukausivaihtelu valtion menoissa {y1}-{y2}",
            "näytä kuukaudet missä heittelee eniten {y1}-{y2}",
        ],
        "expected": {
            "intent": "seasonality",
            "contract": None,
            "primary_template": "seasonality",
            "sql_shape": "monthly_hallinnonala",
            "critical": False,
        },
    },
]

TYPO_MAP = {
    "Mitkä": "Mitk",
    "Näytä": "Nayta",
    "vuosimuutos": "vuosimutos",
    "hallinnonaloittain": "hallinnonalottain",
    "kuukausittain": "kuukasittain",
    "kehittyivät": "kehittyi",
    "prosentteina": "prosenteina",
    "jakautuu": "jakutuu",
}


def typo_variant(text: str) -> str:
    out = text
    for src, dst in TYPO_MAP.items():
        if src in out and random.random() < 0.35:
            out = out.replace(src, dst)
    return out


cases: list[dict] = []
seen_questions: set[str] = set()
seq = 1

for scenario in SCENARIOS:
    for template in scenario["templates"]:
        for y1, y2 in YEARS:
            for n in TOP_N:
                q = template.format(y1=y1, y2=y2, n=n)
                q_typo = typo_variant(q)
                for question in (q, q_typo):
                    question = question.strip()
                    if question in seen_questions:
                        continue
                    seen_questions.add(question)
                    case = {
                        "id": f"R{seq:04d}",
                        "question": question,
                        "expected": dict(scenario["expected"]),
                    }
                    # Keep dataset balanced and realistic.
                    if scenario["id_prefix"] not in {"TG_M", "TG_A"}:
                        case["expected"]["critical"] = random.random() < 0.35
                    cases.append(case)
                    seq += 1
                # Prevent combinatorial explosion per template.
                if len([c for c in cases if c["id"].startswith("R")]) >= 360:
                    break
            if len(cases) >= 360:
                break
        if len(cases) >= 360:
            break
    if len(cases) >= 360:
        break

# Trim to requested robust window (200-500).
cases = cases[:320]

payload = {
    "metadata": {
        "name": "robustness-goldens",
        "version": "2026-03-09",
        "description": "Large NL->intent->SQL shape->visualization golden set with typos and colloquial phrasing.",
        "count": len(cases),
    },
    "cases": cases,
}

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"Wrote {len(cases)} cases to {OUT}")

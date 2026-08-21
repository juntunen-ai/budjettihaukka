#!/usr/bin/env python3
"""Snapshot of company-related budget supports and visible tax lines.

Direct subsidies are curated from expense moments. The only tax expenditure
that appears as its own budget line here is energiaverotuki. Reduced VAT
rates and the corporate tax rate are not recoverable from monthly bookkeeping.
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "reference" / "firm_support_tax_v1.json"

TOPIC_FI = {
    "tki": "TKI-tuet ja -lainat",
    "energia_teollisuus": "Energia- ja teollisuustuet",
    "maatalousyrittajat": "Maatalousyrittäjien tuet",
    "tyovoima_yrityspalvelut": "Työvoima- ja yrityspalvelut",
    "paaomasijoitukset": "Pääomasijoitukset",
    "vienti_rahoitus": "Vienti- ja Finnvera-tuet",
    "yritysten_kehittaminen": "Yritysten kehittäminen",
    "kriisituet": "Kriisi- ja kustannustuet",
}
CORE_TOPICS = [key for key in TOPIC_FI if key != "kriisituet"]
TAX_CODES = {
    "11.01.02.": "yhteisovero",
    "11.08.07.": "energiaverot",
    "11.04.01.": "arvonlisavero",
    "28.91.41.": "energiaverotuki",
    "28.99.43.": "energiaverotuki",
}


def _norm(name: str) -> str:
    return (name or "").lower().replace("\xa0", " ")


def classify_expense(name: str) -> str | None:
    n = _norm(name)
    if any(token in n for token in ("toimintamenot", "arvonlisäveromenot", "palkkaturva", "kotoutum", "ohjaamo")):
        return None
    if any(token in n for token in ("alue- ja rakennepolitiika", "rakennerahasto", "koheesiopolitiikan")):
        return None
    if any(token in n for token in ("kustannustuki", "yksinyrittäj", "ravitsemisyrittäj", "polttoainetuki", "tapahtumatakuu", "turveyrittäj")):
        return "kriisituet"
    if any(token in n for token in ("maataloud", "maaseutuelinkeino")) and any(
        token in n for token in ("investointi", "korkotuki", "aloittamis")
    ):
        return "maatalousyrittajat"
    if any(token in n for token in ("tutkimus-, kehittämis", "innovaatiotoimin", "lainat tutkimus", "vtt oy", "lääkekehitys", "innovaatiotuki")):
        return "tki"
    if any(
        token in n
        for token in (
            "energiatuki",
            "uusiutuvan energian",
            "sähköistämis",
            "kiertotalous",
            "lataus- ja tankkaus",
            "energiatehokku",
            "päästökaupan",
            "sähköhuollon",
        )
    ):
        return "energia_teollisuus"
    if any(token in n for token in ("finnvera", "vienti- ja alus", "vientiluotto")):
        return "vienti_rahoitus"
    if any(token in n for token in ("teollisuussijoitus", "malmijalostus", "venture capital", "pääomasijoitus", "pääomalainat")):
        return "paaomasijoitukset"
    if "työvoima- ja yrityspalvelut" in n:
        return "tyovoima_yrityspalvelut"
    if any(token in n for token in ("yritysten kehittämis", "kuljetustuki", "laivanrakennus")):
        return "yritysten_kehittaminen"
    return None


def fetch_rows(project: str, dataset: str) -> list[dict]:
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    sql = f"""
    SELECT year, fiscal_side, momentti_tunnusp, momentti_snimi,
           SUM(net_accumulation_nominal_eur) AS eur
    FROM `{project}.{dataset}.analytics_fiscal_yearly_core_v1`
    WHERE coverage_status = 'complete'
      AND year BETWEEN 2015 AND 2025
      AND (
        (
          fiscal_side = 'expense'
          AND (
            momentti_tunnusp LIKE '32.%'
            OR momentti_tunnusp LIKE '30.10.%'
            OR LOWER(COALESCE(momentti_snimi, '')) LIKE '%yritys%'
            OR LOWER(COALESCE(momentti_snimi, '')) LIKE '%finnvera%'
            OR LOWER(COALESCE(momentti_snimi, '')) LIKE '%energiatuki%'
            OR LOWER(COALESCE(momentti_snimi, '')) LIKE '%sähköistämis%'
            OR LOWER(COALESCE(momentti_snimi, '')) LIKE '%kustannustuki%'
            OR LOWER(COALESCE(momentti_snimi, '')) LIKE '%päästökaupan%'
          )
        )
        OR momentti_tunnusp IN ('11.01.02.', '11.08.07.', '11.04.01.', '28.91.41.', '28.99.43.')
      )
    GROUP BY 1, 2, 3, 4
    """
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(use_query_cache=True, maximum_bytes_billed=500_000_000),
    )
    return [
        {
            "year": int(row.year),
            "side": row.fiscal_side,
            "code": (row.momentti_tunnusp or "").strip(),
            "name": row.momentti_snimi or "",
            "eur": float(row.eur or 0),
        }
        for row in job.result()
    ]


def build_payload(rows: list[dict]) -> dict:
    years = list(range(2015, 2026))
    topics_year: dict[int, dict[str, float]] = {year: defaultdict(float) for year in years}
    tax_year: dict[int, dict[str, float]] = {year: defaultdict(float) for year in years}
    moments_2025: list[dict] = []
    electric_year = {year: 0.0 for year in years}

    for row in rows:
        year = row["year"]
        if year not in topics_year:
            continue
        tax_id = TAX_CODES.get(row["code"])
        if tax_id:
            tax_year[year][tax_id] += abs(row["eur"])
        if "sähköistämis" in _norm(row["name"]) and row["side"] == "expense":
            electric_year[year] += row["eur"]
        if row["side"] != "expense":
            continue
        topic = classify_expense(row["name"])
        if not topic:
            continue
        topics_year[year][topic] += row["eur"]
        if year == 2025:
            moments_2025.append(
                {
                    "code": row["code"],
                    "name": row["name"],
                    "topic": topic,
                    "topic_fi": TOPIC_FI[topic],
                    "nominal_eur": round(row["eur"], 2),
                }
            )

    moments_2025.sort(key=lambda item: -abs(item["nominal_eur"]))
    topics_2025 = [
        {"id": key, "label_fi": TOPIC_FI[key], "nominal_eur": round(topics_year[2025][key], 2)}
        for key in TOPIC_FI
        if topics_year[2025][key] > 0
    ]
    topics_2025.sort(key=lambda item: -item["nominal_eur"])
    core_2025 = sum(topics_year[2025][key] for key in CORE_TOPICS)

    trend = []
    for year in years:
        core = sum(topics_year[year][key] for key in CORE_TOPICS)
        crisis = topics_year[year]["kriisituet"]
        trend.append(
            {
                "year": year,
                "core_support_eur": round(core, 2),
                "crisis_support_eur": round(crisis, 2),
                "energiaverotuki_eur": round(tax_year[year]["energiaverotuki"], 2),
                "electrification_support_eur": round(electric_year[year], 2),
                "yhteisovero_eur": round(tax_year[year]["yhteisovero"], 2),
                "energiaverot_eur": round(tax_year[year]["energiaverot"], 2),
                "arvonlisavero_eur": round(tax_year[year]["arvonlisavero"], 2),
            }
        )

    return {
        "meta": {
            "title_fi": "Yritystuet ja budjetissa näkyvät veroluokat",
            "year_from": 2015,
            "year_to": 2025,
            "latest_complete_year": 2025,
            "metric": "nettokertymä, toteuma",
            "unit": "EUR",
            "coverage_status": "complete",
            "source_fi": "Valtiokonttori, valtion taloushallinnon kuukausidata",
            "source_url": "https://www.tutkihallintoa.fi/valtiontalous/valtion-taloustiedot/",
            "caveat_fi": (
                "Suorat tuet on poimittu momenttien nimistä. Ainoa tässä erillisenä "
                "näkyvä verotuki on energiaverotuki. Alemmat ALV-kannat ja yhteisöverokanta "
                "eivät ole omia momentejaan."
            ),
        },
        "headline_2025": {
            "core_support_eur": round(core_2025, 2),
            "crisis_support_eur": round(topics_year[2025]["kriisituet"], 2),
            "energiaverotuki_eur": round(tax_year[2025]["energiaverotuki"], 2),
            "electrification_support_eur": round(electric_year[2025], 2),
            "yhteisovero_eur": round(tax_year[2025]["yhteisovero"], 2),
            "energiaverot_eur": round(tax_year[2025]["energiaverot"], 2),
            "arvonlisavero_eur": round(tax_year[2025]["arvonlisavero"], 2),
            "moment_count": len(moments_2025),
        },
        "topics_2025": topics_2025,
        "top_moments_2025": moments_2025[:12],
        "trend": trend,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="budjettihaukka-gpt")
    parser.add_argument("--dataset", default="valtiodata")
    args = parser.parse_args()
    payload = build_payload(fetch_rows(args.project, args.dataset))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {OUT}")
    print("2025 core", round(payload["headline_2025"]["core_support_eur"] / 1e9, 3))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Snapshot of expense moments whose names mention tuki/avustus.

Writes data/reference/subsidy_name_search_v1.json from the visualization mart.
Name search is not an official subsidy definition; the snapshot keeps a
curated list of related moments that the name pattern misses.
"""
from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "reference" / "subsidy_name_search_v1.json"

NAME_RE = re.compile(r"(tuki|tuen|tuet|tukia|avustus)", re.IGNORECASE)
RELATED = {
    "28.90.30.": "Kuntien peruspalvelujen valtionosuus",
    "33.20.52.": "Työttömyysetuuksien perusturva",
    "29.70.55.": "Opintoraha ja asumislisä",
    "33.20.50.": "Työttömyysetuuksien ansioturva",
    "29.70.52.": "Opintolainojen valtiontakaus",
}
TOPIC_LABEL = {
    "kuntien_valtionosuudet": "Kuntien valtionosuudet",
    "asumistuki": "Asumistuki",
    "toimeentulotuki": "Perustoimeentulotuki",
    "maatalous_maaseutu": "Maatalous- ja maaseututuet",
    "energia": "Energiatuet",
    "yritykset_vienti": "Yritys- ja vientituet",
    "kansainvalinen_tuki": "Kansainvälinen tuki",
    "muut_avustukset": "Muut avustukset",
    "muut_tuet": "Muut tuet",
}
DISPLAY_NAMES = {
    "29.10.30.": "VOS esi- ja perusopetus ja varhaiskasvatus",
    "29.10.31.": "VOS vapaa sivistystyö",
    "29.20.30.": "VOS ammatillinen koulutus",
    "29.20.35.": "VOS lukiokoulutus",
    "32.20.43.": "Teollisuuden sähköistämistuki",
}


def _f(value) -> float:
    return 0.0 if value is None else float(value)


def _code(value: str | None) -> str:
    return (value or "").strip()


def _hit(name: str) -> bool:
    return bool(NAME_RE.search(name or ""))


def classify(name: str) -> str:
    n = (name or "").lower()
    if "valtionosuus" in n:
        return "kuntien_valtionosuudet"
    if "asumistuki" in n:
        return "asumistuki"
    if "toimeentulotuki" in n:
        return "toimeentulotuki"
    if any(k in n for k in ("maatalous", "maa- ja puutarha", "eu-tulo", "eu-markkina", "maaseutu", "viljelijä")):
        return "maatalous_maaseutu"
    if any(k in n for k in ("energia", "sähköistämistuki", "uusiutuvan energian")):
        return "energia"
    if any(k in n for k in ("yritys", "vienti", "kuljetustuki")):
        return "yritykset_vienti"
    if any(k in n for k in ("ukrain", "vastaanotto", "demokratia")):
        return "kansainvalinen_tuki"
    if "avustus" in n:
        return "muut_avustukset"
    return "muut_tuet"


def fetch_rows(project: str, dataset: str) -> list[dict]:
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    sql = f"""
    SELECT
      year, hallinnonala, momentti_tunnusp, momentti_snimi,
      SUM(net_accumulation_nominal_eur) AS nominal_eur,
      SUM(net_accumulation_real_cpi_eur) AS real_cpi_eur
    FROM `{project}.{dataset}.analytics_fiscal_yearly_core_v1`
    WHERE fiscal_side = 'expense'
      AND year BETWEEN 1998 AND 2025
      AND coverage_status = 'complete'
    GROUP BY 1,2,3,4
    """
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(use_query_cache=True, maximum_bytes_billed=500_000_000),
    )
    return [dict(row) for row in job.result()]


def build_payload(rows: list[dict]) -> dict:
    years = list(range(1998, 2026))
    yearly = {year: defaultdict(float) for year in years}
    topics_2025: dict[str, float] = defaultdict(float)
    moments_2025: dict[str, dict] = {}
    related_2025: dict[str, dict] = {}
    series = {
        "asumistuki": {year: 0.0 for year in years},
        "toimeentulotuki": {year: 0.0 for year in years},
        "opintoraha": {year: 0.0 for year in years},
        "maatalous": {year: 0.0 for year in years},
    }

    for row in rows:
        year = int(row["year"])
        if year not in yearly:
            continue
        nom = _f(row["nominal_eur"])
        real = _f(row["real_cpi_eur"])
        name = row["momentti_snimi"] or ""
        code = _code(row["momentti_tunnusp"])
        yearly[year]["expense"] += nom
        if _hit(name):
            yearly[year]["name_nominal"] += nom
            yearly[year]["name_real"] += real
            if "valtionosuus" in name.lower():
                yearly[year]["vos_in_name"] += nom
            topic = classify(name)
            if year == 2025:
                topics_2025[topic] += nom
                item = moments_2025.setdefault(
                    code or name,
                    {
                        "code": code,
                        "name": name,
                        "name_short": DISPLAY_NAMES.get(code, name[:64]),
                        "hallinnonala": row["hallinnonala"],
                        "topic": topic,
                        "topic_fi": TOPIC_LABEL[topic],
                        "nominal_eur": 0.0,
                    },
                )
                item["nominal_eur"] += nom
            if topic == "maatalous_maaseutu":
                series["maatalous"][year] += nom
        if code in RELATED and not _hit(name):
            yearly[year]["related_missed"] += nom
            if year == 2025:
                item = related_2025.setdefault(
                    code,
                    {
                        "code": code,
                        "name": name,
                        "label_fi": RELATED[code],
                        "nominal_eur": 0.0,
                    },
                )
                item["nominal_eur"] += nom
        if code == "33.10.54.":
            series["asumistuki"][year] += nom
        if code == "33.10.57.":
            series["toimeentulotuki"][year] += nom
        if code == "29.70.55.":
            series["opintoraha"][year] += nom

    trend = []
    for year in years:
        data = yearly[year]
        expense = data["expense"]
        trend.append(
            {
                "year": year,
                "name_match_nominal_eur": round(data["name_nominal"], 2),
                "name_match_real_cpi_eur": round(data["name_real"], 2),
                "expense_eur": round(expense, 2),
                "name_match_share": round(data["name_nominal"] / expense, 6) if expense else None,
                "valtionosuus_in_name_eur": round(data["vos_in_name"], 2),
                "related_missed_eur": round(data["related_missed"], 2),
            }
        )

    moments = sorted(
        ({**item, "nominal_eur": round(item["nominal_eur"], 2)} for item in moments_2025.values()),
        key=lambda item: -abs(item["nominal_eur"]),
    )
    related = sorted(
        ({**item, "nominal_eur": round(item["nominal_eur"], 2)} for item in related_2025.values()),
        key=lambda item: -item["nominal_eur"],
    )
    headline_match = yearly[2025]["name_nominal"]
    headline_expense = yearly[2025]["expense"]
    return {
        "meta": {
            "title_fi": "Valtion tuet ja avustukset nimihakuna",
            "year_from": 1998,
            "year_to": 2025,
            "latest_complete_year": 2025,
            "metric": "nettokertymä, toteuma",
            "unit": "EUR",
            "price_basis_nominal": "nominal",
            "price_basis_real": "constant_cpi",
            "real_base_year": 2025,
            "coverage_status": "complete",
            "name_pattern": NAME_RE.pattern,
            "source_id": "valtiokonttori_monthly_central_government_finance",
            "source_fi": "Valtiokonttori, valtion taloushallinnon kuukausidata, vuositason nettokertymä",
            "source_url": "https://www.tutkihallintoa.fi/valtiontalous/valtion-taloustiedot/",
            "extracted_from": "analytics_fiscal_yearly_core_v1",
            "caveat_fi": (
                "Nimihaku ei ole virallinen tukien määritelmä. Verotuet, EU:n suoraan "
                "maksamat erät ja ilman tuki/avustus-sanaa nimetyt valtionosuudet jäävät haun ulkopuolelle."
            ),
        },
        "headline_2025": {
            "name_match_eur": round(headline_match, 2),
            "related_missed_eur": round(sum(item["nominal_eur"] for item in related), 2),
            "expense_eur": round(headline_expense, 2),
            "name_match_share": round(headline_match / headline_expense, 6),
            "moment_count": len(moments),
        },
        "trend": trend,
        "topics_2025": [
            {"id": key, "label_fi": TOPIC_LABEL[key], "nominal_eur": round(value, 2)}
            for key, value in sorted(topics_2025.items(), key=lambda item: -item[1])
        ],
        "top_moments_2025": moments[:10],
        "related_missed_2025": related,
        "topic_series": {
            key: [{"year": year, "nominal_eur": round(values[year], 2)} for year in years]
            for key, values in series.items()
        },
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


if __name__ == "__main__":
    main()

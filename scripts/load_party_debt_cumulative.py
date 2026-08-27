#!/usr/bin/env python3
"""Kokoaa valtion nettovelanoton kumulatiivisesti pääministeripuolueittain ja
kirjoittaa snapshotin tiedostoon data/reference/party_debt_cumulative_v1.json.

Velan muutos lasketaan velanhallintamomenteista:

- `15.03.01. Nettolainanotto ja velanhallinta` (2003-), rahaa sisään
- `37.01.94. Nettokuoletukset ja velanhallinta` (2001-2008), rahaa ulos

Lähteessä sisään tuleva raha on negatiivinen, joten velan kasvu on
nettokertymän vastaluku. Positiivinen luku kasvattaa velkaa, negatiivinen
lyhentää sitä.

Kuukausi kohdennetaan sille hallitukselle, joka piti valtaa suurimman osan
kuukauden päivistä, ja hallitus edelleen pääministerinsä puolueelle.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings

OUT = ROOT / "data" / "reference" / "party_debt_cumulative_v1.json"
MAX_BYTES = 4_000_000_000

# Velanhallintamomentit tunnistetaan nimestä, samoin kuin repon jaettu
# fiscal_side-sääntö tekee rahoituserille.
DEBT_NAME_PATTERN = "%velanhallinta%"


def monthly_sql(project: str, dataset: str, semantic_view: str) -> str:
    return f"""
WITH velka AS (
  SELECT
    period_date AS month_start,
    -SUM(nettokertyma) AS debt_change_eur
  FROM `{project}.{dataset}.{semantic_view}`
  WHERE has_valid_nettokertyma
    AND nettokertyma IS NOT NULL
    AND LOWER(momentti_snimi) LIKE '{DEBT_NAME_PATTERN}'
  GROUP BY month_start
),
overlap AS (
  SELECT
    v.month_start,
    g.cabinet_ordinal,
    g.cabinet_name,
    g.pm_party_fi,
    g.start_date,
    g.end_date,
    DATE_DIFF(
      LEAST(COALESCE(g.end_date, CURRENT_DATE()), DATE_ADD(v.month_start, INTERVAL 1 MONTH)),
      GREATEST(g.start_date, v.month_start),
      DAY
    ) AS overlap_days
  FROM velka v
  CROSS JOIN `{project}.{dataset}.government_cabinet_v1` g
),
attribution AS (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT overlap.*,
           ROW_NUMBER() OVER (PARTITION BY month_start ORDER BY overlap_days DESC, cabinet_ordinal) AS rn
    FROM overlap
    WHERE overlap_days > 0
  )
  WHERE rn = 1
)
SELECT
  v.month_start,
  a.cabinet_ordinal,
  a.cabinet_name,
  a.pm_party_fi,
  a.start_date,
  a.end_date,
  v.debt_change_eur
FROM velka v
JOIN attribution a USING (month_start)
ORDER BY v.month_start
""".strip()


def moments_sql(project: str, dataset: str, semantic_view: str) -> str:
    return f"""
SELECT
  momentti_tunnusp,
  momentti_snimi,
  MIN(EXTRACT(YEAR FROM period_date)) AS first_year,
  MAX(EXTRACT(YEAR FROM period_date)) AS last_year,
  -SUM(nettokertyma) AS debt_change_eur
FROM `{project}.{dataset}.{semantic_view}`
WHERE has_valid_nettokertyma
  AND nettokertyma IS NOT NULL
  AND LOWER(momentti_snimi) LIKE '{DEBT_NAME_PATTERN}'
GROUP BY 1, 2
ORDER BY 1
""".strip()


def _run(client: Any, sql: str) -> list[dict[str, Any]]:
    from google.cloud import bigquery

    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(use_query_cache=True, maximum_bytes_billed=MAX_BYTES),
    )
    return [dict(row) for row in job.result()]


def build_payload(monthly: list[dict], moments: list[dict]) -> dict[str, Any]:
    if not monthly:
        raise ValueError("Velkakuukausia ei saatu BigQuerystä")

    series = []
    running = 0.0
    for row in monthly:
        running += float(row["debt_change_eur"])
        series.append(
            {
                "month": row["month_start"].isoformat(),
                "cabinet_ordinal": row["cabinet_ordinal"],
                "cabinet_name": row["cabinet_name"],
                "pm_party_fi": row["pm_party_fi"],
                "debt_change_eur": float(row["debt_change_eur"]),
                "cumulative_eur": running,
            }
        )

    parties: dict[str, dict[str, Any]] = {}
    for row in series:
        party = parties.setdefault(
            row["pm_party_fi"],
            {
                "pm_party_fi": row["pm_party_fi"],
                "months": 0,
                "debt_change_eur": 0.0,
                "first_month": row["month"],
                "last_month": row["month"],
                "cabinets": [],
                "repayment_months": 0,
            },
        )
        party["months"] += 1
        party["debt_change_eur"] += row["debt_change_eur"]
        party["last_month"] = row["month"]
        if row["debt_change_eur"] < 0:
            party["repayment_months"] += 1
        if row["cabinet_name"] not in party["cabinets"]:
            party["cabinets"].append(row["cabinet_name"])

    party_rows = sorted(parties.values(), key=lambda row: row["debt_change_eur"], reverse=True)
    for row in party_rows:
        row["debt_change_per_month_eur"] = row["debt_change_eur"] / row["months"]

    first = series[0]["month"]
    last = series[-1]["month"]
    span = (int(last[:4]) - int(first[:4])) * 12 + (int(last[5:7]) - int(first[5:7])) + 1

    return {
        "meta": {
            "dataset_id": "party_debt_cumulative_v1",
            "measure": "valtion nettovelanotto, velanhallintamomenttien nettokertymän vastaluku",
            "sign_rule": "positiivinen kasvattaa velkaa, negatiivinen lyhentää",
            "attribution_rule": "kuukausi kuuluu hallitukselle, joka piti valtaa suurimman osan "
                                "kuukauden päivistä; hallitus edelleen pääministerinsä puolueelle",
            "first_month": first,
            "last_month": last,
            "observed_months": len(series),
            "span_months": span,
            "missing_months_in_span": span - len(series),
            "total_debt_change_eur": series[-1]["cumulative_eur"],
            "moments": [
                {
                    "momentti_tunnusp": row["momentti_tunnusp"],
                    "momentti_snimi": row["momentti_snimi"],
                    "first_year": row["first_year"],
                    "last_year": row["last_year"],
                    "debt_change_eur": float(row["debt_change_eur"]),
                }
                for row in moments
            ],
            "sources": [
                {
                    "source_id": "valtiokonttori_kuukausidata",
                    "label": "Valtiokonttori, valtion taloushallinnon kuukausidata",
                },
                {
                    "source_id": "valtioneuvosto_hallitukset",
                    "label": "Valtioneuvosto, hallitukset ja ministerit",
                },
            ],
        },
        "parties": party_rows,
        "monthly": series,
    }


def validate(payload: dict[str, Any]) -> None:
    meta = payload["meta"]
    series = payload["monthly"]
    parties = payload["parties"]

    if len(series) != meta["observed_months"]:
        raise ValueError("Kuukausirivien määrä ei vastaa metatietoa")

    months = [row["month"] for row in series]
    if months != sorted(months):
        raise ValueError("Kuukaudet eivät ole aikajärjestyksessä")
    if len(set(months)) != len(months):
        raise ValueError("Sama kuukausi esiintyy useammin kuin kerran")

    # Kumulatiivinen sarja on juokseva summa, ja loppuarvo on kokonaismuutos.
    running = 0.0
    for row in series:
        running += row["debt_change_eur"]
        if abs(running - row["cumulative_eur"]) > 1.0:
            raise ValueError(f"Kumulatiivinen summa ei täsmää kohdassa {row['month']}")

    party_total = sum(row["debt_change_eur"] for row in parties)
    if abs(party_total - meta["total_debt_change_eur"]) > 1.0:
        raise ValueError("Puolueiden summa ei vastaa kokonaismuutosta")
    if sum(row["months"] for row in parties) != len(series):
        raise ValueError("Puolueiden kuukaudet eivät kata sarjaa täsmälleen")

    if not meta["moments"]:
        raise ValueError("Velanhallintamomentteja ei löytynyt")


def main() -> None:
    parser = argparse.ArgumentParser(description="Kokoa nettovelanotto puolueittain.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--semantic-view", default=settings.table)
    args = parser.parse_args()

    from google.cloud import bigquery

    client = bigquery.Client(project=args.project)
    monthly = _run(client, monthly_sql(args.project, args.dataset, args.semantic_view))
    moments = _run(client, moments_sql(args.project, args.dataset, args.semantic_view))

    payload = build_payload(monthly, moments)
    validate(payload)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    meta = payload["meta"]
    print(f"{OUT.relative_to(ROOT)}: {len(payload['parties'])} puoluetta, "
          f"{meta['observed_months']}/{meta['span_months']} kuukautta "
          f"({meta['first_month']}..{meta['last_month']}), "
          f"velkamuutos {meta['total_debt_change_eur'] / 1e9:.1f} mrd €")


if __name__ == "__main__":
    main()

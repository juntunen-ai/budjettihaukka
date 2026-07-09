#!/usr/bin/env python3
"""Lataa Tilastokeskuksen elinkustannusindeksin (StatFin khi/11xm) ja
rakentaa deflaattorin BigQueryyn.

Tuottaa:
- data/reference/elinkustannusindeksi_11xm.csv (versioitu gitiin)
- BQ-taulu price_index_v1 (vuosi, index_value, deflator_to_latest)
- BQ-näkymä concept_yearly_totals_real_v1 — konseptisummat sekä
  nimellisinä että viimeisimmän täyden indeksivuoden hinnoin.

Hiekkalaatikkoyhteensopiva: load job + CREATE OR REPLACE VIEW.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import requests
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings

PXWEB_URL = "https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/khi/11xm.px"
CSV_PATH = ROOT / "data" / "reference" / "elinkustannusindeksi_11xm.csv"


def fetch_index(year_from: int, year_to: int) -> dict[int, float]:
    payload = {
        "query": [
            {
                "code": "timeperiod_y",
                "selection": {
                    "filter": "item",
                    "values": [str(y) for y in range(year_from, year_to + 1)],
                },
            }
        ],
        "response": {"format": "json"},
    }
    resp = requests.post(PXWEB_URL, json=payload, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    series: dict[int, float] = {}
    for row in data["data"]:
        year = int(row["key"][0])
        value = row["values"][0]
        if value not in (".", ".."):
            series[year] = float(value)
    return series


def main() -> int:
    parser = argparse.ArgumentParser(description="Lataa elinkustannusindeksi ja rakenna deflaattori.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--year-from", type=int, default=1998)
    parser.add_argument("--year-to", type=int, default=2025)
    args = parser.parse_args()

    series = fetch_index(args.year_from, args.year_to)
    if len(series) < (args.year_to - args.year_from):
        raise SystemExit(f"Indeksisarja vajaa: {len(series)} vuotta")
    base_year = max(series)
    base_value = series[base_year]
    print(f"Elinkustannusindeksi {min(series)}-{base_year}, perusvuosi {base_year} ({base_value})")

    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CSV_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["vuosi", "index_value", "deflator_to_latest", "base_year", "source"])
        for year in sorted(series):
            writer.writerow(
                [year, series[year], round(base_value / series[year], 6), base_year, "StatFin khi/11xm"]
            )
    print(f"CSV -> {CSV_PATH.relative_to(ROOT)}")

    client = bigquery.Client(project=args.project)
    table_id = f"{args.project}.{args.dataset}.price_index_v1"
    rows = [
        {
            "vuosi": year,
            "index_value": series[year],
            "deflator_to_latest": base_value / series[year],
            "base_year": base_year,
        }
        for year in sorted(series)
    ]
    client.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("vuosi", "INT64"),
                bigquery.SchemaField("index_value", "FLOAT64"),
                bigquery.SchemaField("deflator_to_latest", "FLOAT64"),
                bigquery.SchemaField("base_year", "INT64"),
            ],
        ),
    ).result()
    print(f"BQ-taulu -> {table_id}")

    view_sql = f"""
CREATE OR REPLACE VIEW `{args.project}.{args.dataset}.concept_yearly_totals_real_v1` AS
SELECT
  t.concept,
  t.vuosi,
  t.role,
  t.component,
  t.target_concept,
  t.total_meur AS total_meur_nominal,
  ROUND(t.total_meur * p.deflator_to_latest, 1) AS total_meur_real,
  p.base_year AS real_base_year
FROM `{args.project}.{args.dataset}.concept_yearly_totals_v1` t
LEFT JOIN `{args.project}.{args.dataset}.price_index_v1` p USING (vuosi)
"""
    client.query(view_sql).result()
    print(f"Näkymä -> {args.project}.{args.dataset}.concept_yearly_totals_real_v1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

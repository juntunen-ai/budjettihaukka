#!/usr/bin/env python3
"""Lataa Suomen hallitukset ja niiden vallassaoloajat valtioneuvoston
sivulta tauluksi government_cabinet_v1 ja rakentaa vuosinäkymän
dim_government_by_year_v1.

Lähde listaa hallitukset itsenäisyydestä alkaen. Ketju on jatkuva: jokaisen
hallituksen päättymispäivä on seuraavan aloituspäivä. Lähteen sarake
"hallituspäivät" laskee molemmat päätepäivät mukaan, joten se on
johdonmukaisesti yhden suurempi kuin end - start.

Vuosikohtaisessa näkymässä käytetään puoliavointa väliä [start, end),
jotta vallanvaihtopäivää ei lasketa kahdesti. Vuoden osuudet summautuvat
siten tasan yhteen jokaisena täytenä vuotena.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import html as htmlmod
import re
import sys
import unicodedata
from pathlib import Path
from typing import Any

import requests
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings

SOURCE_ID = "valtioneuvosto_hallitukset"
SOURCE_URL = "https://valtioneuvosto.fi/hallitukset-ja-ministerit/hallitukset/"
REFERENCE_DIR = ROOT / "data" / "reference"
CSV_PATH = REFERENCE_DIR / "government_cabinet_v1.csv"
TABLE_NAME = "government_cabinet_v1"
YEAR_VIEW_NAME = "dim_government_by_year_v1"

FIELDS = [
    "cabinet_ordinal",
    "cabinet_id",
    "cabinet_name",
    "prime_minister_surname",
    "start_date",
    "end_date",
    "is_incumbent",
    "days_in_office_source",
    "days_in_office_computed",
    "pm_party_fi",
    "cabinet_type_fi",
    "source_id",
    "source_url",
    "vintage_date",
]

SCHEMA = [
    bigquery.SchemaField("cabinet_ordinal", "INT64"),
    bigquery.SchemaField("cabinet_id", "STRING"),
    bigquery.SchemaField("cabinet_name", "STRING"),
    bigquery.SchemaField("prime_minister_surname", "STRING"),
    bigquery.SchemaField("start_date", "DATE"),
    bigquery.SchemaField("end_date", "DATE"),
    bigquery.SchemaField("is_incumbent", "BOOL"),
    bigquery.SchemaField("days_in_office_source", "INT64"),
    bigquery.SchemaField("days_in_office_computed", "INT64"),
    bigquery.SchemaField("pm_party_fi", "STRING"),
    bigquery.SchemaField("cabinet_type_fi", "STRING"),
    bigquery.SchemaField("source_id", "STRING"),
    bigquery.SchemaField("source_url", "STRING"),
    bigquery.SchemaField("vintage_date", "DATE"),
]


def _slug(name: str) -> str:
    folded = unicodedata.normalize("NFKD", name)
    folded = "".join(ch for ch in folded if not unicodedata.combining(ch))
    folded = folded.lower().replace(".", " ")
    return "_".join(part for part in re.split(r"[^a-z0-9]+", folded) if part)


def _text(fragment: str) -> str:
    stripped = re.sub(r"<[^>]+>", " ", fragment)
    return re.sub(r"\s+", " ", htmlmod.unescape(stripped)).replace("\xa0", " ").strip()


def _parse_date(value: str) -> dt.date | None:
    value = value.strip()
    if not value:
        return None
    day, month, year = (int(part) for part in value.split("."))
    return dt.date(year, month, day)


def fetch_html(timeout: int = 60) -> str:
    response = requests.get(
        SOURCE_URL,
        timeout=timeout,
        headers={"User-Agent": "budjettihaukka-reference-loader"},
    )
    response.raise_for_status()
    return response.text


def parse_cabinets(page: str, *, vintage: dt.date) -> list[dict[str, Any]]:
    rows: list[list[str]] = []
    for block in re.findall(r"<tr[^>]*>.*?</tr>", page, re.S):
        cells = [_text(cell) for cell in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", block, re.S)]
        if len(cells) >= 5 and not cells[0].startswith("Hallitus"):
            rows.append(cells)
    if not rows:
        raise ValueError("Hallitustaulukkoa ei löytynyt lähdesivulta")

    parsed: list[dict[str, Any]] = []
    for name, period, days, party, cabinet_type in rows:
        bounds = [part.strip() for part in period.split("-")]
        start = _parse_date(bounds[0])
        end = _parse_date(bounds[1]) if len(bounds) > 1 else None
        if start is None:
            raise ValueError(f"Hallitukselta {name} puuttuu alkupäivä")
        parsed.append(
            {
                "cabinet_name": name,
                "prime_minister_surname": re.split(r"\s+(?=[IVX]+$)", name)[0].strip(),
                "start_date": start,
                "end_date": end,
                "is_incumbent": end is None,
                "days_in_office_source": int(days),
                "days_in_office_computed": (end - start).days + 1 if end else None,
                "pm_party_fi": party,
                "cabinet_type_fi": cabinet_type,
                "source_id": SOURCE_ID,
                "source_url": SOURCE_URL,
                "vintage_date": vintage,
            }
        )

    parsed.sort(key=lambda row: row["start_date"])
    for ordinal, row in enumerate(parsed, start=1):
        row["cabinet_ordinal"] = ordinal
        row["cabinet_id"] = f"{ordinal:03d}_{_slug(row['cabinet_name'])}"
    return parsed


def validate(cabinets: list[dict[str, Any]]) -> None:
    if not cabinets:
        raise ValueError("Hallituksia ei jäsennetty yhtään")

    ids = [row["cabinet_id"] for row in cabinets]
    if len(set(ids)) != len(ids):
        raise ValueError("cabinet_id ei ole yksilöllinen")

    incumbents = [row for row in cabinets if row["is_incumbent"]]
    if len(incumbents) != 1:
        raise ValueError(f"Istuvia hallituksia pitäisi olla tasan 1, löytyi {len(incumbents)}")
    if incumbents[0] is not cabinets[-1]:
        raise ValueError("Istuva hallitus ei ole aikajärjestyksessä viimeinen")

    for earlier, later in zip(cabinets, cabinets[1:]):
        if earlier["end_date"] is None:
            raise ValueError(f"Päättyneeltä hallitukselta {earlier['cabinet_name']} puuttuu loppupäivä")
        if earlier["end_date"] != later["start_date"]:
            raise ValueError(
                "Hallitusketju katkeaa: "
                f"{earlier['cabinet_name']} päättyy {earlier['end_date']}, "
                f"{later['cabinet_name']} alkaa {later['start_date']}"
            )

    for row in cabinets:
        computed = row["days_in_office_computed"]
        if computed is not None and computed != row["days_in_office_source"]:
            raise ValueError(
                f"Hallituspäivät eivät täsmää: {row['cabinet_name']} "
                f"lähde={row['days_in_office_source']} laskettu={computed}"
            )


def write_csv(cabinets: list[dict[str, Any]]) -> None:
    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    with CSV_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        for row in cabinets:
            writer.writerow({field: "" if row.get(field) is None else row[field] for field in FIELDS})


def year_view_sql(project: str, dataset: str) -> str:
    return f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.{YEAR_VIEW_NAME}` AS
-- Vallassaolovuodet puoliavoimella välillä [start_date, end_date), jotta
-- vallanvaihtopäivä ei tule lasketuksi kahdelle hallitukselle. Istuvan
-- hallituksen kohdalla väli päättyy vintage_date-päivään.
WITH bounds AS (
  SELECT
    cabinet_ordinal,
    cabinet_id,
    cabinet_name,
    pm_party_fi,
    cabinet_type_fi,
    is_incumbent,
    start_date,
    COALESCE(end_date, vintage_date) AS effective_end_date
  FROM `{project}.{dataset}.{TABLE_NAME}`
), spans AS (
  SELECT
    bounds.*,
    year_value AS year
  FROM bounds,
  UNNEST(GENERATE_ARRAY(
    EXTRACT(YEAR FROM start_date),
    EXTRACT(YEAR FROM effective_end_date)
  )) AS year_value
)
SELECT
  year,
  cabinet_ordinal,
  cabinet_id,
  cabinet_name,
  pm_party_fi,
  cabinet_type_fi,
  is_incumbent,
  GREATEST(start_date, DATE(year, 1, 1)) AS year_start_date,
  LEAST(effective_end_date, DATE(year, 12, 31)) AS year_end_date,
  DATE_DIFF(
    LEAST(effective_end_date, DATE(year + 1, 1, 1)),
    GREATEST(start_date, DATE(year, 1, 1)),
    DAY
  ) AS days_in_year,
  SAFE_DIVIDE(
    DATE_DIFF(
      LEAST(effective_end_date, DATE(year + 1, 1, 1)),
      GREATEST(start_date, DATE(year, 1, 1)),
      DAY
    ),
    DATE_DIFF(DATE(year + 1, 1, 1), DATE(year, 1, 1), DAY)
  ) AS share_of_year
FROM spans
WHERE DATE_DIFF(
        LEAST(effective_end_date, DATE(year + 1, 1, 1)),
        GREATEST(start_date, DATE(year, 1, 1)),
        DAY
      ) > 0
ORDER BY year, cabinet_ordinal
""".strip()


def load_bigquery(cabinets: list[dict[str, Any]], *, project: str, dataset: str) -> None:
    client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{TABLE_NAME}"
    payload = [
        {
            **row,
            "start_date": row["start_date"].isoformat(),
            "end_date": row["end_date"].isoformat() if row["end_date"] else None,
            "vintage_date": row["vintage_date"].isoformat(),
        }
        for row in cabinets
    ]
    client.load_table_from_json(
        payload,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=SCHEMA),
    ).result()
    print(f"BQ-taulu -> {table_id} ({len(payload)} hallitusta)")

    client.query(year_view_sql(project, dataset)).result()
    print(f"BQ-näkymä -> {project}.{dataset}.{YEAR_VIEW_NAME}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Lataa Suomen hallitukset BigQueryyn.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--load-bigquery", action="store_true")
    parser.add_argument("--render-sql", type=Path)
    args = parser.parse_args()

    vintage = dt.datetime.now(dt.UTC).date()
    cabinets = parse_cabinets(fetch_html(), vintage=vintage)
    validate(cabinets)
    write_csv(cabinets)
    print(f"{CSV_PATH.relative_to(ROOT)}: {len(cabinets)} hallitusta "
          f"({cabinets[0]['start_date']} alkaen)")

    if args.render_sql:
        args.render_sql.parent.mkdir(parents=True, exist_ok=True)
        args.render_sql.write_text(year_view_sql(args.project, args.dataset) + "\n", encoding="utf-8")
        print(f"SQL -> {args.render_sql}")

    if args.load_bigquery:
        load_bigquery(cabinets, project=args.project, dataset=args.dataset)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

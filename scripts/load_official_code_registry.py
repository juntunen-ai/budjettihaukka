#!/usr/bin/env python3
"""Lataa Valtiokonttorin viralliset budjettikoodilistat BigQueryyn.

Lähde: api.tutkihallintoa.fi /valtiontalous/v1/{paaluokat,luvut,momentit}
(CSV, kate 2021→). Nämä ovat VIRALLINEN koodirekisteri voimassaolo-
vuosineen — käyttö:
- kanonisten nimien referenssi (vertailu omaan dim-kerrokseen)
- konseptikarttojen year_from/to-rajojen validointi 2021→ osalta
- semanttisen kerroksen nimistön laatuvahti

Tuottaa BQ-taulun official_code_registry_v1 (level, code, code_dotted,
name_fi, year) ja laaturaportin omaa dim-kerrosta vasten.
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
from pathlib import Path

import requests
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings

API = "https://api.tutkihallintoa.fi/valtiontalous/v1"

SOURCES = {
    "paaluokka": ("paaluokat", "Tunnus", "Pääluokka", "Vuosi"),
    "luku": ("luvut", "Tunnus", "Luku", "Vuosi"),
    "momentti": ("momentit", "Momentti", "Tilinimi_suomenk", "Voimassaolovuosi"),
}


def _dotted(level: str, code: str) -> str:
    digits = code.strip()
    if level == "paaluokka":
        return f"{digits}."
    if level == "luku":
        return f"{digits[:2]}.{digits[2:]}."
    return f"{digits[:2]}.{digits[2:4]}.{digits[4:]}."


def fetch_rows() -> list[dict]:
    rows: list[dict] = []
    for level, (endpoint, code_col, name_col, year_col) in SOURCES.items():
        resp = requests.get(f"{API}/{endpoint}", timeout=60)
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.content.decode("utf-8-sig")))
        count = 0
        for record in reader:
            code = (record.get(code_col) or "").strip()
            name = (record.get(name_col) or "").strip()
            year = (record.get(year_col) or "").strip()
            if not code or not year.isdigit():
                continue
            rows.append(
                {
                    "level": level,
                    "code": code,
                    "code_dotted": _dotted(level, code),
                    "name_fi": name,
                    "year": int(year),
                }
            )
            count += 1
        print(f"{level}: {count} riviä ({endpoint})")
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Lataa virallinen koodirekisteri BigQueryyn.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    args = parser.parse_args()

    rows = fetch_rows()
    client = bigquery.Client(project=args.project)
    table_id = f"{args.project}.{args.dataset}.official_code_registry_v1"
    client.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("level", "STRING"),
                bigquery.SchemaField("code", "STRING"),
                bigquery.SchemaField("code_dotted", "STRING"),
                bigquery.SchemaField("name_fi", "STRING"),
                bigquery.SchemaField("year", "INT64"),
            ],
        ),
    ).result()
    print(f"BQ-taulu -> {table_id} ({len(rows)} riviä)")

    # Laaturaportti: montako oman datan (2021->) momenttia EI löydy
    # virallisesta rekisteristä ja päinvastoin.
    sql = f"""
    WITH oma AS (
      SELECT DISTINCT momentti_tunnusp AS code_dotted, vuosi
      FROM `{args.project}.{args.dataset}.valtiontalous_curated_dq_v`
      WHERE vuosi BETWEEN 2021 AND 2026 AND momentti_tunnusp IS NOT NULL
        AND REGEXP_CONTAINS(momentti_tunnusp, r'^[0-9]')
    ),
    virallinen AS (
      SELECT code_dotted, year AS vuosi FROM `{table_id}` WHERE level = 'momentti'
    )
    SELECT
      (SELECT COUNT(*) FROM oma) AS oma_koodivuosia,
      (SELECT COUNT(*) FROM oma o LEFT JOIN virallinen v USING (code_dotted, vuosi) WHERE v.code_dotted IS NULL) AS omassa_ei_rekisterissa,
      (SELECT COUNT(*) FROM virallinen v LEFT JOIN oma o USING (code_dotted, vuosi) WHERE o.code_dotted IS NULL) AS rekisterissa_ei_omassa
    """
    result = list(client.query(sql).result())[0]
    print(
        f"Laatuvertailu 2021-2026: omia koodivuosia {result.oma_koodivuosia}, "
        f"omassa muttei rekisterissä {result.omassa_ei_rekisterissa}, "
        f"rekisterissä muttei omassa (ei toteumaa) {result.rekisterissa_ei_omassa}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

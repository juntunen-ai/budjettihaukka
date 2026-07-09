#!/usr/bin/env python3
"""Lataa Tilastokeskuksen COFOG-aggregaatit (StatFin jmete/12a6) BigQueryyn
ja ristivalidoi konseptikartat niitä vasten.

COFOG on ainoa virallinen tehtäväluokitus — sitä ei julkaista
momenttitasolla, joten se toimii karttojemme suuruusluokka- ja
suunta-ankkurina, ei totuutena riviltä riville (kansantalouden tilinpito
konsolidoi mm. yliopistorahoituksen eri tavalla ja on suoriteperusteinen).

Tuottaa:
- BQ-taulu cofog_reference_v1 (sektori, tehtava, vuosi, meur)
- vertailuraportti konsepti vs. COFOG (suunta + suuruusluokka)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import requests
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings

PXWEB_URL = "https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/jmete/12a6.px"

COFOG_DIVISIONS = ["SSS"] + [f"G{i:02d}" for i in range(1, 11)]

# Konsepti -> COFOG-divisioona vertailua varten. commentti kuvaa eron.
CONCEPT_COFOG_PAIRS = {
    "koulutus": ("G09", "COFOG konsolidoi yliopistot eri tavalla; opintotuki osin G10:ssä"),
    "kulttuuri": ("G08", "G08 sisältää myös virkistyksen, urheilun ja uskonnon — laajempi kuin kulttuuri-käsitteemme"),
}


def fetch_cofog(year_from: int, year_to: int) -> list[dict]:
    payload = {
        "query": [
            {"code": "sektoriluokitus_7_20230101", "selection": {"filter": "item", "values": ["S1311"]}},
            {"code": "taloustoimi_1_20180101", "selection": {"filter": "item", "values": ["OTES"]}},
            {"code": "julkisyht_teht_3_20010101", "selection": {"filter": "item", "values": COFOG_DIVISIONS}},
            {
                "code": "timeperiod_y",
                "selection": {"filter": "item", "values": [str(y) for y in range(year_from, year_to + 1)]},
            },
        ],
        "response": {"format": "json"},
    }
    resp = requests.post(PXWEB_URL, json=payload, timeout=90)
    resp.raise_for_status()
    rows = []
    for row in resp.json()["data"]:
        sector, transaction, function, year = row["key"]
        value = row["values"][0]
        if value in (".", ".."):
            continue
        rows.append(
            {
                "sektori": sector,
                "taloustoimi": transaction,
                "tehtava": function,
                "vuosi": int(year),
                "meur": float(value),
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Lataa COFOG-referenssi ja ristivalidoi konseptit.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--year-from", type=int, default=2001)
    parser.add_argument("--year-to", type=int, default=2024)
    args = parser.parse_args()

    rows = fetch_cofog(args.year_from, args.year_to)
    print(f"COFOG-rivejä: {len(rows)} (S1311, {args.year_from}-{args.year_to})")

    client = bigquery.Client(project=args.project)
    table_id = f"{args.project}.{args.dataset}.cofog_reference_v1"
    client.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("sektori", "STRING"),
                bigquery.SchemaField("taloustoimi", "STRING"),
                bigquery.SchemaField("tehtava", "STRING"),
                bigquery.SchemaField("vuosi", "INT64"),
                bigquery.SchemaField("meur", "FLOAT64"),
            ],
        ),
    ).result()
    print(f"BQ-taulu -> {table_id}")

    # Ristivalidointi: konseptisarja vs. COFOG-sarja
    print("\n=== Ristivalidointi (konsepti vs. virallinen COFOG, S1311) ===")
    failures = 0
    for concept, (division, caveat) in CONCEPT_COFOG_PAIRS.items():
        sql = f"""
        WITH oma AS (
          SELECT vuosi, SUM(total_meur) meur
          FROM `{args.project}.{args.dataset}.concept_yearly_totals_v1`
          WHERE concept = '{concept}' AND role IN ('include','component')
            AND vuosi BETWEEN {args.year_from} AND {args.year_to}
          GROUP BY vuosi
        ),
        virallinen AS (
          SELECT vuosi, meur FROM `{table_id}` WHERE tehtava = '{division}'
        )
        SELECT
          CORR(oma.meur, virallinen.meur) AS korrelaatio,
          AVG(oma.meur / NULLIF(virallinen.meur, 0)) AS suhde_keskiarvo,
          COUNT(*) AS vuosia
        FROM oma JOIN virallinen USING (vuosi)
        """
        result = list(client.query(sql).result())[0]
        corr = result.korrelaatio or 0
        ratio = result.suhde_keskiarvo or 0
        ok = corr > 0.5 and 0.2 < ratio < 5.0
        status = "OK  " if ok else "HUOM"
        if not ok:
            failures += 1
        print(
            f"[{status}] {concept} vs {division}: korrelaatio {corr:.2f}, "
            f"keskisuhde {ratio:.2f}x ({result.vuosia} vuotta) — {caveat}"
        )

    print("\nHuom: täsmäys ei ole tavoite (eri laskentakehikot); suunta ja suuruusluokka ovat.")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

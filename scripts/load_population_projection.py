#!/usr/bin/env python3
"""Lataa Tilastokeskuksen väestöennusteen Budjettihaukkaan tauluksi
population_projection_v1 ja snapshotiksi
data/reference/population_projection_v1.csv.

Mukana on kaksi laskelmaa samasta ennustekierroksesta:

- `vaestoennuste_2024`: virallinen ennuste, jossa nettomaahanmuutto jatkuu.
- `omavaraisennuste_2024`: sama laskelma ilman nettomaahanmuuttoa.

Omavaraisennuste ei ole vaihtoehtoinen ennuste vaan laskennallinen
vertailukohta. Se on mukana, koska se on ainoa tapa nähdä, kuinka paljon
työikäisen väestön kehitys riippuu maahanmuutosta. Ilman sitä virallista
ennustetta voisi lukea niin, että ikärakenne vakautuu itsestään.

Ikävuodet summataan viiteen ryhmään, koska yksittäiset ikävuodet eivät ole
käyttökelpoisia raportoinnissa ja koska rajapinta ei tarjoa valmiita
ryhmäsummia. Huoltosuhteet lasketaan samoista luvuista, jotta ne ovat
johdonmukaisia ryhmien kanssa.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings

OUT_CSV = ROOT / "data" / "reference" / "population_projection_v1.csv"
TABLE_NAME = "population_projection_v1"
MISSING = {".", "..", "...", "", None}

PROJECTIONS = (
    {
        "projection_id": "vaestoennuste_2024",
        "label": "Väestöennuste 2024",
        "includes_net_migration": True,
        "url": "https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/vaenn/14wx.px",
        "content": "vaesto_e24",
    },
    {
        "projection_id": "omavaraisennuste_2024",
        "label": "Omavaraisennuste 2024, ilman nettomaahanmuuttoa",
        "includes_net_migration": False,
        "url": "https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/vaenn/14x1.px",
        "content": "vaesto_omav_e24",
    },
)

# Ikäryhmät, jotka kattavat koko väestön päällekkäisyyksittä.
AGE_BANDS = (
    ("age_0_14", 0, 14),
    ("age_15_64", 15, 64),
    ("age_65_74", 65, 74),
    ("age_75_plus", 75, 200),
)

CSV_FIELDS = [
    "projection_id",
    "projection_label",
    "includes_net_migration",
    "year",
    "population_total",
    "age_0_14",
    "age_15_64",
    "age_65_74",
    "age_75_plus",
    "working_age_share_pct",
    "old_age_dependency_ratio",
    "total_dependency_ratio",
    "source_url",
    "fetched_at",
]


def _px(url: str, query: dict[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(
        url, data=json.dumps(query).encode("utf-8"), headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=180) as response:
        return json.load(response)


def _age_codes() -> list[str]:
    return [f"{age:03d}" for age in range(0, 100)] + ["100-"]


def _band_of(code: str) -> str:
    age = 100 if code == "100-" else int(code)
    for name, low, high in AGE_BANDS:
        if low <= age <= high:
            return name
    raise ValueError(f"Ikäkoodi {code} ei osu yhteenkään ryhmään")


def fetch_projection(spec: dict[str, Any]) -> dict[int, dict[str, float]]:
    metadata_request = urllib.request.Request(spec["url"])
    with urllib.request.urlopen(metadata_request, timeout=60) as response:
        metadata = json.load(response)
    years = [
        value
        for variable in metadata["variables"]
        if variable["code"] == "timeperiod_y"
        for value in variable["values"]
    ]

    payload = _px(spec["url"], {
        "query": [
            {"code": "alue_23_20240101", "selection": {"filter": "item", "values": ["SSS"]}},
            {"code": "timeperiod_y", "selection": {"filter": "item", "values": years}},
            {"code": "sukupuoli_9_20180101", "selection": {"filter": "item", "values": ["SSS"]}},
            {"code": "ikaryhma_10_20180101",
             "selection": {"filter": "item", "values": ["SSS"] + _age_codes()}},
            {"code": "contentscode",
             "selection": {"filter": "item", "values": [spec["content"]]}},
        ],
        "response": {"format": "json"},
    })

    rows: dict[int, dict[str, float]] = {}
    for row in payload["data"]:
        if row["values"][0] in MISSING:
            continue
        year = int(row["key"][1])
        age_code = row["key"][3]
        value = float(row["values"][0])
        bucket = rows.setdefault(year, {})
        if age_code == "SSS":
            bucket["population_total"] = value
        else:
            band = _band_of(age_code)
            bucket[band] = bucket.get(band, 0.0) + value
    if not rows:
        raise ValueError(f"{spec['projection_id']}: ennustetta ei saatu")
    return rows


def build_rows(fetched_at: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for spec in PROJECTIONS:
        for year, bands in sorted(fetch_projection(spec).items()):
            working = bands["age_15_64"]
            old = bands["age_65_74"] + bands["age_75_plus"]
            young = bands["age_0_14"]
            out.append({
                "projection_id": spec["projection_id"],
                "projection_label": spec["label"],
                "includes_net_migration": spec["includes_net_migration"],
                "year": year,
                "population_total": bands["population_total"],
                "age_0_14": young,
                "age_15_64": working,
                "age_65_74": bands["age_65_74"],
                "age_75_plus": bands["age_75_plus"],
                "working_age_share_pct": 100 * working / bands["population_total"],
                "old_age_dependency_ratio": 100 * old / working,
                "total_dependency_ratio": 100 * (young + old) / working,
                "source_url": spec["url"],
                "fetched_at": fetched_at,
            })
    return out


def validate(rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise ValueError("Rivejä ei muodostunut")

    ids = {row["projection_id"] for row in rows}
    if ids != {spec["projection_id"] for spec in PROJECTIONS}:
        raise ValueError(f"Ennusteita puuttuu: {ids}")

    for row in rows:
        # Ikäryhmien on katettava koko väestö. Jos summa ei täsmää, jokin
        # ikävuosi on pudonnut pois tai laskettu kahdesti.
        band_sum = row["age_0_14"] + row["age_15_64"] + row["age_65_74"] + row["age_75_plus"]
        if abs(band_sum - row["population_total"]) > 1.0:
            raise ValueError(
                f"{row['projection_id']} {row['year']}: ikäryhmien summa {band_sum:.0f} "
                f"ei vastaa kokonaisväestöä {row['population_total']:.0f}"
            )
        if not 40 < row["working_age_share_pct"] < 80:
            raise ValueError(f"Epäuskottava työikäisten osuus {row['working_age_share_pct']}")

    by_id: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_id.setdefault(row["projection_id"], []).append(row)
    for projection_id, series in by_id.items():
        years = [row["year"] for row in series]
        if years != sorted(years) or len(set(years)) != len(years):
            raise ValueError(f"{projection_id}: vuodet eivät ole aukoton nouseva sarja")
        if len(series) < 15:
            raise ValueError(f"{projection_id}: liian lyhyt ennustejakso")

    # Ilman nettomaahanmuuttoa työikäisiä on aina vähemmän kuin virallisessa
    # ennusteessa, ja ero kasvaa. Jos näin ei ole, laskelmat ovat menneet
    # sekaisin keskenään.
    official = {row["year"]: row for row in by_id["vaestoennuste_2024"]}
    domestic = {row["year"]: row for row in by_id["omavaraisennuste_2024"]}
    shared = sorted(set(official) & set(domestic))
    first, last = shared[0], shared[-1]
    for year in shared:
        if domestic[year]["age_15_64"] > official[year]["age_15_64"] + 1.0:
            raise ValueError(f"Omavaraisennuste ylittää virallisen vuonna {year}")
    gap_first = official[first]["age_15_64"] - domestic[first]["age_15_64"]
    gap_last = official[last]["age_15_64"] - domestic[last]["age_15_64"]
    if gap_last <= gap_first:
        raise ValueError("Maahanmuuton vaikutus ei kasva ajassa, laskelmat epäilyttävät")


def load_bigquery(rows: list[dict[str, Any]], *, project: str, dataset: str) -> None:
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{TABLE_NAME}"
    schema = [
        bigquery.SchemaField("projection_id", "STRING"),
        bigquery.SchemaField("projection_label", "STRING"),
        bigquery.SchemaField("includes_net_migration", "BOOL"),
        bigquery.SchemaField("year", "INT64"),
        bigquery.SchemaField("population_total", "FLOAT64"),
        bigquery.SchemaField("age_0_14", "FLOAT64"),
        bigquery.SchemaField("age_15_64", "FLOAT64"),
        bigquery.SchemaField("age_65_74", "FLOAT64"),
        bigquery.SchemaField("age_75_plus", "FLOAT64"),
        bigquery.SchemaField("working_age_share_pct", "FLOAT64"),
        bigquery.SchemaField("old_age_dependency_ratio", "FLOAT64"),
        bigquery.SchemaField("total_dependency_ratio", "FLOAT64"),
        bigquery.SchemaField("source_url", "STRING"),
        bigquery.SchemaField("fetched_at", "TIMESTAMP"),
    ]
    client.load_table_from_json(
        rows, table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema),
    ).result()
    print(f"BQ-taulu -> {table_id} ({len(rows)} riviä)")


def main() -> int:
    parser = argparse.ArgumentParser(description="Lataa väestöennuste Budjettihaukkaan.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--output", type=Path, default=OUT_CSV)
    parser.add_argument("--load-bigquery", action="store_true")
    args = parser.parse_args()

    fetched_at = dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    rows = build_rows(fetched_at)
    validate(rows)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

    years = sorted({row["year"] for row in rows})
    print(f"Snapshot -> {args.output.relative_to(ROOT)} ({len(rows)} riviä, "
          f"{years[0]}-{years[-1]}, {len(PROJECTIONS)} laskelmaa)")

    official = [row for row in rows if row["projection_id"] == "vaestoennuste_2024"]
    domestic = [row for row in rows if row["projection_id"] == "omavaraisennuste_2024"]
    gap = official[-1]["age_15_64"] - domestic[-1]["age_15_64"]
    print(f"  työikäiset {years[-1]}: virallinen {official[-1]['age_15_64'] / 1e6:.3f} M, "
          f"ilman maahanmuuttoa {domestic[-1]['age_15_64'] / 1e6:.3f} M, ero {gap / 1e3:.0f} tuhatta")

    if args.load_bigquery:
        load_bigquery(rows, project=args.project, dataset=args.dataset)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

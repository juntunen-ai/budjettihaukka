#!/usr/bin/env python3
"""Fetch official national comparison series for visualization-ready facts.

The checked-in CSV is a reproducible source snapshot. BigQuery loading is
explicit (``--load-bigquery``), so refreshing the reference data never
silently mutates the warehouse.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings

DEFAULT_OUTPUT = ROOT / "data" / "reference" / "official_macro_reference_v1.csv"


@dataclass(frozen=True)
class SeriesSpec:
    series_id: str
    source_id: str
    url: str
    unit: str
    period_basis: str
    dimensions: dict[str, str]
    time_dimension: str
    time_filter: Callable[[str, int, int], bool]
    year_from_time: Callable[[str], int]


def _annual(value: str, year_from: int, year_to: int) -> bool:
    return value.isdigit() and year_from <= int(value) <= year_to


def _q4(value: str, year_from: int, year_to: int) -> bool:
    return value.endswith("Q4") and value[:4].isdigit() and year_from <= int(value[:4]) <= year_to


SERIES = (
    SeriesSpec(
        series_id="population_midyear_persons",
        source_id="statfin_population_midyear_11s1",
        url="https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/vaerak/11s1.px",
        unit="PERSON",
        period_basis="calendar_year_midyear_population",
        dimensions={
            "alue_23_20260101": "SSS",
            "sukupuoli_9_20180101": "SSS",
            "ikaryhma_10_20180101": "SSS",
            "contentscode": "vaerak-keskiv",
        },
        time_dimension="timeperiod_y",
        time_filter=_annual,
        year_from_time=int,
    ),
    SeriesSpec(
        series_id="cost_of_living_index_1951_10_100",
        source_id="statfin_cost_of_living_11xm",
        url="https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/khi/11xm.px",
        unit="INDEX_1951_10_100",
        period_basis="calendar_year_average",
        dimensions={"contentscode": "khi-pisteluku"},
        time_dimension="timeperiod_y",
        time_filter=_annual,
        year_from_time=int,
    ),
    SeriesSpec(
        series_id="gdp_current_prices_meur",
        source_id="statfin_national_accounts_15a9",
        url="https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/ntp/15a9.px",
        unit="MEUR",
        period_basis="calendar_year_flow_current_prices",
        dimensions={"taloustoimi_1_20180101": "B1GMH", "contentscode": "ntp-cp"},
        time_dimension="timeperiod_y",
        time_filter=_annual,
        year_from_time=int,
    ),
    # Talouskasvu virallisena volyymin muutoksena. Kayvin hinnoin laskettu
    # BKT ei kelpaa kasvun mittariksi, eika elinkustannusindeksilla
    # deflatointi tuota samaa lukua: 2023 volyymi -1,3 % mutta CPI-
    # deflatoituna -3,5 %. Hintaindeksi otetaan mukaan, koska se on BKT:n
    # oma deflaattori toisin kuin kolme muuta kaytossa olevaa indeksia.
    SeriesSpec(
        series_id="gdp_volume_change_pct",
        source_id="statfin_national_accounts_15a9",
        url="https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/ntp/15a9.px",
        unit="PERCENT",
        period_basis="calendar_year_volume_change",
        dimensions={"taloustoimi_1_20180101": "B1GMH", "contentscode": "ntp-vol_muutos"},
        time_dimension="timeperiod_y",
        time_filter=_annual,
        year_from_time=int,
    ),
    SeriesSpec(
        series_id="gdp_volume_index_2015_100",
        source_id="statfin_national_accounts_15a9",
        url="https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/ntp/15a9.px",
        unit="INDEX_2015_100",
        period_basis="calendar_year_volume_index",
        dimensions={"taloustoimi_1_20180101": "B1GMH", "contentscode": "ntp-vol_ind"},
        time_dimension="timeperiod_y",
        time_filter=_annual,
        year_from_time=int,
    ),
    SeriesSpec(
        series_id="gdp_price_index_2015_100",
        source_id="statfin_national_accounts_15a9",
        url="https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/ntp/15a9.px",
        unit="INDEX_2015_100",
        period_basis="calendar_year_price_index",
        dimensions={"taloustoimi_1_20180101": "B1GMH", "contentscode": "ntp-vv"},
        time_dimension="timeperiod_y",
        time_filter=_annual,
        year_from_time=int,
    ),
    # Tyopanos: elintason hajotelma asukasta kohden vaatii seka tyollisten
    # maaran etta tehdyt tunnit. Pelkka tyollisyysaste ei riita, koska
    # tunnit tyollista kohden ovat laskeneet samaan aikaan kun tyollisyys
    # on noussut.
    SeriesSpec(
        series_id="employed_persons_thousands",
        source_id="statfin_national_accounts_15ab",
        url="https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/ntp/15ab.px",
        unit="THOUSAND_PERSONS",
        period_basis="calendar_year_domestic_employment",
        dimensions={
            "taloustoimi_1_20180101": "E1",
            "sektoriluokitus_7_20230101": "S1",
            "toimiala_79_20180101": "SSS",
        },
        time_dimension="timeperiod_y",
        time_filter=_annual,
        year_from_time=int,
    ),
    SeriesSpec(
        series_id="hours_worked_millions",
        source_id="statfin_national_accounts_15ab",
        url="https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/ntp/15ab.px",
        unit="MILLION_HOURS",
        period_basis="calendar_year_hours_worked",
        dimensions={
            "taloustoimi_1_20180101": "E2",
            "sektoriluokitus_7_20230101": "S1",
            "toimiala_79_20180101": "SSS",
        },
        time_dimension="timeperiod_y",
        time_filter=_annual,
        year_from_time=int,
    ),
    SeriesSpec(
        series_id="central_government_edp_debt_q4_meur",
        source_id="statfin_central_government_edp_debt_11yv",
        url="https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/jyev/11yv.px",
        unit="MEUR",
        period_basis="fourth_quarter_end_stock",
        dimensions={
            "sektoriluokitus_7_20230101": "S1311",
            "varojenluokitus_5_20180101": "F2TF4",
            "contentscode": "jyev-K",
        },
        time_dimension="timeperiod_q",
        time_filter=_q4,
        year_from_time=lambda value: int(value[:4]),
    ),
)


def _metadata(session: requests.Session, spec: SeriesSpec) -> dict[str, Any]:
    response = session.get(spec.url, timeout=60)
    response.raise_for_status()
    return response.json()


def _values_for(metadata: dict[str, Any], code: str) -> list[str]:
    for variable in metadata.get("variables", []):
        if variable.get("code") == code:
            return [str(value) for value in variable.get("values", [])]
    raise ValueError(f"PxWeb metadata does not contain dimension {code!r}")


def _validate_dimension_values(metadata: dict[str, Any], spec: SeriesSpec) -> None:
    for code, value in spec.dimensions.items():
        if value not in _values_for(metadata, code):
            raise ValueError(f"{spec.series_id}: value {value!r} missing from dimension {code!r}")


def fetch_series(
    spec: SeriesSpec,
    *,
    year_from: int,
    year_to: int,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    http = session or requests.Session()
    metadata = _metadata(http, spec)
    _validate_dimension_values(metadata, spec)
    times = [
        value
        for value in _values_for(metadata, spec.time_dimension)
        if spec.time_filter(value, year_from, year_to)
    ]
    if not times:
        raise ValueError(f"{spec.series_id}: source has no periods in {year_from}-{year_to}")

    query = [
        {"code": code, "selection": {"filter": "item", "values": [value]}}
        for code, value in spec.dimensions.items()
    ]
    query.append(
        {"code": spec.time_dimension, "selection": {"filter": "item", "values": times}}
    )
    response = http.post(spec.url, json={"query": query, "response": {"format": "json"}}, timeout=90)
    response.raise_for_status()
    data = response.json().get("data", [])

    rows: list[dict[str, Any]] = []
    for item in data:
        keys = [str(value) for value in item.get("key", [])]
        time_value = next((value for value in keys if value in times), None)
        values = item.get("values", [])
        raw_value = values[0] if values else None
        if time_value is None or raw_value in (None, ".", "..", "..."):
            continue
        rows.append(
            {
                "series_id": spec.series_id,
                "year": spec.year_from_time(time_value),
                "period": time_value,
                "value": float(str(raw_value).replace(" ", "").replace(",", ".")),
                "unit": spec.unit,
                "period_basis": spec.period_basis,
                "source_id": spec.source_id,
                "source_url": spec.url,
                "source_table_title": str(metadata.get("title") or ""),
            }
        )
    if len(rows) != len(times):
        raise ValueError(f"{spec.series_id}: expected {len(times)} values, received {len(rows)}")
    return sorted(rows, key=lambda row: (row["series_id"], row["year"]))


CSV_FIELDS = [
    "series_id",
    "year",
    "period",
    "value",
    "unit",
    "period_basis",
    "source_id",
    "source_url",
    "source_table_title",
]


def write_snapshot(rows: list[dict[str, Any]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def load_bigquery(rows: list[dict[str, Any]], *, project: str, dataset: str, table: str) -> None:
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("series_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("year", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("period", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("value", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("unit", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("period_basis", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source_url", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source_table_title", "STRING", mode="NULLABLE"),
        ],
    )
    client.load_table_from_json(rows, table_id, job_config=job_config).result()
    print(f"BigQuery table -> {table_id} ({len(rows)} rows)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch official visualization reference series.")
    parser.add_argument("--year-from", type=int, default=1998)
    parser.add_argument("--year-to", type=int, default=2026)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--load-bigquery", action="store_true")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--table", default="official_macro_reference_v1")
    parser.add_argument("--print-json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.year_from > args.year_to:
        raise SystemExit("--year-from must be <= --year-to")
    rows: list[dict[str, Any]] = []
    with requests.Session() as session:
        for spec in SERIES:
            fetched = fetch_series(spec, year_from=args.year_from, year_to=args.year_to, session=session)
            print(f"{spec.series_id}: {len(fetched)} rows")
            rows.extend(fetched)
    rows.sort(key=lambda row: (row["series_id"], row["year"]))
    write_snapshot(rows, args.output)
    print(f"Snapshot -> {args.output}")
    if args.load_bigquery:
        load_bigquery(rows, project=args.project, dataset=args.dataset, table=args.table)
    if args.print_json:
        print(json.dumps(rows, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

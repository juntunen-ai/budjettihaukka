#!/usr/bin/env python3
"""Snapshot Budjettihaukka actuals for the Liberal policy backcast."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from google.cloud import bigquery


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = (
    ROOT
    / "data"
    / "reference"
    / "liberaali_vaihtoehtobudjetti"
    / "historical_inputs"
)
START_YEAR = 2007
END_YEAR = 2025


ACTUAL_QUERY = """
SELECT
  year,
  fiscal_side,
  momentti_tunnusp,
  ARRAY_AGG(DISTINCT momentti_snimi IGNORE NULLS ORDER BY momentti_snimi LIMIT 1)[SAFE_OFFSET(0)] AS momentti_snimi,
  ARRAY_AGG(DISTINCT hallinnonala IGNORE NULLS ORDER BY hallinnonala LIMIT 1)[SAFE_OFFSET(0)] AS hallinnonala,
  SUM(net_accumulation_nominal_eur) AS actual_eur,
  LOGICAL_OR(has_structural_guardrail) AS has_structural_guardrail,
  ARRAY_TO_STRING(ARRAY_CONCAT_AGG(structural_relation_types), '|') AS structural_relation_types,
  SUM(source_rows) AS source_rows,
  MAX(observed_months) AS observed_months,
  MAX(data_as_of) AS data_as_of,
  LOGICAL_AND(is_complete_year) AS is_complete_year,
  ARRAY_AGG(DISTINCT coverage_status ORDER BY coverage_status LIMIT 1)[SAFE_OFFSET(0)] AS coverage_status
FROM `{project}.{dataset}.analytics_fiscal_yearly_core_v1`
WHERE year BETWEEN {start_year} AND {end_year}
  AND momentti_tunnusp IS NOT NULL
GROUP BY year, fiscal_side, momentti_tunnusp
ORDER BY year, fiscal_side, momentti_tunnusp
"""


MACRO_QUERY = """
SELECT
  year,
  MAX(IF(series_id = 'gdp_current_prices_meur', value, NULL)) AS gdp_current_prices_meur,
  MAX(IF(series_id = 'gdp_volume_change_pct', value, NULL)) AS gdp_volume_change_pct,
  MAX(IF(series_id = 'gdp_volume_index_2015_100', value, NULL)) AS gdp_volume_index_2015_100,
  MAX(IF(series_id = 'gdp_price_index_2015_100', value, NULL)) AS gdp_price_index_2015_100,
  MAX(IF(series_id = 'central_government_edp_debt_q4_meur', value, NULL)) AS central_government_edp_debt_q4_meur,
  MAX(IF(series_id = 'employed_persons_thousands', value, NULL)) AS employed_persons_thousands,
  MAX(IF(series_id = 'hours_worked_millions', value, NULL)) AS hours_worked_millions,
  MAX(IF(series_id = 'population_midyear_persons', value, NULL)) AS population_midyear_persons,
  COUNT(DISTINCT series_id) AS available_series_count
FROM `{project}.{dataset}.official_macro_reference_v1`
WHERE year BETWEEN {start_year} AND {end_year}
GROUP BY year
ORDER BY year
"""


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def query_hash(query: str) -> str:
    return sha256_bytes(query.encode("utf-8"))


def scalar(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def rows_as_dicts(rows: Iterable[Any]) -> list[dict[str, Any]]:
    return [{key: scalar(value) for key, value in row.items()} for row in rows]


def write_csv(path: Path, rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        raise ValueError(f"Refusing to write empty snapshot: {path.name}")
    fieldnames = list(rows[0])
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    content = path.read_bytes()
    return {
        "path": str(path.relative_to(ROOT)),
        "row_count": len(rows),
        "sha256": sha256_bytes(content),
        "bytes": len(content),
    }


def table_meta(client: bigquery.Client, table_id: str) -> dict[str, Any]:
    table = client.get_table(table_id)
    return {
        "table_id": table_id,
        "modified": table.modified.astimezone(UTC).isoformat() if table.modified else None,
        "num_rows": table.num_rows,
        "table_type": table.table_type,
    }


def validate(actuals: list[dict[str, Any]], macro: list[dict[str, Any]]) -> None:
    actual_years = sorted({int(row["year"]) for row in actuals})
    macro_years = sorted(int(row["year"]) for row in macro)
    expected = list(range(START_YEAR, END_YEAR + 1))
    if actual_years != expected or macro_years != expected:
        raise ValueError("Historical snapshot does not cover every year 2007-2025")
    if any(row["is_complete_year"] != "true" for row in actuals):
        raise ValueError("Historical actuals include an incomplete year")
    if any(int(row["available_series_count"]) < 9 for row in macro):
        raise ValueError("Historical macro snapshot is missing required official series")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="budjettihaukka-gpt")
    parser.add_argument("--dataset", default="valtiodata")
    parser.add_argument("--location", default="europe-west1")
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = parser.parse_args()

    out_dir = args.out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    client = bigquery.Client(project=args.project, location=args.location)
    actual_query = ACTUAL_QUERY.format(
        project=args.project,
        dataset=args.dataset,
        start_year=START_YEAR,
        end_year=END_YEAR,
    ).strip()
    macro_query = MACRO_QUERY.format(
        project=args.project,
        dataset=args.dataset,
        start_year=START_YEAR,
        end_year=END_YEAR,
    ).strip()

    actual_job = client.query(actual_query, location=args.location)
    macro_job = client.query(macro_query, location=args.location)
    actuals = rows_as_dicts(actual_job.result())
    macro = rows_as_dicts(macro_job.result())
    validate(actuals, macro)

    files = [
        write_csv(out_dir / "budjettihaukka_actual_by_moment_2007_2025.csv", actuals),
        write_csv(out_dir / "budjettihaukka_macro_2007_2025.csv", macro),
    ]
    manifest = {
        "dataset_id": "liberaali_historiallinen_vastelaskelma_inputs_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "year_from": START_YEAR,
        "year_to": END_YEAR,
        "project": args.project,
        "dataset": args.dataset,
        "location": args.location,
        "queries": [
            {
                "name": "actual_by_moment",
                "sha256": query_hash(actual_query),
                "total_bytes_processed": actual_job.total_bytes_processed,
            },
            {
                "name": "official_macro",
                "sha256": query_hash(macro_query),
                "total_bytes_processed": macro_job.total_bytes_processed,
            },
        ],
        "tables": [
            table_meta(
                client,
                f"{args.project}.{args.dataset}.analytics_fiscal_yearly_core_v1",
            ),
            table_meta(
                client,
                f"{args.project}.{args.dataset}.official_macro_reference_v1",
            ),
        ],
        "files": files,
        "source_notes": [
            "Toteumat ovat Valtiokonttorin kuukausiaineistosta rakennettua Budjettihaukan vuosimarttia.",
            "Makrosarjat ovat Budjettihaukkaan jäädytettyjä Tilastokeskuksen virallisia sarjoja.",
            "Talousarviotalouden toteuma ja valtionhallinnon EDP-velka ovat eri tilastokehikkoja.",
        ],
    }
    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"{files[0]['path']} ({files[0]['row_count']} riviä)")
    print(f"{files[1]['path']} ({files[1]['row_count']} riviä)")
    print(manifest_path.relative_to(ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

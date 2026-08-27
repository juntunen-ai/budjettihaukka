#!/usr/bin/env python3
"""Build the state-tax-revenue accounting scenario for the growth report."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
PRODUCTIVITY_PATH = ROOT / "data" / "reference" / "productivity_counterfactual_v1.json"
OUTPUT_PATH = ROOT / "data" / "reference" / "tax_growth_scenario_v1.json"
DEFAULT_PROJECT = "budjettihaukka-gpt"
DEFAULT_DATASET = "valtiodata"
DEFAULT_TABLE = "analytics_fiscal_yearly_core_v1"
OFFICIAL_SOURCE_URL = "https://api.tutkihallintoa.fi/valtiontalous/v1/"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _number(value: Any) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    return value


def build_query(table_id: str) -> str:
    return f"""
SELECT
  year,
  -SUM(CAST(net_accumulation_nominal_eur AS FLOAT64)) / 1000000000 AS nominal_tax_revenue_beur,
  -SUM(net_accumulation_real_cpi_eur) / 1000000000 AS real_tax_revenue_beur,
  ANY_VALUE(real_base_year) AS real_base_year,
  LOGICAL_AND(is_complete_year) AS is_complete_year,
  MAX(data_as_of) AS data_as_of,
  COUNT(DISTINCT momentti_tunnusp) AS tax_moment_count
FROM `{table_id}`
WHERE year BETWEEN @year_from AND @year_to
  AND fiscal_side = 'revenue'
  AND REGEXP_CONTAINS(momentti_tunnusp, r'^11\\.')
GROUP BY year
ORDER BY year
""".strip()


def fetch_tax_rows(
    client: bigquery.Client,
    table_id: str,
    year_from: int,
    year_to: int,
) -> tuple[list[dict[str, Any]], int]:
    query = build_query(table_id)
    parameters = [
        bigquery.ScalarQueryParameter("year_from", "INT64", year_from),
        bigquery.ScalarQueryParameter("year_to", "INT64", year_to),
    ]
    dry_config = bigquery.QueryJobConfig(
        query_parameters=parameters,
        dry_run=True,
        use_query_cache=False,
    )
    dry_job = client.query(query, job_config=dry_config)
    config = bigquery.QueryJobConfig(query_parameters=parameters, use_query_cache=True)
    rows = []
    for row in client.query(query, job_config=config).result():
        item = {key: _number(value) for key, value in dict(row).items()}
        if item.get("data_as_of") is not None:
            item["data_as_of"] = item["data_as_of"].isoformat()
        rows.append(item)
    return rows, int(dry_job.total_bytes_processed or 0)


def build_snapshot(
    productivity: dict[str, Any],
    tax_rows: list[dict[str, Any]],
    *,
    source_table: str,
    dry_run_bytes: int,
    generated_at: str,
) -> dict[str, Any]:
    year_from = 2008
    year_to = 2025
    productivity_by_year = {row["year"]: row for row in productivity["years"]}
    tax_by_year = {row["year"]: row for row in tax_rows}
    rows = []
    for year in range(year_from, year_to + 1):
        tax = tax_by_year.get(year)
        prod = productivity_by_year.get(year)
        if not tax or not prod:
            raise ValueError(f"Missing tax or productivity observation for {year}")
        if tax.get("is_complete_year") is not True:
            raise ValueError(f"Incomplete tax year: {year}")
        if tax.get("real_base_year") != 2025:
            raise ValueError(f"Unexpected tax price base: {year}")
        actual_index = prod["productivity_index"]
        modest_index = prod["modest_index"]
        if actual_index is None or modest_index is None:
            raise ValueError(f"Missing productivity scenario for {year}")
        growth_multiplier = modest_index / actual_index
        actual_revenue = tax["real_tax_revenue_beur"]
        unchanged_rate = actual_revenue * growth_multiplier
        lower_5 = unchanged_rate * 0.95
        lower_10 = unchanged_rate * 0.90
        break_even_cut = 100 * (1 - 1 / growth_multiplier)
        rows.append(
            {
                **tax,
                "actual_productivity_index": actual_index,
                "modest_productivity_index": modest_index,
                "tax_base_multiplier": growth_multiplier,
                "scenario_revenue_unchanged_rate_beur": unchanged_rate,
                "scenario_revenue_5pct_lower_rate_beur": lower_5,
                "scenario_revenue_10pct_lower_rate_beur": lower_10,
                "break_even_relative_rate_cut_pct": break_even_cut,
            }
        )

    final = rows[-1]
    return {
        "meta": {
            "dataset_id": "tax_growth_scenario_v1",
            "schema_version": "1.0.0",
            "generated_at": generated_at,
            "source_table": source_table,
            "official_source_name": "Valtiokonttori, Tutkihallintoa.fi valtiontalouden kuukausidata",
            "official_source_url": OFFICIAL_SOURCE_URL,
            "tax_scope_fi": "Valtion budjettitalouden osaston 11 verot ja veronluonteiset tulot; ei kuntien veroja eikä koko julkisen talouden sosiaalivakuutusmaksuja.",
            "tax_sign_rule_fi": "Tulopuolen negatiivinen nettokertymä esitetään positiivisena verokertymänä.",
            "price_base_year": 2025,
            "year_from": year_from,
            "year_to": year_to,
            "growth_scenario_fi": "Työn tuottavuus kasvaa vuoden 2008 jälkeen 1,0 prosenttia vuodessa ja tehdyt työtunnit säilyvät toteutuneina.",
            "rate_cut_definition_fi": "Suhteellinen alennus valtion keskimääräiseen efektiiviseen verokertymään; ei prosenttiyksikkömuutos työn verokiilassa.",
            "calculation_fi": "Vaihtoehtoinen verokertymä = toteutunut reaalinen verokertymä × (1 % tuottavuusuran indeksi / toteutunut tuottavuusindeksi) × (1 − suhteellinen veroasteen alennus).",
            "causality_warning_fi": "Kirjanpidollinen skenaario ei arvioi dynaamisesti veronalennuksen vaikutusta työn tarjontaan, investointeihin, veropohjan rakenteeseen tai tuottavuuteen eikä osoita, että veronalennus olisi aiheuttanut 1 prosentin kasvun.",
            "query_dry_run_bytes": dry_run_bytes,
            "evidence": [
                {
                    "publisher": "OECD",
                    "title": "OECD Economic Surveys: Finland 2018",
                    "url": "https://www.oecd.org/en/publications/oecd-economic-surveys-finland-2018_eco_surveys-fin-2018-en/full-report/component-3.html",
                    "finding_fi": "OECD:n rakennemallissa verokiilan alentaminen Ruotsin tasolle kasvatti BKT:tä asukasta kohti 1,8 prosenttia ja OECD-keskiarvoon alentaminen yli 2,5 prosenttia kymmenessä vuodessa julkisen talouden tasapaino säilyttäen.",
                    "used_in_formula": False
                },
                {
                    "publisher": "OECD",
                    "title": "Taxing Wages 2026: Finland",
                    "url": "https://www.oecd.org/content/dam/oecd/en/publications/reports/2026/04/taxing-wages-2026-country-notes_491a0e97/finland_fd5780b2/9bc058eb-en.pdf",
                    "finding_fi": "Keskimääräisen lapsettoman palkansaajan verokiila oli Suomessa 42,5 prosenttia vuonna 2025 ja OECD-maiden keskiarvo 35,1 prosenttia.",
                    "used_in_formula": False
                }
            ]
        },
        "rows": rows,
        "summary_2025": {
            "actual_tax_revenue_beur": final["real_tax_revenue_beur"],
            "actual_productivity_index": final["actual_productivity_index"],
            "modest_productivity_index": final["modest_productivity_index"],
            "tax_base_multiplier": final["tax_base_multiplier"],
            "unchanged_rate_revenue_beur": final["scenario_revenue_unchanged_rate_beur"],
            "lower_5pct_revenue_beur": final["scenario_revenue_5pct_lower_rate_beur"],
            "lower_10pct_revenue_beur": final["scenario_revenue_10pct_lower_rate_beur"],
            "break_even_relative_rate_cut_pct": final["break_even_relative_rate_cut_pct"],
        },
    }


def validate_snapshot(snapshot: dict[str, Any]) -> None:
    meta = snapshot.get("meta", {})
    rows = snapshot.get("rows", [])
    if meta.get("dataset_id") != "tax_growth_scenario_v1":
        raise ValueError("Unexpected tax growth scenario dataset id")
    if len(rows) != 18 or rows[0]["year"] != 2008 or rows[-1]["year"] != 2025:
        raise ValueError("Tax scenario must cover every year from 2008 to 2025")
    if any(row["real_tax_revenue_beur"] <= 0 for row in rows):
        raise ValueError("Tax revenue must be displayed as a positive value")
    if any(row["tax_base_multiplier"] < 1 for row in rows):
        raise ValueError("The reviewed 1 percent path must not fall below actual productivity")
    final = snapshot["summary_2025"]
    if not 1.1 < final["tax_base_multiplier"] < 1.2:
        raise ValueError("Unexpected 2025 tax-base multiplier")
    if not 10 < final["break_even_relative_rate_cut_pct"] < 12:
        raise ValueError("Unexpected 2025 break-even tax-rate reduction")
    if final["lower_10pct_revenue_beur"] <= final["actual_tax_revenue_beur"]:
        raise ValueError("Ten percent lower-rate scenario should still exceed actual revenue")
    if any(item.get("used_in_formula") is not False for item in meta.get("evidence", [])):
        raise ValueError("External evidence must not be silently used as a scenario coefficient")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    args = parser.parse_args()

    productivity = load_json(PRODUCTIVITY_PATH)
    table_id = f"{args.project}.{args.dataset}.{args.table}"
    client = bigquery.Client(project=args.project)
    tax_rows, dry_run_bytes = fetch_tax_rows(client, table_id, 2008, 2025)
    snapshot = build_snapshot(
        productivity,
        tax_rows,
        source_table=table_id,
        dry_run_bytes=dry_run_bytes,
        generated_at=datetime.now(UTC).replace(microsecond=0).isoformat(),
    )
    validate_snapshot(snapshot)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(args.output.relative_to(ROOT))
    summary = snapshot["summary_2025"]
    print(
        f"  2025 actual {summary['actual_tax_revenue_beur']:.1f} bn; "
        f"1% path and 10% lower rate {summary['lower_10pct_revenue_beur']:.1f} bn"
    )
    print(f"  break-even relative rate cut {summary['break_even_relative_rate_cut_pct']:.1f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

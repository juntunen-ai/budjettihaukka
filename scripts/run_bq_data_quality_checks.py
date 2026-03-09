#!/usr/bin/env python3
"""Run data quality checks against a BigQuery table and emit reports."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config import settings

logger = logging.getLogger("run_bq_data_quality_checks")


@dataclass(frozen=True)
class DQCheck:
    name: str
    description: str
    sql: str
    fail_ratio: float | None = None
    fail_count: int | None = None
    warn_ratio: float | None = None
    warn_count: int | None = None


def _table_ref(project: str, dataset: str, table: str) -> str:
    if table.count(".") == 2:
        return table
    return f"{project}.{dataset}.{table}"


def _scalar(client: bigquery.Client, sql: str):
    row = next(client.query(sql).result(), None)
    if row is None:
        return None
    return row[0]


def _check_status(failed_count: int, total_rows: int, check: DQCheck) -> tuple[str, float]:
    ratio = (failed_count / total_rows) if total_rows > 0 else 0.0
    if check.fail_count is not None and failed_count > check.fail_count:
        return "FAIL", ratio
    if check.fail_ratio is not None and ratio > check.fail_ratio:
        return "FAIL", ratio
    if check.warn_count is not None and failed_count > check.warn_count:
        return "WARN", ratio
    if check.warn_ratio is not None and ratio > check.warn_ratio:
        return "WARN", ratio
    return "PASS", ratio


def _numeric_parse_expr(raw_col: str) -> str:
    return (
        "SAFE_CAST(REPLACE(REPLACE(REPLACE(REPLACE("
        f"REGEXP_REPLACE(NULLIF(TRIM({raw_col}), ''), r'\\s+', ''), "
        "'−', '-'), ' ', ''), ' ', ''), ',', '.') AS NUMERIC)"
    )


def _detect_table_mode(columns: set[str]) -> str:
    if {"quality_issue_count", "period_date", "is_valid_year", "is_valid_month"} <= columns:
        return "curated"
    return "raw"


def _summary_sql(table_ref: str, mode: str) -> str:
    if mode == "curated":
        return f"""
SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT row_fingerprint) AS distinct_rows,
  COUNTIF(quality_issue_count > 0) AS rows_with_issues,
  MIN(period_date) AS min_period,
  MAX(period_date) AS max_period
FROM `{table_ref}`
"""
    return f"""
WITH typed AS (
  SELECT
    SAFE_CAST(`Vuosi` AS INT64) AS vuosi,
    SAFE_CAST(`Kk` AS INT64) AS kk
  FROM `{table_ref}`
)
SELECT
  COUNT(*) AS row_count,
  COUNT(*) AS distinct_rows,
  COUNTIF(vuosi IS NULL OR kk IS NULL OR kk NOT BETWEEN 1 AND 12) AS rows_with_issues,
  DATE(MIN(vuosi), 1, 1) AS min_period,
  DATE(MAX(vuosi), 12, 1) AS max_period
FROM typed
"""


def _build_checks(table_ref: str, mode: str) -> list[DQCheck]:
    if mode == "curated":
        return [
            DQCheck(
                name="invalid_year_or_month",
                description="Vuosi/kuukausi tulee olla validi analyysia varten.",
                sql=f"SELECT COUNTIF(NOT is_valid_year OR NOT is_valid_month) FROM `{table_ref}`",
                fail_count=0,
            ),
            DQCheck(
                name="invalid_nettokertyma_parse",
                description="Nettokertymän parse-virhe (raw arvo on olemassa, cast epäonnistuu).",
                sql=(
                    f"SELECT COUNTIF(nettokertyma_raw IS NOT NULL AND nettokertyma IS NULL) "
                    f"FROM `{table_ref}`"
                ),
                fail_ratio=0.02,
                warn_ratio=0.005,
            ),
            DQCheck(
                name="missing_hallinnonala",
                description="Hallinnonala puuttuu.",
                sql=f"SELECT COUNTIF(hallinnonala IS NULL) FROM `{table_ref}`",
                fail_ratio=0.02,
                warn_ratio=0.005,
            ),
            DQCheck(
                name="missing_momentti",
                description="Momentti tunnus tai nimi puuttuu molemmat.",
                sql=f"SELECT COUNTIF(COALESCE(momentti_tunnusp, momentti_snimi) IS NULL) FROM `{table_ref}`",
                fail_ratio=0.15,
                warn_ratio=0.05,
            ),
            DQCheck(
                name="duplicate_row_fingerprint",
                description="Täsmälleen samat rivit duplikaatteina.",
                sql=(
                    "WITH d AS ("
                    f"  SELECT row_fingerprint, COUNT(*) c FROM `{table_ref}` GROUP BY row_fingerprint HAVING COUNT(*) > 1"
                    ") SELECT COALESCE(SUM(c - 1), 0) FROM d"
                ),
                fail_count=0,
            ),
            DQCheck(
                name="missing_months_before_latest_year",
                description="Puuttuvia kuukausia ennen viimeisintä vuotta.",
                sql=(
                    "WITH bounds AS ("
                    f"  SELECT MIN(vuosi) min_year, MAX(vuosi) max_year FROM `{table_ref}`"
                    "), expected AS ("
                    "  SELECT y AS vuosi, m AS kk "
                    "  FROM bounds, UNNEST(GENERATE_ARRAY(min_year, max_year - 1)) AS y, UNNEST(GENERATE_ARRAY(1, 12)) AS m"
                    "), actual AS ("
                    f"  SELECT DISTINCT vuosi, kk FROM `{table_ref}`"
                    ") "
                    "SELECT COUNT(*) FROM expected e LEFT JOIN actual a USING (vuosi, kk) WHERE a.vuosi IS NULL"
                ),
                fail_count=12,
                warn_count=0,
            ),
        ]

    return [
        DQCheck(
            name="invalid_year_parse",
            description="Vuosi ei parsennu INT64-arvoksi.",
            sql=f"SELECT COUNTIF(SAFE_CAST(`Vuosi` AS INT64) IS NULL) FROM `{table_ref}`",
            fail_ratio=0.001,
            warn_ratio=0.0001,
        ),
        DQCheck(
            name="invalid_month_parse",
            description="Kk ei parsennu tai ei ole välillä 1-12.",
            sql=(
                f"SELECT COUNTIF(SAFE_CAST(`Kk` AS INT64) IS NULL OR SAFE_CAST(`Kk` AS INT64) NOT BETWEEN 1 AND 12) "
                f"FROM `{table_ref}`"
            ),
            fail_ratio=0.001,
            warn_ratio=0.0001,
        ),
        DQCheck(
            name="missing_hallinnonala",
            description="Hallinnonala puuttuu.",
            sql=f"SELECT COUNTIF(NULLIF(TRIM(`Hallinnonala`), '') IS NULL) FROM `{table_ref}`",
            fail_ratio=0.02,
            warn_ratio=0.005,
        ),
        DQCheck(
            name="missing_momentti",
            description="Momentti tunnus ja nimi puuttuvat molemmat.",
            sql=(
                "SELECT COUNTIF("
                "NULLIF(TRIM(`Momentti_TunnusP`), '') IS NULL AND "
                "NULLIF(TRIM(`Momentti_sNimi`), '') IS NULL"
                f") FROM `{table_ref}`"
            ),
            fail_ratio=0.15,
            warn_ratio=0.05,
        ),
        DQCheck(
            name="invalid_nettokertyma_parse",
            description="Nettokertymä ei parsennu NUMERIC-arvoksi.",
            sql=(
                "SELECT COUNTIF("
                "NULLIF(TRIM(`Nettokertymä`), '') IS NOT NULL AND "
                f"{_numeric_parse_expr('`Nettokertymä`')} IS NULL"
                f") FROM `{table_ref}`"
            ),
            fail_ratio=0.02,
            warn_ratio=0.005,
        ),
        DQCheck(
            name="duplicate_natural_keys",
            description="Duplikaatit laajalla tapahtuma-avaimella (organisaatio + momentti + tili).",
            sql=(
                "WITH d AS ("
                "  SELECT "
                "    SAFE_CAST(`Vuosi` AS INT64) AS vuosi, "
                "    SAFE_CAST(`Kk` AS INT64) AS kk, "
                "    NULLIF(TRIM(`Ha_Tunnus`), '') AS ha_tunnus, "
                "    NULLIF(TRIM(`Tv_Tunnus`), '') AS tv_tunnus, "
                "    NULLIF(TRIM(`PaaluokkaOsasto_TunnusP`), '') AS paaluokkaosasto_tunnusp, "
                "    NULLIF(TRIM(`Luku_TunnusP`), '') AS luku_tunnusp, "
                "    NULLIF(TRIM(`Momentti_TunnusP`), '') AS momentti_tunnusp, "
                "    NULLIF(TRIM(`TakpT_TunnusP`), '') AS takpt_tunnusp, "
                "    NULLIF(TRIM(`TakpMrL_Tunnus`), '') AS alamomentti_tunnus, "
                "    NULLIF(TRIM(`Tililuokka_Tunnus`), '') AS tililuokka_tunnus, "
                "    NULLIF(TRIM(`Ylatiliryhma_Tunnus`), '') AS ylatiliryhma_tunnus, "
                "    NULLIF(TRIM(`Tiliryhma_Tunnus`), '') AS tiliryhma_tunnus, "
                "    NULLIF(TRIM(`Tililaji_Tunnus`), '') AS tililaji_tunnus, "
                "    NULLIF(TRIM(`LkpT_Tunnus`), '') AS lkpt_tunnus, "
                "    COUNT(*) c "
                f"  FROM `{table_ref}` "
                "  GROUP BY vuosi, kk, ha_tunnus, tv_tunnus, paaluokkaosasto_tunnusp, luku_tunnusp, "
                "    momentti_tunnusp, takpt_tunnusp, alamomentti_tunnus, tililuokka_tunnus, "
                "    ylatiliryhma_tunnus, tiliryhma_tunnus, tililaji_tunnus, lkpt_tunnus "
                "  HAVING COUNT(*) > 1"
                ") SELECT COALESCE(SUM(c - 1), 0) FROM d"
            ),
            fail_ratio=0.01,
            warn_ratio=0.001,
        ),
        DQCheck(
            name="missing_months_before_latest_year",
            description="Puuttuvia kuukausia ennen viimeisintä vuotta.",
            sql=(
                "WITH typed AS ("
                "  SELECT SAFE_CAST(`Vuosi` AS INT64) AS vuosi, SAFE_CAST(`Kk` AS INT64) AS kk "
                f"  FROM `{table_ref}`"
                "), bounds AS ("
                "  SELECT MIN(vuosi) min_year, MAX(vuosi) max_year FROM typed WHERE vuosi IS NOT NULL"
                "), expected AS ("
                "  SELECT y AS vuosi, m AS kk "
                "  FROM bounds, UNNEST(GENERATE_ARRAY(min_year, max_year - 1)) AS y, UNNEST(GENERATE_ARRAY(1, 12)) AS m"
                "), actual AS ("
                "  SELECT DISTINCT vuosi, kk FROM typed WHERE vuosi IS NOT NULL AND kk BETWEEN 1 AND 12"
                ") "
                "SELECT COUNT(*) FROM expected e LEFT JOIN actual a USING (vuosi, kk) WHERE a.vuosi IS NULL"
            ),
            fail_count=12,
            warn_count=0,
        ),
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run DQ checks for Budjettihaukka BigQuery tables.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--table", default=settings.table, help="Table id or fully-qualified project.dataset.table")
    parser.add_argument("--output-dir", default="docs/reports")
    parser.add_argument("--print-json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = bigquery.Client(project=args.project)

    table_ref = _table_ref(args.project, args.dataset, args.table)
    output_dir = (ROOT_DIR / args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    table = client.get_table(table_ref)
    columns = {f.name for f in table.schema}
    mode = _detect_table_mode(columns)

    summary_row = next(client.query(_summary_sql(table_ref, mode)).result(), None)
    if summary_row is None:
        logger.error("No data in table: %s", table_ref)
        return 1

    total_rows = int(summary_row.row_count or 0)
    distinct_rows = int(summary_row.distinct_rows or 0)
    rows_with_issues = int(summary_row.rows_with_issues or 0)
    min_period = str(summary_row.min_period) if summary_row.min_period else "-"
    max_period = str(summary_row.max_period) if summary_row.max_period else "-"

    if mode == "curated":
        freshness_sql = (
            f"SELECT COALESCE(DATE_DIFF(CURRENT_DATE(), MAX(period_date), DAY), 999999) FROM `{table_ref}`"
        )
    else:
        freshness_sql = (
            "WITH typed AS ("
            "  SELECT SAFE_CAST(`Vuosi` AS INT64) AS vuosi, SAFE_CAST(`Kk` AS INT64) AS kk "
            f"  FROM `{table_ref}`"
            ") "
            "SELECT COALESCE(DATE_DIFF(CURRENT_DATE(), MAX(DATE(vuosi, kk, 1)), DAY), 999999) "
            "FROM typed "
            "WHERE vuosi IS NOT NULL AND kk BETWEEN 1 AND 12"
        )
    freshness_days = int(_scalar(client, freshness_sql) or 0)

    checks = _build_checks(table_ref, mode)
    results: list[dict] = []
    has_fail = False
    has_warn = False

    for check in checks:
        failed_count = int(_scalar(client, check.sql) or 0)
        status, ratio = _check_status(failed_count, total_rows, check)
        has_fail = has_fail or (status == "FAIL")
        has_warn = has_warn or (status == "WARN")
        results.append(
            {
                "name": check.name,
                "status": status,
                "description": check.description,
                "failed_count": failed_count,
                "failed_ratio": ratio,
            }
        )

    freshness_status = "PASS"
    if freshness_days > 70:
        freshness_status = "FAIL"
        has_fail = True
    elif freshness_days > 40:
        freshness_status = "WARN"
        has_warn = True

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "project": args.project,
        "dataset": args.dataset,
        "table_ref": table_ref,
        "table_mode": mode,
        "summary": {
            "row_count": total_rows,
            "distinct_rows": distinct_rows,
            "rows_with_issues": rows_with_issues,
            "issue_row_ratio": (rows_with_issues / total_rows) if total_rows else 0.0,
            "min_period": min_period,
            "max_period": max_period,
            "freshness_days": freshness_days,
            "freshness_status": freshness_status,
        },
        "checks": results,
    }

    report_json = output_dir / f"data_quality_report_{ts}.json"
    report_md = output_dir / f"data_quality_report_{ts}.md"
    report_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines: list[str] = []
    lines.append("# BigQuery Data Quality Report")
    lines.append("")
    lines.append(f"- Generated (UTC): `{payload['generated_at_utc']}`")
    lines.append(f"- Table: `{table_ref}`")
    lines.append(f"- Mode: `{mode}`")
    lines.append(f"- Rows: `{total_rows}`")
    lines.append(f"- Distinct rows (method dependent): `{distinct_rows}`")
    lines.append(f"- Rows with quality issues: `{rows_with_issues}` ({payload['summary']['issue_row_ratio']:.2%})")
    lines.append(f"- Period: `{min_period}` -> `{max_period}`")
    lines.append(f"- Freshness: `{freshness_days}` days (`{freshness_status}`)")
    lines.append("")
    lines.append("| Check | Status | Failed | Ratio | Description |")
    lines.append("|---|---|---:|---:|---|")
    for result in results:
        lines.append(
            f"| `{result['name']}` | `{result['status']}` | {result['failed_count']} | {result['failed_ratio']:.2%} | {result['description']} |"
        )
    lines.append("")
    overall = "FAIL" if has_fail else ("WARN" if has_warn else "PASS")
    lines.append(f"## Overall: `{overall}`")
    report_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

    logger.info("Report JSON: %s", report_json)
    logger.info("Report MD: %s", report_md)
    logger.info("Overall status: %s", overall)

    if args.print_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print("\n".join(lines))

    return 2 if has_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())

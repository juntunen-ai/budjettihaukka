#!/usr/bin/env python3
"""Read-only operational audit for Budjettihaukka BigQuery assets."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings

SOURCE_LIST_URL = "https://api.tutkihallintoa.fi/valtiontalous/v1/budjettitalousvuosikuukausi"
CORE_OBJECTS = {
    "valtiontalous_raw",
    "valtiontalous_ingest_manifest",
    "valtiontalous_curated_dq_v",
    "valtiontalous_semantic_current",
    "valtiontalous_yearly_agg_v1",
    "official_macro_reference_v1",
    "analytics_fiscal_yearly_core_v1",
    "analytics_fiscal_yearly_v1",
    "analytics_metric_series_v1",
    "analytics_visualization_quality_v1",
}
MUNICIPAL_OBJECTS = {
    "municipal_finance_taxonomy_index_v1",
    "municipal_finance_catalog_v1",
    "municipal_finance_ktas_taxonomy_v1",
    "municipal_finance_ktas_core_v1",
    "municipal_finance_document_manifest_v1",
    "dim_municipal_finance_source_v1",
    "dim_municipal_finance_indicator_v1",
    "analytics_municipal_finance_catalog_v1",
    "analytics_municipal_budget_v1",
    "analytics_municipal_budget_revision_v1",
    "analytics_municipal_finance_coverage_v1",
    "analytics_municipal_finance_quality_v1",
}


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, text=True, capture_output=True)


def _bq_json(command: list[str]):
    result = _run(command)
    if result.returncode:
        raise RuntimeError(result.stderr or result.stdout or "bq command failed")
    return json.loads(result.stdout)


def _parse_source_periods(urls: list[str]) -> list[tuple[int, int]]:
    periods: set[tuple[int, int]] = set()
    for url in urls:
        match = re.search(r"/budjettitalous/(\d{4})/(\d{1,2})/", str(url))
        if match:
            periods.add((int(match.group(1)), int(match.group(2))))
    return sorted(periods)


def _period_lag_months(actual: tuple[int, int], expected: tuple[int, int]) -> int:
    return (expected[0] - actual[0]) * 12 + expected[1] - actual[1]


def _check_object(project: str, dataset: str, table_id: str, object_type: str) -> dict:
    sql = f"SELECT * FROM `{project}.{dataset}.{table_id}` LIMIT 0"
    result = _run([
        "bq", f"--project_id={project}", "query", "--nouse_legacy_sql", "--dry_run", sql
    ])
    return {
        "table_id": table_id,
        "type": object_type,
        "status": "PASS" if result.returncode == 0 else "FAIL",
        "error": "" if result.returncode == 0 else (result.stderr or result.stdout).strip()[:1000],
    }


def _latest_semantic_period(project: str, dataset: str, table: str) -> tuple[int, int]:
    sql = (
        f"SELECT MAX(SAFE_CAST(`Vuosi` AS INT64)) AS year, "
        f"MAX(IF(SAFE_CAST(`Vuosi` AS INT64) = max_year, SAFE_CAST(`Kk` AS INT64), NULL)) AS month "
        f"FROM `{project}.{dataset}.{table}`, "
        f"(SELECT MAX(SAFE_CAST(`Vuosi` AS INT64)) AS max_year FROM `{project}.{dataset}.{table}`)"
    )
    rows = _bq_json([
        "bq", f"--project_id={project}", "query", "--nouse_legacy_sql",
        "--format=prettyjson", "--max_rows=1", sql,
    ])
    if not rows:
        raise RuntimeError(f"No rows in {project}.{dataset}.{table}")
    return int(rows[0]["year"]), int(rows[0]["month"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit BigQuery objects, freshness and expiration without writes.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--semantic-table", default=settings.table)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--expiration-warning-days", type=int, default=21)
    parser.add_argument("--require-municipal", action="store_true")
    parser.add_argument("--skip-source-check", action="store_true")
    parser.add_argument("--json-output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    now = datetime.now(timezone.utc)
    dataset_ref = f"{args.project}:{args.dataset}"
    dataset_meta = _bq_json([
        "bq", f"--project_id={args.project}", "show", "--format=prettyjson", dataset_ref
    ])
    listed = _bq_json([
        "bq", f"--project_id={args.project}", "ls", "--max_results=1000",
        "--format=prettyjson", dataset_ref,
    ])
    table_ids = {item["tableReference"]["tableId"] for item in listed}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        object_checks = list(pool.map(
            lambda item: _check_object(
                args.project, args.dataset, item["tableReference"]["tableId"], item["type"]
            ),
            listed,
        ))

    missing_core = sorted(CORE_OBJECTS - table_ids)
    missing_municipal = sorted(MUNICIPAL_OBJECTS - table_ids)
    broken = [row for row in object_checks if row["status"] == "FAIL"]

    expiring = []
    for item in listed:
        raw = item.get("expirationTime")
        if not raw:
            continue
        expires_at = datetime.fromtimestamp(int(raw) / 1000, tz=timezone.utc)
        days = (expires_at - now).total_seconds() / 86400
        expiring.append({
            "table_id": item["tableReference"]["tableId"],
            "expires_at_utc": expires_at.isoformat(),
            "days_remaining": round(days, 1),
        })
    expiring.sort(key=lambda row: row["days_remaining"])
    expiration_warnings = [
        row for row in expiring if row["days_remaining"] <= args.expiration_warning_days
    ]

    freshness = {"status": "SKIP"}
    if not args.skip_source_check:
        semantic_period = _latest_semantic_period(args.project, args.dataset, args.semantic_table)
        response = requests.get(SOURCE_LIST_URL, timeout=120)
        response.raise_for_status()
        source_periods = _parse_source_periods(response.json())
        if not source_periods:
            raise RuntimeError("Official source list did not contain recognizable periods")
        source_period = source_periods[-1]
        lag = _period_lag_months(semantic_period, source_period)
        freshness = {
            "status": "PASS" if lag <= 0 else "FAIL",
            "semantic_period": f"{semantic_period[0]:04d}-{semantic_period[1]:02d}",
            "official_source_period": f"{source_period[0]:04d}-{source_period[1]:02d}",
            "lag_months": lag,
            "source_url": SOURCE_LIST_URL,
        }

    failures = []
    warnings = []
    if broken:
        failures.append(f"{len(broken)} BigQuery objects failed dry-run")
    if missing_core:
        failures.append(f"Missing core objects: {', '.join(missing_core)}")
    if freshness.get("status") == "FAIL":
        failures.append(f"Semantic data lags official source by {freshness['lag_months']} month(s)")
    if args.require_municipal and missing_municipal:
        failures.append(f"Missing municipal objects: {', '.join(missing_municipal)}")
    elif missing_municipal:
        warnings.append(f"Municipal mart is not deployed ({len(missing_municipal)} objects missing)")
    if expiration_warnings:
        warnings.append(
            f"{len(expiration_warnings)} objects expire within {args.expiration_warning_days} days"
        )

    payload = {
        "generated_at_utc": now.isoformat(),
        "status": "FAIL" if failures else ("WARN" if warnings else "PASS"),
        "project": args.project,
        "dataset": args.dataset,
        "dataset_location": dataset_meta.get("location"),
        "default_table_expiration_ms": dataset_meta.get("defaultTableExpirationMs"),
        "object_summary": {
            "count": len(listed),
            "passed": len(object_checks) - len(broken),
            "failed": len(broken),
        },
        "broken_objects": broken,
        "missing_core_objects": missing_core,
        "missing_municipal_objects": missing_municipal,
        "freshness": freshness,
        "expiration_warnings": expiration_warnings,
        "earliest_expirations": expiring[:10],
        "failures": failures,
        "warnings": warnings,
    }
    rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    print(rendered)
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(rendered + "\n", encoding="utf-8")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

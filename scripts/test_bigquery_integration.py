#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings
from utils.analysis_spec_utils import infer_analysis_spec
from utils.bigquery_utils import (
    _build_bigquery_fallback_sql,
    _build_yearly_agg_sql,
    enforce_sql_security,
    run_sql_query,
)
from utils.semantic_query_contracts import build_contract_sql


CONTRACT_QUESTIONS = [
    "Top 10 eniten kasvaneet momentit 2010-2024",
    "Näytä menojen vuosimuutos 2010-2024",
    "Näytä trendi hallinnonaloittain 2010-2024",
]

UNSUPPORTED_QUESTIONS = [
    "Top 10 eniten kasvaneet alamomentit 2010-2024",
]


FALLBACK_QUESTION = "Näytä menot kuukausittain hallinnonaloittain 2022-2024"


def _dry_run(client: bigquery.Client, sql: str) -> int:
    job = client.query(sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
    return int(job.total_bytes_processed or 0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run BigQuery integration checks for deterministic SQL paths.")
    parser.add_argument("--execute-sample", action="store_true", help="Execute one real sample query after dry-run checks.")
    args = parser.parse_args()

    if settings.use_google_sheets_demo:
        print("SKIP: data source is google_sheets demo")
        raise SystemExit(0)

    try:
        client = bigquery.Client(project=settings.project_id)
    except Exception as exc:
        print(f"SKIP: cannot initialize BigQuery client: {exc}")
        raise SystemExit(0)

    total = 0
    for question in CONTRACT_QUESTIONS:
        spec = infer_analysis_spec(question)
        sql = _build_yearly_agg_sql(question, spec)
        contract_name = "yearly_agg"

        secured_sql, sec_err = enforce_sql_security(sql)
        if sec_err:
            raise AssertionError(f"Security gate rejected contract SQL ({contract_name}): {sec_err}")

        bytes_processed = _dry_run(client, secured_sql)
        if bytes_processed > settings.max_query_bytes:
            raise AssertionError(
                f"Dry-run exceeds application byte cap: {bytes_processed} > {settings.max_query_bytes}"
            )
        print(f"OK dry-run contract={contract_name} bytes={bytes_processed}")
        total += 1

    for question in UNSUPPORTED_QUESTIONS:
        spec = infer_analysis_spec(question)
        sql, contract_name = build_contract_sql(spec, settings.full_table_id)
        if sql is not None or contract_name is not None:
            raise AssertionError(f"Unsupported alamomentti query produced SQL: {question}")
        try:
            _build_bigquery_fallback_sql(question, spec)
        except ValueError:
            pass
        else:
            raise AssertionError(f"Unsupported alamomentti query produced fallback SQL: {question}")
        print("OK fail-closed unsupported entity=alamomentti")
        total += 1

    fallback_sql = _build_bigquery_fallback_sql(FALLBACK_QUESTION)
    secured_fallback, sec_err = enforce_sql_security(fallback_sql)
    if sec_err:
        raise AssertionError(f"Security gate rejected fallback SQL: {sec_err}")
    bytes_processed = _dry_run(client, secured_fallback)
    print(f"OK dry-run fallback bytes={bytes_processed}")
    total += 1

    if args.execute_sample:
        results = run_sql_query(secured_fallback)
        print(f"Execute sample rows={len(results)}")

    print(f"Integration checks passed: {total}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Ingest Valtiokonttori Valtiontalous monthly CSVs into BigQuery.

Pipeline:
1. Reads monthly file URLs from Valtiokonttori endpoint.
2. Downloads CSV files and normalizes column names.
3. Loads all rows into a raw BigQuery table with source metadata.
4. Tracks progress in a manifest table for incremental reruns.
5. Rebuilds a curated typed/partitioned table for analytics.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import re
import sys
import time
import unicodedata
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config import settings

LIST_URL = "https://api.tutkihallintoa.fi/valtiontalous/v1/budjettitalousvuosikuukausi"

logger = logging.getLogger("valtiokonttori_ingest")


@dataclass(frozen=True)
class SourceFile:
    url: str
    year: int
    month: int


def _normalize_name(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"[^0-9a-zA-Z]+", "_", normalized).strip("_").lower()
    if not normalized:
        normalized = "col"
    if normalized[0].isdigit():
        normalized = f"col_{normalized}"
    return normalized


def _build_unique_normalized_names(original_names: list[str]) -> dict[str, str]:
    used: dict[str, int] = {}
    mapping: dict[str, str] = {}
    for original in original_names:
        base = _normalize_name(original)
        idx = used.get(base, 0)
        unique = base if idx == 0 else f"{base}_{idx+1}"
        used[base] = idx + 1
        mapping[original] = unique
    return mapping


def _parse_source_file(url: str) -> SourceFile | None:
    match = re.search(r"/budjettitalous/(\d{4})/(\d{1,2})/", url)
    if not match:
        return None
    return SourceFile(url=url, year=int(match.group(1)), month=int(match.group(2)))


def fetch_source_files(session: requests.Session, start_year: int, end_year: int) -> list[SourceFile]:
    resp = session.get(LIST_URL, timeout=120)
    resp.raise_for_status()
    items = resp.json()
    files: list[SourceFile] = []
    for item in items:
        parsed = _parse_source_file(item)
        if not parsed:
            continue
        if parsed.year < start_year or parsed.year > end_year:
            continue
        files.append(parsed)
    files.sort(key=lambda x: (x.year, x.month, x.url))
    return files


def _read_csv_header(session: requests.Session, url: str) -> list[str]:
    resp = session.get(url, timeout=120, stream=True)
    resp.raise_for_status()
    for line in resp.iter_lines(decode_unicode=True):
        if line:
            return next(pd.read_csv(io.StringIO(line + "\n"), nrows=0)).split(",")
    raise RuntimeError(f"No header line found for {url}")


def discover_column_mapping(session: requests.Session, source_files: list[SourceFile]) -> tuple[list[str], dict[str, str]]:
    originals_ordered: OrderedDict[str, None] = OrderedDict()
    for src in source_files:
        resp = session.get(src.url, timeout=120, stream=True)
        resp.raise_for_status()
        header_line = None
        for line in resp.iter_lines(decode_unicode=True):
            if line:
                header_line = line
                break
        if not header_line:
            continue
        if isinstance(header_line, bytes):
            header_line = header_line.decode("utf-8-sig", errors="ignore")
        cols = [c.strip() for c in header_line.split(",")]
        for c in cols:
            originals_ordered.setdefault(c, None)
    original_names = list(originals_ordered.keys())
    mapping = _build_unique_normalized_names(original_names)
    normalized_order = [mapping[col] for col in original_names]
    return normalized_order, mapping


def ensure_manifest_table(client: bigquery.Client, manifest_table_id: str) -> None:
    schema = [
        bigquery.SchemaField("source_url", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_year", "INT64"),
        bigquery.SchemaField("source_month", "INT64"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("row_count", "INT64"),
        bigquery.SchemaField("error", "STRING"),
        bigquery.SchemaField("processed_at", "TIMESTAMP"),
        bigquery.SchemaField("duration_sec", "FLOAT64"),
    ]
    try:
        client.get_table(manifest_table_id)
    except NotFound:
        table = bigquery.Table(manifest_table_id, schema=schema)
        client.create_table(table)
        logger.info("Created manifest table: %s", manifest_table_id)


def ensure_dataset_sandbox_defaults(client: bigquery.Client, dataset_id: str, expiration_days: int) -> None:
    dataset = client.get_dataset(dataset_id)
    target_ms = expiration_days * 24 * 60 * 60 * 1000
    changed = False
    if not dataset.default_table_expiration_ms or dataset.default_table_expiration_ms > target_ms:
        dataset.default_table_expiration_ms = target_ms
        changed = True
    if (
        not dataset.default_partition_expiration_ms
        or dataset.default_partition_expiration_ms > target_ms
    ):
        dataset.default_partition_expiration_ms = target_ms
        changed = True
    if changed:
        client.update_dataset(
            dataset,
            ["default_table_expiration_ms", "default_partition_expiration_ms"],
        )
        logger.info(
            "Updated dataset defaults for sandbox: %s (table/partition expiration %s days)",
            dataset_id,
            expiration_days,
        )


def get_success_urls(client: bigquery.Client, manifest_table_id: str) -> set[str]:
    query = f"""
        SELECT source_url
        FROM `{manifest_table_id}`
        WHERE status = 'SUCCESS'
    """
    try:
        rows = client.query(query).result()
        return {row.source_url for row in rows}
    except Exception:
        return set()


def insert_manifest_row(client: bigquery.Client, manifest_table_id: str, row: dict) -> None:
    errors = client.insert_rows_json(manifest_table_id, [row])
    if errors:
        logger.warning("Failed to insert manifest row: %s", errors)


def _load_dataframe_to_bigquery(
    client: bigquery.Client,
    df: pd.DataFrame,
    raw_table_id: str,
) -> None:
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    job = client.load_table_from_dataframe(df, raw_table_id, job_config=job_config)
    job.result()


def ingest_files(
    client: bigquery.Client,
    session: requests.Session,
    source_files: list[SourceFile],
    normalized_columns: list[str],
    original_to_normalized: dict[str, str],
    raw_table_id: str,
    manifest_table_id: str,
    max_files: int | None,
    force: bool,
) -> tuple[int, int]:
    processed_success = get_success_urls(client, manifest_table_id) if not force else set()
    todo = [f for f in source_files if (f.url not in processed_success)]
    if max_files is not None:
        todo = todo[:max_files]
    logger.info("Source files total=%s pending=%s", len(source_files), len(todo))

    loaded_files = 0
    loaded_rows = 0
    started_all = time.time()

    for idx, src in enumerate(todo, start=1):
        started = time.time()
        status = "SUCCESS"
        error = None
        row_count = 0
        try:
            resp = session.get(src.url, timeout=300)
            resp.raise_for_status()
            text = resp.text
            if not text.strip():
                df = pd.DataFrame(columns=normalized_columns)
            else:
                df_raw = pd.read_csv(io.StringIO(text), dtype=str)
                df_raw.columns = [original_to_normalized.get(c, _normalize_name(c)) for c in df_raw.columns]
                for col in normalized_columns:
                    if col not in df_raw.columns:
                        df_raw[col] = pd.NA
                df = df_raw[normalized_columns]

            df["source_url"] = src.url
            df["source_year"] = src.year
            df["source_month"] = src.month
            df["ingested_at"] = datetime.now(timezone.utc)
            row_count = len(df)
            if row_count > 0:
                _load_dataframe_to_bigquery(client, df, raw_table_id)
            loaded_files += 1
            loaded_rows += row_count
            logger.info(
                "[%s/%s] loaded year=%s month=%s rows=%s",
                idx,
                len(todo),
                src.year,
                src.month,
                row_count,
            )
        except Exception as exc:
            status = "ERROR"
            error = str(exc)[:2000]
            logger.error(
                "[%s/%s] failed year=%s month=%s url=%s error=%s",
                idx,
                len(todo),
                src.year,
                src.month,
                src.url,
                exc,
            )

        insert_manifest_row(
            client,
            manifest_table_id,
            {
                "source_url": src.url,
                "source_year": src.year,
                "source_month": src.month,
                "status": status,
                "row_count": row_count,
                "error": error,
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "duration_sec": round(time.time() - started, 3),
            },
        )

    logger.info(
        "Ingestion complete loaded_files=%s loaded_rows=%s duration_sec=%.1f",
        loaded_files,
        loaded_rows,
        time.time() - started_all,
    )
    return loaded_files, loaded_rows


def create_or_replace_curated_table(
    client: bigquery.Client,
    raw_table_id: str,
    curated_table_id: str,
    expiration_days: int,
) -> None:
    sql = f"""
    CREATE OR REPLACE TABLE `{curated_table_id}`
    OPTIONS (
      expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {expiration_days} DAY)
    )
    PARTITION BY period_date
    CLUSTER BY hallinnonala, kirjanpitoyksikko, momentti_tunnusp
    AS
    SELECT
      SAFE_CAST(vuosi AS INT64) AS vuosi,
      SAFE_CAST(kk AS INT64) AS kk,
      DATE(SAFE_CAST(vuosi AS INT64), SAFE_CAST(kk AS INT64), 1) AS period_date,
      NULLIF(ha_tunnus, '') AS ha_tunnus,
      NULLIF(hallinnonala, '') AS hallinnonala,
      NULLIF(tv_tunnus, '') AS tv_tunnus,
      NULLIF(kirjanpitoyksikko, '') AS kirjanpitoyksikko,
      NULLIF(paaluokkaosasto_tunnusp, '') AS paaluokkaosasto_tunnusp,
      NULLIF(paaluokkaosasto_snimi, '') AS paaluokkaosasto_snimi,
      NULLIF(luku_tunnusp, '') AS luku_tunnusp,
      NULLIF(luku_snimi, '') AS luku_snimi,
      NULLIF(momentti_tunnusp, '') AS momentti_tunnusp,
      NULLIF(momentti_snimi, '') AS momentti_snimi,
      NULLIF(takpt_tunnusp, '') AS takpt_tunnusp,
      NULLIF(takpt_snimi, '') AS takpt_snimi,
      NULLIF(takptr_snimi, '') AS takptr_snimi,
      NULLIF(takpmrl_tunnus, '') AS takpmrl_tunnus,
      NULLIF(takpmrl_snimi, '') AS takpmrl_snimi,
      NULLIF(takpt_netto, '') AS takpt_netto_raw,
      NULLIF(tililuokka_tunnus, '') AS tililuokka_tunnus,
      NULLIF(tililuokka_snimi, '') AS tililuokka_snimi,
      NULLIF(ylatiliryhma_tunnus, '') AS ylatiliryhma_tunnus,
      NULLIF(ylatiliryhma_snimi, '') AS ylatiliryhma_snimi,
      NULLIF(tiliryhma_tunnus, '') AS tiliryhma_tunnus,
      NULLIF(tiliryhma_snimi, '') AS tiliryhma_snimi,
      NULLIF(tililaji_tunnus, '') AS tililaji_tunnus,
      NULLIF(tililaji_snimi, '') AS tililaji_snimi,
      NULLIF(lkpt_tunnus, '') AS lkpt_tunnus,
      NULLIF(lkpt_snimi, '') AS lkpt_snimi,
      SAFE_CAST(alkuperainen_talousarvio AS NUMERIC) AS alkuperainen_talousarvio,
      SAFE_CAST(lisatalousarvio AS NUMERIC) AS lisatalousarvio,
      SAFE_CAST(voimassaoleva_talousarvio AS NUMERIC) AS voimassaoleva_talousarvio,
      SAFE_CAST(kaytettavissa AS NUMERIC) AS kaytettavissa,
      SAFE_CAST(alkusaldo AS NUMERIC) AS alkusaldo,
      SAFE_CAST(nettokertyma_ko_vuodelta AS NUMERIC) AS nettokertyma_ko_vuodelta,
      SAFE_CAST(nettokertymaaikvuossiirrt AS NUMERIC) AS nettokertymaaikvuossiirrt,
      SAFE_CAST(nettokertyma AS NUMERIC) AS nettokertyma,
      SAFE_CAST(loppusaldo AS NUMERIC) AS loppusaldo,
      SAFE_CAST(jakamatondb AS NUMERIC) AS jakamatondb,
      SAFE_CAST(jakamatonkr AS NUMERIC) AS jakamatonkr,
      source_url,
      SAFE_CAST(source_year AS INT64) AS source_year,
      SAFE_CAST(source_month AS INT64) AS source_month,
      SAFE_CAST(ingested_at AS TIMESTAMP) AS ingested_at
    FROM `{raw_table_id}`
    WHERE SAFE_CAST(vuosi AS INT64) IS NOT NULL
      AND SAFE_CAST(kk AS INT64) BETWEEN 1 AND 12
    """
    client.query(sql).result()
    logger.info("Created/updated curated table: %s", curated_table_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--raw-table", default="valtiontalous_raw")
    parser.add_argument("--manifest-table", default="valtiontalous_ingest_manifest")
    parser.add_argument("--curated-table", default="valtiontalous_curated")
    parser.add_argument("--start-year", type=int, default=1998)
    parser.add_argument("--end-year", type=int, default=datetime.now(timezone.utc).year)
    parser.add_argument("--max-files", type=int, default=None)
    parser.add_argument("--force", action="store_true", help="Reprocess all files, ignore manifest SUCCESS")
    parser.add_argument("--skip-curated", action="store_true")
    parser.add_argument(
        "--sandbox-expiration-days",
        type=int,
        default=59,
        help="Default table/partition expiration for sandbox projects",
    )
    parser.add_argument(
        "--column-map-out",
        default="data/valtiokonttori_column_map.json",
        help="Write original->normalized column map here",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    client = bigquery.Client(project=args.project)
    dataset_id = f"{args.project}.{args.dataset}"
    raw_table_id = f"{dataset_id}.{args.raw_table}"
    manifest_table_id = f"{dataset_id}.{args.manifest_table}"
    curated_table_id = f"{dataset_id}.{args.curated_table}"

    session = requests.Session()
    session.headers.update({"User-Agent": "budjettihaukka-ingest/1.0"})

    source_files = fetch_source_files(session, args.start_year, args.end_year)
    if not source_files:
        logger.error("No source files found in year range %s-%s.", args.start_year, args.end_year)
        return 1

    logger.info(
        "Discovered %s source files for years %s-%s.",
        len(source_files),
        args.start_year,
        args.end_year,
    )
    ensure_dataset_sandbox_defaults(client, dataset_id, args.sandbox_expiration_days)

    normalized_columns, original_to_normalized = discover_column_mapping(session, source_files)
    Path(args.column_map_out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.column_map_out).write_text(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "source_file_count": len(source_files),
                "column_count": len(normalized_columns),
                "columns": original_to_normalized,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    logger.info("Column map written to %s (%s columns)", args.column_map_out, len(normalized_columns))

    ensure_manifest_table(client, manifest_table_id)
    loaded_files, loaded_rows = ingest_files(
        client=client,
        session=session,
        source_files=source_files,
        normalized_columns=normalized_columns,
        original_to_normalized=original_to_normalized,
        raw_table_id=raw_table_id,
        manifest_table_id=manifest_table_id,
        max_files=args.max_files,
        force=args.force,
    )

    if not args.skip_curated:
        create_or_replace_curated_table(
            client,
            raw_table_id,
            curated_table_id,
            expiration_days=args.sandbox_expiration_days,
        )

    logger.info(
        "Done. raw_table=%s curated_table=%s loaded_files=%s loaded_rows=%s",
        raw_table_id,
        curated_table_id,
        loaded_files,
        loaded_rows,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

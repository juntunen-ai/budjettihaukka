#!/usr/bin/env python3
"""Backfill Valtiokonttori monthly CSV data to BigQuery via bq CLI auth."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import shlex
import subprocess
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import requests

LIST_URL = "https://api.tutkihallintoa.fi/valtiontalous/v1/budjettitalousvuosikuukausi"

logger = logging.getLogger("vt_bqcli_ingest")


@dataclass(frozen=True)
class SourceFile:
    url: str
    year: int
    month: int


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    logger.debug("RUN: %s", shlex.join(cmd))
    return subprocess.run(cmd, check=check, text=True, capture_output=True)


def parse_source_file(url: str) -> SourceFile | None:
    m = re.search(r"/budjettitalous/(\d{4})/(\d{1,2})/", url)
    if not m:
        return None
    return SourceFile(url=url, year=int(m.group(1)), month=int(m.group(2)))


def fetch_sources(start_year: int, end_year: int) -> list[SourceFile]:
    resp = requests.get(LIST_URL, timeout=120)
    resp.raise_for_status()
    files = []
    for item in resp.json():
        src = parse_source_file(item)
        if not src:
            continue
        if start_year <= src.year <= end_year:
            files.append(src)
    files.sort(key=lambda x: (x.year, x.month, x.url))
    return files


def read_header(url: str) -> list[str]:
    resp = requests.get(url, timeout=120, stream=True)
    resp.raise_for_status()
    for line in resp.iter_lines(decode_unicode=True):
        if not line:
            continue
        if isinstance(line, bytes):
            line = line.decode("utf-8-sig", errors="ignore")
        return [c.strip() for c in line.split(",")]
    raise RuntimeError(f"No header found: {url}")


def build_union_headers(sources: list[SourceFile]) -> list[str]:
    seen = set()
    ordered: list[str] = []
    for src in sources:
        for col in read_header(src.url):
            if col not in seen:
                seen.add(col)
                ordered.append(col)
    return ordered


def ensure_dataset(project: str, dataset: str, location: str, expiration_sec: int) -> None:
    cmd = [
        "bq",
        f"--project_id={project}",
        "mk",
        "--dataset",
        f"--location={location}",
        f"--default_table_expiration={expiration_sec}",
        dataset,
    ]
    res = run(cmd, check=False)
    if res.returncode == 0:
        logger.info("Created dataset %s:%s", project, dataset)
        return
    out = f"{res.stdout}\n{res.stderr}".lower()
    if "already exists" in out or "already exists:" in out:
        logger.info("Dataset exists %s:%s", project, dataset)
        return
    raise RuntimeError(f"Failed to create dataset {project}:{dataset}: {res.stderr or res.stdout}")


def load_success_urls(manifest_path: Path) -> set[str]:
    if not manifest_path.exists():
        return set()
    ok = set()
    with manifest_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            if row.get("status") == "SUCCESS":
                ok.add(row["url"])
    return ok


def append_manifest(manifest_path: Path, row: dict) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def download_to_temp(url: str) -> str:
    fd, path = tempfile.mkstemp(prefix="vt_month_", suffix=".csv")
    Path(path).unlink(missing_ok=True)
    with requests.get(url, timeout=300, stream=True) as resp:
        resp.raise_for_status()
        with open(path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    return path


def bq_load_csv(
    project: str,
    dataset: str,
    table: str,
    file_path: str,
    schema_json: str,
    replace: bool,
) -> None:
    cmd = [
        "bq",
        f"--project_id={project}",
        "load",
        "--source_format=CSV",
        "--skip_leading_rows=1",
        "--allow_jagged_rows",
        "--allow_quoted_newlines",
        "--source_column_match=NAME",
        f"{dataset}.{table}",
        file_path,
        schema_json,
    ]
    if replace:
        cmd.insert(3, "--replace")
    run(cmd, check=True)


def table_exists(project: str, dataset: str, table: str) -> bool:
    cmd = ["bq", f"--project_id={project}", "show", "--format=none", f"{dataset}.{table}"]
    return run(cmd, check=False).returncode == 0


def create_or_replace_curated(project: str, dataset: str, raw_table: str, curated_table: str, expiration_days: int) -> None:
    sql = f"""
    CREATE OR REPLACE TABLE `{project}.{dataset}.{curated_table}`
    PARTITION BY period_date
    CLUSTER BY hallinnonala, kirjanpitoyksikko, momentti_tunnusp
    OPTIONS (
      expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {expiration_days} DAY)
    )
    AS
    SELECT
      SAFE_CAST(`Vuosi` AS INT64) AS vuosi,
      SAFE_CAST(`Kk` AS INT64) AS kk,
      DATE(SAFE_CAST(`Vuosi` AS INT64), SAFE_CAST(`Kk` AS INT64), 1) AS period_date,
      NULLIF(`Ha_Tunnus`, '') AS ha_tunnus,
      NULLIF(`Hallinnonala`, '') AS hallinnonala,
      NULLIF(`Tv_Tunnus`, '') AS tv_tunnus,
      NULLIF(`Kirjanpitoyksikkö`, '') AS kirjanpitoyksikko,
      NULLIF(`PaaluokkaOsasto_TunnusP`, '') AS paaluokkaosasto_tunnusp,
      NULLIF(`PaaluokkaOsasto_sNimi`, '') AS paaluokkaosasto_snimi,
      NULLIF(`Luku_TunnusP`, '') AS luku_tunnusp,
      NULLIF(`Luku_sNimi`, '') AS luku_snimi,
      NULLIF(`Momentti_TunnusP`, '') AS momentti_tunnusp,
      NULLIF(`Momentti_sNimi`, '') AS momentti_snimi,
      NULLIF(`TakpT_TunnusP`, '') AS takpt_tunnusp,
      NULLIF(`TakpT_sNimi`, '') AS takpt_snimi,
      NULLIF(`TakpTr_sNimi`, '') AS takptr_snimi,
      NULLIF(`TakpMrL_Tunnus`, '') AS takpmrl_tunnus,
      NULLIF(`TakpMrL_sNimi`, '') AS takpmrl_snimi,
      NULLIF(`TakpT_Netto`, '') AS takpt_netto_raw,
      NULLIF(`Tililuokka_Tunnus`, '') AS tililuokka_tunnus,
      NULLIF(`Tililuokka_sNimi`, '') AS tililuokka_snimi,
      NULLIF(`Ylatiliryhma_Tunnus`, '') AS ylatiliryhma_tunnus,
      NULLIF(`Ylatiliryhma_sNimi`, '') AS ylatiliryhma_snimi,
      NULLIF(`Tiliryhma_Tunnus`, '') AS tiliryhma_tunnus,
      NULLIF(`Tiliryhma_sNimi`, '') AS tiliryhma_snimi,
      NULLIF(`Tililaji_Tunnus`, '') AS tililaji_tunnus,
      NULLIF(`Tililaji_sNimi`, '') AS tililaji_snimi,
      NULLIF(`LkpT_Tunnus`, '') AS lkpt_tunnus,
      NULLIF(`LkpT_sNimi`, '') AS lkpt_snimi,
      SAFE_CAST(`Alkuperäinen_talousarvio` AS NUMERIC) AS alkuperainen_talousarvio,
      SAFE_CAST(`Lisätalousarvio` AS NUMERIC) AS lisatalousarvio,
      SAFE_CAST(`Voimassaoleva_talousarvio` AS NUMERIC) AS voimassaoleva_talousarvio,
      SAFE_CAST(`Käytettävissä` AS NUMERIC) AS kaytettavissa,
      SAFE_CAST(`Alkusaldo` AS NUMERIC) AS alkusaldo,
      SAFE_CAST(`Nettokertymä_ko_vuodelta` AS NUMERIC) AS nettokertyma_ko_vuodelta,
      SAFE_CAST(`NettoKertymaAikVuosSiirrt` AS NUMERIC) AS nettokertymaaikvuossiirrt,
      SAFE_CAST(`Nettokertymä` AS NUMERIC) AS nettokertyma,
      SAFE_CAST(`Loppusaldo` AS NUMERIC) AS loppusaldo,
      SAFE_CAST(`JakamatonDb` AS NUMERIC) AS jakamatondb,
      SAFE_CAST(`JakamatonKr` AS NUMERIC) AS jakamatonkr
    FROM `{project}.{dataset}.{raw_table}`
    WHERE SAFE_CAST(`Vuosi` AS INT64) IS NOT NULL
      AND SAFE_CAST(`Kk` AS INT64) BETWEEN 1 AND 12
    """
    cmd = [
        "bq",
        f"--project_id={project}",
        "query",
        "--use_legacy_sql=false",
        "--max_rows=0",
        sql,
    ]
    run(cmd, check=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--project", default="valtion-budjetti-data")
    p.add_argument("--dataset", default="valtiodata_ingest_tmp_20260308")
    p.add_argument("--location", default="EU")
    p.add_argument("--raw-table", default="valtiontalous_raw")
    p.add_argument("--curated-table", default="valtiontalous_curated")
    p.add_argument("--start-year", type=int, default=1998)
    p.add_argument("--end-year", type=int, default=2025)
    p.add_argument("--max-files", type=int, default=None)
    p.add_argument("--force", action="store_true")
    p.add_argument("--recreate-raw", action="store_true")
    p.add_argument("--skip-curated", action="store_true")
    p.add_argument("--expiration-days", type=int, default=59)
    p.add_argument("--manifest-path", default="data/valtiokonttori_bqcli_manifest.jsonl")
    p.add_argument("--schema-path", default="data/valtiokonttori_bq_schema.json")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    ensure_dataset(
        project=args.project,
        dataset=args.dataset,
        location=args.location,
        expiration_sec=args.expiration_days * 24 * 60 * 60,
    )

    sources = fetch_sources(args.start_year, args.end_year)
    if not sources:
        logger.error("No source files found.")
        return 1
    logger.info("Discovered %s files (%s-%s).", len(sources), args.start_year, args.end_year)

    headers = build_union_headers(sources)
    schema = [{"name": h, "type": "STRING", "mode": "NULLABLE"} for h in headers]
    schema_path = Path(args.schema_path)
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    schema_path.write_text(json.dumps(schema, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Schema prepared with %s columns -> %s", len(headers), schema_path)

    manifest_path = Path(args.manifest_path)
    done = set() if args.force else load_success_urls(manifest_path)
    pending = [s for s in sources if s.url not in done]
    if args.max_files is not None:
        pending = pending[: args.max_files]
    logger.info("Pending files: %s", len(pending))
    if not pending:
        logger.info("Nothing to ingest.")
        return 0

    first_replace = args.recreate_raw or (not table_exists(args.project, args.dataset, args.raw_table))
    loaded_rows = 0
    started_all = time.time()
    for idx, src in enumerate(pending, start=1):
        started = time.time()
        tmp_file = None
        status = "SUCCESS"
        err = None
        rows = None
        try:
            tmp_file = download_to_temp(src.url)
            with open(tmp_file, newline="", encoding="utf-8") as f:
                row_count = sum(1 for _ in f) - 1
            rows = max(row_count, 0)
            bq_load_csv(
                project=args.project,
                dataset=args.dataset,
                table=args.raw_table,
                file_path=tmp_file,
                schema_json=str(schema_path),
                replace=first_replace,
            )
            first_replace = False
            loaded_rows += rows
            logger.info(
                "[%s/%s] loaded %s-%02d rows=%s",
                idx,
                len(pending),
                src.year,
                src.month,
                rows,
            )
        except Exception as exc:
            status = "ERROR"
            err = str(exc)[:2000]
            logger.error("[%s/%s] failed %s-%02d: %s", idx, len(pending), src.year, src.month, exc)
        finally:
            if tmp_file:
                Path(tmp_file).unlink(missing_ok=True)
            append_manifest(
                manifest_path,
                {
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                    "url": src.url,
                    "year": src.year,
                    "month": src.month,
                    "status": status,
                    "rows": rows,
                    "duration_sec": round(time.time() - started, 3),
                    "error": err,
                },
            )

    logger.info(
        "Ingestion finished: files=%s rows=%s duration_sec=%.1f",
        len(pending),
        loaded_rows,
        time.time() - started_all,
    )

    if not args.skip_curated:
        create_or_replace_curated(
            project=args.project,
            dataset=args.dataset,
            raw_table=args.raw_table,
            curated_table=args.curated_table,
            expiration_days=args.expiration_days,
        )
        logger.info("Curated table refreshed: %s.%s.%s", args.project, args.dataset, args.curated_table)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

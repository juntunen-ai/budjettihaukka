#!/usr/bin/env python3
"""Quick sampling-based comparison between Valtiokonttori API CSVs and BigQuery."""

from __future__ import annotations

import argparse
import csv
import io
import json
import random
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from google.cloud import bigquery

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config import settings

LIST_URL = "https://api.tutkihallintoa.fi/valtiontalous/v1/budjettitalousvuosikuukausi"
COMPARE_COLUMNS = [
    "Vuosi",
    "Kk",
    "Ha_Tunnus",
    "Hallinnonala",
    "Tv_Tunnus",
    "Kirjanpitoyksikkö",
    "PaaluokkaOsasto_TunnusP",
    "PaaluokkaOsasto_sNimi",
    "Luku_TunnusP",
    "Luku_sNimi",
    "Momentti_TunnusP",
    "Momentti_sNimi",
    "TakpMrL_Tunnus",
    "TakpMrL_sNimi",
    "Nettokertymä",
]
HIERARCHY_LEVELS = {
    "hallinnonala": ["Ha_Tunnus", "Hallinnonala"],
    "kirjanpitoyksikko": ["Tv_Tunnus", "Kirjanpitoyksikkö"],
    "paaluokkaosasto": ["PaaluokkaOsasto_TunnusP", "PaaluokkaOsasto_sNimi"],
    "luku": ["Luku_TunnusP", "Luku_sNimi"],
    "momentti": ["Momentti_TunnusP", "Momentti_sNimi"],
    "alamomentti": ["TakpMrL_Tunnus", "TakpMrL_sNimi"],
}


@dataclass(frozen=True)
class SourceFile:
    url: str
    year: int
    month: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare Valtiokonttori API sample to BigQuery.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--table", default=settings.table)
    parser.add_argument("--sample-size", type=int, default=6)
    parser.add_argument("--seed", type=int, default=20260314)
    parser.add_argument("--start-year", type=int, default=1998)
    parser.add_argument("--end-year", type=int, default=2024)
    parser.add_argument("--output-dir", default="docs/reports")
    parser.add_argument("--print-json", action="store_true")
    return parser.parse_args()


def _parse_source_file(url: str) -> SourceFile | None:
    match = re.search(r"/budjettitalous/(\d{4})/(\d{1,2})/", url)
    if not match:
        return None
    return SourceFile(url=url, year=int(match.group(1)), month=int(match.group(2)))


def fetch_source_files(session: requests.Session, start_year: int, end_year: int) -> list[SourceFile]:
    resp = session.get(LIST_URL, timeout=120)
    resp.raise_for_status()
    files: list[SourceFile] = []
    for item in resp.json():
        parsed = _parse_source_file(item)
        if not parsed:
            continue
        if start_year <= parsed.year <= end_year:
            files.append(parsed)
    files.sort(key=lambda x: (x.year, x.month, x.url))
    return files


def stratified_sample(files: list[SourceFile], sample_size: int, seed: int) -> list[SourceFile]:
    if not files:
        return []
    rnd = random.Random(seed)
    if sample_size >= len(files):
        return list(files)

    years = sorted({src.year for src in files})
    min_year, max_year = years[0], years[-1]
    span = max(1, max_year - min_year + 1)
    buckets: list[list[SourceFile]] = [[], [], []]
    for src in files:
        pos = (src.year - min_year) / span
        if pos < 1 / 3:
            buckets[0].append(src)
        elif pos < 2 / 3:
            buckets[1].append(src)
        else:
            buckets[2].append(src)

    picks: list[SourceFile] = []
    base = sample_size // 3
    remainder = sample_size % 3
    for idx, bucket in enumerate(buckets):
        target = base + (1 if idx < remainder else 0)
        if not bucket:
            continue
        picks.extend(rnd.sample(bucket, min(target, len(bucket))))

    missing = sample_size - len(picks)
    if missing > 0:
        remaining = [src for src in files if src not in picks]
        picks.extend(rnd.sample(remaining, min(missing, len(remaining))))

    picks = sorted(picks, key=lambda x: (x.year, x.month))
    return picks[:sample_size]


def _normalize_text(value) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _normalize_numeric(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace("\u00a0", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace("−", "-", regex=False)
        .str.replace(",", ".", regex=False)
    )
    return pd.to_numeric(cleaned, errors="coerce").round(2)


def load_api_month(session: requests.Session, src: SourceFile) -> pd.DataFrame:
    resp = session.get(src.url, timeout=300)
    resp.raise_for_status()
    reader = csv.DictReader(io.StringIO(resp.text))
    df = pd.DataFrame(reader)
    for col in COMPARE_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[COMPARE_COLUMNS].copy()
    for col in COMPARE_COLUMNS:
        if col == "Nettokertymä":
            df[col] = _normalize_numeric(df[col])
        elif col in {"Vuosi", "Kk"}:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        else:
            df[col] = df[col].map(_normalize_text)
    return df


def load_bq_month(client: bigquery.Client, table_ref: str, src: SourceFile) -> pd.DataFrame:
    sql = f"""
    SELECT
      `Vuosi`,
      `Kk`,
      `Ha_Tunnus`,
      `Hallinnonala`,
      `Tv_Tunnus`,
      `Kirjanpitoyksikkö`,
      `PaaluokkaOsasto_TunnusP`,
      `PaaluokkaOsasto_sNimi`,
      `Luku_TunnusP`,
      `Luku_sNimi`,
      `Momentti_TunnusP`,
      `Momentti_sNimi`,
      `TakpMrL_Tunnus`,
      `TakpMrL_sNimi`,
      `Nettokertymä`
    FROM `{table_ref}`
    WHERE `Vuosi` = {src.year}
      AND `Kk` = {src.month}
    """
    df = client.query(sql).result().to_dataframe(create_bqstorage_client=False)
    for col in COMPARE_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[COMPARE_COLUMNS].copy()
    for col in COMPARE_COLUMNS:
        if col == "Nettokertymä":
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)
        elif col in {"Vuosi", "Kk"}:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        else:
            df[col] = df[col].map(_normalize_text)
    return df


def _tuple_rows(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    filled = df[cols].copy()
    for col in cols:
        if col == "Nettokertymä":
            filled[col] = filled[col].map(lambda v: None if pd.isna(v) else float(v))
        else:
            filled[col] = filled[col].where(filled[col].notna(), None)
    return filled.apply(lambda row: tuple(row.values.tolist()), axis=1)


def compare_full_rows(api_df: pd.DataFrame, bq_df: pd.DataFrame) -> dict:
    api_rows = _tuple_rows(api_df, COMPARE_COLUMNS)
    bq_rows = _tuple_rows(bq_df, COMPARE_COLUMNS)
    api_counts = api_rows.value_counts(dropna=False)
    bq_counts = bq_rows.value_counts(dropna=False)
    all_keys = set(api_counts.index).union(set(bq_counts.index))
    only_api = 0
    only_bq = 0
    mismatched = 0
    for key in all_keys:
        a = int(api_counts.get(key, 0))
        b = int(bq_counts.get(key, 0))
        if a > b:
            only_api += a - b
        elif b > a:
            only_bq += b - a
        if a != b:
            mismatched += abs(a - b)
    exact_match = only_api == 0 and only_bq == 0
    return {
        "api_rows": int(len(api_df)),
        "bq_rows": int(len(bq_df)),
        "api_sum_eur": round(float(api_df["Nettokertymä"].sum()), 2),
        "bq_sum_eur": round(float(bq_df["Nettokertymä"].sum()), 2),
        "row_multiset_exact_match": exact_match,
        "rows_only_in_api": only_api,
        "rows_only_in_bigquery": only_bq,
        "row_delta_total": mismatched,
    }


def compare_level(api_df: pd.DataFrame, bq_df: pd.DataFrame, level_cols: list[str]) -> dict:
    key_cols = list(level_cols)
    agg_cols = key_cols + ["Nettokertymä"]
    api_grouped = (
        api_df[agg_cols]
        .groupby(key_cols, dropna=False, as_index=False)["Nettokertymä"]
        .sum()
        .rename(columns={"Nettokertymä": "api_sum"})
    )
    bq_grouped = (
        bq_df[agg_cols]
        .groupby(key_cols, dropna=False, as_index=False)["Nettokertymä"]
        .sum()
        .rename(columns={"Nettokertymä": "bq_sum"})
    )
    merged = api_grouped.merge(bq_grouped, on=key_cols, how="outer")
    merged["api_sum"] = merged["api_sum"].fillna(0.0).round(2)
    merged["bq_sum"] = merged["bq_sum"].fillna(0.0).round(2)
    merged["diff"] = (merged["api_sum"] - merged["bq_sum"]).round(2)
    groups_only_api = int(((merged["api_sum"] != 0) & (merged["bq_sum"] == 0)).sum())
    groups_only_bq = int(((merged["api_sum"] == 0) & (merged["bq_sum"] != 0)).sum())
    equal_groups = int((merged["diff"].abs() <= 0.01).sum())
    return {
        "api_groups": int(len(api_grouped)),
        "bq_groups": int(len(bq_grouped)),
        "joined_groups": int(len(merged)),
        "equal_sum_groups": equal_groups,
        "equal_sum_ratio": (equal_groups / len(merged)) if len(merged) else 1.0,
        "groups_only_in_api": groups_only_api,
        "groups_only_in_bigquery": groups_only_bq,
        "total_abs_diff_eur": round(float(merged["diff"].abs().sum()), 2),
        "max_abs_diff_eur": round(float(merged["diff"].abs().max() if len(merged) else 0.0), 2),
    }


def summarize_period(src: SourceFile, api_df: pd.DataFrame, bq_df: pd.DataFrame) -> dict:
    summary = {
        "period": f"{src.year}-{src.month:02d}",
        "source_url": src.url,
        "full_row_compare": compare_full_rows(api_df, bq_df),
        "levels": {},
    }
    for level, cols in HIERARCHY_LEVELS.items():
        summary["levels"][level] = compare_level(api_df, bq_df, cols)
    return summary


def aggregate_results(period_results: list[dict]) -> dict:
    level_summary: dict[str, dict] = {}
    for level in HIERARCHY_LEVELS:
        equal_groups = 0
        joined_groups = 0
        only_api = 0
        only_bq = 0
        total_abs_diff = 0.0
        max_abs_diff = 0.0
        for result in period_results:
            level_result = result["levels"][level]
            equal_groups += level_result["equal_sum_groups"]
            joined_groups += level_result["joined_groups"]
            only_api += level_result["groups_only_in_api"]
            only_bq += level_result["groups_only_in_bigquery"]
            total_abs_diff += level_result["total_abs_diff_eur"]
            max_abs_diff = max(max_abs_diff, level_result["max_abs_diff_eur"])
        level_summary[level] = {
            "equal_sum_groups": equal_groups,
            "joined_groups": joined_groups,
            "equal_sum_ratio": (equal_groups / joined_groups) if joined_groups else 1.0,
            "groups_only_in_api": only_api,
            "groups_only_in_bigquery": only_bq,
            "total_abs_diff_eur": round(total_abs_diff, 2),
            "max_abs_diff_eur": round(max_abs_diff, 2),
        }

    row_exact_matches = sum(1 for result in period_results if result["full_row_compare"]["row_multiset_exact_match"])
    return {
        "sample_periods": len(period_results),
        "full_row_exact_match_periods": row_exact_matches,
        "full_row_exact_match_ratio": (row_exact_matches / len(period_results)) if period_results else 0.0,
        "levels": level_summary,
    }


def render_markdown(payload: dict) -> str:
    lines: list[str] = []
    lines.append("# Valtiokonttori API vs BigQuery Sample Comparison")
    lines.append("")
    lines.append(f"- Generated (UTC): `{payload['generated_at_utc']}`")
    lines.append(f"- BigQuery table: `{payload['table_ref']}`")
    lines.append(f"- Sample method: `{payload['method']}`")
    lines.append(f"- Sample seed: `{payload['seed']}`")
    lines.append(f"- Sample periods: `{', '.join(p['period'] for p in payload['period_results'])}`")
    lines.append("")
    lines.append("## Aggregate results")
    lines.append("")
    lines.append(
        f"- Full row exact matches: `{payload['aggregate']['full_row_exact_match_periods']}/{payload['aggregate']['sample_periods']}` "
        f"({payload['aggregate']['full_row_exact_match_ratio']:.0%})"
    )
    lines.append("")
    lines.append("| Level | Equal groups | Joined groups | Match ratio | Only in API | Only in BigQuery | Total abs diff EUR | Max abs diff EUR |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for level, result in payload["aggregate"]["levels"].items():
        lines.append(
            f"| `{level}` | {result['equal_sum_groups']} | {result['joined_groups']} | {result['equal_sum_ratio']:.2%} | "
            f"{result['groups_only_in_api']} | {result['groups_only_in_bigquery']} | "
            f"{result['total_abs_diff_eur']:.2f} | {result['max_abs_diff_eur']:.2f} |"
        )
    lines.append("")
    lines.append("## Period details")
    lines.append("")
    for result in payload["period_results"]:
        lines.append(f"### {result['period']}")
        full_row = result["full_row_compare"]
        lines.append(
            f"- Full row exact match: `{full_row['row_multiset_exact_match']}` "
            f"(api rows `{full_row['api_rows']}`, bq rows `{full_row['bq_rows']}`, "
            f"api sum `{full_row['api_sum_eur']:.2f}`, bq sum `{full_row['bq_sum_eur']:.2f}`)"
        )
        for level, level_result in result["levels"].items():
            lines.append(
                f"  - `{level}`: match `{level_result['equal_sum_ratio']:.2%}`, "
                f"only_api `{level_result['groups_only_in_api']}`, only_bq `{level_result['groups_only_in_bigquery']}`, "
                f"total_abs_diff `{level_result['total_abs_diff_eur']:.2f}`"
            )
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def main() -> int:
    args = parse_args()
    session = requests.Session()
    client = bigquery.Client(project=args.project)
    table_ref = f"{args.project}.{args.dataset}.{args.table}"
    source_files = fetch_source_files(session, args.start_year, args.end_year)
    sample = stratified_sample(source_files, args.sample_size, args.seed)

    period_results: list[dict] = []
    for src in sample:
        api_df = load_api_month(session, src)
        bq_df = load_bq_month(client, table_ref, src)
        period_results.append(summarize_period(src, api_df, bq_df))

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "table_ref": table_ref,
        "method": "stratified_random_sample_by_period",
        "seed": args.seed,
        "period_results": period_results,
        "aggregate": aggregate_results(period_results),
    }

    output_dir = (ROOT_DIR / args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"api_vs_bigquery_sample_{ts}.json"
    md_path = output_dir / f"api_vs_bigquery_sample_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(payload), encoding="utf-8")

    if args.print_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(render_markdown(payload))
        print(f"JSON: {json_path}")
        print(f"MD: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

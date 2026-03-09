import logging
import re
import sqlite3
import unicodedata
from functools import lru_cache

import pandas as pd

from config import settings

logger = logging.getLogger(__name__)


def _sheet_csv_url(sheet_id: str, tab_name: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={tab_name}"


def _normalize_column_name(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"[^0-9a-zA-Z]+", "_", normalized).strip("_").lower()
    if not normalized:
        normalized = "col"
    if normalized[0].isdigit():
        normalized = f"col_{normalized}"
    return normalized


def _normalize_column_names(columns: list[str]) -> tuple[list[str], dict[str, str]]:
    used: dict[str, int] = {}
    final_names: list[str] = []
    original_by_normalized: dict[str, str] = {}

    for col in columns:
        base = _normalize_column_name(col)
        idx = used.get(base, 0)
        if idx == 0:
            final_name = base
        else:
            final_name = f"{base}_{idx+1}"
        used[base] = idx + 1
        final_names.append(final_name)
        original_by_normalized[final_name] = col

    return final_names, original_by_normalized


def _coerce_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype != object:
            continue
        converted = pd.to_numeric(df[col], errors="coerce")
        if len(df) == 0:
            continue
        ratio = converted.notna().mean()
        if ratio >= 0.98:
            df[col] = converted
    return df


@lru_cache(maxsize=1)
def get_demo_dataframe() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    missing_years: list[str] = []

    for year, sheet_id in settings.demo_sheet_ids.items():
        sheet_id = (sheet_id or "").strip()
        if not sheet_id:
            missing_years.append(year)
            continue

        url = _sheet_csv_url(sheet_id, f"data_{year}")
        logger.info("Loading Google Sheets demo data for year=%s from %s", year, url)
        df_year = pd.read_csv(url)
        if df_year.empty:
            logger.warning("Demo sheet for year=%s returned 0 rows.", year)
            continue
        frames.append(df_year)

    if missing_years:
        logger.warning("Missing demo sheet IDs for years: %s", ", ".join(missing_years))

    if not frames:
        raise RuntimeError("No Google Sheets demo data could be loaded. Check sheet IDs and sharing permissions.")

    combined = pd.concat(frames, ignore_index=True)
    normalized_names, original_map = _normalize_column_names(combined.columns.tolist())
    combined.columns = normalized_names
    combined = _coerce_numeric_columns(combined)
    combined.attrs["original_columns"] = original_map
    logger.info("Loaded demo dataframe with %s rows and %s columns.", len(combined), len(combined.columns))
    return combined


def get_demo_schema_context() -> str:
    df = get_demo_dataframe()
    original_map = df.attrs.get("original_columns", {})
    lines = []
    for col in df.columns:
        original = original_map.get(col, col)
        dtype = str(df[col].dtype)
        lines.append(f"- `{col}` : original='{original}' ({dtype})")
    return "\n".join(lines)


def get_demo_table_name() -> str:
    return settings.demo_sql_table


def adapt_sql_to_demo_table(sql: str) -> str:
    table_name = get_demo_table_name()
    if not sql:
        return ""

    fixed = sql
    full_pattern = r"`?" + re.escape(settings.full_table_id) + r"`?"
    fixed = re.sub(full_pattern, table_name, fixed)

    if settings.table and settings.table != table_name:
        bare_pattern = r"(?<![A-Za-z0-9_])`?" + re.escape(settings.table) + r"`?(?![A-Za-z0-9_])"
        fixed = re.sub(bare_pattern, table_name, fixed)

    return fixed


@lru_cache(maxsize=1)
def _get_sqlite_connection() -> sqlite3.Connection:
    df = get_demo_dataframe()
    table_name = get_demo_table_name()
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    df.to_sql(table_name, conn, index=False, if_exists="replace")
    logger.info("SQLite demo table '%s' initialized with %s rows.", table_name, len(df))
    return conn


def execute_demo_sql(query: str) -> pd.DataFrame:
    conn = _get_sqlite_connection()
    return pd.read_sql_query(query, conn)

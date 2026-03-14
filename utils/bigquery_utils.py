import logging
import re
import uuid
from dataclasses import replace
from functools import lru_cache
from typing import Any

import pandas as pd
from google.cloud import bigquery
import sqlglot
from sqlglot import exp

from config import settings
from utils.analysis_spec_utils import AnalysisSpec, coverage_notice, infer_analysis_spec
from utils.demo_data_utils import adapt_sql_to_demo_table, execute_demo_sql, get_demo_table_name
from utils.semantic_query_contracts import build_contract_sql
from utils.vertex_ai_utils import PROJECT_ID, generate_query_plan_from_natural_language

logger = logging.getLogger(__name__)

# Säilötään viimeisin BQ-virhe debuggausta varten.
last_bq_error = None
last_query_stats: dict[str, Any] = {}
last_execution_meta: dict[str, Any] = {}
DATA_MIN_YEAR = 1998
DATA_MAX_YEAR = 2025
YEAR_BETWEEN_PATTERN = re.compile(
    r"(?i)(SAFE_CAST\(\s*`?Vuosi`?\s+AS\s+INT64\s*\)\s+BETWEEN\s+)(\d{4})(\s+AND\s+)(\d{4})"
)
YEAR_EQUAL_PATTERN = re.compile(
    r"(?i)(SAFE_CAST\(\s*`?Vuosi`?\s+AS\s+INT64\s*\)\s*=\s*)(\d{4})"
)
LIMIT_PATTERN = re.compile(r"(?i)(\bLIMIT\s+)(\d+)")
LOWER_SIGNATURE_PATTERN = re.compile(r"(?i)LOWER\(([^)]+)\)")
UNRECOGNIZED_NAME_PATTERN = re.compile(r"(?i)unrecognized name:\s*([A-Za-z_][A-Za-z0-9_]*)")

REPAIR_UNKNOWN_NAME_MAP = {
    "hallinnonala": "`Hallinnonala`",
    "vuosi": "SAFE_CAST(`Vuosi` AS INT64)",
    "kk": "SAFE_CAST(`Kk` AS INT64)",
    "nettokertyma": "SAFE_CAST(`Nettokertymä` AS NUMERIC)",
    "momentti_tunnusp": "NULLIF(`Momentti_TunnusP`, '')",
    "momentti_snimi": "NULLIF(`Momentti_sNimi`, '')",
    "alamomentti_tunnus": "NULLIF(`TakpMrL_Tunnus`, '')",
    "alamomentti_snimi": "NULLIF(`TakpMrL_sNimi`, '')",
}


def _format_bytes(num_bytes: int | None) -> str:
    if not num_bytes:
        return "0 B"
    unit = 1024.0
    size = float(num_bytes)
    for suffix in ("B", "KB", "MB", "GB", "TB", "PB"):
        if size < unit or suffix == "PB":
            return f"{size:.2f} {suffix}"
        size /= unit
    return f"{num_bytes} B"


def get_last_query_stats() -> dict[str, Any]:
    return dict(last_query_stats)


def get_last_execution_meta() -> dict[str, Any]:
    return dict(last_execution_meta)


def _extract_years(text: str) -> list[int]:
    if not text:
        return []
    years = [int(m) for m in re.findall(r"\b(?:19|20)\d{2}\b", text)]
    # Preserve order but deduplicate
    seen = set()
    result = []
    for year in years:
        if year in seen:
            continue
        seen.add(year)
        result.append(year)
    return result


def _year_bounds(text: str) -> tuple[int | None, int | None]:
    years = _extract_years(text)
    if not years:
        return None, None
    if len(years) == 1:
        return years[0], years[0]
    return min(years), max(years)


def _effective_year_bounds(text: str) -> tuple[int | None, int | None]:
    year_from, year_to = _year_bounds(text)
    if year_from is None or year_to is None:
        return None, None
    year_from = max(year_from, DATA_MIN_YEAR)
    year_to = min(year_to, DATA_MAX_YEAR)
    if year_from > year_to:
        return None, None
    return year_from, year_to


def _normalize_year_bounds(year_from: int | None, year_to: int | None) -> tuple[int, int]:
    start = DATA_MIN_YEAR if year_from is None else max(DATA_MIN_YEAR, min(DATA_MAX_YEAR, int(year_from)))
    end = DATA_MAX_YEAR if year_to is None else max(DATA_MIN_YEAR, min(DATA_MAX_YEAR, int(year_to)))
    if start > end:
        start, end = end, start
    return start, end


def _coerce_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


def _merge_analysis_spec_with_query_plan(spec: AnalysisSpec, query_plan: dict[str, Any] | None) -> AnalysisSpec:
    if not query_plan:
        return spec

    allowed_intents = {"overview", "trend", "growth", "top_growth", "composition", "seasonality"}
    allowed_entities = {"kokonais", "hallinnonala", "momentti", "alamomentti", "molemmat"}
    allowed_growth = {"absolute", "pct"}

    merged = replace(spec)
    intent = str(query_plan.get("intent", "")).strip().lower()
    entity = str(query_plan.get("entity_level", "")).strip().lower()
    growth = str(query_plan.get("growth_type", "")).strip().lower()

    if intent in allowed_intents:
        merged.intent = intent
    if entity in allowed_entities:
        merged.entity_level = entity
    if growth in allowed_growth:
        merged.growth_type = growth

    ranking_n = _coerce_int(query_plan.get("ranking_n"))
    if ranking_n is not None:
        merged.ranking_n = max(1, min(ranking_n, 100))

    plan_from = _coerce_int(query_plan.get("time_from"))
    plan_to = _coerce_int(query_plan.get("time_to"))
    if plan_from is not None or plan_to is not None:
        year_from, year_to = _normalize_year_bounds(
            plan_from if plan_from is not None else merged.time_from,
            plan_to if plan_to is not None else merged.time_to,
        )
        merged.time_from = year_from
        merged.time_to = year_to
        if merged.requested_time_from is None:
            merged.requested_time_from = year_from
        if merged.requested_time_to is None:
            merged.requested_time_to = year_to
    return merged


def _with_default_year_bounds(year_from: int | None, year_to: int | None) -> tuple[int, int]:
    if year_from is None or year_to is None:
        return DATA_MIN_YEAR, DATA_MAX_YEAR
    return _normalize_year_bounds(year_from, year_to)


def _year_range_notice(text: str) -> str:
    req_from, req_to = _year_bounds(text)
    if req_from is None or req_to is None:
        return ""
    eff_from, eff_to = _effective_year_bounds(text)
    if eff_from is None or eff_to is None:
        return (
            f"Huom: pyydetty aikaväli {req_from}-{req_to} on datan saatavuuden ulkopuolella "
            f"(saatavilla {DATA_MIN_YEAR}-{DATA_MAX_YEAR})."
        )
    if req_from != eff_from or req_to != eff_to:
        return (
            f"Huom: pyydetty aikaväli {req_from}-{req_to} rajattiin datan saatavuuteen "
            f"{eff_from}-{eff_to}."
        )
    return ""


def _is_growth_query(text: str) -> bool:
    return any(
        token in text
        for token in (
            "kasv",
            "muutos",
            "muuttu",
            "kehitys",
            "kehitty",
            "trend",
            "yoy",
            "year over year",
        )
    )


def _is_top_moment_growth_query(text: str) -> bool:
    has_moment = any(token in text for token in ("moment", "alamoment"))
    has_growth = any(token in text for token in ("kasv", "muutos"))
    has_top = any(token in text for token in ("eniten", "suurin", "top", "absoluutt"))
    return has_moment and has_growth and has_top


def _is_defense_query(text: str) -> bool:
    return any(token in text for token in ("puolustus", "defence", "defense"))


def _is_higher_education_query(text: str) -> bool:
    return any(token in text for token in ("korkeakoulu", "yliopisto", "ammattikorkeakoulu"))


def _build_topic_where_clause(text: str, dialect: str) -> str | None:
    if _is_higher_education_query(text):
        if dialect == "bigquery":
            return (
                "("
                "LOWER(`Hallinnonala`) LIKE '%opetus%' "
                "OR LOWER(`Momentti_sNimi`) LIKE '%korkeakoul%' "
                "OR LOWER(`Momentti_sNimi`) LIKE '%yliopist%' "
                "OR LOWER(`Momentti_sNimi`) LIKE '%ammattikorkeakoul%'"
                ")"
            )
        return (
            "("
            "LOWER(hallinnonala) LIKE '%opetus%' "
            "OR LOWER(kirjanpitoyksikko) LIKE '%korkeakoul%' "
            "OR LOWER(kirjanpitoyksikko) LIKE '%yliopist%' "
            "OR LOWER(kirjanpitoyksikko) LIKE '%ammattikorkeakoul%'"
            ")"
        )
    return None


def _build_demo_fallback_sql(question: str) -> str:
    table = get_demo_table_name()
    text = (question or "").lower()
    year_from, year_to = _with_default_year_bounds(*_effective_year_bounds(text))
    where_parts = []
    if year_from == year_to:
        where_parts.append(f"vuosi = {year_from}")
    else:
        where_parts.append(f"vuosi BETWEEN {year_from} AND {year_to}")
    if _is_defense_query(text):
        where_parts.append("LOWER(hallinnonala) LIKE '%puolustus%'")
    topic_clause = _build_topic_where_clause(text, "demo")
    if topic_clause:
        where_parts.append(topic_clause)
    where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""

    if any(token in text for token in ("montako", "kuinka monta", "count", "lukum")):
        return f"SELECT COUNT(*) AS rows_count FROM {table}{where_clause} LIMIT 1"

    if _is_growth_query(text) and _is_higher_education_query(text):
        return (
            "WITH yearly AS ("
            f"  SELECT vuosi, momentti_tunnusp, momentti_snimi, SUM(CAST(nettokertyma AS REAL)) AS nettokertyma_sum FROM {table}{where_clause} "
            "  GROUP BY vuosi, momentti_tunnusp, momentti_snimi"
            "), ranked AS ("
            "  SELECT *, ROW_NUMBER() OVER (PARTITION BY vuosi ORDER BY ABS(nettokertyma_sum) DESC) AS rnk "
            "  FROM yearly"
            ") "
            "SELECT "
            "  vuosi, "
            "  momentti_tunnusp, "
            "  momentti_snimi, "
            "  nettokertyma_sum, "
            "  nettokertyma_sum - LAG(nettokertyma_sum) OVER (PARTITION BY momentti_tunnusp, momentti_snimi ORDER BY vuosi) AS muutos_eur, "
            "  CASE "
            "    WHEN LAG(nettokertyma_sum) OVER (PARTITION BY momentti_tunnusp, momentti_snimi ORDER BY vuosi) IS NULL THEN NULL "
            "    WHEN LAG(nettokertyma_sum) OVER (PARTITION BY momentti_tunnusp, momentti_snimi ORDER BY vuosi) = 0 THEN NULL "
            "    ELSE ((nettokertyma_sum - LAG(nettokertyma_sum) OVER (PARTITION BY momentti_tunnusp, momentti_snimi ORDER BY vuosi)) / ABS(LAG(nettokertyma_sum) OVER (PARTITION BY momentti_tunnusp, momentti_snimi ORDER BY vuosi))) * 100 "
            "  END AS muutos_pct "
            "FROM ranked "
            "WHERE rnk <= 12 "
            "ORDER BY vuosi, ABS(nettokertyma_sum) DESC"
        )

    if _is_growth_query(text):
        return (
            "WITH yearly AS ("
            f"  SELECT vuosi, SUM(CAST(nettokertyma AS REAL)) AS nettokertyma_sum FROM {table}{where_clause} "
            "  GROUP BY vuosi"
            ") "
            "SELECT "
            "  vuosi, "
            "  nettokertyma_sum, "
            "  nettokertyma_sum - LAG(nettokertyma_sum) OVER (ORDER BY vuosi) AS muutos_eur, "
            "  CASE "
            "    WHEN LAG(nettokertyma_sum) OVER (ORDER BY vuosi) IS NULL THEN NULL "
            "    WHEN LAG(nettokertyma_sum) OVER (ORDER BY vuosi) = 0 THEN NULL "
            "    ELSE ((nettokertyma_sum - LAG(nettokertyma_sum) OVER (ORDER BY vuosi)) / ABS(LAG(nettokertyma_sum) OVER (ORDER BY vuosi))) * 100 "
            "  END AS muutos_pct "
            "FROM yearly "
            "ORDER BY vuosi "
            "LIMIT 400"
        )

    if any(token in text for token in ("summa", "yhteensa", "kokonais", "menot", "nettokertym")):
        has_month_intent = any(token in text for token in ("kuukaus", "kk", "month"))
        if not has_month_intent:
            return (
                f"SELECT vuosi, hallinnonala, SUM(CAST(nettokertyma AS REAL)) AS nettokertyma_sum "
                f"FROM {table}{where_clause} "
                "GROUP BY vuosi, hallinnonala "
                "ORDER BY vuosi DESC, nettokertyma_sum DESC "
                "LIMIT 300"
            )
        return (
            f"SELECT vuosi, kk, hallinnonala, SUM(nettokertyma) AS nettokertyma_sum "
            f"FROM {table}{where_clause} "
            "GROUP BY vuosi, kk, hallinnonala "
            "ORDER BY vuosi DESC, kk DESC, nettokertyma_sum DESC "
            "LIMIT 200"
        )

    return (
        f"SELECT vuosi, kk, hallinnonala, kirjanpitoyksikko, nettokertyma "
        f"FROM {table}{where_clause} "
        "ORDER BY vuosi DESC, kk DESC "
        "LIMIT 200"
    )


def _build_bigquery_fallback_sql(question: str) -> str:
    table = f"`{settings.full_table_id}`"
    text = (question or "").lower()
    year_from, year_to = _with_default_year_bounds(*_effective_year_bounds(text))
    where_parts = []
    if year_from == year_to:
        where_parts.append(f"SAFE_CAST(`Vuosi` AS INT64) = {year_from}")
    else:
        where_parts.append(f"SAFE_CAST(`Vuosi` AS INT64) BETWEEN {year_from} AND {year_to}")
    if _is_defense_query(text):
        where_parts.append("LOWER(`Hallinnonala`) LIKE '%puolustus%'")
    topic_clause = _build_topic_where_clause(text, "bigquery")
    if topic_clause:
        where_parts.append(topic_clause)
    where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""

    if any(token in text for token in ("montako", "kuinka monta", "count", "lukum")):
        return f"SELECT COUNT(*) AS rows_count FROM {table}{where_clause} LIMIT 1"

    if _is_growth_query(text) and _is_higher_education_query(text):
        return (
            "WITH yearly AS ("
            "  SELECT "
            "    SAFE_CAST(`Vuosi` AS INT64) AS vuosi, "
            "    NULLIF(`Momentti_TunnusP`, '') AS momentti_tunnusp, "
            "    NULLIF(`Momentti_sNimi`, '') AS momentti_snimi, "
            "    SUM(SAFE_CAST(`Nettokertymä` AS NUMERIC)) AS nettokertyma_sum "
            f"  FROM {table}{where_clause} "
            "  GROUP BY vuosi, momentti_tunnusp, momentti_snimi"
            "), ranked AS ("
            "  SELECT *, ROW_NUMBER() OVER (PARTITION BY vuosi ORDER BY ABS(nettokertyma_sum) DESC) AS rnk "
            "  FROM yearly"
            ") "
            "SELECT "
            "  vuosi, "
            "  momentti_tunnusp, "
            "  momentti_snimi, "
            "  nettokertyma_sum, "
            "  nettokertyma_sum - LAG(nettokertyma_sum) OVER (PARTITION BY momentti_tunnusp, momentti_snimi ORDER BY vuosi) AS muutos_eur, "
            "  SAFE_DIVIDE("
            "    nettokertyma_sum - LAG(nettokertyma_sum) OVER (PARTITION BY momentti_tunnusp, momentti_snimi ORDER BY vuosi), "
            "    ABS(LAG(nettokertyma_sum) OVER (PARTITION BY momentti_tunnusp, momentti_snimi ORDER BY vuosi))"
            "  ) * 100 AS muutos_pct "
            "FROM ranked "
            "WHERE rnk <= 12 "
            "ORDER BY vuosi, ABS(nettokertyma_sum) DESC"
        )

    if _is_top_moment_growth_query(text):
        if year_from is None or year_to is None:
            year_from, year_to = DATA_MIN_YEAR, DATA_MAX_YEAR
        return (
            "WITH yearly AS ("
            "  SELECT "
            "    SAFE_CAST(`Vuosi` AS INT64) AS vuosi, "
            "    NULLIF(`Momentti_TunnusP`, '') AS momentti_tunnusp, "
            "    NULLIF(`Momentti_sNimi`, '') AS momentti_snimi, "
            "    NULLIF(`TakpMrL_Tunnus`, '') AS alamomentti_tunnus, "
            "    NULLIF(`TakpMrL_sNimi`, '') AS alamomentti_snimi, "
            "    SUM(SAFE_CAST(`Nettokertymä` AS NUMERIC)) AS nettokertyma_sum "
            f"  FROM {table} "
            f"  WHERE SAFE_CAST(`Vuosi` AS INT64) BETWEEN {year_from} AND {year_to} "
            "  GROUP BY vuosi, momentti_tunnusp, momentti_snimi, alamomentti_tunnus, alamomentti_snimi"
            "), "
            "start_end AS ("
            "  SELECT "
            "    momentti_tunnusp, "
            "    momentti_snimi, "
            "    alamomentti_tunnus, "
            "    alamomentti_snimi, "
            f"    SUM(IF(vuosi = {year_from}, nettokertyma_sum, 0)) AS alkuvuosi_sum, "
            f"    SUM(IF(vuosi = {year_to}, nettokertyma_sum, 0)) AS loppuvuosi_sum "
            "  FROM yearly "
            "  GROUP BY momentti_tunnusp, momentti_snimi, alamomentti_tunnus, alamomentti_snimi"
            ") "
            "SELECT "
            "  momentti_tunnusp, "
            "  momentti_snimi, "
            "  alamomentti_tunnus, "
            "  alamomentti_snimi, "
            "  alkuvuosi_sum, "
            "  loppuvuosi_sum, "
            "  loppuvuosi_sum - alkuvuosi_sum AS kasvu_eur, "
            "  SAFE_DIVIDE(loppuvuosi_sum - alkuvuosi_sum, NULLIF(ABS(alkuvuosi_sum), 0)) * 100 AS kasvu_pct "
            "FROM start_end "
            "WHERE momentti_tunnusp IS NOT NULL OR momentti_snimi IS NOT NULL "
            "ORDER BY kasvu_eur DESC "
            "LIMIT 100"
        )

    if _is_growth_query(text):
        return (
            "WITH yearly AS ("
            "  SELECT "
            "    SAFE_CAST(`Vuosi` AS INT64) AS vuosi, "
            "    SUM(SAFE_CAST(`Nettokertymä` AS NUMERIC)) AS nettokertyma_sum "
            f"  FROM {table}{where_clause} "
            "  GROUP BY vuosi"
            ") "
            "SELECT "
            "  vuosi, "
            "  nettokertyma_sum, "
            "  nettokertyma_sum - LAG(nettokertyma_sum) OVER (ORDER BY vuosi) AS muutos_eur, "
            "  SAFE_DIVIDE(nettokertyma_sum - LAG(nettokertyma_sum) OVER (ORDER BY vuosi), ABS(LAG(nettokertyma_sum) OVER (ORDER BY vuosi))) * 100 AS muutos_pct "
            "FROM yearly "
            "ORDER BY vuosi "
            "LIMIT 400"
        )

    if any(token in text for token in ("summa", "yhteensa", "kokonais", "menot", "nettokertym")):
        has_month_intent = any(token in text for token in ("kuukaus", "kk", "month"))
        if not has_month_intent:
            return (
                "SELECT "
                "SAFE_CAST(`Vuosi` AS INT64) AS vuosi, "
                "`Hallinnonala` AS hallinnonala, "
                "SUM(SAFE_CAST(`Nettokertymä` AS NUMERIC)) AS nettokertyma_sum "
                f"FROM {table}{where_clause} "
                "GROUP BY vuosi, hallinnonala "
                "ORDER BY vuosi DESC, nettokertyma_sum DESC "
                "LIMIT 300"
            )
        return (
            "SELECT "
            "SAFE_CAST(`Vuosi` AS INT64) AS vuosi, "
            "SAFE_CAST(`Kk` AS INT64) AS kk, "
            "`Hallinnonala` AS hallinnonala, "
            "SUM(SAFE_CAST(`Nettokertymä` AS NUMERIC)) AS nettokertyma_sum "
            f"FROM {table}{where_clause} "
            "GROUP BY vuosi, kk, hallinnonala "
            "ORDER BY vuosi DESC, kk DESC, nettokertyma_sum DESC "
            "LIMIT 200"
        )

    return (
        "SELECT "
        "SAFE_CAST(`Vuosi` AS INT64) AS vuosi, "
        "SAFE_CAST(`Kk` AS INT64) AS kk, "
        "`Hallinnonala` AS hallinnonala, "
        "`Kirjanpitoyksikkö` AS kirjanpitoyksikko, "
        "SAFE_CAST(`Nettokertymä` AS NUMERIC) AS nettokertyma "
        f"FROM {table}{where_clause} "
        "ORDER BY vuosi DESC, kk DESC "
        "LIMIT 200"
    )


@lru_cache(maxsize=1)
def _get_bq_client():
    if settings.use_google_sheets_demo:
        return None
    try:
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        logger.error("BigQuery Clientin alustus epäonnistui: %s", e)
        return None


def validate_sql(sql: str) -> str:
    """
    Tarkistaa ja yrittää korjata yleisimpiä SQL-syntaksivirheitä,
    erityisesti liittyen backtickeihin ja taulun nimeen.
    """
    global last_bq_error
    last_bq_error = None

    if not sql:
        return ""

    sql = re.sub(r"^```sql`?\s*", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\s*`?```$", "", sql)
    sql = sql.strip()

    if settings.use_google_sheets_demo:
        sql = adapt_sql_to_demo_table(sql)
        table_name = get_demo_table_name()
        sql = sql.replace(f"`{table_name}`", table_name)
        return sql

    table_name = settings.full_table_id
    correct_table_name = f"`{table_name}`"
    pattern = r"`?" + re.escape(table_name) + r"`?"
    return re.sub(pattern, correct_table_name, sql)


def _parse_sql(sql: str):
    dialect = "sqlite" if settings.use_google_sheets_demo else "bigquery"
    return sqlglot.parse_one(sql, read=dialect)


def _normalize_table_id(table_id: str) -> str:
    return table_id.replace("`", "").strip().lower()


def _table_to_fqn(table: exp.Table) -> str:
    catalog = (table.catalog or "").replace("`", "")
    db = (table.db or "").replace("`", "")
    name = (table.name or "").replace("`", "")
    if not name:
        return ""
    if catalog and db:
        return f"{catalog}.{db}.{name}"
    if db:
        return f"{settings.project_id}.{db}.{name}"
    return f"{settings.project_id}.{settings.dataset}.{name}"


def _physical_source_tables(parsed_sql) -> set[str]:
    cte_names = {cte.alias_or_name.lower() for cte in parsed_sql.find_all(exp.CTE) if cte.alias_or_name}
    tables: set[str] = set()
    for table in parsed_sql.find_all(exp.Table):
        if not table.catalog and not table.db and (table.name or "").lower() in cte_names:
            continue
        fqn = _table_to_fqn(table)
        if fqn:
            tables.add(_normalize_table_id(fqn))
    return tables


def _enforce_year_bounds(sql: str) -> tuple[str, str | None]:
    def _clamp_between(match: re.Match[str]) -> str:
        start = int(match.group(2))
        end = int(match.group(4))
        start, end = _normalize_year_bounds(start, end)
        return f"{match.group(1)}{start}{match.group(3)}{end}"

    if YEAR_BETWEEN_PATTERN.search(sql):
        updated_sql = YEAR_BETWEEN_PATTERN.sub(_clamp_between, sql)
        return updated_sql, None

    def _clamp_equal(match: re.Match[str]) -> str:
        year = max(DATA_MIN_YEAR, min(DATA_MAX_YEAR, int(match.group(2))))
        return f"{match.group(1)}{year}"

    if YEAR_EQUAL_PATTERN.search(sql):
        updated_sql = YEAR_EQUAL_PATTERN.sub(_clamp_equal, sql)
        return updated_sql, None

    return sql, "SQL-kyselystä puuttuu pakollinen Vuosi-aikarajaus."


def _enforce_limit_cap(sql: str) -> tuple[str, str | None]:
    cap = max(1, int(settings.sql_max_limit))

    if not LIMIT_PATTERN.search(sql):
        return f"{sql.rstrip(';')} LIMIT {cap}", None

    def _cap_limit(match: re.Match[str]) -> str:
        value = int(match.group(2))
        return f"{match.group(1)}{min(value, cap)}"

    return LIMIT_PATTERN.sub(_cap_limit, sql), None


def enforce_sql_security(sql: str) -> tuple[str, str | None]:
    """Lint + enforce secure query constraints before execution."""
    if not sql:
        return "", "Tyhjä SQL-kysely."

    if not re.match(r"^\s*(select|with)\b", sql, re.IGNORECASE):
        return "", "Vain SELECT/WITH-kyselyt ovat sallittuja."

    try:
        parsed = _parse_sql(sql)
    except Exception as e:
        return "", f"SQL lint epäonnistui: {e}"

    if not settings.use_google_sheets_demo:
        source_tables = _physical_source_tables(parsed)
        allowed = _normalize_table_id(settings.full_table_id)
        if source_tables != {allowed}:
            return (
                "",
                "SQL käyttää kiellettyä taulua tai useita tauluja. "
                f"Sallittu lähde: `{settings.full_table_id}`.",
            )

    secured_sql, year_error = _enforce_year_bounds(sql)
    if year_error:
        return "", year_error

    secured_sql, limit_error = _enforce_limit_cap(secured_sql)
    if limit_error:
        return "", limit_error

    try:
        _parse_sql(secured_sql)
    except Exception as e:
        return "", f"SQL lint epäonnistui turvamuunnosten jälkeen: {e}"

    return secured_sql, None


def _classify_error(error_text: str | None) -> str:
    if not error_text:
        return ""
    text = str(error_text).lower()
    if "syntax error" in text or "invalidquery" in text:
        return "syntax_error"
    if "unrecognized name" in text or ("name " in text and " not found" in text):
        return "unknown_name"
    if "permission" in text or "access denied" in text or "forbidden" in text:
        return "permission"
    if "maximum bytes billed" in text or "kustannusrajan vuoksi" in text:
        return "bytes_limit"
    if "resources exceeded" in text:
        return "resources_exceeded"
    if "timed out" in text or "deadline exceeded" in text:
        return "timeout"
    if "client ei ole alustettu" in text:
        return "client_init"
    if "tyhjä sql" in text:
        return "empty_sql"
    return "unknown"


def _force_limit(sql: str, new_limit: int) -> str:
    cap = max(1, int(new_limit))
    if LIMIT_PATTERN.search(sql):
        return LIMIT_PATTERN.sub(lambda m: f"{m.group(1)}{cap}", sql)
    return f"{sql.rstrip(';')} LIMIT {cap}"


def _repair_sql_from_error(sql: str, error_text: str, attempt: int) -> str | None:
    if not sql or not error_text:
        return None

    repaired = sql
    changed = False
    lower_error = error_text.lower()

    if "syntax error" in lower_error:
        try:
            parsed = _parse_sql(repaired)
            repaired = parsed.sql(dialect="sqlite" if settings.use_google_sheets_demo else "bigquery")
            changed = repaired.strip() != sql.strip()
        except Exception:
            pass

    name_match = UNRECOGNIZED_NAME_PATTERN.search(error_text)
    if name_match:
        token = name_match.group(1).lower()
        replacement = REPAIR_UNKNOWN_NAME_MAP.get(token)
        if replacement:
            repaired_new = re.sub(rf"\b{re.escape(name_match.group(1))}\b", replacement, repaired)
            if repaired_new != repaired:
                repaired = repaired_new
                changed = True

    if "no matching signature for function lower" in lower_error:
        repaired_new = LOWER_SIGNATURE_PATTERN.sub(r"LOWER(CAST(\1 AS STRING))", repaired)
        if repaired_new != repaired:
            repaired = repaired_new
            changed = True

    if "resources exceeded" in lower_error or "maximum bytes billed" in lower_error:
        tightened_limit = min(max(50, settings.sql_max_limit // (attempt + 1)), 300)
        repaired_new = _force_limit(repaired, tightened_limit)
        if repaired_new != repaired:
            repaired = repaired_new
            changed = True

    if not changed:
        return None
    return repaired


def _prepare_sql_for_execution(sql: str) -> tuple[str | None, str | None]:
    validated_sql = validate_sql(sql)
    if not validated_sql:
        return None, "SQL-kyselyn validointi epäonnistui tai tyhjensi kyselyn."

    secured_sql, security_error = enforce_sql_security(validated_sql)
    if security_error:
        return None, security_error
    return secured_sql, None


def run_sql_query(query: str) -> pd.DataFrame:
    """Suorittaa SQL-kyselyn valitussa tietolähteessä ja palauttaa Pandas DataFramena."""
    global last_bq_error, last_query_stats, last_execution_meta
    last_bq_error = None
    last_query_stats = {}
    last_execution_meta = {
        "stage": None,
        "error_class": "",
        "estimated_bytes": None,
    }

    if not query:
        last_bq_error = "Tyhjä SQL-kysely annettu."
        last_execution_meta.update({"stage": "precheck", "error_class": _classify_error(last_bq_error)})
        return pd.DataFrame()

    if not re.match(r"^\s*(select|with)\b", query, re.IGNORECASE):
        last_bq_error = "Vain SELECT-kyselyt (myös WITH CTE) ovat sallittuja."
        last_execution_meta.update({"stage": "precheck", "error_class": _classify_error(last_bq_error)})
        return pd.DataFrame()

    if settings.use_google_sheets_demo:
        try:
            demo_query = adapt_sql_to_demo_table(query)
            results = execute_demo_sql(demo_query)
            last_query_stats = {"rows": len(results), "source": "google_sheets_demo"}
            last_execution_meta.update({"stage": "execute", "error_class": ""})
            return results
        except Exception as e:
            last_bq_error = f"Demo SQL Exception: {str(e)}"
            last_execution_meta.update({"stage": "execute", "error_class": _classify_error(last_bq_error)})
            return pd.DataFrame()

    bq_client = _get_bq_client()
    if not bq_client:
        last_bq_error = "BigQuery Client ei ole alustettu."
        last_execution_meta.update({"stage": "precheck", "error_class": _classify_error(last_bq_error)})
        return pd.DataFrame()

    try:
        dry_job = bq_client.query(
            query,
            job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False),
        )
        estimated_bytes = int(dry_job.total_bytes_processed or 0)
        last_execution_meta.update({"stage": "dry_run", "estimated_bytes": estimated_bytes})

        if settings.max_query_bytes > 0 and estimated_bytes > settings.max_query_bytes:
            last_bq_error = (
                "Kysely hylätty kustannusrajan vuoksi. "
                f"Arvioitu skannaus: {_format_bytes(estimated_bytes)}, "
                f"raja: {_format_bytes(settings.max_query_bytes)}."
            )
            last_execution_meta.update({"error_class": _classify_error(last_bq_error)})
            return pd.DataFrame()
    except Exception as e:
        last_bq_error = f"BigQuery Dry-run Exception: {str(e)}"
        last_execution_meta.update({"stage": "dry_run", "error_class": _classify_error(last_bq_error)})
        return pd.DataFrame()

    try:
        job_config = bigquery.QueryJobConfig(use_query_cache=True)
        if settings.max_query_bytes > 0:
            job_config.maximum_bytes_billed = settings.max_query_bytes

        query_job = bq_client.query(query, job_config=job_config)
        # Avoid requiring BigQuery Storage API permissions (readsessions.create).
        results_df = query_job.result().to_dataframe(create_bqstorage_client=False)
        if query_job.error_result:
            last_bq_error = f"BigQuery Job Error: {query_job.error_result}"
            last_execution_meta.update({"stage": "execute", "error_class": _classify_error(last_bq_error)})
            return pd.DataFrame()

        last_query_stats = {
            "estimated_bytes": int(last_execution_meta.get("estimated_bytes") or 0),
            "processed_bytes": int(query_job.total_bytes_processed or 0),
            "billed_bytes": int(query_job.total_bytes_billed or 0),
            "cache_hit": bool(query_job.cache_hit),
            "rows": len(results_df),
        }
        last_execution_meta.update({"stage": "execute", "error_class": ""})
        return results_df
    except Exception as e:
        last_bq_error = f"Python Exception: {str(e)}"
        last_execution_meta.update({"stage": "execute", "error_class": _classify_error(last_bq_error)})
        return pd.DataFrame()


def _execute_with_auto_repair(sql: str, max_repair_attempts: int) -> dict[str, Any]:
    global last_bq_error

    current_sql = sql
    retries = 0
    dry_run_bytes = None
    last_error = None
    last_error_class = ""

    for attempt in range(max(0, max_repair_attempts) + 1):
        secured_sql, prep_error = _prepare_sql_for_execution(current_sql)
        if prep_error or not secured_sql:
            return {
                "ok": False,
                "sql": current_sql,
                "results_df": pd.DataFrame(),
                "error": prep_error or "SQL-kyselyn valmistelu epäonnistui.",
                "error_class": _classify_error(prep_error),
                "retries": retries,
                "dry_run_bytes": dry_run_bytes,
            }

        results_df = run_sql_query(secured_sql)
        execution_meta = get_last_execution_meta()
        if execution_meta.get("estimated_bytes") is not None:
            dry_run_bytes = execution_meta.get("estimated_bytes")

        if not last_bq_error:
            return {
                "ok": True,
                "sql": secured_sql,
                "results_df": results_df,
                "error": None,
                "error_class": "",
                "retries": retries,
                "dry_run_bytes": dry_run_bytes,
            }

        last_error = last_bq_error
        last_error_class = execution_meta.get("error_class") or _classify_error(last_bq_error)
        if settings.use_google_sheets_demo or attempt >= max_repair_attempts:
            break

        repaired_sql = _repair_sql_from_error(secured_sql, last_bq_error, attempt + 1)
        if not repaired_sql or repaired_sql.strip() == secured_sql.strip():
            break
        current_sql = repaired_sql
        retries += 1

    return {
        "ok": False,
        "sql": current_sql,
        "results_df": pd.DataFrame(),
        "error": last_error or "SQL-suoritus epäonnistui.",
        "error_class": last_error_class or _classify_error(last_error),
        "retries": retries,
        "dry_run_bytes": dry_run_bytes,
    }


def _deterministic_fallback_sql(question: str) -> str:
    if settings.use_google_sheets_demo:
        return _build_demo_fallback_sql(question)
    return _build_bigquery_fallback_sql(question)


def process_natural_language_query(question: str) -> dict:
    """
    Käsittelee luonnollisen kielen kysymyksen: generoi SQL, validoi, suorittaa ja palauttaa tulokset.
    """
    logger.info("Käsitellään kysymys: %s", question)
    query_id = str(uuid.uuid4())
    analysis_spec = infer_analysis_spec(question)
    query_contract = None
    query_source = "contract"
    query_plan = None

    generated_sql = None
    if not settings.use_google_sheets_demo and settings.enable_llm_query_plan:
        fallback_plan = {
            "intent": analysis_spec.intent,
            "metric": analysis_spec.metric,
            "entity_level": analysis_spec.entity_level,
            "growth_type": analysis_spec.growth_type,
            "time_from": analysis_spec.time_from,
            "time_to": analysis_spec.time_to,
            "ranking_n": analysis_spec.ranking_n,
        }
        query_plan = generate_query_plan_from_natural_language(question, fallback_plan=fallback_plan)
        analysis_spec = _merge_analysis_spec_with_query_plan(analysis_spec, query_plan)

    if not settings.use_google_sheets_demo:
        generated_sql, query_contract = build_contract_sql(analysis_spec, settings.full_table_id)

    if not generated_sql:
        query_source = "fallback"
        generated_sql = _deterministic_fallback_sql(question)

    if not generated_sql:
        return {
            "query_id": query_id,
            "sql_query": "",
            "results_df": pd.DataFrame(),
            "error": "Deterministisen SQL-kyselyn muodostus epäonnistui.",
            "explanation": "❌ Ei saatu muodostettua turvallista SQL-kyselyä.",
            "analysis_spec": analysis_spec,
            "query_contract": query_contract,
            "query_source": query_source,
            "query_plan": query_plan,
            "query_retries": 0,
            "dry_run_bytes": None,
            "error_class": "sql_generation_failed",
        }

    repair_attempts = 0 if settings.use_google_sheets_demo else max(0, settings.bq_auto_repair_attempts)
    execution = _execute_with_auto_repair(generated_sql, max_repair_attempts=repair_attempts)
    total_retries = int(execution.get("retries") or 0)
    executed_sql = execution.get("sql") or generated_sql

    if not execution.get("ok"):
        fallback_sql = _deterministic_fallback_sql(question)
        if fallback_sql and fallback_sql.strip() != generated_sql.strip():
            fallback_execution = _execute_with_auto_repair(
                fallback_sql,
                max_repair_attempts=repair_attempts,
            )
            total_retries += int(fallback_execution.get("retries") or 0)
            if fallback_execution.get("ok"):
                execution = fallback_execution
                executed_sql = fallback_execution.get("sql") or fallback_sql
                query_source = "fallback_contract"
                query_contract = None

    results_df = execution.get("results_df") if isinstance(execution.get("results_df"), pd.DataFrame) else pd.DataFrame()
    error = execution.get("error")
    error_class = execution.get("error_class") or ""
    dry_run_bytes = execution.get("dry_run_bytes")

    global last_bq_error
    year_notice = coverage_notice(analysis_spec) or _year_range_notice(question)
    source_info = f"Kyselypolku: {query_source}"
    if query_contract:
        source_info = f"{source_info} ({query_contract})"
    if error:
        source_name = "Google Sheets demo" if settings.use_google_sheets_demo else "BigQuery"
        explanation = (
            f"❌ Virhe suoritettaessa SQL-kyselyä lähteessä {source_name}: {error}. {source_info}."
        )
        if total_retries > 0:
            explanation = f"{explanation} Auto-repair yritykset: {total_retries}."
        if year_notice:
            explanation = f"{explanation} {year_notice}"
        return {
            "query_id": query_id,
            "sql_query": executed_sql,
            "results_df": results_df,
            "error": error,
            "explanation": explanation,
            "analysis_spec": analysis_spec,
            "query_contract": query_contract,
            "query_source": query_source,
            "query_plan": query_plan,
            "query_retries": total_retries,
            "dry_run_bytes": dry_run_bytes,
            "error_class": error_class or _classify_error(error),
        }
    if results_df.empty:
        explanation = f"✅ Kysely suoritettiin onnistuneesti, mutta se ei palauttanut tuloksia. {source_info}."
        if total_retries > 0:
            explanation = f"{explanation} Auto-repair yritykset: {total_retries}."
        if year_notice:
            explanation = f"{explanation} {year_notice}"
        return {
            "query_id": query_id,
            "sql_query": executed_sql,
            "results_df": results_df,
            "error": None,
            "explanation": explanation,
            "analysis_spec": analysis_spec,
            "query_contract": query_contract,
            "query_source": query_source,
            "query_plan": query_plan,
            "query_retries": total_retries,
            "dry_run_bytes": dry_run_bytes,
            "error_class": "",
        }

    explanation = f"✅ Kysely onnistui ja palautti {len(results_df)} riviä dataa. {source_info}."
    if total_retries > 0:
        explanation = f"{explanation} Auto-repair yritykset: {total_retries}."
    if year_notice:
        explanation = f"{explanation} {year_notice}"
    return {
        "query_id": query_id,
        "sql_query": executed_sql,
        "results_df": results_df,
        "error": None,
        "explanation": explanation,
        "analysis_spec": analysis_spec,
        "query_contract": query_contract,
        "query_source": query_source,
        "query_plan": query_plan,
        "query_retries": total_retries,
        "dry_run_bytes": dry_run_bytes,
        "error_class": "",
    }

import logging
import re
from functools import lru_cache
from typing import Any

import pandas as pd
from google.cloud import bigquery

from config import settings
from utils.analysis_spec_utils import coverage_notice, infer_analysis_spec
from utils.demo_data_utils import adapt_sql_to_demo_table, execute_demo_sql, get_demo_table_name
from utils.semantic_query_contracts import build_contract_sql
from utils.vertex_ai_utils import PROJECT_ID, generate_sql_from_natural_language

logger = logging.getLogger(__name__)

# Säilötään viimeisin BQ-virhe debuggausta varten.
last_bq_error = None
last_query_stats: dict[str, Any] = {}
DATA_MIN_YEAR = 1998
DATA_MAX_YEAR = 2025


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
    year_from, year_to = _effective_year_bounds(text)
    where_parts = []
    if year_from is not None and year_to is not None:
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
        return f"SELECT COUNT(*) AS rows_count FROM {table}{where_clause}"

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
            "ORDER BY vuosi"
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
    year_from, year_to = _effective_year_bounds(text)
    where_parts = []
    if year_from is not None and year_to is not None:
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
        return f"SELECT COUNT(*) AS rows_count FROM {table}{where_clause}"

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
            "ORDER BY vuosi"
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


def run_sql_query(query: str) -> pd.DataFrame:
    """Suorittaa SQL-kyselyn valitussa tietolähteessä ja palauttaa Pandas DataFramena."""
    global last_bq_error, last_query_stats
    last_bq_error = None
    last_query_stats = {}

    if not query:
        last_bq_error = "Tyhjä SQL-kysely annettu."
        return pd.DataFrame()

    if not re.match(r"^\s*(select|with)\b", query, re.IGNORECASE):
        last_bq_error = "Vain SELECT-kyselyt (myös WITH CTE) ovat sallittuja."
        return pd.DataFrame()

    if settings.use_google_sheets_demo:
        try:
            demo_query = adapt_sql_to_demo_table(query)
            return execute_demo_sql(demo_query)
        except Exception as e:
            last_bq_error = f"Demo SQL Exception: {str(e)}"
            return pd.DataFrame()

    bq_client = _get_bq_client()
    if not bq_client:
        last_bq_error = "BigQuery Client ei ole alustettu."
        return pd.DataFrame()

    try:
        dry_job = bq_client.query(
            query,
            job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False),
        )
        estimated_bytes = int(dry_job.total_bytes_processed or 0)

        if settings.max_query_bytes > 0 and estimated_bytes > settings.max_query_bytes:
            last_bq_error = (
                "Kysely hylätty kustannusrajan vuoksi. "
                f"Arvioitu skannaus: {_format_bytes(estimated_bytes)}, "
                f"raja: {_format_bytes(settings.max_query_bytes)}."
            )
            return pd.DataFrame()

        job_config = bigquery.QueryJobConfig(use_query_cache=True)
        if settings.max_query_bytes > 0:
            job_config.maximum_bytes_billed = settings.max_query_bytes

        query_job = bq_client.query(query, job_config=job_config)
        # Avoid requiring BigQuery Storage API permissions (readsessions.create).
        results_df = query_job.result().to_dataframe(create_bqstorage_client=False)
        if query_job.error_result:
            last_bq_error = f"BigQuery Job Error: {query_job.error_result}"
            return pd.DataFrame()

        last_query_stats = {
            "estimated_bytes": estimated_bytes,
            "processed_bytes": int(query_job.total_bytes_processed or 0),
            "billed_bytes": int(query_job.total_bytes_billed or 0),
            "cache_hit": bool(query_job.cache_hit),
            "rows": len(results_df),
        }
        return results_df
    except Exception as e:
        last_bq_error = f"Python Exception: {str(e)}"
        return pd.DataFrame()


def process_natural_language_query(question: str) -> dict:
    """
    Käsittelee luonnollisen kielen kysymyksen: generoi SQL, validoi, suorittaa ja palauttaa tulokset.
    """
    logger.info("Käsitellään kysymys: %s", question)
    analysis_spec = infer_analysis_spec(question)
    query_contract = None
    query_source = "llm"

    generated_sql = None
    if not settings.use_google_sheets_demo:
        generated_sql, query_contract = build_contract_sql(analysis_spec, settings.full_table_id)
        if generated_sql:
            query_source = "contract"

    if not generated_sql:
        generated_sql = generate_sql_from_natural_language(question)
        if generated_sql:
            query_source = "llm"

    if not generated_sql:
        query_source = "fallback"
        if settings.use_google_sheets_demo:
            generated_sql = _build_demo_fallback_sql(question)
        else:
            generated_sql = _build_bigquery_fallback_sql(question)

    if not generated_sql:
        return {
            "sql_query": "",
            "results_df": pd.DataFrame(),
            "error": "SQL-kyselyn generointi Vertex AI:lla epäonnistui.",
            "explanation": "❌ Ei saatu SQL-kyselyä tekoälyltä.",
            "analysis_spec": analysis_spec,
            "query_contract": query_contract,
            "query_source": query_source,
        }

    validated_sql = validate_sql(generated_sql)
    if not validated_sql:
        return {
            "sql_query": generated_sql,
            "results_df": pd.DataFrame(),
            "error": "SQL-kyselyn validointi epäonnistui tai tyhjensi kyselyn.",
            "explanation": "❌ Generoitu SQL ei läpäissyt validointia.",
            "analysis_spec": analysis_spec,
            "query_contract": query_contract,
            "query_source": query_source,
        }

    results_df = run_sql_query(validated_sql)

    global last_bq_error
    year_notice = coverage_notice(analysis_spec) or _year_range_notice(question)
    source_info = f"Kyselypolku: {query_source}"
    if query_contract:
        source_info = f"{source_info} ({query_contract})"
    if last_bq_error:
        source_name = "Google Sheets demo" if settings.use_google_sheets_demo else "BigQuery"
        explanation = (
            f"❌ Virhe suoritettaessa SQL-kyselyä lähteessä {source_name}: {last_bq_error}. {source_info}."
        )
        if year_notice:
            explanation = f"{explanation} {year_notice}"
        return {
            "sql_query": validated_sql,
            "results_df": results_df,
            "error": last_bq_error,
            "explanation": explanation,
            "analysis_spec": analysis_spec,
            "query_contract": query_contract,
            "query_source": query_source,
        }
    if results_df.empty:
        explanation = f"✅ Kysely suoritettiin onnistuneesti, mutta se ei palauttanut tuloksia. {source_info}."
        if year_notice:
            explanation = f"{explanation} {year_notice}"
        return {
            "sql_query": validated_sql,
            "results_df": results_df,
            "error": None,
            "explanation": explanation,
            "analysis_spec": analysis_spec,
            "query_contract": query_contract,
            "query_source": query_source,
        }

    explanation = f"✅ Kysely onnistui ja palautti {len(results_df)} riviä dataa. {source_info}."
    if year_notice:
        explanation = f"{explanation} {year_notice}"
    return {
        "sql_query": validated_sql,
        "results_df": results_df,
        "error": None,
        "explanation": explanation,
        "analysis_spec": analysis_spec,
        "query_contract": query_contract,
        "query_source": query_source,
    }

from __future__ import annotations

from utils.analysis_spec_utils import AnalysisSpec, DATA_MAX_YEAR, DATA_MIN_YEAR


def _effective_years(spec: AnalysisSpec) -> tuple[int, int]:
    if spec.time_from is None or spec.time_to is None:
        return DATA_MIN_YEAR, DATA_MAX_YEAR
    return spec.time_from, spec.time_to


def choose_contract(spec: AnalysisSpec) -> str | None:
    if spec.intent == "top_growth":
        if spec.entity_level == "momentti":
            return "top_growth_moment"
        if spec.entity_level in {"alamomentti", "molemmat"}:
            return "top_growth_alamoment"
    if spec.intent == "trend" and spec.entity_level == "hallinnonala":
        return "trend_by_hallinnonala"
    if spec.intent == "growth":
        return "yoy_change"
    return None


def _order_expression(spec: AnalysisSpec) -> str:
    if spec.growth_type == "pct":
        return "kasvu_pct DESC"
    return "kasvu_eur DESC"


def _top_limit(spec: AnalysisSpec, default_limit: int = 100) -> int:
    if not spec.ranking_n:
        return default_limit
    return max(1, min(spec.ranking_n * 10, 300))


def _sql_top_growth_moment(spec: AnalysisSpec, table_id: str) -> str:
    year_from, year_to = _effective_years(spec)
    limit_n = _top_limit(spec)
    order_expr = _order_expression(spec)
    return (
        "WITH yearly AS ("
        "  SELECT "
        "    SAFE_CAST(`Vuosi` AS INT64) AS vuosi, "
        "    NULLIF(`Momentti_TunnusP`, '') AS momentti_tunnusp, "
        "    NULLIF(`Momentti_sNimi`, '') AS momentti_snimi, "
        "    SUM(SAFE_CAST(`Nettokertymä` AS NUMERIC)) AS nettokertyma_sum "
        f"  FROM `{table_id}` "
        f"  WHERE SAFE_CAST(`Vuosi` AS INT64) BETWEEN {year_from} AND {year_to} "
        "  GROUP BY vuosi, momentti_tunnusp, momentti_snimi"
        "), "
        "start_end AS ("
        "  SELECT "
        "    momentti_tunnusp, "
        "    momentti_snimi, "
        f"    SUM(IF(vuosi = {year_from}, nettokertyma_sum, 0)) AS alkuvuosi_sum, "
        f"    SUM(IF(vuosi = {year_to}, nettokertyma_sum, 0)) AS loppuvuosi_sum "
        "  FROM yearly "
        "  GROUP BY momentti_tunnusp, momentti_snimi"
        ") "
        "SELECT "
        "  momentti_tunnusp, "
        "  momentti_snimi, "
        "  alkuvuosi_sum, "
        "  loppuvuosi_sum, "
        "  loppuvuosi_sum - alkuvuosi_sum AS kasvu_eur, "
        "  SAFE_DIVIDE(loppuvuosi_sum - alkuvuosi_sum, NULLIF(ABS(alkuvuosi_sum), 0)) * 100 AS kasvu_pct "
        "FROM start_end "
        "WHERE momentti_tunnusp IS NOT NULL OR momentti_snimi IS NOT NULL "
        f"ORDER BY {order_expr} "
        f"LIMIT {limit_n}"
    )


def _sql_top_growth_alamoment(spec: AnalysisSpec, table_id: str) -> str:
    year_from, year_to = _effective_years(spec)
    limit_n = _top_limit(spec)
    order_expr = _order_expression(spec)
    return (
        "WITH yearly AS ("
        "  SELECT "
        "    SAFE_CAST(`Vuosi` AS INT64) AS vuosi, "
        "    NULLIF(`Momentti_TunnusP`, '') AS momentti_tunnusp, "
        "    NULLIF(`Momentti_sNimi`, '') AS momentti_snimi, "
        "    NULLIF(`TakpMrL_Tunnus`, '') AS alamomentti_tunnus, "
        "    NULLIF(`TakpMrL_sNimi`, '') AS alamomentti_snimi, "
        "    SUM(SAFE_CAST(`Nettokertymä` AS NUMERIC)) AS nettokertyma_sum "
        f"  FROM `{table_id}` "
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
        "WHERE (alamomentti_tunnus IS NOT NULL OR alamomentti_snimi IS NOT NULL OR momentti_tunnusp IS NOT NULL) "
        f"ORDER BY {order_expr} "
        f"LIMIT {limit_n}"
    )


def _sql_trend_by_hallinnonala(spec: AnalysisSpec, table_id: str) -> str:
    year_from, year_to = _effective_years(spec)
    return (
        "SELECT "
        "  SAFE_CAST(`Vuosi` AS INT64) AS vuosi, "
        "  `Hallinnonala` AS hallinnonala, "
        "  SUM(SAFE_CAST(`Nettokertymä` AS NUMERIC)) AS nettokertyma_sum "
        f"FROM `{table_id}` "
        f"WHERE SAFE_CAST(`Vuosi` AS INT64) BETWEEN {year_from} AND {year_to} "
        "GROUP BY vuosi, hallinnonala "
        "ORDER BY vuosi ASC, nettokertyma_sum DESC "
        "LIMIT 500"
    )


def _sql_yoy_change(spec: AnalysisSpec, table_id: str) -> str:
    year_from, year_to = _effective_years(spec)
    return (
        "WITH yearly AS ("
        "  SELECT "
        "    SAFE_CAST(`Vuosi` AS INT64) AS vuosi, "
        "    SUM(SAFE_CAST(`Nettokertymä` AS NUMERIC)) AS nettokertyma_sum "
        f"  FROM `{table_id}` "
        f"  WHERE SAFE_CAST(`Vuosi` AS INT64) BETWEEN {year_from} AND {year_to} "
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


def build_contract_sql(spec: AnalysisSpec, table_id: str) -> tuple[str | None, str | None]:
    contract_name = choose_contract(spec)
    if contract_name == "top_growth_moment":
        return _sql_top_growth_moment(spec, table_id), contract_name
    if contract_name == "top_growth_alamoment":
        return _sql_top_growth_alamoment(spec, table_id), contract_name
    if contract_name == "trend_by_hallinnonala":
        return _sql_trend_by_hallinnonala(spec, table_id), contract_name
    if contract_name == "yoy_change":
        return _sql_yoy_change(spec, table_id), contract_name
    return None, None

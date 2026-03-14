from __future__ import annotations

import pandas as pd

from utils.analysis_spec_utils import AnalysisSpec, DATA_MAX_YEAR, DATA_MIN_YEAR

CANONICAL_COLUMNS = ("time", "entity", "metric", "delta", "pct")
CONTRACT_TEMPLATE_MAP = {
    "top_growth_moment": ["top_growth", "top_categories"],
    "top_growth_alamoment": ["top_growth", "top_categories"],
    "trend_by_hallinnonala": ["trend", "top_categories"],
    "yoy_change": ["trend", "growth"],
}


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
        "ORDER BY vuosi "
        "LIMIT 400"
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


def contract_template_order(contract_name: str | None) -> list[str]:
    if not contract_name:
        return []
    return list(CONTRACT_TEMPLATE_MAP.get(contract_name, []))


def _to_numeric(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")
    cleaned = (
        series.astype(str)
        .str.replace("\u00a0", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def _coalesce_text(df: pd.DataFrame, columns: list[str], fallback: str) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="object")
    acc = pd.Series([""] * len(df), index=df.index, dtype="object")
    for column in columns:
        if column not in df.columns:
            continue
        values = df[column].fillna("").astype(str).str.strip()
        acc = acc.mask(acc == "", values)
    acc = acc.mask(acc == "", fallback)
    return acc


def _combine_entity_from_cols(
    df: pd.DataFrame,
    left_cols: list[str],
    right_cols: list[str],
    fallback: str,
) -> pd.Series:
    left = _coalesce_text(df, left_cols, "")
    right = _coalesce_text(df, right_cols, "")
    combined = left
    both = (left != "") & (right != "")
    combined = combined.mask(both, left + " / " + right)
    combined = combined.mask((combined == "") & (right != ""), right)
    combined = combined.mask(combined == "", fallback)
    return combined


def _empty_canonical_frame(df: pd.DataFrame) -> pd.DataFrame:
    empty = pd.DataFrame(index=df.index)
    for column in CANONICAL_COLUMNS:
        empty[column] = pd.NA
    return empty


def _canonical_top_growth_moment(df: pd.DataFrame, spec: AnalysisSpec) -> pd.DataFrame:
    out = _empty_canonical_frame(df)
    out["time"] = int(spec.time_to) if spec.time_to is not None else pd.NA
    out["entity"] = _coalesce_text(df, ["momentti_snimi", "momentti_tunnusp"], "Tuntematon momentti")
    if "kasvu_eur" in df.columns:
        out["delta"] = _to_numeric(df["kasvu_eur"])
    if "kasvu_pct" in df.columns:
        out["pct"] = _to_numeric(df["kasvu_pct"])
    out["metric"] = out["pct"] if spec.growth_type == "pct" else out["delta"]
    return out


def _canonical_top_growth_alamoment(df: pd.DataFrame, spec: AnalysisSpec) -> pd.DataFrame:
    out = _empty_canonical_frame(df)
    out["time"] = int(spec.time_to) if spec.time_to is not None else pd.NA
    out["entity"] = _combine_entity_from_cols(
        df,
        left_cols=["momentti_snimi", "momentti_tunnusp"],
        right_cols=["alamomentti_snimi", "alamomentti_tunnus"],
        fallback="Tuntematon momentti/alamomentti",
    )
    if "kasvu_eur" in df.columns:
        out["delta"] = _to_numeric(df["kasvu_eur"])
    if "kasvu_pct" in df.columns:
        out["pct"] = _to_numeric(df["kasvu_pct"])
    out["metric"] = out["pct"] if spec.growth_type == "pct" else out["delta"]
    return out


def _canonical_trend_by_hallinnonala(df: pd.DataFrame) -> pd.DataFrame:
    out = _empty_canonical_frame(df)
    if "vuosi" in df.columns:
        out["time"] = _to_numeric(df["vuosi"]).astype("Int64")
    out["entity"] = _coalesce_text(df, ["hallinnonala"], "Tuntematon hallinnonala")
    if "nettokertyma_sum" in df.columns:
        out["metric"] = _to_numeric(df["nettokertyma_sum"])
    out = out.sort_values(["entity", "time"], na_position="last").reset_index(drop=True)
    out["delta"] = out.groupby("entity", dropna=False)["metric"].diff()
    prev = out.groupby("entity", dropna=False)["metric"].shift(1).abs()
    out["pct"] = (out["delta"] / prev) * 100
    return out


def _canonical_yoy_change(df: pd.DataFrame) -> pd.DataFrame:
    out = _empty_canonical_frame(df)
    if "vuosi" in df.columns:
        out["time"] = _to_numeric(df["vuosi"]).astype("Int64")
    out["entity"] = "Kokonais"
    if "nettokertyma_sum" in df.columns:
        out["metric"] = _to_numeric(df["nettokertyma_sum"])
    if "muutos_eur" in df.columns:
        out["delta"] = _to_numeric(df["muutos_eur"])
    if "muutos_pct" in df.columns:
        out["pct"] = _to_numeric(df["muutos_pct"])
    return out


def normalize_contract_result(
    df: pd.DataFrame,
    contract_name: str | None,
    spec: AnalysisSpec,
) -> pd.DataFrame:
    if df is None or df.empty or not contract_name:
        return pd.DataFrame(columns=list(CANONICAL_COLUMNS))

    if contract_name == "top_growth_moment":
        out = _canonical_top_growth_moment(df, spec)
    elif contract_name == "top_growth_alamoment":
        out = _canonical_top_growth_alamoment(df, spec)
    elif contract_name == "trend_by_hallinnonala":
        out = _canonical_trend_by_hallinnonala(df)
    elif contract_name == "yoy_change":
        out = _canonical_yoy_change(df)
    else:
        return pd.DataFrame(columns=list(CANONICAL_COLUMNS))

    for column in CANONICAL_COLUMNS:
        if column not in out.columns:
            out[column] = pd.NA
    return out[list(CANONICAL_COLUMNS)]

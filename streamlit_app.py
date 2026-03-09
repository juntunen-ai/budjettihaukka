import streamlit as st
from config import settings
from utils import bigquery_utils
from utils.analysis_spec_utils import (
    AnalysisSpec,
    apply_clarifications_to_question,
    coverage_notice,
    infer_analysis_spec,
    renderable_summary,
)
from utils.visualization_plan_utils import extract_intent_signals, template_order
import streamlit.components.v1 as components
import altair as alt
import pandas as pd
import random

def _format_bytes(num_bytes):
    if not num_bytes:
        return "0 B"
    unit = 1024.0
    value = float(num_bytes)
    for suffix in ("B", "KB", "MB", "GB", "TB", "PB"):
        if value < unit or suffix == "PB":
            return f"{value:.2f} {suffix}"
        value /= unit
    return f"{num_bytes} B"


def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    columns_map = {col.lower(): col for col in df.columns}
    for candidate in candidates:
        found = columns_map.get(candidate.lower())
        if found:
            return found
    return None


def render_banner_ad(slot_id: str, fallback_label: str) -> None:
    if not settings.show_ads:
        return

    if settings.has_adsense and slot_id:
        ad_html = f"""
        <script async src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client={settings.adsense_client_id}" crossorigin="anonymous"></script>
        <ins class="adsbygoogle"
             style="display:block"
             data-ad-client="{settings.adsense_client_id}"
             data-ad-slot="{slot_id}"
             data-ad-format="auto"
             data-full-width-responsive="true"></ins>
        <script>
             (adsbygoogle = window.adsbygoogle || []).push({{}});
        </script>
        """
        components.html(ad_html, height=140)
        return

    st.markdown(
        f"""
        <div style="border:1px dashed #999;padding:12px;border-radius:8px;text-align:center;color:#666;font-size:0.9rem;">
          {settings.ad_placeholder_text} ({fallback_label})
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_usage_meter() -> None:
    if "queries_used" not in st.session_state:
        st.session_state["queries_used"] = 0

    limit = max(settings.free_queries_per_session, 0)
    used = int(st.session_state.get("queries_used", 0))

    if limit == 0:
        st.sidebar.caption("Kyselykiintiö: rajoittamaton")
        return

    remaining = max(limit - used, 0)
    st.sidebar.metric("Ilmaiset kyselyt", f"{remaining}/{limit}")
    st.sidebar.progress(min(used / limit, 1.0))


def can_run_more_queries() -> bool:
    limit = max(settings.free_queries_per_session, 0)
    if limit == 0:
        return True
    used = int(st.session_state.get("queries_used", 0))
    return used < limit


def render_query_cost_stats() -> None:
    if settings.use_google_sheets_demo:
        return
    stats = bigquery_utils.get_last_query_stats()
    if not stats:
        return

    estimated = _format_bytes(stats.get("estimated_bytes"))
    processed = _format_bytes(stats.get("processed_bytes"))
    billed = _format_bytes(stats.get("billed_bytes"))
    cache_hit = "kyllä" if stats.get("cache_hit") else "ei"
    st.caption(
        f"Kustannusarvio: estimoitu {estimated} | prosessoitu {processed} | laskutettu {billed} | välimuisti: {cache_hit}"
    )


def render_insight_cards(df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return

    rows_col, cols_col, period_col = st.columns(3)
    rows_col.metric("Rivejä", f"{len(df):,}".replace(",", " "))
    cols_col.metric("Sarakkeita", str(len(df.columns)))

    year_col = _find_column(df, ["vuosi", "Vuosi"])
    month_col = _find_column(df, ["kk", "Kk"])
    period_text = "ei tunnistettu"
    if year_col:
        years = pd.to_numeric(df[year_col], errors="coerce").dropna()
        if not years.empty:
            y_min = int(years.min())
            y_max = int(years.max())
            if month_col:
                months = pd.to_numeric(df[month_col], errors="coerce").dropna()
                if not months.empty:
                    m_min = int(months.min())
                    m_max = int(months.max())
                    period_text = f"{y_min}-{m_min:02d} ... {y_max}-{m_max:02d}"
                else:
                    period_text = f"{y_min} ... {y_max}"
            else:
                period_text = f"{y_min} ... {y_max}"
    period_col.metric("Ajanjakso", period_text)

    hallinnonala_col = _find_column(df, ["hallinnonala", "Hallinnonala"])
    net_col = _find_column(df, ["nettokertyma", "Nettokertymä", "nettokertyma_sum"])
    if hallinnonala_col and net_col:
        tmp = df[[hallinnonala_col, net_col]].copy()
        tmp[net_col] = pd.to_numeric(tmp[net_col], errors="coerce")
        grouped = tmp.dropna().groupby(hallinnonala_col, as_index=False)[net_col].sum()
        if not grouped.empty:
            top_row = grouped.sort_values(net_col, ascending=False).iloc[0]
            st.caption(
                f"Suurin nettokertymä tuloksessa: {top_row[hallinnonala_col]} ({top_row[net_col]:,.2f})".replace(",", " ")
            )


def _format_year_range(year_from: int | None, year_to: int | None) -> str:
    if year_from is None or year_to is None:
        return "ei määritelty"
    return str(year_from) if year_from == year_to else f"{year_from}-{year_to}"


def _format_missing_years(spec: AnalysisSpec) -> str:
    if spec.requested_time_from is None or spec.requested_time_to is None:
        return "ei pyydetty"
    if spec.time_from is None or spec.time_to is None:
        return f"{spec.requested_time_from}-{spec.requested_time_to}"
    requested = set(range(spec.requested_time_from, spec.requested_time_to + 1))
    effective = set(range(spec.time_from, spec.time_to + 1))
    missing = sorted(requested - effective)
    if not missing:
        return "0"
    if len(missing) <= 6:
        return ", ".join(str(y) for y in missing)
    return f"{missing[0]}...{missing[-1]} ({len(missing)} v)"


def render_scope_cards(spec: AnalysisSpec) -> None:
    req_col, eff_col, miss_col = st.columns(3)
    req_col.metric("Pyydetty aikaväli", _format_year_range(spec.requested_time_from, spec.requested_time_to))
    eff_col.metric("Käytetty aikaväli", _format_year_range(spec.time_from, spec.time_to))
    miss_col.metric("Puuttuvat vuodet", _format_missing_years(spec))


def render_interpretation_block(question: str) -> tuple[AnalysisSpec, dict[str, str]]:
    spec = infer_analysis_spec(question)
    st.markdown("**Tulkinta ennen ajoa**")
    st.caption(renderable_summary(spec))
    render_scope_cards(spec)

    if spec.assumptions:
        assumptions = " | ".join(spec.assumptions)
        st.caption(f"Oletukset: {assumptions}")

    coverage = coverage_notice(spec)
    if coverage:
        st.caption(f"Huomio: {coverage}")

    selections: dict[str, str] = {}
    if spec.clarifications:
        st.markdown("**Tarkenna (valinnainen)**")
        for field in spec.clarifications:
            try:
                default_idx = field.options.index(field.recommended)
            except ValueError:
                default_idx = 0
            selected = st.radio(
                field.question,
                field.options,
                index=default_idx,
                horizontal=True,
                key=f"clarify_{field.field}",
            )
            selections[field.field] = selected
    return spec, selections

def generate_sample_budget_data():
    """
    Generoi esimerkkidataa budjettidatasta visualisoinnin testaamiseen.
    
    Returns:
        pd.DataFrame: Esimerkkibudjettidataa
    """
    # Luodaan esimerkki kategoriat
    categories = [
        'Puolustusministeriö', 
        'Opetusministeriö', 
        'Sosiaali- ja terveysministeriö',
        'Liikenne- ja viestintäministeriö',
        'Valtiovarainministeriö',
        'Ympäristöministeriö'
    ]
    
    # Luodaan vuodet
    years = list(range(2018, 2025))
    
    # Alustetaan data lista
    data = []
    
    # Luodaan jokaiselle kategorialle ja vuodelle dataa
    for category in categories:
        base_amount = random.randint(100, 1000) * 1000000  # Perusmäärä miljoonissa
        
        for year in years:
            # Lisätään hieman satunnaisuutta, mutta pidetään trendi
            yearly_change = random.uniform(-0.1, 0.2)  # -10% to +20% vuosimuutos
            amount = base_amount * (1 + yearly_change)
            
            # Lisätään vuosineljännes data
            for quarter in range(1, 5):
                quarterly_amount = amount / 4 * (1 + random.uniform(-0.05, 0.05))
                
                data.append({
                    'Vuosi': year,
                    'Vuosineljännes': quarter,
                    'Ministeriö': category,
                    'Määräraha_EUR': round(quarterly_amount, 2),
                    'Päivämäärä': f"{year}-{quarter*3:02d}-01"
                })
                
        # Lisätään vaihtelua perusmäärään seuraavaa kategoriaa varten
        base_amount = base_amount * (1 + random.uniform(-0.3, 0.3))
    
    # Luodaan DataFrame
    df = pd.DataFrame(data)
    
    # Muunnetaan päivämäärä-sarake datetime-tyyppiseksi
    df['Päivämäärä'] = pd.to_datetime(df['Päivämäärä'])
    
    return df

def _to_numeric_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")
    cleaned = (
        series.astype(str)
        .str.replace("\u00a0", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def _numeric_from_any(values) -> pd.Series:
    if isinstance(values, pd.DataFrame):
        return pd.to_numeric(pd.Series(values.to_numpy().ravel()), errors="coerce")
    if isinstance(values, pd.Series):
        return pd.to_numeric(values, errors="coerce")
    return pd.to_numeric(pd.Series(values), errors="coerce")


def _choose_euro_scale(values) -> tuple[float, str]:
    numeric = _numeric_from_any(values).dropna()
    if numeric.empty:
        return 1.0, "€"
    max_abs = numeric.abs().max()
    if max_abs >= 1_000_000_000:
        return 1_000_000_000.0, "mrd €"
    if max_abs >= 1_000_000:
        return 1_000_000.0, "milj. €"
    if max_abs >= 1_000:
        return 1_000.0, "t€"
    return 1.0, "€"


def _year_index_as_str(index) -> pd.Index:
    numeric = pd.to_numeric(pd.Series(index), errors="coerce")
    labels = []
    for original, value in zip(index, numeric):
        if pd.notna(value):
            labels.append(str(int(value)))
        else:
            labels.append(str(original))
    return pd.Index(labels)


def _format_number(value, decimals: int = 2, suffix: str = "") -> str:
    if pd.isna(value):
        return ""
    rendered = f"{float(value):,.{decimals}f}".replace(",", " ")
    return f"{rendered}{suffix}"


def _truncate_label(value: str, max_len: int = 60) -> str:
    text = str(value or "").strip()
    if len(text) <= max_len:
        return text
    return f"{text[: max_len - 1]}…"


def _title_with_scope(base: str, spec: AnalysisSpec) -> str:
    scope = _format_year_range(spec.time_from, spec.time_to)
    return base if scope == "ei määritelty" else f"{base} ({scope})"


def _pick_value_column(df: pd.DataFrame) -> str | None:
    preferred = [
        "kasvu_eur",
        "kasvu_pct",
        "nettokertyma_sum",
        "nettokertyma",
        "Nettokertymä",
        "Määräraha_EUR",
        "muutos_eur",
    ]
    col = _find_column(df, preferred)
    if col:
        return col

    numeric_candidates = []
    for column in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[column]):
            continue
        numeric = _to_numeric_series(df[column])
        if numeric.notna().sum() >= max(3, len(df) // 3):
            numeric_candidates.append(column)
    return numeric_candidates[0] if numeric_candidates else None


def _pick_time_columns(df: pd.DataFrame) -> tuple[str | None, str | None, str | None]:
    date_candidates = ["period_date", "Päivämäärä", "paivamaara", "date"]
    date_col = _find_column(df, date_candidates)
    if not date_col:
        for column in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[column]):
                date_col = column
                break

    year_col = _find_column(df, ["vuosi", "Vuosi", "year"])
    month_col = _find_column(df, ["kk", "Kk", "month", "kuukausi"])
    return date_col, year_col, month_col


def _pick_category_column(df: pd.DataFrame) -> str | None:
    preferred = [
        "hallinnonala",
        "Hallinnonala",
        "Ministeriö",
        "kirjanpitoyksikko",
        "Kirjanpitoyksikkö",
        "momentti_snimi",
        "alamomentti_snimi",
    ]
    columns_map = {col.lower(): col for col in df.columns}
    for candidate in preferred:
        col = columns_map.get(candidate.lower())
        if not col:
            continue
        # Skip metric-like columns even when dtype is object (e.g. Decimal from BigQuery).
        if _to_numeric_series(df[col]).notna().mean() >= 0.8:
            continue
        unique_count = df[col].nunique(dropna=True)
        non_null_count = df[col].notna().sum()
        if non_null_count > 0 and unique_count >= 2:
            return col

    for column in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[column]) or pd.api.types.is_numeric_dtype(df[column]):
            continue
        if _to_numeric_series(df[column]).notna().mean() >= 0.8:
            continue
        unique_count = df[column].nunique(dropna=True)
        if 2 <= unique_count <= 20:
            return column
    return None


def _build_time_axis(df: pd.DataFrame, date_col: str | None, year_col: str | None, month_col: str | None) -> pd.Series | None:
    if date_col:
        return pd.to_datetime(df[date_col], errors="coerce")
    if year_col and month_col:
        years = _to_numeric_series(df[year_col]).astype("Int64")
        months = _to_numeric_series(df[month_col]).astype("Int64")
        return pd.to_datetime(
            {
                "year": years,
                "month": months,
                "day": 1,
            },
            errors="coerce",
        )
    if year_col:
        years = _to_numeric_series(df[year_col]).astype("Int64")
        return pd.to_datetime({"year": years, "month": 1, "day": 1}, errors="coerce")
    return None


def _render_top_growth_template(
    work: pd.DataFrame,
    spec: AnalysisSpec,
    value_col: str | None,
    growth_pct_col: str | None,
    growth_eur_col: str | None,
) -> bool:
    label_col = _find_column(
        work,
        [
            "alamomentti_snimi",
            "momentti_snimi",
            "hallinnonala",
            "kirjanpitoyksikko",
            "Ministeriö",
        ],
    )
    if not label_col:
        return False

    if spec.growth_type == "pct" and growth_pct_col:
        metric_col = growth_pct_col
    else:
        metric_col = growth_eur_col or value_col
    if not metric_col:
        return False

    rank_df = work[[label_col, metric_col]].copy()
    rank_df[metric_col] = _to_numeric_series(rank_df[metric_col])
    rank_df[label_col] = rank_df[label_col].astype(str).str.strip()
    rank_df = rank_df.dropna(subset=[label_col, metric_col])
    rank_df = rank_df[rank_df[label_col] != ""]
    if rank_df.empty or rank_df[label_col].nunique() < 2:
        return False

    agg_func = "mean" if metric_col == growth_pct_col else "sum"
    ranked = (
        rank_df.groupby(label_col, as_index=False)[metric_col]
        .agg(agg_func)
        .sort_values(metric_col, ascending=False)
        .head(spec.ranking_n or 10)
    )
    if ranked.empty:
        return False

    ranked = ranked.rename(columns={label_col: "label", metric_col: "metric"})
    ranked["label"] = ranked["label"].astype(str)
    ranked["label_short"] = ranked["label"].apply(_truncate_label)

    unit = "%"
    tooltip_format = ",.2f"
    ranked["plot_metric"] = ranked["metric"]
    if metric_col != growth_pct_col:
        scale, unit = _choose_euro_scale(ranked["metric"])
        ranked["plot_metric"] = ranked["metric"] / scale

    st.markdown(f"**{_title_with_scope('Eniten kasvaneet kategoriat', spec)}**")
    st.caption(f"X-akseli: kasvu ({unit}) | Y-akseli: kategoria")

    top_chart = (
        alt.Chart(ranked)
        .mark_bar(color="#1565c0")
        .encode(
            y=alt.Y("label_short:N", sort="-x", title=""),
            x=alt.X("plot_metric:Q", title=f"Kasvu ({unit})"),
            tooltip=[
                alt.Tooltip("label:N", title="Kategoria"),
                alt.Tooltip("metric:Q", title="Kasvu", format=tooltip_format),
            ],
        )
        .properties(height=max(280, min(700, 28 * len(ranked))))
    )
    st.altair_chart(top_chart, use_container_width=True)

    detail_cols = [
        col
        for col in (
            "momentti_tunnusp",
            "momentti_snimi",
            "alamomentti_tunnus",
            "alamomentti_snimi",
            "alkuvuosi_sum",
            "loppuvuosi_sum",
            "kasvu_eur",
            "kasvu_pct",
        )
        if col in work.columns
    ]
    if detail_cols:
        details = work[detail_cols].copy()
        for euro_col in ("alkuvuosi_sum", "loppuvuosi_sum", "kasvu_eur"):
            if euro_col in details.columns:
                details[euro_col] = _to_numeric_series(details[euro_col]).map(lambda x: _format_number(x, 2))
        if "kasvu_pct" in details.columns:
            details["kasvu_pct"] = _to_numeric_series(details["kasvu_pct"]).map(lambda x: _format_number(x, 2, " %"))
        st.markdown("**Kasvutaulukko**")
        st.dataframe(details.head(spec.ranking_n or 10), width="stretch")
    return True


def _render_trend_template(
    work: pd.DataFrame,
    spec: AnalysisSpec,
    value_col: str | None,
    year_col: str | None,
) -> bool:
    if not value_col:
        return False

    if year_col:
        trend_df = work[[year_col, value_col]].copy()
        trend_df[year_col] = _to_numeric_series(trend_df[year_col]).astype("Int64")
        trend_df[value_col] = _to_numeric_series(trend_df[value_col])
        trend_df = trend_df.dropna(subset=[year_col, value_col])
        if trend_df.empty or trend_df[year_col].nunique() < 2:
            return False
        series = trend_df.groupby(year_col)[value_col].sum().sort_index()
        scale, unit = _choose_euro_scale(series)
        chart_df = series.reset_index(name="summa")
        chart_df["summa_scaled"] = chart_df["summa"] / scale
        chart_df["vuosi"] = chart_df[year_col].astype(int).astype(str)
        st.markdown(f"**{_title_with_scope('Trendikuva', spec)}**")
        st.caption(f"X-akseli: vuosi | Y-akseli: summa ({unit})")
        trend_chart = (
            alt.Chart(chart_df)
            .mark_line(point=True, color="#1565c0")
            .encode(
                x=alt.X("vuosi:N", title="Vuosi"),
                y=alt.Y("summa_scaled:Q", title=f"Summa ({unit})"),
                tooltip=[
                    alt.Tooltip("vuosi:N", title="Vuosi"),
                    alt.Tooltip("summa:Q", title="Summa", format=",.2f"),
                ],
            )
        )
        st.altair_chart(trend_chart, use_container_width=True)
        return True

    if "_time_axis" not in work.columns:
        return False

    trend_df = work[["_time_axis", value_col]].copy()
    trend_df[value_col] = _to_numeric_series(trend_df[value_col])
    trend_df = trend_df.dropna(subset=["_time_axis", value_col])
    if trend_df.empty:
        return False
    series = trend_df.groupby("_time_axis")[value_col].sum().sort_index()
    scale, unit = _choose_euro_scale(series)
    chart_df = series.reset_index(name="summa")
    chart_df["summa_scaled"] = chart_df["summa"] / scale
    st.markdown(f"**{_title_with_scope('Trendikuva', spec)}**")
    st.caption(f"X-akseli: aika | Y-akseli: summa ({unit})")
    trend_chart = (
        alt.Chart(chart_df)
        .mark_line(point=True, color="#1565c0")
        .encode(
            x=alt.X("_time_axis:T", title="Aika"),
            y=alt.Y("summa_scaled:Q", title=f"Summa ({unit})"),
            tooltip=[
                alt.Tooltip("_time_axis:T", title="Aika"),
                alt.Tooltip("summa:Q", title="Summa", format=",.2f"),
            ],
        )
    )
    st.altair_chart(trend_chart, use_container_width=True)
    return True


def _render_growth_template(
    work: pd.DataFrame,
    spec: AnalysisSpec,
    value_col: str | None,
    year_col: str | None,
    growth_pct_col: str | None,
    growth_eur_col: str | None,
) -> bool:
    if not year_col:
        return False

    if spec.growth_type == "pct" and growth_pct_col:
        growth_col = growth_pct_col
    elif growth_eur_col:
        growth_col = growth_eur_col
    else:
        growth_col = None

    growth_df = work.copy()
    growth_df[year_col] = _to_numeric_series(growth_df[year_col]).astype("Int64")
    growth_df = growth_df.dropna(subset=[year_col])

    if growth_col:
        growth_df[growth_col] = _to_numeric_series(growth_df[growth_col])
        growth_df = growth_df.dropna(subset=[growth_col])
        if growth_df.empty:
            return False
        growth_series = growth_df.groupby(year_col)[growth_col].mean().sort_index()
        if growth_col == growth_pct_col:
            unit = "%"
            scaled = growth_series
        else:
            scale, unit = _choose_euro_scale(growth_series)
            scaled = growth_series / scale
    else:
        if not value_col:
            return False
        growth_df[value_col] = _to_numeric_series(growth_df[value_col])
        growth_df = growth_df.dropna(subset=[value_col])
        if growth_df.empty or growth_df[year_col].nunique() < 2:
            return False
        yearly = growth_df.groupby(year_col)[value_col].sum().sort_index()
        growth_series = yearly.pct_change() * 100
        growth_series = growth_series.dropna()
        if growth_series.empty:
            return False
        unit = "%"
        scaled = growth_series

    chart_df = scaled.reset_index(name="value")
    chart_df["vuosi"] = chart_df[year_col].astype(int).astype(str)

    st.markdown(f"**{_title_with_scope('Kasvu vuositasolla', spec)}**")
    st.caption(f"X-akseli: vuosi | Y-akseli: kasvu ({unit})")
    growth_chart = (
        alt.Chart(chart_df)
        .mark_bar(color="#1976d2")
        .encode(
            x=alt.X("vuosi:N", title="Vuosi"),
            y=alt.Y("value:Q", title=f"Kasvu ({unit})"),
            tooltip=[
                alt.Tooltip("vuosi:N", title="Vuosi"),
                alt.Tooltip("value:Q", title="Kasvu", format=",.2f"),
            ],
        )
    )
    st.altair_chart(growth_chart, use_container_width=True)
    return True


def _render_composition_template(
    work: pd.DataFrame,
    spec: AnalysisSpec,
    value_col: str | None,
    year_col: str | None,
    category_col: str | None,
) -> bool:
    if not value_col or not year_col or not category_col:
        return False
    comp_df = work[[year_col, category_col, value_col]].copy()
    comp_df[year_col] = _to_numeric_series(comp_df[year_col]).astype("Int64")
    comp_df[value_col] = _to_numeric_series(comp_df[value_col])
    comp_df = comp_df.dropna(subset=[year_col, category_col, value_col])
    if comp_df.empty:
        return False
    if comp_df[category_col].nunique() < 2 or comp_df[category_col].nunique() > 10:
        return False

    pivot = comp_df.pivot_table(index=year_col, columns=category_col, values=value_col, aggfunc="sum").sort_index()
    if pivot.empty:
        return False
    scale, unit = _choose_euro_scale(pivot)
    pivot = pivot / scale
    pivot.index = _year_index_as_str(pivot.index)

    st.markdown(f"**{_title_with_scope('Rakenteen kehitys', spec)}**")
    st.caption(f"X-akseli: vuosi | Y-akseli: summa ({unit}) | Väri: kategoria")
    st.area_chart(pivot)
    return True


def _render_seasonality_template(
    work: pd.DataFrame,
    spec: AnalysisSpec,
    value_col: str | None,
    year_col: str | None,
    month_col: str | None,
) -> bool:
    if not value_col or not year_col or not month_col:
        return False
    season_df = work[[year_col, month_col, value_col]].copy()
    season_df[year_col] = _to_numeric_series(season_df[year_col]).astype("Int64")
    season_df[month_col] = _to_numeric_series(season_df[month_col]).astype("Int64")
    season_df[value_col] = _to_numeric_series(season_df[value_col])
    season_df = season_df.dropna(subset=[year_col, month_col, value_col])
    if season_df.empty or season_df[month_col].nunique() < 6:
        return False

    heat = season_df.pivot_table(index=year_col, columns=month_col, values=value_col, aggfunc="sum").sort_index()
    if heat.empty:
        return False
    scale, unit = _choose_euro_scale(heat)
    heat = heat / scale
    st.markdown(f"**{_title_with_scope('Kausivaihtelu (vuosi x kuukausi)', spec)}**")
    st.caption(f"Taulukon yksikkö: {unit}")
    st.dataframe(heat.style.format("{:,.2f}").background_gradient(cmap="Blues"), width="stretch")
    return True


def _render_top_categories_template(
    work: pd.DataFrame,
    spec: AnalysisSpec,
    value_col: str | None,
    category_col: str | None,
) -> bool:
    if not value_col or not category_col:
        return False
    if value_col == category_col:
        return False

    top_df = work[[category_col, value_col]].copy()
    top_df[value_col] = _to_numeric_series(top_df[value_col])
    top_df = top_df.dropna(subset=[category_col, value_col])
    if top_df.empty or top_df[category_col].nunique() < 2:
        return False

    grouped = (
        top_df.groupby(category_col, as_index=False)[value_col]
        .sum()
        .sort_values(value_col, ascending=False)
        .head(spec.ranking_n or 10)
    )
    grouped = grouped.rename(columns={category_col: "label", value_col: "value"})
    grouped["label"] = grouped["label"].astype(str)
    grouped["label_short"] = grouped["label"].apply(_truncate_label)
    scale, unit = _choose_euro_scale(grouped["value"])
    grouped["value_scaled"] = grouped["value"] / scale

    st.markdown(f"**{_title_with_scope('Top kategoriat', spec)}**")
    st.caption(f"X-akseli: summa ({unit}) | Y-akseli: kategoria")
    chart = (
        alt.Chart(grouped)
        .mark_bar(color="#1976d2")
        .encode(
            y=alt.Y("label_short:N", sort="-x", title=""),
            x=alt.X("value_scaled:Q", title=f"Summa ({unit})"),
            tooltip=[
                alt.Tooltip("label:N", title="Kategoria"),
                alt.Tooltip("value:Q", title="Summa", format=",.2f"),
            ],
        )
        .properties(height=max(250, min(650, 28 * len(grouped))))
    )
    st.altair_chart(chart, use_container_width=True)
    return True


def visualize_data(
    df: pd.DataFrame,
    title: str = "Budjettidata visualisointi",
    question: str = "",
    analysis_spec: AnalysisSpec | None = None,
):
    st.subheader(title)
    if df is None or df.empty:
        st.warning("Ei dataa visualisoitavaksi.")
        return

    spec = analysis_spec if isinstance(analysis_spec, AnalysisSpec) else infer_analysis_spec(question)
    intent_signals = extract_intent_signals(question)
    templates = template_order(spec, intent_signals)

    work = df.copy()
    date_col, year_col, month_col = _pick_time_columns(work)
    category_col = _pick_category_column(work)
    value_col = _pick_value_column(work)
    growth_pct_col = _find_column(work, ["kasvu_pct", "muutos_pct", "growth_pct", "yoy_pct"])
    growth_eur_col = _find_column(work, ["kasvu_eur", "muutos_eur", "growth_eur", "delta_eur"])

    if value_col:
        work[value_col] = _to_numeric_series(work[value_col])
    if growth_pct_col:
        work[growth_pct_col] = _to_numeric_series(work[growth_pct_col])
    if growth_eur_col:
        work[growth_eur_col] = _to_numeric_series(work[growth_eur_col])

    time_axis = _build_time_axis(work, date_col, year_col, month_col)
    if time_axis is not None:
        work["_time_axis"] = time_axis

    rendered_templates: list[str] = []
    for template in templates:
        if template == "top_growth":
            ok = _render_top_growth_template(work, spec, value_col, growth_pct_col, growth_eur_col)
        elif template == "trend":
            ok = _render_trend_template(work, spec, value_col, year_col)
        elif template == "growth":
            ok = _render_growth_template(work, spec, value_col, year_col, growth_pct_col, growth_eur_col)
        elif template == "composition":
            ok = _render_composition_template(work, spec, value_col, year_col, category_col)
        elif template == "seasonality":
            ok = _render_seasonality_template(work, spec, value_col, year_col, month_col)
        elif template == "top_categories":
            ok = _render_top_categories_template(work, spec, value_col, category_col)
        else:
            ok = False
        if ok:
            rendered_templates.append(template)
        if spec.intent == "top_growth" and len(rendered_templates) >= 2:
            # Top growth -kyselyissä pidetään visualisointi fokusoituna.
            break
        if len(rendered_templates) >= 3:
            break

    if not rendered_templates:
        st.info("Automaattinen visualisointi ei löytänyt sopivaa rakennetta. Näytetään taulukkodata.")
    else:
        st.caption(f"Käytetyt visualisointimallit: {', '.join(rendered_templates)}")

def main():
    st.set_page_config(page_title="Budjettihaukka", layout="wide")
    st.title("Budjettihaukka")
    st.write("Budjettihaukka on avoimen lähdekoodin web-sovellus, jonka tarkoituksena on tuoda talouspolitiikkaan liittyvä tieto helposti saataville, analysoitavaksi ja visualisoitavaksi. Sovelluksen käyttäjä voi esittää kysymyksiä luonnollisella kielellä, ja tekoälyn avulla saa kansantaloudelliseen optimaalisuuteen ja empiiriseen taloustutkimukseen perustuvia analyyseja. Tulokset voidaan näyttää taulukkoina, dynaamisina visualisointeina sekä analyyttisinä raportteina.")
    render_banner_ad(settings.adsense_slot_top, "Yläbanneri")

    # Lisätään diagnostiikkatila kehittäjille
    debug_mode = st.sidebar.checkbox("Kehittäjätila", value=False)
    source_label = "Google Sheets demo" if settings.use_google_sheets_demo else "BigQuery"
    st.sidebar.caption(f"Tietolähde: {source_label}")
    render_usage_meter()
    
    # Lisätään visualisoinnin testaustila
    test_visualization = st.sidebar.checkbox("Testaa visualisointia", value=False)
    
    if test_visualization:
        st.header("Visualisoinnin testaus")
        st.write("Tämä tila käyttää generoitua esimerkkidataa visualisoinnin testaamiseen.")
        
        # Generoidaan esimerkkidata
        sample_data = generate_sample_budget_data()
        
        # Näytetään esimerkkidata
        st.subheader("Esimerkkidata")
        st.dataframe(sample_data.head(10), width="stretch")
        
        # Visualisoidaan data
        visualize_data(sample_data, question="testidata visualisointi")
        
        # Tarjotaan CSV-latausmahdollisuus
        csv = sample_data.to_csv(index=False)
        st.download_button(
            label="Lataa esimerkkidata CSV-tiedostona",
            data=csv,
            file_name="budjettihaukka_esimerkkidata.csv",
            mime="text/csv"
        )
        
        return  # Palataan tästä, jos testataan visualisointia
    
    # Normaalin sovelluksen kulku tästä eteenpäin
    # Input for natural language question
    question = st.text_area("Kirjoita kysymyksesi:", placeholder="Esim. Mitkä olivat puolustusministeriön menot vuonna 2023?", height=100)
    clarification_choices: dict[str, str] = {}
    interpreted_spec: AnalysisSpec | None = None
    if question.strip():
        interpreted_spec, clarification_choices = render_interpretation_block(question)

    if st.button("Hae tulokset"):
        if not question.strip():
            st.warning("Ole hyvä ja kirjoita kysymys.")
            return

        if not can_run_more_queries():
            st.error(
                "Ilmaisversion kyselykiintiö on käytetty tässä sessiossa. "
                "Nollaa sessio tai ota käyttöön Pro-tila."
            )
            return

        st.session_state["queries_used"] = int(st.session_state.get("queries_used", 0)) + 1

        with st.spinner("Generoidaan SQL-kyselyä..."):
            try:
                # Näytetään kysymys selkeästi
                st.subheader("Esitetty kysymys:")
                st.info(question)

                execution_question = apply_clarifications_to_question(question, clarification_choices)
                if execution_question != question:
                    st.caption("Käytettiin käyttäjän tarkenteita analyysin suorittamiseen.")

                # Käsitellään kysymys yhdellä polulla (sisältää fallbackin).
                result = bigquery_utils.process_natural_language_query(execution_question)
                sql_query = result.get("sql_query", "")
                results = result.get("results_df")
                error = result.get("error")
                explanation = result.get("explanation")
                analysis_spec = result.get("analysis_spec") or interpreted_spec
                query_source = result.get("query_source")
                query_contract = result.get("query_contract")

                if explanation:
                    st.caption(explanation)
                if debug_mode and query_source:
                    st.caption(f"Kyselypolku: {query_source}" + (f" ({query_contract})" if query_contract else ""))

                if not sql_query:
                    st.error("SQL-kyselyn generointi epäonnistui. Kokeile muotoilla kysymyksesi toisin.")
                    return

                # Näytetään generoitu SQL kehittäjille tai debug-tilassa
                if debug_mode:
                    st.subheader("Generoitu SQL-kysely:")
                    st.code(sql_query, language="sql")

                if error:
                    st.error(f"Kyselyn suoritus epäonnistui: {error}")
                    return

                if results is not None and not results.empty:
                    render_query_cost_stats()
                    if isinstance(analysis_spec, AnalysisSpec):
                        render_scope_cards(analysis_spec)
                    st.subheader("Kyselyn tulokset:")
                    st.dataframe(results, width="stretch")
                    render_insight_cards(results)
                    
                    # Tarjotaan CSV-latausmahdollisuus
                    csv = results.to_csv(index=False)
                    st.download_button(
                        label="Lataa tulokset CSV-tiedostona",
                        data=csv,
                        file_name="budjettihaukka_tulokset.csv",
                        mime="text/csv"
                    )
                    
                    # Visualisoi tulokset
                    visualize_data(
                        results,
                        title="Kyselyn tulokset visualisoituna",
                        question=execution_question,
                        analysis_spec=analysis_spec if isinstance(analysis_spec, AnalysisSpec) else None,
                    )
                else:
                    st.warning("Kysely ei palauttanut tuloksia. Kokeile muokata kysymystäsi.")
            
            except Exception as e:
                st.error(f"Virhe sovelluksessa: {str(e)}")
                if debug_mode:
                    st.exception(e)

    # Alatunnisteen näyttäminen
    render_banner_ad(settings.adsense_slot_bottom, "Alabanneri")
    st.markdown("---")
    st.markdown("Budjettihaukka | Powered by LangGraph & Vertex AI © 2025")

if __name__ == "__main__":
    main()

import streamlit as st
from config import settings
from utils import bigquery_utils
from api.client import BudgetAnalyticsClient
from domain.contracts import deserialize_analysis_spec
from services.semantic_parser import parse_question
from utils.analysis_spec_utils import (
    AnalysisSpec,
    coverage_notice,
    renderable_summary,
)
from utils.observability_utils import log_query_event
from utils.semantic_query_contracts import contract_template_order, normalize_contract_result
from utils.visualization_plan_utils import extract_intent_signals, template_order
import streamlit.components.v1 as components
import altair as alt
import pandas as pd
import random
from uuid import uuid4
from utils.question_library_utils import log_question_library_entry, read_question_library

EURO_DISPLAY_SCALE = 1_000_000.0
EURO_DISPLAY_UNIT = "milj. €"
DISPLAY_FONT_BODY = "Open Sans"
DISPLAY_FONT_HEADING = "Raleway"
CHART_BG = "#fff3b6"
CHART_BG_SOFT = "#fde992"
CHART_INK = "#111111"
CHART_GRID = "#11111122"

TRUST_BADGE_META = {
    "trusted": {
        "label": "Luotettava",
        "css_class": "bh-trust-trusted",
        "message": "Tulkinta ja käytetty budjettirajaus näyttävät olevan hyvällä tasolla tämän kysymyksen kannalta.",
    },
    "trusted_with_warning": {
        "label": "Luotettava rajauksin",
        "css_class": "bh-trust-warning",
        "message": "Vastaus on käyttökelpoinen, mutta siihen liittyy rajaus- tai tulkintavaroituksia, jotka kannattaa lukea mukaan.",
    },
    "needs_clarification": {
        "label": "Tarkennus tarvitaan",
        "css_class": "bh-trust-clarify",
        "message": "Järjestelmä ei halua arvata liian rohkeasti. Kysymystä pitää tarkentaa ennen luotettavaa vastausta.",
    },
    "unsupported": {
        "label": "Ei riittävän luotettava",
        "css_class": "bh-trust-unsupported",
        "message": "Tätä kysymystä ei voida vastata nykyisestä datasta riittävän tarkasti ilman, että riski harhaanjohtavasta tuloksesta kasvaa liikaa.",
    },
}


def apply_custom_theme() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;600;700&family=Raleway:wght@300;400;700;800&display=swap');

        :root {
          --bh-yellow: #f6d84f;
          --bh-yellow-soft: #fde992;
          --bh-yellow-card: #fff3b6;
          --bh-border: #111111;
          --bh-text: #111111;
        }

        .stApp,
        [data-testid="stAppViewContainer"],
        [data-testid="stHeader"] {
          background: var(--bh-yellow);
          color: var(--bh-text);
          font-family: 'Open Sans', sans-serif !important;
        }

        [data-testid="stSidebar"],
        [data-testid="stSidebarNav"],
        [data-testid="collapsedControl"] {
          display: none !important;
        }

        [data-testid="stToolbar"] {
          right: 0.75rem;
        }

        .block-container {
          padding-top: 2rem;
          padding-bottom: 3rem;
          max-width: 1160px;
        }

        .bh-topnav {
          display: flex;
          justify-content: flex-end;
          align-items: center;
          margin-bottom: 0.75rem;
        }

        .bh-topnav-link {
          display: inline-flex;
          align-items: center;
          gap: 0.4rem;
          padding: 0.45rem 0.9rem;
          border: 2px solid #111111;
          border-radius: 999px;
          background: #fff3b6;
          color: #111111 !important;
          text-decoration: none !important;
          font-family: 'Raleway', sans-serif !important;
          font-weight: 700;
          font-size: 0.95rem;
          line-height: 1;
        }

        .bh-topnav-link:hover {
          background: #fde992;
          transform: translateY(-1px);
        }

        .bh-trust-shell {
          display: flex;
          align-items: flex-start;
          justify-content: space-between;
          gap: 0.8rem;
          flex-wrap: wrap;
          margin: 0.5rem 0 1rem 0;
          padding: 0.8rem 1rem;
          border: 2px solid #111111;
          border-radius: 18px;
          background: #fff3b6;
        }

        .bh-trust-main {
          display: flex;
          align-items: center;
          gap: 0.7rem;
          flex-wrap: wrap;
        }

        .bh-trust-badge {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          padding: 0.35rem 0.75rem;
          border: 2px solid #111111;
          border-radius: 999px;
          font-family: 'Raleway', sans-serif !important;
          font-size: 0.88rem;
          font-weight: 800;
          letter-spacing: 0.02em;
          line-height: 1;
          text-transform: uppercase;
          white-space: nowrap;
        }

        .bh-trust-trusted {
          background: #c5f0b5;
        }

        .bh-trust-warning {
          background: #ffe08f;
        }

        .bh-trust-clarify {
          background: #ffd4a3;
        }

        .bh-trust-unsupported {
          background: #f6c2c2;
        }

        .bh-trust-note {
          font-size: 0.98rem;
          line-height: 1.35;
        }

        .bh-trust-observability {
          display: inline-flex;
          align-items: center;
          padding: 0.32rem 0.7rem;
          border: 1.5px solid #111111;
          border-radius: 999px;
          font-size: 0.82rem;
          font-weight: 700;
          background: #fff9d6;
          line-height: 1;
          white-space: nowrap;
        }

        h1, h2, h3, h4, h5, h6,
        p, label, div, span, li,
        .stMarkdown, .stCaption, .stMetric, .stAlert {
          color: var(--bh-text) !important;
        }

        h1 {
          font-family: 'Raleway', sans-serif !important;
          font-weight: 700 !important;
          text-transform: uppercase;
          letter-spacing: -0.04em;
        }

        .bh-hero-title {
          text-align: center;
          font-size: clamp(3.4rem, 7vw, 5.6rem);
          font-weight: 800;
          text-transform: uppercase;
          letter-spacing: -0.05em;
          line-height: 0.92;
          margin: 0.35rem 0 1.1rem 0;
          color: #111111;
          font-family: 'Raleway', sans-serif !important;
        }

        h2, h3, h4 {
          font-family: 'Raleway', sans-serif !important;
          font-weight: 700 !important;
          letter-spacing: -0.03em;
        }

        p, label, li, .stMarkdown, .stCaption, .stMetric, .stAlert, .stDataFrame {
          font-family: 'Open Sans', sans-serif !important;
          font-weight: 400 !important;
        }

        [data-testid="stTextArea"] label p {
          font-family: 'Raleway', sans-serif !important;
          text-transform: uppercase;
          font-weight: 800 !important;
          letter-spacing: 0.04em;
        }

        [data-testid="stTextArea"] textarea,
        [data-testid="stSelectbox"] div[data-baseweb="select"],
        [data-testid="stRadio"] label,
        [data-testid="stDataFrame"],
        [data-testid="stMetric"],
        [data-testid="stVerticalBlock"] > div:has(> [data-testid="stMetric"]) {
          font-family: 'Open Sans', sans-serif !important;
          color: var(--bh-text) !important;
        }

        [data-testid="stTextArea"] textarea {
          background: #fff9d6 !important;
          border: 2px solid var(--bh-border) !important;
          border-radius: 14px !important;
        }

        div[data-testid="stButton"] > button {
          font-family: 'Raleway', sans-serif !important;
          background: #f6d84f !important;
          color: #111111 !important;
          border: 2px solid #111111 !important;
          border-radius: 999px !important;
          font-weight: 700 !important;
        }

        div[data-testid="stButton"] > button:hover {
          background: #fde992 !important;
          color: #111111 !important;
          border-color: #111111 !important;
        }

        div[data-testid="stDownloadButton"] > button {
          font-family: 'Raleway', sans-serif !important;
          background: #111111 !important;
          color: #f8e36c !important;
          border: 2px solid #111111 !important;
          border-radius: 999px !important;
          font-weight: 700 !important;
        }

        div[data-testid="stDownloadButton"] > button:hover {
          background: #2a2a2a !important;
          color: #fff2a8 !important;
        }

        [data-testid="stMetric"],
        [data-testid="stAlert"],
        [data-testid="stDataFrame"] {
          background: var(--bh-yellow-card);
          border: 2px solid var(--bh-border);
          border-radius: 18px;
          padding: 0.5rem;
        }

        [data-testid="stVegaLiteChart"] {
          background: var(--bh-yellow-card);
          border: 2px solid var(--bh-border);
          border-radius: 18px;
          padding: 0.9rem 0.9rem 0.4rem 0.9rem;
          overflow: hidden;
        }

        hr {
          border-color: #111111 !important;
        }

        .bh-footer-logo {
          display: flex;
          align-items: center;
          justify-content: center;
          gap: 1.5rem;
          width: 100%;
          margin: 2.5rem auto 0.5rem auto;
          flex-wrap: wrap;
        }

        .bh-footer-link {
          display: inline-block;
          text-decoration: none !important;
          color: inherit !important;
        }

        .bh-footer-shell {
          display: flex;
          flex-direction: column;
          align-items: center;
          gap: 0.5rem;
        }

        .bh-footer-campaign {
          text-align: center;
          font-size: clamp(1.3rem, 3vw, 2.6rem);
          font-weight: 800;
          letter-spacing: -0.03em;
          color: #111111;
          margin-top: 1.5rem;
          font-family: 'Raleway', sans-serif !important;
        }

        .bh-footer-logo-text {
          font-size: clamp(2.2rem, 6vw, 7rem);
          line-height: 0.95;
          font-weight: 300;
          letter-spacing: -0.04em;
          color: #1b1b1b;
          text-transform: none;
          font-family: 'Raleway', sans-serif !important;
        }

        .bh-social-links {
          display: flex;
          align-items: center;
          justify-content: center;
          flex-wrap: wrap;
          gap: 0.75rem;
          margin: 0.1rem auto 0.7rem auto;
        }

        .bh-social-link {
          display: inline-flex;
          align-items: center;
          gap: 0.55rem;
          padding: 0.6rem 0.95rem;
          border: 2px solid #111111;
          border-radius: 999px;
          background: #fff3b6;
          color: #111111 !important;
          text-decoration: none !important;
          font-weight: 700;
          font-size: 0.98rem;
          line-height: 1;
          transition: transform 120ms ease, background 120ms ease;
          font-family: 'Raleway', sans-serif !important;
        }

        .bh-social-link:hover {
          background: #fde992;
          transform: translateY(-1px);
        }

        .bh-social-icon {
          width: 1rem;
          height: 1rem;
          display: inline-block;
          flex: 0 0 auto;
        }

        @media (max-width: 900px) {
          .bh-footer-logo {
            gap: 1rem;
          }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


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
        <div style="border:2px solid #111;padding:12px;border-radius:14px;text-align:center;color:#111;background:#fff3b6;font-size:0.9rem;font-weight:600;">
          {settings.ad_placeholder_text} ({fallback_label})
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_footer_logo() -> None:
    st.markdown(
        """
        <div class="bh-footer-shell">
          <div class="bh-footer-campaign">#RohkeuttaPriorisoida</div>
          <a class="bh-footer-link" href="https://liberaalipuolue.fi/rohkeuttapriorisoida/" target="_blank" rel="noopener noreferrer" aria-label="Avaa Rohkeutta priorisoida -sivu">
            <div class="bh-footer-logo" aria-label="liberaalipuolue.fi logo">
              <div class="bh-footer-logo-text">liberaalipuolue.fi</div>
            </div>
          </a>
          <div class="bh-social-links" aria-label="Liberaalipuolueen sosiaalinen media">
            <a class="bh-social-link" href="https://www.facebook.com/liberaalipuolue/" target="_blank" rel="noopener noreferrer" aria-label="Liberaalipuolue Facebookissa">
              <svg class="bh-social-icon" viewBox="0 0 24 24" aria-hidden="true">
                <path fill="currentColor" d="M13.5 21v-8.2h2.8l.4-3.2h-3.2V7.5c0-.9.3-1.6 1.6-1.6h1.7V3.1c-.3 0-1.3-.1-2.5-.1-2.5 0-4.1 1.5-4.1 4.3v2.4H8v3.2h2.7V21h2.8z"/>
              </svg>
              Facebook
            </a>
            <a class="bh-social-link" href="https://instagram.com/liberaalipuolue/" target="_blank" rel="noopener noreferrer" aria-label="Liberaalipuolue Instagramissa">
              <svg class="bh-social-icon" viewBox="0 0 24 24" aria-hidden="true">
                <path fill="currentColor" d="M7 3h10a4 4 0 0 1 4 4v10a4 4 0 0 1-4 4H7a4 4 0 0 1-4-4V7a4 4 0 0 1 4-4zm0 2.2A1.8 1.8 0 0 0 5.2 7v10c0 1 .8 1.8 1.8 1.8h10c1 0 1.8-.8 1.8-1.8V7c0-1-.8-1.8-1.8-1.8H7zm5 2.3A4.5 4.5 0 1 1 7.5 12 4.5 4.5 0 0 1 12 7.5zm0 2.2A2.3 2.3 0 1 0 14.3 12 2.3 2.3 0 0 0 12 9.7zm4.8-3.3a1.1 1.1 0 1 1-1.1 1.1 1.1 1.1 0 0 1 1.1-1.1z"/>
              </svg>
              Instagram
            </a>
            <a class="bh-social-link" href="https://twitter.com/liberaalipuolue/" target="_blank" rel="noopener noreferrer" aria-label="Liberaalipuolue X:ssä">
              <svg class="bh-social-icon" viewBox="0 0 24 24" aria-hidden="true">
                <path fill="currentColor" d="M18.9 3H22l-6.8 7.8L23 21h-6.2l-4.9-6.4L6.4 21H3.3l7.3-8.3L2 3h6.3l4.4 5.8L18.9 3zm-1.1 16h1.7L7.3 4.9H5.5L17.8 19z"/>
              </svg>
              X
            </a>
            <a class="bh-social-link" href="https://liberaalipuolue.fi/chat/" target="_blank" rel="noopener noreferrer" aria-label="Liberaalipuolue Discordissa">
              <svg class="bh-social-icon" viewBox="0 0 24 24" aria-hidden="true">
                <path fill="currentColor" d="M20.3 4.4A16.7 16.7 0 0 0 16.2 3l-.2.4c1.5.4 2.2.9 3 1.4-2.6-1.2-5.5-1.8-8.4-1.8-2.9 0-5.8.6-8.4 1.8.8-.5 1.5-1 3-1.4L5 3A16.7 16.7 0 0 0 .9 4.4C-1 7.2-1.5 9.8-1.3 12.4A16.9 16.9 0 0 0 4 15l.7-1.1c-1-.3-1.9-.7-2.8-1.3.2.1.5.3.7.4 2.3 1.1 4.8 1.6 7.4 1.6 2.6 0 5.1-.5 7.4-1.6.2-.1.5-.2.7-.4-.9.6-1.8 1-2.8 1.3l.7 1.1a16.9 16.9 0 0 0 5.3-2.6c.3-3-.5-5.6-1.7-8zM8 11.8c-.8 0-1.5-.8-1.5-1.7 0-.9.6-1.7 1.5-1.7.8 0 1.5.8 1.5 1.7 0 .9-.7 1.7-1.5 1.7zm8 0c-.8 0-1.5-.8-1.5-1.7 0-.9.6-1.7 1.5-1.7.8 0 1.5.8 1.5 1.7 0 .9-.7 1.7-1.5 1.7z"/>
              </svg>
              Discord
            </a>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_top_nav(admin_mode: bool = False) -> None:
    target_href = "/" if admin_mode else "/?admin=1"
    link_label = "Etusivu" if admin_mode else "Admin"
    st.markdown(
        f"""
        <div class="bh-topnav">
          <a class="bh-topnav-link" href="{target_href}" target="_self" rel="noopener noreferrer">{link_label}</a>
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
                f"Suurin nettokertymä tuloksessa: {top_row[hallinnonala_col]} "
                f"({_format_euro_millions(top_row[net_col])} {EURO_DISPLAY_UNIT})"
            )


def render_budget_moment_evidence(
    question: str,
    results_df: pd.DataFrame,
    analysis_spec: AnalysisSpec | None = None,
    limit: int = 30,
    evidence_rows: list[dict] | None = None,
) -> None:
    st.subheader("Käytetyt budjettimomentit")
    if evidence_rows:
        evidence_df = pd.DataFrame(evidence_rows)
        evidence_source = "analytics_api"
    else:
        evidence = bigquery_utils.get_budget_moment_evidence(
            question=question,
            results_df=results_df,
            analysis_spec=analysis_spec if isinstance(analysis_spec, AnalysisSpec) else None,
            limit=limit,
        )
        evidence_df = evidence.get("evidence_df")
        evidence_source = evidence.get("source")
    if not isinstance(evidence_df, pd.DataFrame) or evidence_df.empty:
        st.warning("Budjettimomentteja ei voitu tunnistaa tästä vastauksesta.")
        return

    if evidence_source == "results_df":
        st.caption("Momentit tunnistettiin suoraan tulosaineistosta, jota visualisoinnit käyttävät.")
    elif evidence_source == "analytics_api":
        st.caption("Momentit palautettiin valmiiksi analytiikka-API:n canonical-tulkinnasta.")
    elif evidence_source == "concept_bridge":
        st.caption("Momentit haettiin semanttisesta concept bridgestä, joka käyttää ontologiaa ja budjettipuolen rajauksia.")
    else:
        st.caption(
            f"Visualisoinnin momentit haettiin erillisellä tukikyselyllä samasta rajauksesta. "
            f"Näytetään enintään {min(limit, len(evidence_df))} suurinta momenttia."
        )

    display_columns = [
        column
        for column in (
            "momentti_tunnusp",
            "momentti_snimi",
            "alamomentti_tunnus",
            "alamomentti_snimi",
            "nettokertyma_sum",
            "vuosia",
        )
        if column in evidence_df.columns
    ]
    if not display_columns:
        st.warning("Budjettimomentteihin liittyviä sarakkeita ei löytynyt näytettäväksi.")
        return

    st.dataframe(_format_results_for_display(evidence_df[display_columns]), width="stretch")


def _format_year_range(year_from: int | None, year_to: int | None) -> str:
    if year_from is None or year_to is None:
        return "ei määritelty"
    return str(year_from) if year_from == year_to else f"{year_from}-{year_to}"


def _results_df_from_api_response(result: dict) -> pd.DataFrame:
    rows = result.get("result_rows") or []
    columns = result.get("result_columns") or []
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns or None)


def _observability_label(value: str | None) -> str | None:
    labels = {
        "exact": "Tulkinta: exact",
        "composite": "Tulkinta: composite",
        "proxy": "Tulkinta: proxy",
        "unsupported": "Tulkinta: unsupported",
    }
    return labels.get((value or "").strip().lower())


def render_trust_badge(result: dict) -> None:
    verification_status = (result.get("verification_status") or "").strip().lower()
    if not verification_status:
        status = (result.get("status") or "").strip().lower()
        if status == "unsupported":
            verification_status = "unsupported"
        elif status == "clarification_required":
            verification_status = "needs_clarification"
        else:
            return

    meta = TRUST_BADGE_META.get(verification_status)
    if not meta:
        return

    resolved = result.get("resolved_analysis") or {}
    note = meta["message"]
    observability_reason = str(resolved.get("observability_reason") or "").strip()
    if observability_reason:
        note = f"{note} {observability_reason}"

    observability_text = _observability_label(resolved.get("observability_class"))
    observability_html = (
        f'<span class="bh-trust-observability">{observability_text}</span>' if observability_text else ""
    )
    st.markdown(
        f"""
        <div class="bh-trust-shell">
          <div class="bh-trust-main">
            <span class="bh-trust-badge {meta['css_class']}">{meta['label']}</span>
            {observability_html}
          </div>
          <div class="bh-trust-note">{note}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _is_truthy_query_param(name: str) -> bool:
    try:
        raw = st.query_params.get(name)
    except Exception:
        return False
    if isinstance(raw, list):
        raw = raw[0] if raw else ""
    return str(raw or "").strip().lower() in {"1", "true", "yes", "on"}


def _load_question_library_df(limit: int = 5000) -> pd.DataFrame:
    rows = read_question_library(limit=limit)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)
    return df.sort_values("ts", ascending=False, na_position="last")


def render_admin_view() -> None:
    st.markdown('<div class="bh-hero-title" style="font-size: clamp(2.2rem, 5vw, 3.4rem); margin-bottom: 0.6rem;">ADMIN</div>', unsafe_allow_html=True)
    st.header("Admin-näkymä")
    st.caption(
        "Tämä näkymä näyttää kysymyskirjaston kertymää palvelun kehittämistä varten. "
        "Se avautuu vain osoitteella, jossa on query-parametri `?admin=1`."
    )

    library_df = _load_question_library_df()
    if library_df.empty:
        st.info("Kysymyskirjasto on vielä tyhjä.")
        return

    total_queries = len(library_df)
    unique_questions = int(library_df["question"].nunique()) if "question" in library_df.columns else 0
    unique_sessions = int(library_df["session_id"].nunique()) if "session_id" in library_df.columns else 0
    success_rate = (
        float((library_df["status"] == "success").mean() * 100.0)
        if "status" in library_df.columns
        else 0.0
    )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Kyselyitä yhteensä", f"{total_queries:,}".replace(",", " "))
    col2.metric("Uniikkeja kysymyksiä", f"{unique_questions:,}".replace(",", " "))
    col3.metric("Sessioita", f"{unique_sessions:,}".replace(",", " "))
    col4.metric("Onnistumisaste", f"{success_rate:.1f} %")

    tab_overview, tab_questions, tab_download = st.tabs(["Yhteenveto", "Kysymykset", "Lataus"])

    with tab_overview:
        left, right = st.columns(2)

        if "status" in library_df.columns:
            status_counts = (
                library_df["status"]
                .fillna("unknown")
                .value_counts()
                .rename_axis("status")
                .reset_index(name="kyselyitä")
            )
            left.subheader("Statukset")
            left.dataframe(status_counts, width="stretch", hide_index=True)

        if "intent" in library_df.columns:
            intent_counts = (
                library_df["intent"]
                .fillna("unknown")
                .value_counts()
                .head(15)
                .rename_axis("intent")
                .reset_index(name="kyselyitä")
            )
            right.subheader("Intentit")
            right.dataframe(intent_counts, width="stretch", hide_index=True)

        lower_left, lower_right = st.columns(2)

        if "resolved_concept_label" in library_df.columns:
            concept_counts = (
                library_df["resolved_concept_label"]
                .fillna("ei ratkaistu")
                .replace("", "ei ratkaistu")
                .value_counts()
                .head(20)
                .rename_axis("käsite")
                .reset_index(name="kyselyitä")
            )
            lower_left.subheader("Ratkaistut käsitteet")
            lower_left.dataframe(concept_counts, width="stretch", hide_index=True)

        repeated_questions = (
            library_df["question"]
            .fillna("")
            .value_counts()
            .head(20)
            .rename_axis("kysymys")
            .reset_index(name="toistot")
        )
        lower_right.subheader("Toistuvimmat kysymykset")
        lower_right.dataframe(repeated_questions, width="stretch", hide_index=True)

    with tab_questions:
        status_options = ["Kaikki"]
        if "status" in library_df.columns:
            status_options += sorted(v for v in library_df["status"].dropna().unique().tolist() if v)
        selected_status = st.selectbox("Suodata statuksen mukaan", status_options, index=0, key="admin_status_filter")
        search_text = st.text_input("Hae kysymystekstistä", key="admin_search_text")
        show_limit = st.slider("Näytä rivejä", min_value=20, max_value=500, value=100, step=20, key="admin_row_limit")

        filtered = library_df.copy()
        if selected_status != "Kaikki" and "status" in filtered.columns:
            filtered = filtered[filtered["status"] == selected_status]
        if search_text.strip():
            filtered = filtered[
                filtered["question"].astype(str).str.contains(search_text.strip(), case=False, na=False)
            ]

        display_df = filtered.copy()
        if "ts" in display_df.columns:
            display_df["ts"] = (
                display_df["ts"]
                .dt.tz_convert("Europe/Helsinki")
                .dt.strftime("%Y-%m-%d %H:%M:%S")
            )

        wanted_columns = [
            "ts",
            "question",
            "status",
            "intent",
            "fiscal_side",
            "resolved_concept_label",
            "query_source",
            "result_row_count",
            "used_moment_count",
            "error_class",
        ]
        visible_columns = [column for column in wanted_columns if column in display_df.columns]
        st.dataframe(display_df[visible_columns].head(show_limit), width="stretch", hide_index=True)
        st.caption(f"Suodatuksen jälkeen rivejä: {len(filtered)}")

    with tab_download:
        export_df = library_df.copy()
        if "ts" in export_df.columns:
            export_df["ts"] = export_df["ts"].astype(str)
        st.download_button(
            "Lataa kysymyskirjasto CSV",
            data=export_df.to_csv(index=False).encode("utf-8"),
            file_name="budjettihaukka_question_library.csv",
            mime="text/csv",
        )
        st.download_button(
            "Lataa kysymyskirjasto JSON",
            data=export_df.to_json(orient="records", force_ascii=False, indent=2),
            file_name="budjettihaukka_question_library.json",
            mime="application/json",
        )


def _log_question_library_event(
    *,
    question: str,
    session_id: str,
    status: str,
    analysis_spec: AnalysisSpec | None = None,
    query_source: str | None = None,
    query_contract: str | None = None,
    clarification_required: bool = False,
    clarification_choices: dict[str, str] | None = None,
    missing_required: list[str] | None = None,
    result: dict | None = None,
    results_df: pd.DataFrame | None = None,
    error_class: str | None = None,
    error_message: str | None = None,
) -> None:
    spec = analysis_spec if isinstance(analysis_spec, AnalysisSpec) else None
    payload = {
        "session_id": session_id,
        "surface": "streamlit",
        "language": "fi",
        "status": status,
        "question": question.strip(),
        "clarification_required": clarification_required,
        "clarification_choices": clarification_choices or {},
        "clarification_missing_fields": missing_required or [],
        "intent": spec.intent if spec else None,
        "metric": spec.metric if spec else None,
        "fiscal_side": spec.fiscal_side if spec else None,
        "entity_level": spec.entity_level if spec else None,
        "growth_type": spec.growth_type if spec else None,
        "time_from": spec.time_from if spec else None,
        "time_to": spec.time_to if spec else None,
        "requested_time_from": spec.requested_time_from if spec else None,
        "requested_time_to": spec.requested_time_to if spec else None,
        "confidence": spec.confidence if spec else None,
        "resolved_concept_id": spec.resolved_concept_id if spec else None,
        "resolved_concept_label": spec.resolved_concept_label if spec else None,
        "ontology_match_score": spec.ontology_match_score if spec else None,
        "ontology_risk_level": spec.ontology_risk_level if spec else None,
        "ontology_must_clarify": spec.ontology_must_clarify if spec else None,
        "query_source": query_source,
        "query_contract": query_contract,
        "query_id": (result or {}).get("query_id"),
        "used_moment_count": len((result or {}).get("used_moments") or []),
        "result_row_count": len(results_df) if isinstance(results_df, pd.DataFrame) else 0,
        "error_class": error_class,
        "error_message": error_message,
    }
    log_question_library_entry(payload)


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


def render_interpretation_block(question: str) -> tuple[AnalysisSpec, dict[str, str], bool, list[str]]:
    spec = parse_question(question).analysis_spec
    required_clarification = bool(spec.clarifications) and spec.confidence < settings.clarification_required_confidence
    selections: dict[str, str] = {}
    missing_required: list[str] = []
    if spec.clarifications:
        st.markdown("**Tarkenna**")
        for field in spec.clarifications:
            widget_key = f"clarify_{field.field}"
            if required_clarification:
                options = [""] + list(field.options)
                selected = st.selectbox(
                    f"{field.question} (pakollinen)",
                    options,
                    index=0,
                    key=widget_key,
                    format_func=lambda value: "Valitse..." if value == "" else value,
                )
                if selected:
                    selections[field.field] = selected
                else:
                    missing_required.append(field.field)
            else:
                try:
                    default_idx = field.options.index(field.recommended)
                except ValueError:
                    default_idx = 0
                selected = st.radio(
                    f"{field.question} (valinnainen)",
                    field.options,
                    index=default_idx,
                    horizontal=True,
                    key=widget_key,
                )
                selections[field.field] = selected

    if required_clarification and missing_required:
        st.warning("Kysely vaatii tarkennuksen ennen ajoa, koska tulkinnan luottamus on matala.")
    return spec, selections, required_clarification, missing_required

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
    return EURO_DISPLAY_SCALE, EURO_DISPLAY_UNIT


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


def _format_display_number(value, decimals: int = 2) -> str:
    if pd.isna(value):
        return ""
    number = float(value)
    rounded = round(number)
    use_decimals = 0 if abs(number - rounded) < 0.005 else decimals
    return f"{number:,.{use_decimals}f}".replace(",", " ")


def _format_euro_millions(value, decimals: int = 2, suffix: str = "") -> str:
    if pd.isna(value):
        return ""
    value_in_millions = float(value) / EURO_DISPLAY_SCALE
    return f"{_format_display_number(value_in_millions, decimals)}{suffix}"


def _vega_space_grouping_expr() -> str:
    return "replace(format(datum.value, ',.2~f'), ',', ' ')"


def _altair_axis(label_expr: str | None = None) -> alt.Axis:
    axis_kwargs = dict(
        labelColor=CHART_INK,
        titleColor=CHART_INK,
        domainColor=CHART_INK,
        tickColor=CHART_INK,
        gridColor=CHART_GRID,
        gridOpacity=1,
        labelFont=DISPLAY_FONT_BODY,
        titleFont=DISPLAY_FONT_HEADING,
        labelFontSize=13,
        titleFontSize=16,
        titleFontWeight="bold",
    )
    if label_expr is not None:
        axis_kwargs["labelExpr"] = label_expr
    return alt.Axis(**axis_kwargs)


def _style_altair_chart(chart: alt.Chart, height: int | None = None) -> alt.Chart:
    if height is not None:
        chart = chart.properties(height=height)
    return (
        chart.properties(background=CHART_BG)
        .configure_view(stroke=CHART_INK, strokeWidth=2, fill=CHART_BG)
        .configure_axis(
            labelColor=CHART_INK,
            titleColor=CHART_INK,
            domainColor=CHART_INK,
            tickColor=CHART_INK,
            gridColor=CHART_GRID,
            labelFont=DISPLAY_FONT_BODY,
            titleFont=DISPLAY_FONT_HEADING,
            labelFontSize=13,
            titleFontSize=16,
            titleFontWeight="bold",
        )
        .configure_legend(
            orient="bottom",
            labelColor=CHART_INK,
            titleColor=CHART_INK,
            labelFont=DISPLAY_FONT_BODY,
            titleFont=DISPLAY_FONT_HEADING,
            symbolStrokeColor=CHART_INK,
            symbolFillColor=CHART_INK,
        )
        .configure_title(
            color=CHART_INK,
            font=DISPLAY_FONT_HEADING,
            fontSize=18,
            fontWeight="bold",
            anchor="start",
        )
        .configure_header(
            labelColor=CHART_INK,
            titleColor=CHART_INK,
            labelFont=DISPLAY_FONT_BODY,
            titleFont=DISPLAY_FONT_HEADING,
        )
    )


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


def _looks_like_euro_column(column_name: str) -> bool:
    name = (column_name or "").lower()
    return any(
        token in name
        for token in (
            "nettokertyma",
            "nettokertymä",
            "sum",
            "maararaha",
            "määräraha",
            "muutos_eur",
            "kasvu_eur",
            "alkuvuosi",
            "loppuvuosi",
        )
    )


def _looks_like_pct_column(column_name: str) -> bool:
    name = (column_name or "").lower()
    return "pct" in name or "%" in name


def _format_results_for_display(df: pd.DataFrame) -> pd.DataFrame:
    display_df = df.copy()
    for column in display_df.columns:
        if _looks_like_pct_column(column):
            display_df[column] = _to_numeric_series(display_df[column]).map(lambda x: _format_number(x, 2, " %"))
            continue
        if _looks_like_euro_column(column):
            display_df[column] = _to_numeric_series(display_df[column]).map(
                lambda x: f"{_format_euro_millions(x)} {EURO_DISPLAY_UNIT}"
            )
    return display_df


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
            "entity",
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
    ascending = spec.intent in {"top_cuts", "revenue_decline"}
    ranked = (
        rank_df.groupby(label_col, as_index=False)[metric_col]
        .agg(agg_func)
        .sort_values(metric_col, ascending=ascending)
        .head(spec.ranking_n or 10)
    )
    if ranked.empty:
        return False

    ranked = ranked.rename(columns={label_col: "label", metric_col: "metric"})
    ranked["label"] = ranked["label"].astype(str)
    ranked["label_short"] = ranked["label"].apply(_truncate_label)

    unit = "%"
    ranked["plot_metric"] = ranked["metric"]
    ranked["metric_label"] = ranked["metric"].map(lambda x: _format_number(x, 2))
    if metric_col != growth_pct_col:
        scale, unit = _choose_euro_scale(ranked["metric"])
        ranked["plot_metric"] = ranked["metric"] / scale
        ranked["metric_label"] = ranked["metric"].map(_format_euro_millions)

    title = "Eniten kasvaneet kategoriat"
    x_title = f"Kasvu ({unit})"
    if spec.intent == "top_cuts":
        title = "Eniten leikatut kategoriat"
        x_title = f"Muutos ({unit})"
    elif spec.intent == "revenue_decline":
        title = "Eniten pienentyneet tulokategoriat"
        x_title = f"Muutos ({unit})"
    st.markdown(f"**{_title_with_scope(title, spec)}**")
    st.caption(f"X-akseli: {x_title.lower()} | Y-akseli: kategoria")

    top_chart = (
        alt.Chart(ranked)
        .mark_bar(color=CHART_INK, stroke=CHART_INK, strokeWidth=1.2, cornerRadiusEnd=2)
        .encode(
            y=alt.Y("label_short:N", sort="-x", title=""),
            x=alt.X(
                "plot_metric:Q",
                title=x_title,
                axis=_altair_axis(_vega_space_grouping_expr()),
            ),
            tooltip=[
                alt.Tooltip("label:N", title="Kategoria"),
                alt.Tooltip("metric_label:N", title=x_title),
            ],
        )
    )
    st.altair_chart(
        _style_altair_chart(top_chart, height=max(280, min(700, 28 * len(ranked)))),
        use_container_width=True,
    )

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
            "muutos_eur",
            "muutos_abs_eur",
            "kasvu_pct",
            "muutos_pct",
        )
        if col in work.columns
    ]
    if detail_cols:
        details = work[detail_cols].copy()
        for euro_col in ("alkuvuosi_sum", "loppuvuosi_sum", "kasvu_eur", "muutos_eur", "muutos_abs_eur"):
            if euro_col in details.columns:
                details[euro_col] = _to_numeric_series(details[euro_col]).map(
                    lambda x: f"{_format_euro_millions(x)} {EURO_DISPLAY_UNIT}"
                )
        for pct_col in ("kasvu_pct", "muutos_pct"):
            if pct_col in details.columns:
                details[pct_col] = _to_numeric_series(details[pct_col]).map(lambda x: _format_number(x, 2, " %"))
        detail_title = "Kasvutaulukko"
        if spec.intent == "top_cuts":
            detail_title = "Leikkaustaulukko"
        elif spec.intent == "revenue_decline":
            detail_title = "Tulokertymän heikkenemistaulukko"
        st.markdown(f"**{detail_title}**")
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
        chart_df["summa_label"] = chart_df["summa"].map(_format_euro_millions)
        st.markdown(f"**{_title_with_scope('Trendikuva', spec)}**")
        st.caption(f"X-akseli: vuosi | Y-akseli: summa ({unit})")
        trend_chart = (
            alt.Chart(chart_df)
            .mark_line(
                point=alt.OverlayMarkDef(color=CHART_INK, filled=True, fill=CHART_INK, size=68),
                color=CHART_INK,
                strokeWidth=3,
            )
            .encode(
                x=alt.X("vuosi:N", title="Vuosi", axis=_altair_axis()),
                y=alt.Y(
                    "summa_scaled:Q",
                    title=f"Summa ({unit})",
                    axis=_altair_axis(_vega_space_grouping_expr()),
                ),
                tooltip=[
                    alt.Tooltip("vuosi:N", title="Vuosi"),
                    alt.Tooltip("summa_label:N", title=f"Summa ({unit})"),
                ],
            )
        )
        st.altair_chart(_style_altair_chart(trend_chart), use_container_width=True)
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
    chart_df["summa_label"] = chart_df["summa"].map(_format_euro_millions)
    st.markdown(f"**{_title_with_scope('Trendikuva', spec)}**")
    st.caption(f"X-akseli: aika | Y-akseli: summa ({unit})")
    trend_chart = (
        alt.Chart(chart_df)
        .mark_line(
            point=alt.OverlayMarkDef(color=CHART_INK, filled=True, fill=CHART_INK, size=60),
            color=CHART_INK,
            strokeWidth=3,
        )
        .encode(
            x=alt.X("_time_axis:T", title="Aika", axis=_altair_axis()),
            y=alt.Y(
                "summa_scaled:Q",
                title=f"Summa ({unit})",
                axis=_altair_axis(_vega_space_grouping_expr()),
            ),
            tooltip=[
                alt.Tooltip("_time_axis:T", title="Aika"),
                alt.Tooltip("summa_label:N", title=f"Summa ({unit})"),
            ],
        )
    )
    st.altair_chart(_style_altair_chart(trend_chart), use_container_width=True)
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
            labels = growth_series.map(lambda x: _format_number(x, 2, " %"))
        else:
            scale, unit = _choose_euro_scale(growth_series)
            scaled = growth_series / scale
            labels = growth_series.map(_format_euro_millions)
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
        labels = growth_series.map(lambda x: _format_number(x, 2, " %"))

    chart_df = scaled.reset_index(name="value")
    chart_df["vuosi"] = chart_df[year_col].astype(int).astype(str)
    chart_df["value_label"] = list(labels.values)
    value_axis = _altair_axis(_vega_space_grouping_expr()) if unit != "%" else _altair_axis()

    st.markdown(f"**{_title_with_scope('Kasvu vuositasolla', spec)}**")
    st.caption(f"X-akseli: vuosi | Y-akseli: kasvu ({unit})")
    growth_chart = (
        alt.Chart(chart_df)
        .mark_bar(color=CHART_INK, stroke=CHART_INK, strokeWidth=1.2, cornerRadiusTopLeft=2, cornerRadiusTopRight=2)
        .encode(
            x=alt.X("vuosi:N", title="Vuosi", axis=_altair_axis()),
            y=alt.Y("value:Q", title=f"Kasvu ({unit})", axis=value_axis),
            tooltip=[
                alt.Tooltip("vuosi:N", title="Vuosi"),
                alt.Tooltip("value_label:N", title=f"Kasvu ({unit})"),
            ],
        )
    )
    st.altair_chart(_style_altair_chart(growth_chart), use_container_width=True)
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

    grouped = (
        comp_df.groupby([year_col, category_col], as_index=False)[value_col]
        .sum()
        .sort_values([year_col, value_col], ascending=[True, False])
    )
    if grouped.empty:
        return False
    scale, unit = _choose_euro_scale(grouped[value_col])
    grouped["value_scaled"] = grouped[value_col] / scale
    grouped["vuosi"] = _to_numeric_series(grouped[year_col]).astype("Int64").astype(str)
    grouped["category"] = grouped[category_col].astype(str)
    grouped["value_label"] = grouped[value_col].map(_format_euro_millions)

    st.markdown(f"**{_title_with_scope('Rakenteen kehitys', spec)}**")
    st.caption(f"X-akseli: vuosi | Y-akseli: summa ({unit}) | Viivakuvio: kategoria")
    composition_chart = (
        alt.Chart(grouped)
        .mark_line(point=alt.OverlayMarkDef(color=CHART_INK, filled=True, fill=CHART_BG_SOFT, stroke=CHART_INK, size=52), color=CHART_INK, strokeWidth=2.4)
        .encode(
            x=alt.X("vuosi:N", title="Vuosi", axis=_altair_axis()),
            y=alt.Y("value_scaled:Q", title=f"Summa ({unit})", axis=_altair_axis(_vega_space_grouping_expr())),
            detail=alt.Detail("category:N"),
            strokeDash=alt.StrokeDash("category:N", title="Kategoria"),
            tooltip=[
                alt.Tooltip("vuosi:N", title="Vuosi"),
                alt.Tooltip("category:N", title="Kategoria"),
                alt.Tooltip("value_label:N", title=f"Summa ({unit})"),
            ],
        )
    )
    st.altair_chart(_style_altair_chart(composition_chart), use_container_width=True)
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

    heat = (
        season_df.groupby([year_col, month_col], as_index=False)[value_col]
        .sum()
        .sort_values([year_col, month_col])
    )
    if heat.empty:
        return False
    scale, unit = _choose_euro_scale(heat[value_col])
    heat["value_scaled"] = heat[value_col] / scale
    heat["vuosi"] = _to_numeric_series(heat[year_col]).astype("Int64").astype(str)
    heat["kk"] = _to_numeric_series(heat[month_col]).astype("Int64")
    heat["value_label"] = heat[value_col].map(_format_euro_millions)
    st.markdown(f"**{_title_with_scope('Kausivaihtelu (vuosi x kuukausi)', spec)}**")
    st.caption(f"X-akseli: kuukausi | Y-akseli: vuosi | Intensiivisyys: summa ({unit})")
    seasonality_chart = (
        alt.Chart(heat)
        .mark_rect(stroke=CHART_INK, strokeWidth=1)
        .encode(
            x=alt.X("kk:O", title="Kuukausi", axis=_altair_axis()),
            y=alt.Y("vuosi:N", title="Vuosi", axis=_altair_axis()),
            color=alt.value(CHART_INK),
            opacity=alt.Opacity("value_scaled:Q", legend=None, scale=alt.Scale(range=[0.12, 1.0])),
            tooltip=[
                alt.Tooltip("vuosi:N", title="Vuosi"),
                alt.Tooltip("kk:O", title="Kuukausi"),
                alt.Tooltip("value_label:N", title=f"Summa ({unit})"),
            ],
        )
    )
    st.altair_chart(_style_altair_chart(seasonality_chart), use_container_width=True)
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
    grouped["value_label"] = grouped["value"].map(_format_euro_millions)

    st.markdown(f"**{_title_with_scope('Top kategoriat', spec)}**")
    st.caption(f"X-akseli: summa ({unit}) | Y-akseli: kategoria")
    chart = (
        alt.Chart(grouped)
        .mark_bar(color=CHART_INK, stroke=CHART_INK, strokeWidth=1.2, cornerRadiusEnd=2)
        .encode(
            y=alt.Y("label_short:N", sort="-x", title=""),
            x=alt.X(
                "value_scaled:Q",
                title=f"Summa ({unit})",
                axis=_altair_axis(_vega_space_grouping_expr()),
            ),
            tooltip=[
                alt.Tooltip("label:N", title="Kategoria"),
                alt.Tooltip("value_label:N", title=f"Summa ({unit})"),
            ],
        )
    )
    st.altair_chart(
        _style_altair_chart(chart, height=max(250, min(650, 28 * len(grouped)))),
        use_container_width=True,
    )
    return True


def visualize_data(
    df: pd.DataFrame,
    title: str = "Budjettidata visualisointi",
    question: str = "",
    analysis_spec: AnalysisSpec | None = None,
    query_contract: str | None = None,
) -> list[str]:
    st.subheader(title)
    if df is None or df.empty:
        st.warning("Ei dataa visualisoitavaksi.")
        return []

    spec = analysis_spec if isinstance(analysis_spec, AnalysisSpec) else parse_question(question).analysis_spec
    work = df.copy()

    rendered_templates: list[str] = []
    contract_templates = contract_template_order(query_contract)
    if query_contract and contract_templates:
        canonical = normalize_contract_result(work, query_contract, spec)
        if not canonical.empty:
            canonical["metric"] = _to_numeric_series(canonical["metric"])
            canonical["delta"] = _to_numeric_series(canonical["delta"])
            canonical["pct"] = _to_numeric_series(canonical["pct"])
            canonical["entity"] = canonical["entity"].fillna("Tuntematon").astype(str)
            if "time" in canonical.columns:
                canonical["time"] = _to_numeric_series(canonical["time"]).astype("Int64")
            for template in contract_templates:
                if template == "top_growth":
                    ok = _render_top_growth_template(canonical, spec, "metric", "pct", "delta")
                elif template == "trend":
                    ok = _render_trend_template(canonical, spec, "metric", "time")
                elif template == "growth":
                    ok = _render_growth_template(canonical, spec, "metric", "time", "pct", "delta")
                elif template == "top_categories":
                    ok = _render_top_categories_template(canonical, spec, "metric", "entity")
                else:
                    ok = False
                if ok:
                    rendered_templates.append(template)
                if len(rendered_templates) >= 3:
                    break

    if not rendered_templates:
        intent_signals = extract_intent_signals(question)
        templates = template_order(spec, intent_signals)

        date_col, year_col, month_col = _pick_time_columns(work)
        category_col = _pick_category_column(work)
        value_col = _pick_value_column(work)
        growth_pct_col = _find_column(work, ["kasvu_pct", "muutos_pct", "growth_pct", "yoy_pct"])
        growth_eur_col = _find_column(work, ["kasvu_eur", "muutos_eur", "muutos_abs_eur", "growth_eur", "delta_eur"])

        if value_col:
            work[value_col] = _to_numeric_series(work[value_col])
        if growth_pct_col:
            work[growth_pct_col] = _to_numeric_series(work[growth_pct_col])
        if growth_eur_col:
            work[growth_eur_col] = _to_numeric_series(work[growth_eur_col])

        time_axis = _build_time_axis(work, date_col, year_col, month_col)
        if time_axis is not None:
            work["_time_axis"] = time_axis

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
    return rendered_templates

def main():
    st.set_page_config(page_title="Budjettihaukka", layout="wide", initial_sidebar_state="collapsed")
    apply_custom_theme()
    analytics_client = BudgetAnalyticsClient()
    if "query_session_id" not in st.session_state:
        st.session_state["query_session_id"] = str(uuid4())
    admin_mode = _is_truthy_query_param("admin")
    render_top_nav(admin_mode=admin_mode)
    if admin_mode:
        render_admin_view()
        return
    st.markdown('<div class="bh-hero-title">BUDJETTIHAUKKA</div>', unsafe_allow_html=True)
    st.write(
        "Budjettihaukka tekee julkisesta taloudesta läpinäkyvämpää ja päätöksenteosta ymmärrettävämpää. "
        "Sen tavoitteena on auttaa näkemään, mihin yhteisiä varoja käytetään, miten priorisoinnit muuttuvat ja "
        "mitä vaikutuksia niillä voi olla vapauden, vastuun ja tehokkuuden näkökulmasta. "
        "Budjettihaukan tarkoituksena on tuoda talouspolitiikkaan liittyvä tieto helposti saataville, "
        "analysoitavaksi ja visualisoitavaksi. Sovelluksen käyttäjä voi esittää kysymyksiä luonnollisella kielellä, "
        "ja tekoälyn avulla saa kansantaloudelliseen optimaalisuuteen ja empiiriseen taloustutkimukseen perustuvia "
        "analyyseja. Tulokset voidaan näyttää taulukkoina, dynaamisina visualisointeina sekä analyyttisinä "
        "raportteina. Liberaalipuolue - vapaus valita"
    )
    debug_mode = False
    
    # Normaalin sovelluksen kulku tästä eteenpäin
    # Input for natural language question
    question = st.text_area("KIRJOITA KYSYMYKSESI:", placeholder="Esim. Mitkä olivat puolustusministeriön menot vuonna 2023?", height=100)
    clarification_choices: dict[str, str] = {}
    interpreted_spec: AnalysisSpec | None = None
    clarification_required = False
    missing_required: list[str] = []
    if question.strip():
        interpreted_spec, clarification_choices, clarification_required, missing_required = render_interpretation_block(question)

    submit_clicked = st.button("Hae tulokset")
    render_banner_ad(settings.adsense_slot_top, "Yläbanneri")

    if submit_clicked:
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
                if clarification_required and missing_required:
                    st.error("Valitse pakolliset tarkennukset ennen kyselyn ajoa.")
                    _log_question_library_event(
                        question=question,
                        session_id=st.session_state["query_session_id"],
                        status="blocked_clarification",
                        analysis_spec=interpreted_spec,
                        clarification_required=True,
                        clarification_choices=clarification_choices,
                        missing_required=missing_required,
                        error_class="clarification_required",
                        error_message="Pakollinen tarkennus puuttuu.",
                    )
                    log_query_event(
                        {
                            "question": question.strip(),
                            "query_source": "blocked_clarification",
                            "contract": None,
                            "confidence": interpreted_spec.confidence if isinstance(interpreted_spec, AnalysisSpec) else None,
                            "clarification_required": True,
                            "clarification_missing_fields": missing_required,
                            "clarification_applied": False,
                            "retries": 0,
                            "dry_run_bytes": None,
                            "render_template": [],
                            "query_success": False,
                            "chart_render_success": False,
                            "error_class": "clarification_required",
                        }
                    )
                    return

                result = analytics_client.analyze(
                    question,
                    clarifications=clarification_choices,
                    language="fi",
                    ui_context={"surface": "streamlit"},
                )
                execution_question = result.get("execution_question") or question
                sql_query = result.get("sql_query", "")
                results = _results_df_from_api_response(result)
                error = result.get("error")
                explanation = result.get("explanation")
                analysis_spec_payload = result.get("analysis_spec") or {}
                analysis_spec = (
                    deserialize_analysis_spec(analysis_spec_payload)
                    if analysis_spec_payload
                    else interpreted_spec
                )
                query_source = result.get("query_source")
                query_contract = result.get("query_contract")
                query_plan = (result.get("metadata") or {}).get("query_plan")

                if debug_mode and query_source:
                    st.caption(f"Kyselypolku: {query_source}" + (f" ({query_contract})" if query_contract else ""))
                if debug_mode and query_plan:
                    st.caption(f"QueryPlan: {query_plan}")

                render_trust_badge(result)

                if result.get("status") == "clarification_required":
                    st.error("Analyysi tarvitsee tarkennuksen ennen ajoa.")
                    for warning in result.get("warnings") or []:
                        st.caption(warning)
                    _log_question_library_event(
                        question=question,
                        session_id=st.session_state["query_session_id"],
                        status="clarification_required",
                        analysis_spec=analysis_spec,
                        query_source="clarification_required",
                        clarification_required=True,
                        clarification_choices=clarification_choices,
                        missing_required=missing_required,
                        result=result,
                        error_class="clarification_required",
                        error_message="Analyysi tarvitsee tarkennuksen ennen ajoa.",
                    )
                    log_query_event(
                        {
                            "query_id": result.get("query_id"),
                            "question": question.strip(),
                            "query_source": "clarification_required",
                            "contract": None,
                            "confidence": analysis_spec.confidence if isinstance(analysis_spec, AnalysisSpec) else None,
                            "clarification_required": True,
                            "clarification_missing_fields": missing_required,
                            "clarification_applied": bool(clarification_choices),
                            "retries": int(result.get("retries") or 0),
                            "dry_run_bytes": result.get("dry_run_bytes"),
                            "render_template": [],
                            "query_success": False,
                            "chart_render_success": False,
                            "error_class": "clarification_required",
                        }
                    )
                    return

                if result.get("status") == "unsupported":
                    st.warning("Kysymystä ei voida tällä hetkellä vastata riittävän luotettavasti käytettävissä olevasta datasta.")
                    if explanation:
                        st.caption(explanation)
                    for warning in result.get("warnings") or []:
                        st.caption(warning)
                    _log_question_library_event(
                        question=question,
                        session_id=st.session_state["query_session_id"],
                        status="unsupported",
                        analysis_spec=analysis_spec,
                        query_source=query_source,
                        query_contract=query_contract,
                        clarification_required=clarification_required,
                        clarification_choices=clarification_choices,
                        missing_required=missing_required,
                        result=result,
                        error_class="unsupported",
                        error_message="Kysymystä ei voida vastata riittävän luotettavasti.",
                    )
                    return

                if not sql_query and result.get("status") != "success":
                    st.error("SQL-kyselyn generointi epäonnistui. Kokeile muotoilla kysymyksesi toisin.")
                    _log_question_library_event(
                        question=question,
                        session_id=st.session_state["query_session_id"],
                        status="sql_generation_failed",
                        analysis_spec=analysis_spec,
                        query_source=query_source,
                        query_contract=query_contract,
                        clarification_required=clarification_required,
                        clarification_choices=clarification_choices,
                        missing_required=missing_required,
                        result=result,
                        error_class=result.get("error_class") or "sql_generation_failed",
                        error_message="SQL-kyselyn generointi epäonnistui.",
                    )
                    log_query_event(
                        {
                            "query_id": result.get("query_id"),
                            "question": question.strip(),
                            "query_source": query_source,
                            "contract": query_contract,
                            "confidence": analysis_spec.confidence if isinstance(analysis_spec, AnalysisSpec) else None,
                            "clarification_required": clarification_required,
                            "clarification_missing_fields": missing_required,
                            "clarification_applied": bool(clarification_choices),
                            "retries": int(result.get("retries") or 0),
                            "dry_run_bytes": result.get("dry_run_bytes"),
                            "render_template": [],
                            "query_success": False,
                            "chart_render_success": False,
                            "error_class": result.get("error_class") or "sql_generation_failed",
                        }
                    )
                    return

                # Näytetään generoitu SQL kehittäjille tai debug-tilassa
                if debug_mode:
                    st.subheader("Generoitu SQL-kysely:")
                    st.code(sql_query, language="sql")

                if error:
                    st.error(f"Kyselyn suoritus epäonnistui: {error}")
                    _log_question_library_event(
                        question=question,
                        session_id=st.session_state["query_session_id"],
                        status="query_error",
                        analysis_spec=analysis_spec,
                        query_source=query_source,
                        query_contract=query_contract,
                        clarification_required=clarification_required,
                        clarification_choices=clarification_choices,
                        missing_required=missing_required,
                        result=result,
                        results_df=results,
                        error_class=result.get("error_class"),
                        error_message=str(error),
                    )
                    log_query_event(
                        {
                            "query_id": result.get("query_id"),
                            "question": question.strip(),
                            "query_source": query_source,
                            "contract": query_contract,
                            "confidence": analysis_spec.confidence if isinstance(analysis_spec, AnalysisSpec) else None,
                            "clarification_required": clarification_required,
                            "clarification_missing_fields": missing_required,
                            "clarification_applied": bool(clarification_choices),
                            "retries": int(result.get("retries") or 0),
                            "dry_run_bytes": result.get("dry_run_bytes"),
                            "render_template": [],
                            "query_success": False,
                            "chart_render_success": False,
                            "error_class": result.get("error_class"),
                        }
                    )
                    return

                rendered_templates: list[str] = []
                if results is not None and not results.empty:
                    # Visualisoi tulokset
                    rendered_templates = visualize_data(
                        results,
                        title="Kyselyjen tulokset visualisoituna",
                        question=execution_question,
                        analysis_spec=analysis_spec if isinstance(analysis_spec, AnalysisSpec) else None,
                        query_contract=query_contract,
                    )
                    render_budget_moment_evidence(
                        question=execution_question,
                        results_df=results,
                        analysis_spec=analysis_spec if isinstance(analysis_spec, AnalysisSpec) else None,
                        evidence_rows=result.get("used_moments") or [],
                    )
                else:
                    st.warning("Kysely ei palauttanut tuloksia. Kokeile muokata kysymystäsi.")

                _log_question_library_event(
                    question=question,
                    session_id=st.session_state["query_session_id"],
                    status="success" if results is not None and not results.empty else "empty_result",
                    analysis_spec=analysis_spec,
                    query_source=query_source,
                    query_contract=query_contract,
                    clarification_required=clarification_required,
                    clarification_choices=clarification_choices,
                    missing_required=missing_required,
                    result=result,
                    results_df=results,
                    error_class=result.get("error_class"),
                    error_message=None,
                )
                log_query_event(
                    {
                        "query_id": result.get("query_id"),
                        "question": question.strip(),
                        "query_source": query_source,
                        "contract": query_contract,
                        "confidence": analysis_spec.confidence if isinstance(analysis_spec, AnalysisSpec) else None,
                        "clarification_required": clarification_required,
                        "clarification_missing_fields": missing_required,
                        "clarification_applied": bool(clarification_choices),
                        "retries": int(result.get("retries") or 0),
                        "dry_run_bytes": result.get("dry_run_bytes"),
                        "render_template": rendered_templates,
                        "query_success": True,
                        "chart_render_success": bool(rendered_templates),
                        "error_class": result.get("error_class"),
                    }
                )
            
            except Exception as e:
                st.error(f"Virhe sovelluksessa: {str(e)}")
                _log_question_library_event(
                    question=question,
                    session_id=st.session_state["query_session_id"],
                    status="app_exception",
                    analysis_spec=interpreted_spec,
                    clarification_required=clarification_required,
                    clarification_choices=clarification_choices,
                    missing_required=missing_required,
                    error_class="app_exception",
                    error_message=str(e),
                )
                log_query_event(
                    {
                        "question": question.strip(),
                        "query_source": "app_exception",
                        "contract": None,
                        "confidence": interpreted_spec.confidence if isinstance(interpreted_spec, AnalysisSpec) else None,
                        "clarification_required": clarification_required,
                        "clarification_missing_fields": missing_required,
                        "clarification_applied": bool(clarification_choices),
                        "retries": 0,
                        "dry_run_bytes": None,
                        "render_template": [],
                        "query_success": False,
                        "chart_render_success": False,
                        "error_class": "app_exception",
                    }
                )
                if debug_mode:
                    st.exception(e)

    # Alatunnisteen näyttäminen
    st.markdown("---")
    render_footer_logo()
    render_banner_ad(settings.adsense_slot_bottom, "Alabanneri")

if __name__ == "__main__":
    main()

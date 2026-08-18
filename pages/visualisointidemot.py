from __future__ import annotations

import altair as alt
import streamlit as st

from services.enrichment_demo_data import (
    DEFLATOR_LABELS,
    education_series,
    final_account_scale,
    grant_funnels,
    health_scatter,
    load_reference_data,
    purchasing_power_series,
)


YELLOW = "#f6d84f"
YELLOW_SOFT = "#fff3b6"
INK = "#111111"
BLUE = "#0067a3"
ORANGE = "#d95f02"
GREEN = "#228833"
PURPLE = "#7b3294"


def page_style() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;600;700&family=Raleway:wght@600;700;800&display=swap');
        .stApp, [data-testid="stAppViewContainer"], [data-testid="stHeader"] { background: #f6d84f; color: #111; }
        .block-container { max-width: 1180px; padding-top: 2rem; padding-bottom: 4rem; }
        h1, h2, h3 { font-family: 'Raleway', sans-serif !important; color: #111 !important; }
        p, label, div, span { font-family: 'Open Sans', sans-serif; color: #111; }
        h1 { text-transform: uppercase; letter-spacing: -0.04em; }
        [data-testid="stMetric"], [data-testid="stExpander"] { background: #fff3b6; border: 2px solid #111; border-radius: 16px; padding: .7rem; }
        [data-testid="stSelectbox"] > div > div, [data-testid="stMultiSelect"] > div > div,
        [data-testid="stNumberInput"] input { background: #fff9d6 !important; }
        .bh-back { display:inline-block; padding:.45rem .85rem; border:2px solid #111; border-radius:999px; background:#fff3b6; color:#111 !important; text-decoration:none !important; font-weight:700; }
        .bh-number { font-family:'Raleway',sans-serif; font-weight:800; letter-spacing:.02em; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def chart_base(chart: alt.Chart) -> alt.Chart:
    return chart.configure_view(stroke=INK, strokeWidth=1).configure_axis(
        labelColor=INK,
        titleColor=INK,
        gridColor="#11111122",
        domainColor=INK,
        tickColor=INK,
        labelFont="Open Sans",
        titleFont="Open Sans",
    ).configure_legend(labelColor=INK, titleColor=INK, labelFont="Open Sans", titleFont="Open Sans")


@st.cache_data
def data():
    return load_reference_data()


st.set_page_config(page_title="Budjettihaukka – visualisointidemot", layout="wide")
page_style()
refs = data()

st.markdown('<a class="bh-back" href="/" target="_self">← Takaisin Budjettihaukkaan</a>', unsafe_allow_html=True)
st.title("Viisi tapaa nähdä julkinen data")
st.write(
    "Sama rikastettu tietopohja voi vastata eri kysymyksiin: kehitykseen, vertailuun, yhteyksiin, "
    "valintaprosessiin ja talouden mittakaavaan. Kaaviot käyttävät tarkistettuja lähdepoimintoja."
)

st.header("1. Mitä miljardilla saa eri vuosina?")
left, right = st.columns([1, 2.4])
with left:
    amount_million = st.number_input("Nimellinen määräraha (milj. €)", 100, 10_000, 1_000, 100)
    choices = st.multiselect(
        "Hintakehitys",
        options=list(DEFLATOR_LABELS),
        default=["cpi_general_purchasing_power", "public_service_cost_municipal"],
        format_func=DEFLATOR_LABELS.get,
    )
    st.caption("Kaikki viivat muunnetaan vuoden 2024 ostovoimaan. Rakennuskustannussarja alkaa vuodesta 2021.")
with right:
    real = purchasing_power_series(refs["deflators"], amount_million * 1_000_000, choices)
    if real.empty:
        st.info("Valitse vähintään yksi hintasarja.")
    else:
        purchasing_chart = alt.Chart(real).mark_line(point=True, strokeWidth=3).encode(
            x=alt.X("year:O", title="Vuosi"),
            y=alt.Y("real_amount_million_eur:Q", title="Ostovoima vuoden 2024 rahassa (milj. €)", scale=alt.Scale(zero=False)),
            color=alt.Color("deflator_label:N", title="Hintasarja", scale=alt.Scale(range=[BLUE, ORANGE, GREEN])),
            strokeDash=alt.StrokeDash("deflator_label:N", legend=None),
            tooltip=[
                alt.Tooltip("year:O", title="Vuosi"),
                alt.Tooltip("deflator_label:N", title="Hintasarja"),
                alt.Tooltip("real_amount_million_eur:Q", title="2024 rahassa", format=".1f"),
            ],
        ).properties(height=340)
        st.altair_chart(chart_base(purchasing_chart), use_container_width=True)
st.caption("Lähde: Tilastokeskus; kuluttajahinta-, julkisten menojen hinta- ja rakennuskustannusindeksit.")

st.header("2. Koulutuksen tuotokset ja tulokset rinnakkain")
available_regions = sorted(refs["sectors"].query("dashboard_id == 'education'")["region_name_fi"].unique())
regions = st.multiselect(
    "Vertailualueet",
    available_regions,
    default=["Koko maa", "Uusimaa", "Lappi"],
    max_selections=5,
)
education = education_series(refs["sectors"], regions)
if education.empty:
    st.info("Valitse vähintään yksi alue.")
else:
    education_chart = alt.Chart(education).mark_line(point=True, strokeWidth=2.5).encode(
        x=alt.X("year:O", title="Vuosi"),
        y=alt.Y("display_value:Q", title=None, scale=alt.Scale(zero=False)),
        color=alt.Color("region_name_fi:N", title="Alue"),
        strokeDash=alt.StrokeDash("region_name_fi:N", legend=None),
        tooltip=[
            alt.Tooltip("year:O", title="Vuosi"),
            alt.Tooltip("region_name_fi:N", title="Alue"),
            alt.Tooltip("display_metric:N", title="Mittari"),
            alt.Tooltip("display_value:Q", title="Arvo", format=".1f"),
        ],
    ).properties(height=230).facet(row=alt.Row("display_metric:N", title=None))
    st.altair_chart(chart_base(education_chart), use_container_width=True)
st.caption("Tuotoksen ja tuloksen samanaikainen muutos näkyy, mutta kaavio ei väitä niiden välillä syy-seuraussuhdetta.")

st.header("3. Palvelujen käyttö ja koettu terveys")
health_year = st.select_slider("Vuosi", options=[2018, 2020, 2022, 2024], value=2024)
health = health_scatter(refs["sectors"], health_year)
country = health[health["region_name_fi"].eq("Koko maa")]
regions_only = health[health["region_name_fi"].ne("Koko maa")]
scatter = alt.Chart(regions_only).mark_circle(size=150, opacity=0.85, color=BLUE, stroke=INK).encode(
    x=alt.X("primary_care_doctor_contacts_per_1000:Q", title="Lääkärikäynnit / 1 000 asukasta", scale=alt.Scale(zero=False)),
    y=alt.Y("self_rated_health_mediocre_or_worse_pct:Q", title="Terveytensä keskitasoiseksi tai huonommaksi kokevat (%)", scale=alt.Scale(zero=False)),
    tooltip=[
        alt.Tooltip("region_name_fi:N", title="Alue"),
        alt.Tooltip("primary_care_doctor_contacts_per_1000:Q", title="Lääkärikäynnit", format=".0f"),
        alt.Tooltip("self_rated_health_mediocre_or_worse_pct:Q", title="Koettu terveys", format=".1f"),
    ],
)
labels = alt.Chart(regions_only).mark_text(dx=8, align="left", fontSize=11, color=INK).encode(
    x="primary_care_doctor_contacts_per_1000:Q",
    y="self_rated_health_mediocre_or_worse_pct:Q",
    text="region_name_fi:N",
)
if not country.empty:
    cross = alt.Chart(country).mark_point(shape="diamond", size=230, filled=True, color=ORANGE, stroke=INK).encode(
        x="primary_care_doctor_contacts_per_1000:Q",
        y="self_rated_health_mediocre_or_worse_pct:Q",
        tooltip=[alt.Tooltip("region_name_fi:N", title="Vertailupiste")],
    )
    scatter = scatter + labels + cross
else:
    scatter = scatter + labels
st.altair_chart(chart_base(scatter.properties(height=440)), use_container_width=True)
st.caption("Timantti on koko maan taso. Alueiden välinen yhteys on kuvaileva, ei arvio palvelujen kausaalivaikutuksesta.")

st.header("4. Miten hakemukset muuttuvat myönnöiksi?")
funnels = grant_funnels(refs["grants"])
funnel_chart = alt.Chart(funnels).mark_bar(cornerRadiusEnd=8, height=48).encode(
    x=alt.X("share:Q", title="Osuus lähtötilanteesta", axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
    y=alt.Y("stage:N", title=None, sort=alt.SortField("stage_order")),
    color=alt.Color("stage:N", legend=None, scale=alt.Scale(range=[BLUE, ORANGE])),
    tooltip=[
        alt.Tooltip("measure:N", title="Näkökulma"),
        alt.Tooltip("stage:N", title="Vaihe"),
        alt.Tooltip("display_value:N", title="Määrä"),
        alt.Tooltip("share:Q", title="Osuus", format=".1%"),
    ],
).properties(height=150).facet(column=alt.Column("measure:N", title=None))
st.altair_chart(chart_base(funnel_chart), use_container_width=True)
st.caption("OKM-pilotti: 8 786 hakemusta ja 1 664,4 milj. € haettua; 5 260 myönteistä päätöstä ja 886,7 milj. € myönnettyä.")

st.header("5. Valtion tilinpäätös samassa mittakaavassa")
accounts = final_account_scale(refs["final_accounts"])
account_bars = alt.Chart(accounts).mark_bar(cornerRadiusEnd=7, color=PURPLE, height=28).encode(
    x=alt.X("value_billion_eur:Q", title="Miljardia euroa"),
    y=alt.Y("metric_label:N", title=None, sort="-x"),
    tooltip=[
        alt.Tooltip("metric_label:N", title="Erä"),
        alt.Tooltip("value_billion_eur:Q", title="Mrd. €", format=".1f"),
    ],
)
account_labels = alt.Chart(accounts).mark_text(align="left", dx=6, color=INK, fontWeight="bold").encode(
    x="value_billion_eur:Q",
    y=alt.Y("metric_label:N", sort="-x"),
    text=alt.Text("value_billion_eur:Q", format=".1f"),
)
st.altair_chart(chart_base((account_bars + account_labels).properties(height=350)), use_container_width=True)
st.caption("Vuoden 2025 viralliset tilinpäätösluvut. Palkit näyttävät erien koon, eivät sitä, että eri käsitteet voisi laskea yhteen.")

with st.expander("Datan alkuperä ja tulkintarajat"):
    st.write(
        "Deflaattorit ja alueindikaattorit ovat Tilastokeskuksen lähteistä. Avustuspilotti perustuu "
        "Tutkiavustuksia-palvelun koontiraporttiin ja tilinpäätösluvut valtion vuoden 2025 tilinpäätökseen. "
        "Visualisoinnit ovat kuvailevia; ne eivät yksin osoita politiikkatoimenpiteiden vaikutuksia."
    )

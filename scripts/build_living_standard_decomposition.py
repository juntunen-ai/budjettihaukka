#!/usr/bin/env python3
"""Hajottaa asukaskohtaisen BKT:n neljään tekijään ja piirtää tuloksen
tiedostoon docs/figures/elintason_hajotelma_2008_2025.png.

Kysymys: miksi elintaso asukasta kohden ei ole palannut vuoden 2008 tasolle?

Vastaus saadaan identiteetistä, joka pätee tarkalleen eikä ole malli:

    BKT / väestö = (BKT / työtunnit)          tuottavuus
                 x (työtunnit / työlliset)    tunnit työllistä kohden
                 x (työlliset / työikäiset)   työllisyysaste
                 x (työikäiset / väestö)      väestörakenne

Kun jokainen tekijä indeksoidaan vuoteen 2008, niiden tulo on tasan
asukaskohtaisen BKT:n indeksi. Hajotelma ei kerro syytä vaan sen, mihin
kohtaan ketjua muutos kohdistui.

Työikäisten määrä lasketaan summaamalla keskiväkiluku ikävuosilta 15-64,
koska Tilastokeskuksen rajapinta ei tarjoa valmista ikäryhmäsummaa.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings

OUT_PNG = ROOT / "docs" / "figures" / "elintason_hajotelma_2008_2025.png"
OUT_JSON = ROOT / "data" / "reference" / "living_standard_decomposition_v1.json"

BASE_YEAR, END_YEAR = 2008, 2025
TREND_FROM = 1995
POPULATION_URL = "https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/vaerak/11s1.px"
GDP_URL = "https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/ntp/15a9.px"
HOURS_URL = "https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/ntp/15ab.px"
VOLUME_CONTENT = "ntp-vv_hinta_2015"
MISSING = {".", "..", "...", "", None}

FACTORS = (
    ("productivity_index", "Tuottavuus\nBKT / työtunti", "#1B6CA8"),
    ("hours_per_worker_index", "Tunnit\ntyöllistä kohden", "#C94F1B"),
    ("employment_rate_index", "Työllisyysaste\n15–64-vuotiaat", "#2E7D46"),
    ("demography_index", "Väestörakenne\ntyöikäisten osuus", "#8E3B8F"),
)
PALETTE = {"ink": "#171713", "base": "#5F5B50", "paper": "#FBFAF7",
           "grid": "#D9D5CC", "total": "#171713"}


def _px(url: str, query: dict[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(
        url, data=json.dumps(query).encode("utf-8"), headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.load(response)


def fetch_working_age(year_from: int, year_to: int) -> dict[int, float]:
    """Keskiväkiluku 15-64, summattuna yksittäisistä ikävuosista."""
    payload = _px(POPULATION_URL, {
        "query": [
            {"code": "alue_23_20260101", "selection": {"filter": "item", "values": ["SSS"]}},
            {"code": "sukupuoli_9_20180101", "selection": {"filter": "item", "values": ["SSS"]}},
            {"code": "ikaryhma_10_20180101",
             "selection": {"filter": "item", "values": [f"{age:03d}" for age in range(15, 65)]}},
            {"code": "timeperiod_y",
             "selection": {"filter": "item",
                           "values": [str(y) for y in range(year_from, year_to + 1)]}},
            {"code": "contentscode", "selection": {"filter": "item", "values": ["vaerak-keskiv"]}},
        ],
        "response": {"format": "json"},
    })
    totals: dict[int, float] = {}
    for row in payload["data"]:
        if row["values"][0] in MISSING:
            continue
        year = int(next(key for key in row["key"] if key.isdigit() and len(key) == 4))
        totals[year] = totals.get(year, 0.0) + float(row["values"][0])
    if not totals:
        raise ValueError("Työikäistä väestöä ei saatu")
    return totals


def fetch_gdp_volume(year_from: int, year_to: int) -> dict[int, float]:
    payload = _px(GDP_URL, {
        "query": [
            {"code": "taloustoimi_1_20180101", "selection": {"filter": "item", "values": ["B1GMH"]}},
            {"code": "timeperiod_y",
             "selection": {"filter": "item",
                           "values": [str(y) for y in range(year_from, year_to + 1)]}},
            {"code": "contentscode", "selection": {"filter": "item", "values": [VOLUME_CONTENT]}},
        ],
        "response": {"format": "json"},
    })
    return {int(row["key"][-1]): float(row["values"][0])
            for row in payload["data"] if row["values"][0] not in MISSING}


def fetch_hours(year_from: int, year_to: int) -> dict[int, float]:
    payload = _px(HOURS_URL, {
        "query": [
            {"code": "taloustoimi_1_20180101", "selection": {"filter": "item", "values": ["E2"]}},
            {"code": "sektoriluokitus_7_20230101", "selection": {"filter": "item", "values": ["S1"]}},
            {"code": "toimiala_79_20180101", "selection": {"filter": "item", "values": ["SSS"]}},
            {"code": "timeperiod_y",
             "selection": {"filter": "item",
                           "values": [str(y) for y in range(year_from, year_to + 1)]}},
        ],
        "response": {"format": "json"},
    })
    return {int(row["key"][3]): float(row["values"][0]) * 1e6
            for row in payload["data"] if row["values"][0] not in MISSING}


def fetch_from_warehouse(project: str, dataset: str) -> dict[str, dict[int, float]]:
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    sql = f"""
    SELECT series_id, year, value
    FROM `{project}.{dataset}.official_macro_reference_v1`
    WHERE series_id IN ('population_midyear_persons', 'employed_persons_thousands')
      AND year BETWEEN {BASE_YEAR} AND {END_YEAR}
    """
    out: dict[str, dict[int, float]] = {}
    for row in client.query(sql).result():
        out.setdefault(row["series_id"], {})[row["year"]] = float(row["value"])
    return out


def build_payload(
    *,
    gdp: dict[int, float],
    hours: dict[int, float],
    employed: dict[int, float],
    working_age: dict[int, float],
    population: dict[int, float],
    trend_gdp: dict[int, float],
    trend_hours: dict[int, float],
) -> dict[str, Any]:
    years = sorted(set(gdp) & set(hours) & set(employed) & set(working_age) & set(population))
    if BASE_YEAR not in years:
        raise ValueError(f"Perusvuosi {BASE_YEAR} puuttuu")

    def factors(year: int) -> dict[str, float]:
        return {
            "productivity": gdp[year] / hours[year],
            "hours_per_worker": hours[year] / employed[year],
            "employment_rate": employed[year] / working_age[year],
            "demography": working_age[year] / population[year],
            "per_capita": gdp[year] / population[year],
        }

    base = factors(BASE_YEAR)
    rows = []
    for year in years:
        current = factors(year)
        row = {"year": year}
        for key, index_name in (
            ("productivity", "productivity_index"),
            ("hours_per_worker", "hours_per_worker_index"),
            ("employment_rate", "employment_rate_index"),
            ("demography", "demography_index"),
            ("per_capita", "gdp_per_capita_index"),
        ):
            row[index_name] = 100 * current[key] / base[key]
        rows.append(row)

    # Tuottavuuden trendivertailu: mihin oltaisiin paadytty, jos ennen
    # finanssikriisia vallinnut vauhti olisi jatkunut.
    trend_years = sorted(set(trend_gdp) & set(trend_hours))
    productivity = {year: trend_gdp[year] / trend_hours[year] for year in trend_years}
    span = BASE_YEAR - TREND_FROM
    pre_cagr = (productivity[BASE_YEAR] / productivity[TREND_FROM]) ** (1 / span) - 1
    post_span = END_YEAR - BASE_YEAR
    post_cagr = (productivity[END_YEAR] / productivity[BASE_YEAR]) ** (1 / post_span) - 1

    return {
        "meta": {
            "dataset_id": "living_standard_decomposition_v1",
            "base_year": BASE_YEAR,
            "end_year": END_YEAR,
            "identity": "BKT/väestö = (BKT/tunnit) x (tunnit/työlliset) x "
                        "(työlliset/työikäiset) x (työikäiset/väestö)",
            "productivity_cagr_pre_pct": 100 * pre_cagr,
            "productivity_cagr_post_pct": 100 * post_cagr,
            "productivity_trend_from": TREND_FROM,
            "productivity_index_if_trend": 100 * (1 + pre_cagr) ** post_span,
            "working_age_definition": "keskiväkiluku 15-64, summattu ikävuosittain",
            "sources": [
                {"label": "Tilastokeskus, kansantalouden tilinpito (BKT:n volyymi, työtunnit)",
                 "url": GDP_URL},
                {"label": "Tilastokeskus, työllisyys ja työtunnit", "url": HOURS_URL},
                {"label": "Tilastokeskus, keskiväkiluku", "url": POPULATION_URL},
            ],
        },
        "years": rows,
    }


def validate(payload: dict[str, Any]) -> None:
    rows = payload["years"]
    if not rows:
        raise ValueError("Vuosia ei muodostunut")

    base = next(row for row in rows if row["year"] == payload["meta"]["base_year"])
    for key, value in base.items():
        if key != "year" and abs(value - 100) > 1e-9:
            raise ValueError(f"Perusvuoden {key} ei ole 100")

    # Identiteetin on toteuduttava jokaisena vuotena. Jos tama pettaa,
    # jokin tekija on laskettu eri nimittajalla ja koko selitys kaatuu.
    for row in rows:
        product = 1.0
        for key, _label, _color in FACTORS:
            product *= row[key] / 100
        if abs(100 * product - row["gdp_per_capita_index"]) > 1e-6:
            raise ValueError(f"Hajotelma ei täsmää vuonna {row['year']}")

    meta = payload["meta"]
    if meta["productivity_cagr_pre_pct"] <= meta["productivity_cagr_post_pct"]:
        raise ValueError("Tuottavuuden kasvu ei hidastunut, vertailun peruste ei päde")


def fi(value: float, digits: int = 1, sign: bool = False) -> str:
    """Suomalainen desimaalipilkku vain lukuun, ei ymparoivaan lauseeseen."""
    text = f"{value:+.{digits}f}" if sign else f"{value:.{digits}f}"
    return text.replace(".", ",")


def render(payload: dict[str, Any], out_png: Path, dpi: int) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MultipleLocator

    rows = payload["years"]
    meta = payload["meta"]
    last = rows[-1]
    years = [row["year"] for row in rows]

    fig, (ax1, ax2) = plt.subplots(
        1, 2, figsize=(15.4, 7.6), gridspec_kw={"width_ratios": [1.32, 1]}
    )
    fig.patch.set_facecolor(PALETTE["paper"])

    # Vasen: tekijoiden kehitys aikasarjana.
    ax1.set_facecolor(PALETTE["paper"])
    ax1.axhline(100, color=PALETTE["base"], linestyle="--", linewidth=1.5, zorder=2)
    for key, label, color in FACTORS:
        values = [row[key] for row in rows]
        ax1.plot(years, values, color=color, linewidth=2.5, marker="o", markersize=4,
                 markerfacecolor=PALETTE["paper"], markeredgewidth=1.4,
                 label=label.replace("\n", " "), zorder=4)
        ax1.annotate(fi(values[-1], 0), xy=(years[-1], values[-1]),
                     xytext=(6, 0), textcoords="offset points", color=color,
                     fontsize=10.5, fontweight="bold", va="center")
    per_capita = [row["gdp_per_capita_index"] for row in rows]
    ax1.plot(years, per_capita, color=PALETTE["total"], linewidth=3.2, linestyle=(0, (5, 2)),
             label="BKT asukasta kohden (tekijöiden tulo)", zorder=5)
    ax1.annotate(fi(per_capita[-1], 0), xy=(years[-1], per_capita[-1]),
                 xytext=(6, -12), textcoords="offset points", color=PALETTE["total"],
                 fontsize=10.5, fontweight="bold", va="center")

    ax1.set_xlim(BASE_YEAR - 0.4, END_YEAR + 2.2)
    ax1.xaxis.set_major_locator(MultipleLocator(3))
    ax1.yaxis.set_major_locator(MultipleLocator(4))
    ax1.grid(True, color=PALETTE["grid"], linewidth=0.8, alpha=0.7)
    ax1.set_axisbelow(True)
    for side in ("top", "right"):
        ax1.spines[side].set_visible(False)
    ax1.set_ylabel("Indeksi (2008 = 100)", fontsize=11, color=PALETTE["ink"])
    ax1.tick_params(colors=PALETTE["base"], labelsize=10)
    ax1.legend(loc="lower center", bbox_to_anchor=(0.5, -0.005), ncol=2, fontsize=9, frameon=True, framealpha=0.95,
               edgecolor=PALETTE["grid"], facecolor=PALETTE["paper"])
    ax1.set_title("Neljä tekijää, 2008 = 100", fontsize=13, fontweight="bold",
                  color=PALETTE["ink"], loc="left", pad=14)

    # Oikea: kunkin tekijan kokonaismuutos 2008-2025.
    ax2.set_facecolor(PALETTE["paper"])
    labels = [label for _key, label, _color in FACTORS]
    values = [last[key] - 100 for key, _label, _color in FACTORS]
    colors = [color for _key, _label, color in FACTORS]
    positions = list(range(len(values)))
    ax2.barh(positions, values, color=colors, edgecolor=PALETTE["ink"], linewidth=1.1, height=0.6)
    for pos, value in zip(positions, values):
        offset = 0.35 if value >= 0 else -0.35
        ax2.text(value + offset, pos, f"{fi(value, 1, sign=True)} %",
                 va="center", ha="left" if value >= 0 else "right",
                 fontsize=11.5, fontweight="bold", color=PALETTE["ink"])
    total = last["gdp_per_capita_index"] - 100
    ax2.barh([len(values)], [total], color=PALETTE["total"], edgecolor=PALETTE["ink"],
             linewidth=1.1, height=0.6)
    ax2.text(total - 0.35, len(values), f"{fi(total, 1, sign=True)} %",
             va="center", ha="right", fontsize=11.5, fontweight="bold", color=PALETTE["ink"])

    ax2.set_yticks(positions + [len(values)])
    ax2.set_yticklabels(labels + ["BKT asukasta kohden\nyhteensä"], fontsize=10)
    ax2.invert_yaxis()
    ax2.axvline(0, color=PALETTE["ink"], linewidth=1.4)
    ax2.set_xlim(-11, 11)
    ax2.grid(True, axis="x", color=PALETTE["grid"], linewidth=0.8, alpha=0.7)
    ax2.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax2.spines[side].set_visible(False)
    ax2.tick_params(colors=PALETTE["base"], labelsize=10)
    # Vasen akseliviiva on piilotettu, joten sen viivamerkit jaisivat
    # roikkumaan nimikkeiden perään.
    ax2.tick_params(axis="y", length=0)
    ax2.set_xlabel("Muutos 2008 → 2025, %", fontsize=11, color=PALETTE["ink"])
    ax2.set_title("Kokonaismuutos tekijöittäin", fontsize=13, fontweight="bold",
                  color=PALETTE["ink"], loc="left", pad=10)

    fig.text(0.036, 0.955,
             "Miksi elintaso asukasta kohden ei ole palannut vuoden 2008 tasolle",
             fontsize=20, fontweight="bold", color=PALETTE["ink"], va="top")
    fig.text(0.036, 0.902,
             f"Työllisyysaste nousi {fi(last['employment_rate_index'] - 100, 1, sign=True)} "
             f"prosenttia, mutta työikäisten osuus väestöstä laski "
             f"{fi(last['demography_index'] - 100, 1, sign=True)} prosenttia. "
             f"Ne kumoavat lähes tarkalleen toisensa.",
             fontsize=11.5, color=PALETTE["base"], va="top")

    footnote = "\n".join([
        "Identiteetti: BKT/väestö = (BKT/työtunnit) × (työtunnit/työlliset) × "
        "(työlliset/työikäiset) × (työikäiset/väestö).\nTekijöiden tulo on tasan "
        "asukaskohtainen indeksi, mikä on tarkistettu joka vuodelta.",
        f"Tuottavuus kasvoi {fi(meta['productivity_cagr_pre_pct'], 2)} % vuodessa "
        f"{meta['productivity_trend_from']}–{BASE_YEAR} mutta vain "
        f"{fi(meta['productivity_cagr_post_pct'], 2)} % vuodessa {BASE_YEAR}–{END_YEAR}. "
        f"Vanhalla vauhdilla taso olisi nyt {fi(meta['productivity_index_if_trend'], 0)}, "
        f"toteutui {fi(last['productivity_index'], 0)}.",
        "Lähteet: Tilastokeskus, kansantalouden tilinpito (BKT:n volyymisarja viitevuosi 2015, "
        "työlliset ja tehdyt työtunnit) sekä keskiväkiluku (työikäiset 15–64 summattuna\nikävuosittain). Hajotelma kertoo mihin kohtaan ketjua muutos osui, ei sen syytä.",
    ])
    fig.text(0.036, 0.022, footnote, fontsize=8.8, color=PALETTE["base"],
             va="bottom", linespacing=1.6)

    fig.subplots_adjust(left=0.058, right=0.975, top=0.815, bottom=0.215, wspace=0.42)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=dpi, facecolor=fig.get_facecolor())
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description="Hajota asukaskohtainen BKT tekijöihin.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--out", type=Path, default=OUT_PNG)
    parser.add_argument("--dpi", type=int, default=300)
    args = parser.parse_args()

    warehouse = fetch_from_warehouse(args.project, args.dataset)
    population = warehouse["population_midyear_persons"]
    employed = {year: value * 1000 for year, value in warehouse["employed_persons_thousands"].items()}

    payload = build_payload(
        gdp=fetch_gdp_volume(BASE_YEAR, END_YEAR),
        hours=fetch_hours(BASE_YEAR, END_YEAR),
        employed=employed,
        working_age=fetch_working_age(BASE_YEAR, END_YEAR),
        population=population,
        trend_gdp=fetch_gdp_volume(TREND_FROM, END_YEAR),
        trend_hours=fetch_hours(TREND_FROM, END_YEAR),
    )
    validate(payload)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    render(payload, args.out, args.dpi)

    last = payload["years"][-1]
    print(f"{args.out.relative_to(ROOT)} ({args.dpi} dpi)")
    for key, label, _color in FACTORS:
        print(f"  {label.replace(chr(10), ' '):34} {last[key] - 100:+6.1f} %")
    print(f"  {'BKT asukasta kohden':34} {last['gdp_per_capita_index'] - 100:+6.1f} %")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

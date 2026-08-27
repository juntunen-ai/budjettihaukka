#!/usr/bin/env python3
"""Piirtää Suomen elintason ja talouskasvun kehityksen 2008-2025,
lähtötaso 2008 = 100, tiedostoon docs/figures/elintaso_2008_2025.png.

Kolme sarjaa:

1. Kokonais-BKT:n volyymi.
2. BKT asukasta kohden, eli sama volyymi jaettuna keskiväkiluvulla.
3. BKT asukasta kohden ilman asuntojen omistusta ja vuokrausta. Tämä on
   Etlan havainnon mukainen rajaus: toimialan 68201-68202 arvonlisäys
   sisältää omistusasumisen laskennallisen asuntotulon, joka on kasvanut
   muuta taloutta nopeammin. Osuus nousi 8,3 prosentista 10,3 prosenttiin
   vuosina 2008-2024.

Sarja 3 päättyy vuoteen 2024, koska Tilastokeskus ei ole vielä julkaissut
toimialoittaista arvonlisäystä vuodelle 2025. Sitä ei jatketa arvaamalla.

Ketjutetut volyymisarjat eivät ole tarkasti yhteenlaskettavia, joten
toimialan vähentäminen kokonais-BKT:stä on likiarvo. Lisäksi BKT on
markkinahintaan ja toimialan luku perushintaan, joten erotus ei ole
täsmällinen kansantalouden tilinpidon suure vaan tarkoituksellinen
rajauslaskelma.
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

OUT_PNG = ROOT / "docs" / "figures" / "elintaso_2008_2025.png"
OUT_JSON = ROOT / "data" / "reference" / "living_standard_index_v1.json"

BASE_YEAR = 2008
END_YEAR = 2025
GDP_URL = "https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/ntp/15a9.px"
INDUSTRY_URL = "https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/ntp/15ad.px"
HOUSING_CODE = "L68201_L68202"
VOLUME_CONTENT = "ntp-vv_hinta_2015"

MISSING = {".", "..", "...", "", None}

PALETTE = {
    "gdp": "#1B6CA8",
    "per_capita": "#E4761B",
    "ex_housing": "#8E3B8F",
    "base": "#5F5B50",
    "shortfall": "#C94F1B",
    "ink": "#171713",
    "paper": "#FBFAF7",
    "grid": "#D9D5CC",
}


def _px(url: str, query: dict[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(
        url, data=json.dumps(query).encode("utf-8"), headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=90) as response:
        return json.load(response)


def _years() -> list[str]:
    return [str(year) for year in range(BASE_YEAR, END_YEAR + 1)]


def fetch_gdp_volume() -> dict[int, float]:
    payload = _px(GDP_URL, {
        "query": [
            {"code": "taloustoimi_1_20180101", "selection": {"filter": "item", "values": ["B1GMH"]}},
            {"code": "timeperiod_y", "selection": {"filter": "item", "values": _years()}},
            {"code": "contentscode", "selection": {"filter": "item", "values": [VOLUME_CONTENT]}},
        ],
        "response": {"format": "json"},
    })
    return {
        int(row["key"][-1]): float(row["values"][0])
        for row in payload["data"] if row["values"][0] not in MISSING
    }


def fetch_housing_value_added() -> dict[int, float]:
    payload = _px(INDUSTRY_URL, {
        "query": [
            {"code": "sektoriluokitus_7_20230101", "selection": {"filter": "item", "values": ["S1"]}},
            {"code": "taloustoimi_1_20180101", "selection": {"filter": "item", "values": ["B1GPH"]}},
            {"code": "toimiala_79_20180101", "selection": {"filter": "item", "values": [HOUSING_CODE]}},
            {"code": "timeperiod_y", "selection": {"filter": "item", "values": _years()}},
            {"code": "contentscode", "selection": {"filter": "item", "values": [VOLUME_CONTENT]}},
        ],
        "response": {"format": "json"},
    })
    return {
        int(row["key"][-1]): float(row["values"][0])
        for row in payload["data"] if row["values"][0] not in MISSING
    }


def fetch_population(project: str, dataset: str) -> dict[int, float]:
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    sql = f"""
    SELECT year, value
    FROM `{project}.{dataset}.official_macro_reference_v1`
    WHERE series_id = 'population_midyear_persons'
      AND year BETWEEN {BASE_YEAR} AND {END_YEAR}
    ORDER BY year
    """
    return {row["year"]: float(row["value"]) for row in client.query(sql).result()}


def build_series(
    gdp: dict[int, float], housing: dict[int, float], population: dict[int, float]
) -> dict[str, Any]:
    for name, data in (("BKT", gdp), ("väestö", population)):
        if BASE_YEAR not in data:
            raise ValueError(f"{name}-sarjasta puuttuu perusvuosi {BASE_YEAR}")

    gdp0, pop0 = gdp[BASE_YEAR], population[BASE_YEAR]
    ex0 = gdp[BASE_YEAR] - housing[BASE_YEAR]

    rows = []
    for year in sorted(gdp):
        if year not in population:
            continue
        row: dict[str, Any] = {
            "year": year,
            "gdp_volume_index": 100 * gdp[year] / gdp0,
            "gdp_per_capita_index": 100 * (gdp[year] / population[year]) / (gdp0 / pop0),
            "gdp_per_capita_ex_housing_index": None,
            "housing_share_pct": None,
        }
        if year in housing:
            ex = gdp[year] - housing[year]
            row["gdp_per_capita_ex_housing_index"] = 100 * (ex / population[year]) / (ex0 / pop0)
            row["housing_share_pct"] = 100 * housing[year] / gdp[year]
        rows.append(row)

    return {
        "meta": {
            "dataset_id": "living_standard_index_v1",
            "base_year": BASE_YEAR,
            "index_note": f"{BASE_YEAR} = 100",
            "volume_content": VOLUME_CONTENT,
            "housing_industry": HOUSING_CODE,
            "ex_housing_last_year": max(
                (row["year"] for row in rows if row["gdp_per_capita_ex_housing_index"] is not None),
                default=None,
            ),
            "caveat": "ketjutetut volyymisarjat eivät ole tarkasti yhteenlaskettavia, ja BKT on "
                      "markkinahintaan mutta toimialan arvonlisäys perushintaan, joten ilman "
                      "asuntotuloa laskettu sarja on rajauslaskelma eikä virallinen suure",
            "sources": [
                {"label": "Tilastokeskus, kansantalouden tilinpito (BKT ja toimialat)",
                 "url": GDP_URL},
                {"label": "Tilastokeskus, keskiväkiluku", "url": "official_macro_reference_v1"},
                {"label": "Etla, havainto laskennallisen asuntotulon vaikutuksesta",
                 "url": "https://www.etla.fi/"},
            ],
        },
        "years": rows,
    }


def validate(payload: dict[str, Any]) -> None:
    rows = payload["years"]
    if not rows:
        raise ValueError("Sarjoja ei muodostunut")

    base = next(row for row in rows if row["year"] == BASE_YEAR)
    for key in ("gdp_volume_index", "gdp_per_capita_index", "gdp_per_capita_ex_housing_index"):
        if abs(base[key] - 100) > 1e-9:
            raise ValueError(f"Perusvuoden {key} ei ole 100")

    if [row["year"] for row in rows] != sorted(row["year"] for row in rows):
        raise ValueError("Vuodet eivät ole järjestyksessä")

    # Asuntotulon osuus on kasvanut, mika on koko rajauksen peruste. Jos nain
    # ei ole, sarja 3 ei enaa kerro sita mita sen on tarkoitus kertoa.
    shares = [row["housing_share_pct"] for row in rows if row["housing_share_pct"] is not None]
    if shares[-1] <= shares[0]:
        raise ValueError("Asuntotulon osuus ei ole kasvanut, rajauksen peruste ei pade")

    # Ilman asuntotuloa laskettu sarja jaa aina henkea kohden lasketun alle
    # perusvuoden jalkeen, koska asuminen on kasvanut muuta taloutta nopeammin.
    for row in rows:
        value = row["gdp_per_capita_ex_housing_index"]
        if value is not None and row["year"] > BASE_YEAR and value > row["gdp_per_capita_index"]:
            raise ValueError(f"Vuonna {row['year']} rajattu sarja ylittää henkeä kohden lasketun")


def render(payload: dict[str, Any], out_png: Path, dpi: int) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MultipleLocator

    rows = payload["years"]
    years = [row["year"] for row in rows]
    series = [
        ("gdp_volume_index", "Kokonais-BKT:n volyymi", PALETTE["gdp"], "o"),
        ("gdp_per_capita_index", "BKT asukasta kohden", PALETTE["per_capita"], "s"),
        ("gdp_per_capita_ex_housing_index",
         "BKT asukasta kohden ilman laskennallista asuntotuloa", PALETTE["ex_housing"], "^"),
    ]

    fig, ax = plt.subplots(figsize=(12.6, 7.4))
    fig.patch.set_facecolor(PALETTE["paper"])
    ax.set_facecolor(PALETTE["paper"])

    # Vaje: alue, jolla henkea kohden laskettu taso jaa vuoden 2008 alle.
    per_capita = [row["gdp_per_capita_index"] for row in rows]
    ax.fill_between(
        years, per_capita, 100, where=[value < 100 for value in per_capita],
        interpolate=True, color=PALETTE["shortfall"], alpha=0.10, linewidth=0,
        label=f"Vaje vuoden {BASE_YEAR} tasoon nähden",
    )

    ax.axhline(100, color=PALETTE["base"], linestyle="--", linewidth=1.6, zorder=2)
    ax.annotate(
        f"{BASE_YEAR} = 100", xy=(years[0], 100), xytext=(years[0] + 0.15, 100.9),
        color=PALETTE["base"], fontsize=10.5, fontweight="bold",
    )

    for key, label, color, marker in series:
        xs = [row["year"] for row in rows if row[key] is not None]
        ys = [row[key] for row in rows if row[key] is not None]
        ax.plot(xs, ys, color=color, linewidth=2.6, marker=marker, markersize=5.2,
                markerfacecolor=PALETTE["paper"], markeredgewidth=1.6, label=label, zorder=4)
        change = ys[-1] - 100
        suffix = "" if xs[-1] == END_YEAR else f" ({xs[-1]})"
        ax.annotate(
            f"{ys[-1]:.1f}  ({change:+.1f} %){suffix}".replace(".", ","),
            xy=(xs[-1], ys[-1]), xytext=(7, 0), textcoords="offset points",
            color=color, fontsize=11, fontweight="bold", va="center",
        )

    ax.set_xlim(BASE_YEAR - 0.4, END_YEAR + 3.4)
    ax.set_ylim(88, 107)
    ax.xaxis.set_major_locator(MultipleLocator(2))
    ax.yaxis.set_major_locator(MultipleLocator(2))
    ax.grid(True, color=PALETTE["grid"], linewidth=0.8, alpha=0.7)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(PALETTE["grid"])

    # Otsikko ja alaotsikko kuvan koordinaateissa, jotta ne eivat mene
    # paallekkain akselin otsikon kanssa.
    fig.text(0.062, 0.955, "Suomen elintaso ja talouskasvu 2008–2025",
             fontsize=20, fontweight="bold", color=PALETTE["ink"], va="top")
    fig.text(0.062, 0.905,
             "Indeksi, 2008 = 100. Talous on kasvanut, mutta asukasta kohden se ei ole "
             "palannut finanssikriisiä edeltäneelle tasolle.",
             fontsize=11.5, color=PALETTE["base"], va="top")
    ax.set_ylabel("Indeksi (2008 = 100)", fontsize=11.5, color=PALETTE["ink"])
    ax.tick_params(colors=PALETTE["base"], labelsize=10.5)

    ax.legend(loc="lower right", frameon=True, fontsize=10.5, framealpha=0.95,
              edgecolor=PALETTE["grid"], facecolor=PALETTE["paper"])

    meta = payload["meta"]
    last_ex = meta["ex_housing_last_year"]
    footnote = "\n".join([
        "Lähteet: Tilastokeskus, kansantalouden tilinpito (BKT:n volyymisarja, viitevuosi 2015; "
        "toimialan 68201–68202 arvonlisäys) ja keskiväkiluku.",
        f"Laskennallisen asuntotulon rajaus seuraa Etlan havaintoa; sarja päättyy vuoteen {last_ex}, "
        f"koska toimialatietoa vuodelle {END_YEAR} ei ole vielä julkaistu.",
        "Ketjutetut volyymisarjat eivät ole tarkasti yhteenlaskettavia, joten kolmas sarja on "
        "rajauslaskelma eikä virallinen kansantalouden tilinpidon suure.",
    ])
    fig.text(0.062, 0.028, footnote, fontsize=8.8, color=PALETTE["base"],
             va="bottom", linespacing=1.6)

    fig.subplots_adjust(left=0.062, right=0.985, top=0.855, bottom=0.175)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=dpi, facecolor=fig.get_facecolor())
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description="Piirrä elintason kehitys 2008-2025.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--out", type=Path, default=OUT_PNG)
    parser.add_argument("--dpi", type=int, default=300)
    args = parser.parse_args()

    gdp = fetch_gdp_volume()
    housing = fetch_housing_value_added()
    population = fetch_population(args.project, args.dataset)

    payload = build_series(gdp, housing, population)
    validate(payload)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    render(payload, args.out, args.dpi)

    last = payload["years"][-1]
    ex_last = next(
        row for row in reversed(payload["years"])
        if row["gdp_per_capita_ex_housing_index"] is not None
    )
    print(f"{args.out.relative_to(ROOT)} ({args.dpi} dpi)")
    print(f"  BKT:n volyymi {last['year']}          {last['gdp_volume_index']:.1f}")
    print(f"  BKT asukasta kohden {last['year']}    {last['gdp_per_capita_index']:.1f}")
    print(f"  ilman asuntotuloa {ex_last['year']}      "
          f"{ex_last['gdp_per_capita_ex_housing_index']:.1f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

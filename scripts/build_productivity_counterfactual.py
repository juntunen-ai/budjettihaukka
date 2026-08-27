#!/usr/bin/env python3
"""Piirtää toteutuneen ja vaihtoehtoisen tuottavuusuran eron tiedostoon
docs/figures/tuottavuuskuilu_1995_2025.png.

Vertailu-ura jatkaa vuosien 1995-2008 keskimääräistä kasvuvauhtia vuodesta
2008 eteenpäin. Vauhti on 2,34 prosenttia vuodessa, ja se on Suomen omaan
historiaan nähden varovainen valinta: 1975-1995 tuottavuus kasvoi 3,07 ja
koko jaksolla 1975-2008 keskimäärin 2,79 prosenttia vuodessa. Vertailu-ura
ei siis nojaa poikkeukselliseen ICT-nousuun.

Rinnalle piirretään vaatimattomampi ura, joka olettaa yhden prosentin
vuosikasvun. Sekin riittäisi selvästi toteutuneen yläpuolelle.

Euromääräinen ero lasketaan pitämällä tehdyt työtunnit toteutuneina ja
korvaamalla vain tuottavuus. Se on mekaaninen laskelma, ei ennuste siitä
mitä olisi tapahtunut: korkeampi tuottavuus olisi muuttanut myös työtunteja,
investointeja ja hintoja. Luku kertoo eron suuruusluokan, ei tarkkaa
menetystä.
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

OUT_PNG = ROOT / "docs" / "figures" / "tuottavuuskuilu_1995_2025.png"
OUT_JSON = ROOT / "data" / "reference" / "productivity_counterfactual_v1.json"

TREND_FROM, BASE_YEAR, END_YEAR = 1995, 2008, 2025
HISTORY_FROM = 1975
MODEST_RATE = 0.01
GDP_URL = "https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/ntp/15a9.px"
HOURS_URL = "https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/ntp/15ab.px"
VOLUME_CONTENT = "ntp-vv_hinta_2015"
MISSING = {".", "..", "...", "", None}

PALETTE = {
    "actual": "#1B6CA8", "trend": "#C94F1B", "modest": "#8A8476",
    "gap": "#C94F1B", "ink": "#171713", "base": "#5F5B50",
    "paper": "#FBFAF7", "grid": "#D9D5CC",
}


def _px(url: str, query: dict[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(
        url, data=json.dumps(query).encode("utf-8"), headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.load(response)


def _years() -> list[str]:
    return [str(year) for year in range(HISTORY_FROM, END_YEAR + 1)]


def fetch_gdp() -> dict[int, float]:
    payload = _px(GDP_URL, {
        "query": [
            {"code": "taloustoimi_1_20180101", "selection": {"filter": "item", "values": ["B1GMH"]}},
            {"code": "timeperiod_y", "selection": {"filter": "item", "values": _years()}},
            {"code": "contentscode", "selection": {"filter": "item", "values": [VOLUME_CONTENT]}},
        ],
        "response": {"format": "json"},
    })
    return {int(row["key"][-1]): float(row["values"][0]) * 1e6
            for row in payload["data"] if row["values"][0] not in MISSING}


def fetch_hours() -> dict[int, float]:
    payload = _px(HOURS_URL, {
        "query": [
            {"code": "taloustoimi_1_20180101", "selection": {"filter": "item", "values": ["E2"]}},
            {"code": "sektoriluokitus_7_20230101", "selection": {"filter": "item", "values": ["S1"]}},
            {"code": "toimiala_79_20180101", "selection": {"filter": "item", "values": ["SSS"]}},
            {"code": "timeperiod_y", "selection": {"filter": "item", "values": _years()}},
        ],
        "response": {"format": "json"},
    })
    return {int(row["key"][3]): float(row["values"][0]) * 1e6
            for row in payload["data"] if row["values"][0] not in MISSING}


def _cagr(series: dict[int, float], start: int, end: int) -> float:
    return (series[end] / series[start]) ** (1 / (end - start)) - 1


def build_payload(gdp: dict[int, float], hours: dict[int, float]) -> dict[str, Any]:
    productivity = {year: gdp[year] / hours[year] for year in sorted(set(gdp) & set(hours))}
    for year in (HISTORY_FROM, TREND_FROM, BASE_YEAR, END_YEAR):
        if year not in productivity:
            raise ValueError(f"Tuottavuussarjasta puuttuu vuosi {year}")

    trend_rate = _cagr(productivity, TREND_FROM, BASE_YEAR)
    base = productivity[BASE_YEAR]

    rows = []
    cumulative = 0.0
    for year in sorted(year for year in productivity if year >= TREND_FROM):
        actual = productivity[year]
        if year <= BASE_YEAR:
            trend = actual if year == BASE_YEAR else None
            modest = trend
            gap_eur = 0.0
        else:
            trend = base * (1 + trend_rate) ** (year - BASE_YEAR)
            modest = base * (1 + MODEST_RATE) ** (year - BASE_YEAR)
            gap_eur = (trend - actual) * hours[year]
            cumulative += gap_eur
        rows.append({
            "year": year,
            "productivity_index": 100 * actual / base,
            "trend_index": None if trend is None else 100 * trend / base,
            "modest_index": None if modest is None else 100 * modest / base,
            "gdp_actual_eur": gdp[year],
            "gdp_trend_eur": None if trend is None else trend * hours[year],
            "annual_gap_eur": gap_eur,
            "cumulative_gap_eur": cumulative,
        })

    return {
        "meta": {
            "dataset_id": "productivity_counterfactual_v1",
            "base_year": BASE_YEAR,
            "trend_from": TREND_FROM,
            "end_year": END_YEAR,
            "trend_rate_pct": 100 * trend_rate,
            "modest_rate_pct": 100 * MODEST_RATE,
            "actual_rate_pct": 100 * _cagr(productivity, BASE_YEAR, END_YEAR),
            "benchmark_rates_pct": {
                "1975-1995": 100 * _cagr(productivity, HISTORY_FROM, 1995),
                "1975-2008": 100 * _cagr(productivity, HISTORY_FROM, BASE_YEAR),
                "1995-2008": 100 * trend_rate,
            },
            "counterfactual_note": "työtunnit pidetään toteutuneina ja vain tuottavuus "
                                   "korvataan; mekaaninen laskelma eikä ennuste",
            "price_basis": "vuoden 2015 hinnat, ketjutettu volyymisarja",
            "sources": [
                {"label": "Tilastokeskus, kansantalouden tilinpito (BKT:n volyymi)", "url": GDP_URL},
                {"label": "Tilastokeskus, tehdyt työtunnit", "url": HOURS_URL},
            ],
        },
        "years": rows,
    }


def validate(payload: dict[str, Any]) -> None:
    rows = payload["years"]
    meta = payload["meta"]
    if not rows:
        raise ValueError("Vuosia ei muodostunut")

    base = next(row for row in rows if row["year"] == meta["base_year"])
    if abs(base["productivity_index"] - 100) > 1e-9:
        raise ValueError("Perusvuoden indeksi ei ole 100")
    if abs(base["trend_index"] - 100) > 1e-9:
        raise ValueError("Vertailu-ura ei lähde perusvuodesta")

    # Vertailu-ura on Suomen omaan historiaan nahden varovainen. Jos tama
    # ei pade, kuvan keskeinen perustelu kaatuu.
    benchmarks = meta["benchmark_rates_pct"]
    if meta["trend_rate_pct"] > benchmarks["1975-2008"]:
        raise ValueError("Vertailu-ura on nopeampi kuin pitkän jakson keskiarvo")

    # Kuilu kasvaa monotonisesti perusvuoden jalkeen eika kavenny.
    after = [row for row in rows if row["year"] > meta["base_year"]]
    for previous, current in zip(after, after[1:]):
        if current["cumulative_gap_eur"] < previous["cumulative_gap_eur"]:
            raise ValueError(f"Kumulatiivinen kuilu kaventuu vuonna {current['year']}")
        if current["annual_gap_eur"] <= 0:
            raise ValueError(f"Vuosikuilu ei ole positiivinen vuonna {current['year']}")

    if meta["actual_rate_pct"] >= meta["trend_rate_pct"]:
        raise ValueError("Toteutunut kasvu ei jää vertailu-uran alle")


def fi(value: float, digits: int = 1, sign: bool = False) -> str:
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

    fig, (ax1, ax2) = plt.subplots(
        1, 2, figsize=(15.4, 7.6), gridspec_kw={"width_ratios": [1.3, 1]}
    )
    fig.patch.set_facecolor(PALETTE["paper"])

    # Vasen paneeli: tuottavuusindeksi ja vaihtoehtoiset urat.
    ax1.set_facecolor(PALETTE["paper"])
    years = [row["year"] for row in rows]
    actual = [row["productivity_index"] for row in rows]
    trend_years = [row["year"] for row in rows if row["trend_index"] is not None]
    trend = [row["trend_index"] for row in rows if row["trend_index"] is not None]
    modest = [row["modest_index"] for row in rows if row["modest_index"] is not None]

    ax1.fill_between(trend_years, trend, [row["productivity_index"] for row in rows
                                          if row["trend_index"] is not None],
                     color=PALETTE["gap"], alpha=0.13, linewidth=0, label="Kuilu")
    ax1.plot(years, actual, color=PALETTE["actual"], linewidth=2.8, marker="o", markersize=3.6,
             markerfacecolor=PALETTE["paper"], markeredgewidth=1.2, label="Toteutunut tuottavuus")
    ax1.plot(trend_years, trend, color=PALETTE["trend"], linewidth=2.6, linestyle="--",
             label=f"Vuosien {meta['trend_from']}–{meta['base_year']} vauhti jatkuu "
                   f"({fi(meta['trend_rate_pct'], 2)} %/v)")
    ax1.plot(trend_years, modest, color=PALETTE["modest"], linewidth=2.0, linestyle=":",
             label=f"Vaatimaton ura ({fi(meta['modest_rate_pct'], 1)} %/v)")
    ax1.axvline(meta["base_year"], color=PALETTE["base"], linewidth=1.2, alpha=0.6)
    ax1.annotate(str(meta["base_year"]), xy=(meta["base_year"], 63), fontsize=10,
                 color=PALETTE["base"], ha="center", fontweight="bold")

    for value, color, dy in ((trend[-1], PALETTE["trend"], 0),
                             (modest[-1], PALETTE["modest"], 0),
                             (actual[-1], PALETTE["actual"], 0)):
        ax1.annotate(fi(value, 0), xy=(END_YEAR, value), xytext=(7, dy),
                     textcoords="offset points", color=color, fontsize=11.5,
                     fontweight="bold", va="center")

    ax1.set_xlim(TREND_FROM - 0.5, END_YEAR + 3.0)
    ax1.set_ylim(60, 160)
    ax1.xaxis.set_major_locator(MultipleLocator(5))
    ax1.yaxis.set_major_locator(MultipleLocator(20))
    ax1.grid(True, color=PALETTE["grid"], linewidth=0.8, alpha=0.7)
    ax1.set_axisbelow(True)
    for side in ("top", "right"):
        ax1.spines[side].set_visible(False)
    ax1.set_ylabel(f"Tuottavuus, BKT / työtunti ({meta['base_year']} = 100)",
                   fontsize=11, color=PALETTE["ink"])
    ax1.tick_params(colors=PALETTE["base"], labelsize=10)
    ax1.legend(loc="upper left", fontsize=9.4, frameon=True, framealpha=0.95,
               edgecolor=PALETTE["grid"], facecolor=PALETTE["paper"])
    ax1.set_title("Tuottavuuden ura", fontsize=13, fontweight="bold",
                  color=PALETTE["ink"], loc="left", pad=18)

    # Oikea paneeli: vuosittainen ero euroina ja kumulatiivinen summa.
    ax2.set_facecolor(PALETTE["paper"])
    gap_rows = [row for row in rows if row["year"] > meta["base_year"]]
    gap_years = [row["year"] for row in gap_rows]
    gap_values = [row["annual_gap_eur"] / 1e9 for row in gap_rows]
    ax2.bar(gap_years, gap_values, color=PALETTE["gap"], edgecolor=PALETTE["ink"],
            linewidth=0.8, width=0.72)
    ax2.annotate(f"{fi(gap_values[-1], 0)} mrd €", xy=(gap_years[-1], gap_values[-1]),
                 xytext=(0, 8), textcoords="offset points", ha="center",
                 fontsize=11.5, fontweight="bold", color=PALETTE["gap"])

    ax2.set_xlim(meta["base_year"] + 0.4, END_YEAR + 0.8)
    ax2.xaxis.set_major_locator(MultipleLocator(4))
    ax2.yaxis.set_major_locator(MultipleLocator(20))
    ax2.grid(True, axis="y", color=PALETTE["grid"], linewidth=0.8, alpha=0.7)
    ax2.set_axisbelow(True)
    for side in ("top", "right"):
        ax2.spines[side].set_visible(False)
    ax2.set_ylabel("Vuotuinen ero, mrd € (vuoden 2015 hinnoin)",
                   fontsize=11, color=PALETTE["ink"])
    ax2.tick_params(colors=PALETTE["base"], labelsize=10)
    ax2.set_title("Ero vuosittain", fontsize=13, fontweight="bold",
                  color=PALETTE["ink"], loc="left", pad=18)
    ax2.text(0.03, 0.93,
             f"Kumulatiivisesti {fi(last['cumulative_gap_eur'] / 1e9, 0)} mrd €\n"
             f"vuosina {meta['base_year'] + 1}–{END_YEAR}",
             transform=ax2.transAxes, fontsize=12, fontweight="bold",
             color=PALETTE["ink"], va="top")

    fig.text(0.036, 0.955, "Tuottavuuskuilu: mihin vanha kasvuvauhti olisi vienyt",
             fontsize=20, fontweight="bold", color=PALETTE["ink"], va="top")
    fig.text(0.036, 0.902,
             f"Tuottavuus kasvoi {fi(meta['actual_rate_pct'], 2)} prosenttia vuodessa "
             f"{meta['base_year']}–{END_YEAR}. Vuosien {meta['trend_from']}–{meta['base_year']} "
             f"vauhdilla taso olisi nyt {fi(next(r['trend_index'] for r in rows if r['year'] == END_YEAR), 0)} "
             f"eikä {fi(last['productivity_index'], 0)}.",
             fontsize=11.5, color=PALETTE["base"], va="top")

    benchmarks = meta["benchmark_rates_pct"]
    footnote = "\n".join([
        f"Vertailu-ura käyttää vuosien {meta['trend_from']}–{meta['base_year']} vauhtia "
        f"{fi(meta['trend_rate_pct'], 2)} %/v. Se on Suomen omaan historiaan nähden varovainen: "
        f"1975–1995 tuottavuus kasvoi {fi(benchmarks['1975-1995'], 2)} %/v ja "
        f"1975–2008 keskimäärin {fi(benchmarks['1975-2008'], 2)} %/v.",
        "Euromääräinen ero saadaan pitämällä tehdyt työtunnit toteutuneina ja korvaamalla vain "
        "tuottavuus. Se on mekaaninen laskelma, ei ennuste siitä mitä olisi tapahtunut:\n"
        "korkeampi tuottavuus olisi muuttanut myös työtunteja, investointeja ja hintoja. Luku "
        "kertoo eron suuruusluokan, ei tarkkaa menetystä.",
        "Lähteet: Tilastokeskus, kansantalouden tilinpito (BKT:n volyymisarja viitevuosi 2015 ja "
        "tehdyt työtunnit).",
    ])
    fig.text(0.036, 0.022, footnote, fontsize=8.8, color=PALETTE["base"],
             va="bottom", linespacing=1.6)

    fig.subplots_adjust(left=0.062, right=0.975, top=0.815, bottom=0.235, wspace=0.24)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=dpi, facecolor=fig.get_facecolor())
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description="Piirrä tuottavuuskuilu.")
    parser.add_argument("--out", type=Path, default=OUT_PNG)
    parser.add_argument("--dpi", type=int, default=300)
    args = parser.parse_args()

    payload = build_payload(fetch_gdp(), fetch_hours())
    validate(payload)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    render(payload, args.out, args.dpi)

    meta, last = payload["meta"], payload["years"][-1]
    trend_last = next(row["trend_index"] for row in payload["years"] if row["year"] == END_YEAR)
    print(f"{args.out.relative_to(ROOT)} ({args.dpi} dpi)")
    print(f"  vertailuvauhti {meta['trend_rate_pct']:.2f} %/v, toteutunut {meta['actual_rate_pct']:.2f} %/v")
    print(f"  tuottavuus {END_YEAR}: toteutunut {last['productivity_index']:.0f}, "
          f"vertailu-ura {trend_last:.0f}")
    print(f"  ero {END_YEAR}: {last['annual_gap_eur'] / 1e9:.0f} mrd, "
          f"kumulatiivisesti {last['cumulative_gap_eur'] / 1e9:.0f} mrd")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Piirtää Liberaalipuolueen vaihtoehtobudjetin analyysin kuvat
hakemistoon docs/figures/.

Kolme kuvaa:

1. rakenne   - mistä sopeutus koostuu ja mihin se käytetään
2. kysynta   - menoleikkausten kysyntävaikutus menolajeittain
3. polut     - BKT-vaikutus ja velkasuhde eri vaiheistuksilla

Luvut luetaan analyysin snapshotista, jotta kuvat ja raportti eivät voi
erota toisistaan.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ANALYSIS = ROOT / "data" / "reference" / "liberaali_vaihtoehtobudjetti" / "analyysi_v1.json"
FIG_DIR = ROOT / "docs" / "figures"

PALETTE = {
    "cut": "#C94F1B", "gain": "#1F7A3D", "neutral": "#8A8476",
    "deficit": "#1B6CA8", "tax": "#8E3B8F",
    "ink": "#171713", "base": "#5F5B50", "paper": "#FBFAF7", "grid": "#D9D5CC",
}
SCENARIO_COLORS = {
    "kerralla": "#C94F1B",
    "kaksi_vuotta": "#E4761B",
    "etupainotteinen_vaalikausi": "#1B6CA8",
    "vaalikausi": "#1F7A3D",
}


def fi(value: float, digits: int = 1, sign: bool = False) -> str:
    text = f"{value:+.{digits}f}" if sign else f"{value:.{digits}f}"
    return text.replace(".", ",")


def _frame(fig, title: str, subtitle: str, footnote: str) -> None:
    fig.patch.set_facecolor(PALETTE["paper"])
    fig.text(0.038, 0.955, title, fontsize=19, fontweight="bold",
             color=PALETTE["ink"], va="top")
    fig.text(0.038, 0.900, subtitle, fontsize=11.5, color=PALETTE["base"], va="top")
    fig.text(0.038, 0.022, footnote, fontsize=8.8, color=PALETTE["base"],
             va="bottom", linespacing=1.6)


def _clean(ax) -> None:
    ax.set_facecolor(PALETTE["paper"])
    ax.grid(True, color=PALETTE["grid"], linewidth=0.8, alpha=0.7)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.tick_params(colors=PALETTE["base"], labelsize=10)


def chart_structure(analysis: dict[str, Any], dpi: int) -> Path:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    head = analysis["headline"]
    exp = analysis["expenditure_by_class"]
    rev = analysis["revenue_by_class"]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15.4, 7.6),
                                   gridspec_kw={"width_ratios": [1.25, 1]})

    # Vasen: menoleikkaukset menolajeittain.
    rows = sorted(
        [(entry["label"], entry["change_eur"] / 1e9) for entry in exp.values()
         if abs(entry["change_eur"]) > 5e7],
        key=lambda item: item[1])
    _clean(ax1)
    positions = range(len(rows))
    ax1.barh(list(positions), [value for _label, value in rows],
             color=[PALETTE["cut"] if value < 0 else PALETTE["gain"] for _l, value in rows],
             edgecolor=PALETTE["ink"], linewidth=1.1, height=0.62)
    for pos, (_label, value) in zip(positions, rows):
        # Pitkan palkin lukema palkin sisaan, muuten se tormaisi vasemmalla
        # olevaan nimikkeeseen.
        inside = abs(value) > 3.0
        offset = (0.14 if value < 0 else -0.14) if inside else (-0.14 if value < 0 else 0.14)
        align = ("left" if value < 0 else "right") if inside else ("right" if value < 0 else "left")
        ax1.text(value + offset, pos, f"{fi(value, 2, sign=True)}", va="center", ha=align,
                 fontsize=11, fontweight="bold",
                 color=PALETTE["paper"] if inside else PALETTE["ink"])
    ax1.set_yticks(list(positions))
    ax1.set_yticklabels([label for label, _v in rows], fontsize=10)
    ax1.tick_params(axis="y", length=0)
    ax1.axvline(0, color=PALETTE["ink"], linewidth=1.4)
    ax1.set_xlim(-6.2, 2.6)
    ax1.set_xlabel("Miljardia euroa", fontsize=11, color=PALETTE["ink"])
    ax1.set_title("Menomuutokset menolajeittain", fontsize=13, fontweight="bold",
                  color=PALETTE["ink"], loc="left", pad=20)

    # Oikea: mihin sopeutus kaytetaan.
    _clean(ax2)
    uses = [
        ("Nettolainanoton\nlopettaminen", -rev["lainanotto"]["change_eur"] / 1e9, PALETTE["deficit"]),
        ("Verotuksen\nnettokevennys", -head["tax_change_eur"] / 1e9, PALETTE["tax"]),
    ]
    labels = [label for label, _v, _c in uses]
    values = [value for _l, value, _c in uses]
    colors = [color for _l, _v, color in uses]
    bars = ax2.bar(labels, values, color=colors, edgecolor=PALETTE["ink"],
                   linewidth=1.2, width=0.5)
    for bar, value in zip(bars, values):
        ax2.text(bar.get_x() + bar.get_width() / 2, value + 0.15, f"{fi(value, 2)} mrd €",
                 ha="center", fontsize=13, fontweight="bold", color=PALETTE["ink"])
    ax2.set_ylim(0, max(values) * 1.30)
    ax2.set_ylabel("Miljardia euroa", fontsize=11, color=PALETTE["ink"])
    ax2.set_title("Mihin menoleikkaukset käytetään", fontsize=13, fontweight="bold",
                  color=PALETTE["ink"], loc="left", pad=20)
    ax2.text(0.5, 0.86,
             f"Menoleikkaukset yhteensä {fi(abs(head['expenditure_change_eur']) / 1e9, 2)} mrd €\n"
             f"= {fi(abs(head['total_change_pct_of_gdp']), 2)} % BKT:sta",
             transform=ax2.transAxes, ha="center", fontsize=12, fontweight="bold",
             color=PALETTE["ink"])

    _frame(
        fig,
        "Vaihtoehtobudjetin rakenne: mistä sopeutus tulee ja mihin se menee",
        f"Talousarvioesitys 2026 pienenee {fi(abs(head['total_change_eur']) / 1e9, 2)} miljardia "
        f"eli {fi(abs(head['total_change_pct_of_gdp']), 2)} prosenttia bruttokansantuotteesta. "
        f"{head['item_count']} momenttia muuttuu.",
        "Menolajiluokitus on valtion talousarvion oma: momenttinumeron viimeinen osa kertoo, "
        "onko kyse toimintamenoista, valtionavusta vai siirrosta.\n"
        "Lähde: Liberaalipuolueen vaihtoehtobudjetti 2026 ja valtion talousarvioesitys 2026. "
        "Vertailuluvut Suomen taloudesta Budjettihaukan aineistosta.",
    )
    fig.subplots_adjust(left=0.245, right=0.975, top=0.805, bottom=0.155, wspace=0.55)
    out = FIG_DIR / "liberaali_vb_rakenne.png"
    fig.savefig(out, dpi=dpi, facecolor=fig.get_facecolor())
    plt.close(fig)
    return out


def chart_demand(analysis: dict[str, Any], dpi: int) -> Path:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    exp = analysis["expenditure_by_class"]
    rows = sorted(
        [(entry["label"], entry["change_eur"] / 1e9,
          entry["change_eur"] * entry["multiplier"] / 1e9,
          entry["change_eur"] * entry["multiplier_low"] / 1e9,
          entry["change_eur"] * entry["multiplier_high"] / 1e9,
          entry["multiplier"])
         for entry in exp.values() if abs(entry["change_eur"]) > 5e7],
        key=lambda item: item[2])

    fig, ax = plt.subplots(figsize=(14.2, 7.6))
    _clean(ax)
    positions = list(range(len(rows)))
    ax.barh(positions, [row[1] for row in rows], color="#D9D5CC",
            edgecolor=PALETTE["neutral"], linewidth=1.0, height=0.62,
            label="Muutos euroina")
    ax.barh(positions, [row[2] for row in rows], color=PALETTE["cut"],
            edgecolor=PALETTE["ink"], linewidth=1.1, height=0.34,
            label="Kysyntävaikutus kertoimella")
    for pos, row in zip(positions, rows):
        ax.plot([row[3], row[4]], [pos, pos], color=PALETTE["ink"], linewidth=1.4)
        ax.plot([row[3], row[4]], [pos, pos], marker="|", color=PALETTE["ink"],
                linestyle="none", markersize=8)
        ax.text(row[2] - 0.10, pos, f"{fi(row[2], 2)}", va="center", ha="right",
                fontsize=10.5, fontweight="bold", color=PALETTE["ink"])
    ax.set_yticks(positions)
    ax.set_yticklabels([f"{row[0]}\nkerroin {fi(row[5], 2)}" for row in rows], fontsize=9.6)
    ax.tick_params(axis="y", length=0)
    ax.axvline(0, color=PALETTE["ink"], linewidth=1.4)
    ax.set_xlabel("Miljardia euroa", fontsize=11, color=PALETTE["ink"])
    ax.legend(loc="lower left", fontsize=10, frameon=True, framealpha=0.95,
              edgecolor=PALETTE["grid"], facecolor=PALETTE["paper"])

    impulse = analysis["fiscal_impulse_pct_of_gdp"]
    _frame(
        fig,
        "Kaikki leikatut eurot eivät vaikuta kysyntään yhtä paljon",
        f"Kokonaisvaikutus kotimaiseen kysyntään on {fi(impulse['mid'], 2)} prosenttia "
        f"bruttokansantuotteesta, haarukka {fi(impulse['high'], 2)}–{fi(impulse['low'], 2)}. "
        f"Vaakaviiva näyttää kertoimen epävarmuuden.",
        "Kertoimet ovat kirjallisuuden haarukoita pienelle avotaloudelle rahaliitossa, eivät "
        "Suomelle estimoituja parametreja. Ne ovat skriptissä näkyvissä ja muutettavissa.\n"
        "Ulkomaille menevä euro vaikuttaa Suomen kysyntään vähän, palvelujen rahoitus paljon. "
        "Siksi sama leikattu summa voi tarkoittaa hyvin eri kokoista vaikutusta.",
    )
    fig.subplots_adjust(left=0.235, right=0.975, top=0.800, bottom=0.165)
    out = FIG_DIR / "liberaali_vb_kysynta.png"
    fig.savefig(out, dpi=dpi, facecolor=fig.get_facecolor())
    plt.close(fig)
    return out


def chart_paths(analysis: dict[str, Any], dpi: int) -> Path:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MultipleLocator

    scenarios = analysis["scenarios"]
    debt = analysis["debt_paths"]
    years = analysis["meta"]["projection_years"]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15.4, 7.6))

    _clean(ax1)
    for key, scenario in scenarios.items():
        path = scenario["paths"]["mid"]["gdp_level_effect_pct"]
        xs = list(range(1, len(path) + 1))
        ax1.plot(xs, path, color=SCENARIO_COLORS[key], linewidth=2.6, marker="o",
                 markersize=5, markerfacecolor=PALETTE["paper"], markeredgewidth=1.5,
                 label=scenario["label"])
    ax1.axhline(0, color=PALETTE["ink"], linewidth=1.3)
    final = scenarios["kerralla"]["paths"]["mid"]["final_level_pct"]
    ax1.axhline(final, color=PALETTE["base"], linestyle="--", linewidth=1.4)
    ax1.annotate(f"Lopullinen taso {fi(final, 2)} %", xy=(5.3, final), xytext=(0, 8),
                 textcoords="offset points", ha="right", fontsize=10.5,
                 color=PALETTE["base"], fontweight="bold")
    ax1.set_xlim(0.7, 5.4)
    ax1.set_ylim(final * 1.22, 0.28)
    ax1.xaxis.set_major_locator(MultipleLocator(1))
    ax1.set_xlabel("Vuosi sopeutuksen alusta", fontsize=11, color=PALETTE["ink"])
    ax1.set_ylabel("BKT:n tasovaikutus, %", fontsize=11, color=PALETTE["ink"])
    # Selite ylanurkkaan, koska kayrat laskevat ja peittyisivat alhaalla.
    ax1.legend(loc="upper right", fontsize=9.6, frameon=True, framealpha=0.95,
               edgecolor=PALETTE["grid"], facecolor=PALETTE["paper"])
    ax1.set_title("Vaiheistus ei muuta määränpäätä, vain matkaa",
                  fontsize=13, fontweight="bold", color=PALETTE["ink"], loc="left", pad=20)

    _clean(ax2)
    xs = list(range(1, years + 1))
    ax2.plot(xs, debt["kerralla"]["baseline_pct"], color=PALETTE["ink"], linewidth=2.8,
             linestyle=(0, (5, 2)), label="Perusura, lainanotto jatkuu")
    for key in ("kerralla", "vaalikausi"):
        ax2.plot(xs, debt[key]["alternative_pct"], color=SCENARIO_COLORS[key],
                 linewidth=2.6, marker="o", markersize=4.4,
                 markerfacecolor=PALETTE["paper"], markeredgewidth=1.4,
                 label=f"Vaihtoehto, {scenarios[key]['label'].lower()}")
    base_last = debt["kerralla"]["baseline_pct"][-1]
    alt_last = debt["kerralla"]["alternative_pct"][-1]
    ax2.annotate(f"{fi(base_last, 1)} %", xy=(years, base_last), xytext=(8, 0),
                 textcoords="offset points", fontsize=11.5, fontweight="bold",
                 color=PALETTE["ink"], va="center")
    ax2.annotate(f"{fi(alt_last, 1)} %", xy=(years, alt_last), xytext=(8, 0),
                 textcoords="offset points", fontsize=11.5, fontweight="bold",
                 color=SCENARIO_COLORS["kerralla"], va="center")
    ax2.set_xlim(0.7, years + 1.9)
    ax2.xaxis.set_major_locator(MultipleLocator(2))
    ax2.set_xlabel("Vuosi", fontsize=11, color=PALETTE["ink"])
    ax2.set_ylabel("Valtionvelka suhteessa BKT:hen, %", fontsize=11, color=PALETTE["ink"])
    ax2.legend(loc="upper left", fontsize=9.6, frameon=True, framealpha=0.95,
               edgecolor=PALETTE["grid"], facecolor=PALETTE["paper"])
    ax2.set_title("Velkasuhde kymmenen vuoden kuluttua", fontsize=13, fontweight="bold",
                  color=PALETTE["ink"], loc="left", pad=20)

    fast = scenarios["kerralla"]["paths"]["mid"]["worst_year_growth_pct"]
    slow = scenarios["vaalikausi"]["paths"]["mid"]["worst_year_growth_pct"]
    _frame(
        fig,
        "Vaiheistus ratkaisee iskun kovuuden, ei lopputulosta",
        f"Kerralla tehtynä pahin vuosi on {fi(fast, 2)} prosenttia, neljään vuoteen jaettuna "
        f"{fi(slow, 2)}. Lopullinen tasovaikutus on molemmissa sama.",
        "Velkapolku olettaa nimellisen BKT:n kasvavan 2,0 prosenttia vuodessa ja perusuralla "
        "nettolainanoton jatkuvan talousarvioesityksen tasolla 8,66 mrd euroa vuodessa.\n"
        "Vaihtoehdossa BKT jää kysyntävaikutuksen verran matalammaksi, mikä nostaa "
        "velkasuhdetta, mutta lainanoton loppuminen laskee sitä enemmän. Laskelma ei sisällä "
        "korkokannan muutoksia eikä käyttäytymisvaikutuksia.",
    )
    fig.subplots_adjust(left=0.062, right=0.965, top=0.800, bottom=0.185, wspace=0.26)
    out = FIG_DIR / "liberaali_vb_polut.png"
    fig.savefig(out, dpi=dpi, facecolor=fig.get_facecolor())
    plt.close(fig)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Piirrä vaihtoehtobudjetin kuvat.")
    parser.add_argument("--dpi", type=int, default=300)
    args = parser.parse_args()

    analysis = json.loads(ANALYSIS.read_text(encoding="utf-8"))
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    for builder in (chart_structure, chart_demand, chart_paths):
        out = builder(analysis, args.dpi)
        print(f"  {out.relative_to(ROOT)}  ({out.stat().st_size // 1024} kt)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

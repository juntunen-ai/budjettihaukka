#!/usr/bin/env python3
"""Jäsentää Liberaalipuolueen vaihtoehtobudjetin 2026 ja simuloi sen
kansantaloudelliset vaikutukset eri vaiheistuksilla.

Tuloste: data/reference/liberaali_vaihtoehtobudjetti/analyysi_v1.json

Tämä ei ole ennuste vaan simulaatio. Se kertoo mitä annetuista kertoimista
ja vaiheistuksesta seuraa. Kertoimet ovat kirjallisuuden haarukoita pienelle
avotaloudelle rahaliitossa, eivät Suomelle estimoituja parametreja, ja ne
ovat tiedostossa näkyvissä ja muutettavissa.

Menot luokitellaan valtion talousarvion omalla menolajiluokituksella, eli
momenttinumeron viimeisellä osalla. Se on virallinen luokitus eikä oma
tulkinta:

    01-28  toimintamenot
    29     arvonlisäveromenot, tekninen erä
    30-39  valtionavut kunnille ja hyvinvointialueille
    40-49  valtionavut elinkeinoelämälle
    50-59  valtionavut kotitalouksille ja yhteisöille
    60-69  siirrot rahastoihin, EU:lle ja ulkomaille
    70-79  reaalisijoitukset
    80-89  lainat ja finanssisijoitukset
    90-99  muut menot, muun muassa korot

Kertoimen suuruus riippuu siitä, kuinka suuri osa eurosta jää kotimaiseen
kysyntään. Ulkomaille menevä kehitysyhteistyö ja EU-maksu vaikuttavat
Suomen BKT:hen vähän, kotitalouksien etuudet ja palvelujen rahoitus paljon.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DATA_DIR = ROOT / "data" / "reference" / "liberaali_vaihtoehtobudjetti"
RAW_CSV = DATA_DIR / "vaihtoehtobudjetti_2026_raw.csv"
OUT_JSON = DATA_DIR / "analyysi_v1.json"

SOURCE_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "11UDqKOwld7tLxzuD-KwHN2-4rxqtl6JsykV9igvxSQo/edit?gid=1938612580"
)

# Vertailuluvut Suomen taloudesta, Budjettihaukan aineistosta vuodelta 2025.
GDP_EUR = 281_674_000_000.0
EMPLOYED = 2_732_100.0
HOURS = 4_224_000_000.0
WAGES_EUR = 114_600_000_000.0
DEBT_EUR = 209_131_000_000.0

# Menolajiryhmät ja niiden kertoimet. Keskiarvo ja haarukka.
EXPENDITURE_CLASSES = {
    "toimintamenot": {
        "range": (1, 28), "label": "Valtion toimintamenot",
        "multiplier": 0.70, "low": 0.45, "high": 1.00,
    },
    "tekninen": {
        "range": (29, 29), "label": "Arvonlisäveromenot, tekninen",
        "multiplier": 0.0, "low": 0.0, "high": 0.0,
    },
    "kunnat_ja_hyvinvointialueet": {
        "range": (30, 39), "label": "Valtionavut kunnille ja hyvinvointialueille",
        "multiplier": 0.80, "low": 0.55, "high": 1.10,
    },
    "elinkeinoelama": {
        "range": (40, 49), "label": "Valtionavut elinkeinoelämälle",
        "multiplier": 0.35, "low": 0.15, "high": 0.60,
    },
    "kotitaloudet": {
        "range": (50, 59), "label": "Valtionavut kotitalouksille ja yhteisöille",
        "multiplier": 0.65, "low": 0.40, "high": 0.95,
    },
    "rahastot_eu_ulkomaat": {
        "range": (60, 69), "label": "Siirrot rahastoihin, EU:lle ja ulkomaille",
        "multiplier": 0.20, "low": 0.05, "high": 0.40,
    },
    "investoinnit": {
        "range": (70, 79), "label": "Reaalisijoitukset",
        "multiplier": 0.90, "low": 0.60, "high": 1.30,
    },
    "lainat": {
        "range": (80, 89), "label": "Lainat ja finanssisijoitukset",
        "multiplier": 0.20, "low": 0.05, "high": 0.40,
    },
    "muut": {
        "range": (90, 99), "label": "Muut menot, muun muassa korot",
        "multiplier": 0.15, "low": 0.05, "high": 0.30,
    },
    # Puolueen omat uudet avaukset on merkitty momenttinumerolla, joka
    # sisaltaa tunnisteen "lib". Ne eivat ole valtion talousarvion
    # menolajeja lainkaan, joten ne kasitellaan omana ryhmanaan. Ilman
    # tata ne putoaisivat oletusluokkaan ja nayttaisivat korkomenoilta.
    "uudet_avaukset": {
        "range": None, "label": "Uudet avaukset",
        "multiplier": 0.60, "low": 0.35, "high": 0.90,
    },
}

# Tuloerien kertoimet. Merkki: veron kiristys vähentää kysyntää, joten
# kertoimen etumerkki käännetään laskennassa.
REVENUE_CLASSES = {
    "lainanotto": {
        "label": "Nettolainanotto", "multiplier": 0.0, "low": 0.0, "high": 0.0,
        "keywords": ("nettolainanotto", "velanhallinta"),
    },
    "tyon_verotus": {
        "label": "Työn ja pääomatulon verotus", "multiplier": 0.40, "low": 0.20, "high": 0.65,
        "keywords": ("ansio- ja pääomatuloverot", "yhteisövero"),
    },
    "kulutusverot": {
        "label": "Kulutusverot", "multiplier": 0.50, "low": 0.30, "high": 0.75,
        "keywords": ("arvonlisävero", "energiaverot", "autovero", "ajoneuvovero",
                     "tupakkavero", "alkoholijuomavero", "arpajaisvero", "apteekkivero",
                     "kannabisvero", "virvoitusjuomavero", "jätevero"),
    },
    "varallisuusverot": {
        "label": "Varallisuus- ja varainsiirtoverot", "multiplier": 0.20, "low": 0.05, "high": 0.40,
        "keywords": ("perintö- ja lahjavero", "varainsiirtovero"),
    },
    "muut_tulot": {
        "label": "Muut tulot", "multiplier": 0.10, "low": 0.0, "high": 0.25,
        "keywords": (),
    },
}

# Vaiheistusvaihtoehdot: kuinka suuri osa sopeutuksesta toteutuu kunakin
# vuonna. Osuuksien on summauduttava yhteen.
PHASINGS = {
    "kerralla": {
        "label": "Kerralla vuonna 1",
        "weights": [1.0],
    },
    "kaksi_vuotta": {
        "label": "Kahdessa vuodessa",
        "weights": [0.5, 0.5],
    },
    "vaalikausi": {
        "label": "Vaalikaudessa, neljä vuotta",
        "weights": [0.25, 0.25, 0.25, 0.25],
    },
    "etupainotteinen_vaalikausi": {
        "label": "Vaalikausi, etupainotteinen",
        "weights": [0.40, 0.30, 0.20, 0.10],
    },
}

# Velkapolun oletukset. Nimellinen BKT:n kasvu on viime vuosien tasolla,
# ja perusuralla nettolainanotto jatkuu talousarvioesityksen tasolla.
NOMINAL_GDP_GROWTH = 0.020
BASELINE_BORROWING_EUR = 8_659_000_000.0
PROJECTION_YEARS = 10

# Kerroinvaikutus ei katoa heti. Osuus, joka vaikuttaa samana vuonna ja
# seuraavana. Yksinkertainen kahden vuoden jakauma.
IMPACT_PROFILE = (0.7, 0.3)


def parse_number(text: str | None) -> float | None:
    if not text:
        return None
    cleaned = text.replace("−", "-").replace(" ", "").replace(" ", "")
    cleaned = re.sub(r"[^0-9,.\-]", "", cleaned).replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def expenditure_class(moment: str) -> str:
    text = (moment or "").lower()
    if "lib" in text:
        return "uudet_avaukset"
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)", text)
    if not match:
        return "uudet_avaukset"
    code = int(match.group(3))
    for name, spec in EXPENDITURE_CLASSES.items():
        if spec["range"] is None:
            continue
        low, high = spec["range"]
        if low <= code <= high:
            return name
    return "muut"


def revenue_class(name: str) -> str:
    lowered = (name or "").lower()
    for key, spec in REVENUE_CLASSES.items():
        if any(word in lowered for word in spec["keywords"]):
            return key
    return "muut_tulot"


def load_items() -> tuple[list[dict[str, Any]], dict[str, float]]:
    with RAW_CSV.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    totals = {}
    for row in rows:
        if row["momenttitaso"] == "0":
            totals[row["tulo/meno"]] = {
                "proposal": parse_number(row["numero"]),
                "alternative": parse_number(row["Lib budjetti"]),
            }

    items = []
    for row in rows:
        if row["momenttitaso"] != "3":
            continue
        change = parse_number(row["Leikattavaa momentista"])
        if not change:
            continue
        side = row["tulo/meno"]
        name = row["nimi"] or row["3.momentti"] or ""
        items.append({
            "side": side,
            "moment": (row["momenttitaso"] and row["momenttinumerot"] or "").strip(),
            "name": name.strip(),
            "proposal_eur": parse_number(row["numero"]),
            "alternative_eur": parse_number(row["Lib budjetti"]),
            "change_eur": change,
            "priority": row["Liikennevalo"] or "",
            "class": expenditure_class(row["momenttinumerot"]) if side == "meno"
                     else revenue_class(name),
        })
    return items, totals


def build_analysis(items: list[dict[str, Any]], totals: dict[str, Any]) -> dict[str, Any]:
    expenditure = [item for item in items if item["side"] == "meno"]
    revenue = [item for item in items if item["side"] == "tulo"]

    exp_by_class: dict[str, dict[str, Any]] = {}
    for name, spec in EXPENDITURE_CLASSES.items():
        selected = [item for item in expenditure if item["class"] == name]
        exp_by_class[name] = {
            "label": spec["label"],
            "change_eur": sum(item["change_eur"] for item in selected),
            "count": len(selected),
            "multiplier": spec["multiplier"],
            "multiplier_low": spec["low"],
            "multiplier_high": spec["high"],
        }

    rev_by_class: dict[str, dict[str, Any]] = {}
    for name, spec in REVENUE_CLASSES.items():
        selected = [item for item in revenue if item["class"] == name]
        rev_by_class[name] = {
            "label": spec["label"],
            "change_eur": sum(item["change_eur"] for item in selected),
            "count": len(selected),
            "multiplier": spec["multiplier"],
            "multiplier_low": spec["low"],
            "multiplier_high": spec["high"],
        }

    # Kysyntavaikutus: menoleikkaus vahentaa kysyntaa kertoimella. Tulopuolella
    # veron kevennys lisaa kysyntaa, joten muutoksen merkki kaannetaan.
    def demand_effect(bound: str) -> float:
        key = {"low": "multiplier_low", "mid": "multiplier", "high": "multiplier_high"}[bound]
        total = sum(entry["change_eur"] * entry[key] for entry in exp_by_class.values())
        for name, entry in rev_by_class.items():
            if name == "lainanotto":
                continue
            total += -entry["change_eur"] * entry[key]
        return total

    impulse = {bound: demand_effect(bound) for bound in ("low", "mid", "high")}

    borrowing = rev_by_class["lainanotto"]["change_eur"]
    tax_change = sum(entry["change_eur"] for name, entry in rev_by_class.items()
                     if name != "lainanotto")
    expenditure_change = sum(entry["change_eur"] for entry in exp_by_class.values())

    # Tasovaikutus ja kasvuvaikutus ovat eri asioita. Sopeutuksen lopullinen
    # tasovaikutus BKT:hen on sama riippumatta vaiheistuksesta, koska kaikki
    # vaihtoehdot toteuttavat saman kokonaisuuden. Vaiheistus muuttaa vain
    # sita, kuinka kova isku osuu yhteen vuoteen.
    #
    # Vuonna i tehdyn sopeutuksen tasovaikutus tuntuu IMPACT_PROFILE[0]
    # verran samana vuonna ja taysimaaraisesti siita eteenpain.
    scenarios = {}
    for key, spec in PHASINGS.items():
        weights = spec["weights"]
        years = len(weights) + 1
        paths = {}
        for bound in ("low", "mid", "high"):
            level = []
            for year in range(years):
                effect = 0.0
                for index, weight in enumerate(weights):
                    if index > year:
                        continue
                    share = IMPACT_PROFILE[0] if index == year else 1.0
                    effect += weight * share * impulse[bound]
                level.append(effect)
            growth = [level[0]] + [level[i] - level[i - 1] for i in range(1, years)]
            paths[bound] = {
                "gdp_level_effect_eur": level,
                "gdp_level_effect_pct": [100 * value / GDP_EUR for value in level],
                "gdp_growth_effect_pct": [100 * value / GDP_EUR for value in growth],
                "final_level_pct": 100 * level[-1] / GDP_EUR,
                "worst_year_growth_pct": min(100 * value / GDP_EUR for value in growth),
            }
        scenarios[key] = {"label": spec["label"], "weights": weights, "paths": paths}

    # Tyollisyysvaikutus karkeasti: BKT:n muutos jaettuna tyon tuottavuudella
    # henkea kohden. Tama olettaa ettei tuottavuus jousta, mika liioittelee
    # vaikutusta lyhyella aikavalilla.
    gdp_per_employed = GDP_EUR / EMPLOYED

    # Velkapolku: perusuralla lainanotto jatkuu, vaihtoehdossa se loppuu
    # mutta BKT jaa kysyntavaikutuksen verran matalammaksi. Kumpikin
    # vaikuttaa velkasuhteeseen, ja ne vetavat eri suuntiin.
    debt_paths = {}
    for key, scenario in scenarios.items():
        levels = scenario["paths"]["mid"]["gdp_level_effect_pct"]
        base_debt = alt_debt = DEBT_EUR
        base_gdp = alt_gdp = GDP_EUR
        rows_base, rows_alt = [], []
        for year in range(PROJECTION_YEARS):
            base_gdp *= 1 + NOMINAL_GDP_GROWTH
            alt_gdp = base_gdp * (1 + (levels[min(year, len(levels) - 1)] / 100))
            base_debt += BASELINE_BORROWING_EUR
            weight_done = sum(scenario["weights"][: year + 1])
            alt_debt += BASELINE_BORROWING_EUR * (1 - min(weight_done, 1.0))
            rows_base.append(100 * base_debt / base_gdp)
            rows_alt.append(100 * alt_debt / alt_gdp)
        debt_paths[key] = {
            "baseline_pct": rows_base,
            "alternative_pct": rows_alt,
            "difference_pp": [a - b for a, b in zip(rows_alt, rows_base)],
        }

    return {
        "meta": {
            "dataset_id": "liberaali_vaihtoehtobudjetti_analyysi_v1",
            "kind": "simulaatio annetuilla kertoimilla, ei ennuste",
            "source_url": SOURCE_URL,
            "budget_year": 2026,
            "baseline": {
                "gdp_eur": GDP_EUR, "employed": EMPLOYED, "hours": HOURS,
                "wages_eur": WAGES_EUR, "central_government_debt_eur": DEBT_EUR,
            },
            "impact_profile": list(IMPACT_PROFILE),
            "nominal_gdp_growth": NOMINAL_GDP_GROWTH,
            "baseline_borrowing_eur": BASELINE_BORROWING_EUR,
            "projection_years": PROJECTION_YEARS,
            "gdp_per_employed_eur": gdp_per_employed,
        },
        "headline": {
            "proposal_total_eur": totals["meno"]["proposal"],
            "alternative_total_eur": totals["meno"]["alternative"],
            "total_change_eur": totals["meno"]["alternative"] - totals["meno"]["proposal"],
            "total_change_pct_of_gdp": 100 * (
                totals["meno"]["alternative"] - totals["meno"]["proposal"]) / GDP_EUR,
            "expenditure_change_eur": expenditure_change,
            "tax_change_eur": tax_change,
            "borrowing_change_eur": borrowing,
            "item_count": len(items),
        },
        "expenditure_by_class": exp_by_class,
        "revenue_by_class": rev_by_class,
        "fiscal_impulse_eur": impulse,
        "fiscal_impulse_pct_of_gdp": {k: 100 * v / GDP_EUR for k, v in impulse.items()},
        "scenarios": scenarios,
        "debt_paths": debt_paths,
        "largest_cuts": sorted(
            [item for item in expenditure if item["change_eur"] < 0],
            key=lambda item: item["change_eur"])[:15],
        "largest_revenue_changes": sorted(
            revenue, key=lambda item: abs(item["change_eur"]), reverse=True)[:15],
    }


def validate(analysis: dict[str, Any]) -> None:
    head = analysis["headline"]

    # Menojen ja tulojen muutosten on vastattava kokonaismuutosta. Jos tama
    # ei pade, jokin rivi on luettu kahdesti tai jaanyt pois.
    revenue_total = head["tax_change_eur"] + head["borrowing_change_eur"]
    for name, value in (("menot", head["expenditure_change_eur"]), ("tulot", revenue_total)):
        if abs(value - head["total_change_eur"]) > 1_000_000:
            raise ValueError(
                f"{name} {value:.0f} ei vastaa kokonaismuutosta {head['total_change_eur']:.0f}"
            )

    if head["total_change_eur"] >= 0:
        raise ValueError("Vaihtoehtobudjetin pitäisi pienentää kokonaisuutta")

    # Nettolainanoton on lahes nollauduttava, se on koko esityksen ydin.
    borrowing = analysis["revenue_by_class"]["lainanotto"]
    if borrowing["change_eur"] >= 0:
        raise ValueError("Nettolainanotto ei vähene")

    # Kysyntavaikutuksen haarukan on oltava jarjestyksessa ja negatiivinen.
    impulse = analysis["fiscal_impulse_eur"]
    if not impulse["high"] < impulse["mid"] < impulse["low"] < 0:
        raise ValueError(f"Kysyntävaikutuksen haarukka on epäjohdonmukainen: {impulse}")

    # Luokittelun on katettava kaikki muutetut momentit.
    exp_sum = sum(entry["change_eur"] for entry in analysis["expenditure_by_class"].values())
    if abs(exp_sum - head["expenditure_change_eur"]) > 1000:
        raise ValueError("Menoluokittelu ei kata kaikkia momentteja")
    rev_sum = sum(entry["change_eur"] for entry in analysis["revenue_by_class"].values())
    if abs(rev_sum - revenue_total) > 1000:
        raise ValueError("Tuloluokittelu ei kata kaikkia momentteja")

    # Uudet avaukset ovat lisayksia, eivat leikkauksia. Jos tama ei pade,
    # luokitteluun on eksynyt vaaria momentteja.
    new_items = analysis["expenditure_by_class"]["uudet_avaukset"]
    if new_items["change_eur"] <= 0:
        raise ValueError("Uudet avaukset eivät ole nettona lisäyksiä")

    # Korkomenojen luokan on oltava pieni ja negatiivinen, koska
    # vaihtoehtobudjetissa velka pienenee.
    if analysis["expenditure_by_class"]["muut"]["change_eur"] > 0:
        raise ValueError("Korkomenoluokka kasvaa, tarkista luokittelu")

    # Hitaampi vaiheistus loiventaa yksittaisen vuoden iskua mutta paatyy
    # samaan tasovaikutukseen. Molemmat ehdot tarkistetaan, koska raportin
    # johtopaatos vaiheistuksesta nojaa juuri tahan eroon.
    fast = analysis["scenarios"]["kerralla"]["paths"]["mid"]
    slow = analysis["scenarios"]["vaalikausi"]["paths"]["mid"]
    if not slow["worst_year_growth_pct"] > fast["worst_year_growth_pct"]:
        raise ValueError("Vaiheistus ei loivenna vuosi-iskua, tarkista laskenta")
    if abs(slow["final_level_pct"] - fast["final_level_pct"]) > 1e-9:
        raise ValueError("Vaiheistus muuttaa lopullista tasovaikutusta, mikä on virhe")

    # Velkasuhde jaa vaihtoehdossa perusuraa matalammaksi kymmenen vuoden
    # kuluttua. Jos nain ei ole, koko sopeutuksen peruste kaatuu.
    for key, path in analysis["debt_paths"].items():
        if path["difference_pp"][-1] >= 0:
            raise ValueError(f"Velkasuhde ei laske vaihtoehdossa, skenaario {key}")
        if path["baseline_pct"][-1] <= path["baseline_pct"][0]:
            raise ValueError("Perusuran velkasuhde ei kasva, tarkista oletukset")


def main() -> int:
    parser = argparse.ArgumentParser(description="Analysoi Liberaalipuolueen vaihtoehtobudjetti.")
    parser.add_argument("--out", type=Path, default=OUT_JSON)
    args = parser.parse_args()

    items, totals = load_items()
    analysis = build_analysis(items, totals)
    validate(analysis)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(analysis, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    head = analysis["headline"]
    print(f"{args.out.relative_to(ROOT)}")
    print(f"  momentteja muutettu        {head['item_count']}")
    print(f"  kokonaismuutos             {head['total_change_eur'] / 1e9:7.2f} mrd "
          f"({head['total_change_pct_of_gdp']:.2f} % BKT:sta)")
    print(f"  menoleikkaukset            {head['expenditure_change_eur'] / 1e9:7.2f} mrd")
    print(f"  verotus netto              {head['tax_change_eur'] / 1e9:7.2f} mrd")
    print(f"  nettolainanotto            {head['borrowing_change_eur'] / 1e9:7.2f} mrd")
    impulse = analysis["fiscal_impulse_pct_of_gdp"]
    print(f"  kysyntävaikutus            {impulse['high']:.2f} ... {impulse['low']:.2f} % BKT:sta "
          f"(keskiarvo {impulse['mid']:.2f})")
    print()
    for key, scenario in analysis["scenarios"].items():
        path = scenario["paths"]["mid"]
        print(f"  {scenario['label']:32} pahin vuosi {path['worst_year_growth_pct']:6.2f} %, "
              f"lopullinen taso {path['final_level_pct']:6.2f} %")
    print()
    print("  velkasuhde 10 vuoden kuluttua:")
    for key, path in analysis["debt_paths"].items():
        label = analysis["scenarios"][key]["label"]
        print(f"    {label:32} perusura {path['baseline_pct'][-1]:5.1f} %  "
              f"vaihtoehto {path['alternative_pct'][-1]:5.1f} %  "
              f"ero {path['difference_pp'][-1]:6.1f} pp")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Skenaariolaskelma: mitä tapahtuu, jos automaatio syrjäyttää osan
työvoimasta, ja kuinka paljon tuottavuuden on noustava sen kompensoimiseksi.

Tämä ei ole ennuste. Se on mekaaninen laskelma, joka näyttää mitä annetuista
oletuksista seuraa. Kaikki oletukset ovat tässä tiedostossa näkyvissä ja
muutettavissa komentoriviltä.

Perusidentiteetti on sama kuin elintason hajotelmassa: BKT on tuottavuuden ja
työtuntien tulo. Jos työtunnit vähenevät osuudella s, tuottavuuden on noustava
kertoimella 1/(1-s), jotta BKT pysyy ennallaan. Se on aritmetiikkaa, ei
oletus.

Julkisen talouden puoli on epävarmempi ja esitetään haarukkana. Varmimmat
erät ovat palkkasumman pieneneminen ja työttömyysetuuksien kasvu. Yhteisöveron
vastapaino riippuu siitä, muuttuuko säästynyt palkkasumma verotettavaksi
voitoksi Suomessa, mikä ei ole itsestään selvää.

Lähtöluvut ovat vuodelta 2025 ja peräisin Tilastokeskuksesta ja
Valtiokonttorilta Budjettihaukan kautta.
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

from config import settings

OUT_JSON = ROOT / "data" / "reference" / "scenario_ai_displacement_v1.json"

# Kiintopisteet, jotka luetaan Budjettihaukasta. Nama tarkistetaan ajossa,
# jotta skenaario ei jaa vanhentuneiden lukujen varaan.
BASELINE = {
    "year": 2025,
    "employed_persons": 2_732_100,
    "hours_worked": 4_224_000_000.0,
    "gdp_nominal_eur": 281_674_000_000.0,
    "wages_and_salaries_eur": 114_600_000_000.0,
    "unemployed_persons": 278_000,
    "state_revenue_eur": 75_300_000_000.0,
    "state_expense_eur": 89_300_000_000.0,
    "income_and_capital_tax_eur": 26_090_000_000.0,
    "vat_eur": 22_610_000_000.0,
    "corporate_tax_eur": 5_390_000_000.0,
    "unemployment_benefit_eur": 1_910_000_000.0,
    "recent_productivity_growth_pct": 0.29,
}

# Oletukset, joita voi muuttaa. Jokainen on nakyvissa tuloksessa.
ASSUMPTIONS = {
    # Osuus tyollisista, jonka tyopanos korvautuu automaatiolla.
    "displaced_share": 0.15,
    # Yhteisoverokanta. Ylaraja sille, paljonko saastynyt palkkasumma
    # tuottaa veroa jos se muuttuu kokonaan Suomessa verotettavaksi voitoksi.
    "corporate_tax_rate": 0.20,
    # Osuus saastyneesta palkkasummasta, joka realisoituu verotettavana
    # voittona Suomessa. Ylaraja 1,0 on epauskottava, siksi haarukka.
    "profit_realisation_low": 0.3,
    "profit_realisation_high": 0.7,
    # Asumistuen ja toimeentulotuen lisays syrjaytettya kohden vuodessa.
    "extra_means_tested_per_person_eur_low": 1_500.0,
    "extra_means_tested_per_person_eur_high": 3_000.0,
    # Vuosia, joiden aikana tuottavuuden nousu tapahtuisi.
    "horizons_years": [10, 15, 20],
}


def check_baseline(project: str, dataset: str) -> dict[str, Any]:
    """Varmistaa, etta kasin kirjatut lahtoluvut vastaavat varastoa."""
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    sql = f"""
    SELECT series_id, value
    FROM `{project}.{dataset}.official_macro_reference_v1`
    WHERE year = {BASELINE['year']}
      AND series_id IN ('gdp_current_prices_meur', 'employed_persons_thousands',
                        'hours_worked_millions')
    """
    warehouse = {row["series_id"]: float(row["value"]) for row in client.query(sql).result()}
    checks = {
        "gdp_nominal_eur": warehouse["gdp_current_prices_meur"] * 1e6,
        "employed_persons": warehouse["employed_persons_thousands"] * 1e3,
        "hours_worked": warehouse["hours_worked_millions"] * 1e6,
    }
    drift = {}
    for key, expected in checks.items():
        actual = BASELINE[key]
        if expected and abs(actual - expected) / expected > 0.01:
            drift[key] = {"scenario": actual, "warehouse": expected}
    if drift:
        raise ValueError(f"Lähtöluvut ovat vanhentuneet varastoon nähden: {drift}")
    return checks


def compute(assumptions: dict[str, Any]) -> dict[str, Any]:
    share = assumptions["displaced_share"]
    if not 0 < share < 1:
        raise ValueError("Syrjäytetty osuus on oltava välillä 0-1")

    displaced = BASELINE["employed_persons"] * share
    remaining_factor = 1 - share

    # 1. Tuottavuusvaatimus. Puhdasta aritmetiikkaa: BKT = tuottavuus x tunnit.
    required_lift = 1 / remaining_factor - 1
    horizons = {
        str(years): 100 * ((1 + required_lift) ** (1 / years) - 1)
        for years in assumptions["horizons_years"]
    }

    # 2. Julkisen talouden erat, kun BKT pidetaan ennallaan tuottavuudella.
    wage_bill_loss = BASELINE["wages_and_salaries_eur"] * share
    effective_state_rate = (
        BASELINE["income_and_capital_tax_eur"] / BASELINE["wages_and_salaries_eur"]
    )
    income_tax_loss = wage_bill_loss * effective_state_rate

    benefit_per_unemployed = (
        BASELINE["unemployment_benefit_eur"] / BASELINE["unemployed_persons"]
    )
    unemployment_cost = displaced * benefit_per_unemployed
    means_tested_low = displaced * assumptions["extra_means_tested_per_person_eur_low"]
    means_tested_high = displaced * assumptions["extra_means_tested_per_person_eur_high"]

    rate = assumptions["corporate_tax_rate"]
    corporate_gain_low = wage_bill_loss * assumptions["profit_realisation_low"] * rate
    corporate_gain_high = wage_bill_loss * assumptions["profit_realisation_high"] * rate

    net_worse_low = income_tax_loss + unemployment_cost + means_tested_low - corporate_gain_high
    net_worse_high = income_tax_loss + unemployment_cost + means_tested_high - corporate_gain_low

    baseline_gap = BASELINE["state_expense_eur"] - BASELINE["state_revenue_eur"]

    return {
        "meta": {
            "dataset_id": "scenario_ai_displacement_v1",
            "kind": "mekaaninen skenaariolaskelma, ei ennuste",
            "baseline_year": BASELINE["year"],
            "baseline": BASELINE,
            "assumptions": assumptions,
        },
        "labour": {
            "displaced_persons": displaced,
            "remaining_hours_factor": remaining_factor,
        },
        "productivity_requirement": {
            "lift_to_hold_gdp_pct": 100 * required_lift,
            "annual_rate_by_horizon_pct": horizons,
            "recent_actual_growth_pct": BASELINE["recent_productivity_growth_pct"],
            "multiple_of_recent": {
                years: rate_pct / BASELINE["recent_productivity_growth_pct"]
                for years, rate_pct in horizons.items()
            },
        },
        "fiscal": {
            "wage_bill_loss_eur": wage_bill_loss,
            "effective_state_tax_rate_on_wages": effective_state_rate,
            "income_tax_loss_eur": income_tax_loss,
            "state_benefit_per_unemployed_eur": benefit_per_unemployed,
            "unemployment_benefit_cost_eur": unemployment_cost,
            "means_tested_cost_low_eur": means_tested_low,
            "means_tested_cost_high_eur": means_tested_high,
            "corporate_tax_offset_low_eur": corporate_gain_low,
            "corporate_tax_offset_high_eur": corporate_gain_high,
            "net_worsening_low_eur": net_worse_low,
            "net_worsening_high_eur": net_worse_high,
            "baseline_state_gap_eur": baseline_gap,
            "net_worsening_share_of_gap_low": net_worse_low / baseline_gap,
            "net_worsening_share_of_gap_high": net_worse_high / baseline_gap,
        },
    }


def validate(result: dict[str, Any]) -> None:
    productivity = result["productivity_requirement"]
    fiscal = result["fiscal"]

    # Tuottavuusvaatimus on aritmetiikkaa: tarkistetaan se suoraan.
    share = result["meta"]["assumptions"]["displaced_share"]
    expected = 100 * (1 / (1 - share) - 1)
    if abs(productivity["lift_to_hold_gdp_pct"] - expected) > 1e-9:
        raise ValueError("Tuottavuusvaatimus ei vastaa identiteettiä")

    if fiscal["net_worsening_low_eur"] > fiscal["net_worsening_high_eur"]:
        raise ValueError("Haarukan rajat ovat väärin päin")
    if fiscal["net_worsening_low_eur"] <= 0:
        raise ValueError("Skenaario ei heikennä taloutta, tarkista oletukset")

    # Efektiivinen verokanta on valtion osuus palkkasummasta, ei koko
    # veroaste. Jos se ylittaa 40 %, jokin luku on vaarin.
    if not 0.1 < fiscal["effective_state_tax_rate_on_wages"] < 0.4:
        raise ValueError("Epäuskottava efektiivinen verokanta")


def main() -> int:
    parser = argparse.ArgumentParser(description="Automaatioskenaario ja valtiontalous.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--displaced-share", type=float, default=ASSUMPTIONS["displaced_share"])
    parser.add_argument("--skip-baseline-check", action="store_true")
    args = parser.parse_args()

    if not args.skip_baseline_check:
        check_baseline(args.project, args.dataset)

    assumptions = {**ASSUMPTIONS, "displaced_share": args.displaced_share}
    result = compute(assumptions)
    validate(result)
    OUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    labour, prod, fiscal = result["labour"], result["productivity_requirement"], result["fiscal"]
    share_pct = 100 * assumptions["displaced_share"]
    print(f"Skenaario: automaatio korvaa {share_pct:.0f} % työllisistä "
          f"({labour['displaced_persons']:,.0f} henkeä)".replace(",", " "))
    print()
    print("TUOTTAVUUSVAATIMUS (aritmetiikkaa)")
    print(f"  BKT:n säilyttäminen vaatii tuottavuuteen  +{prod['lift_to_hold_gdp_pct']:.1f} %")
    for years, rate in prod["annual_rate_by_horizon_pct"].items():
        multiple = prod["multiple_of_recent"][years]
        print(f"  {years:>2} vuodessa                              "
              f"{rate:.2f} %/v  = {multiple:.1f}x nykyvauhti")
    print(f"  toteutunut vauhti 2008-2025                {prod['recent_actual_growth_pct']:.2f} %/v")
    print()
    print("VALTIONTALOUS (BKT pidetään ennallaan tuottavuudella)")
    print(f"  palkkasumma pienenee                      -{fiscal['wage_bill_loss_eur'] / 1e9:.1f} mrd")
    print(f"  ansiotuloverot (efektiivinen "
          f"{100 * fiscal['effective_state_tax_rate_on_wages']:.1f} %)      "
          f"-{fiscal['income_tax_loss_eur'] / 1e9:.1f} mrd")
    print(f"  työttömyysetuudet "
          f"({fiscal['state_benefit_per_unemployed_eur']:,.0f} €/hlö)".replace(",", " ")
          + f"      -{fiscal['unemployment_benefit_cost_eur'] / 1e9:.1f} mrd")
    print(f"  asumis- ja toimeentulotuki                "
          f"-{fiscal['means_tested_cost_low_eur'] / 1e9:.1f}..."
          f"-{fiscal['means_tested_cost_high_eur'] / 1e9:.1f} mrd")
    print(f"  yhteisövero vastapainona                  "
          f"+{fiscal['corporate_tax_offset_low_eur'] / 1e9:.1f}..."
          f"+{fiscal['corporate_tax_offset_high_eur'] / 1e9:.1f} mrd")
    print(f"  NETTO                                     "
          f"-{fiscal['net_worsening_low_eur'] / 1e9:.1f}..."
          f"-{fiscal['net_worsening_high_eur'] / 1e9:.1f} mrd vuodessa")
    print(f"  nykyinen meno-tuloero                     "
          f"{fiscal['baseline_state_gap_eur'] / 1e9:.1f} mrd")
    print(f"  skenaario kasvattaisi sitä                "
          f"{100 * fiscal['net_worsening_share_of_gap_low']:.0f}..."
          f"{100 * fiscal['net_worsening_share_of_gap_high']:.0f} %")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

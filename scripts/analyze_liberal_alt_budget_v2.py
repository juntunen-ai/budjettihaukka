#!/usr/bin/env python3
"""Build an auditable scenario model for Liberaalipuolue's 2026 budget.

The model deliberately separates three layers:

1. accounting facts copied from the source spreadsheet;
2. short- and medium-run macro scenarios based on published multipliers; and
3. explicitly labelled supply-side sensitivities.

It is a scenario calculation, not a forecast. Its distributional module is a
public-data synthetic household microsimulation, not a register-based SISU run.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any

from analyze_liberal_historical_counterfactual import build_historical_counterfactual
from analyze_liberal_alt_budget_microsim import build_microsimulation

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "reference" / "liberaali_vaihtoehtobudjetti"
RAW_CSV = DATA_DIR / "vaihtoehtobudjetti_2026_raw.csv"
OUT_JSON = DATA_DIR / "analyysi_v2.json"

SOURCE_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "11UDqKOwld7tLxzuD-KwHN2-4rxqtl6JsykV9igvxSQo/edit?gid=1938612580"
)
SOURCE_SHA256 = "ab1f36d0ee98312172205a289df14c64e52ad49881c1f7f895002c8899a35aa3"
MODEL_VINTAGE = "2026-08-23"

# The spreadsheet says that EUR 2.7139bn of state income tax and an equal
# amount of wellbeing-services financing are devolved to the regions.
REGIONAL_TAX_DEVOLUTION_EUR = 2_713_900_000.0
EMISSIONS_RECLASSIFICATION_EUR = 503_000_000.0
# The increase on moment 13.03.01 is explicitly based on selling Alko. It
# improves cash flow once, but cannot finance a permanent annual tax cut.
IDENTIFIED_ONE_OFF_REVENUE_EUR = 614_000_000.0

# Bank of Finland June 2026 forecast, current-price GDP and EDP debt.
YEARS = list(range(2026, 2036))
BASELINE_GDP_EUR = {
    2025: 280_570_000_000.0,
    2026: 287_468_000_000.0,
    2027: 296_683_000_000.0,
    2028: 306_713_000_000.0,
}
BASELINE_DEBT_EUR = {
    2025: 248_400_000_000.0,
    2026: 264_100_000_000.0,
    2027: 279_300_000_000.0,
    2028: 297_200_000_000.0,
}
LONG_RUN_NOMINAL_GDP_GROWTH = 0.032
LONG_RUN_BASELINE_DEFICIT_PCT = 4.5
AUTOMATIC_STABILISER_SEMI_ELASTICITY = 0.5
EFFECTIVE_INTEREST_SAVING_RATE = 0.025
WAGE_BILL_EUR = 114_600_000_000.0

EXPENDITURE_CLASSES = {
    "toimintamenot": ((1, 28), "Valtion toimintamenot"),
    "tekninen": ((29, 29), "Arvonlisäveromenot, tekninen erä"),
    "kunnat_ja_hyvinvointialueet": ((30, 39), "Kunnat ja hyvinvointialueet"),
    "elinkeinoelama": ((40, 49), "Valtionavut elinkeinoelämälle"),
    "kotitaloudet": ((50, 59), "Tulonsiirrot kotitalouksille ja yhteisöille"),
    "rahastot_eu_ulkomaat": ((60, 69), "Rahastot, EU ja ulkomaat"),
    "investoinnit": ((70, 79), "Reaalisijoitukset"),
    "lainat": ((80, 89), "Lainat ja finanssisijoitukset"),
    "muut": ((90, 99), "Muut menot"),
    "uudet_avaukset": (None, "Uudet avaukset"),
}

# Five-year cumulative multiplier cases. Central estimates primarily follow
# Government's Research Activities 2026:3. The supply targets are transparent
# sensitivities, not estimated effects of this package.
PARAMETER_CASES = {
    "optimistinen": {
        "label": "Optimistinen",
        "spending": 0.70,
        "investment": 0.80,
        "rdi": 0.80,
        "consumption_tax": 0.50,
        "household_income_tax": 0.40,
        "corporate_tax": 1.00,
        "wealth_tax": 0.40,
        "other_tax": 0.20,
        "supply_target_pct": 1.20,
        "recurring_balance_delivery": 1.00,
    },
    "keskinen": {
        "label": "Keskinen",
        "spending": 1.00,
        "investment": 1.50,
        "rdi": 1.50,
        "consumption_tax": 1.30,
        "household_income_tax": 0.20,
        "corporate_tax": 1.40,
        "wealth_tax": 0.20,
        "other_tax": 0.10,
        "supply_target_pct": 0.60,
        "recurring_balance_delivery": 0.90,
    },
    "varovainen": {
        "label": "Varovainen",
        "spending": 1.40,
        "investment": 2.70,
        "rdi": 2.00,
        "consumption_tax": 2.20,
        "household_income_tax": 0.00,
        "corporate_tax": 1.80,
        "wealth_tax": 0.10,
        "other_tax": 0.00,
        "supply_target_pct": 0.15,
        "recurring_balance_delivery": 0.75,
    },
}

SCENARIOS = {
    "kerralla": {
        "label": "Taulukko sellaisenaan, kerralla",
        "short_label": "Kerralla",
        "phase": [1.0],
        "composition_factor": 1.0,
        "consumption_factor": 1.0,
        "supply_factor": 0.70,
        "protect_growth_spending": False,
    },
    "vaiheistettu": {
        "label": "Taulukko sellaisenaan, neljässä vuodessa",
        "short_label": "Vaiheistettu",
        "phase": [0.15, 0.25, 0.30, 0.30],
        "composition_factor": 1.0,
        "consumption_factor": 1.0,
        "supply_factor": 0.85,
        "protect_growth_spending": False,
    },
    "suojattu": {
        "label": "Suositus: vaiheistus ja kasvumenojen suoja",
        "short_label": "Suositus",
        "phase": [0.10, 0.20, 0.30, 0.40],
        "composition_factor": 0.68,
        "consumption_factor": 0.85,
        "supply_factor": 1.25,
        "protect_growth_spending": True,
    },
}

RESPONSE_SHAPES = {
    "spending": [0.10, 0.50, 0.20, 0.10, 0.10],
    "investment": [0.00, 0.20, 0.25, 0.25, 0.30],
    "rdi": [0.00, 0.10, 0.20, 0.30, 0.40],
    "consumption_tax": [0.00, 0.15, 0.25, 0.30, 0.30],
    "household_income_tax": [0.00, 0.20, 0.30, 0.30, 0.20],
    "corporate_tax": [0.00, 0.10, 0.25, 0.35, 0.30],
    "wealth_tax": [0.00, 0.25, 0.30, 0.25, 0.20],
    "other_tax": [0.00, 0.25, 0.30, 0.25, 0.20],
}
SUPPLY_RAMP = [0.0, 0.15, 0.35, 0.60, 0.82, 1.0]


def parse_number(text: str | None) -> float:
    cleaned = (text or "").replace("−", "-").replace("\u00a0", "").replace(" ", "")
    cleaned = re.sub(r"[^0-9,.\-]", "", cleaned).replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def expenditure_class(moment: str) -> str:
    lowered = (moment or "").lower()
    if "lib" in lowered:
        return "uudet_avaukset"
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)", lowered)
    if not match:
        return "uudet_avaukset"
    code = int(match.group(3))
    for name, (bounds, _label) in EXPENDITURE_CLASSES.items():
        if bounds and bounds[0] <= code <= bounds[1]:
            return name
    return "muut"


def load_rows() -> list[dict[str, str]]:
    with RAW_CSV.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def row_change(row: dict[str, str]) -> float:
    return parse_number(row["Leikattavaa momentista"])


def find_row(rows: list[dict[str, str]], moment: str, level: str = "3") -> dict[str, str]:
    matches = [r for r in rows if r["momenttinumerot"] == moment and r["momenttitaso"] == level]
    if len(matches) != 1:
        raise ValueError(f"Expected one row for {moment} level {level}, got {len(matches)}")
    return matches[0]


def baseline_paths() -> tuple[dict[int, float], dict[int, float]]:
    gdp = dict(BASELINE_GDP_EUR)
    debt = dict(BASELINE_DEBT_EUR)
    for year in range(2029, 2036):
        gdp[year] = gdp[year - 1] * (1 + LONG_RUN_NOMINAL_GDP_GROWTH)
        debt[year] = debt[year - 1] + LONG_RUN_BASELINE_DEFICIT_PCT / 100 * gdp[year]
    return gdp, debt


def add_response(
    path: list[float],
    total_change: float,
    phase: list[float],
    multiplier: float,
    shape: list[float],
    sign: float,
) -> None:
    for phase_index, phase_share in enumerate(phase):
        tranche = total_change * phase_share
        for lag, response_share in enumerate(shape):
            target = phase_index + lag
            if target < len(path):
                path[target] += sign * tranche * multiplier * response_share


def build_scenario_paths(
    accounting: dict[str, Any],
    parameters: dict[str, float],
    scenario: dict[str, Any],
) -> list[dict[str, float | int]]:
    phase = scenario["phase"]
    demand = [0.0 for _ in YEARS]
    instruments = accounting["macro_instruments"]

    spending_multiplier = parameters["spending"] * scenario["composition_factor"]
    add_response(
        demand,
        instruments["consolidated_spending_change_eur"],
        phase,
        spending_multiplier,
        RESPONSE_SHAPES["spending"],
        1.0,
    )

    # In the unmodified package, investment and R&D cuts get their own higher
    # multipliers. In the safeguarded package they are replaced euro-for-euro
    # by lower-multiplier savings identified in a spending review.
    if not scenario["protect_growth_spending"]:
        add_response(
            demand,
            instruments["public_investment_change_eur"],
            phase,
            parameters["investment"] - spending_multiplier,
            RESPONSE_SHAPES["investment"],
            1.0,
        )
        add_response(
            demand,
            instruments["rdi_support_change_eur"],
            phase,
            parameters["rdi"] - spending_multiplier,
            RESPONSE_SHAPES["rdi"],
            1.0,
        )

    for name in (
        "consumption_tax",
        "household_income_tax",
        "corporate_tax",
        "wealth_tax",
        "other_tax",
    ):
        factor = scenario["consumption_factor"] if name == "consumption_tax" else 1.0
        add_response(
            demand,
            instruments[f"{name}_change_eur"],
            phase,
            parameters[name] * factor,
            RESPONSE_SHAPES[name],
            -1.0,
        )

    gdp, baseline_debt = baseline_paths()
    supply_target = parameters["supply_target_pct"] * scenario["supply_factor"]
    supply_pct = [0.0 for _ in YEARS]
    for phase_index, phase_share in enumerate(phase):
        for lag in range(len(YEARS) - phase_index):
            ramp = SUPPLY_RAMP[min(lag, len(SUPPLY_RAMP) - 1)]
            supply_pct[phase_index + lag] += supply_target * phase_share * ramp

    rows: list[dict[str, float | int]] = []
    previous_alt_debt = BASELINE_DEBT_EUR[2025]
    previous_base_debt = BASELINE_DEBT_EUR[2025]
    recurring_balance_improvement = (
        accounting["direct_balance_improvement_eur"]
        - accounting["identified_one_off_revenue_eur"]
    ) * parameters["recurring_balance_delivery"]
    completed = 0.0
    for index, year in enumerate(YEARS):
        if index < len(phase):
            completed += phase[index]
        completed = min(completed, 1.0)
        supply_effect = gdp[year] * supply_pct[index] / 100
        total_output = demand[index] + supply_effect
        output_pct = 100 * total_output / gdp[year]
        one_off_revenue = accounting["identified_one_off_revenue_eur"] if year == YEARS[0] else 0.0
        direct_improvement = recurring_balance_improvement * completed + one_off_revenue
        cyclical_feedback = AUTOMATIC_STABILISER_SEMI_ELASTICITY * total_output
        interest_saving = EFFECTIVE_INTEREST_SAVING_RATE * max(
            previous_base_debt - previous_alt_debt, 0.0
        )
        net_improvement = direct_improvement + cyclical_feedback + interest_saving
        baseline_increment = baseline_debt[year] - previous_base_debt
        alternative_debt = previous_alt_debt + baseline_increment - net_improvement
        alternative_gdp = gdp[year] + total_output
        rows.append(
            {
                "year": year,
                "implementation_pct": 100 * completed,
                "demand_effect_eur": demand[index],
                "supply_effect_eur": supply_effect,
                "supply_effect_pct": supply_pct[index],
                "total_output_effect_eur": total_output,
                "total_output_effect_pct": output_pct,
                "direct_balance_improvement_eur": direct_improvement,
                "recurring_balance_improvement_eur": recurring_balance_improvement * completed,
                "identified_one_off_revenue_eur": one_off_revenue,
                "cyclical_feedback_eur": cyclical_feedback,
                "interest_saving_eur": interest_saving,
                "net_balance_improvement_eur": net_improvement,
                "baseline_gdp_eur": gdp[year],
                "alternative_gdp_eur": alternative_gdp,
                "baseline_debt_eur": baseline_debt[year],
                "alternative_debt_eur": alternative_debt,
                "baseline_debt_pct": 100 * baseline_debt[year] / gdp[year],
                "alternative_debt_pct": 100 * alternative_debt / alternative_gdp,
            }
        )
        previous_base_debt = baseline_debt[year]
        previous_alt_debt = alternative_debt
    return rows


def build_analysis(rows: list[dict[str, str]]) -> dict[str, Any]:
    level0 = {r["tulo/meno"]: r for r in rows if r["momenttitaso"] == "0"}
    proposal_total = parse_number(level0["meno"]["numero"])
    alternative_total = parse_number(level0["meno"]["Lib budjetti"])
    total_spending_change = alternative_total - proposal_total

    level3 = [r for r in rows if r["momenttitaso"] == "3" and row_change(r)]
    spending_items = [r for r in level3 if r["tulo/meno"] == "meno"]
    revenue_items = [r for r in level3 if r["tulo/meno"] == "tulo"]

    exp_by_class: dict[str, dict[str, Any]] = {}
    for class_id, (_bounds, label) in EXPENDITURE_CLASSES.items():
        selected = [r for r in spending_items if expenditure_class(r["momenttinumerot"]) == class_id]
        exp_by_class[class_id] = {
            "label": label,
            "change_eur": sum(row_change(r) for r in selected),
            "count": len(selected),
        }

    departments = []
    for row in rows:
        if row["tulo/meno"] == "meno" and row["momenttitaso"] == "1" and row_change(row):
            departments.append(
                {
                    "code": row["momenttinumerot"],
                    "label": re.sub(r"^\d+\.\s*", "", row["nimi"]).title(),
                    "change_eur": row_change(row),
                }
            )
    departments.sort(key=lambda item: item["change_eur"])

    tax_chapter_change = row_change(find_row(rows, "11.", "1"))
    non_tax_change = row_change(find_row(rows, "12.", "1")) + row_change(find_row(rows, "13.", "1"))
    borrowing_change = row_change(find_row(rows, "15.03.01."))
    central_nonborrowing_revenue_change = tax_chapter_change + non_tax_change

    household_income_central = row_change(find_row(rows, "11.01.01."))
    corporate_tax = row_change(find_row(rows, "11.01.02."))
    inheritance_tax = row_change(find_row(rows, "11.01.04."))
    transfer_tax = row_change(find_row(rows, "11.10.05."))
    wealth_tax = inheritance_tax + transfer_tax

    consumption_names = (
        "arvonlisävero",
        "energiaverot",
        "autovero",
        "ajoneuvovero",
        "tupakkavero",
        "alkoholijuomavero",
        "arpajaisvero",
        "apteekkivero",
        "kannabisvero",
        "virvoitusjuomavero",
        "jätevero",
    )
    consumption_tax = sum(
        row_change(r)
        for r in revenue_items
        if any(name in r["nimi"].lower() for name in consumption_names)
    )
    reported_other_tax = (
        tax_chapter_change
        - household_income_central
        - corporate_tax
        - consumption_tax
        - wealth_tax
    )
    actual_other_tax = reported_other_tax - EMISSIONS_RECLASSIFICATION_EUR
    consolidated_household_income_tax = household_income_central + REGIONAL_TAX_DEVOLUTION_EUR
    payer_tax_burden_change = (
        consolidated_household_income_tax
        + corporate_tax
        + consumption_tax
        + wealth_tax
        + actual_other_tax
    )
    adjusted_non_tax_change = non_tax_change + EMISSIONS_RECLASSIFICATION_EUR
    consolidated_revenue_change = payer_tax_burden_change + adjusted_non_tax_change
    consolidated_spending_change = total_spending_change + REGIONAL_TAX_DEVOLUTION_EUR
    direct_balance_improvement = consolidated_revenue_change - consolidated_spending_change

    public_investment_change = exp_by_class["investoinnit"]["change_eur"]
    rdi_support_change = row_change(find_row(rows, "32.20.40."))
    macro_instruments = {
        "consolidated_spending_change_eur": consolidated_spending_change,
        "public_investment_change_eur": public_investment_change,
        "rdi_support_change_eur": rdi_support_change,
        "consumption_tax_change_eur": consumption_tax,
        "household_income_tax_change_eur": consolidated_household_income_tax,
        "corporate_tax_change_eur": corporate_tax,
        "wealth_tax_change_eur": wealth_tax,
        "other_tax_change_eur": actual_other_tax,
        "non_tax_revenue_change_eur": adjusted_non_tax_change,
    }

    accounting = {
        "central_budget": {
            "proposal_total_eur": proposal_total,
            "alternative_total_eur": alternative_total,
            "spending_change_eur": total_spending_change,
            "tax_chapter_change_eur": tax_chapter_change,
            "non_tax_revenue_change_eur": non_tax_change,
            "nonborrowing_revenue_change_eur": central_nonborrowing_revenue_change,
            "borrowing_change_eur": borrowing_change,
        },
        "consolidated_public_sector": {
            "regional_tax_assumption_eur": REGIONAL_TAX_DEVOLUTION_EUR,
            "emissions_reclassification_eur": EMISSIONS_RECLASSIFICATION_EUR,
            "payer_tax_burden_change_eur": payer_tax_burden_change,
            "adjusted_non_tax_revenue_change_eur": adjusted_non_tax_change,
            "nonborrowing_revenue_change_eur": consolidated_revenue_change,
            "spending_change_eur": consolidated_spending_change,
            "direct_balance_improvement_eur": direct_balance_improvement,
            "identified_one_off_revenue_eur": IDENTIFIED_ONE_OFF_REVENUE_EUR,
            "identified_recurring_balance_improvement_eur": (
                direct_balance_improvement - IDENTIFIED_ONE_OFF_REVENUE_EUR
            ),
            "tax_burden_if_half_regional_tax_eur": (
                tax_chapter_change
                - EMISSIONS_RECLASSIFICATION_EUR
                + REGIONAL_TAX_DEVOLUTION_EUR / 2
            ),
            "tax_burden_if_no_regional_tax_eur": (
                tax_chapter_change - EMISSIONS_RECLASSIFICATION_EUR
            ),
        },
        "macro_instruments": macro_instruments,
    }

    scenario_results: dict[str, Any] = {}
    for scenario_id, scenario in SCENARIOS.items():
        cases = {}
        for case_id, parameters in PARAMETER_CASES.items():
            path = build_scenario_paths(
                {
                    "direct_balance_improvement_eur": direct_balance_improvement,
                    "identified_one_off_revenue_eur": IDENTIFIED_ONE_OFF_REVENUE_EUR,
                    "macro_instruments": macro_instruments,
                },
                parameters,
                scenario,
            )
            first_five = path[:5]
            cases[case_id] = {
                "parameters": parameters,
                "yearly": path,
                "summary": {
                    "worst_output_year": min(path, key=lambda r: r["total_output_effect_pct"])["year"],
                    "worst_output_effect_pct": min(r["total_output_effect_pct"] for r in path),
                    "cumulative_output_effect_2026_2030_eur": sum(
                        r["total_output_effect_eur"] for r in first_five
                    ),
                    "debt_ratio_2035_pct": path[-1]["alternative_debt_pct"],
                    "debt_ratio_difference_2035_pp": (
                        path[-1]["alternative_debt_pct"] - path[-1]["baseline_debt_pct"]
                    ),
                    "debt_eur_difference_2035": (
                        path[-1]["alternative_debt_eur"] - path[-1]["baseline_debt_eur"]
                    ),
                    "long_run_supply_target_pct": (
                        parameters["supply_target_pct"] * scenario["supply_factor"]
                    ),
                    "recurring_balance_delivery_pct": (
                        100 * parameters["recurring_balance_delivery"]
                    ),
                    "delivered_recurring_balance_improvement_eur": (
                        (direct_balance_improvement - IDENTIFIED_ONE_OFF_REVENUE_EUR)
                        * parameters["recurring_balance_delivery"]
                    ),
                },
            }
        scenario_results[scenario_id] = {
            "label": scenario["label"],
            "short_label": scenario["short_label"],
            "phase": scenario["phase"],
            "protect_growth_spending": scenario["protect_growth_spending"],
            "cases": cases,
        }

    hierarchy_level1 = sum(
        row_change(r) for r in rows if r["tulo/meno"] == "meno" and r["momenttitaso"] == "1"
    )
    hierarchy_level3 = sum(row_change(r) for r in spending_items)
    source_hash = hashlib.sha256(RAW_CSV.read_bytes()).hexdigest()

    largest_cuts = sorted(
        [r for r in spending_items if row_change(r) < 0], key=row_change
    )[:18]
    largest_revenue = sorted(revenue_items, key=lambda r: abs(row_change(r)), reverse=True)[:16]

    return {
        "meta": {
            "dataset_id": "liberaali_vaihtoehtobudjetti_analyysi_v2",
            "model_vintage": MODEL_VINTAGE,
            "budget_year": 2026,
            "kind": (
                "kirjanpito-, historiallinen vastelaskelma, makroskenaario- ja synteettinen "
                "kotitalousmikrosimulaatio; ei ennuste eikä SISU-aineistosimulaatio"
            ),
            "source_url": SOURCE_URL,
            "source_sha256": source_hash,
            "live_source_verified_sha256": SOURCE_SHA256,
            "price_basis": "vuoden 2026 nimelliset eurot",
        },
        "source_audit": {
            "row_count": len(rows),
            "changed_level3_count": len(level3),
            "changed_spending_item_count": len(spending_items),
            "changed_revenue_item_count": len(revenue_items),
            "level0_spending_change_eur": total_spending_change,
            "level1_spending_change_eur": hierarchy_level1,
            "level3_spending_change_eur": hierarchy_level3,
            "hierarchy_difference_level3_vs_total_eur": hierarchy_level3 - total_spending_change,
            "known_hierarchy_issue": (
                "Valtioneuvoston kanslian lukujen alaerät ovat 0,7 milj. euroa "
                "hallinnonalan yhteissummaa suuremmat. Makromalli käyttää lähteen päätason summaa."
            ),
            "snapshot_matches_live_export": source_hash == SOURCE_SHA256,
        },
        "accounting": accounting,
        "expenditure_by_class": exp_by_class,
        "expenditure_by_department": departments,
        "tax_instruments": {
            "household_income_central_eur": household_income_central,
            "regional_income_tax_eur": REGIONAL_TAX_DEVOLUTION_EUR,
            "household_income_consolidated_eur": consolidated_household_income_tax,
            "corporate_income_tax_eur": corporate_tax,
            "consumption_taxes_eur": consumption_tax,
            "inheritance_and_transfer_taxes_eur": wealth_tax,
            "other_actual_taxes_eur": actual_other_tax,
            "payer_tax_burden_change_eur": payer_tax_burden_change,
        },
        "scenarios": scenario_results,
        "historical_counterfactual": build_historical_counterfactual(accounting),
        "microsimulation": build_microsimulation(rows),
        "largest_spending_cuts": [
            {
                "moment": r["momenttinumerot"],
                "label": r["nimi"],
                "change_eur": row_change(r),
                "priority": r["Liikennevalo"],
                "reason": r["Perustelu"],
            }
            for r in largest_cuts
        ],
        "largest_revenue_changes": [
            {
                "moment": r["momenttinumerot"],
                "label": r["nimi"],
                "change_eur": row_change(r),
                "priority": r["Liikennevalo"],
                "reason": r["Perustelu"],
            }
            for r in largest_revenue
        ],
        "distributional_stress_tests": distributional_stress_tests(),
        "implementation_gates": implementation_gates(),
        "recommendations": recommendations(),
        "research_basis": research_basis(),
        "model_limits": [
            "Synteettinen kotitalousmalli ei korvaa rekisteripohjaista SISU/FIONA-aineistosimulaatiota.",
            "Historiallinen vastelaskelma ei ole kausaalinen arvio toteutumatta jääneestä historiasta, vaan vuoden 2026 politiikkarakenteen ehdollinen sovellus toteutuneeseen 2008-2025 dataan.",
            "Viiden vuoden kertoimet ovat kumulatiivisia tuotantovaikutuksia, eivät pysyviä BKT-tasoja.",
            "Tarjontavaikutus on näkyvä herkkyysoletus, koska juuri tästä paketista ei ole kausaalista estimaattia.",
            "Velkapolku soveltaa valtion budjettipaketin vaikutusta EDP-velkaan olettaen muiden julkisyhteisöjen perusuran ennallaan.",
            "Vuodesta 2029 eteenpäin perusura on tekninen jatko, ei Suomen Pankin ennuste.",
            "Kuntien ja hyvinvointialueiden tehtävien, verojen ja asiakasmaksujen lopullista jakaumaa ei ole lakitasolla määritelty.",
            "Alkon 614 milj. euron myyntitulo on erotettu kertaluonteiseksi; menetettyjä tulevia osinkoja ei ole voitu arvioida lähdetaulukosta.",
            "Toistuvan tasapainovaikutuksen 100/90/75 prosentin toteutuminen on velkakestävyyden herkkyys, ei tilastollinen ennuste.",
        ],
    }


def distributional_stress_tests() -> list[dict[str, str]]:
    return [
        {
            "reform": "ALV-kantojen yhtenäistäminen",
            "risk": "korkea",
            "incidence": "Kulutusosuus tuloista on pienituloisilla suuri; 359,1 milj. euron kompensaatio on vain 8,8 % 4,086 mrd. euron bruttokertymästä.",
            "gate": "Tulodesiili- ja kotitaloustyyppikohtainen SISU-arvio sekä automaattinen, täysimääräinen kompensaatio pienituloisille.",
        },
        {
            "reform": "Eläketulovähennyksen ja muiden vähennysten poisto",
            "risk": "korkea",
            "incidence": "Työtulovähennys ei automaattisesti korvaa eläkeläisten, työmatkalaisten, yrittäjien tai lapsiperheiden menetyksiä.",
            "gate": "Ryhmäkohtainen efektiivisen veroasteen testi ja siirtymäsuoja ennen verokorttien muutosta.",
        },
        {
            "reform": "Hyvinvointialueiden rahoitus ja verotusoikeus",
            "risk": "korkea",
            "incidence": "Valtion veron lasku korvautuu alueverolla tai asiakasmaksuilla; ilman tuottavuusparannusta kokonaisrasitus ei alene.",
            "gate": "Tehtävien ja rahoituksen vastaavuus, alueveron katto, palvelutasomittarit ja pienituloisten maksukatto.",
        },
        {
            "reform": "Kuntien valtionosuuksien leikkaus",
            "risk": "korkea",
            "incidence": "Vaikutus kohdistuu eri tavoin veropohjaltaan heikkoihin kuntiin ja voi siirtyä kunnallisveroihin tai palveluihin.",
            "gate": "Kuntakohtainen rahoitus- ja palveluvelvoitelaskelma sekä monivuotinen tasausmekanismi.",
        },
        {
            "reform": "Perintöveron poisto",
            "risk": "keskikorkea",
            "incidence": "Likviditeettiongelma helpottuu, mutta hyöty painottuu varakkaisiin ja julkinen tulonmenetys on suuri.",
            "gate": "Carry-over basis, maksuaikahuojennus ja luovutusvoittoveron tuottoarvion riippumaton varmennus.",
        },
        {
            "reform": "Varainsiirtoveron poisto ja oman asunnon luovutusvoittovero",
            "risk": "keskikorkea",
            "incidence": "Varainsiirtoveron muuttoeste poistuu, mutta täysi luovutusvoittovero voi luoda uuden ja suuremman lukitusvaikutuksen.",
            "gate": "Inflaatio-oikaisu, elinikäinen vapaa osa tai veron lykkäys uuteen vakituiseen asuntoon.",
        },
        {
            "reform": "Ansiosidonnaisen lyhentäminen",
            "risk": "keskikorkea",
            "incidence": "Työllistymiskannustin vahvistuu, mutta tuloriski kasvaa aloilla ja alueilla, joilla avoimia työpaikkoja on vähän.",
            "gate": "Porrastus, uudelleenkoulutus ja suhdannejarru; perusturvan reaaliarvo suojataan.",
        },
    ]


def implementation_gates() -> list[dict[str, Any]]:
    return [
        {
            "stage": "0. Todennus",
            "timing": "6-12 kk ennen voimaantuloa",
            "share_pct": 0,
            "actions": [
                "SISU-mikrosimulaatio veroista, etuuksista ja asiakasmaksuista",
                "momenttikohtainen oikeudellinen ja EU-rahoituksen auditointi",
                "kuntien ja hyvinvointialueiden tehtävä-rahoituslaskelma",
                "riippumaton dynaamisten verotuottojen arvio",
            ],
            "stop_rule": "Toimea ei budjetoida, jos pysyvää nettotuottoa tai suojamekanismia ei voida todentaa.",
        },
        {
            "stage": "1. Nopeat, matalan haitan toimet",
            "timing": "vuosi 1",
            "share_pct": 10,
            "actions": [
                "hallinnon päällekkäisyydet ja vaikuttamattomat yritystuet",
                "veropohjien aukkojen sulkeminen ennen nimellisten verokantojen muutoksia",
                "varainsiirtoveron asteittainen poisto hyvin suunnitellulla luovutusvoittoverolla",
                "EU-vastinrahoitus ja jo ansaitut tulot suojataan",
            ],
            "stop_rule": "Kasvua tukevia investointeja tai TKI-rahoitusta ei leikata korvaavaa säästöä löytämättä.",
        },
        {
            "stage": "2. Verorakenteen vaihto",
            "timing": "vuodet 2-3",
            "share_pct": 50,
            "actions": [
                "ALV-uudistus ja kompensaatiot samana päivänä",
                "energia- ja autoverojen vaihto ennakoitavalla päästöhinnoittelulla",
                "työn veronkevennykset vain toteutuneita pysyviä säästöjä vastaan",
                "verovähennysten poisto siirtymäsäännöillä",
            ],
            "stop_rule": "Pienituloisen käytettävissä oleva tulo ei saa laskea ilman automaattista korvausta.",
        },
        {
            "stage": "3. Alue- ja palvelurakenne",
            "timing": "vuodet 3-4",
            "share_pct": 40,
            "actions": [
                "alueverotus vasta tehtävien, veropohjan ja tasausjärjestelmän jälkeen",
                "rahoitus leikkautuu vain toteutuneen tuottavuuden tai poistuneen tehtävän mukana",
                "palvelujen saatavuus ja hoitotulokset julkaistaan neljännesvuosittain",
                "suhdanne- ja turvallisuuspoikkeus aktivoidaan ennalta sovituilla kriteereillä",
            ],
            "stop_rule": "Rahoitusleikkaus keskeytyy, jos lakisääteinen saatavuus, potilasturvallisuus tai alueellinen yhdenvertaisuus alittuu.",
        },
    ]


def recommendations() -> list[dict[str, str]]:
    return [
        {
            "priority": "1",
            "title": "Korjaa verokevennyksen määritelmä",
            "text": "Viestinnässä käytetään maksajien konsolidoitua verorasitusta. Valtion 3,89 mrd. euron muiden tulojen vähenemä ei ole veronkevennys; täyden alueveron ja momenttisiirron jälkeen aito nettokevennys on noin 0,31 mrd. euroa.",
        },
        {
            "priority": "2",
            "title": "Sido veronalennukset toteutuneisiin pysyviin säästöihin",
            "text": "Verokiilaa lasketaan euro eurosta vasta, kun pysyvä menovähennys on toteutunut. Omaisuuden myyntiä, osinkopiikkiä tai EU-tulojen menetystä ei käytetä pysyvän veronalennuksen rahoittamiseen.",
        },
        {
            "priority": "3",
            "title": "Suojaa TKI, infrastruktuuri ja ydintoiminnot",
            "text": "Korvaa 669 milj. euron TKI-leikkaus ja 196 milj. euron investointileikkaukset matalan vaikuttavuuden yritystuilla, hankintojen standardoinnilla ja tehtävien aidolla poistolla. Uusin Suomen kerrointutkimus pitää investointileikkauksia erityisen kalliina.",
        },
        {
            "priority": "4",
            "title": "Suojaa pienituloiset ja eläkeläiset",
            "text": "Yhtenäinen ALV-pohja on perusteltu, mutta 359 milj. euron yleiskompensaatio ei vielä todista tulonjaollista neutraaliutta. Korvaus kohdennetaan kulutuskoriin, lääkekattoon ja kotitaloustyyppiin, ja eläketulovähennyksen poistolle rakennetaan tulosidonnainen siirtymäsuoja. Lopulliset rajat asetetaan SISU/FIONA-ajon perusteella.",
        },
        {
            "priority": "5",
            "title": "Uudista asumisen verotus ilman uutta lukkoa",
            "text": "Poista varainsiirtovero asteittain, mutta käytä oman asunnon luovutusvoitossa inflaatio-oikaisua ja roll-over-lykkäystä. Muuten uusi vero voi estää muuttoa enemmän kuin poistettava vero.",
        },
        {
            "priority": "6",
            "title": "Muuta perintöveroa ennen täyttä poistoa",
            "text": "Likviditeettiongelma ratkaistaan pitkällä maksuajalla ja yritysvarallisuuden turvalla. Jos vero poistetaan, hankintameno siirtyy perilliselle ilman step-upia, jotta luovutusvoittoveron tuotto ei jää oletukseksi.",
        },
        {
            "priority": "7",
            "title": "Erota alueautonomia säästöväitteestä",
            "text": "Hyvinvointialuevero on vastuun siirto, ei automaattinen kansantalouden säästö. Leikkaus kirjataan vasta, kun tehtävä poistuu tai tuottavuus näkyy palvelu- ja terveystuloksissa.",
        },
        {
            "priority": "8",
            "title": "Julkaise riippumaton toimeenpanotilinpito",
            "text": "Jokaiselle toimelle näytetään bruttosumma, käyttäytymisvaikutus, toteutunut nettosäästö, jakautumisvaikutus ja oikeudellinen tila. Epävarmoja tuloja ei lasketa velkajarrun täyttymiseen.",
        },
    ]


def research_basis() -> list[dict[str, Any]]:
    return [
        {
            "id": "liberaali_sheet_2026",
            "label": "Liberaalipuolueen vaihtoehtobudjetti 2026",
            "url": SOURCE_URL,
            "use": "Momentti-, perustelu- ja kirjanpitoluvut",
        },
        {
            "id": "statfin_income_deciles_2024",
            "label": "Tilastokeskus: Asuntokuntien tulot ja tulojen rakenne tulokymmenyksittäin 2024",
            "url": "https://pxdata.stat.fi/PxWeb/pxweb/fi/StatFin/StatFin__tjt/statfin_tjt_pxt_128c.px/",
            "use": "Synteettisen kotitalousmallin tulot, verot, tulonsiirrot, kotitalouspainot ja kulutusyksiköt",
        },
        {
            "id": "statfin_household_margins_2024",
            "label": "Tilastokeskus: Kotitalouksien elinvaihe ja etuuksien saajat tulokymmenyksittäin 2024",
            "url": "https://pxdata.stat.fi/PxWeb/pxweb/fi/StatFin/StatFin__tjt/statfin_tjt_pxt_12ew.px/",
            "use": "Mikrosimulaation 90 kotitaloussolun elinvaihepainot; etuuskohdennus taulukosta 122s",
        },
        {
            "id": "statfin_consumption_2022",
            "label": "Tilastokeskus: Kotitalouksien kulutusmenot tuloviidenneksittäin 2022",
            "url": "https://pxdata.stat.fi/PxWeb/pxweb/fi/StatFin/StatFin__ktutk/statfin_ktutk_pxt_14pg.px/",
            "use": "ALV-, energia-, liikenne- ja valmisteverojen kulutusperusteinen kohdennus",
        },
        {
            "id": "tax_admin_deductions_2024",
            "label": "Verohallinto: Henkilöasiakkaiden tulot, vähennykset ja verot 2024",
            "url": "https://www.vero.fi/tietoa-verohallinnosta/tilastot/henkiloasiakkaiden_tuloverotilastoj/verovuosi-2024/henkiloasiakkaiden-tulot-vahennykset-ja-verot-verovuonna-2024/",
            "use": "Matkakulu- ja kotitalousvähennysten tuloryhmittäisen kohdennuksen ankkurit",
        },
        {
            "id": "tax_admin_vat_2026",
            "label": "Verohallinto: Arvonlisäveroprosentit vuodesta 2026",
            "url": "https://www.vero.fi/yritykset-ja-yhteisot/verot-ja-maksut/arvonlisaverotus/arvonlisaveroprosentit/",
            "use": "Vuoden 2026 yleinen 25,5 prosentin ja alennettu 13,5 prosentin verokanta",
        },
        {
            "id": "tax_admin_transfer_tax_2026",
            "label": "Verohallinto: Varainsiirtovero 2026",
            "url": "https://vero.fi/henkiloasiakkaat/omaisuus/varainsiirtovero/",
            "use": "Asunto-osakkeiden 1,5 prosentin ja kiinteistöjen 3 prosentin vero tapahtumaesimerkeissä",
        },
        {
            "id": "tax_admin_capital_income_tax_2026",
            "label": "Verohallinto: Pääomatulojen veroperusteet 2026",
            "url": "https://www.vero.fi/henkiloasiakkaat/verokortti-ja-veroilmoitus/verokortti/veroprosentti-ja-tuloraja/veroperusteet/",
            "use": "Luovutusvoittoesimerkkien 30 ja 34 prosentin verokannat",
        },
        {
            "id": "statfin_sisu",
            "label": "Tilastokeskus: SISU-mikrosimulointimalli",
            "url": "https://stat.fi/fi/palvelut/palvelut-tutkijoille/sisu-mikrosimulointimalli",
            "use": "Virallisen rekisteripohjaisen aineistosimulaation käyttötarkoitus ja synteettisen mallin luottamusraja",
        },
        {
            "id": "valtiokonttori_budget_actuals",
            "label": "Valtiokonttori: talousarviotalouden toteumat",
            "url": "https://www.tutkihallintoa.fi/valtio/taloustiedot/talousarviotalous-eli-budjettitalous/",
            "use": "Historiallisen vastelaskelman toteutuneet momenttikohtaiset nettokertymät 2007-2025",
        },
        {
            "id": "statfin_central_government_edp_debt",
            "label": "Tilastokeskus: valtionhallinnon EDP-velka",
            "url": "https://pxdata.stat.fi/PXWeb/pxweb/fi/StatFin/StatFin__jyev/11yv.px/",
            "use": "Historiallisen vastelaskelman toteutunut vuoden lopun velkakanta",
        },
        {
            "id": "statfin_national_accounts",
            "label": "Tilastokeskus: kansantalouden vuositilinpito",
            "url": "https://stat.fi/fi/tilasto/ntp",
            "use": "Historiallisen vastelaskelman BKT, hintaindeksi, työlliset ja työtunnit",
        },
        {
            "id": "vn_fiscal_multipliers_2026",
            "label": "Lähdemäki, Puonti & Tervala (2026): Fiscal Multipliers and the Effects of Consolidation Measures in Finland",
            "url": "https://julkaisut.valtioneuvosto.fi/server/api/core/bitstreams/f5cfd8d8-8b91-46f3-9bf3-58436584a5f3/content",
            "use": "Suomen viiden vuoden kertoimet, automaattisten vakauttajien 0,5 semielastisuus ja vaiheistussuositus",
            "parameters": {
                "aggregate_expenditure_5y": 1.0,
                "aggregate_revenue_5y_absolute": 2.3,
                "public_investment_5y": 1.5,
                "consumption_tax_5y_absolute": 1.3,
                "household_income_tax_5y": 0.2,
                "corporate_income_tax_5y_absolute": 1.4,
            },
        },
        {
            "id": "vn_income_tax_employment_2020",
            "label": "Valtioneuvosto 2020: Työpolitiikka ja työllisyysaste",
            "url": "https://julkaisut.valtioneuvosto.fi/items/134b1aaa-c0f5-45a0-82e3-a2dfb7f13f7f",
            "use": "Tutkimuskatsaus työn verotuksen ja työttömyysturvan työllisyysvaikutuksista; peruste varovaiselle tarjontaherkkyydelle",
        },
        {
            "id": "bof_forecast_2026_06",
            "label": "Suomen Pankki: Forecast tables 2025-2028, June 2026",
            "url": "https://www.bofbulletin.fi/article/forecast/2026/forecast-tables-2025-2028-june-2026/",
            "use": "BKT-, työttömyys-, alijäämä-, korko- ja EDP-velkaperusura",
        },
        {
            "id": "oecd_finland_2025",
            "label": "OECD Economic Surveys: Finland 2025",
            "url": "https://www.oecd.org/en/publications/oecd-economic-surveys-finland-2025_985d0555-en.html",
            "use": "ALV-pohjan laajentaminen, pienituloisten kompensaatio, menokartoitukset ja alueverotuksen ehdot",
        },
        {
            "id": "oecd_taxing_wages_2026",
            "label": "OECD Taxing Wages 2026: Finland",
            "url": "https://www.oecd.org/content/dam/oecd/en/publications/reports/2026/04/taxing-wages-2026-country-notes_491a0e97/finland_fd5780b2/9bc058eb-en.pdf",
            "use": "Suomen 42,5 prosentin verokiila keskipalkkaiselle yksinasuvalle vuonna 2025",
        },
        {
            "id": "vatt_transfer_tax",
            "label": "VATT Muistiot 38: Asumisen verotus ja muuttaminen",
            "url": "https://vatt.fi/documents/2956369/3012237/muistiot_38.pdf",
            "use": "Varainsiirtoveron kauppa- ja muuttoliikkuvuusvaikutukset sekä luovutusvoittoveron lukitusriski",
        },
        {
            "id": "vn_rdi_2022",
            "label": "Valtioneuvosto 2022: T&K-tukien kohdentamisen tutkimus",
            "url": "https://valtioneuvosto.fi/en/-/study-increasing-r-d-subsidies-and-tax-incentives-pays-off-but-support-must-be-targeted-carefully",
            "use": "TKI-tukien ulkoisvaikutus ja tarve kohdentaa tuki korkean innovaatiokyvyn yrityksiin",
        },
        {
            "id": "vn_business_subsidies_2015",
            "label": "VATT/Valtioneuvosto 2015: Yritystukien vaikuttavuus",
            "url": "https://valtioneuvosto.fi/en/-/effective-or-unnecessary-a-study-is-published-on-the-effects-of-business-subsidies",
            "use": "Investointi- ja työllisyystukien heikko lisäisyys, mutta TKI-tukien erillinen peruste",
        },
        {
            "id": "oecd_inheritance_2021",
            "label": "OECD 2021: Inheritance Taxation in OECD Countries",
            "url": "https://www.oecd.org/en/publications/inheritance-taxation-in-oecd-countries_e2879a7d-en.html",
            "use": "Perintöveron tehokkuus-, tulonjako- ja veropohjavaikutusten arviointikehikko",
        },
    ]


def validate(analysis: dict[str, Any]) -> None:
    audit = analysis["source_audit"]
    if not audit["snapshot_matches_live_export"]:
        raise ValueError("Local source snapshot no longer matches the audited live export")
    if audit["row_count"] != 687:
        raise ValueError(f"Unexpected source row count: {audit['row_count']}")

    central = analysis["accounting"]["central_budget"]
    consolidated = analysis["accounting"]["consolidated_public_sector"]
    if abs(
        central["nonborrowing_revenue_change_eur"]
        + central["borrowing_change_eur"]
        - central["spending_change_eur"]
    ) > 1.0:
        raise ValueError("Central budget does not reconcile")
    if abs(
        consolidated["nonborrowing_revenue_change_eur"]
        - consolidated["spending_change_eur"]
        - consolidated["direct_balance_improvement_eur"]
    ) > 1.0:
        raise ValueError("Consolidated public-sector bridge does not reconcile")
    if abs(
        consolidated["direct_balance_improvement_eur"] + central["borrowing_change_eur"]
    ) > 1.0:
        raise ValueError("Balance improvement must equal the reduction in borrowing")
    if consolidated["payer_tax_burden_change_eur"] >= 0:
        raise ValueError("Adjusted payer tax burden should decline slightly")
    if abs(
        consolidated["direct_balance_improvement_eur"]
        - consolidated["identified_one_off_revenue_eur"]
        - consolidated["identified_recurring_balance_improvement_eur"]
    ) > 1.0:
        raise ValueError("Recurring and one-off balance effects do not reconcile")

    for scenario_id, scenario in analysis["scenarios"].items():
        if abs(sum(scenario["phase"]) - 1.0) > 1e-12:
            raise ValueError(f"Scenario phases do not sum to one: {scenario_id}")
        for case_id, case in scenario["cases"].items():
            if len(case["yearly"]) != len(YEARS):
                raise ValueError(f"Wrong scenario path length: {scenario_id}/{case_id}")
            if case["summary"]["debt_ratio_difference_2035_pp"] >= 0:
                raise ValueError(f"Debt ratio does not improve: {scenario_id}/{case_id}")

    immediate = analysis["scenarios"]["kerralla"]["cases"]["keskinen"]["summary"]
    safeguarded = analysis["scenarios"]["suojattu"]["cases"]["keskinen"]["summary"]
    if safeguarded["worst_output_effect_pct"] <= immediate["worst_output_effect_pct"]:
        raise ValueError("Safeguarded implementation does not reduce the worst output loss")

    historical = analysis["historical_counterfactual"]
    if historical["meta"]["causal_claim"] is not False:
        raise ValueError("Historical backcast must remain explicitly non-causal")
    if len(historical["cases"]["keskinen"]["yearly"]) != 18:
        raise ValueError("Historical backcast does not cover 2008-2025")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the alternative-budget scenario model.")
    parser.add_argument("--out", type=Path, default=OUT_JSON)
    args = parser.parse_args()

    analysis = build_analysis(load_rows())
    validate(analysis)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(analysis, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    central = analysis["accounting"]["central_budget"]
    public = analysis["accounting"]["consolidated_public_sector"]
    try:
        output_label = args.out.relative_to(ROOT)
    except ValueError:
        output_label = args.out
    print(output_label)
    print(f"  menomuutos                  {central['spending_change_eur'] / 1e9:8.3f} mrd")
    print(f"  ei-lainanottotulot          {central['nonborrowing_revenue_change_eur'] / 1e9:8.3f} mrd")
    print(f"  lainanoton vähennys         {central['borrowing_change_eur'] / 1e9:8.3f} mrd")
    print(f"  maksajien verorasitus       {public['payer_tax_burden_change_eur'] / 1e9:8.3f} mrd")
    for scenario_id, scenario in analysis["scenarios"].items():
        summary = scenario["cases"]["keskinen"]["summary"]
        print(
            f"  {scenario_id:24s} pahin BKT {summary['worst_output_effect_pct']:6.2f} %, "
            f"velkasuhde 2035 {summary['debt_ratio_2035_pct']:6.1f} %"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

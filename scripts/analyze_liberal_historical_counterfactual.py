#!/usr/bin/env python3
"""Backcast Liberal policy against Budjettihaukka's realised 2008-2025 data."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "reference" / "liberaali_vaihtoehtobudjetti"
INPUT_DIR = DATA_DIR / "historical_inputs"
ACTUALS_CSV = INPUT_DIR / "budjettihaukka_actual_by_moment_2007_2025.csv"
MACRO_CSV = INPUT_DIR / "budjettihaukka_macro_2007_2025.csv"
INPUT_MANIFEST = INPUT_DIR / "manifest.json"
POLICY_CSV = DATA_DIR / "vaihtoehtobudjetti_2026_raw.csv"
STRUCTURAL_EVENTS = ROOT / "data" / "reference" / "structural_events.yaml"
ANALYSIS_V2 = DATA_DIR / "analyysi_v2.json"
OUT_JSON = DATA_DIR / "historiallinen_vastelaskelma_v1.json"

START_YEAR = 2008
END_YEAR = 2025
YEARS = list(range(START_YEAR, END_YEAR + 1))
POLICY_REFERENCE_GDP_EUR = 287_468_000_000.0
REGIONAL_TAX_DEVOLUTION_EUR = 2_713_900_000.0
CONSOLIDATED_HOUSEHOLD_INCOME_TAX_CHANGE_EUR = -3_230_350_350.0
AUTOMATIC_STABILISER_SEMI_ELASTICITY = 0.5
SPENDING_BASE_CAP = 0.20
REVENUE_CUT_BASE_CAP = 0.95
NON_TAX_REVENUE_CUT_BASE_CAP = 0.50

IMPLEMENTATION_CUMULATIVE = {
    2008: 0.10,
    2009: 0.30,
    2010: 0.60,
    2011: 1.00,
}
IMPLEMENTATION_INCREMENTS = {2008: 0.10, 2009: 0.20, 2010: 0.30, 2011: 0.40}

RESPONSE_SHAPES = {
    "spending": [0.10, 0.50, 0.20, 0.10, 0.10],
    "consumption_tax": [0.00, 0.15, 0.25, 0.30, 0.30],
    "household_income_tax": [0.00, 0.20, 0.30, 0.30, 0.20],
    "corporate_tax": [0.00, 0.10, 0.25, 0.35, 0.30],
    "wealth_tax": [0.00, 0.25, 0.30, 0.25, 0.20],
    "other_tax": [0.00, 0.25, 0.30, 0.25, 0.20],
}
SUPPLY_RAMP = [0.0, 0.15, 0.35, 0.60, 0.82, 1.0]

CASES = {
    "suotuisa": {
        "label": "Suotuisa",
        "delivery": 1.00,
        "spending_multiplier": 0.70,
        "consumption_tax_multiplier": 0.50,
        "household_income_tax_multiplier": 0.40,
        "corporate_tax_multiplier": 1.00,
        "wealth_tax_multiplier": 0.40,
        "other_tax_multiplier": 0.20,
        "long_run_supply_effect_pct": 1.50,
    },
    "keskinen": {
        "label": "Keskinen",
        "delivery": 0.90,
        "spending_multiplier": 1.00,
        "consumption_tax_multiplier": 1.30,
        "household_income_tax_multiplier": 0.20,
        "corporate_tax_multiplier": 1.40,
        "wealth_tax_multiplier": 0.20,
        "other_tax_multiplier": 0.10,
        "long_run_supply_effect_pct": 0.75,
    },
    "varovainen": {
        "label": "Varovainen ilman tarjontahyötyä",
        "delivery": 0.75,
        "spending_multiplier": 1.40,
        "consumption_tax_multiplier": 2.20,
        "household_income_tax_multiplier": 0.00,
        "corporate_tax_multiplier": 1.80,
        "wealth_tax_multiplier": 0.10,
        "other_tax_multiplier": 0.00,
        "long_run_supply_effect_pct": 0.00,
    },
}

CHANNEL_META = {
    "consolidated_spending": {
        "label": "Konsolidoitu menomuutos",
        "accounting_key": "consolidated_spending_change_eur",
        "response": "spending",
        "response_sign": 1.0,
    },
    "consumption_tax": {
        "label": "Kulutus- ja haittaverot",
        "accounting_key": "consumption_tax_change_eur",
        "response": "consumption_tax",
        "response_sign": -1.0,
    },
    "household_income_tax": {
        "label": "Kotitalouksien tuloverot",
        "accounting_key": "household_income_tax_change_eur",
        "response": "household_income_tax",
        "response_sign": -1.0,
    },
    "corporate_tax": {
        "label": "Yhteisövero",
        "accounting_key": "corporate_tax_change_eur",
        "response": "corporate_tax",
        "response_sign": -1.0,
    },
    "wealth_tax": {
        "label": "Perintö- ja varainsiirtoverot",
        "accounting_key": "wealth_tax_change_eur",
        "response": "wealth_tax",
        "response_sign": -1.0,
    },
    "other_tax": {
        "label": "Muut verot",
        "accounting_key": "other_tax_change_eur",
        "response": "other_tax",
        "response_sign": -1.0,
    },
    "non_tax_revenue": {
        "label": "Muut kuin verotulot",
        "accounting_key": "non_tax_revenue_change_eur",
        "response": None,
        "response_sign": 0.0,
    },
}

CONSUMPTION_TAX_CODES = {
    "11.04.01.",
    "11.04.03.",
    "11.08.01.",
    "11.08.04.",
    "11.08.05.",
    "11.08.07.",
    "11.08.08.",
    "11.08.09.",
    "11.08.10.",
    "11.10.03.",
    "11.10.06.",
    "11.10.07.",
}
WEALTH_TAX_CODES = {"11.01.04.", "11.10.05."}
MAPPING_EXCLUSIONS = {
    "15.03.01.": "borrowing_is_not_policy_revenue",
    "13.03.01.": "identified_one_off_asset_sale",
    "12.32.21.": "emissions_reclassification",
}
MANUAL_STRUCTURAL_BREAKS = {
    (2008, "28.90.30."),
    (2009, "28.90.30."),
    (2010, "28.90.30."),
    (2022, "28.89.31."),
    (2023, "28.89.31."),
    (2023, "28.90.30."),
}


def parse_number(text: str | None) -> float:
    cleaned = (text or "").replace("−", "-").replace("\u00a0", "").replace(" ", "")
    cleaned = re.sub(r"[^0-9,.\-]", "", cleaned).replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def load_actuals() -> tuple[list[dict[str, Any]], dict[tuple[int, str, str], dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    lookup: dict[tuple[int, str, str], dict[str, Any]] = {}
    for raw in load_csv(ACTUALS_CSV):
        value = parse_number(raw["actual_eur"])
        row = {
            **raw,
            "year": int(raw["year"]),
            "actual_eur": value,
            "has_structural_guardrail": raw["has_structural_guardrail"] == "true",
            "is_complete_year": raw["is_complete_year"] == "true",
        }
        rows.append(row)
        lookup[(row["year"], row["fiscal_side"], row["momentti_tunnusp"])] = row
    return rows, lookup


def load_macro() -> dict[int, dict[str, float]]:
    output: dict[int, dict[str, float]] = {}
    for raw in load_csv(MACRO_CSV):
        year = int(raw["year"])
        output[year] = {
            key: float(value)
            for key, value in raw.items()
            if key != "year" and value not in {None, ""}
        }
    return output


def fiscal_bases(
    actual_rows: list[dict[str, Any]],
    actual_lookup: dict[tuple[int, str, str], dict[str, Any]],
) -> dict[int, dict[str, float]]:
    by_year: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in actual_rows:
        by_year[row["year"]].append(row)
    output: dict[int, dict[str, float]] = {}
    for year in range(2007, END_YEAR + 1):
        rows = by_year[year]
        expense = sum(row["actual_eur"] for row in rows if row["fiscal_side"] == "expense")
        revenue_signed = sum(
            row["actual_eur"] for row in rows if row["fiscal_side"] == "revenue"
        )
        tax_total = -sum(
            row["actual_eur"]
            for row in rows
            if row["fiscal_side"] == "revenue"
            and row["momentti_tunnusp"].startswith("11.")
        )
        non_tax = -sum(
            row["actual_eur"]
            for row in rows
            if row["fiscal_side"] == "revenue"
            and row["momentti_tunnusp"].startswith(("12.", "13."))
        )

        def revenue_base(codes: set[str]) -> float:
            return sum(
                abs(actual_lookup.get((year, "revenue", code), {}).get("actual_eur", 0.0))
                for code in codes
            )

        household_income_tax = revenue_base({"11.01.01."})
        corporate_tax = revenue_base({"11.01.02."})
        consumption_tax = revenue_base(CONSUMPTION_TAX_CODES)
        wealth_tax = revenue_base(WEALTH_TAX_CODES)
        other_tax = max(
            tax_total
            - household_income_tax
            - corporate_tax
            - consumption_tax
            - wealth_tax,
            0.0,
        )
        interest = abs(
            actual_lookup.get((year, "expense", "36.01.90."), {}).get("actual_eur", 0.0)
        )
        output[year] = {
            "expense_eur": expense,
            "revenue_eur": -revenue_signed,
            "tax_revenue_eur": tax_total,
            "non_tax_revenue_eur": non_tax,
            "household_income_tax_eur": household_income_tax,
            "corporate_tax_eur": corporate_tax,
            "consumption_tax_eur": consumption_tax,
            "wealth_tax_eur": wealth_tax,
            "other_tax_eur": other_tax,
            "interest_expense_eur": interest,
        }
    return output


def proposal_mappings() -> list[dict[str, Any]]:
    mappings = []
    for row in load_csv(POLICY_CSV):
        if row["momenttitaso"] != "3":
            continue
        code = row["momenttinumerot"]
        baseline = parse_number(row["numero"])
        change = parse_number(row["Leikattavaa momentista"])
        if not change:
            continue
        exclusion = MAPPING_EXCLUSIONS.get(code)
        if code == "11.01.01.":
            change = CONSOLIDATED_HOUSEHOLD_INCOME_TAX_CHANGE_EUR
        elif code == "28.89.31.":
            change += REGIONAL_TAX_DEVOLUTION_EUR
        if exclusion:
            change = 0.0
        rate = change / baseline if baseline else None
        mappings.append(
            {
                "fiscal_side": "expense" if row["tulo/meno"] == "meno" else "revenue",
                "momentti_tunnusp": code,
                "label": row["nimi"],
                "reference_baseline_eur": baseline,
                "reference_change_eur": change,
                "rate": rate,
                "exclusion": exclusion,
                "is_bounded_rate": rate is not None and abs(rate) <= 1.0,
            }
        )
    return mappings


def build_grounding(
    mappings: list[dict[str, Any]],
    actual_lookup: dict[tuple[int, str, str], dict[str, Any]],
) -> dict[str, Any]:
    eligible = [row for row in mappings if row["reference_change_eur"] and row["is_bounded_rate"]]
    denominator = sum(abs(row["reference_change_eur"]) for row in eligible)
    yearly = []
    for year in YEARS:
        mapped_amount = 0.0
        stable_amount = 0.0
        mechanical_direct = 0.0
        stable_mechanical_direct = 0.0
        mapped_count = 0
        stable_count = 0
        for mapping in eligible:
            actual = actual_lookup.get(
                (year, mapping["fiscal_side"], mapping["momentti_tunnusp"])
            )
            if actual is None:
                continue
            mapped_count += 1
            mapped_amount += abs(mapping["reference_change_eur"])
            historical_change = mapping["rate"] * abs(actual["actual_eur"])
            improvement = (
                -historical_change
                if mapping["fiscal_side"] == "expense"
                else historical_change
            )
            mechanical_direct += improvement
            is_structural = actual["has_structural_guardrail"] or (
                year,
                mapping["momentti_tunnusp"],
            ) in MANUAL_STRUCTURAL_BREAKS
            if not is_structural:
                stable_count += 1
                stable_amount += abs(mapping["reference_change_eur"])
                stable_mechanical_direct += improvement
        yearly.append(
            {
                "year": year,
                "mapped_moment_count": mapped_count,
                "stable_moment_count": stable_count,
                "mapped_reference_share_pct": 100 * mapped_amount / denominator,
                "stable_reference_share_pct": 100 * stable_amount / denominator,
                "mechanical_direct_improvement_eur": mechanical_direct,
                "stable_mechanical_direct_improvement_eur": stable_mechanical_direct,
            }
        )
    return {
        "method": (
            "Vertailukelpoisuuskattavuus mittaa, kuinka suuri osa rajatuista vuoden 2026 "
            "momenttimuutoksista löytyy samalta toteumamomentilta ilman kyseisen vuoden "
            "rakennemurrosvartiointia. Mekaanista momenttisummaa ei käytetä velka- tai BKT-tuloksena."
        ),
        "eligible_reference_adjustment_abs_eur": denominator,
        "mapping_count": len(mappings),
        "eligible_mapping_count": len(eligible),
        "yearly": yearly,
    }


def implementation_share(year: int) -> float:
    if year >= 2011:
        return 1.0
    return IMPLEMENTATION_CUMULATIVE[year]


def reference_policy_channels(accounting: dict[str, Any]) -> dict[str, float]:
    macro = accounting["macro_instruments"]
    one_off = accounting["consolidated_public_sector"]["identified_one_off_revenue_eur"]
    channels = {
        channel: float(macro[meta["accounting_key"]])
        for channel, meta in CHANNEL_META.items()
    }
    channels["non_tax_revenue"] -= float(one_off)
    expected = accounting["consolidated_public_sector"][
        "identified_recurring_balance_improvement_eur"
    ]
    observed = -channels["consolidated_spending"] + sum(
        value for channel, value in channels.items() if channel != "consolidated_spending"
    )
    if not math.isclose(observed, expected, abs_tol=1.0):
        raise ValueError("Historical policy channels do not reconcile to the recurring package")
    return channels


def cap_channel(channel: str, value: float, bases: dict[str, float]) -> tuple[float, bool]:
    limits = {
        "consolidated_spending": SPENDING_BASE_CAP * bases["expense_eur"],
        "household_income_tax": REVENUE_CUT_BASE_CAP * bases["household_income_tax_eur"],
        "wealth_tax": REVENUE_CUT_BASE_CAP * bases["wealth_tax_eur"],
        "other_tax": REVENUE_CUT_BASE_CAP * bases["other_tax_eur"],
        "non_tax_revenue": NON_TAX_REVENUE_CUT_BASE_CAP * bases["non_tax_revenue_eur"],
    }
    limit = limits.get(channel)
    if limit is None or value >= 0:
        return value, False
    capped = max(value, -limit)
    return capped, not math.isclose(capped, value, abs_tol=0.01)


def build_policy_levels(
    reference_channels: dict[str, float],
    macro: dict[int, dict[str, float]],
    bases: dict[int, dict[str, float]],
) -> list[dict[str, Any]]:
    output = []
    for year in YEARS:
        gdp = macro[year]["gdp_current_prices_meur"] * 1_000_000
        share = implementation_share(year)
        channels: dict[str, float] = {}
        capped_channels = []
        for channel, reference_value in reference_channels.items():
            full_value = reference_value * gdp / POLICY_REFERENCE_GDP_EUR
            full_value, was_capped = cap_channel(channel, full_value, bases[year])
            channels[channel] = full_value * share
            if was_capped:
                capped_channels.append(channel)
        direct = -channels["consolidated_spending"] + sum(
            value for channel, value in channels.items() if channel != "consolidated_spending"
        )
        output.append(
            {
                "year": year,
                "implementation_pct": 100 * share,
                "channels": channels,
                "direct_balance_improvement_target_eur": direct,
                "direct_balance_improvement_target_pct_gdp": 100 * direct / gdp,
                "capped_channels": capped_channels,
            }
        )
    return output


def add_response(
    demand: dict[int, float],
    impulse_year: int,
    impulse_pct_gdp: float,
    multiplier: float,
    shape: list[float],
    sign: float,
) -> None:
    for lag, weight in enumerate(shape):
        year = impulse_year + lag
        if year in demand:
            demand[year] += sign * impulse_pct_gdp * multiplier * weight


def demand_path(
    policy_levels: list[dict[str, Any]],
    macro: dict[int, dict[str, float]],
    parameters: dict[str, float | str],
) -> dict[int, float]:
    demand = {year: 0.0 for year in YEARS}
    previous = {channel: 0.0 for channel in CHANNEL_META}
    delivery = float(parameters["delivery"])
    for row in policy_levels:
        year = row["year"]
        gdp = macro[year]["gdp_current_prices_meur"] * 1_000_000
        for channel, value in row["channels"].items():
            delivered_level = value * delivery
            impulse = delivered_level - previous[channel]
            previous[channel] = delivered_level
            meta = CHANNEL_META[channel]
            response = meta["response"]
            if response is None:
                continue
            multiplier = float(parameters[f"{response}_multiplier"])
            if response == "spending":
                multiplier *= 0.68
            elif response == "consumption_tax":
                multiplier *= 0.85
            add_response(
                demand,
                year,
                100 * impulse / gdp,
                multiplier,
                RESPONSE_SHAPES[response],
                float(meta["response_sign"]),
            )
    return demand


def supply_path(parameters: dict[str, float | str]) -> dict[int, float]:
    output = {year: 0.0 for year in YEARS}
    target = float(parameters["long_run_supply_effect_pct"])
    for start_year, share in IMPLEMENTATION_INCREMENTS.items():
        for year in YEARS:
            if year < start_year:
                continue
            lag = min(year - start_year, len(SUPPLY_RAMP) - 1)
            output[year] += target * share * SUPPLY_RAMP[lag]
    return output


def run_case(
    case_id: str,
    parameters: dict[str, float | str],
    policy_levels: list[dict[str, Any]],
    macro: dict[int, dict[str, float]],
    bases: dict[int, dict[str, float]],
) -> dict[str, Any]:
    demand = demand_path(policy_levels, macro, parameters)
    supply = supply_path(parameters)
    policy_by_year = {row["year"]: row for row in policy_levels}
    previous_alternative_debt = (
        macro[START_YEAR - 1]["central_government_edp_debt_q4_meur"] * 1_000_000
    )
    rows = []
    for year in YEARS:
        gdp = macro[year]["gdp_current_prices_meur"] * 1_000_000
        actual_debt = macro[year]["central_government_edp_debt_q4_meur"] * 1_000_000
        previous_actual_debt = (
            macro[year - 1]["central_government_edp_debt_q4_meur"] * 1_000_000
        )
        demand_pct = demand[year]
        supply_pct = supply[year]
        output_pct = demand_pct + supply_pct
        output_eur = gdp * output_pct / 100
        direct = (
            policy_by_year[year]["direct_balance_improvement_target_eur"]
            * float(parameters["delivery"])
        )
        cyclical_feedback = AUTOMATIC_STABILISER_SEMI_ELASTICITY * output_eur
        effective_interest_rate = min(
            max(bases[year]["interest_expense_eur"] / previous_actual_debt, 0.0),
            0.08,
        )
        debt_gap = max(previous_actual_debt - previous_alternative_debt, 0.0)
        interest_saving = effective_interest_rate * debt_gap
        net_improvement = direct + cyclical_feedback + interest_saving
        actual_debt_change = actual_debt - previous_actual_debt
        alternative_debt = max(
            previous_alternative_debt + actual_debt_change - net_improvement,
            0.0,
        )
        alternative_gdp = gdp + output_eur
        price_index = macro[year]["gdp_price_index_2015_100"]
        price_index_2025 = macro[2025]["gdp_price_index_2015_100"]
        real_gdp_2025_prices = gdp * price_index_2025 / price_index
        rows.append(
            {
                "year": year,
                "implementation_pct": policy_by_year[year]["implementation_pct"],
                "actual_expense_eur": bases[year]["expense_eur"],
                "actual_revenue_eur": bases[year]["revenue_eur"],
                "policy_spending_change_eur": policy_by_year[year]["channels"][
                    "consolidated_spending"
                ]
                * float(parameters["delivery"]),
                "policy_revenue_change_eur": sum(
                    value
                    for channel, value in policy_by_year[year]["channels"].items()
                    if channel != "consolidated_spending"
                )
                * float(parameters["delivery"]),
                "direct_balance_improvement_eur": direct,
                "demand_effect_pct": demand_pct,
                "supply_effect_pct": supply_pct,
                "total_output_effect_pct": output_pct,
                "total_output_effect_eur": output_eur,
                "total_output_effect_real_2025_eur": real_gdp_2025_prices
                * output_pct
                / 100,
                "cyclical_feedback_eur": cyclical_feedback,
                "effective_interest_rate_pct": 100 * effective_interest_rate,
                "interest_saving_eur": interest_saving,
                "net_balance_improvement_eur": net_improvement,
                "actual_gdp_eur": gdp,
                "alternative_gdp_eur": alternative_gdp,
                "actual_debt_eur": actual_debt,
                "alternative_debt_eur": alternative_debt,
                "actual_debt_pct_gdp": 100 * actual_debt / gdp,
                "alternative_debt_pct_gdp": 100 * alternative_debt / alternative_gdp,
                "actual_employed_persons": macro[year]["employed_persons_thousands"]
                * 1_000,
            }
        )
        previous_alternative_debt = alternative_debt

    final = rows[-1]
    worst = min(rows, key=lambda row: row["total_output_effect_pct"])
    summary = {
        "actual_debt_2025_eur": final["actual_debt_eur"],
        "alternative_debt_2025_eur": final["alternative_debt_eur"],
        "debt_difference_2025_eur": final["alternative_debt_eur"]
        - final["actual_debt_eur"],
        "actual_debt_ratio_2025_pct": final["actual_debt_pct_gdp"],
        "alternative_debt_ratio_2025_pct": final["alternative_debt_pct_gdp"],
        "debt_ratio_difference_2025_pp": final["alternative_debt_pct_gdp"]
        - final["actual_debt_pct_gdp"],
        "actual_debt_change_2007_2025_eur": final["actual_debt_eur"]
        - macro[2007]["central_government_edp_debt_q4_meur"] * 1_000_000,
        "alternative_debt_change_2007_2025_eur": final["alternative_debt_eur"]
        - macro[2007]["central_government_edp_debt_q4_meur"] * 1_000_000,
        "cumulative_direct_balance_improvement_eur": sum(
            row["direct_balance_improvement_eur"] for row in rows
        ),
        "cumulative_interest_saving_eur": sum(row["interest_saving_eur"] for row in rows),
        "cumulative_net_balance_improvement_eur": sum(
            row["net_balance_improvement_eur"] for row in rows
        ),
        "cumulative_output_effect_real_2025_eur": sum(
            row["total_output_effect_real_2025_eur"] for row in rows
        ),
        "average_annual_output_effect_pct": sum(
            row["total_output_effect_pct"] for row in rows
        )
        / len(rows),
        "worst_output_year": worst["year"],
        "worst_output_effect_pct": worst["total_output_effect_pct"],
        "output_effect_2025_pct": final["total_output_effect_pct"],
        "direct_balance_improvement_2025_eur": final["direct_balance_improvement_eur"],
    }
    return {
        "case_id": case_id,
        "label": parameters["label"],
        "parameters": parameters,
        "yearly": rows,
        "summary": summary,
    }


def load_structural_events() -> list[dict[str, Any]]:
    raw = yaml.safe_load(STRUCTURAL_EVENTS.read_text(encoding="utf-8"))
    selected_ids = {
        "finanssikriisi_2009",
        "vos_uudistus_2010",
        "covid_2020",
        "sote_uudistus_2023",
    }
    return [event for event in raw["events"] if event["id"] in selected_ids]


def build_envelope(cases: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for year in YEARS:
        annual = [
            next(row for row in case["yearly"] if row["year"] == year)
            for case in cases.values()
        ]
        central = next(
            row for row in cases["keskinen"]["yearly"] if row["year"] == year
        )
        rows.append(
            {
                "year": year,
                "central_alternative_debt_eur": central["alternative_debt_eur"],
                "low_alternative_debt_eur": min(
                    row["alternative_debt_eur"] for row in annual
                ),
                "high_alternative_debt_eur": max(
                    row["alternative_debt_eur"] for row in annual
                ),
                "central_output_effect_pct": central["total_output_effect_pct"],
                "low_output_effect_pct": min(
                    row["total_output_effect_pct"] for row in annual
                ),
                "high_output_effect_pct": max(
                    row["total_output_effect_pct"] for row in annual
                ),
            }
        )
    return rows


def build_historical_counterfactual(accounting: dict[str, Any]) -> dict[str, Any]:
    actual_rows, actual_lookup = load_actuals()
    macro = load_macro()
    bases = fiscal_bases(actual_rows, actual_lookup)
    mappings = proposal_mappings()
    grounding = build_grounding(mappings, actual_lookup)
    reference_channels = reference_policy_channels(accounting)
    policy_levels = build_policy_levels(reference_channels, macro, bases)
    cases = {
        case_id: run_case(case_id, parameters, policy_levels, macro, bases)
        for case_id, parameters in CASES.items()
    }
    manifest = json.loads(INPUT_MANIFEST.read_text(encoding="utf-8"))
    output = {
        "meta": {
            "dataset_id": "liberaali_historiallinen_vastelaskelma_v1",
            "period": "2008-2025",
            "policy_start_year": START_YEAR,
            "trust_class": "counterfactual_scenario",
            "causal_claim": False,
            "headline_scope": (
                "Suositellun, neljässä vuodessa vaiheistetun ja kasvumenot suojaavan "
                "vuoden 2026 politiikkarakenteen historiallinen vastelaskelma."
            ),
            "price_basis": (
                "Vuosittaiset budjettivirrat ja velka käyvin hinnoin; kumulatiivinen "
                "tuotantovaikutus vuoden 2025 hinnoin."
            ),
        },
        "source_audit": {
            "input_manifest": manifest,
            "actual_file_sha256": file_hash(ACTUALS_CSV),
            "macro_file_sha256": file_hash(MACRO_CSV),
            "policy_file_sha256": file_hash(POLICY_CSV),
            "actual_row_count": len(actual_rows),
            "macro_year_count": len(macro),
            "complete_actual_years": sorted(
                {
                    row["year"]
                    for row in actual_rows
                    if row["is_complete_year"] and row["year"] in YEARS
                }
            ),
        },
        "baseline": {
            "central_government_edp_debt_2007_eur": macro[2007][
                "central_government_edp_debt_q4_meur"
            ]
            * 1_000_000,
            "central_government_edp_debt_2025_eur": macro[2025][
                "central_government_edp_debt_q4_meur"
            ]
            * 1_000_000,
            "gdp_2025_eur": macro[2025]["gdp_current_prices_meur"] * 1_000_000,
        },
        "method": {
            "observed": [
                "Valtiokonttorin talousarviotalouden momenttikohtaiset nettokertymät",
                "Tilastokeskuksen käypähintainen BKT ja BKT:n hintaindeksi",
                "Tilastokeskuksen valtionhallinnon EDP-velka vuoden viimeisellä neljänneksellä",
                "Toteutunut valtionvelan korkomeno momentilta 36.01.90.",
            ],
            "assumed": [
                "Vuoden 2026 toistuva politiikkarakenne skaalautuu vuosittaiseen toteutuneeseen BKT:hen.",
                "Uudistus toteutuu 10, 20, 30 ja 40 prosentin lisäaskelin vuosina 2008-2011.",
                "Muut toteutuneet kriisit ja velan kantaerot säilyvät perusuran mukaisina.",
                "Finanssikertoimet, 0,5 automaattinen vakauttaja ja tarjontavaikutus ovat herkkyysoletuksia.",
                "Velkaero vastaa kumuloitua nettovaikutusta; muut EDP:n kanta-virtakorjaukset oletetaan samoiksi.",
            ],
            "modelled": [
                "Politiikan vuosittainen suora tasapainovaikutus",
                "BKT-poikkeama toteutuneesta urasta",
                "Korkosäästö ja valtionhallinnon vaihtoehtoinen EDP-velka",
            ],
            "not_modelled": [
                "Kotitalouksien historiallinen tulonjako ilman vuosittaisia SISU-rekisteriajoja",
                "Palvelutason, terveyden, koulutuksen tai turvallisuuden hyvinvointivaikutukset",
                "Työllisyysvaikutus, koska tarjontahyödyn jakautumista tuottavuuteen ja työpanokseen ei tunneta",
                "Poliittisen päätöksenteon ja markkinakorkojen endogeeninen reaktio vaihtoehtoiseen velkauraan",
            ],
        },
        "policy_reference": {
            "reference_year": 2026,
            "reference_gdp_eur": POLICY_REFERENCE_GDP_EUR,
            "channels": [
                {
                    "channel": channel,
                    "label": CHANNEL_META[channel]["label"],
                    "reference_change_eur": value,
                    "reference_change_pct_gdp": 100 * value / POLICY_REFERENCE_GDP_EUR,
                }
                for channel, value in reference_channels.items()
            ],
            "recurring_direct_balance_improvement_eur": accounting[
                "consolidated_public_sector"
            ]["identified_recurring_balance_improvement_eur"],
            "implementation_steps_pct": [10, 20, 30, 40],
            "annual_targets": policy_levels,
        },
        "moment_grounding": grounding,
        "cases": cases,
        "envelope": build_envelope(cases),
        "structural_events": load_structural_events(),
        "interpretation_rules": [
            "Toteutunut ura on havainto; vaihtoehtoinen ura on ehdollinen skenaario.",
            "Velkaeroa ei pidä tulkita kausaaliseksi arvioksi yksittäisen puolueen historiallisesta vaikutuksesta.",
            "Vuoden 2026 politiikkakokonaisuus ei ollut sellaisenaan institutionaalisesti mahdollinen vuonna 2008.",
            "Momenttivarmennus kertoo datakattavuudesta, ei toimien oikeudellisesta tai poliittisesta toteutettavuudesta.",
            "Keskiskenaario on raportin vertailupiste; suotuisa ja varovainen rajaavat malliepävarmuutta.",
        ],
        "sources": [
            {
                "label": "Budjettihaukka: analytics_fiscal_yearly_core_v1",
                "url": "https://www.tutkihallintoa.fi/valtio/taloustiedot/talousarviotalous-eli-budjettitalous/",
                "use": "Toteutuneet momenttikohtaiset talousarviotalouden nettokertymät 2007-2025",
            },
            {
                "label": "Tilastokeskus: valtionhallinnon EDP-velka",
                "url": "https://pxdata.stat.fi/PXWeb/pxweb/fi/StatFin/StatFin__jyev/11yv.px/",
                "use": "Valtionhallinnon vuoden viimeisen neljänneksen velkakanta",
            },
            {
                "label": "Tilastokeskus: kansantalouden vuositilinpito",
                "url": "https://stat.fi/fi/tilasto/ntp",
                "use": "BKT, hintaindeksi, työlliset ja työtunnit",
            },
            {
                "label": "Valtioneuvoston selvitys 2026:3: finanssikertoimet Suomessa",
                "url": "https://julkaisut.valtioneuvosto.fi/items/1c9e5bd6-c59c-4998-9160-4361e094f931",
                "use": "Instrumenttikohtaiset kerroinhaarukat ja ajoitus",
            },
        ],
    }
    validate(output)
    return output


def validate(output: dict[str, Any]) -> None:
    audit = output["source_audit"]
    manifest_files = {
        Path(item["path"]).name: item
        for item in audit["input_manifest"]["files"]
    }
    actual_manifest = manifest_files.get(ACTUALS_CSV.name)
    macro_manifest = manifest_files.get(MACRO_CSV.name)
    if not actual_manifest or not macro_manifest:
        raise ValueError("Historical input manifest does not cover both snapshots")
    if audit["actual_row_count"] != actual_manifest["row_count"]:
        raise ValueError("Budjettihaukka actual snapshot row count changed")
    if audit["macro_year_count"] != macro_manifest["row_count"]:
        raise ValueError("Budjettihaukka macro snapshot year count changed")
    if audit["actual_file_sha256"] != actual_manifest["sha256"]:
        raise ValueError("Budjettihaukka actual snapshot hash changed")
    if audit["macro_file_sha256"] != macro_manifest["sha256"]:
        raise ValueError("Budjettihaukka macro snapshot hash changed")
    if audit["complete_actual_years"] != YEARS:
        raise ValueError("Not every counterfactual year has complete realised data")
    if len(output["cases"]) != 3 or len(output["envelope"]) != len(YEARS):
        raise ValueError("Historical sensitivity cases are incomplete")
    for case in output["cases"].values():
        rows = case["yearly"]
        if [row["year"] for row in rows] != YEARS:
            raise ValueError("Historical case has an incomplete year path")
        if rows[-1]["actual_debt_eur"] != 209_131_000_000.0:
            raise ValueError("Observed 2025 central-government EDP debt changed")
        previous_alternative_debt = output["baseline"]["central_government_edp_debt_2007_eur"]
        previous_actual_debt = previous_alternative_debt
        for row in rows:
            if not math.isclose(
                row["net_balance_improvement_eur"],
                row["direct_balance_improvement_eur"]
                + row["cyclical_feedback_eur"]
                + row["interest_saving_eur"],
                abs_tol=0.05,
            ):
                raise ValueError(f"Net fiscal effect does not reconcile in {row['year']}")
            expected_alternative_debt = max(
                previous_alternative_debt
                + row["actual_debt_eur"]
                - previous_actual_debt
                - row["net_balance_improvement_eur"],
                0.0,
            )
            if not math.isclose(
                row["alternative_debt_eur"],
                expected_alternative_debt,
                abs_tol=0.05,
            ):
                raise ValueError(f"Alternative debt does not reconcile in {row['year']}")
            if row["alternative_debt_eur"] < 0:
                raise ValueError("Alternative debt cannot be negative")
            previous_alternative_debt = row["alternative_debt_eur"]
            previous_actual_debt = row["actual_debt_eur"]
    central = output["cases"]["keskinen"]["summary"]
    if not (80_000_000_000 < central["alternative_debt_2025_eur"] < 130_000_000_000):
        raise ValueError("Central historical debt result is outside the reviewed range")
    if central["worst_output_effect_pct"] >= 0:
        raise ValueError("Central case must retain a short-run consolidation cost")
    grounding = output["moment_grounding"]["yearly"]
    if min(row["stable_reference_share_pct"] for row in grounding) < 50:
        raise ValueError("Moment grounding is too weak for the reported historical period")
    if output["meta"]["causal_claim"] is not False:
        raise ValueError("Historical backcast must not claim causal identification")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--analysis", type=Path, default=ANALYSIS_V2)
    parser.add_argument("--out", type=Path, default=OUT_JSON)
    args = parser.parse_args()
    analysis = json.loads(args.analysis.read_text(encoding="utf-8"))
    output = build_historical_counterfactual(analysis["accounting"])
    args.out.write_text(
        json.dumps(output, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    central = output["cases"]["keskinen"]["summary"]
    try:
        output_label = args.out.relative_to(ROOT)
    except ValueError:
        output_label = args.out
    print(output_label)
    print(
        "  keskinen velka 2025 "
        f"{central['alternative_debt_2025_eur'] / 1e9:.1f} mrd. euroa "
        f"({central['debt_difference_2025_eur'] / 1e9:+.1f} mrd. toteutuneesta)"
    )
    print(
        "  kumulatiivinen reaalinen BKT-ero "
        f"{central['cumulative_output_effect_real_2025_eur'] / 1e9:+.1f} mrd. euroa"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

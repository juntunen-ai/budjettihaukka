#!/usr/bin/env python3
"""Build a public-data synthetic household microsimulation.

This is a static incidence model over weighted representative household cells.
It distributes explicitly specified policy totals using public aggregate
margins. It is not the register-based SISU model and cannot replace a FIONA
run for legislative impact assessment.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "reference" / "liberaali_vaihtoehtobudjetti"
INPUT_DIR = DATA_DIR / "microsim_inputs"
RAW_CSV = DATA_DIR / "vaihtoehtobudjetti_2026_raw.csv"
OUT_JSON = DATA_DIR / "microsimulaatio_v1.json"

MODEL_VINTAGE = "2026-08-23"
DECILES = [str(value) for value in range(1, 11)]
LIFE_STAGES = ["10", "11", "12", "21", "22", "23", "31", "32", "4"]

LIFE_STAGE_LABELS = {
    "10": "Yhden hengen talous, alle 35 v",
    "11": "Yhden hengen talous, 35-64 v",
    "12": "Yhden hengen talous, yli 64 v",
    "21": "Lapseton pari, alle 35 v",
    "22": "Lapseton pari, 35-64 v",
    "23": "Lapseton pari, yli 64 v",
    "31": "Kahden huoltajan lapsiperhe",
    "32": "Yhden huoltajan talous",
    "4": "Muu kotitalous",
}

# Mapping from income-statistics life stages to Household Budget Survey types.
CONSUMPTION_TYPE = {
    "10": "1",
    "11": "1",
    "12": "5",
    "21": "2",
    "22": "2",
    "23": "5",
    "31": "4",
    "32": "3",
    "4": "6",
}

TYPE_FACTORS = {
    "work": {
        "10": 0.85,
        "11": 1.00,
        "12": 0.04,
        "21": 1.65,
        "22": 1.85,
        "23": 0.08,
        "31": 1.65,
        "32": 0.82,
        "4": 1.05,
    },
    "pension": {
        "10": 0.01,
        "11": 0.08,
        "12": 1.00,
        "21": 0.01,
        "22": 0.10,
        "23": 1.80,
        "31": 0.02,
        "32": 0.03,
        "4": 0.35,
    },
    "child": {
        "10": 0.0,
        "11": 0.0,
        "12": 0.0,
        "21": 0.0,
        "22": 0.0,
        "23": 0.0,
        "31": 1.0,
        "32": 1.0,
        "4": 0.18,
    },
    "home_credit": {
        "10": 0.35,
        "11": 0.75,
        "12": 0.80,
        "21": 0.75,
        "22": 1.35,
        "23": 1.45,
        "31": 1.20,
        "32": 0.45,
        "4": 0.85,
    },
    "commuting": {
        "10": 0.80,
        "11": 1.00,
        "12": 0.02,
        "21": 1.50,
        "22": 1.75,
        "23": 0.03,
        "31": 1.45,
        "32": 0.78,
        "4": 0.90,
    },
}

# The cases are a non-statistical incidence envelope. Totals from the source
# budget remain fixed; only targeting and the assumed household pass-through
# of indirect taxes vary.
INCIDENCE_CASES = {
    "jakauma_suotuisa": {
        "label": "Pienituloisille suotuisampi kohdentuminen",
        "work_tax_exponent": 0.15,
        "vat_household_share": 0.80,
        "energy_household_share": 0.50,
        "vehicle_relief_household_share": 0.90,
        "excise_household_share": 0.80,
        "compensation_low_income_power": 1.50,
    },
    "keskinen": {
        "label": "Keskinen synteettinen kohdentuminen",
        "work_tax_exponent": 0.35,
        "vat_household_share": 0.90,
        "energy_household_share": 0.70,
        "vehicle_relief_household_share": 0.80,
        "excise_household_share": 0.90,
        "compensation_low_income_power": 1.00,
    },
    "jakauma_rasittava": {
        "label": "Pienituloisille rasittavampi kohdentuminen",
        "work_tax_exponent": 0.65,
        "vat_household_share": 1.00,
        "energy_household_share": 0.90,
        "vehicle_relief_household_share": 0.65,
        "excise_household_share": 1.00,
        "compensation_low_income_power": 0.55,
    },
}

DEDUCTION_AMOUNTS_EUR = {
    "elaketulovahennys": 2_345_000_000.0,
    "elaketulon_lisaveron_muutos": 59_000_000.0,
    "kotitalousvahennys": 360_000_000.0,
    "lapsilisan_verovapaus": 480_000_000.0,
    "yrittajavahennys": 135_000_000.0,
    "muut_vahennykset": 348_000_000.0,
    "ateriaetu": 60_000_000.0,
    "matkakuluvahennys": 902_000_000.0,
}
WORK_INCOME_CREDIT_EUR = 5_100_000_000.0 + sum(DEDUCTION_AMOUNTS_EUR.values())
REGIONAL_INCOME_TAX_EUR = 2_713_900_000.0

VAT_REVENUE_EUR = 4_085_500_000.0
VAT_COMPENSATION_EUR = 359_100_000.0
ENERGY_TAX_EUR = 1_859_000_000.0
CAR_TAX_RELIEF_EUR = 270_000_000.0
VEHICLE_TAX_RELIEF_EUR = 1_016_000_000.0
TOBACCO_TAX_EUR = 100_000_000.0
ALCOHOL_TAX_EUR = 70_000_000.0
SOFT_DRINK_TAX_EUR = 500_000.0

PENSION_MOMENTS = [
    "28.50.15.",
    "28.50.16.",
    "28.50.63.",
    "33.40.50.",
    "33.40.51.",
    "33.40.52.",
]

VAT_CATEGORIES = ["01", "0611", "073", "08391", "0946", "096", "0971", "0972", "111", "112"]
ENERGY_CATEGORIES = ["045", "0722"]


def parse_number(text: str | None) -> float:
    cleaned = (text or "").replace("−", "-").replace("\u00a0", "").replace(" ", "")
    cleaned = re.sub(r"[^0-9,.\-]", "", cleaned).replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def load_budget_rows() -> list[dict[str, str]]:
    with RAW_CSV.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def budget_change(rows: list[dict[str, str]], moment: str) -> float:
    matches = [
        row
        for row in rows
        if row["momenttinumerot"] == moment and row["momenttitaso"] == "3"
    ]
    if len(matches) != 1:
        raise ValueError(f"Expected exactly one level-3 row for {moment}, got {len(matches)}")
    return parse_number(matches[0]["Leikattavaa momentista"])


class JsonStat:
    def __init__(self, payload: dict[str, Any]):
        self.data = payload["response"] if "response" in payload else payload
        self.ids: list[str] = self.data["id"]
        self.sizes: list[int] = self.data["size"]
        self.values: list[float | None] = self.data["value"]
        self.positions: dict[str, dict[str, int]] = {}
        self.labels: dict[str, dict[str, str]] = {}
        for dimension_id in self.ids:
            category = self.data["dimension"][dimension_id]["category"]
            raw_index = category["index"]
            if isinstance(raw_index, list):
                index = {value: offset for offset, value in enumerate(raw_index)}
            else:
                index = {str(value): int(offset) for value, offset in raw_index.items()}
            self.positions[dimension_id] = index
            self.labels[dimension_id] = {
                str(value): str(label) for value, label in category.get("label", {}).items()
            }

    def get(self, **coordinates: str) -> float:
        if set(coordinates) != set(self.ids):
            missing = set(self.ids) - set(coordinates)
            extra = set(coordinates) - set(self.ids)
            raise KeyError(f"Invalid coordinates, missing={missing}, extra={extra}")
        index = 0
        for axis, (dimension_id, size) in enumerate(zip(self.ids, self.sizes)):
            position = self.positions[dimension_id][str(coordinates[dimension_id])]
            stride = math.prod(self.sizes[axis + 1 :])
            index += position * stride
        value = self.values[index]
        return float(value or 0.0)


def load_source(source_id: str) -> tuple[JsonStat, dict[str, Any]]:
    payload = json.loads((INPUT_DIR / f"{source_id}.json").read_text(encoding="utf-8"))
    return JsonStat(payload), payload


def decile_value(dataset: JsonStat, measure: str, decile: str) -> float:
    return dataset.get(
        contentscode=measure,
        timeperiod_y="2024",
        desiilit_2_20120101=decile,
    )


def transfer_value(dataset: JsonStat, measure: str, transfer: str, decile: str) -> float:
    return dataset.get(
        desiilit_2_20120101=decile,
        tulolaji_1_20161118=transfer,
        timeperiod_y="2024",
        contentscode=measure,
    )


def consumption_value(
    dataset: JsonStat,
    category: str,
    group_dimension: str,
    group: str,
) -> float:
    return dataset.get(
        timeperiod_y="2022",
        coicop_46_20231201=category,
        **{group_dimension: group},
        contentscode="kulu_kt_hk_1_2",
    )


def safe_ratio(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def build_cells() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    income, income_meta = load_source("statfin_income_deciles_2024")
    life, life_meta = load_source("statfin_life_stage_deciles_2024")
    socio, socio_meta = load_source("statfin_socioeconomic_deciles_2024")
    transfers, transfer_meta = load_source("statfin_transfer_recipients_2024")
    consumption_q, consumption_q_meta = load_source("statfin_consumption_quintiles_2022")
    consumption_t, consumption_t_meta = load_source("statfin_consumption_household_types_2022")
    type_background, type_background_meta = load_source("statfin_consumption_type_background_2022")
    consumption_background, consumption_background_meta = load_source(
        "statfin_consumption_background_2022"
    )

    categories = [
        "0",
        *VAT_CATEGORIES,
        *ENERGY_CATEGORIES,
        "071",
        "0724",
        "0126",
        "021",
        "023",
    ]
    categories = list(dict.fromkeys(categories))
    national_type_consumption = {
        category: consumption_value(
            consumption_t, category, "elinvaihe_13_20160101", "SSS"
        )
        for category in categories
    }

    cells: list[dict[str, Any]] = []
    decile_totals: dict[str, dict[str, float]] = {}
    for decile in DECILES:
        households = decile_value(income, "asuntok", decile)
        persons = decile_value(income, "tjt-henkiloita", decile)
        life_counts = {
            stage: life.get(
                desiilit_2_20120101=decile,
                elinvaihe_5_20200201=stage,
                contentscode="ykor_sumwgt",
                timeperiod_y="2024",
            )
            for stage in LIFE_STAGES
        }
        life_total = sum(life_counts.values())
        if not life_total:
            raise ValueError(f"No household life-stage observations for decile {decile}")

        employee_persons = socio.get(
            desiilit_2_20120101=decile,
            sosioekon_asema_4_20200214="2",
            contentscode="ykorpop_sumwgt",
            timeperiod_y="2024",
        )
        entrepreneur_persons = socio.get(
            desiilit_2_20120101=decile,
            sosioekon_asema_4_20200214="1",
            contentscode="ykorpop_sumwgt",
            timeperiod_y="2024",
        )
        pensioner_persons = socio.get(
            desiilit_2_20120101=decile,
            sosioekon_asema_4_20200214="7",
            contentscode="ykorpop_sumwgt",
            timeperiod_y="2024",
        )

        quintile = str((int(decile) - 1) // 2 + 1)
        consumption = {}
        for category in categories:
            quintile_value = consumption_value(
                consumption_q, category, "tuloviidennes_2_20140501", quintile
            )
            consumption[category] = quintile_value

        decile_data = {
            "households": households,
            "persons": persons,
            "equivalence_units": decile_value(income, "modoecd_mean", decile),
            "disposable_income": decile_value(income, "kturaha", decile),
            "equivalised_disposable_income": decile_value(
                income, "tjt-ekvikturaha_mean", decile
            ),
            "wage_income": decile_value(income, "tjt-palk", decile),
            "entrepreneur_income": decile_value(income, "tjt-yrtu", decile),
            "property_income": decile_value(income, "tjt-omtu", decile),
            "pension_income": decile_value(income, "tjt-vanel", decile)
            + decile_value(income, "tjt-pelake", decile),
            "earned_income_tax": decile_value(income, "tjt-ltva", decile)
            + decile_value(income, "tjt-lkuve", decile),
            "capital_income_tax": decile_value(income, "tjt-ltvp", decile),
            "employee_rate": safe_ratio(employee_persons, persons),
            "entrepreneur_rate": safe_ratio(entrepreneur_persons, persons),
            "pensioner_rate": safe_ratio(pensioner_persons, persons),
            "housing_benefit_recipients": transfer_value(
                transfers, "asuntok_ehd", "PT9", decile
            ),
            "social_assistance_recipients": transfer_value(
                transfers, "asuntok_ehd", "PT8", decile
            ),
            "basic_unemployment_recipients": transfer_value(
                transfers, "asuntok_ehd", "PT6", decile
            )
            + transfer_value(transfers, "asuntok_ehd", "PT7", decile),
            "earnings_unemployment_recipients": transfer_value(
                transfers, "asuntok_ehd", "MT2", decile
            ),
            "child_allowance_recipients": transfer_value(
                transfers, "asuntok_ehd", "PT16", decile
            ),
        }
        decile_totals[decile] = decile_data

        for stage, source_count in life_counts.items():
            weight = households * source_count / life_total
            consumption_type = CONSUMPTION_TYPE[stage]
            stage_consumption = {}
            for category in categories:
                type_value = consumption_value(
                    consumption_t,
                    category,
                    "elinvaihe_13_20160101",
                    consumption_type,
                )
                type_ratio = safe_ratio(type_value, national_type_consumption[category])
                # The square root tempers noisy small-cell HBS type estimates.
                stage_consumption[category] = consumption[category] * math.sqrt(
                    max(type_ratio, 0.05)
                )

            type_income = type_background.get(
                timeperiod_y="2022",
                elinvaihe_13_20160101=consumption_type,
                contentscode="kaytetmk_2_mean",
            )
            national_type_income = type_background.get(
                timeperiod_y="2022",
                elinvaihe_13_20160101="SSS",
                contentscode="kaytetmk_2_mean",
            )
            income_type_ratio = safe_ratio(type_income, national_type_income)
            cell_income = decile_data["disposable_income"] * math.sqrt(
                max(income_type_ratio, 0.10)
            )

            cell = {
                "cell_id": f"D{decile}-{stage}",
                "decile": int(decile),
                "life_stage": stage,
                "life_stage_label": LIFE_STAGE_LABELS[stage],
                "weight": weight,
                "baseline_disposable_income_eur": cell_income,
                "equivalence_units": decile_data["equivalence_units"],
                "employee_rate": decile_data["employee_rate"],
                "entrepreneur_rate": decile_data["entrepreneur_rate"],
                "pensioner_rate": decile_data["pensioner_rate"],
                "wage_income_eur": decile_data["wage_income"],
                "entrepreneur_income_eur": decile_data["entrepreneur_income"],
                "pension_income_eur": decile_data["pension_income"],
                "earned_income_tax_eur": decile_data["earned_income_tax"],
                "capital_income_tax_eur": decile_data["capital_income_tax"],
                "property_income_eur": decile_data["property_income"],
                "consumption": stage_consumption,
                "recipient_rates": {
                    "housing": safe_ratio(
                        decile_data["housing_benefit_recipients"], households
                    ),
                    "social_assistance": safe_ratio(
                        decile_data["social_assistance_recipients"], households
                    ),
                    "basic_unemployment": safe_ratio(
                        decile_data["basic_unemployment_recipients"], households
                    ),
                    "earnings_unemployment": safe_ratio(
                        decile_data["earnings_unemployment_recipients"], households
                    ),
                    "child_allowance": safe_ratio(
                        decile_data["child_allowance_recipients"], households
                    ),
                },
            }
            cells.append(cell)

    sources = [
        income_meta,
        life_meta,
        socio_meta,
        transfer_meta,
        consumption_q_meta,
        consumption_t_meta,
        type_background_meta,
        consumption_background_meta,
    ]
    source_audit = {
        "sources": [
            {
                "source_id": source["source_id"],
                "description": source["description"],
                "retrieved_at": source["retrieved_at"],
                "landing_page": source["landing_page"],
                "response_sha256": source["response_sha256"],
            }
            for source in sources
        ],
        "income_data_year": 2024,
        "consumption_data_year": 2022,
        "cell_count": len(cells),
        "represented_households": sum(cell["weight"] for cell in cells),
        "official_income_households": sum(
            decile_totals[decile]["households"] for decile in DECILES
        ),
    }
    return cells, source_audit


def sum_categories(cell: dict[str, Any], categories: list[str]) -> float:
    return sum(cell["consumption"].get(category, 0.0) for category in categories)


def allocate(
    cells: list[dict[str, Any]],
    results: dict[str, dict[str, float]],
    instrument_id: str,
    household_resource_change_eur: float,
    score: Callable[[dict[str, Any]], float],
) -> dict[str, Any]:
    scores = [max(float(score(cell)), 0.0) for cell in cells]
    denominator = sum(cell["weight"] * value for cell, value in zip(cells, scores))
    if denominator <= 0:
        raise ValueError(f"Instrument {instrument_id} has no positive allocation score")
    allocated = 0.0
    for cell, value in zip(cells, scores):
        aggregate = household_resource_change_eur * cell["weight"] * value / denominator
        per_household = aggregate / cell["weight"] if cell["weight"] else 0.0
        results[cell["cell_id"]][instrument_id] = per_household
        allocated += aggregate
    if not math.isclose(allocated, household_resource_change_eur, abs_tol=0.05):
        raise AssertionError(f"Allocation error for {instrument_id}: {allocated}")
    return {
        "instrument_id": instrument_id,
        "household_resource_change_eur": household_resource_change_eur,
        "allocated_eur": allocated,
    }


def run_case(
    cells: list[dict[str, Any]],
    budget_rows: list[dict[str, str]],
    case_id: str,
    parameters: dict[str, float | str],
) -> dict[str, Any]:
    results: dict[str, dict[str, float]] = defaultdict(dict)
    allocations: list[dict[str, Any]] = []

    def add(
        instrument_id: str,
        total: float,
        score: Callable[[dict[str, Any]], float],
    ) -> None:
        allocations.append(allocate(cells, results, instrument_id, total, score))

    tax_exponent = float(parameters["work_tax_exponent"])
    add(
        "tyotulovahennys",
        WORK_INCOME_CREDIT_EUR,
        lambda c: (
            c["employee_rate"]
            * TYPE_FACTORS["work"][c["life_stage"]]
            * max(c["earned_income_tax_eur"], 1.0) ** tax_exponent
        ),
    )
    add(
        "aluevero",
        -REGIONAL_INCOME_TAX_EUR,
        lambda c: c["earned_income_tax_eur"] * TYPE_FACTORS["work"][c["life_stage"]],
    )
    add(
        "elaketulovahennys",
        -DEDUCTION_AMOUNTS_EUR["elaketulovahennys"],
        lambda c: c["pension_income_eur"]
        * TYPE_FACTORS["pension"][c["life_stage"]]
        * max(c["pensioner_rate"], 0.02),
    )
    add(
        "elaketulon_lisaveron_muutos",
        -DEDUCTION_AMOUNTS_EUR["elaketulon_lisaveron_muutos"],
        lambda c: c["pension_income_eur"] * TYPE_FACTORS["pension"][c["life_stage"]],
    )
    add(
        "kotitalousvahennys",
        -DEDUCTION_AMOUNTS_EUR["kotitalousvahennys"],
        lambda c: TYPE_FACTORS["home_credit"][c["life_stage"]]
        * (0.18 + (c["decile"] / 10) ** 2.2),
    )
    add(
        "lapsilisan_verovapaus",
        -DEDUCTION_AMOUNTS_EUR["lapsilisan_verovapaus"],
        lambda c: TYPE_FACTORS["child"][c["life_stage"]]
        * c["recipient_rates"]["child_allowance"]
        * (0.05 + safe_ratio(c["earned_income_tax_eur"], c["wage_income_eur"])),
    )
    add(
        "yrittajavahennys",
        -DEDUCTION_AMOUNTS_EUR["yrittajavahennys"],
        lambda c: c["entrepreneur_rate"] * max(c["entrepreneur_income_eur"], 1.0),
    )
    add(
        "muut_vahennykset",
        -DEDUCTION_AMOUNTS_EUR["muut_vahennykset"],
        lambda c: c["earned_income_tax_eur"] + c["capital_income_tax_eur"],
    )
    add(
        "ateriaetu",
        -DEDUCTION_AMOUNTS_EUR["ateriaetu"],
        lambda c: c["employee_rate"]
        * TYPE_FACTORS["work"][c["life_stage"]]
        * max(c["wage_income_eur"], 1.0) ** 0.35,
    )
    add(
        "matkakuluvahennys",
        -DEDUCTION_AMOUNTS_EUR["matkakuluvahennys"],
        lambda c: c["employee_rate"]
        * TYPE_FACTORS["commuting"][c["life_stage"]]
        * math.exp(-((c["decile"] - 7.0) ** 2) / 10.0),
    )

    add(
        "alv_yhtenaistaminen",
        -VAT_REVENUE_EUR * float(parameters["vat_household_share"]),
        lambda c: sum_categories(c, VAT_CATEGORIES),
    )
    compensation_power = float(parameters["compensation_low_income_power"])
    add(
        "alv_kompensaatio",
        VAT_COMPENSATION_EUR,
        lambda c: (
            c["recipient_rates"]["housing"]
            + c["recipient_rates"]["social_assistance"]
            + c["recipient_rates"]["basic_unemployment"]
        )
        * ((11 - c["decile"]) / 10) ** compensation_power,
    )

    add(
        "asumistuen_siirto",
        budget_change(budget_rows, "33.10.54."),
        lambda c: c["recipient_rates"]["housing"]
        * (1.1 if c["life_stage"] in {"31", "32"} else 1.0),
    )
    add(
        "toimeentulotuen_kompensaatio",
        budget_change(budget_rows, "33.10.57."),
        lambda c: c["recipient_rates"]["social_assistance"]
        * (1.2 if c["life_stage"] in {"31", "32"} else 1.0),
    )
    add(
        "perusturvan_kompensaatio",
        budget_change(budget_rows, "33.20.52."),
        lambda c: c["recipient_rates"]["basic_unemployment"],
    )
    add(
        "ansioturvan_leikkaus",
        budget_change(budget_rows, "33.20.50."),
        lambda c: c["recipient_rates"]["earnings_unemployment"]
        * TYPE_FACTORS["work"][c["life_stage"]],
    )
    add(
        "perhe_etuuksien_leikkaus",
        budget_change(budget_rows, "33.10.50."),
        lambda c: TYPE_FACTORS["child"][c["life_stage"]]
        * max(c["recipient_rates"]["child_allowance"], 0.01),
    )
    pension_transfer_cut = sum(budget_change(budget_rows, moment) for moment in PENSION_MOMENTS)
    add(
        "elakemenojen_leikkaukset",
        pension_transfer_cut,
        lambda c: c["pension_income_eur"]
        * TYPE_FACTORS["pension"][c["life_stage"]]
        * max(c["pensioner_rate"], 0.02),
    )

    energy_share = float(parameters["energy_household_share"])
    vehicle_share = float(parameters["vehicle_relief_household_share"])
    excise_share = float(parameters["excise_household_share"])
    add(
        "energiaverot",
        -ENERGY_TAX_EUR * energy_share,
        lambda c: sum_categories(c, ENERGY_CATEGORIES),
    )
    add(
        "autoveron_poisto",
        CAR_TAX_RELIEF_EUR * vehicle_share,
        lambda c: c["consumption"].get("071", 0.0),
    )
    add(
        "ajoneuvoveron_poisto",
        VEHICLE_TAX_RELIEF_EUR * vehicle_share,
        lambda c: c["consumption"].get("0724", 0.0)
        + 0.15 * c["consumption"].get("0722", 0.0),
    )
    add(
        "tupakkavero",
        -TOBACCO_TAX_EUR * excise_share,
        lambda c: c["consumption"].get("023", 0.0),
    )
    add(
        "alkoholivero",
        -ALCOHOL_TAX_EUR * excise_share,
        lambda c: c["consumption"].get("021", 0.0),
    )
    add(
        "virvoitusjuomavero",
        -SOFT_DRINK_TAX_EUR * excise_share,
        lambda c: c["consumption"].get("0126", 0.0),
    )

    cell_rows = []
    for cell in cells:
        instruments = results[cell["cell_id"]]
        change = sum(instruments.values())
        cell_rows.append(
            {
                "cell_id": cell["cell_id"],
                "decile": cell["decile"],
                "life_stage": cell["life_stage"],
                "life_stage_label": cell["life_stage_label"],
                "weight": cell["weight"],
                "baseline_disposable_income_eur": cell["baseline_disposable_income_eur"],
                "change_per_household_eur": change,
                "change_pct_disposable_income": 100
                * safe_ratio(change, cell["baseline_disposable_income_eur"]),
                "instruments": instruments,
            }
        )

    return {
        "case_id": case_id,
        "label": parameters["label"],
        "parameters": parameters,
        "allocations": allocations,
        "cells": cell_rows,
    }


def aggregate_rows(
    cells: list[dict[str, Any]],
    key: Callable[[dict[str, Any]], str | int],
) -> list[dict[str, Any]]:
    groups: dict[str | int, list[dict[str, Any]]] = defaultdict(list)
    for cell in cells:
        groups[key(cell)].append(cell)
    rows = []
    for group_id, selected in groups.items():
        weight = sum(cell["weight"] for cell in selected)
        change = sum(cell["weight"] * cell["change_per_household_eur"] for cell in selected)
        baseline = sum(
            cell["weight"] * cell["baseline_disposable_income_eur"] for cell in selected
        )
        winners = sum(
            cell["weight"] for cell in selected if cell["change_per_household_eur"] > 0
        )
        instruments: dict[str, float] = defaultdict(float)
        for cell in selected:
            for instrument, value in cell["instruments"].items():
                instruments[instrument] += cell["weight"] * value
        rows.append(
            {
                "group_id": group_id,
                "households": weight,
                "baseline_disposable_income_per_household_eur": safe_ratio(
                    baseline, weight
                ),
                "change_eur": change,
                "change_per_household_eur": safe_ratio(change, weight),
                "post_policy_disposable_income_per_household_eur": safe_ratio(
                    baseline + change, weight
                ),
                "change_pct_disposable_income": 100 * safe_ratio(change, baseline),
                "winner_households_pct": 100 * safe_ratio(winners, weight),
                "instrument_change_eur": dict(instruments),
            }
        )
    return sorted(rows, key=lambda row: row["group_id"])


def add_case_summaries(case: dict[str, Any]) -> None:
    case["by_decile"] = aggregate_rows(case["cells"], lambda cell: cell["decile"])
    by_type = aggregate_rows(case["cells"], lambda cell: cell["life_stage"])
    for row in by_type:
        row["label"] = LIFE_STAGE_LABELS[str(row["group_id"])]
    case["by_household_type"] = by_type
    weight = sum(cell["weight"] for cell in case["cells"])
    total = sum(cell["weight"] * cell["change_per_household_eur"] for cell in case["cells"])
    winners = sum(
        cell["weight"] for cell in case["cells"] if cell["change_per_household_eur"] > 0
    )
    losers = sum(
        cell["weight"] for cell in case["cells"] if cell["change_per_household_eur"] < 0
    )
    case["summary"] = {
        "represented_households": weight,
        "modeled_household_resource_change_eur": total,
        "average_change_per_household_eur": safe_ratio(total, weight),
        "winner_households_pct": 100 * safe_ratio(winners, weight),
        "loser_households_pct": 100 * safe_ratio(losers, weight),
        "bottom_40_change_eur": sum(
            row["change_eur"] for row in case["by_decile"] if int(row["group_id"]) <= 4
        ),
    }


def build_envelope(cases: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    by_case = {
        case_id: {int(row["group_id"]): row for row in case["by_decile"]}
        for case_id, case in cases.items()
    }
    central = by_case["keskinen"]
    rows = []
    for decile in range(1, 11):
        values = [by_case[case_id][decile]["change_per_household_eur"] for case_id in cases]
        pct_values = [
            by_case[case_id][decile]["change_pct_disposable_income"] for case_id in cases
        ]
        rows.append(
            {
                "decile": decile,
                "households": central[decile]["households"],
                "baseline_disposable_income_per_household_eur": central[decile][
                    "baseline_disposable_income_per_household_eur"
                ],
                "central_post_policy_disposable_income_per_household_eur": central[
                    decile
                ]["post_policy_disposable_income_per_household_eur"],
                "central_change_per_household_eur": central[decile][
                    "change_per_household_eur"
                ],
                "low_change_per_household_eur": min(values),
                "high_change_per_household_eur": max(values),
                "central_change_pct_disposable_income": central[decile][
                    "change_pct_disposable_income"
                ],
                "low_change_pct_disposable_income": min(pct_values),
                "high_change_pct_disposable_income": max(pct_values),
                "winner_households_pct": central[decile]["winner_households_pct"],
                "instrument_change_eur": central[decile]["instrument_change_eur"],
            }
        )
    return rows


def event_examples() -> list[dict[str, Any]]:
    return [
        {
            "event_id": "asunto_osake_ilman_voittoa",
            "label": "Asunto-osakkeen ostaja, ei veronalaista myyntivoittoa",
            "purchase_price_eur": 300_000.0,
            "taxable_home_sale_gain_eur": 0.0,
            "transfer_tax_saved_eur": 4_500.0,
            "capital_gains_tax_added_eur": 0.0,
            "net_household_change_eur": 4_500.0,
            "assumption": (
                "Bruttoesimerkki: ei huomioi rahoituskuluja, markkinahintojen muutosta "
                "tai mahdollisia siirtymäsääntöjä."
            ),
        },
        {
            "event_id": "asunnon_vaihtaja_100k_voitto",
            "label": "Asunto-osakkeen vaihtaja, 100 000 euron nimellinen myyntivoitto",
            "purchase_price_eur": 300_000.0,
            "taxable_home_sale_gain_eur": 100_000.0,
            "transfer_tax_saved_eur": 4_500.0,
            "capital_gains_tax_added_eur": 32_800.0,
            "net_household_change_eur": -28_300.0,
            "capital_tax_rule": "30 % 30 000 euroon, 34 % ylimenevältä osalta",
            "assumption": (
                "100 000 euroa käsitellään kokonaan veronalaisena luovutusvoittona. "
                "Tappioita, hankintameno-olettamaa tai mahdollisia siirtymäsääntöjä ei huomioida."
            ),
        },
        {
            "event_id": "omakotitalon_vaihtaja_100k_voitto",
            "label": "Omakotitalon vaihtaja, 100 000 euron nimellinen myyntivoitto",
            "purchase_price_eur": 350_000.0,
            "taxable_home_sale_gain_eur": 100_000.0,
            "transfer_tax_saved_eur": 10_500.0,
            "capital_gains_tax_added_eur": 32_800.0,
            "net_household_change_eur": -22_300.0,
            "capital_tax_rule": "30 % 30 000 euroon, 34 % ylimenevältä osalta",
            "assumption": (
                "100 000 euroa käsitellään kokonaan veronalaisena luovutusvoittona. "
                "Tappioita, hankintameno-olettamaa tai mahdollisia siirtymäsääntöjä ei huomioida."
            ),
        },
    ]


def build_microsimulation(
    budget_rows: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    cells, source_audit = build_cells()
    budget_rows = budget_rows or load_budget_rows()
    cases = {
        case_id: run_case(cells, budget_rows, case_id, parameters)
        for case_id, parameters in INCIDENCE_CASES.items()
    }
    for case in cases.values():
        add_case_summaries(case)

    pension_transfer_cut = sum(budget_change(budget_rows, moment) for moment in PENSION_MOMENTS)
    modeled_policy_totals = {
        "gross_work_income_credit_eur": WORK_INCOME_CREDIT_EUR,
        "deductions_removed_eur": sum(DEDUCTION_AMOUNTS_EUR.values()),
        "regional_income_tax_eur": REGIONAL_INCOME_TAX_EUR,
        "net_work_income_package_resource_change_eur": (
            WORK_INCOME_CREDIT_EUR
            - sum(DEDUCTION_AMOUNTS_EUR.values())
            - REGIONAL_INCOME_TAX_EUR
        ),
        "vat_revenue_eur": VAT_REVENUE_EUR,
        "vat_compensation_eur": VAT_COMPENSATION_EUR,
        "housing_benefit_change_eur": budget_change(budget_rows, "33.10.54."),
        "social_assistance_change_eur": budget_change(budget_rows, "33.10.57."),
        "basic_unemployment_change_eur": budget_change(budget_rows, "33.20.52."),
        "earnings_unemployment_change_eur": budget_change(budget_rows, "33.20.50."),
        "family_benefit_change_eur": budget_change(budget_rows, "33.10.50."),
        "pension_payment_change_eur": pension_transfer_cut,
        "energy_tax_revenue_eur": ENERGY_TAX_EUR,
        "car_tax_relief_eur": CAR_TAX_RELIEF_EUR,
        "vehicle_tax_relief_eur": VEHICLE_TAX_RELIEF_EUR,
    }

    output = {
        "meta": {
            "dataset_id": "liberaali_vaihtoehtobudjetti_microsimulaatio_v1",
            "model_vintage": MODEL_VINTAGE,
            "kind": "julkiseen aggregaattidataan kalibroitu synteettinen staattinen kotitalousmikrosimulaatio",
            "trust_class": "suuntaa_antava",
            "official_validation": "SISU/FIONA-aineistosimulointi puuttuu",
            "price_and_time_basis": (
                "Politiikkamuutokset vuoden 2026 euroina; tulot vuoden 2024 tasossa ja "
                "kulutus vuoden 2022 hinnoissa. Euromäärät kalibroidaan politiikan "
                "kokonaissummiin, prosenttivaikutukset ovat suuntaa antavia."
            ),
        },
        "source_audit": source_audit,
        "policy_totals": modeled_policy_totals,
        "cases": cases,
        "decile_envelope": build_envelope(cases),
        "event_examples": event_examples(),
        "coverage": {
            "included": [
                "työtulovähennyksen bruttolisäys sekä poistettujen vähennysten siirto työtulovähennykseen",
                "oletettu hyvinvointialuevero",
                "ALV-kantojen yhtenäistäminen ja 359,1 milj. euron kompensaatio",
                "asumistuen, perusturvan, ansioturvan ja perhe-etuuksien suorat muutokset",
                "lähdetaulukossa yksilöidyt eläkemenojen muutokset",
                "energia-, auto-, ajoneuvo-, tupakka-, alkoholi- ja virvoitusjuomaverot",
            ],
            "excluded_from_population_average": [
                "oman asunnon luovutusvoittovero, varainsiirtovero ja perintovero: tapahtuma- ja varallisuusyhteys puuttuu",
                "hyvinvointialueiden mahdolliset asiakasmaksut ja aluekohtaiset veropäätökset",
                "julkisten palvelujen ja järjestörahoituksen leikkausten luontoismuotoinen hyvinvointivaikutus",
                "yritys- ja yhteisoverojen kohtaanto",
                "käyttäytymis-, työllisyys-, hinta- ja muuttoliikevaikutukset",
            ],
        },
        "interpretation_rules": [
            "Tuloksia ei kutsuta viralliseksi SISU-mikrosimulaatioksi.",
            "Herkkyysväli on oletusskenaarioiden vaihteluväli, ei tilastollinen luottamusväli.",
            "Solujen sisäinen tulohajonta puuttuu, joten köyhyysasteita tai Gini-kerrointa ei raportoida.",
            "Voittajaosuus perustuu 90 edustavan solun painoihin ja on siksi karkea.",
            "Kaikki tulonjakoväitteet on varmennettava SISU/FIONA-ajolla ennen kampanjan lopullisia numeroväitteitä.",
        ],
        "required_sisu_outputs": [
            "muutos käytettävissä oleviin rahatuloihin tulodesiileittäin ja kotitaloustyypeittäin",
            "voittajien ja häviävien osuudet sekä mediaanivaikutus",
            "pienituloisuusaste ja lapsiköyhyysaste",
            "Gini-kertoimen muutos",
            "työllistymisveroasteet ja efektiiviset marginaaliveroasteet",
            "julkisen talouden staattinen vaikutus vero- ja etuuslajeittain",
        ],
    }
    validate(output)
    return output


def validate(output: dict[str, Any]) -> None:
    audit = output["source_audit"]
    if audit["cell_count"] != 90:
        raise ValueError(f"Expected 90 representative cells, got {audit['cell_count']}")
    if not math.isclose(
        audit["represented_households"], audit["official_income_households"], abs_tol=0.5
    ):
        raise ValueError("Household weights do not reproduce the income-statistics total")
    if len(output["decile_envelope"]) != 10:
        raise ValueError("Decile envelope is incomplete")
    for case in output["cases"].values():
        if len(case["cells"]) != 90 or len(case["by_decile"]) != 10:
            raise ValueError("Case aggregation is incomplete")
        for allocation in case["allocations"]:
            if not math.isclose(
                allocation["allocated_eur"],
                allocation["household_resource_change_eur"],
                abs_tol=0.05,
            ):
                raise ValueError(f"Calibration failed for {allocation['instrument_id']}")
    totals = output["policy_totals"]
    if not math.isclose(
        totals["net_work_income_package_resource_change_eur"],
        2_386_100_000.0,
        abs_tol=0.01,
    ):
        raise ValueError("Work-income package does not reconcile")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the synthetic household model.")
    parser.add_argument("--out", type=Path, default=OUT_JSON)
    args = parser.parse_args()
    output = build_microsimulation()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    central = output["cases"]["keskinen"]["summary"]
    print(args.out.relative_to(ROOT))
    print(
        "  kotitalouksien mallinnettu nettomuutos "
        f"{central['modeled_household_resource_change_eur'] / 1e9:.3f} mrd. euroa"
    )
    print(
        "  keskimäärin "
        f"{central['average_change_per_household_eur']:.0f} euroa / kotitalous / vuosi"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

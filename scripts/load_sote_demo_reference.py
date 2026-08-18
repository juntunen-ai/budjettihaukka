#!/usr/bin/env python3
"""Fetch a reproducible Finland 2008–2025 SOTE demo snapshot from Eurostat."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "data" / "reference" / "official_sote_demo_v1.csv"
JSON_OUTPUT = ROOT / "data" / "reference" / "official_sote_demo_v1.json"
BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

SERIES = {
    "tax_burden_pct_gdp": {
        "dataset": "gov_10a_taxag",
        "params": {
            "geo": "FI",
            "sector": "S13",
            "unit": "PC_GDP",
            "na_item": "D2_D5_D91_D61_M_D612_M_D614_M_D995",
        },
        "name_fi": "Verot ja pakolliset sosiaaliturvamaksut",
        "unit": "PERCENT_GDP",
        "theme": "financing",
    },
    "public_health_expenditure_pct_gdp": {
        "dataset": "gov_10a_exp",
        "params": {
            "geo": "FI",
            "sector": "S13",
            "unit": "PC_GDP",
            "cofog99": "GF07",
            "na_item": "TE",
        },
        "name_fi": "Julkiset terveysmenot",
        "unit": "PERCENT_GDP",
        "theme": "financing",
    },
    "life_expectancy_years": {
        "dataset": "demo_mlexpec",
        "params": {"geo": "FI", "sex": "T", "age": "Y_LT1"},
        "name_fi": "Elinajanodote syntymähetkellä",
        "unit": "YEARS",
        "theme": "outcome",
    },
    "healthy_life_years": {
        "dataset": "hlth_hlye",
        "params": {"geo": "FI", "sex": "T", "unit": "YR", "hlth_hle": "HLY_Y0"},
        "name_fi": "Terveet elinvuodet syntymähetkellä",
        "unit": "YEARS",
        "theme": "outcome",
    },
    "unmet_medical_needs_pct": {
        "dataset": "hlth_silc_08",
        "params": {
            "geo": "FI",
            "sex": "T",
            "age": "Y_GE16",
            "unit": "PC",
            "quant_inc": "TOTAL",
            "reason": "TXP_TFAR_WLIST",
        },
        "name_fi": "Hoitotarve jäi täyttymättä hinnan, matkan tai jonon vuoksi",
        "unit": "PERCENT",
        "theme": "access",
    },
    "treatable_mortality_per_100k": {
        "dataset": "hlth_cd_apr",
        "params": {"geo": "FI", "sex": "T", "unit": "RT", "icd10": "TOTAL", "mortalit": "TRT"},
        "name_fi": "Hoidolla vältettävissä oleva kuolleisuus",
        "unit": "PER_100K",
        "theme": "outcome",
    },
    "preventable_mortality_per_100k": {
        "dataset": "hlth_cd_apr",
        "params": {"geo": "FI", "sex": "T", "unit": "RT", "icd10": "TOTAL", "mortalit": "PRVT"},
        "name_fi": "Ennaltaehkäisyllä vältettävissä oleva kuolleisuus",
        "unit": "PER_100K",
        "theme": "outcome",
    },
    "practising_physicians_per_100k": {
        "dataset": "hlth_rs_prs2",
        "params": {"geo": "FI", "unit": "P_HTHAB", "wstatus": "PRACT", "med_spec": "PHYS"},
        "name_fi": "Potilastyötä tekevät lääkärit",
        "unit": "PER_100K",
        "theme": "capacity",
    },
    "doctor_consultations_per_person": {
        "dataset": "hlth_hc_phys2",
        "params": {"geo": "FI", "unit": "NR_HAB", "hlthcare": "CONSULT"},
        "name_fi": "Lääkärikontaktit asukasta kohden",
        "unit": "PER_PERSON",
        "theme": "use",
    },
    "population_65plus_pct": {
        "dataset": "demo_pjanind",
        "params": {"geo": "FI", "indic_de": "PC_Y65_MAX"},
        "name_fi": "65 vuotta täyttäneiden osuus väestöstä",
        "unit": "PERCENT",
        "theme": "demand",
    },
}


def _ordered_categories(document: dict, dimension: str) -> list[str]:
    index = document["dimension"][dimension]["category"]["index"]
    if isinstance(index, dict):
        return [key for key, _ in sorted(index.items(), key=lambda item: item[1])]
    return list(index)


def _observations(document: dict) -> list[tuple[dict[str, str], float, str]]:
    dimensions = document["id"]
    sizes = document["size"]
    categories = [_ordered_categories(document, dimension) for dimension in dimensions]
    statuses = document.get("status", {})
    rows = []
    for flat_index, value in document.get("value", {}).items():
        remainder = int(flat_index)
        coordinates = []
        for size in reversed(sizes):
            coordinates.append(remainder % size)
            remainder //= size
        coordinates.reverse()
        keys = {
            dimension: categories[position][coordinates[position]]
            for position, dimension in enumerate(dimensions)
        }
        rows.append((keys, float(value), statuses.get(flat_index, "")))
    return rows


def main() -> None:
    output_rows: list[dict[str, object]] = []
    for metric_id, spec in SERIES.items():
        response = requests.get(
            f"{BASE_URL}/{spec['dataset']}",
            params=spec["params"],
            timeout=60,
        )
        response.raise_for_status()
        document = response.json()
        source_url = response.url
        metric_rows = []
        for dimensions, value, status in _observations(document):
            year = int(dimensions["time"])
            if 2008 <= year <= 2025:
                metric_rows.append(
                    {
                        "metric_id": metric_id,
                        "metric_name_fi": spec["name_fi"],
                        "theme": spec["theme"],
                        "year": year,
                        "value": value,
                        "unit": spec["unit"],
                        "status": status,
                        "source_id": f"eurostat_{spec['dataset']}",
                        "source_url": source_url,
                        "source_updated": document.get("updated", ""),
                    }
                )
        if not metric_rows:
            raise RuntimeError(f"No 2008–2025 observations for {metric_id}")
        latest_year = max(int(row["year"]) for row in metric_rows)
        for row in metric_rows:
            row["is_latest_available"] = str(row["year"] == latest_year).lower()
        output_rows.extend(metric_rows)

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "metric_id",
        "metric_name_fi",
        "theme",
        "year",
        "value",
        "unit",
        "status",
        "is_latest_available",
        "source_id",
        "source_url",
        "source_updated",
    ]
    with OUTPUT.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sorted(output_rows, key=lambda row: (str(row["metric_id"]), int(row["year"]))))
    ordered_rows = sorted(output_rows, key=lambda row: (str(row["metric_id"]), int(row["year"])))
    JSON_OUTPUT.write_text(
        json.dumps(ordered_rows, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote {len(output_rows)} observations to {OUTPUT} and {JSON_OUTPUT}")


if __name__ == "__main__":
    main()

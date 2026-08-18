#!/usr/bin/env python3
"""Build the 2020–2025 primary-care service-value pilot.

The output is deliberately snapshot-first.  It combines THL Sotkanet
observations reported on the current wellbeing-area geography with a StatFin
public-service deflator, writes the source observations, and then builds one
wellbeing-area/year panel.  Missing observations are kept missing: in
particular, no survey years are interpolated and no 2025 cost is invented.
"""

from __future__ import annotations

import csv
import json
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parents[1]
REFERENCE_DIR = ROOT / "data" / "reference"
SOURCE_OUTPUT = REFERENCE_DIR / "official_primary_care_value_v1.csv"
PANEL_OUTPUT = REFERENCE_DIR / "primary_care_value_panel_v1.csv"
JSON_OUTPUT = REFERENCE_DIR / "primary_care_value_panel_v1.json"
SOURCES_OUTPUT = REFERENCE_DIR / "primary_care_value_sources_v1.json"
RISK_OUTPUT = REFERENCE_DIR / "primary_care_steering_risk_v1.csv"
RISK_JSON_OUTPUT = REFERENCE_DIR / "primary_care_steering_risk_v1.json"

YEARS = tuple(range(2020, 2026))
SOTKANET_JSON_URL = "https://sotkanet.fi/rest/1.1/json"
SOTKANET_REGIONS_URL = "https://sotkanet.fi/rest/1.1/regions"
STATFIN_DEFLATOR_URL = "https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/jmhi/11m2.px"
FETCHED_AT = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


INDICATORS: dict[int, dict[str, str]] = {
    3764: {
        "metric_id": "primary_care_inpatient_cost_eur_per_resident",
        "metric_name_fi": "Perusterveydenhuollon vuodeosastohoidon nettokäyttökustannukset",
        "unit": "EUR_PER_RESIDENT",
        "dimension": "cost_component",
        "direction": "lower_is_less_input",
    },
    3766: {
        "metric_id": "primary_care_outpatient_cost_eur_per_resident",
        "metric_name_fi": "Perusterveydenhuollon avohoidon nettokäyttökustannukset",
        "unit": "EUR_PER_RESIDENT",
        "dimension": "cost_component",
        "direction": "lower_is_less_input",
    },
    4604: {
        "metric_id": "public_health_staff_per_10000",
        "metric_name_fi": "Julkisen terveydenhuollon henkilöstö yhteensä",
        "unit": "PER_10000_RESIDENTS",
        "dimension": "workforce_context",
        "direction": "context_dependent",
    },
    1080: {
        "metric_id": "primary_care_doctor_visits_per_1000",
        "metric_name_fi": "Perusterveydenhuollon avohoidon lääkärikäynnit, kaikki yhteystavat",
        "unit": "PER_1000_RESIDENTS",
        "dimension": "output",
        "direction": "context_dependent",
    },
    6411: {
        "metric_id": "primary_care_wait_over_7d_pct",
        "metric_name_fi": "Kiireettömän lääkärikäynnin odotusaika yli 7 päivää",
        "unit": "PERCENT_OF_REALIZED_VISITS",
        "dimension": "timely_access",
        "direction": "lower_is_better",
    },
    5502: {
        "metric_id": "primary_care_doctor_continuity_coci",
        "metric_name_fi": "Hoidon jatkuvuus lääkärikäynneillä (COCI)",
        "unit": "INDEX_0_TO_1",
        "dimension": "continuity",
        "direction": "higher_is_better",
    },
    5186: {
        "metric_id": "experienced_fast_access_pct",
        "metric_name_fi": "Hoitopaikkaan riittävän nopeasti yhteyden saaneet",
        "unit": "PERCENT",
        "dimension": "experienced_access",
        "direction": "higher_is_better",
    },
    4333: {
        "metric_id": "self_rated_health_mediocre_or_worse_pct",
        "metric_name_fi": "Terveytensä keskitasoiseksi tai huonommaksi kokevat",
        "unit": "PERCENT",
        "dimension": "experienced_health",
        "direction": "lower_is_better",
    },
    4909: {
        "metric_id": "experienced_insufficient_doctor_services_pct",
        "metric_name_fi": "Tarpeeseen nähden riittämättömästi lääkäripalveluja saaneet",
        "unit": "PERCENT_OF_PEOPLE_NEEDING_SERVICES",
        "dimension": "unmet_need",
        "direction": "lower_is_better",
    },
    5081: {
        "metric_id": "primary_care_emergency_visits_per_1000",
        "metric_name_fi": "Päivystyskäynnit perusterveydenhuollossa, mukaan lukien yhteispäivystys",
        "unit": "PER_1000_RESIDENTS",
        "dimension": "care_path_spillover",
        "direction": "lower_is_better_with_need_adjustment",
    },
    5587: {
        "metric_id": "avoidable_emergency_hospitalizations_per_100000",
        "metric_name_fi": "Avohoidon keinoin vältettävissä olevat päivystykselliset sairaalahoitojaksot",
        "unit": "PER_100000_RESIDENTS_AGE_25_PLUS",
        "dimension": "whole_chain_outcome_guardrail",
        "direction": "lower_is_better",
    },
}

PANEL_METRICS = (
    "primary_care_cost_nominal_eur_per_resident",
    "public_health_staff_per_10000",
    "primary_care_doctor_visits_per_1000",
    "primary_care_wait_over_7d_pct",
    "primary_care_doctor_continuity_coci",
    "experienced_fast_access_pct",
    "self_rated_health_mediocre_or_worse_pct",
    "experienced_insufficient_doctor_services_pct",
    "primary_care_emergency_visits_per_1000",
    "avoidable_emergency_hospitalizations_per_100000",
)

QUALITY_BALANCE_COMPONENTS = (
    "primary_care_doctor_continuity_coci",
    "experienced_fast_access_pct",
    "experienced_sufficient_doctor_services_pct",
    "self_rated_healthy_pct",
)

PLANNED_GUARDRAILS = {
    "care_request_count": "All requests for care, including requests that do not lead to an appointment",
    "no_appointment_share_pct": "Share of assessed needs where no appointment or other resolution was provided",
    "interrupted_care_path_share_pct": "Share of started care paths ending without a documented resolution",
    "median_wait_days": "Median wait from needs assessment to first care event",
    "whole_chain_cost_real_eur_per_resident": (
        "Risk-adjusted total cost across primary care, emergency care and specialised care"
    ),
}

RISK_THRESHOLDS = {
    "activity_index_increase": 3.0,
    "quality_index_decrease": -2.0,
    "wait_improvement_pct_points": -3.0,
    "primary_care_real_cost_decrease_eur": -25.0,
    "emergency_visits_increase_per_1000": 25.0,
    "avoidable_hospitalizations_increase_per_100000": 50.0,
    "continuity_index_decrease": -2.5,
}


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _selected_regions(session: requests.Session) -> dict[int, dict[str, str]]:
    response = session.get(SOTKANET_REGIONS_URL, timeout=60)
    response.raise_for_status()
    result: dict[int, dict[str, str]] = {}
    for row in response.json():
        category = row.get("category")
        if category not in {"MAA", "HYVINVOINTIALUE"}:
            continue
        result[int(row["id"])] = {
            "region_code": "FI" if category == "MAA" else f"HVA-{row['code']}",
            "region_name_fi": row["title"]["fi"],
            "region_type": "country" if category == "MAA" else "wellbeing_area",
            "official_uri": row.get("uri") or "",
        }
    if len(result) != 24:
        raise RuntimeError(f"Expected Finland and 23 wellbeing areas, received {len(result)}")
    return result


def _fetch_sotkanet(
    session: requests.Session,
    indicator_id: int,
    spec: dict[str, str],
    regions: dict[int, dict[str, str]],
) -> list[dict[str, Any]]:
    params: list[tuple[str, str | int]] = [("indicator", indicator_id), ("genders", "total")]
    params.extend(("years", year) for year in YEARS)
    response = session.get(SOTKANET_JSON_URL, params=params, timeout=90)
    response.raise_for_status()
    rows: list[dict[str, Any]] = []
    for item in response.json():
        region_id = int(item["region"])
        if region_id not in regions:
            continue
        year = int(item["year"])
        if year not in YEARS:
            continue
        region = regions[region_id]
        rows.append(
            {
                "metric_id": spec["metric_id"],
                "metric_name_fi": spec["metric_name_fi"],
                "dimension": spec["dimension"],
                "year": year,
                "region_code": region["region_code"],
                "region_name_fi": region["region_name_fi"],
                "region_type": region["region_type"],
                "value": float(item["value"]),
                "absolute_value": item.get("absValue", ""),
                "unit": spec["unit"],
                "direction": spec["direction"],
                "source_indicator_id": indicator_id,
                "source_id": f"thl_sotkanet_indicator_{indicator_id}",
                "source_url": f"https://sotkanet.fi/sotkanet/fi/metadata/indicators/{indicator_id}",
                "vintage_date": FETCHED_AT[:10],
            }
        )
    return rows


def _deflator(session: requests.Session) -> dict[int, float]:
    query = {
        "query": [
            {
                "code": "timeperiod_y",
                "selection": {"filter": "item", "values": [str(year) for year in YEARS]},
            },
            {
                "code": "kuntatyyppi_1_20140101",
                "selection": {"filter": "item", "values": ["1"]},
            },
            {
                "code": "jmhi_7_20190101",
                "selection": {"filter": "item", "values": ["SSS"]},
            },
            {
                "code": "contentscode",
                "selection": {"filter": "item", "values": ["pojo_kunteht"]},
            },
        ],
        "response": {"format": "json-stat2"},
    }
    response = session.post(STATFIN_DEFLATOR_URL, json=query, timeout=90)
    response.raise_for_status()
    document = response.json()
    ordered_years = sorted(
        document["dimension"]["timeperiod_y"]["category"]["index"],
        key=document["dimension"]["timeperiod_y"]["category"]["index"].get,
    )
    return {
        int(year): float(value)
        for year, value in zip(ordered_years, document["value"], strict=True)
        if value is not None
    }


def _source_rows(session: requests.Session, regions: dict[int, dict[str, str]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for indicator_id, spec in INDICATORS.items():
        rows.extend(_fetch_sotkanet(session, indicator_id, spec, regions))

    cost_components: dict[tuple[str, int], dict[str, dict[str, Any]]] = {}
    for row in rows:
        if row["metric_id"] not in {
            "primary_care_inpatient_cost_eur_per_resident",
            "primary_care_outpatient_cost_eur_per_resident",
        }:
            continue
        cost_components.setdefault((str(row["region_code"]), int(row["year"])), {})[
            str(row["metric_id"])
        ] = row

    derived_rows: list[dict[str, Any]] = []
    for (_region_code, _year), parts in cost_components.items():
        if len(parts) != 2:
            continue
        inpatient = parts["primary_care_inpatient_cost_eur_per_resident"]
        outpatient = parts["primary_care_outpatient_cost_eur_per_resident"]
        derived_rows.append(
            {
                **outpatient,
                "metric_id": "primary_care_cost_nominal_eur_per_resident",
                "metric_name_fi": "Perusterveydenhuollon nettokäyttökustannukset",
                "dimension": "cost",
                "value": round(float(inpatient["value"]) + float(outpatient["value"]), 4),
                "absolute_value": "",
                "direction": "lower_is_less_input",
                "source_indicator_id": "3764+3766",
                "source_id": "thl_sotkanet_indicators_3764_3766",
                "source_url": (
                    "https://sotkanet.fi/sotkanet/fi/metadata/indicators/3764;"
                    "https://sotkanet.fi/sotkanet/fi/metadata/indicators/3766"
                ),
            }
        )
    return sorted(
        rows + derived_rows,
        key=lambda row: (str(row["metric_id"]), str(row["region_code"]), int(row["year"])),
    )


def _governance_period(year: int) -> str:
    return "municipalities_aggregated_to_current_hva" if year <= 2022 else "wellbeing_areas_reported"


def _round(value: float | None, digits: int = 4) -> float | str:
    return "" if value is None else round(value, digits)


def _panel(
    source_rows: list[dict[str, Any]],
    regions: dict[int, dict[str, str]],
    deflator: dict[int, float],
) -> list[dict[str, Any]]:
    observations = {
        (str(row["region_code"]), int(row["year"]), str(row["metric_id"])): float(row["value"])
        for row in source_rows
    }
    unique_regions = sorted(
        {row["region_code"]: row for row in regions.values()}.values(),
        key=lambda row: (row["region_type"] != "country", row["region_code"]),
    )
    reference_deflator = deflator.get(2024)
    if reference_deflator is None:
        raise RuntimeError("StatFin deflator is missing the 2024 reference year")

    panel: list[dict[str, Any]] = []
    for region in unique_regions:
        code = region["region_code"]
        for year in YEARS:
            row: dict[str, Any] = {
                "region_code": code,
                "region_name_fi": region["region_name_fi"],
                "region_type": region["region_type"],
                "year": year,
                "governance_period": _governance_period(year),
                "reform_break": "true" if year == 2023 else "false",
                "official_uri": region["official_uri"],
            }
            for metric_id in PANEL_METRICS:
                row[metric_id] = _round(observations.get((code, year, metric_id)))

            nominal = row["primary_care_cost_nominal_eur_per_resident"]
            year_deflator = deflator.get(year)
            row["public_service_deflator_2015_100"] = _round(year_deflator, 1)
            row["primary_care_cost_real_2024_eur_per_resident"] = ""
            if nominal != "" and year_deflator is not None:
                row["primary_care_cost_real_2024_eur_per_resident"] = round(
                    float(nominal) * reference_deflator / year_deflator,
                    2,
                )

            poor_health = row["self_rated_health_mediocre_or_worse_pct"]
            row["self_rated_healthy_pct"] = "" if poor_health == "" else round(100 - float(poor_health), 4)
            insufficient_services = row["experienced_insufficient_doctor_services_pct"]
            row["experienced_sufficient_doctor_services_pct"] = (
                "" if insufficient_services == "" else round(100 - float(insufficient_services), 4)
            )
            real_cost = row["primary_care_cost_real_2024_eur_per_resident"]
            visits = row["primary_care_doctor_visits_per_1000"]
            row["activity_per_real_cost"] = ""
            if real_cost != "" and visits != "" and float(real_cost) > 0:
                row["activity_per_real_cost"] = round(float(visits) / float(real_cost), 6)
            for metric_id in PLANNED_GUARDRAILS:
                row[metric_id] = ""

            available = sum(row[metric_id] != "" for metric_id in PANEL_METRICS)
            row["available_metric_count"] = available
            row["requested_metric_count"] = len(PANEL_METRICS)
            row["data_coverage_pct"] = round(available / len(PANEL_METRICS) * 100, 1)
            row["coverage_status"] = (
                "complete" if available == len(PANEL_METRICS) else "partial" if available else "unavailable"
            )
            panel.append(row)

    national_2020 = next(
        row for row in panel if row["region_code"] == "FI" and int(row["year"]) == 2020
    )
    baselines = {metric: national_2020[metric] for metric in QUALITY_BALANCE_COMPONENTS}
    if any(value == "" or float(value) <= 0 for value in baselines.values()):
        raise RuntimeError(f"National 2020 quality-balance baseline is incomplete: {baselines}")

    activity_baseline = national_2020["activity_per_real_cost"]
    if activity_baseline == "" or float(activity_baseline) <= 0:
        raise RuntimeError("National 2020 activity-per-real-cost baseline is incomplete")

    component_columns = {
        "primary_care_doctor_continuity_coci": "continuity_index_fi_2020_100",
        "experienced_fast_access_pct": "experienced_access_index_fi_2020_100",
        "experienced_sufficient_doctor_services_pct": (
            "experienced_service_adequacy_index_fi_2020_100"
        ),
        "self_rated_healthy_pct": "experienced_health_index_fi_2020_100",
    }
    for row in panel:
        activity = row["activity_per_real_cost"]
        row["activity_per_real_cost_index_fi_2020_100"] = (
            ""
            if activity == ""
            else round(float(activity) / float(activity_baseline) * 100, 2)
        )
        component_values: list[float] = []
        for metric, index_column in component_columns.items():
            value = row[metric]
            if value == "" or float(value) <= 0:
                row[index_column] = ""
                continue
            index_value = float(value) / float(baselines[metric]) * 100
            row[index_column] = round(index_value, 2)
            component_values.append(index_value)
        row["quality_balance_component_count"] = len(component_values)
        row["quality_balance_index"] = ""
        row["quality_balance_index_status"] = "not_published_incomplete_quality_core"
        if len(component_values) == len(QUALITY_BALANCE_COMPONENTS):
            row["quality_balance_index"] = round(
                math.exp(sum(math.log(value) for value in component_values) / len(component_values)),
                2,
            )
            row["quality_balance_index_status"] = "published_equal_weight_quality_core"
        row["quality_balance_index_basis"] = "Finland 2020 = 100; equal-weight geometric mean"

        # A volume-to-cost ratio is not a value or efficiency measure without
        # resolved-need, whole-chain cost and safety guardrails.
        row["service_value_index"] = ""
        row["service_value_index_status"] = "retired_requires_whole_chain_value_data"
        row["service_value_index_basis"] = (
            "Not published until resolved need, whole-chain cost and safety are available"
        )
    return panel


def _number(row: dict[str, Any], key: str) -> float | None:
    value = row.get(key, "")
    return None if value == "" else float(value)


def _delta(before: dict[str, Any], after: dict[str, Any], key: str) -> float | None:
    first, second = _number(before, key), _number(after, key)
    return None if first is None or second is None else second - first


def _steering_risks(panel: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Detect contradictory steering signals, never intent or causality.

    The comparison uses 2022 and 2024 because they are the closest complete
    population-survey years on opposite sides of the 2023 reform boundary.
    """
    by_region_year = {
        (str(row["region_code"]), int(row["year"])): row
        for row in panel
        if row["region_type"] == "wellbeing_area"
    }
    risks: list[dict[str, Any]] = []
    for region_code in sorted({key[0] for key in by_region_year}):
        before = by_region_year[(region_code, 2022)]
        after = by_region_year[(region_code, 2024)]
        deltas = {
            "activity_per_real_cost_delta": _delta(
                before, after, "activity_per_real_cost_index_fi_2020_100"
            ),
            "continuity_delta": _delta(before, after, "continuity_index_fi_2020_100"),
            "experienced_access_delta": _delta(
                before, after, "experienced_access_index_fi_2020_100"
            ),
            "service_adequacy_delta": _delta(
                before, after, "experienced_service_adequacy_index_fi_2020_100"
            ),
            "quality_balance_delta": _delta(before, after, "quality_balance_index"),
            "wait_over_7d_delta_pct_points": _delta(
                before, after, "primary_care_wait_over_7d_pct"
            ),
            "primary_care_real_cost_delta_eur": _delta(
                before, after, "primary_care_cost_real_2024_eur_per_resident"
            ),
            "emergency_visits_delta_per_1000": _delta(
                before, after, "primary_care_emergency_visits_per_1000"
            ),
            "avoidable_hospitalizations_delta_per_100000": _delta(
                before, after, "avoidable_emergency_hospitalizations_per_100000"
            ),
        }

        flags: list[str] = []
        evaluated: list[str] = []
        quality_deltas = [
            deltas["continuity_delta"],
            deltas["experienced_access_delta"],
            deltas["service_adequacy_delta"],
        ]
        activity_data_comparable = region_code not in {"HVA-04", "HVA-90"}
        if activity_data_comparable and deltas["activity_per_real_cost_delta"] is not None and any(
            value is not None for value in quality_deltas
        ):
            evaluated.append("activity_quality_conflict")
            if deltas["activity_per_real_cost_delta"] >= RISK_THRESHOLDS[
                "activity_index_increase"
            ] and any(
                value is not None and value <= RISK_THRESHOLDS["quality_index_decrease"]
                for value in quality_deltas
            ):
                flags.append("activity_quality_conflict")

        population_access_deltas = [
            deltas["experienced_access_delta"],
            deltas["service_adequacy_delta"],
        ]
        if deltas["wait_over_7d_delta_pct_points"] is not None and any(
            value is not None for value in population_access_deltas
        ):
            evaluated.append("wait_denominator_conflict")
            if deltas["wait_over_7d_delta_pct_points"] <= RISK_THRESHOLDS[
                "wait_improvement_pct_points"
            ] and any(
                value is not None and value <= RISK_THRESHOLDS["quality_index_decrease"]
                for value in population_access_deltas
            ):
                flags.append("wait_denominator_conflict")

        spillover_deltas = [
            deltas["emergency_visits_delta_per_1000"],
            deltas["avoidable_hospitalizations_delta_per_100000"],
        ]
        if deltas["primary_care_real_cost_delta_eur"] is not None and any(
            value is not None for value in spillover_deltas
        ):
            evaluated.append("cost_shift_signal")
            emergency_delta, avoidable_delta = spillover_deltas
            spillover_worsened = (
                emergency_delta is not None
                and emergency_delta >= RISK_THRESHOLDS["emergency_visits_increase_per_1000"]
            ) or (
                avoidable_delta is not None
                and avoidable_delta
                >= RISK_THRESHOLDS["avoidable_hospitalizations_increase_per_100000"]
            )
            if deltas["primary_care_real_cost_delta_eur"] <= RISK_THRESHOLDS[
                "primary_care_real_cost_decrease_eur"
            ] and spillover_worsened:
                flags.append("cost_shift_signal")

        if deltas["continuity_delta"] is not None:
            evaluated.append("continuity_countermeasure_breach")
            if deltas["continuity_delta"] <= RISK_THRESHOLDS["continuity_index_decrease"]:
                flags.append("continuity_countermeasure_breach")

        if len(evaluated) < 3:
            risk_level = "insufficient_data"
        elif len(flags) >= 2:
            risk_level = "high_signal"
        elif flags:
            risk_level = "watch_signal"
        else:
            risk_level = "no_observed_signal"

        quality_notes = ["2023_governance_and_recording_break"]
        if region_code in {"HVA-04", "HVA-90"}:
            quality_notes.append("known_2022_visit_recording_deficit")
        if region_code == "HVA-91":
            quality_notes.append("avoidable_hospitalization_metric_unavailable")

        risks.append(
            {
                "region_code": region_code,
                "region_name_fi": after["region_name_fi"],
                "comparison_period": "2022-2024",
                "steering_risk_level": risk_level,
                "triggered_rule_count": len(flags),
                "evaluated_rule_count": len(evaluated),
                "triggered_rules": "|".join(flags),
                "evaluated_rules": "|".join(evaluated),
                "data_quality_notes": "|".join(quality_notes),
                "missing_guardrails": "|".join(PLANNED_GUARDRAILS),
                **{key: _round(value, 2) for key, value in deltas.items()},
                "quality_balance_2022": before["quality_balance_index"],
                "quality_balance_2024": after["quality_balance_index"],
                "activity_per_real_cost_2022": before[
                    "activity_per_real_cost_index_fi_2020_100"
                ],
                "activity_per_real_cost_2024": after[
                    "activity_per_real_cost_index_fi_2020_100"
                ],
            }
        )
    return risks


def _sources_document(deflator: dict[int, float]) -> dict[str, Any]:
    return {
        "dataset_id": "primary_care_value_panel_v1",
        "created_at": FETCHED_AT,
        "period": {"from": 2020, "to": 2025},
        "geography": (
            "Current wellbeing-area boundaries as published by Sotkanet. "
            "Years 2020–2022 aggregate municipal-era observations to those boundaries."
        ),
        "reform_break": "The organising responsibility changed on 1 January 2023.",
        "missing_data_policy": "No interpolation, carry-forward or synthetic observations.",
        "service_value_index": {
            "status": "retired_requires_whole_chain_value_data",
            "reason": (
                "Contacts per real-cost euro is an activity ratio, not efficiency or value. "
                "A value score remains unpublished until resolved need, whole-chain cost and safety "
                "guardrails are available."
            ),
        },
        "quality_balance_index": {
            "baseline": "Finland 2020 = 100",
            "method": "Equal-weight geometric mean of four consistently oriented quality indices.",
            "components": list(QUALITY_BALANCE_COMPONENTS),
            "excluded_but_displayed": [
                "activity_per_real_cost",
                "public_health_staff_per_10000",
                "primary_care_wait_over_7d_pct",
                "primary_care_emergency_visits_per_1000",
                "avoidable_emergency_hospitalizations_per_100000",
            ],
            "reason": (
                "Activity, personnel, realised-visit waiting time and spillover outcomes are safeguards or "
                "context, not interchangeable quality outcomes."
            ),
        },
        "activity_metric": {
            "metric_id": "activity_per_real_cost_index_fi_2020_100",
            "label_fi": "Käyntituotos suhteessa reaalikustannukseen",
            "prohibited_labels": ["efficiency", "cost_effectiveness", "value"],
        },
        "planned_guardrails": [
            {"metric_id": metric_id, "status": "not_available_in_official_snapshot", "definition": definition}
            for metric_id, definition in PLANNED_GUARDRAILS.items()
        ],
        "steering_risk": {
            "comparison_period": "2022-2024",
            "meaning": "Contradictory metric signal; not evidence of intent, causality or gaming.",
            "materiality_thresholds": RISK_THRESHOLDS,
            "rules": {
                "activity_quality_conflict": (
                    "Activity per real-cost euro rises while continuity, experienced access or service "
                    "adequacy falls."
                ),
                "wait_denominator_conflict": (
                    "The realised-visit waiting share improves while population experience worsens."
                ),
                "cost_shift_signal": (
                    "Primary-care real cost falls while emergency visits or avoidable hospitalisations rise."
                ),
                "continuity_countermeasure_breach": "Doctor continuity falls.",
            },
        },
        "sources": [
            {
                "source_id": f"thl_sotkanet_indicator_{indicator_id}",
                "indicator_id": indicator_id,
                "metric_id": spec["metric_id"],
                "url": f"https://sotkanet.fi/sotkanet/fi/metadata/indicators/{indicator_id}",
            }
            for indicator_id, spec in INDICATORS.items()
        ]
        + [
            {
                "source_id": "statfin_public_expenditure_price_11m2",
                "metric_id": "public_service_deflator_2015_100",
                "url": STATFIN_DEFLATOR_URL,
                "values": deflator,
                "scope_note": (
                    "Kuntatalous total public-expenditure price index is used as a transparent public-service "
                    "cost proxy; it is not a primary-care-specific deflator."
                ),
            }
        ],
    }


def main() -> int:
    with requests.Session() as session:
        session.headers.update(
            {
                "User-Agent": (
                    "Budjettihaukka/2.0 primary-care-value-pilot "
                    "(+https://github.com/juntunen-ai/budjettihaukka)"
                )
            }
        )
        regions = _selected_regions(session)
        source_rows = _source_rows(session, regions)
        deflator = _deflator(session)
    panel = _panel(source_rows, regions, deflator)
    steering_risks = _steering_risks(panel)

    source_fields = [
        "metric_id",
        "metric_name_fi",
        "dimension",
        "year",
        "region_code",
        "region_name_fi",
        "region_type",
        "value",
        "absolute_value",
        "unit",
        "direction",
        "source_indicator_id",
        "source_id",
        "source_url",
        "vintage_date",
    ]
    _write_csv(SOURCE_OUTPUT, source_rows, source_fields)
    _write_csv(PANEL_OUTPUT, panel, list(panel[0]))
    _write_csv(RISK_OUTPUT, steering_risks, list(steering_risks[0]))
    JSON_OUTPUT.write_text(json.dumps(panel, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    RISK_JSON_OUTPUT.write_text(
        json.dumps(steering_risks, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    SOURCES_OUTPUT.write_text(
        json.dumps(_sources_document(deflator), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Source observations: {len(source_rows)} -> {SOURCE_OUTPUT}")
    print(f"Panel rows: {len(panel)} -> {PANEL_OUTPUT} and {JSON_OUTPUT}")
    print(f"Steering-risk rows: {len(steering_risks)} -> {RISK_OUTPUT} and {RISK_JSON_OUTPUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

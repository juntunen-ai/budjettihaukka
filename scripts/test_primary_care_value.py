#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REFERENCE = ROOT / "data" / "reference"


def _float(row: dict[str, str], key: str) -> float | None:
    return None if row[key] == "" else float(row[key])


def main() -> None:
    with (REFERENCE / "official_primary_care_value_v1.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        source_rows = list(csv.DictReader(handle))
    with (REFERENCE / "primary_care_value_panel_v1.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        panel = list(csv.DictReader(handle))
    json_panel = json.loads(
        (REFERENCE / "primary_care_value_panel_v1.json").read_text(encoding="utf-8")
    )
    sources = json.loads(
        (REFERENCE / "primary_care_value_sources_v1.json").read_text(encoding="utf-8")
    )
    with (REFERENCE / "primary_care_steering_risk_v1.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        steering_risks = list(csv.DictReader(handle))
    json_risks = json.loads(
        (REFERENCE / "primary_care_steering_risk_v1.json").read_text(encoding="utf-8")
    )
    demo = (ROOT / "perusterveydenhuollon-palveluarvo.html").read_text(encoding="utf-8")
    documentation = (ROOT / "docs" / "primary_care_service_value.md").read_text(
        encoding="utf-8"
    )

    assert len(source_rows) >= 1300
    assert len(panel) == len(json_panel) == 24 * 6
    assert len(steering_risks) == len(json_risks) == 23
    assert {int(row["year"]) for row in panel} == set(range(2020, 2026))
    assert len({row["region_code"] for row in panel}) == 24
    assert sum(row["region_type"] == "wellbeing_area" for row in panel) == 23 * 6
    assert all(
        row["governance_period"] == "municipalities_aggregated_to_current_hva"
        for row in panel
        if int(row["year"]) <= 2022
    )
    assert all(
        row["governance_period"] == "wellbeing_areas_reported"
        for row in panel
        if int(row["year"]) >= 2023
    )
    assert all(
        (row["reform_break"] == "true") == (int(row["year"]) == 2023)
        for row in panel
    )

    source_years: dict[str, set[int]] = defaultdict(set)
    for row in source_rows:
        source_years[row["metric_id"]].add(int(row["year"]))
        assert row["source_url"].startswith("https://sotkanet.fi/")
    assert source_years["primary_care_cost_nominal_eur_per_resident"] == set(
        range(2020, 2025)
    )
    assert source_years["public_health_staff_per_10000"] == set(range(2020, 2024))
    assert source_years["primary_care_doctor_visits_per_1000"] == set(range(2020, 2025))
    assert source_years["primary_care_wait_over_7d_pct"] == set(range(2021, 2026))
    assert source_years["primary_care_doctor_continuity_coci"] == set(range(2020, 2026))
    assert source_years["experienced_fast_access_pct"] == {2020, 2022, 2024}
    assert source_years["self_rated_health_mediocre_or_worse_pct"] == {2020, 2022, 2024}
    assert source_years["experienced_insufficient_doctor_services_pct"] == {
        2020,
        2022,
        2024,
    }
    assert source_years["primary_care_emergency_visits_per_1000"] == set(
        range(2020, 2026)
    )
    assert source_years["avoidable_emergency_hospitalizations_per_100000"] == set(
        range(2020, 2026)
    )

    national = {int(row["year"]): row for row in panel if row["region_code"] == "FI"}
    assert float(national[2020]["quality_balance_index"]) == 100.0
    assert float(national[2022]["quality_balance_index"]) == 92.61
    assert float(national[2024]["quality_balance_index"]) == 92.2
    assert national[2021]["quality_balance_index"] == ""
    assert national[2023]["quality_balance_index"] == ""
    assert national[2025]["quality_balance_index"] == ""
    assert all(row["service_value_index"] == "" for row in panel)
    assert all(
        row["service_value_index_status"] == "retired_requires_whole_chain_value_data"
        for row in panel
    )
    assert national[2025]["coverage_status"] == "partial"

    nominal_2020 = float(national[2020]["primary_care_cost_nominal_eur_per_resident"])
    real_2020 = float(national[2020]["primary_care_cost_real_2024_eur_per_resident"])
    assert math.isclose(real_2020, nominal_2020 * 119.6 / 103.0, abs_tol=0.01)
    assert (
        national[2024]["primary_care_cost_nominal_eur_per_resident"]
        == national[2024]["primary_care_cost_real_2024_eur_per_resident"]
    )

    component_columns = [
        "continuity_index_fi_2020_100",
        "experienced_access_index_fi_2020_100",
        "experienced_service_adequacy_index_fi_2020_100",
        "experienced_health_index_fi_2020_100",
    ]
    planned_guardrail_ids = {
        item["metric_id"] for item in sources["planned_guardrails"]
    }
    for row in panel:
        components = [_float(row, column) for column in component_columns]
        assert "efficiency_index_fi_2020_100" not in row
        assert "output_per_real_cost" not in row
        assert all(row[metric] == "" for metric in planned_guardrail_ids)
        if row["quality_balance_index"]:
            assert all(value is not None and value > 0 for value in components)
            expected = math.exp(
                sum(math.log(float(value)) for value in components if value is not None)
                / len(component_columns)
            )
            assert math.isclose(float(row["quality_balance_index"]), expected, abs_tol=0.02)
            assert row["quality_balance_index_status"] == "published_equal_weight_quality_core"
        else:
            assert row["quality_balance_index_status"] == "not_published_incomplete_quality_core"

    assert sources["period"] == {"from": 2020, "to": 2025}
    assert sources["missing_data_policy"].startswith("No interpolation")
    assert sources["service_value_index"]["status"] == "retired_requires_whole_chain_value_data"
    assert len(sources["quality_balance_index"]["components"]) == 4
    assert len(sources["planned_guardrails"]) == 5
    assert len(sources["steering_risk"]["rules"]) == 4
    assert len(sources["steering_risk"]["materiality_thresholds"]) == 7

    expected_rules = set(sources["steering_risk"]["rules"])
    for row in steering_risks:
        triggered = {item for item in row["triggered_rules"].split("|") if item}
        evaluated = {item for item in row["evaluated_rules"].split("|") if item}
        assert triggered <= evaluated <= expected_rules
        assert int(row["triggered_rule_count"]) == len(triggered)
        assert int(row["evaluated_rule_count"]) == len(evaluated)
        assert "2023_governance_and_recording_break" in row["data_quality_notes"]
        assert set(row["missing_guardrails"].split("|")) == planned_guardrail_ids
        if row["steering_risk_level"] == "high_signal":
            assert len(triggered) >= 2
        elif row["steering_risk_level"] == "watch_signal":
            assert len(triggered) == 1
        elif row["steering_risk_level"] == "no_observed_signal":
            assert not triggered and len(evaluated) >= 3
        else:
            assert row["steering_risk_level"] == "insufficient_data"
            assert len(evaluated) < 3

    assert "primary_care_value_panel_v1.json" in demo
    assert "primary_care_steering_risk_v1.json" in demo
    assert "efficiency_index_fi_2020_100" not in demo
    assert "Kustannustehokkuus" not in demo
    assert demo.count("<strong>Päähavainto.</strong>") == 6
    assert demo.count("<strong>Mitä kuva ei kerro.</strong>") == 6
    assert not any(tag in demo for tag in ("<input", "<select", "<button"))
    assert "2020–2022" in documentation
    assert "2025" in documentation
    assert "ei ole paremmuusjärjestys" in documentation
    assert "Käyntituotos ei ole tehokkuus" in documentation
    assert "Ohjausriskin säännöt" in documentation
    print("Primary-care quality and steering-risk pilot OK")


if __name__ == "__main__":
    main()

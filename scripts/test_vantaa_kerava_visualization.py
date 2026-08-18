#!/usr/bin/env python3
"""Regression checks for the Vantaa-Kerava primary-care visualization."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PANEL_PATH = ROOT / "data/reference/primary_care_value_panel_v1.json"
RISK_PATH = ROOT / "data/reference/primary_care_steering_risk_v1.json"
HTML_PATH = ROOT / "vantaa-kerava-sote.html"


def close(actual: float, expected: float) -> None:
    assert abs(float(actual) - expected) < 0.001, (actual, expected)


def main() -> None:
    panel = json.loads(PANEL_PATH.read_text(encoding="utf-8"))
    risks = json.loads(RISK_PATH.read_text(encoding="utf-8"))
    html = HTML_PATH.read_text(encoding="utf-8")

    vk = sorted(
        (row for row in panel if row["region_code"] == "HVA-04"),
        key=lambda row: row["year"],
    )
    country = [row for row in panel if row["region_code"] == "FI"]
    assert [row["year"] for row in vk] == list(range(2020, 2026))
    assert len(country) == 6

    by_year = {row["year"]: row for row in vk}
    close(by_year[2025]["primary_care_wait_over_7d_pct"], 85.4)
    close(by_year[2025]["primary_care_doctor_continuity_coci"], 0.34)
    close(by_year[2024]["quality_balance_index"], 93.34)
    close(by_year[2024]["experienced_insufficient_doctor_services_pct"], 24.9)
    assert by_year[2022]["primary_care_doctor_continuity_coci"] == ""

    risk = next(row for row in risks if row["region_code"] == "HVA-04")
    assert risk["steering_risk_level"] == "insufficient_data"
    assert risk["triggered_rule_count"] == 0
    assert set(risk["evaluated_rules"].split("|")) == {
        "wait_denominator_conflict",
        "cost_shift_signal",
    }
    assert "activity_quality_conflict" not in risk["evaluated_rules"]
    assert "known_2022_visit_recording_deficit" in risk["data_quality_notes"]

    assert "HVA-04" in html
    assert PANEL_PATH.name in html and RISK_PATH.name in html
    assert html.count('<div class="finding">') == 6
    assert html.count('class="chart"') == 7
    assert "Tunnettu kirjaamispuute" in html
    assert "ei ole luotettava tehokkuustulos" in html
    assert "efficiency_index" not in html
    assert "service_value_index" not in html
    assert all(tag not in html for tag in ("<button", "<select", "<input"))

    print("Vantaa-Kerava visualization regression checks OK")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Regression tests for the Liberal 2008-2025 historical counterfactual."""

from __future__ import annotations

import hashlib
import json
import math
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "reference" / "liberaali_vaihtoehtobudjetti"
INPUT_DIR = DATA_DIR / "historical_inputs"
YEARS = list(range(2008, 2026))


def close(actual: float, expected: float, tolerance: float = 0.05) -> None:
    if not math.isclose(actual, expected, rel_tol=0.0, abs_tol=tolerance):
        raise AssertionError(f"Expected {expected}, got {actual}")


def main() -> int:
    with tempfile.TemporaryDirectory() as temp_dir:
        output_path = Path(temp_dir) / "historical.json"
        subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts" / "analyze_liberal_historical_counterfactual.py"),
                "--out",
                str(output_path),
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        analysis = json.loads(output_path.read_text(encoding="utf-8"))

    assert analysis["meta"]["trust_class"] == "counterfactual_scenario"
    assert analysis["meta"]["causal_claim"] is False
    assert analysis["meta"]["period"] == "2008-2025"

    audit = analysis["source_audit"]
    manifest = audit["input_manifest"]
    manifest_files = {Path(row["path"]).name: row for row in manifest["files"]}
    for filename, audit_key in (
        ("budjettihaukka_actual_by_moment_2007_2025.csv", "actual_file_sha256"),
        ("budjettihaukka_macro_2007_2025.csv", "macro_file_sha256"),
    ):
        path = INPUT_DIR / filename
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        assert digest == manifest_files[filename]["sha256"]
        assert digest == audit[audit_key]
    assert audit["actual_row_count"] == manifest_files[
        "budjettihaukka_actual_by_moment_2007_2025.csv"
    ]["row_count"]
    assert audit["macro_year_count"] == manifest_files[
        "budjettihaukka_macro_2007_2025.csv"
    ]["row_count"]
    assert audit["complete_actual_years"] == YEARS

    baseline = analysis["baseline"]
    close(baseline["central_government_edp_debt_2007_eur"], 60_711_000_000.0)
    close(baseline["central_government_edp_debt_2025_eur"], 209_131_000_000.0)
    close(baseline["gdp_2025_eur"], 281_674_000_000.0)

    policy = analysis["policy_reference"]
    close(policy["recurring_direct_balance_improvement_eur"], 8_045_220_000.0)
    assert policy["implementation_steps_pct"] == [10, 20, 30, 40]
    assert [row["implementation_pct"] for row in policy["annual_targets"][:4]] == [
        10.0,
        30.0,
        60.0,
        100.0,
    ]

    assert set(analysis["cases"]) == {"suotuisa", "keskinen", "varovainen"}
    for case in analysis["cases"].values():
        rows = case["yearly"]
        assert [row["year"] for row in rows] == YEARS
        previous_actual = baseline["central_government_edp_debt_2007_eur"]
        previous_alternative = previous_actual
        for row in rows:
            close(
                row["net_balance_improvement_eur"],
                row["direct_balance_improvement_eur"]
                + row["cyclical_feedback_eur"]
                + row["interest_saving_eur"],
            )
            expected_debt = max(
                previous_alternative
                + row["actual_debt_eur"]
                - previous_actual
                - row["net_balance_improvement_eur"],
                0.0,
            )
            close(row["alternative_debt_eur"], expected_debt)
            previous_actual = row["actual_debt_eur"]
            previous_alternative = row["alternative_debt_eur"]

    summaries = {
        case_id: case["summary"] for case_id, case in analysis["cases"].items()
    }
    final_debts = [
        summaries[case_id]["alternative_debt_2025_eur"]
        for case_id in ("suotuisa", "keskinen", "varovainen")
    ]
    assert final_debts == sorted(final_debts)
    assert final_debts[-1] < baseline["central_government_edp_debt_2025_eur"]
    close(summaries["keskinen"]["alternative_debt_2025_eur"], 100_054_135_243.27, 1.0)
    close(summaries["keskinen"]["cumulative_direct_balance_improvement_eur"], 94_113_195_793.97, 1.0)
    assert summaries["keskinen"]["worst_output_effect_pct"] < 0
    assert summaries["keskinen"]["cumulative_output_effect_real_2025_eur"] > 0
    assert summaries["varovainen"]["cumulative_output_effect_real_2025_eur"] < 0

    grounding = analysis["moment_grounding"]["yearly"]
    assert [row["year"] for row in grounding] == YEARS
    close(grounding[0]["stable_reference_share_pct"], 57.6707687442, 1e-6)
    assert grounding[-1]["stable_reference_share_pct"] > 95
    assert {row["id"] for row in analysis["structural_events"]} == {
        "finanssikriisi_2009",
        "vos_uudistus_2010",
        "covid_2020",
        "sote_uudistus_2023",
    }
    assert any("Työllisyysvaikutus" in item for item in analysis["method"]["not_modelled"])

    print("Liberal historical counterfactual tests PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Regression tests for the alternative-budget model and browser report."""

from __future__ import annotations

import hashlib
import json
import math
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "reference" / "liberaali_vaihtoehtobudjetti" / "vaihtoehtobudjetti_2026_raw.csv"
EXPECTED_SOURCE_HASH = "ab1f36d0ee98312172205a289df14c64e52ad49881c1f7f895002c8899a35aa3"


def close(actual: float, expected: float, tolerance: float = 1.0) -> None:
    if not math.isclose(actual, expected, rel_tol=0.0, abs_tol=tolerance):
        raise AssertionError(f"Expected {expected}, got {actual}")


def main() -> int:
    with tempfile.TemporaryDirectory() as temp_dir:
        temp = Path(temp_dir)
        analysis_path = temp / "analysis.json"
        report_path = temp / "report.html"
        subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "analyze_liberal_alt_budget_v2.py"), "--out", str(analysis_path)],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts" / "build_liberal_alt_budget_report.py"),
                "--analysis",
                str(analysis_path),
                "--out",
                str(report_path),
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )

        analysis = json.loads(analysis_path.read_text(encoding="utf-8"))
        audit = analysis["source_audit"]
        assert hashlib.sha256(RAW.read_bytes()).hexdigest() == EXPECTED_SOURCE_HASH
        assert analysis["meta"]["source_sha256"] == EXPECTED_SOURCE_HASH
        assert audit["snapshot_matches_live_export"] is True
        assert audit["row_count"] == 687
        assert audit["changed_level3_count"] == 324
        close(audit["hierarchy_difference_level3_vs_total_eur"], -700_000.0)

        central = analysis["accounting"]["central_budget"]
        public = analysis["accounting"]["consolidated_public_sector"]
        close(central["spending_change_eur"], -12_552_770_850.0)
        close(central["nonborrowing_revenue_change_eur"], -3_893_550_850.0)
        close(central["borrowing_change_eur"], -8_659_220_000.0)
        close(central["nonborrowing_revenue_change_eur"] + central["borrowing_change_eur"], central["spending_change_eur"])
        close(public["payer_tax_burden_change_eur"], -311_800_250.0)
        close(public["direct_balance_improvement_eur"], 8_659_220_000.0)
        close(public["identified_one_off_revenue_eur"], 614_000_000.0)
        close(public["identified_recurring_balance_improvement_eur"], 8_045_220_000.0)
        close(
            public["nonborrowing_revenue_change_eur"] - public["spending_change_eur"],
            public["direct_balance_improvement_eur"],
        )

        assert set(analysis["scenarios"]) == {"kerralla", "vaiheistettu", "suojattu"}
        assert len(analysis["research_basis"]) >= 16
        assert len(analysis["recommendations"]) == 8
        historical = analysis["historical_counterfactual"]
        assert historical["meta"]["trust_class"] == "counterfactual_scenario"
        assert historical["meta"]["causal_claim"] is False
        assert historical["source_audit"]["complete_actual_years"] == list(range(2008, 2026))
        assert set(historical["cases"]) == {"suotuisa", "keskinen", "varovainen"}
        assert len(historical["envelope"]) == 18
        assert historical["cases"]["keskinen"]["summary"]["worst_output_effect_pct"] < 0
        for scenario in analysis["scenarios"].values():
            close(sum(scenario["phase"]), 1.0, 1e-12)
            for case in scenario["cases"].values():
                yearly = case["yearly"]
                assert len(yearly) == 10
                assert [row["year"] for row in yearly] == list(range(2026, 2036))
                close(yearly[0]["identified_one_off_revenue_eur"], 614_000_000.0)
                assert all(row["identified_one_off_revenue_eur"] == 0 for row in yearly[1:])
                assert case["summary"]["debt_ratio_difference_2035_pp"] < 0

        immediate = analysis["scenarios"]["kerralla"]["cases"]["keskinen"]
        safeguarded = analysis["scenarios"]["suojattu"]["cases"]["keskinen"]
        close(immediate["summary"]["delivered_recurring_balance_improvement_eur"], 7_240_698_000.0)
        assert safeguarded["summary"]["worst_output_effect_pct"] > immediate["summary"]["worst_output_effect_pct"]
        assert safeguarded["summary"]["debt_ratio_2035_pct"] < safeguarded["yearly"][-1]["baseline_debt_pct"]

        microsim = analysis["microsimulation"]
        assert microsim["meta"]["trust_class"] == "suuntaa_antava"
        assert "SISU/FIONA" in microsim["meta"]["official_validation"]
        assert microsim["source_audit"]["cell_count"] == 90
        close(
            microsim["source_audit"]["represented_households"],
            microsim["source_audit"]["official_income_households"],
            0.5,
        )
        assert len(microsim["source_audit"]["sources"]) == 8
        assert set(microsim["cases"]) == {
            "jakauma_suotuisa",
            "keskinen",
            "jakauma_rasittava",
        }
        close(
            microsim["policy_totals"]["net_work_income_package_resource_change_eur"],
            2_386_100_000.0,
        )
        assert len(microsim["decile_envelope"]) == 10
        for row in microsim["decile_envelope"]:
            assert row["low_change_per_household_eur"] <= row["central_change_per_household_eur"]
            assert row["central_change_per_household_eur"] <= row["high_change_per_household_eur"]
        micro_central = microsim["cases"]["keskinen"]
        assert len(micro_central["cells"]) == 90
        assert len(micro_central["by_decile"]) == 10
        assert len(micro_central["by_household_type"]) == 9
        assert micro_central["summary"]["modeled_household_resource_change_eur"] < 0
        assert any(row["change_per_household_eur"] > 0 for row in micro_central["by_decile"])
        assert any(row["change_per_household_eur"] < 0 for row in micro_central["by_decile"])
        for allocation in micro_central["allocations"]:
            close(
                allocation["allocated_eur"],
                allocation["household_resource_change_eur"],
                0.05,
            )

        html = report_path.read_text(encoding="utf-8")
        assert len(html) > 400_000
        assert "__DATA__" not in html
        assert "NaN" not in html
        assert html.count("lineChart('debt-chart'") == 1
        assert 'id="assumption-grid"' in html
        assert "Tulonjako: synteettinen mikromalli" in html
        assert "SISU/FIONA-varmennus puuttuu" in html
        assert 'id="decile-chart"' in html
        assert 'id="household-type-chart"' in html
        assert 'id="history-debt-chart"' in html
        assert 'id="history-output-chart"' in html
        assert 'id="history-coverage-chart"' in html
        assert html.count("data-history-case=") == 3
        assert "Kontrafaktuaali<br>ei toteutunut historia" in html
        assert "ei havaittu tai kausaalisesti tunnistettu historia" in html
        assert 'data-micro-case="keskinen"' in html
        assert "Alkon myyntiin perustuva 614 milj. euroa on kertatulo" in html
        assert "Ehdollisesti toteuttamiskelpoinen" in html
        assert 'aria-label="Raportin osiot"' in html
        embedded = re.search(
            r'<script type="application/json" id="report-data">(.*?)</script>',
            html,
            flags=re.DOTALL,
        )
        assert embedded, "Embedded report data is missing"
        embedded_data = json.loads(embedded.group(1))
        assert embedded_data["meta"]["source_sha256"] == EXPECTED_SOURCE_HASH
        assert embedded_data["microsimulation"]["source_audit"]["cell_count"] == 90
        assert embedded_data["historical_counterfactual"]["meta"]["causal_claim"] is False

    print("Alternative-budget v2 model and report tests PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

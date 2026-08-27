#!/usr/bin/env python3
"""Offline regression tests for the policy growth ledger and its page."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_policy_growth_visualization import (  # noqa: E402
    BUDGET_EVIDENCE_PATH,
    LEDGER_PATH,
    OUTPUT_PATH,
    PRODUCTIVITY_PATH,
    TAX_SCENARIO_PATH,
    build_html,
    validate_budget_evidence,
    validate_ledger,
    validate_tax_scenario,
)

EXPECTED_POLICIES = {
    "rd_funding_cuts_2011_2016",
    "higher_education_capacity_2000_2025",
    "labour_tax_wedge_and_benefit_traps",
    "delayed_labour_reallocation_reforms",
    "consolidation_composition_2011_2016",
    "incumbent_preserving_state_aid",
}


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def policy_by_id(ledger: dict, policy_id: str) -> dict:
    return next(policy for policy in ledger["policies"] if policy["policy_id"] == policy_id)


def main() -> None:
    ledger = load(LEDGER_PATH)
    productivity = load(PRODUCTIVITY_PATH)
    budget_evidence = load(BUDGET_EVIDENCE_PATH)
    tax_scenario = load(TAX_SCENARIO_PATH)
    validate_ledger(ledger, productivity)
    validate_budget_evidence(budget_evidence, ledger)
    validate_tax_scenario(tax_scenario)

    policies = ledger["policies"]
    assert {policy["policy_id"] for policy in policies} == EXPECTED_POLICIES
    assert [policy["priority"] for policy in policies] == list(range(1, 7))
    assert all(policy["overlap_groups"] for policy in policies)
    assert all(policy["budget_grounding"]["status"] for policy in policies)

    # Different estimates and outcome metrics must never be added to the
    # productivity index gap. This is the central trust constraint.
    aggregation = ledger["meta"]["aggregation_policy"]
    assert aggregation["do_not_sum_policy_estimates"] is True
    assert aggregation["do_not_allocate_total_gap"] is True
    estimates = [
        estimate
        for policy in policies
        for estimate in policy["quantitative_evidence"]
    ]
    assert estimates
    assert all(estimate["comparable_to_productivity_gap"] is False for estimate in estimates)

    rd = policy_by_id(ledger, "rd_funding_cuts_2011_2016")
    rd_funding = next(
        item
        for item in rd["quantitative_evidence"]
        if item["metric_id"] == "government_rd_funding_eur_million"
    )
    assert (rd_funding["start_year"], rd_funding["start_value"]) == (2010, 2065.3)
    assert (rd_funding["end_year"], rd_funding["end_value"]) == (2016, 1836.4)

    tax = policy_by_id(ledger, "labour_tax_wedge_and_benefit_traps")
    tax_values = {item["metric_id"]: item["value"] for item in tax["quantitative_evidence"]}
    assert tax_values["gdp_per_capita_lift_vs_sweden_tax_wedge_10y_pct"] == 1.8
    assert tax_values["gdp_per_capita_lift_vs_oecd_tax_wedge_10y_pct"] == 2.5
    assert all(item["budget_neutral"] is True for item in tax["quantitative_evidence"])

    subsidy = policy_by_id(ledger, "incumbent_preserving_state_aid")
    assert subsidy["quantitative_evidence"][0]["value"] == 0.4

    html = build_html(ledger, productivity, budget_evidence, tax_scenario)
    assert "fetch(" not in html
    assert "ei summata" in html
    assert "ei ole virallinen" in html
    assert "politiikan ulkopuolisista häiriöistä" in html
    assert "politiikan ulkopuolisista sokeista" not in html
    assert "Näillä momenteilla toteutunut rahankäyttö väheni" in html
    assert "Vain reaaliarvo pieneni" in html
    assert "Momentin historiallinen kehitys" in html
    assert 'id="budget-moment-select"' in html
    assert 'id="budget-history-chart"' in html
    assert "Voiko alempi verotus tuottaa suuremman verokertymän?" in html
    assert 'id="tax-cut-range"' in html
    assert 'id="tax-history-chart"' in html
    assert "ammattikorkeakoulut" in html
    assert html.count('class="policy"') == 1  # One template, repeated from data at runtime.
    for policy in policies:
        assert policy["title_fi"] in html

    embedded = re.search(
        r'<script type="application/json" id="ledger-data">(.*?)</script>',
        html,
        flags=re.DOTALL,
    )
    assert embedded, "Embedded ledger data is missing"
    assert json.loads(embedded.group(1))["meta"]["dataset_id"] == "policy_growth_ledger_v1"

    budget_embedded = re.search(
        r'<script type="application/json" id="budget-evidence-data">(.*?)</script>',
        html,
        flags=re.DOTALL,
    )
    assert budget_embedded, "Embedded budget evidence is missing"
    embedded_budget = json.loads(budget_embedded.group(1))
    assert embedded_budget["meta"]["dataset_id"] == "policy_budget_evidence_v1"
    assert len(embedded_budget["rows"]) == 11

    tax_embedded = re.search(
        r'<script type="application/json" id="tax-scenario-data">(.*?)</script>',
        html,
        flags=re.DOTALL,
    )
    assert tax_embedded, "Embedded tax scenario is missing"
    embedded_tax = json.loads(tax_embedded.group(1))
    assert embedded_tax["meta"]["dataset_id"] == "tax_growth_scenario_v1"
    assert len(embedded_tax["rows"]) == 18
    assert embedded_tax["summary_2025"]["lower_10pct_revenue_beur"] > embedded_tax[
        "summary_2025"
    ]["actual_tax_revenue_beur"]

    OUTPUT_PATH.write_text(html, encoding="utf-8")
    assert OUTPUT_PATH.exists()
    assert 20_000 < OUTPUT_PATH.stat().st_size < 1_000_000

    last = next(row for row in productivity["years"] if row["year"] == 2025)
    print(
        "Policy growth ledger OK "
        f"({len(policies)} policies; productivity {last['productivity_index']:.0f} "
        f"vs mechanical trend {last['trend_index']:.0f}; "
        f"{len(budget_evidence['rows'])} budget moments; estimates non-additive)"
    )


if __name__ == "__main__":
    main()

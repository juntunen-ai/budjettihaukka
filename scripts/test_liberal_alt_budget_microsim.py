#!/usr/bin/env python3
"""Regression tests for the synthetic household microsimulation."""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

from analyze_liberal_alt_budget_microsim import build_microsimulation

ROOT = Path(__file__).resolve().parents[1]
INPUT_DIR = (
    ROOT
    / "data"
    / "reference"
    / "liberaali_vaihtoehtobudjetti"
    / "microsim_inputs"
)


def canonical_hash(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def close(actual: float, expected: float, tolerance: float = 0.05) -> None:
    if not math.isclose(actual, expected, rel_tol=0.0, abs_tol=tolerance):
        raise AssertionError(f"Expected {expected}, got {actual}")


def main() -> int:
    manifest = json.loads((INPUT_DIR / "manifest.json").read_text(encoding="utf-8"))
    assert len(manifest["sources"]) == 8
    for source in manifest["sources"]:
        snapshot = json.loads((INPUT_DIR / source["path"]).read_text(encoding="utf-8"))
        assert snapshot["source_id"] == source["source_id"]
        assert snapshot["response"]["class"] == "dataset"
        assert canonical_hash(snapshot["response"]) == source["response_sha256"]
        assert canonical_hash(snapshot["response"]) == snapshot["response_sha256"]

    model = build_microsimulation()
    audit = model["source_audit"]
    assert audit["cell_count"] == 90
    close(audit["represented_households"], 2_860_961.0, 0.5)
    close(audit["represented_households"], audit["official_income_households"], 0.5)
    close(
        model["policy_totals"]["net_work_income_package_resource_change_eur"],
        2_386_100_000.0,
    )

    expected_central_total = (
        2_386_100_000.0
        - 4_085_500_000.0 * 0.90
        + 359_100_000.0
        - 238_800_000.0
        - 12_820_000.0
        - 366_634_500.0
        - 1_859_000_000.0 * 0.70
        + (270_000_000.0 + 1_016_000_000.0) * 0.80
        - (100_000_000.0 + 70_000_000.0 + 500_000.0) * 0.90
    )
    central = model["cases"]["keskinen"]
    close(central["summary"]["modeled_household_resource_change_eur"], expected_central_total)
    close(sum(row["change_eur"] for row in central["by_decile"]), expected_central_total)
    close(sum(row["change_eur"] for row in central["by_household_type"]), expected_central_total)

    deciles = {int(row["group_id"]): row for row in central["by_decile"]}
    types = {str(row["group_id"]): row for row in central["by_household_type"]}
    assert deciles[3]["change_per_household_eur"] < 0
    assert deciles[8]["change_per_household_eur"] > 0
    for row in deciles.values():
        close(
            row["post_policy_disposable_income_per_household_eur"],
            row["baseline_disposable_income_per_household_eur"]
            + row["change_per_household_eur"],
        )
    assert types["12"]["change_per_household_eur"] < 0
    assert types["23"]["change_per_household_eur"] < 0
    assert types["22"]["change_per_household_eur"] > 0

    for case in model["cases"].values():
        assert len(case["cells"]) == 90
        assert len(case["by_decile"]) == 10
        assert len(case["by_household_type"]) == 9
        for allocation in case["allocations"]:
            close(
                allocation["allocated_eur"],
                allocation["household_resource_change_eur"],
            )

    events = {row["event_id"]: row for row in model["event_examples"]}
    mover = events["asunnon_vaihtaja_100k_voitto"]
    close(mover["capital_gains_tax_added_eur"], 32_800.0)
    close(
        mover["net_household_change_eur"],
        mover["transfer_tax_saved_eur"] - mover["capital_gains_tax_added_eur"],
    )
    assert "Gini" in " ".join(model["interpretation_rules"])
    assert len(model["required_sisu_outputs"]) >= 6

    print("Alternative-budget synthetic microsimulation tests PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

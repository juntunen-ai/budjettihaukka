from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.enrichment_demo_data import (
    education_series,
    final_account_scale,
    grant_funnels,
    health_scatter,
    load_reference_data,
    purchasing_power_series,
)


def main() -> None:
    refs = load_reference_data()
    assert set(refs) == {"deflators", "sectors", "grants", "final_accounts"}

    purchasing = purchasing_power_series(
        refs["deflators"],
        1_000_000_000,
        ["cpi_general_purchasing_power", "public_service_cost_municipal"],
    )
    assert purchasing["deflator_id"].nunique() == 2
    assert (purchasing[purchasing["year"].eq(2024)]["real_amount_eur"].round() == 1_000_000_000).all()

    education = education_series(refs["sectors"], ["Koko maa", "Uusimaa"])
    assert education["display_metric"].nunique() == 2
    indexed = education[
        education["display_metric"].str.startswith("Tutkinnon") & education["year"].eq(2015)
    ]
    assert (indexed["display_value"].round(6) == 100).all()

    health = health_scatter(refs["sectors"], 2024)
    assert len(health) == 20
    assert health["primary_care_doctor_contacts_per_1000"].notna().all()

    grants = grant_funnels(refs["grants"])
    assert len(grants) == 4
    assert grants["share"].between(0, 1).all()

    accounts = final_account_scale(refs["final_accounts"])
    assert len(accounts) == 6
    assert accounts["value_billion_eur"].is_monotonic_decreasing

    demo_html = (ROOT / "visualisointidemot.html").read_text(encoding="utf-8")
    assert demo_html.count("<section>") == 5
    assert demo_html.count("Päähavainto.") == 5
    assert demo_html.count("Mitä kuva ei kerro.") == 5
    assert "<input" not in demo_html
    assert "<select" not in demo_html
    assert "--paper: #f5f1e8" in demo_html
    assert '<header class="hero">' in demo_html
    for snapshot in (
        "official_deflator_reference_v1.csv",
        "official_sector_indicator_v1.csv",
        "official_grants_okm_pilot_v1.csv",
        "official_final_accounts_reference_v1.csv",
    ):
        assert snapshot in demo_html
    print("Enrichment visualization demos OK")


if __name__ == "__main__":
    main()

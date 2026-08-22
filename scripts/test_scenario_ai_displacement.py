#!/usr/bin/env python3
"""Regressiotestit automaatioskenaariolle.

Testi ei ota verkkoyhteytta eika BigQueryyn. Se ajaa laskelman uudelleen
muistissa ja tarkistaa, etta aritmetiikka pitaa ja etta haarukat ovat
oikein pain.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.scenario_ai_labour_displacement import (
    ASSUMPTIONS, BASELINE, OUT_JSON, compute, validate,
)


def main() -> None:
    payload = json.loads(OUT_JSON.read_text(encoding="utf-8"))
    validate(payload)

    # Tuottavuusvaatimus on identiteetti eika oletus: 15 % vahemman tunteja
    # vaatii tasan 1/0,85 - 1 = 17,65 % lisaa tuottavuutta.
    for share, expected in ((0.15, 17.647), (0.10, 11.111), (0.25, 33.333)):
        result = compute({**ASSUMPTIONS, "displaced_share": share})
        lift = result["productivity_requirement"]["lift_to_hold_gdp_pct"]
        assert abs(lift - expected) < 0.01, (share, lift, expected)

    result = compute(ASSUMPTIONS)
    prod = result["productivity_requirement"]
    fiscal = result["fiscal"]

    # Vaadittu vauhti on moninkertainen toteutuneeseen nahden jokaisella
    # aikajanteella. Jos nain ei ole, skenaarion viesti on eri.
    for years, multiple in prod["multiple_of_recent"].items():
        assert multiple > 2.5, (years, multiple)

    # Haarukka on oikein pain ja skenaario heikentaa taloutta.
    assert 0 < fiscal["net_worsening_low_eur"] < fiscal["net_worsening_high_eur"]
    assert fiscal["net_worsening_high_eur"] < fiscal["baseline_state_gap_eur"]

    # Ansiotuloveron menetys on suurin yksittainen era, ja yhteisovero ei
    # kata sita edes ylarajalla. Tama on laskelman keskeinen havainto.
    assert fiscal["income_tax_loss_eur"] > fiscal["corporate_tax_offset_high_eur"]

    # Etuuskustannus per henkilo on johdettu datasta, ei arvattu.
    expected_benefit = BASELINE["unemployment_benefit_eur"] / BASELINE["unemployed_persons"]
    assert abs(fiscal["state_benefit_per_unemployed_eur"] - expected_benefit) < 1.0

    # Pienempi syrjaytys heikentaa taloutta vahemman, isompi enemman.
    small = compute({**ASSUMPTIONS, "displaced_share": 0.05})
    large = compute({**ASSUMPTIONS, "displaced_share": 0.25})
    assert (small["fiscal"]["net_worsening_low_eur"]
            < fiscal["net_worsening_low_eur"]
            < large["fiscal"]["net_worsening_low_eur"])

    print(f"AI displacement scenario OK (+{prod['lift_to_hold_gdp_pct']:.1f} % tuottavuutta, "
          f"netto -{fiscal['net_worsening_low_eur'] / 1e9:.1f}..."
          f"-{fiscal['net_worsening_high_eur'] / 1e9:.1f} mrd)")


if __name__ == "__main__":
    main()

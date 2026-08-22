#!/usr/bin/env python3
"""Regressiotestit velkaantumisen nopeuden visualisoinnille.

Testi ei ota verkkoyhteyttä eikä BigQueryyn. Se ajaa saman
sopimusvalidoinnin kuin lataaja ja varmistaa, että sivu kertoo
askelmien laskentaperusteen näkyvästi.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.load_debt_speed_steps import STEP_EUR, validate

PAGE = ROOT / "velkavauhti.html"
REFERENCE = ROOT / "data" / "reference" / "debt_speed_steps_v1.json"
PARTY_REFERENCE = ROOT / "data" / "reference" / "party_debt_cumulative_v1.json"

# Kumulatiivinen kertymä painui pohjaan 9/2008. Askelmat lasketaan siitä,
# koska sitä ennen valtio lyhensi velkaa.
EXPECTED_TROUGH_MONTH = "2008-09-01"


def main() -> None:
    payload = json.loads(REFERENCE.read_text(encoding="utf-8"))
    meta = payload["meta"]
    steps = payload["steps"]
    phase = payload["repayment_phase"]
    pending = payload["pending_step"]

    validate(payload)

    assert meta["step_eur"] == STEP_EUR
    assert meta["trough_month"] == EXPECTED_TROUGH_MONTH
    assert meta["trough_cumulative_eur"] < 0
    assert len(steps) >= 14

    # Lyhennysvaihe on aidosti olemassa eikä se ole askelma.
    assert phase["months"] > 0
    assert phase["repaid_eur"] > 0
    assert phase["from_month"] == meta["first_month"]
    assert phase["to_month"] == meta["trough_month"]

    # Kesken oleva askelma ei ole täysi eikä sitä saa verrata valmiisiin.
    assert pending["is_complete"] is False
    assert 0 <= pending["accumulated_eur"] < STEP_EUR
    assert pending["months_so_far"] >= 0

    # Nopeus on aidosti vaihdellut molempiin suuntiin.
    months = [row["months"] for row in steps]
    assert min(months) < max(months) / 2, "askelmien nopeuserot katosivat"

    # Sama velkamääritelmä kuin puoluesivulla: kumulatiivinen loppuarvo täsmää.
    party = json.loads(PARTY_REFERENCE.read_text(encoding="utf-8"))
    assert abs(meta["total_cumulative_eur"] - party["meta"]["total_debt_change_eur"]) < 1.0
    assert meta["observed_months"] == party["meta"]["observed_months"]

    html = PAGE.read_text(encoding="utf-8")
    assert 'lang="fi"' in html
    assert "<input" not in html and "<select" not in html
    assert html.count("<section>") == 1, "yksi kuva, yksi osio"
    assert html.count("Päähavainto.") == 1
    assert html.count("Mitä kuva ei kerro.") == 1
    # Laskentaperuste ja sen syy on kerrottava, muuten ensimmäinen askelma
    # näyttäisi hitaalta velkaantumiselta.
    assert "pohjalta 9/2008" in html
    assert "velan maksamista" in html
    assert "Lyhyt palkki = nopea velkaantuminen" in html
    # Nimellisen summan vertailukelpoisuus 25 vuoden yli on rajattava.
    assert "sama nimellinen summa" in html
    assert "ei valtionvelan kokonaismäärästä" in html

    embedded = re.search(r'<script type="application/json" id="speed-data">(.*?)</script>', html, re.S)
    assert embedded, "upotettu snapshot puuttuu"
    parsed = json.loads(embedded.group(1))
    assert parsed["meta"]["trough_month"] == meta["trough_month"]
    assert len(parsed["steps"]) == len(steps)
    assert [row["months"] for row in parsed["steps"]] == months

    print(f"Debt speed steps OK ({len(steps)} askelmaa, "
          f"nopein {min(months)} kk, hitain {max(months)} kk, "
          f"lyhennysvaihe {phase['months']} kk)")


if __name__ == "__main__":
    main()

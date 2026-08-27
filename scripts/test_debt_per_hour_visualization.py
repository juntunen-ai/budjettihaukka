#!/usr/bin/env python3
"""Offline regression checks for the hourly debt comparison visualization."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PAGE = ROOT / "velka-tunnissa.html"


def close(left: float, right: float, tolerance: float = 1.0) -> None:
    assert abs(left - right) <= tolerance, (left, right)


def main() -> int:
    subprocess.run([sys.executable, str(ROOT / "scripts" / "build_debt_per_hour_visualization.py")], cwd=ROOT, check=True)
    html = PAGE.read_text(encoding="utf-8")
    assert 'lang="fi"' in html
    assert "Kolmas kortti on mallilaskelma." in html
    assert "Toteutunut kuukausidata" in html
    assert "Ratkaisut rahapulaan" in html
    assert "ei ole toteutunut hallituskauden velkaluku eikä ennuste" in html
    assert 'class="direction"' in html
    assert "direction-arrow" in html
    assert "Nettovelanotto kasvaa" in html
    assert "Nettovelanotto vähenee" in html
    assert "Liberaalien ehdotuksilla valtion velkaantuminen hidastuisi" in html
    assert "Mallin mukaan velkaa otetaan silti edelleen" not in html
    assert "#F9B000" in html
    assert "#FFD500" in html
    assert "--green:#148142" in html
    assert "#9a6700" in html
    assert "Oranssi palkki" in html
    assert "velka-tunnissa" not in html  # no broken self-link dependency

    match = re.search(r'<script type="application/json" id="hour-data">(.*?)</script>', html, re.S)
    assert match, "Embedded visualization data missing"
    data = json.loads(match.group(1))
    marin, orpo = data["observed"]
    liberal = data["modelled_counterfactual"]

    assert marin["cabinet_name"] == "Marin"
    assert marin["label"] == "Marinin hallitus"
    assert marin["period"] == "2019-12–2023-06"
    assert marin["months"] == 42
    close(marin["debt_change_eur"], 41_102_677_817.41)
    close(marin["rate_eur_per_hour"], 1_309_336.067)
    close(marin["annualized_debt_change_eur"], 11_469_783_947.519)
    assert "trend_points_eur" not in marin

    assert orpo["cabinet_name"] == "Orpo"
    assert orpo["label"] == "Orpon hallitus"
    assert orpo["period"] == "2023-07–2026-06"
    assert orpo["months"] == 36
    close(orpo["debt_change_eur"], 43_687_727_074.66)
    close(orpo["rate_eur_per_hour"], 1_660_877.702)
    close(orpo["annualized_debt_change_eur"], 14_549_288_669.937)
    assert "trend_points_eur" not in orpo

    assert liberal["scenario_case"] == "Keskinen"
    assert liberal["label"] == "Liberaalien ehdotus"
    assert liberal["period"] is None
    close(liberal["baseline_debt_change_eur"], 16_428_186_852.26)
    close(liberal["direct_balance_improvement_eur"], 7_143_887_568.338132)
    close(liberal["debt_change_eur"], 9_284_299_283.921867)
    close(liberal["rate_eur_per_hour"], 1_059_851.516)
    close(liberal["annualized_debt_change_eur"], 9_284_299_283.921867)
    assert "trend_points_eur" not in liberal
    assert data["meta"]["scenario_causal_claim"] is False
    print("Debt per hour visualization PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

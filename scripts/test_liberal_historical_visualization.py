#!/usr/bin/env python3
"""Regression checks for the public historical-counterfactual visualization."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "data" / "reference" / "liberaali_vaihtoehtobudjetti" / "historiallinen_vastelaskelma_v1.json"


def main() -> int:
    with tempfile.TemporaryDirectory() as temp_dir:
        output = Path(temp_dir) / "visualization.html"
        subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts" / "build_liberal_historical_visualization.py"),
                "--input",
                str(INPUT),
                "--out",
                str(output),
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        html = output.read_text(encoding="utf-8")

    data = json.loads(INPUT.read_text(encoding="utf-8"))
    assert len(html) > 80_000
    assert "__DATA__" not in html
    assert "NaN" not in html
    assert html.count("data-case=") == 3
    assert html.count("data-micro-case=") == 3
    assert 'id="debt-chart"' in html
    assert 'id="growth-chart"' in html
    assert 'id="decile-chart"' in html
    assert 'id="income-chart"' in html
    assert "vertailulaskelma, ei menneisyyden ennustus" in html
    assert "ei kuitenkaan ole sama malli" in html
    assert "BSD-3-Clause-lisenssillä" in html
    assert "SISU olisi Budjettihaukan seuraava erillinen analyysikerros" in html
    assert "Ei SISU-ajo." in html
    assert "Vuoden 2026 paketti osuisi eri tavoin eri tulotasoihin." in html
    assert "Käytettävissä olevat tulot vuodessa" in html
    assert "microsimulation" in html
    assert "Mikromalli:" in html
    assert "statfin_income_deciles_2024" in html
    assert data["meta"]["causal_claim"] is False
    assert str(data["cases"]["keskinen"]["summary"]["alternative_debt_2025_eur"]) in html

    print("Historical counterfactual visualization tests PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

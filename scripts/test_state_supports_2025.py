#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    payload = json.loads((ROOT / "data" / "reference" / "state_supports_2025_v1.json").read_text(encoding="utf-8"))
    headline = payload["headline"]
    families = {row["id"]: row for row in payload["families"]}
    codes = {row["code"] for row in payload["moments"]}
    assert payload["meta"]["year"] == 2025
    assert 30e9 < headline["included_eur"] < 34e9
    assert 25e9 < headline["hva_excluded_eur"] < 28e9
    assert 6.5e9 < headline["discretionary_eur"] < 7.5e9
    assert headline["moment_count"] >= 100
    assert {"etuudet", "valtionosuudet", "yritykset_energia_tki", "maatalous_maaseutu", "jarjestot_kulttuuri"} <= set(families)
    assert "28.89.31." not in codes
    assert "32.20.40." in codes
    assert "33.10.54." in codes
    assert "28.91.41." in codes
    assert families["etuudet"]["nominal_eur"] > 6e9
    authorities = {row["id"]: row for row in payload["targeted_by_authority"]}
    assert authorities["tem"]["nominal_eur"] > 2e9
    assert authorities["mmm"]["nominal_eur"] > 1.8e9
    assert authorities["um"]["nominal_eur"] > 0.6e9
    agencies = {row["id"]: row for row in payload["targeted_by_agency"]}
    assert agencies["ruokavirasto"]["nominal_eur"] > 1.4e9
    assert agencies["business_finland"]["nominal_eur"] > 0.5e9
    html = (ROOT / "valtion-tuet-2025.html").read_text(encoding="utf-8")
    assert html.count("<section>") == 5
    assert html.count("Päähavainto.") == 5
    assert html.count("Mitä kuva ei kerro.") == 5
    assert "<input" not in html and "<select" not in html
    assert "lang=\"fi\"" in html
    assert "Hyvinvointialueiden" in html
    assert "verotuk" in html.lower()
    assert "Ruokavirasto" in html
    assert "Business Finland" in html
    assert "targeted_by_authority" in html
    embedded = re.search(r'<script type="application/json" id="support-data">(.*?)</script>', html, re.S)
    assert embedded, "embedded snapshot missing"
    parsed = json.loads(embedded.group(1))
    assert parsed["headline"]["moment_count"] == headline["moment_count"]
    print("State supports 2025 visualizations OK")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    payload = json.loads((ROOT / "data" / "reference" / "firm_support_tax_v1.json").read_text(encoding="utf-8"))
    headline = payload["headline_2025"]
    trend = {row["year"]: row for row in payload["trend"]}
    assert payload["meta"]["year_from"] == 2015
    assert payload["meta"]["year_to"] == 2025
    assert 1.6e9 < headline["core_support_eur"] < 2.1e9
    assert 30e6 < headline["energiaverotuki_eur"] < 80e6
    assert 5.0e9 < headline["yhteisovero_eur"] < 6.0e9
    assert 20e9 < headline["arvonlisavero_eur"] < 25e9
    assert 0.12e9 < headline["electrification_support_eur"] < 0.18e9
    assert trend[2019]["energiaverotuki_eur"] > trend[2025]["energiaverotuki_eur"] * 3
    assert trend[2021]["crisis_support_eur"] > 0.7e9
    assert trend[2025]["crisis_support_eur"] < 0.05e9
    topic_ids = {row["id"] for row in payload["topics_2025"]}
    assert {"tki", "energia_teollisuus", "maatalousyrittajat"} <= topic_ids
    html = (ROOT / "yritystuet-veroluokat.html").read_text(encoding="utf-8")
    assert html.count("<section>") == 5
    assert html.count("Päähavainto.") == 5
    assert html.count("Mitä kuva ei kerro.") == 5
    assert "<input" not in html and "<select" not in html
    assert "lang=\"fi\"" in html
    assert "verotukiselvityksessä" in html
    assert "Alemmat arvonlisäverokannat" in html
    assert "sähköistämistuki" in html.lower()
    embedded = re.search(r'<script type="application/json" id="firm-data">(.*?)</script>', html, re.S)
    assert embedded, "embedded snapshot missing"
    parsed = json.loads(embedded.group(1))
    assert parsed["headline_2025"]["moment_count"] == headline["moment_count"]
    print("Firm support and tax-class visualizations OK")


if __name__ == "__main__":
    main()

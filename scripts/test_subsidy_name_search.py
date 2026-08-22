#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    payload = json.loads((ROOT / "data" / "reference" / "subsidy_name_search_v1.json").read_text(encoding="utf-8"))
    headline = payload["headline_2025"]
    trend = {row["year"]: row for row in payload["trend"]}
    assert payload["meta"]["year_from"] == 1998
    assert payload["meta"]["year_to"] == 2025
    assert payload["meta"]["coverage_status"] == "complete"
    assert payload["meta"]["latest_complete_year"] == 2025
    assert set(trend) == set(range(1998, 2026))
    assert 6.5e9 < headline["name_match_eur"] < 7.0e9
    assert 6.0e9 < headline["related_missed_eur"] < 6.5e9
    assert 0.07 < headline["name_match_share"] < 0.08
    assert headline["moment_count"] >= 40
    assert abs(sum(row["nominal_eur"] for row in payload["topics_2025"]) - headline["name_match_eur"]) < 1
    assert abs(sum(row["nominal_eur"] for row in payload["related_missed_2025"]) - headline["related_missed_eur"]) < 1
    assert trend[2009]["name_match_nominal_eur"] > trend[2010]["name_match_nominal_eur"] * 1.4
    assert payload["top_moments_2025"][0]["code"] == "33.10.54."
    assert payload["topic_series"]["toimeentulotuki"][2016 - 1998]["nominal_eur"] == 0
    assert payload["topic_series"]["toimeentulotuki"][2017 - 1998]["nominal_eur"] > 1e8
    html = (ROOT / "tuki-avustus.html").read_text(encoding="utf-8")
    assert html.count("<section>") == 5
    assert html.count("Päähavainto.") == 5
    assert html.count("Mitä kuva ei kerro.") == 5
    assert "<input" not in html
    assert "<select" not in html
    assert "lang=\"fi\"" in html
    assert "2010 VOS-uudistus" in html
    assert "Opintoraha ja asumislisä" in html
    assert "Verotuet" in html
    embedded = re.search(r'<script type="application/json" id="subsidy-data">(.*?)</script>', html, re.S)
    assert embedded, "embedded snapshot missing"
    parsed = json.loads(embedded.group(1))
    assert parsed["headline_2025"]["moment_count"] == headline["moment_count"]
    print("Subsidy name-search visualizations OK")


if __name__ == "__main__":
    main()

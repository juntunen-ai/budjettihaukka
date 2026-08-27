#!/usr/bin/env python3
"""Regressiotestit velkasuhteen nousun ajureille.

Testi ei ota verkkoyhteyttä eikä BigQueryyn. Se ajaa saman
sopimusvalidoinnin kuin lataaja ja varmistaa, että sivu erottaa kaksi
mekanismia toisistaan ja merkitsee rakennemuutoksen.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.load_debt_ratio_drivers import OUT, validate
from utils.budget_semantics import TECHNICAL_NAME_KEYWORDS, fiscal_side_case_sql

PAGE = ROOT / "velkasuhteen-ajurit.html"


def main() -> None:
    payload = json.loads(OUT.read_text(encoding="utf-8"))
    meta = payload["meta"]
    mechanism = payload["mechanism"]
    drivers = payload["drivers"]

    validate(payload)

    by_year = {row["year"]: row for row in mechanism}
    assert {2020, 2022, 2024, 2025} <= set(by_year), sorted(by_year)

    # Mekanismi 1: nimittajan apu seuraa nimellisen BKT:n kasvua. Vuonna 2022
    # apu oli suuri ja 2024 pieni, koska inflaatio hiipui.
    assert by_year[2022]["denominator_effect_pp"] < -3.0, by_year[2022]
    assert by_year[2024]["denominator_effect_pp"] > -1.5, by_year[2024]
    assert by_year[2022]["gdp_nominal_change_pct"] > by_year[2024]["gdp_nominal_change_pct"] + 4

    # Vuonna 2020 nimittaja nosti suhdetta, koska talous kutistui. Tama on
    # se ero, jonka koko sivu selittaa.
    assert by_year[2020]["denominator_effect_pp"] > 0, by_year[2020]
    assert by_year[2024]["denominator_effect_pp"] < 0, by_year[2024]

    # Mekanismi 2: korkomenot moninkertaistuivat.
    assert by_year[2025]["interest_eur"] > 3 * by_year[2021]["interest_eur"]

    # Budjettiero kaventui vertailuvuosien valilla, vaikka velkasuhde nousi.
    # Ilman tata havaintoa sivun johtopaatos olisi eri.
    assert meta["budget_gap_to_eur"] < meta["budget_gap_from_eur"], meta
    assert meta["compare_from"] == 2023 and meta["compare_to"] == 2025

    # Korkoera on omanaan eika hallinnonalan sisalla.
    interest = [row for row in drivers if row["bucket"] == "interest"]
    assert len(interest) == 1, interest
    assert interest[0]["gap_effect_eur"] > 0

    # Ajureiden summa vastaa eron muutosta, muuten kuva vaittaisi liikaa.
    total = sum(row["gap_effect_eur"] for row in drivers)
    assert abs(total - (meta["budget_gap_to_eur"] - meta["budget_gap_from_eur"])) < 1_000_000

    # Tekniset erat on suodatettava meno- ja tulopuolelta. Aiemmin SQL tunsi
    # vain yksikkomuodon "peruutus", joten monikko "peruutukset" luettiin
    # tuloksi. Varmistetaan etta jokainen avainsana on mukana SQL:ssa.
    sql = fiscal_side_case_sql(code_expr="c", name_expr="n", hallinnonala_expr="h")
    for keyword in TECHNICAL_NAME_KEYWORDS:
        assert f"'%{keyword}%'" in sql, keyword

    html = PAGE.read_text(encoding="utf-8")
    flat = re.sub(r"\s+", " ", html)
    assert 'lang="fi"' in html
    assert "<input" not in html and "<select" not in html
    assert html.count("<section>") == 2, "kaksi kuvaa, kaksi osiota"
    assert html.count("Päähavainto.") == 2
    assert html.count("Mitä kuva ei kerro.") == 2
    # Rakennemuutos ja sen vaikutus vertailuun on kerrottava.
    assert "sote-uudistuksen jälkeisiä" in flat
    assert "Vuoteen 2022 ei voi verrata" in flat
    # Inflaation rooli nimittajassa on avattava, muuten kuva johtaa harhaan.
    assert "Inflaatio pienentää velkasuhdetta mutta ei pienennä velkaa" in flat
    # Budjettiero ei ole sama asia kuin velkakannan muutos.
    assert "ei myöskään ole sama asia kuin velkakannan muutos" in flat

    embedded = re.search(r'<script type="application/json" id="drivers-data">(.*?)</script>', html, re.S)
    assert embedded, "upotettu snapshot puuttuu"
    parsed = json.loads(embedded.group(1))
    assert len(parsed["mechanism"]) == len(mechanism)
    assert len(parsed["drivers"]) == len(drivers)

    print(f"Debt ratio drivers OK ({len(mechanism)} vuotta, {len(drivers)} ajuria, "
          f"budjettiero {meta['budget_gap_from_eur'] / 1e9:.1f} -> "
          f"{meta['budget_gap_to_eur'] / 1e9:.1f} mrd)")


if __name__ == "__main__":
    main()

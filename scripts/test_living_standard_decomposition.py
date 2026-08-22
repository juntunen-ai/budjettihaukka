#!/usr/bin/env python3
"""Regressiotestit asukaskohtaisen BKT:n hajotelmalle.

Testi ei ota verkkoyhteytta eika BigQueryyn. Se lukee committoidun
snapshotin ja ajaa saman sopimusvalidoinnin kuin piirtoskripti.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_living_standard_decomposition import (
    BASE_YEAR, FACTORS, OUT_JSON, OUT_PNG, validate,
)

# Kokonaismuutokset 2008-2025, todennettu Tilastokeskuksen aineistosta.
EXPECTED = {
    "productivity_index": 5.0,
    "hours_per_worker_index": -6.5,
    "employment_rate_index": 7.5,
    "demography_index": -7.1,
    "gdp_per_capita_index": -1.9,
}
TOLERANCE = 1.5


def main() -> None:
    payload = json.loads(OUT_JSON.read_text(encoding="utf-8"))
    meta = payload["meta"]
    rows = payload["years"]

    validate(payload)

    assert meta["base_year"] == BASE_YEAR
    assert len(rows) >= 18, len(rows)

    last = rows[-1]
    for key, expected in EXPECTED.items():
        change = last[key] - 100
        assert abs(change - expected) <= TOLERANCE, (key, change, expected)

    # Vastauksen ydin: tyollisyysasteen nousu ja vaestorakenteen heikkeneminen
    # ovat samaa suuruusluokkaa ja kumoavat toisensa.
    employment = last["employment_rate_index"] - 100
    demography = last["demography_index"] - 100
    assert employment > 0 and demography < 0, (employment, demography)
    assert abs(employment + demography) < 2.0, (employment, demography)

    # Tuottavuus kasvoi mutta tunnit tyollista kohden laskivat enemman, joten
    # tyopanoksen puoli vetaa kokonaisuutta alas.
    assert last["productivity_index"] > 100
    assert last["hours_per_worker_index"] < 100
    assert last["hours_per_worker_index"] - 100 < -(last["productivity_index"] - 100)

    # Tuottavuuden kasvu hidastui selvasti finanssikriisin jalkeen.
    assert meta["productivity_cagr_pre_pct"] > 2.0, meta
    assert meta["productivity_cagr_post_pct"] < 1.0, meta
    assert meta["productivity_index_if_trend"] > 1.3 * last["productivity_index"]

    # Identiteetti patee joka vuosi, ei vain viimeisena.
    for row in rows:
        product = 1.0
        for key, _label, _color in FACTORS:
            product *= row[key] / 100
        assert abs(100 * product - row["gdp_per_capita_index"]) < 1e-6, row["year"]

    assert OUT_PNG.exists(), f"kuvatiedosto puuttuu: {OUT_PNG}"
    assert OUT_PNG.stat().st_size > 100_000, "kuva vaikuttaa liian pienelta"

    print(f"Living standard decomposition OK ({len(rows)} vuotta; tuottavuus "
          f"{last['productivity_index'] - 100:+.1f} %, tunnit "
          f"{last['hours_per_worker_index'] - 100:+.1f} %, työllisyys "
          f"{employment:+.1f} %, väestö {demography:+.1f} %)")


if __name__ == "__main__":
    main()

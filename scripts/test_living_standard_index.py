#!/usr/bin/env python3
"""Regressiotestit elintasoindeksille 2008-2025.

Testi ei ota verkkoyhteytta eika BigQueryyn. Se lukee committoidun
snapshotin, ajaa saman sopimusvalidoinnin kuin piirtoskripti ja tarkistaa
etta kuva on olemassa.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_living_standard_chart import BASE_YEAR, OUT_JSON, OUT_PNG, validate

# Kolme kiintopistetta, jotka on todennettu Tilastokeskuksen aineistosta.
# Toleranssi sallii tilinpidon tavanomaisen tarkistuksen mutta kaataa
# testin, jos sarja vaihtuu toiseen mittariin.
EXPECTED = {
    "gdp_volume_index": (2025, 104.2),
    "gdp_per_capita_index": (2025, 98.1),
    "gdp_per_capita_ex_housing_index": (2024, 95.6),
}
TOLERANCE = 1.5


def main() -> None:
    payload = json.loads(OUT_JSON.read_text(encoding="utf-8"))
    meta = payload["meta"]
    rows = payload["years"]

    validate(payload)

    assert meta["base_year"] == BASE_YEAR
    assert len(rows) >= 18, len(rows)

    by_year = {row["year"]: row for row in rows}
    for key, (year, expected) in EXPECTED.items():
        value = by_year[year][key]
        assert value is not None, (key, year)
        assert abs(value - expected) <= TOLERANCE, (key, year, value, expected)

    # Kokonaistalous on kasvanut mutta asukasta kohden ei ole palannut
    # lahtotasolle. Tama on kuvan koko vaite.
    last = rows[-1]
    assert last["gdp_volume_index"] > 100, last
    assert last["gdp_per_capita_index"] < 100, last

    # Vaestonkasvu selittaa eron: asukasta kohden laskettu jaa kokonaisuuden
    # alle jokaisena vuotena perusvuoden jalkeen.
    for row in rows:
        if row["year"] > BASE_YEAR:
            assert row["gdp_per_capita_index"] < row["gdp_volume_index"], row["year"]

    # Rajattu sarja paattyy aiemmin, koska toimialatietoa ei ole viela
    # julkaistu viimeiselle vuodelle. Sita ei saa jatkaa arvaamalla.
    ex_years = [row["year"] for row in rows if row["gdp_per_capita_ex_housing_index"] is not None]
    assert meta["ex_housing_last_year"] == max(ex_years)
    assert max(ex_years) < max(row["year"] for row in rows)

    # Asuntotulon osuus on kasvanut, mika on rajauksen peruste.
    shares = [row["housing_share_pct"] for row in rows if row["housing_share_pct"] is not None]
    assert shares[-1] > shares[0] + 1.0, (shares[0], shares[-1])

    assert OUT_PNG.exists(), f"kuvatiedosto puuttuu: {OUT_PNG}"
    assert OUT_PNG.stat().st_size > 100_000, "kuva vaikuttaa liian pienelta"

    print(f"Living standard index OK ({len(rows)} vuotta, volyymi "
          f"{last['gdp_volume_index']:.1f}, asukasta kohden "
          f"{last['gdp_per_capita_index']:.1f}, ilman asuntotuloa "
          f"{by_year[max(ex_years)]['gdp_per_capita_ex_housing_index']:.1f} "
          f"({max(ex_years)}))")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Regressiotestit tuottavuuden vertailu-uralle.

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

from scripts.build_productivity_counterfactual import (
    BASE_YEAR, END_YEAR, OUT_JSON, OUT_PNG, TREND_FROM, validate,
)


def main() -> None:
    payload = json.loads(OUT_JSON.read_text(encoding="utf-8"))
    meta = payload["meta"]
    rows = payload["years"]

    validate(payload)

    assert meta["trend_from"] == TREND_FROM and meta["base_year"] == BASE_YEAR
    assert rows[0]["year"] == TREND_FROM and rows[-1]["year"] == END_YEAR

    # Vertailu-ura on Suomen omaan historiaan nahden varovainen. Tama on
    # kuvan keskeinen perustelu, joten se lukitaan testiin.
    benchmarks = meta["benchmark_rates_pct"]
    assert meta["trend_rate_pct"] < benchmarks["1975-1995"], benchmarks
    assert meta["trend_rate_pct"] < benchmarks["1975-2008"], benchmarks
    assert 2.0 < meta["trend_rate_pct"] < 3.0, meta["trend_rate_pct"]

    # Toteutunut kasvu on murto-osa vertailu-urasta.
    assert meta["actual_rate_pct"] < 0.6, meta["actual_rate_pct"]
    assert meta["trend_rate_pct"] > 5 * meta["actual_rate_pct"]

    last = rows[-1]
    trend_last = next(row["trend_index"] for row in rows if row["year"] == END_YEAR)
    assert 100 < last["productivity_index"] < 115, last["productivity_index"]
    assert trend_last > 140, trend_last

    # Vaatimatonkin ura jaa selvasti toteutuneen ylapuolelle.
    modest_last = next(row["modest_index"] for row in rows if row["year"] == END_YEAR)
    assert modest_last > last["productivity_index"] + 8, (modest_last, last["productivity_index"])
    assert modest_last < trend_last

    # Perusvuotta edeltavina vuosina ei ole vertailu-uraa eika kuilua.
    for row in rows:
        if row["year"] < BASE_YEAR:
            assert row["trend_index"] is None, row["year"]
            assert row["annual_gap_eur"] == 0.0, row["year"]

    # Kuilu kasvaa ja on suuruusluokaltaan kymmenia miljardeja vuodessa.
    assert last["annual_gap_eur"] / 1e9 > 50, last["annual_gap_eur"]
    assert last["cumulative_gap_eur"] > 10 * last["annual_gap_eur"] * 0.5

    assert OUT_PNG.exists(), f"kuvatiedosto puuttuu: {OUT_PNG}"
    assert OUT_PNG.stat().st_size > 100_000

    print(f"Productivity counterfactual OK (vertailu {meta['trend_rate_pct']:.2f} %/v vs "
          f"toteutunut {meta['actual_rate_pct']:.2f} %/v; taso {trend_last:.0f} vs "
          f"{last['productivity_index']:.0f}; kuilu {last['cumulative_gap_eur'] / 1e9:.0f} mrd)")


if __name__ == "__main__":
    main()

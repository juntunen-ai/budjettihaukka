#!/usr/bin/env python3
"""Regressiotestit vaestoennusteelle.

Testi ei ota verkkoyhteytta eika BigQueryyn. Se lukee committoidun
snapshotin ja ajaa saman sopimusvalidoinnin kuin lataaja.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.load_population_projection import CSV_FIELDS, OUT_CSV, PROJECTIONS, validate

FLOAT_FIELDS = {
    "population_total", "age_0_14", "age_15_64", "age_65_74", "age_75_plus",
    "working_age_share_pct", "old_age_dependency_ratio", "total_dependency_ratio",
}


def read_rows() -> list[dict]:
    with OUT_CSV.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == CSV_FIELDS, reader.fieldnames
        rows = []
        for raw in reader:
            row = dict(raw)
            row["year"] = int(row["year"])
            row["includes_net_migration"] = row["includes_net_migration"] == "True"
            for field in FLOAT_FIELDS:
                row[field] = float(row[field])
            rows.append(row)
    return rows


def main() -> None:
    rows = read_rows()
    validate(rows)

    assert len({row["projection_id"] for row in rows}) == len(PROJECTIONS)
    years = sorted({row["year"] for row in rows})
    assert years[0] <= 2025 and years[-1] >= 2045, years

    official = {row["year"]: row for row in rows if row["includes_net_migration"]}
    domestic = {row["year"]: row for row in rows if not row["includes_net_migration"]}
    assert set(official) == set(domestic)

    last = max(official)

    # Virallisessa ennusteessa tyoikaisten osuus pysyy vakaana, mika on
    # koko aineiston keskeinen havainto. Jos tama muuttuu, elintason
    # hajotelman tulkinta tulevaisuudesta on tehtava uudelleen.
    shares = [official[year]["working_age_share_pct"] for year in sorted(official)]
    assert max(shares) - min(shares) < 2.0, (min(shares), max(shares))

    # Ilman nettomaahanmuuttoa osuus sen sijaan laskee selvasti.
    domestic_shares = [domestic[year]["working_age_share_pct"] for year in sorted(domestic)]
    assert domestic_shares[0] - domestic_shares[-1] > 2.0, (domestic_shares[0], domestic_shares[-1])

    # Maahanmuuton merkitys tyoikaisten maaralle on suuri ja kasvava.
    gap = official[last]["age_15_64"] - domestic[last]["age_15_64"]
    assert gap > 500_000, gap
    assert gap / official[last]["age_15_64"] > 0.15, gap

    # Vanhushuoltosuhde heikkenee molemmissa, mutta enemman ilman
    # maahanmuuttoa.
    assert official[last]["old_age_dependency_ratio"] > official[min(official)]["old_age_dependency_ratio"]
    assert domestic[last]["old_age_dependency_ratio"] > official[last]["old_age_dependency_ratio"] + 5

    # Ikaryhmat kattavat vaeston ilman paallekkaisyytta joka rivilla.
    for row in rows:
        total = row["age_0_14"] + row["age_15_64"] + row["age_65_74"] + row["age_75_plus"]
        assert abs(total - row["population_total"]) < 1.0, row["year"]
        assert row["source_url"].startswith("https://pxdata.stat.fi/")

    print(f"Population projection OK ({len(rows)} riviä, {years[0]}-{years[-1]}, "
          f"työikäisten ero {gap / 1e3:.0f} tuhatta vuonna {last})")


if __name__ == "__main__":
    main()

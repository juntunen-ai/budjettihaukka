#!/usr/bin/env python3
"""Kokoaa Suomen talouskasvun neljästä lähteestä yhteen vertailukelpoiseen
tauluun gdp_growth_outlook_v1.

Mittari on kaikilla sama: bruttokansantuotteen volyymin muutos prosentteina
eli se, mitä talouskasvulla yleensä tarkoitetaan. Nimellinen BKT ja
elinkustannusindeksillä deflatoitu BKT eivät ole sama asia, eikä niitä saa
sekoittaa tähän.

Lähteet ja niiden rooli:

- Tilastokeskus, kansantalouden tilinpito: toteutuneet vuodet. Tämä on
  kansallinen virallinen luku ja toimii vertailukohtana muille.
- Suomen Pankki: ennuste. Julkaistaan verkkosivuna eikä rajapintana, joten
  luvut on kirjattu tähän käsin lähdeosoitteineen ja julkaisupäivineen.
- OECD Economic Outlook: toteumat ja ennuste, SDMX-rajapinta.
- IMF World Economic Outlook: toteumat ja ennuste, DataMapper-rajapinta.

Saman vuoden luvut eroavat lähteittäin, koska aineistoversiot ovat eri
ikäisiä. Vuoden 2024 kasvu on Tilastokeskuksen mukaan eri luku kuin OECD:n
tai IMF:n mukaan. Eroa ei tasoiteta, vaan jokainen rivi kantaa oman
lähteensä ja vintagensa.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings

OUT = ROOT / "data" / "reference" / "gdp_growth_outlook_v1.csv"
TABLE_NAME = "gdp_growth_outlook_v1"

STATFIN_URL = "https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/ntp/15a9.px"
OECD_URL = (
    "https://sdmx.oecd.org/public/rest/data/"
    "OECD.ECO.MAD,DSD_EO@DF_EO/FIN.GDPV_ANNPCT.A"
)
IMF_URL = "https://www.imf.org/external/datamapper/api/v1/NGDP_RPCH/FIN"

# Suomen Pankki julkaisee ennusteen verkkosivuna eikä koneluettavana
# rajapintana, joten luvut on luettu julkaisusta ja kirjattu tähän. Jokainen
# rivi kantaa julkaisupäivän ja osoitteen, jotta luku on jäljitettävissä.
BOF_FORECASTS: tuple[dict[str, Any], ...] = (
    {
        "vintage_label": "Suomen Pankki, kesäkuun 2026 ennuste",
        "vintage_date": "2026-06-12",
        "source_url": "https://www.suomenpankki.fi/en/news-and-topical/press-releases-and-news/"
                      "releases/2026/finlands-economy-at-a-turning-point/",
        "values": {2026: 0.7, 2027: 1.2, 2028: 1.4},
    },
    {
        "vintage_label": "Suomen Pankki, maaliskuun 2026 väliennuste",
        "vintage_date": "2026-03-24",
        "source_url": "https://www.suomenpankki.fi/en/news-and-topical/press-releases-and-news/"
                      "releases/2026/bank-of-finlands-interim-forecast-economic-performance-"
                      "overshadowed-by-rising-energy-prices/",
        "values": {2026: 0.6, 2027: 1.4, 2028: 1.5},
    },
)

CSV_FIELDS = [
    "source_id",
    "institution",
    "year",
    "gdp_volume_change_pct",
    "is_national_official",
    "is_beyond_national_actual",
    "vintage_label",
    "vintage_date",
    "source_url",
    "fetched_at",
]


def fetch_statfin(session: requests.Session) -> list[dict[str, Any]]:
    metadata = session.get(STATFIN_URL, timeout=60)
    metadata.raise_for_status()
    meta = metadata.json()
    years = [
        value
        for variable in meta["variables"]
        if variable["code"] == "timeperiod_y"
        for value in variable["values"]
    ]
    query = {
        "query": [
            {"code": "taloustoimi_1_20180101", "selection": {"filter": "item", "values": ["B1GMH"]}},
            {"code": "timeperiod_y", "selection": {"filter": "item", "values": years}},
            {"code": "contentscode", "selection": {"filter": "item", "values": ["ntp-vol_muutos"]}},
        ],
        "response": {"format": "json"},
    }
    response = session.post(STATFIN_URL, json=query, timeout=90)
    response.raise_for_status()
    rows = []
    for item in response.json().get("data", []):
        year = next((key for key in item["key"] if key.isdigit() and len(key) == 4), None)
        raw = item["values"][0] if item.get("values") else None
        if year is None or raw in (None, ".", "..", "..."):
            continue
        rows.append({"year": int(year), "value": float(str(raw).replace(",", "."))})
    if not rows:
        raise ValueError("Tilastokeskuksesta ei saatu volyymin muutoksia")
    return sorted(rows, key=lambda row: row["year"])


def fetch_oecd(session: requests.Session) -> tuple[list[dict[str, Any]], str]:
    response = session.get(
        OECD_URL,
        headers={"Accept": "application/vnd.sdmx.data+json;version=1.0"},
        timeout=90,
    )
    response.raise_for_status()
    payload = response.json()
    structure = payload["data"]["structure"]
    edition = str(structure.get("name") or "OECD Economic Outlook")
    years = [value["id"] for value in structure["dimensions"]["observation"][0]["values"]]
    series = payload["data"]["dataSets"][0].get("series", {})
    if not series:
        raise ValueError("OECD-vastauksessa ei ole sarjoja")
    rows = []
    for entry in series.values():
        for index, values in entry["observations"].items():
            if values and values[0] is not None:
                rows.append({"year": int(years[int(index)]), "value": round(float(values[0]), 2)})
    return sorted(rows, key=lambda row: row["year"]), edition


def fetch_imf(_session: requests.Session) -> list[dict[str, Any]]:
    # IMF:n DataMapper vastaa 403, jos User-Agent on oma tunniste. Kaytetaan
    # siksi requests-kirjaston oletusta omassa istunnossaan. Selaimeksi ei
    # tekeydyta, vaan pyynto tehdaan sellaisena kuin kirjasto sen tekee.
    response = requests.get(IMF_URL, timeout=90)
    response.raise_for_status()
    values = response.json().get("values", {}).get("NGDP_RPCH", {}).get("FIN")
    if not values:
        raise ValueError("IMF-vastauksessa ei ole Suomen sarjaa")
    return sorted(
        ({"year": int(year), "value": float(value)} for year, value in values.items()),
        key=lambda row: row["year"],
    )


def build_rows(
    *,
    statfin: list[dict[str, Any]],
    oecd: list[dict[str, Any]],
    oecd_edition: str,
    imf: list[dict[str, Any]],
    fetched_at: str,
) -> list[dict[str, Any]]:
    latest_actual = max(row["year"] for row in statfin)

    def make(source_id, institution, year, value, official, vintage_label, vintage_date, url):
        return {
            "source_id": source_id,
            "institution": institution,
            "year": year,
            "gdp_volume_change_pct": value,
            "is_national_official": official,
            # Vuodet toteutuneen kansallisen tilinpidon jälkeen ovat
            # ennusteita. Myös sitä edeltävät kansainvälisten järjestöjen
            # luvut voivat olla arvioita, ks. moduulin kuvaus.
            "is_beyond_national_actual": year > latest_actual,
            "vintage_label": vintage_label,
            "vintage_date": vintage_date,
            "source_url": url,
            "fetched_at": fetched_at,
        }

    rows: list[dict[str, Any]] = []
    for row in statfin:
        rows.append(make(
            "statfin_national_accounts_15a9", "Tilastokeskus", row["year"], row["value"],
            True, "Kansantalouden tilinpito, vuositilinpito", fetched_at[:10], STATFIN_URL,
        ))
    for forecast in BOF_FORECASTS:
        for year, value in forecast["values"].items():
            rows.append(make(
                "bank_of_finland_forecast", "Suomen Pankki", year, value, False,
                forecast["vintage_label"], forecast["vintage_date"], forecast["source_url"],
            ))
    for row in oecd:
        rows.append(make(
            "oecd_economic_outlook", "OECD", row["year"], row["value"], False,
            oecd_edition, fetched_at[:10], OECD_URL,
        ))
    for row in imf:
        rows.append(make(
            "imf_world_economic_outlook", "IMF", row["year"], row["value"], False,
            "World Economic Outlook, DataMapper NGDP_RPCH", fetched_at[:10], IMF_URL,
        ))
    return sorted(rows, key=lambda row: (row["source_id"], row["vintage_date"], row["year"]))


def validate(rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise ValueError("Rivejä ei muodostunut")

    institutions = {row["institution"] for row in rows}
    expected = {"Tilastokeskus", "Suomen Pankki", "OECD", "IMF"}
    if institutions != expected:
        raise ValueError(f"Laitokset puuttuvat tai ylimääräisiä: {institutions ^ expected}")

    for row in rows:
        if not row["source_url"].startswith("https://"):
            raise ValueError(f"Lähdeosoite puuttuu: {row}")
        if not row["vintage_date"]:
            raise ValueError(f"Vintage puuttuu: {row}")
        # Vuosikasvu on prosenttia. Yli 25 prosentin luku olisi merkki siitä,
        # että sarjaksi on eksynyt indeksi tai taso eikä muutos.
        if abs(row["gdp_volume_change_pct"]) > 25:
            raise ValueError(f"Epäuskottava kasvuluku: {row}")

    # Sama laitos ja vintage ei saa antaa samalle vuodelle kahta lukua.
    keys = {(row["source_id"], row["vintage_date"], row["year"]) for row in rows}
    if len(keys) != len(rows):
        raise ValueError("Sama lähde, vintage ja vuosi esiintyy useasti")

    official = [row for row in rows if row["is_national_official"]]
    if not official:
        raise ValueError("Kansallista virallista sarjaa ei ole")
    if any(row["institution"] != "Tilastokeskus" for row in official):
        raise ValueError("Vain Tilastokeskus saa olla kansallinen virallinen lähde")


def load_bigquery(rows: list[dict[str, Any]], *, project: str, dataset: str) -> None:
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{TABLE_NAME}"
    client.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("source_id", "STRING"),
                bigquery.SchemaField("institution", "STRING"),
                bigquery.SchemaField("year", "INT64"),
                bigquery.SchemaField("gdp_volume_change_pct", "FLOAT64"),
                bigquery.SchemaField("is_national_official", "BOOL"),
                bigquery.SchemaField("is_beyond_national_actual", "BOOL"),
                bigquery.SchemaField("vintage_label", "STRING"),
                bigquery.SchemaField("vintage_date", "DATE"),
                bigquery.SchemaField("source_url", "STRING"),
                bigquery.SchemaField("fetched_at", "TIMESTAMP"),
            ],
        ),
    ).result()
    print(f"BQ-taulu -> {table_id} ({len(rows)} riviä)")


def main() -> int:
    parser = argparse.ArgumentParser(description="Kokoa Suomen talouskasvu neljästä lähteestä.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--output", type=Path, default=OUT)
    parser.add_argument("--load-bigquery", action="store_true")
    args = parser.parse_args()

    fetched_at = dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    session = requests.Session()
    session.headers.update({"User-Agent": "budjettihaukka-reference-loader"})

    statfin = fetch_statfin(session)
    print(f"Tilastokeskus: {len(statfin)} vuotta "
          f"({statfin[0]['year']}-{statfin[-1]['year']})")
    oecd, oecd_edition = fetch_oecd(session)
    print(f"OECD: {len(oecd)} vuotta ({oecd[0]['year']}-{oecd[-1]['year']}), {oecd_edition}")
    imf = fetch_imf(session)
    print(f"IMF: {len(imf)} vuotta ({imf[0]['year']}-{imf[-1]['year']})")
    print(f"Suomen Pankki: {sum(len(f['values']) for f in BOF_FORECASTS)} riviä "
          f"{len(BOF_FORECASTS)} ennusteesta")

    rows = build_rows(
        statfin=statfin, oecd=oecd, oecd_edition=oecd_edition, imf=imf, fetched_at=fetched_at
    )
    validate(rows)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Snapshot -> {args.output.relative_to(ROOT)} ({len(rows)} riviä)")

    if args.load_bigquery:
        load_bigquery(rows, project=args.project, dataset=args.dataset)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

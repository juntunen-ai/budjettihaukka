#!/usr/bin/env python3
"""Build a snapshot joining THL placements to State Treasury HVA budgets.

The budget measure is the whole organiser's budgeted operating expenditure
(`Toimintamenot`), not a child-welfare appropriation. Budget figures are
nominal and come from the HTAS report for each reporting year's own budget.
"""

from __future__ import annotations

import csv
import io
import json
import math
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
REFERENCE = ROOT / "data" / "reference"
CSV_PATH = REFERENCE / "child_welfare_budget_panel_v1.csv"
JSON_PATH = REFERENCE / "child_welfare_budget_panel_v1.json"
SOURCES_PATH = REFERENCE / "child_welfare_budget_sources_v1.json"

PLACEMENT_YEARS = (2021, 2022, 2023, 2024)
BUDGET_YEARS = (2023, 2024, 2025, 2026)
SOTKANET_INDICATOR = 3563
SOTKANET_CSV = "https://sotkanet.fi/rest/1.1/csv"
SOTKANET_REGIONS = "https://sotkanet.fi/rest/1.1/regions"
SOTKANET_METADATA = (
    "https://sotkanet.fi/sotkanet/fi/metadata/indicators/3563"
)
PBI_REPORT_ID = "7e56c49e-af1b-40b3-9cde-cbf0210dc9fa"
PBI_GROUP_ID = "60aed8b2-ec4c-4202-86e1-9531ef389f47"
PBI_MODEL_ID = 219647
PBI_TOKEN = (
    "https://www.tutkihallintoa.fi/wp-json/wp/v2/powerbi/ext/getReportToken"
    f"?embed_type=report&group_id={PBI_GROUP_ID}&id={PBI_REPORT_ID}&dashboard_id="
)
PBI_QUERY = (
    "https://wabi-north-europe-q-primary-redirect.analysis.windows.net/"
    "explore/querydata?synchronous=true"
)
HTAS_PAGE = (
    "https://www.tutkihallintoa.fi/hyvinvointialueiden-talous/"
    "hyvinvointialueen-ja-hyvinvointiyhtyman-talousarvio-ja-suunnitelma/"
)
USER_AGENT = "Budjettihaukka/1.0 official-data snapshot"


def fetch(url: str, *, data: bytes | None = None, headers: dict | None = None) -> bytes:
    request_headers = {"User-Agent": USER_AGENT, **(headers or {})}
    with urlopen(Request(url, data=data, headers=request_headers), timeout=60) as response:
        return response.read()


def fetch_json(url: str, **kwargs):
    return json.loads(fetch(url, **kwargs).decode("utf-8-sig"))


def pct_change(current: float, base: float) -> float | None:
    if not base:
        return None
    return round((current / base - 1) * 100, 2)


def in_filter(source: str, property_name: str, values: list[str]) -> dict:
    return {
        "Condition": {
            "In": {
                "Expressions": [
                    {
                        "Column": {
                            "Expression": {"SourceRef": {"Source": source}},
                            "Property": property_name,
                        }
                    }
                ],
                "Values": [
                    [{"Literal": {"Value": repr(value)}}] for value in values
                ],
            }
        }
    }


def powerbi_query(token: str) -> dict:
    sources = [
        ("i", "ilmoittaja"),
        ("a", "aineisto"),
        ("t", "tunnusluvut"),
        ("m", "_Mittarit"),
    ]
    fields = [
        ("i", "ilmoittaja", "Column"),
        ("a", "Raportointikausi", "Column"),
        ("a", "Tehtävä", "Column"),
        ("t", "Tunnusluvut", "Column"),
        ("m", "Ilmoitettu luku", "Measure"),
    ]
    select = [
        {
            kind: {
                "Expression": {"SourceRef": {"Source": source}},
                "Property": property_name,
            },
            "Name": f"{source}.{property_name}",
        }
        for source, property_name, kind in fields
    ]
    body = {
        "version": "1.0.0",
        "queries": [
            {
                "Query": {
                    "Commands": [
                        {
                            "SemanticQueryDataShapeCommand": {
                                "Query": {
                                    "Version": 2,
                                    "From": [
                                        {"Name": name, "Entity": entity, "Type": 0}
                                        for name, entity in sources
                                    ],
                                    "Where": [
                                        in_filter(
                                            "i",
                                            "Organisaatiotyyppi",
                                            ["hyvinvointialue", "kunta"],
                                        ),
                                        in_filter("a", "Tehtävä", ["Talousarvio 202x"]),
                                        in_filter("t", "Tunnusluvut", ["Toimintamenot"]),
                                    ],
                                    "Select": select,
                                },
                                "Binding": {
                                    "DataReduction": {
                                        "DataVolume": 6,
                                        "Primary": {"Window": {"Count": 5000}},
                                    },
                                    "Primary": {
                                        "Groupings": [
                                            {"Projections": list(range(len(select)))}
                                        ]
                                    },
                                },
                                "ExecutionMetricsKind": 1,
                            }
                        }
                    ]
                }
            }
        ],
        "cancelQueries": [],
        "modelId": PBI_MODEL_ID,
    }
    activity_id = str(uuid.uuid4())
    result = fetch_json(
        PBI_QUERY,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"EmbedToken {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "ActivityId": activity_id,
            "RequestId": str(uuid.uuid4()),
            "X-PowerBI-HostEnv": "Embed for Customers",
        },
    )
    return result["results"][0]["result"]["data"]


def decode_powerbi_rows(data: dict) -> list[dict]:
    dataset = data["dsr"]["DS"][0]
    rows = dataset["PH"][1]["DM1"]
    schema = rows[0]["S"]
    dictionaries = dataset.get("ValueDicts", {})
    previous = [None] * len(schema)
    decoded: list[dict] = []
    for row in rows:
        repeated = int(row.get("R", 0))
        supplied = iter(row.get("C", []))
        current = []
        for index, field in enumerate(schema):
            if repeated & (1 << index):
                raw = previous[index]
            else:
                raw = next(supplied, None)
            current.append(raw)
        previous = current
        output = {}
        for field, raw in zip(schema, current):
            dictionary_name = field.get("DN")
            if dictionary_name and isinstance(raw, int):
                raw = dictionaries[dictionary_name][raw]
            output[field["N"]] = raw
        decoded.append(output)
    return decoded


def load_budgets() -> dict[tuple[str, int], float]:
    token = fetch_json(PBI_TOKEN)["token"]
    data = powerbi_query(token)
    rows = decode_powerbi_rows(data)
    budgets = {}
    for row in rows:
        year = int(row["G1"])
        if year in BUDGET_YEARS and row.get("M0") is not None:
            budgets[(row["G0"], year)] = float(row["M0"])
    return budgets


def load_placements() -> tuple[list[dict], list[dict]]:
    query = urlencode(
        [
            ("indicator", str(SOTKANET_INDICATOR)),
            *[("years", str(year)) for year in PLACEMENT_YEARS],
            ("genders", "total"),
        ]
    )
    placement_rows = list(
        csv.DictReader(
            io.StringIO(fetch(f"{SOTKANET_CSV}?{query}").decode("utf-8-sig")),
            delimiter=";",
        )
    )
    regions = fetch_json(SOTKANET_REGIONS)
    return placement_rows, regions


def build_snapshot() -> dict:
    budgets = load_budgets()
    placements, region_metadata = load_placements()
    regions_by_id = {str(row["id"]): row for row in region_metadata}
    yta_by_id = {
        row["id"]: row
        for row in region_metadata
        if row.get("category") == "YTA" and row.get("code") != "6"
    }
    organisers = [
        row
        for row in region_metadata
        if row.get("category") == "HYVINVOINTIALUE"
        and row.get("code") in {*(f"{number:02d}" for number in range(1, 22)), "90"}
    ]
    placement_index = {
        (row["region"], int(row["year"])): row for row in placements
    }
    output = []
    for region in sorted(organisers, key=lambda row: row["code"]):
        code = region["code"]
        region_id = str(region["id"])
        official_name = region["title"]["fi"]
        budget_name = "Helsinki" if code == "90" else official_name
        yta_id = next((member for member in region.get("memberOf", []) if member in yta_by_id), None)
        if yta_id is None:
            raise ValueError(f"Missing YTA for {official_name}")
        row = {
            "region_code": code,
            "region_name": (
                "Helsinki (erillinen järjestäjä)" if code == "90" else official_name
            ),
            "region_name_official": official_name,
            "yta_name": yta_by_id[yta_id]["title"]["fi"],
        }
        for year in BUDGET_YEARS:
            value = budgets.get((budget_name, year))
            if value is None:
                raise ValueError(f"Missing budget for {budget_name}, {year}")
            row[f"budget_nominal_meur_{year}"] = round(value / 1_000_000, 3)
        for year in PLACEMENT_YEARS:
            source = placement_index.get((region_id, year))
            if source is None:
                raise ValueError(f"Missing placement for {official_name}, {year}")
            row[f"placed_children_{year}"] = int(float(source["absolute value"]))
            row[f"placed_children_pct_{year}"] = float(source["primary value"])
        row["budget_change_meur_2023_2026"] = round(
            row["budget_nominal_meur_2026"] - row["budget_nominal_meur_2023"], 3
        )
        row["budget_change_pct_2023_2026"] = pct_change(
            row["budget_nominal_meur_2026"], row["budget_nominal_meur_2023"]
        )
        row["budget_change_pct_2023_2024"] = pct_change(
            row["budget_nominal_meur_2024"], row["budget_nominal_meur_2023"]
        )
        row["budget_change_pct_2024_2025"] = pct_change(
            row["budget_nominal_meur_2025"], row["budget_nominal_meur_2024"]
        )
        row["budget_change_pct_2025_2026"] = pct_change(
            row["budget_nominal_meur_2026"], row["budget_nominal_meur_2025"]
        )
        row["placed_change_n_2021_2024"] = (
            row["placed_children_2024"] - row["placed_children_2021"]
        )
        row["placed_change_pct_2021_2024"] = pct_change(
            row["placed_children_2024"], row["placed_children_2021"]
        )
        row["placed_change_n_2023_2024"] = (
            row["placed_children_2024"] - row["placed_children_2023"]
        )
        row["placed_change_pct_2023_2024"] = pct_change(
            row["placed_children_2024"], row["placed_children_2023"]
        )
        row["budget_minus_placements_change_pp_2023_2024"] = round(
            row["budget_change_pct_2023_2024"]
            - row["placed_change_pct_2023_2024"],
            2,
        )
        output.append(row)

    yta_rows = []
    for yta_id, yta in sorted(yta_by_id.items(), key=lambda pair: pair[1]["code"]):
        members = [row for row in output if row["yta_name"] == yta["title"]["fi"]]
        if not members:
            continue
        yta_source_id = str(yta_id)
        yta_row = {
            "yta_name": yta["title"]["fi"],
            "region_count": len(members),
        }
        for year in BUDGET_YEARS:
            yta_row[f"budget_nominal_meur_{year}"] = round(
                sum(row[f"budget_nominal_meur_{year}"] for row in members), 3
            )
        for year in PLACEMENT_YEARS:
            source = placement_index[(yta_source_id, year)]
            yta_row[f"placed_children_{year}"] = int(float(source["absolute value"]))
            yta_row[f"placed_children_pct_{year}"] = float(source["primary value"])
        yta_row["budget_change_pct_2023_2024"] = pct_change(
            yta_row["budget_nominal_meur_2024"], yta_row["budget_nominal_meur_2023"]
        )
        yta_row["placed_change_pct_2023_2024"] = pct_change(
            yta_row["placed_children_2024"], yta_row["placed_children_2023"]
        )
        yta_rows.append(yta_row)

    extracted_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    return {
        "metadata": {
            "version": "1.0.0",
            "extracted_at_utc": extracted_at,
            "budget_measure": "HTAS Toimintamenot, Talousarvio 202x",
            "budget_price_basis": "nominal current euros",
            "placement_indicator_id": SOTKANET_INDICATOR,
            "placement_age_group": "0–17",
            "placement_latest_year": max(PLACEMENT_YEARS),
            "notes": [
                "The budget is the organiser's whole operating expenditure, not child-welfare expenditure.",
                "Budget and placement changes are descriptive and do not establish causality or efficiency.",
                "Helsinki is included as a separate organiser although it is not formally a wellbeing services county.",
            ],
        },
        "regions": output,
        "yta": yta_rows,
    }


def write_snapshot(snapshot: dict) -> None:
    REFERENCE.mkdir(parents=True, exist_ok=True)
    JSON_PATH.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    fields = list(snapshot["regions"][0].keys())
    with CSV_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(snapshot["regions"])
    sources = {
        "version": "1.0.0",
        "extracted_at_utc": snapshot["metadata"]["extracted_at_utc"],
        "sources": [
            {
                "name": "THL Sotkanet indicator 3563",
                "url": SOTKANET_METADATA,
                "api_url": SOTKANET_CSV,
                "license": "CC BY 4.0",
            },
            {
                "name": "State Treasury HTAS Power BI report",
                "url": HTAS_PAGE,
                "report_id": PBI_REPORT_ID,
                "measure": "Toimintamenot / Talousarvio 202x",
                "license": "CC BY 4.0",
            },
        ],
    }
    SOURCES_PATH.write_text(
        json.dumps(sources, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def main() -> None:
    snapshot = build_snapshot()
    write_snapshot(snapshot)
    print(
        f"Wrote {len(snapshot['regions'])} organisers and {len(snapshot['yta'])} YTA groups"
    )


if __name__ == "__main__":
    main()

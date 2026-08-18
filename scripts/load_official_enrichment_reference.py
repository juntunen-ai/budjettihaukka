#!/usr/bin/env python3
"""Refresh the official enrichment snapshots used by the analytics mart.

The command is deliberately snapshot-first: network reads produce reviewable CSV
files and a content-addressed vintage manifest. BigQuery is mutated only when
``--load-bigquery`` is supplied.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings

REFERENCE_DIR = ROOT / "data" / "reference"
FETCHED_AT = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _number(value: Any) -> float:
    return float(str(value).replace("\u00a0", "").replace(" ", "").replace(",", "."))


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _pxweb(
    session: requests.Session,
    url: str,
    selections: dict[str, list[str]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    metadata_response = session.get(url, timeout=60)
    metadata_response.raise_for_status()
    metadata = metadata_response.json()
    variables = {row["code"]: row for row in metadata["variables"]}
    for code, values in selections.items():
        if code not in variables:
            raise ValueError(f"{url}: missing dimension {code}")
        unknown = set(values) - set(variables[code]["values"])
        if unknown:
            raise ValueError(f"{url}: unknown {code} values {sorted(unknown)}")
    query = [
        {"code": code, "selection": {"filter": "item", "values": values}}
        for code, values in selections.items()
    ]
    response = session.post(url, json={"query": query, "response": {"format": "json"}}, timeout=90)
    response.raise_for_status()
    return metadata, response.json()["data"]


def fetch_education(session: requests.Session) -> list[dict[str, Any]]:
    url = "https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/sijk/111l.px"
    years = [str(year) for year in range(2015, 2025)]
    regions = ["SS", "01", "02", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "21"]
    contents = ["tutkinto1vaiemmin", "tyollinen1vpaastayhtosuus"]
    selections = {
        "timeperiod_y": years,
        "koulutusaste_17_20180101": ["SSS"],
        "sukupuoli_1_20120101": ["S"],
        "maakunta_2_20130101": regions,
        "koulutusala_1_20160101": ["SS"],
    }
    metadata_response = session.get(url, timeout=60)
    metadata_response.raise_for_status()
    metadata = metadata_response.json()
    region_meta = next(row for row in metadata["variables"] if row["code"] == "maakunta_2_20130101")
    region_names = dict(zip(region_meta["values"], region_meta["valueTexts"], strict=True))
    definitions = {
        "tutkinto1vaiemmin": ("education_graduates_previous_year", "Tutkinnon suorittaneet", "PERSON", "output", "higher_is_more_output"),
        "tyollinen1vpaastayhtosuus": ("education_employed_one_year_pct", "Työlliset vuosi valmistumisen jälkeen", "PERCENT", "outcome", "higher_is_better"),
    }
    rows: list[dict[str, Any]] = []
    for content in contents:
        _metadata, data = _pxweb(session, url, selections | {"contentscode": [content]})
        for item in data:
            key = item["key"]
            year = next(value for value in key if value in years)
            region = next(value for value in key if value in regions)
            if not item.get("values") or item["values"][0] in (None, ".", ".."):
                continue
            metric_id, name, unit, indicator_type, direction = definitions[content]
            rows.append({
                "dashboard_id": "education",
                "metric_id": metric_id,
                "metric_name_fi": name,
                "indicator_type": indicator_type,
                "year": int(year),
                "region_code": "FI" if region == "SS" else f"MK{region}",
                "region_name_fi": region_names[region],
                "region_type": "country" if region == "SS" else "region",
                "value": _number(item["values"][0]),
                "unit": unit,
                "direction": direction,
                "source_id": "statfin_education_placement_111l",
                "source_url": url,
                "is_causal_effect": "false",
                "vintage_date": "2026-01-23",
            })
    return rows


def _sotkanet_metadata(session: requests.Session) -> tuple[dict[int, dict[str, Any]], dict[int, dict[str, Any]]]:
    indicators = session.get("https://sotkanet.fi/rest/1.1/indicators", timeout=60)
    indicators.raise_for_status()
    regions = session.get("https://sotkanet.fi/rest/1.1/regions", timeout=60)
    regions.raise_for_status()
    return ({int(row["id"]): row for row in indicators.json()}, {int(row["id"]): row for row in regions.json()})


def fetch_health(session: requests.Session) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    indicator_meta, region_meta = _sotkanet_metadata(session)
    specs = {
        1080: ("primary_care_doctor_contacts_per_1000", "Perusterveydenhuollon lääkärikäynnit", "PER_1000_PERSONS", "output", "context_dependent"),
        4333: ("self_rated_health_mediocre_or_worse_pct", "Terveytensä keskitasoiseksi tai huonommaksi kokevat", "PERCENT", "outcome", "lower_is_better"),
    }
    years = list(range(2018, 2025))
    selected_regions = {
        region_id: row
        for region_id, row in region_meta.items()
        if row.get("category") in {"MAA", "MAAKUNTA"}
    }
    rows: list[dict[str, Any]] = []
    for indicator_id, (metric_id, name, unit, indicator_type, direction) in specs.items():
        params: list[tuple[str, str | int]] = [("indicator", indicator_id), ("genders", "total")]
        params.extend(("years", year) for year in years)
        response = session.get("https://sotkanet.fi/rest/1.1/json", params=params, timeout=90)
        response.raise_for_status()
        for item in response.json():
            region_id = int(item["region"])
            if region_id not in selected_regions:
                continue
            region = selected_regions[region_id]
            rows.append({
                "dashboard_id": "health",
                "metric_id": metric_id,
                "metric_name_fi": name,
                "indicator_type": indicator_type,
                "year": int(item["year"]),
                "region_code": "FI" if region["category"] == "MAA" else f"MK{region['code']}",
                "region_name_fi": region["title"]["fi"],
                "region_type": "country" if region["category"] == "MAA" else "region",
                "value": _number(item["value"]),
                "unit": unit,
                "direction": direction,
                "source_id": f"thl_sotkanet_indicator_{indicator_id}",
                "source_url": f"https://sotkanet.fi/sotkanet/fi/metadata/indicators/{indicator_id}",
                "is_causal_effect": "false",
                "vintage_date": FETCHED_AT[:10],
            })
    region_rows = [
        {
            "region_code": "FI" if row["category"] == "MAA" else f"MK{row['code']}",
            "region_name_fi": row["title"]["fi"],
            "region_type": "country" if row["category"] == "MAA" else "region",
            "official_uri": row.get("uri") or "",
            "source_id": "thl_sotkanet_regions",
            "valid_from": "2026-01-01",
        }
        for row in selected_regions.values()
    ]
    return rows, sorted(region_rows, key=lambda row: row["region_code"])


def fetch_deflators(session: requests.Session) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    macro_path = REFERENCE_DIR / "official_macro_reference_v1.csv"
    with macro_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["series_id"] == "cost_of_living_index_1951_10_100":
                rows.append({
                    "deflator_id": "cpi_general_purchasing_power",
                    "year": row["year"], "index_value": row["value"], "base_year": "1951-10", "unit": row["unit"],
                    "target_scope": "generic_nominal_amounts", "selection_priority": 30,
                    "source_id": row["source_id"], "source_url": row["source_url"], "is_preliminary": "false", "vintage_date": FETCHED_AT[:10],
                })
    def fetch_px(url: str, selections: dict[str, list[str]], definition: tuple[str, str, str, int, str]) -> None:
        deflator_id, base_year, target_scope, priority, source_id = definition
        _metadata, data = _pxweb(session, url, selections)
        years = set(selections["timeperiod_y"])
        for item in data:
            year = next(value for value in item["key"] if value in years)
            value = item.get("values", [None])[0]
            if value in (None, ".", ".."):
                continue
            rows.append({
                "deflator_id": deflator_id, "year": year, "index_value": _number(value), "base_year": base_year,
                "unit": f"INDEX_{base_year}_100", "target_scope": target_scope, "selection_priority": priority,
                "source_id": source_id, "source_url": url, "is_preliminary": "true" if year == "2025" and source_id.endswith("11m2") else "false",
                "vintage_date": FETCHED_AT[:10],
            })
    fetch_px(
        "https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/rki/13g9.px",
        {"timeperiod_y": [str(year) for year in range(1998, 2026)], "perusv_1_20180101": ["2021_100"], "contentscode": ["rki-pisteluku"]},
        ("building_cost_investment", "2021", "building_investments", 10, "statfin_building_cost_13g9"),
    )
    fetch_px(
        "https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/jmhi/11m2.px",
        {"timeperiod_y": [str(year) for year in range(2015, 2026)], "kuntatyyppi_1_20140101": ["1"], "jmhi_7_20190101": ["SSS"], "contentscode": ["pojo_kunteht"]},
        ("public_service_cost_municipal", "2015", "municipal_public_services_only", 20, "statfin_public_expenditure_price_11m2"),
    )
    return sorted(rows, key=lambda row: (row["deflator_id"], int(row["year"])))


ORGANIZATION_NAMES = (
    "Nokia Oyj",
    "KONE Oyj",
    "Wärtsilä Oyj Abp",
)


def _business_id_checksum_valid(business_id: str) -> bool:
    if len(business_id) != 9 or business_id[7] != "-" or not (business_id[:7] + business_id[8]).isdigit():
        return False
    remainder = sum(int(number) * weight for number, weight in zip(business_id[:7], (7, 9, 10, 5, 8, 4, 2), strict=True)) % 11
    expected = 0 if remainder == 0 else 11 - remainder
    return expected != 10 and expected == int(business_id[8])


def fetch_organization_master(session: requests.Session) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    url = "https://avoindata.prh.fi/opendata-ytj-api/v3/companies"
    for query_name in ORGANIZATION_NAMES:
        response = session.get(url, params={"name": query_name}, timeout=60)
        response.raise_for_status()
        matches = []
        for company in response.json().get("companies", []):
            current_names = [row["name"] for row in company.get("names", []) if not row.get("endDate")]
            if query_name.casefold() in {name.casefold() for name in current_names}:
                matches.append((company, current_names))
        if len(matches) != 1:
            raise ValueError(f"PRH exact-name resolution for {query_name!r} returned {len(matches)} matches")
        company, current_names = matches[0]
        addresses = [row for row in company.get("addresses", []) if not row.get("endDate")]
        address = next((row for row in addresses if row.get("type") == 1), addresses[0] if addresses else {})
        post_office = (address.get("postOffices") or [{}])[0]
        business_id = company["businessId"]["value"] if isinstance(company["businessId"], dict) else company["businessId"]
        rows.append({
            "organization_id": f"FI-YT-{business_id}", "business_id": business_id, "canonical_name": current_names[0],
            "business_id_checksum_valid": str(_business_id_checksum_valid(business_id)).lower(),
            "municipality_code": post_office.get("municipalityCode") or "", "postal_code": address.get("postCode") or "",
            "organization_status": company.get("status") or company.get("tradeRegisterStatus") or "",
            "master_scope": "reference_organizations_not_grant_joined", "source_id": "prh_ytj_v3",
            "source_url": f"{url}?businessId={business_id}", "valid_from": company.get("registrationDate") or "", "valid_to": company.get("endDate") or "",
            "vintage_date": FETCHED_AT[:10],
        })
    return rows


def static_final_accounts() -> list[dict[str, Any]]:
    url = "https://www.tutkihallintoa.fi/valtio/taloustiedot/valtion-tilinpaatos/"
    values = [
        ("budget_execution_balance_eur", 3_700_000, "EUR", 100_000, "not_rounded_on_page"),
        ("balance_sheet_total_eur", 76_500_000_000, "EUR", 50_000_000, "rounded_to_0_1_beur"),
        ("balance_sheet_government_debt_eur", 185_000_000_000, "EUR", 50_000_000, "rounded_to_0_1_beur"),
        ("income_statement_deficit_eur", 11_900_000_000, "EUR", 50_000_000, "rounded_to_0_1_beur"),
        ("transfer_expenses_eur", 67_500_000_000, "EUR", 50_000_000, "rounded_to_0_1_beur"),
        ("net_borrowing_eur", 16_400_000_000, "EUR", 50_000_000, "rounded_to_0_1_beur"),
        ("tax_revenue_eur", 65_600_000_000, "EUR", 50_000_000, "rounded_to_0_1_beur"),
    ]
    return [
        {"fiscal_year": 2025, "metric_id": metric_id, "official_value": value, "unit": unit, "tolerance_eur": tolerance,
         "precision_status": precision, "audit_status": "official_final_accounts", "publication_date": "2026-03-27",
         "source_id": "state_treasury_final_accounts_2025", "source_url": url, "vintage_date": "2026-03-27"}
        for metric_id, value, unit, tolerance, precision in values
    ]


def static_grants_pilot() -> list[dict[str, Any]]:
    base = {
        "pilot_id": "okm_all_available_decisions", "granting_authority": "Opetus- ja kulttuuriministeriö",
        "administrative_branch": "opetus_ja_kulttuuriministerion_hallinnonala", "period_start": "2004-12-31", "period_end": "2026-11-25",
        "geographic_allocation": "national_unallocated", "allocation_basis": "report_filter_no_recipient_level_export",
        "source_id": "tutkiavustuksia_public_report", "source_url": "https://www.tutkihallintoa.fi/valtionavustukset/tutkiavustuksia/",
        "vintage_date": FETCHED_AT[:10], "publication_status": "aggregate_pilot", "recipient_join_status": "blocked_raw_decisions_unavailable",
    }
    metrics = [
        ("applications_received_count", 8786, "COUNT"), ("positive_decisions_count", 5260, "COUNT"),
        ("applied_amount_eur", 1_664_447_580, "EUR"), ("granted_amount_eur", 886_700_051, "EUR"),
    ]
    return [base | {"metric_id": metric_id, "value": value, "unit": unit} for metric_id, value, unit in metrics]


@dataclass(frozen=True)
class SnapshotSpec:
    source_id: str
    filename: str
    rows: list[dict[str, Any]]
    fields: list[str]


def _manifest(specs: list[SnapshotSpec]) -> list[dict[str, Any]]:
    result = []
    for spec in specs:
        canonical_rows = [
            {key: "" if value is None else str(value) for key, value in row.items() if key != "vintage_date"}
            for row in spec.rows
        ]
        content = json.dumps(canonical_rows, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        digest = hashlib.sha256(content).hexdigest()
        years = [int(row["year"]) for row in spec.rows if row.get("year") not in (None, "")]
        result.append({
            "vintage_id": f"{spec.source_id}:{digest[:16]}", "source_id": spec.source_id, "fetched_at": FETCHED_AT,
            "content_sha256": digest, "row_count": len(spec.rows), "min_year": min(years) if years else "",
            "max_year": max(years) if years else "", "schema_sha256": hashlib.sha256("|".join(spec.fields).encode()).hexdigest(),
            "is_current": "true", "snapshot_path": f"data/reference/{spec.filename}",
        })
    return result


def refresh() -> list[SnapshotSpec]:
    with requests.Session() as session:
        session.headers.update({"User-Agent": "Budjettihaukka/2.0 official-data-refresh (+https://github.com/juntunen-ai/budjettihaukka)"})
        education = fetch_education(session)
        health, regions = fetch_health(session)
        sector = sorted(education + health, key=lambda row: (row["dashboard_id"], row["metric_id"], row["region_code"], row["year"]))
        deflators = fetch_deflators(session)
        organizations = fetch_organization_master(session)
    final_accounts = static_final_accounts()
    grants = static_grants_pilot()
    specs = [
        SnapshotSpec("official_sector_indicators", "official_sector_indicator_v1.csv", sector, list(sector[0])),
        SnapshotSpec("official_deflators", "official_deflator_reference_v1.csv", deflators, list(deflators[0])),
        SnapshotSpec("official_regions", "official_region_reference_v1.csv", regions, list(regions[0])),
        SnapshotSpec("prh_organization_master", "organization_master_v1.csv", organizations, list(organizations[0])),
        SnapshotSpec("state_treasury_final_accounts", "official_final_accounts_reference_v1.csv", final_accounts, list(final_accounts[0])),
        SnapshotSpec("tutkiavustuksia_okm_pilot", "official_grants_okm_pilot_v1.csv", grants, list(grants[0])),
    ]
    for spec in specs:
        _write_csv(REFERENCE_DIR / spec.filename, spec.rows, spec.fields)
    manifest = _manifest(specs)
    _write_csv(REFERENCE_DIR / "source_vintage_manifest_v1.csv", manifest, list(manifest[0]))
    return specs


def _run(command: list[str], *, stdin: str | None = None) -> None:
    result = subprocess.run(command, input=stdin, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(result.stderr or result.stdout or "command failed")


def load_bigquery(specs: list[SnapshotSpec], *, project: str, dataset: str, location: str) -> None:
    table_names = {
        "official_sector_indicators": "official_sector_indicator_v1",
        "official_deflators": "official_deflator_reference_v1",
        "official_regions": "official_region_reference_v1",
        "prh_organization_master": "organization_master_v1",
        "state_treasury_final_accounts": "official_final_accounts_reference_v1",
        "tutkiavustuksia_okm_pilot": "official_grants_okm_pilot_v1",
    }
    for spec in specs:
        table_id = f"{dataset}.{table_names[spec.source_id]}"
        _run([
            "bq", f"--project_id={project}", f"--location={location}", "load", "--replace", "--autodetect",
            "--source_format=CSV", "--skip_leading_rows=1", table_id, str(REFERENCE_DIR / spec.filename),
        ])
        print(f"BigQuery table -> {table_id} ({len(spec.rows)} rows)")
    manifest_path = REFERENCE_DIR / "source_vintage_manifest_v1.csv"
    staging = f"{dataset}._source_vintage_manifest_stage_v1"
    _run([
        "bq", f"--project_id={project}", f"--location={location}", "load", "--replace", "--autodetect",
        "--source_format=CSV", "--skip_leading_rows=1", staging, str(manifest_path),
    ])
    merge_sql = f"""
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.source_vintage_manifest_v1`
AS SELECT * FROM `{project}.{dataset}._source_vintage_manifest_stage_v1` WHERE FALSE;
CREATE OR REPLACE TABLE `{project}.{dataset}.source_vintage_manifest_v1` AS
SELECT * FROM (
  SELECT * FROM `{project}.{dataset}.source_vintage_manifest_v1`
  UNION ALL
  SELECT * FROM `{project}.{dataset}._source_vintage_manifest_stage_v1`
)
QUALIFY ROW_NUMBER() OVER (PARTITION BY vintage_id ORDER BY fetched_at DESC) = 1
"""
    _run(["bq", f"--project_id={project}", f"--location={location}", "query", "--nouse_legacy_sql"], stdin=merge_sql)
    print(f"BigQuery revision union -> {project}.{dataset}.source_vintage_manifest_v1")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh official enrichment snapshots.")
    parser.add_argument("--load-bigquery", action="store_true")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--location", default="europe-west1")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    specs = refresh()
    for spec in specs:
        print(f"{spec.filename}: {len(spec.rows)} rows")
    if args.load_bigquery:
        load_bigquery(specs, project=args.project, dataset=args.dataset, location=args.location)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

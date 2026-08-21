#!/usr/bin/env python3
"""Create reviewable municipal-finance snapshots from the official open APIs.

The loader deliberately keeps budgets, forecasts and actuals separate.  The
first semantic fact table contains KTAS budget-plan facts only.  Other public
reporting packages remain discoverable in the catalog, but are not assigned a
meaning until their official taxonomy can be processed safely.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings

REFERENCE_DIR = ROOT / "data" / "reference"
TAXONOMY_INDEX_URL = "https://api.tutkihallintoa.fi/kuntatalous/v1/taksonomia"
CATALOG_URL = "https://prodkuntarest.westeurope.cloudapp.azure.com/rest/v1/json/aineistot"
CATALOG_ENDPOINTS = (
    (CATALOG_URL, "aineistot", "tunnusluvut"),
    ("https://prodkuntarest.westeurope.cloudapp.azure.com/rest/v1/json/kklmy-aineistot", "kklmy_aineisto", "kklmy_tiedot"),
    ("https://prodkuntarest.westeurope.cloudapp.azure.com/rest/v1/json/tolt-aineistot", "tolt_aineisto", "tolt_tiedot"),
)
SOURCE_PAGE_URL = "https://www.tutkihallintoa.fi/kuntien-ja-kuntayhtymien-talous/"
FETCHED_AT = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
USER_AGENT = "Budjettihaukka/2.0 municipal-finance-refresh (+https://github.com/juntunen-ai/budjettihaukka)"

# Public reporting packages for municipalities and joint municipal authorities.
# HVA (prefix H) and Aland (prefix A) packages are intentionally outside this
# municipal-finance layer.
MUNICIPAL_PACKAGES = frozenset({
    "KKLMY", "KKNR", "KKOTR", "KKTPA", "KKTPP", "KKTR", "KKYTT",
    "KLTPA", "KLTR", "KTAS", "KTPE", "TOLT", "TOLTA", "TOLTB", "TOTT",
})

APPROVAL_RANK = {
    "alustava": 10,
    "hyvaksytty": 20,
    "lopullinen": 30,
    "jalkikorjattu": 40,
}

METRIC_IDS = {
    "Toimintatulot": "operating_income_eur",
    "Toimintamenot": "operating_expenses_eur",
    "Henkilöstömenot": "personnel_expenses_eur",
    "Palvelujen ostot": "service_purchases_eur",
    "VUOSIKATE": "annual_margin_eur",
    "TILIKAUDEN TULOS": "financial_year_result_eur",
    "Lainakanta 31.12.": "loan_stock_end_year_eur",
    "Investointimenot yhteensä (brutto)": "gross_investment_expenditure_eur",
}

PLANNING_STAGES = {
    "Laadintakauden talousarvio 202x-1 (mukaan lukien talousarviomuutokset)": ("prior_year_amended_budget", -1),
    "Talousarvio 202x": ("current_budget", 0),
    "Taloussuunnitelma 202x+1": ("plan_year_plus_1", 1),
    "Taloussuunnitelma 202x+2": ("plan_year_plus_2", 2),
}

TAXONOMY_INDEX_FIELDS = [
    "reporting_package", "taxonomy_url", "content_length_bytes", "last_modified",
    "etag", "snapshot_policy", "source_id", "source_url", "vintage_date",
]
CATALOG_FIELDS = [
    "business_id", "reporting_package", "reporting_period", "period_year",
    "period_type", "period_coverage", "is_complete_year", "accounting_stage",
    "approval_stage", "approval_rank", "approved_at", "published_at",
    "document_url", "is_selected_document", "source_id", "source_url", "vintage_date",
]
TAXONOMY_FIELDS = [
    "indicator_code", "semantic_metric_id", "indicator_name_fi", "indicator_name_sv",
    "indicator_name_en", "task_name_fi", "task_name_sv", "task_name_en",
    "planning_stage", "value_year_offset", "level", "service_class", "sector",
    "entrypoint", "taxonomy_version", "technical_taxonomy_version", "subpackage",
    "mandatory", "protected", "is_core_metric", "unit", "source_id", "source_url",
    "vintage_date",
]
FACT_FIELDS = [
    "business_id", "reporting_package", "reporting_period", "reporting_year",
    "value_year", "planning_stage", "accounting_stage", "is_budget", "is_forecast",
    "is_actual", "approval_stage", "approved_at", "published_at", "indicator_code",
    "semantic_metric_id", "indicator_name_fi", "value_raw", "value_numeric", "unit",
    "taxonomy_version", "technical_taxonomy_version", "subpackage", "comment",
    "validation_finding_count", "validation_max_severity", "validation_findings",
    "source_id", "source_url", "vintage_date",
]
DOCUMENT_FIELDS = [
    "business_id", "reporting_package", "reporting_period", "approval_stage",
    "document_url", "fetch_status", "content_sha256", "document_row_count",
    "core_row_count", "validation_finding_count", "validation_max_severity",
    "error_message", "fetched_at",
]


def _ascii_key(value: Any) -> str:
    return (
        str(value or "").strip().casefold()
        .replace("ä", "a").replace("ö", "o").replace("å", "a")
    )


def _iso_timestamp(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text.replace(" ", "T") + ("Z" if not re.search(r"(?:Z|[+-]\d\d:\d\d)$", text) else "")


def _bool(value: bool) -> str:
    return "true" if value else "false"


def _number(value: Any) -> float | None:
    if value in (None, "", ".", ".."):  # missing is never zero
        return None
    text = str(value).strip().replace("\u00a0", "").replace(" ", "")
    if "," in text and "." not in text:
        text = text.replace(",", ".")
    return float(text)


def _canonical_hash(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _get_json(url: str, *, timeout: int = 90, attempts: int = 3) -> Any:
    error: Exception | None = None
    for attempt in range(attempts):
        try:
            response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            error = exc
            if attempt + 1 < attempts:
                time.sleep(0.25 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed after {attempts} attempts: {error}")


def fetch_taxonomy_index(max_snapshot_bytes: int) -> tuple[list[dict[str, Any]], str]:
    urls = _get_json(TAXONOMY_INDEX_URL, timeout=60)
    if not isinstance(urls, list) or not urls:
        raise ValueError("Municipal taxonomy index must be a non-empty URL list")
    ktas_url = ""
    rows: list[dict[str, Any]] = []
    for url in sorted(urls):
        package = Path(urlparse(url).path).stem.upper()
        response = requests.head(url, headers={"User-Agent": USER_AGENT}, timeout=30, allow_redirects=True)
        response.raise_for_status()
        size = int(response.headers.get("Content-Length") or 0)
        if package == "KTAS":
            policy = "snapshotted"
            ktas_url = url
        elif size > max_snapshot_bytes:
            policy = "too_large_for_default_snapshot"
        else:
            policy = "available_not_selected"
        rows.append({
            "reporting_package": package,
            "taxonomy_url": url,
            "content_length_bytes": size,
            "last_modified": response.headers.get("Last-Modified", ""),
            "etag": response.headers.get("ETag", "").strip('"'),
            "snapshot_policy": policy,
            "source_id": "state_treasury_municipal_taxonomy_index",
            "source_url": TAXONOMY_INDEX_URL,
            "vintage_date": FETCHED_AT[:10],
        })
    if not ktas_url:
        raise ValueError("KTAS taxonomy URL missing from official taxonomy index")
    ktas_row = next(row for row in rows if row["reporting_package"] == "KTAS")
    if int(ktas_row["content_length_bytes"]) > max_snapshot_bytes:
        raise ValueError("KTAS taxonomy exceeds configured snapshot size limit")
    return rows, ktas_url


def _period_semantics(package: str, period: str) -> tuple[str, str, bool, str]:
    if package == "KTAS":
        return "annual_budget_plan", "full_year_budget_plan", True, "budget_plan"
    if package == "KTPE":
        return "annual_forecast", "full_year_forecast", True, "forecast"
    match = re.fullmatch(r"(\d{4})C(03|06|09|12)", period)
    if match:
        quarter = int(match.group(2)) // 3
        return "cumulative_quarter", f"cumulative_q{quarter}", quarter == 4, "cumulative_actual"
    if re.fullmatch(r"\d{4}", period):
        return "annual_report", "full_year_report", True, "reported_annual"
    return "other", "unknown", False, "unclassified"


def fetch_catalog() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, ...]] = set()
    for endpoint_url, payload_key, document_field in CATALOG_ENDPOINTS:
        payload = _get_json(endpoint_url, timeout=120)
        source_rows = payload.get(payload_key) if isinstance(payload, dict) else None
        if not isinstance(source_rows, list):
            raise ValueError(f"Municipal catalog response {endpoint_url} is missing {payload_key} list")
        for source in source_rows:
            package = str(source.get("raportointikokonaisuus") or "").upper()
            if package not in MUNICIPAL_PACKAGES:
                continue
            period = str(source.get("raportointikausi") or "")
            year_match = re.match(r"^(\d{4})", period)
            period_type, coverage, complete, accounting_stage = _period_semantics(package, period)
            approval = _ascii_key(source.get("hyvaksymisvaihe"))
            business_id = str(source.get("ytunnus") or "")
            approved_at = _iso_timestamp(source.get("hyvaksymispvm"))
            published_at = _iso_timestamp(source.get("julkaisupvm"))
            document_url = str(source.get(document_field) or "")
            identity = (business_id, package, period, approval, approved_at, published_at, document_url)
            if identity in seen:
                continue
            seen.add(identity)
            rows.append({
                "business_id": business_id,
                "reporting_package": package,
                "reporting_period": period,
                "period_year": int(year_match.group(1)) if year_match else "",
                "period_type": period_type,
                "period_coverage": coverage,
                "is_complete_year": _bool(complete),
                "accounting_stage": accounting_stage,
                "approval_stage": approval,
                "approval_rank": APPROVAL_RANK.get(approval, 0),
                "approved_at": approved_at,
                "published_at": published_at,
                "document_url": document_url,
                "is_selected_document": "false",
                "source_id": "state_treasury_municipal_finance_catalog",
                "source_url": endpoint_url,
                "vintage_date": FETCHED_AT[:10],
            })
    if not rows:
        raise ValueError("No municipal reporting packages found in official catalog")
    selected: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (row["business_id"], row["reporting_package"], row["reporting_period"])
        order = (int(row["approval_rank"]), row["approved_at"], row["published_at"], row["document_url"])
        previous = selected.get(key)
        if previous is None:
            selected[key] = row
        else:
            previous_order = (int(previous["approval_rank"]), previous["approved_at"], previous["published_at"], previous["document_url"])
            if order > previous_order:
                selected[key] = row
    for row in selected.values():
        row["is_selected_document"] = "true"
    return sorted(rows, key=lambda row: (
        row["reporting_package"], row["reporting_period"], row["business_id"],
        int(row["approval_rank"]), row["document_url"],
    ))


def fetch_ktas_taxonomy(url: str) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    payload = _get_json(url, timeout=90)
    if not isinstance(payload, list) or not payload:
        raise ValueError("KTAS taxonomy must be a non-empty list")
    rows: list[dict[str, Any]] = []
    core: dict[str, dict[str, Any]] = {}
    for source in payload:
        code = str(source.get("solutunniste") or "")
        name = str(source.get("tunnusluku") or "")
        task = str(source.get("tehtava") or "")
        metric_id = METRIC_IDS.get(name, "")
        planning_stage, offset = PLANNING_STAGES.get(task, ("", ""))
        is_core = bool(metric_id and planning_stage)
        row = {
            "indicator_code": code,
            "semantic_metric_id": metric_id if is_core else "",
            "indicator_name_fi": name,
            "indicator_name_sv": source.get("tunnusluku_sv") or "",
            "indicator_name_en": source.get("tunnusluku_en") or "",
            "task_name_fi": task,
            "task_name_sv": source.get("tehtava_sv") or "",
            "task_name_en": source.get("tehtava_en") or "",
            "planning_stage": planning_stage,
            "value_year_offset": offset,
            "level": source.get("taso") or "",
            "service_class": source.get("palvelu") or "",
            "sector": source.get("sektori") or "",
            "entrypoint": source.get("entrypoint") or "",
            "taxonomy_version": source.get("versio") or "",
            "technical_taxonomy_version": source.get("tekninen_versio") or "",
            "subpackage": source.get("osakokonaisuus") or "",
            "mandatory": source.get("pakollisuus") or "",
            "protected": source.get("suojattu") or "",
            "is_core_metric": _bool(is_core),
            "unit": "EUR" if is_core else "",
            "source_id": "state_treasury_municipal_ktas_taxonomy",
            "source_url": url,
            "vintage_date": FETCHED_AT[:10],
        }
        rows.append(row)
        if is_core:
            if code in core:
                raise ValueError(f"Duplicate KTAS core indicator code {code}")
            core[code] = row
    expected = len(METRIC_IDS) * len(PLANNING_STAGES)
    if len(core) != expected:
        missing = sorted(set(METRIC_IDS.values()) - {row["semantic_metric_id"] for row in core.values()})
        raise ValueError(f"KTAS core taxonomy expected {expected} codes, got {len(core)}; missing metrics: {missing}")
    return sorted(rows, key=lambda row: (
        not str(row["indicator_code"]).isdigit(),
        int(row["indicator_code"]) if str(row["indicator_code"]).isdigit() else str(row["indicator_code"]),
    )), core


def _severity(value: Any) -> int:
    text = _ascii_key(value)
    if text in {"virhe", "error"}:
        return 30
    if text in {"varoitus", "warning"}:
        return 20
    if text:
        return 10
    return 0


def _findings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [
        item for item in value
        if isinstance(item, dict)
        and any(str(item.get(key) or "").strip() for key in ("havainto", "tarkennus", "vakavuus"))
    ]


def _fetch_document(catalog_row: dict[str, Any], core: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    url = str(catalog_row["document_url"])
    try:
        payload = _get_json(url, timeout=90)
        if not isinstance(payload, list):
            raise ValueError("document response is not a list")
        facts: list[dict[str, Any]] = []
        finding_count = 0
        max_severity = 0
        for source in payload:
            findings = _findings(source.get("tarkastushavainnot"))
            finding_count += len(findings)
            max_severity = max([max_severity, *(_severity(item.get("vakavuus")) for item in findings)])
            code = str(source.get("tunnusluku") or "")
            taxonomy = core.get(code)
            if not taxonomy:
                continue
            planning_stage = str(taxonomy["planning_stage"])
            report_year = int(str(catalog_row["reporting_period"])[:4])
            value = source.get("arvo")
            finding_text = " | ".join(
                ": ".join(filter(None, [str(item.get("vakavuus") or ""), str(item.get("havainto") or ""), str(item.get("tarkennus") or "")]))
                for item in findings
            )
            facts.append({
                "business_id": catalog_row["business_id"],
                "reporting_package": "KTAS",
                "reporting_period": catalog_row["reporting_period"],
                "reporting_year": report_year,
                "value_year": report_year + int(taxonomy["value_year_offset"]),
                "planning_stage": planning_stage,
                "accounting_stage": "budget_plan",
                "is_budget": _bool(planning_stage in {"prior_year_amended_budget", "current_budget"}),
                "is_forecast": _bool(planning_stage.startswith("plan_year_")),
                "is_actual": "false",
                "approval_stage": _ascii_key(source.get("hyväksymisvaihe") or catalog_row["approval_stage"]),
                "approved_at": _iso_timestamp(source.get("hyväksymispvm") or catalog_row["approved_at"]),
                "published_at": catalog_row["published_at"],
                "indicator_code": code,
                "semantic_metric_id": taxonomy["semantic_metric_id"],
                "indicator_name_fi": taxonomy["indicator_name_fi"],
                "value_raw": "" if value is None else str(value),
                "value_numeric": _number(value),
                "unit": "EUR",
                "taxonomy_version": source.get("taksonomia") or taxonomy["taxonomy_version"],
                "technical_taxonomy_version": taxonomy["technical_taxonomy_version"],
                "subpackage": source.get("osakokonaisuus") or taxonomy["subpackage"],
                "comment": source.get("kommentti") or "",
                "validation_finding_count": len(findings),
                "validation_max_severity": max((_severity(item.get("vakavuus")) for item in findings), default=0),
                "validation_findings": finding_text,
                "source_id": "state_treasury_municipal_ktas_facts",
                "source_url": url,
                "vintage_date": FETCHED_AT[:10],
            })
        manifest = {
            "business_id": catalog_row["business_id"],
            "reporting_package": "KTAS",
            "reporting_period": catalog_row["reporting_period"],
            "approval_stage": catalog_row["approval_stage"],
            "document_url": url,
            "fetch_status": "ok",
            "content_sha256": _canonical_hash(payload),
            "document_row_count": len(payload),
            "core_row_count": len(facts),
            "validation_finding_count": finding_count,
            "validation_max_severity": max_severity,
            "error_message": "",
            "fetched_at": FETCHED_AT,
        }
        return facts, manifest
    except Exception as exc:  # captured into manifest; caller decides whether partial data is allowed
        return [], {
            "business_id": catalog_row["business_id"],
            "reporting_package": "KTAS",
            "reporting_period": catalog_row["reporting_period"],
            "approval_stage": catalog_row["approval_stage"],
            "document_url": url,
            "fetch_status": "error",
            "content_sha256": "",
            "document_row_count": 0,
            "core_row_count": 0,
            "validation_finding_count": 0,
            "validation_max_severity": 0,
            "error_message": str(exc)[:500],
            "fetched_at": FETCHED_AT,
        }


def fetch_ktas_facts(
    catalog: list[dict[str, Any]],
    core: dict[str, dict[str, Any]],
    *,
    start_year: int,
    end_year: int,
    workers: int,
    max_documents: int | None,
    allow_partial: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    documents = [
        row for row in catalog
        if row["reporting_package"] == "KTAS"
        and row["is_selected_document"] == "true"
        and start_year <= int(row["period_year"]) <= end_year
    ]
    documents.sort(key=lambda row: (row["reporting_period"], row["business_id"]))
    if max_documents is not None:
        documents = documents[:max_documents]
    if not documents:
        raise ValueError(f"No selected KTAS documents for {start_year}-{end_year}")
    facts: list[dict[str, Any]] = []
    manifests: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_fetch_document, row, core): row for row in documents}
        for future in as_completed(futures):
            document_facts, manifest = future.result()
            facts.extend(document_facts)
            manifests.append(manifest)
    errors = [row for row in manifests if row["fetch_status"] != "ok"]
    if errors and not allow_partial:
        examples = "; ".join(f"{row['business_id']} {row['reporting_period']}: {row['error_message']}" for row in errors[:3])
        raise RuntimeError(f"{len(errors)} KTAS documents failed; no partial snapshot written. {examples}")
    facts.sort(key=lambda row: (
        row["reporting_year"], row["business_id"], row["semantic_metric_id"], row["planning_stage"],
    ))
    manifests.sort(key=lambda row: (row["reporting_period"], row["business_id"]))
    return facts, manifests


@dataclass(frozen=True)
class Snapshot:
    table_name: str
    filename: str
    rows: list[dict[str, Any]]
    fields: list[str]


def refresh(
    *, start_year: int, end_year: int, workers: int = 12,
    max_documents: int | None = None, allow_partial: bool = False,
    max_taxonomy_bytes: int = 10_000_000,
) -> list[Snapshot]:
    index, ktas_url = fetch_taxonomy_index(max_taxonomy_bytes)
    catalog = fetch_catalog()
    taxonomy, core = fetch_ktas_taxonomy(ktas_url)
    facts, documents = fetch_ktas_facts(
        catalog, core, start_year=start_year, end_year=end_year, workers=workers,
        max_documents=max_documents, allow_partial=allow_partial,
    )
    snapshots = [
        Snapshot("municipal_finance_taxonomy_index_v1", "municipal_finance_taxonomy_index_v1.csv", index, TAXONOMY_INDEX_FIELDS),
        Snapshot("municipal_finance_catalog_v1", "municipal_finance_catalog_v1.csv", catalog, CATALOG_FIELDS),
        Snapshot("municipal_finance_ktas_taxonomy_v1", "municipal_finance_ktas_taxonomy_v1.csv", taxonomy, TAXONOMY_FIELDS),
        Snapshot("municipal_finance_ktas_core_v1", "municipal_finance_ktas_core_v1.csv", facts, FACT_FIELDS),
        Snapshot("municipal_finance_document_manifest_v1", "municipal_finance_document_manifest_v1.csv", documents, DOCUMENT_FIELDS),
    ]
    for snapshot in snapshots:
        _write_csv(REFERENCE_DIR / snapshot.filename, snapshot.rows, snapshot.fields)
    metadata = {
        "schema_version": "1.0.0",
        "fetched_at": FETCHED_AT,
        "source_page_url": SOURCE_PAGE_URL,
        "taxonomy_index_url": TAXONOMY_INDEX_URL,
        "catalog_url": CATALOG_URL,
        "catalog_urls": [url for url, _payload_key, _document_field in CATALOG_ENDPOINTS],
        "ktas_year_range": {"start": start_year, "end": end_year},
        "max_documents": max_documents,
        "is_partial_snapshot": bool(max_documents or any(row["fetch_status"] != "ok" for row in documents)),
        "tables": [
            {
                "table_name": snapshot.table_name,
                "snapshot_path": f"data/reference/{snapshot.filename}",
                "row_count": len(snapshot.rows),
                "content_sha256": _canonical_hash(snapshot.rows),
                "schema_sha256": hashlib.sha256("|".join(snapshot.fields).encode()).hexdigest(),
            }
            for snapshot in snapshots
        ],
        "semantic_guards": {
            "ktas_accounting_stage": "budget_plan",
            "ktas_actual_values_allowed": False,
            "missing_value_means_zero": False,
            "large_taxonomies_downloaded_by_default": False,
            "known_source_anomaly": "KKNR 2022C03 interest-expense hierarchy (accounts 6200-6299); quarantine when KKNR facts are added.",
        },
    }
    (REFERENCE_DIR / "municipal_finance_sources_v1.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return snapshots


def _run(command: list[str], *, stdin: str | None = None) -> None:
    result = subprocess.run(command, input=stdin, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(result.stderr or result.stdout or "command failed")


# Columns the mart compares against string literals such as '2022C03'. CSV
# autodetection types them as INT64 whenever a snapshot happens to contain only
# plain years, which makes analytics_municipal_finance_quality_v1 fail to
# compile. Pin them to STRING so the data contract does not depend on the sample.
STRING_CONTRACT_COLUMNS = ("reporting_period",)


def load_bigquery(snapshots: list[Snapshot], *, project: str, dataset: str, location: str) -> None:
    for snapshot in snapshots:
        table_id = f"{dataset}.{snapshot.table_name}"
        _run([
            "bq", f"--project_id={project}", f"--location={location}", "load", "--replace", "--autodetect",
            "--source_format=CSV", "--skip_leading_rows=1", table_id, str(REFERENCE_DIR / snapshot.filename),
        ])
        _enforce_string_columns(snapshot, project=project, dataset=dataset, location=location)
        print(f"BigQuery table -> {table_id} ({len(snapshot.rows)} rows)")


def _enforce_string_columns(snapshot: Snapshot, *, project: str, dataset: str, location: str) -> None:
    columns = [name for name in STRING_CONTRACT_COLUMNS if name in snapshot.fields]
    if not columns:
        return
    replacements = ", ".join(f"CAST({name} AS STRING) AS {name}" for name in columns)
    table_ref = f"`{project}.{dataset}.{snapshot.table_name}`"
    sql = (
        f"CREATE OR REPLACE TABLE {table_ref} AS "
        f"SELECT * REPLACE ({replacements}) FROM {table_ref}"
    )
    _run(
        ["bq", f"--project_id={project}", f"--location={location}", "query", "--nouse_legacy_sql"],
        stdin=sql,
    )


def parse_args() -> argparse.Namespace:
    current_year = datetime.now(UTC).year
    parser = argparse.ArgumentParser(description="Refresh municipal-finance reference snapshots.")
    parser.add_argument("--start-year", type=int, default=current_year - 3)
    parser.add_argument("--end-year", type=int, default=current_year)
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--max-documents", type=int)
    parser.add_argument("--allow-partial", action="store_true")
    parser.add_argument("--max-taxonomy-bytes", type=int, default=10_000_000)
    parser.add_argument("--load-bigquery", action="store_true")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--location", default="europe-west1")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.start_year > args.end_year:
        raise SystemExit("--start-year must be less than or equal to --end-year")
    snapshots = refresh(
        start_year=args.start_year,
        end_year=args.end_year,
        workers=max(1, args.workers),
        max_documents=args.max_documents,
        allow_partial=args.allow_partial,
        max_taxonomy_bytes=args.max_taxonomy_bytes,
    )
    for snapshot in snapshots:
        print(f"{snapshot.filename}: {len(snapshot.rows)} rows")
    if args.load_bigquery:
        load_bigquery(snapshots, project=args.project, dataset=args.dataset, location=args.location)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

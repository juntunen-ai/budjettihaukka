#!/usr/bin/env python3
"""Build a reviewed BigQuery snapshot of policy-relevant moment spending."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
SELECTION_PATH = ROOT / "data" / "reference" / "policy_budget_moment_selection_v1.json"
OUTPUT_PATH = ROOT / "data" / "reference" / "policy_budget_evidence_v1.json"
DEFAULT_PROJECT = "budjettihaukka-gpt"
DEFAULT_DATASET = "valtiodata"
DEFAULT_TABLE = "analytics_fiscal_yearly_core_v1"
OFFICIAL_SOURCE_URL = "https://api.tutkihallintoa.fi/valtiontalous/v1/"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _number(value: Any) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    return value


def build_query(table_id: str) -> str:
    return f"""
SELECT
  year,
  momentti_tunnusp,
  ANY_VALUE(momentti_snimi HAVING MAX net_accumulation_nominal_eur) AS source_name,
  SUM(CAST(net_accumulation_nominal_eur AS FLOAT64)) / 1000000 AS nominal_meur,
  SUM(net_accumulation_real_cpi_eur) / 1000000 AS real_meur,
  ANY_VALUE(real_base_year) AS real_base_year,
  LOGICAL_OR(has_structural_guardrail) AS has_structural_guardrail,
  LOGICAL_AND(is_complete_year) AS is_complete_year,
  ARRAY_AGG(DISTINCT comparability_status IGNORE NULLS) AS comparability_statuses,
  MAX(data_as_of) AS data_as_of,
  SUM(source_rows) AS source_rows
FROM `{table_id}`
WHERE year IN UNNEST(@years)
  AND fiscal_side = @fiscal_side
  AND momentti_tunnusp IN UNNEST(@codes)
GROUP BY year, momentti_tunnusp
ORDER BY momentti_tunnusp, year
""".strip()


def build_history_query(table_id: str) -> str:
    return f"""
SELECT
  year,
  momentti_tunnusp,
  ANY_VALUE(momentti_snimi HAVING MAX net_accumulation_nominal_eur) AS source_name,
  SUM(CAST(net_accumulation_nominal_eur AS FLOAT64)) / 1000000 AS nominal_meur,
  SUM(net_accumulation_real_cpi_eur) / 1000000 AS real_meur,
  ANY_VALUE(real_base_year) AS real_base_year,
  LOGICAL_OR(has_structural_guardrail) AS has_structural_guardrail,
  LOGICAL_AND(is_complete_year) AS is_complete_year,
  ARRAY_AGG(DISTINCT comparability_status IGNORE NULLS) AS comparability_statuses,
  MAX(data_as_of) AS data_as_of,
  SUM(source_rows) AS source_rows
FROM `{table_id}`
WHERE year BETWEEN @history_from AND @history_to
  AND fiscal_side = @fiscal_side
  AND momentti_tunnusp IN UNNEST(@codes)
GROUP BY year, momentti_tunnusp
ORDER BY momentti_tunnusp, year
""".strip()


def validate_selection(selection: dict[str, Any]) -> None:
    meta = selection.get("meta", {})
    moments = selection.get("moments", [])
    if meta.get("dataset_id") != "policy_budget_moment_selection_v1":
        raise ValueError("Unexpected moment selection dataset id")
    if meta.get("fiscal_side") != "expense":
        raise ValueError("Policy cut evidence must be restricted to expenses")
    if not meta.get("baseline_year") < meta.get("comparison_year"):
        raise ValueError("Invalid comparison period")
    codes = [item.get("momentti_tunnusp") for item in moments]
    if len(codes) != len(set(codes)) or not codes:
        raise ValueError("Moment codes must be present and unique")
    for item in moments:
        if not str(item["momentti_tunnusp"]).endswith("."):
            raise ValueError(f"Invalid moment code: {item['momentti_tunnusp']}")
        if not item.get("canonical_label_fi") or not item.get("primary_policy_id"):
            raise ValueError(f"Incomplete selection metadata: {item['momentti_tunnusp']}")
        members = item.get("historical_members", [])
        if not members:
            raise ValueError(f"Missing historical membership: {item['momentti_tunnusp']}")
        for member in members:
            if not str(member.get("code", "")).endswith("."):
                raise ValueError(f"Invalid historical code for {item['momentti_tunnusp']}")
            if member.get("valid_from_year") > member.get("valid_to_year"):
                raise ValueError(f"Invalid historical period for {item['momentti_tunnusp']}")


def fetch_rows(
    client: bigquery.Client,
    selection: dict[str, Any],
    table_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    meta = selection["meta"]
    codes = [item["momentti_tunnusp"] for item in selection["moments"]]
    years = [meta["baseline_year"], meta["comparison_year"]]
    query = build_query(table_id)
    parameters = [
        bigquery.ArrayQueryParameter("years", "INT64", years),
        bigquery.ArrayQueryParameter("codes", "STRING", codes),
        bigquery.ScalarQueryParameter("fiscal_side", "STRING", meta["fiscal_side"]),
    ]
    dry_config = bigquery.QueryJobConfig(
        query_parameters=parameters,
        dry_run=True,
        use_query_cache=False,
    )
    dry_job = client.query(query, job_config=dry_config)
    query_config = bigquery.QueryJobConfig(query_parameters=parameters, use_query_cache=True)
    rows = []
    for row in client.query(query, job_config=query_config).result():
        item = {key: _number(value) for key, value in dict(row).items()}
        if item.get("data_as_of") is not None:
            item["data_as_of"] = item["data_as_of"].isoformat()
        rows.append(item)
    history_codes = sorted(
        {
            member["code"]
            for selected in selection["moments"]
            for member in selected["historical_members"]
        }
    )
    history_query = build_history_query(table_id)
    history_parameters = [
        bigquery.ScalarQueryParameter("history_from", "INT64", 2008),
        bigquery.ScalarQueryParameter("history_to", "INT64", 2025),
        bigquery.ArrayQueryParameter("codes", "STRING", history_codes),
        bigquery.ScalarQueryParameter("fiscal_side", "STRING", meta["fiscal_side"]),
    ]
    history_dry_config = bigquery.QueryJobConfig(
        query_parameters=history_parameters,
        dry_run=True,
        use_query_cache=False,
    )
    history_dry_job = client.query(history_query, job_config=history_dry_config)
    history_config = bigquery.QueryJobConfig(
        query_parameters=history_parameters,
        use_query_cache=True,
    )
    history_rows = []
    for row in client.query(history_query, job_config=history_config).result():
        item = {key: _number(value) for key, value in dict(row).items()}
        if item.get("data_as_of") is not None:
            item["data_as_of"] = item["data_as_of"].isoformat()
        history_rows.append(item)
    total_dry_bytes = int(dry_job.total_bytes_processed or 0) + int(
        history_dry_job.total_bytes_processed or 0
    )
    return rows, history_rows, total_dry_bytes


def build_snapshot(
    selection: dict[str, Any],
    source_rows: list[dict[str, Any]],
    historical_source_rows: list[dict[str, Any]],
    *,
    source_table: str,
    dry_run_bytes: int,
    generated_at: str,
) -> dict[str, Any]:
    validate_selection(selection)
    meta = selection["meta"]
    baseline_year = meta["baseline_year"]
    comparison_year = meta["comparison_year"]
    by_key = {(row["momentti_tunnusp"], row["year"]): row for row in source_rows}
    history_by_key = {
        (row["momentti_tunnusp"], row["year"]): row for row in historical_source_rows
    }
    evidence_rows = []

    for selected in selection["moments"]:
        code = selected["momentti_tunnusp"]
        baseline = by_key.get((code, baseline_year))
        comparison = by_key.get((code, comparison_year))
        if not baseline or not comparison:
            raise ValueError(f"Missing comparison year for {code}")
        for row in (baseline, comparison):
            if row.get("is_complete_year") is not True:
                raise ValueError(f"Incomplete year for {code}: {row.get('year')}")
            if row.get("has_structural_guardrail") is True:
                raise ValueError(f"Structural guardrail blocks direct comparison for {code}")
            if row.get("real_base_year") != meta["real_price_base_year"]:
                raise ValueError(f"Unexpected real price base for {code}")

        nominal_change = comparison["nominal_meur"] - baseline["nominal_meur"]
        real_change = comparison["real_meur"] - baseline["real_meur"]
        if real_change >= 0:
            raise ValueError(f"Selected moment does not show a real decline: {code}")
        nominal_pct = 100 * nominal_change / baseline["nominal_meur"]
        real_pct = 100 * real_change / baseline["real_meur"]
        evidence_class = (
            "nominal_and_real_reduction" if nominal_change < 0 else "real_value_reduction_only"
        )
        history_by_year: dict[int, dict[str, Any]] = {}
        for member in selected["historical_members"]:
            for year in range(member["valid_from_year"], member["valid_to_year"] + 1):
                source = history_by_key.get((member["code"], year))
                if not source:
                    continue
                target = history_by_year.setdefault(
                    year,
                    {
                        "year": year,
                        "nominal_meur": 0.0,
                        "real_meur": 0.0,
                        "source_codes": [],
                        "source_names": [],
                        "has_structural_guardrail": False,
                        "is_complete_year": True,
                        "comparability_statuses": [],
                    },
                )
                target["nominal_meur"] += source["nominal_meur"]
                target["real_meur"] += source["real_meur"]
                target["source_codes"].append(member["code"])
                target["source_names"].append(source["source_name"])
                target["has_structural_guardrail"] |= source["has_structural_guardrail"]
                target["is_complete_year"] &= source["is_complete_year"]
                target["comparability_statuses"] = sorted(
                    set(target["comparability_statuses"] + source["comparability_statuses"])
                )
        history = []
        for year in sorted(history_by_year):
            item = history_by_year[year]
            item["nominal_meur"] = round(item["nominal_meur"], 6)
            item["real_meur"] = round(item["real_meur"], 6)
            history.append(item)
        evidence_rows.append(
            {
                **selected,
                "baseline": baseline,
                "comparison": comparison,
                "nominal_change_meur": round(nominal_change, 6),
                "nominal_change_pct": round(nominal_pct, 6),
                "real_change_meur": round(real_change, 6),
                "real_change_pct": round(real_pct, 6),
                "evidence_class": evidence_class,
                "claim_status": "verified_actual_spending_decline",
                "history": history,
                "history_has_structural_guardrails": any(
                    item["has_structural_guardrail"] for item in history
                ),
            }
        )

    evidence_rows.sort(key=lambda item: item["real_change_meur"])
    return {
        "meta": {
            "dataset_id": "policy_budget_evidence_v1",
            "schema_version": "1.1.0",
            "generated_at": generated_at,
            "source_project": source_table.split(".")[0],
            "source_table": source_table,
            "source_metric_fi": "Valtion budjettitalouden toteutunut nettokertymä menopuolella",
            "official_source_name": "Valtiokonttori, Tutkihallintoa.fi valtiontalouden kuukausidata",
            "official_source_url": OFFICIAL_SOURCE_URL,
            "baseline_year": baseline_year,
            "comparison_year": comparison_year,
            "history_from_year": 2008,
            "history_to_year": 2025,
            "real_price_base_year": meta["real_price_base_year"],
            "fiscal_side": meta["fiscal_side"],
            "query_dry_run_bytes": dry_run_bytes,
            "selection_dataset": meta["dataset_id"],
            "claim_rule_fi": meta["claim_rule_fi"],
            "interpretation_fi": meta["non_claim_fi"],
        },
        "rows": evidence_rows,
        "excluded_comparisons": selection.get("excluded_comparisons", []),
    }


def validate_snapshot(snapshot: dict[str, Any], selection: dict[str, Any]) -> None:
    meta = snapshot.get("meta", {})
    rows = snapshot.get("rows", [])
    if meta.get("dataset_id") != "policy_budget_evidence_v1":
        raise ValueError("Unexpected budget evidence dataset id")
    if len(rows) != len(selection["moments"]):
        raise ValueError("Budget evidence row count differs from reviewed selection")
    if {row["momentti_tunnusp"] for row in rows} != {
        row["momentti_tunnusp"] for row in selection["moments"]
    }:
        raise ValueError("Budget evidence codes differ from reviewed selection")
    for row in rows:
        if row["claim_status"] != "verified_actual_spending_decline":
            raise ValueError(f"Unverified budget evidence: {row['momentti_tunnusp']}")
        if row["real_change_meur"] >= 0:
            raise ValueError(f"Non-decline in budget evidence: {row['momentti_tunnusp']}")
        if row["evidence_class"] == "nominal_and_real_reduction":
            if row["nominal_change_meur"] >= 0:
                raise ValueError(f"Invalid nominal reduction: {row['momentti_tunnusp']}")
        elif row["evidence_class"] == "real_value_reduction_only":
            if row["nominal_change_meur"] < 0:
                raise ValueError(f"Invalid real-only reduction: {row['momentti_tunnusp']}")
        else:
            raise ValueError(f"Unknown evidence class: {row['evidence_class']}")
        history = row.get("history", [])
        if not history or history != sorted(history, key=lambda item: item["year"]):
            raise ValueError(f"Invalid historical series: {row['momentti_tunnusp']}")
        if any(item["is_complete_year"] is not True for item in history):
            raise ValueError(f"Incomplete historical year: {row['momentti_tunnusp']}")
        history_values = {item["year"]: item for item in history}
        for key, expected in (
            (meta["baseline_year"], row["baseline"]["real_meur"]),
            (meta["comparison_year"], row["comparison"]["real_meur"]),
        ):
            if key not in history_values or abs(history_values[key]["real_meur"] - expected) > 0.001:
                raise ValueError(f"Historical comparison mismatch for {row['momentti_tunnusp']}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    args = parser.parse_args()

    selection = load_json(SELECTION_PATH)
    validate_selection(selection)
    table_id = f"{args.project}.{args.dataset}.{args.table}"
    client = bigquery.Client(project=args.project)
    source_rows, historical_source_rows, dry_run_bytes = fetch_rows(client, selection, table_id)
    snapshot = build_snapshot(
        selection,
        source_rows,
        historical_source_rows,
        source_table=table_id,
        dry_run_bytes=dry_run_bytes,
        generated_at=datetime.now(UTC).replace(microsecond=0).isoformat(),
    )
    validate_snapshot(snapshot, selection)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(args.output.relative_to(ROOT))
    print(f"  {len(snapshot['rows'])} verified moment comparisons")
    print(f"  dry run: {dry_run_bytes:,} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

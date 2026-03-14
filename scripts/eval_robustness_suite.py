#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings
from utils.analysis_spec_utils import infer_analysis_spec
from utils.bigquery_utils import _build_bigquery_fallback_sql
from utils.semantic_query_contracts import build_contract_sql, choose_contract, contract_template_order
from utils.visualization_plan_utils import extract_intent_signals, template_order


def _pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def _safe_ratio(ok: int, total: int) -> float:
    if total == 0:
        return 0.0
    return ok / total


def _load_cases(path: Path) -> list[dict]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        return list(raw.get("cases", []))
    if isinstance(raw, list):
        return raw
    raise ValueError("Invalid dataset format")


def _infer_sql_shape(sql: str) -> str:
    normalized = " ".join((sql or "").lower().split())
    if "alamomentti_tunnus" in normalized and "alkuvuosi_sum" in normalized and "loppuvuosi_sum" in normalized:
        return "top_growth_alamoment"
    if "momentti_tunnusp" in normalized and "alkuvuosi_sum" in normalized and "loppuvuosi_sum" in normalized:
        return "top_growth_moment"
    if "lag(nettokertyma_sum) over (order by vuosi)" in normalized and "muutos_eur" in normalized:
        return "yoy_change"
    if "group by vuosi, hallinnonala" in normalized and "nettokertyma_sum" in normalized:
        return "trend_by_hallinnonala"
    if "group by vuosi, kk, hallinnonala" in normalized:
        return "monthly_hallinnonala"
    if "group by vuosi, hallinnonala" in normalized:
        return "yearly_hallinnonala"
    if "count(*) as rows_count" in normalized:
        return "count_rows"
    return "generic_select"


def _primary_template(spec, contract_name: str | None, question: str) -> str:
    if contract_name:
        contract_templates = contract_template_order(contract_name)
        if contract_templates:
            return contract_templates[0]
    return template_order(spec, extract_intent_signals(question))[0]


def evaluate(dataset: Path, show_failures: int) -> int:
    cases = _load_cases(dataset)
    totals = {
        "intent_ok": 0,
        "contract_ok": 0,
        "sql_shape_ok": 0,
        "viz_ok": 0,
        "all_ok": 0,
    }
    failures: list[str] = []

    for case in cases:
        expected = case.get("expected", {}) or {}
        question = str(case.get("question", "")).strip()

        spec = infer_analysis_spec(question)
        predicted_intent = spec.intent
        predicted_contract = choose_contract(spec)
        predicted_viz = _primary_template(spec, predicted_contract, question)

        if predicted_contract:
            sql, _ = build_contract_sql(spec, settings.full_table_id)
        else:
            sql = _build_bigquery_fallback_sql(question)
        predicted_shape = _infer_sql_shape(sql or "")

        expected_intent = expected.get("intent")
        expected_contract = expected.get("contract")
        expected_shape = expected.get("sql_shape")
        expected_viz = expected.get("primary_template")

        intent_ok = predicted_intent == expected_intent
        contract_ok = predicted_contract == expected_contract
        shape_ok = predicted_shape == expected_shape if expected_shape else True
        viz_ok = predicted_viz == expected_viz
        all_ok = intent_ok and contract_ok and shape_ok and viz_ok

        totals["intent_ok"] += int(intent_ok)
        totals["contract_ok"] += int(contract_ok)
        totals["sql_shape_ok"] += int(shape_ok)
        totals["viz_ok"] += int(viz_ok)
        totals["all_ok"] += int(all_ok)

        if not all_ok:
            failures.append(
                (
                    f"{case.get('id', '?')}: "
                    f"intent {expected_intent}->{predicted_intent}, "
                    f"contract {expected_contract}->{predicted_contract}, "
                    f"shape {expected_shape}->{predicted_shape}, "
                    f"viz {expected_viz}->{predicted_viz} | q={question}"
                )
            )

    total = len(cases)
    intent_acc = _safe_ratio(totals["intent_ok"], total)
    contract_acc = _safe_ratio(totals["contract_ok"], total)
    shape_acc = _safe_ratio(totals["sql_shape_ok"], total)
    viz_acc = _safe_ratio(totals["viz_ok"], total)
    all_acc = _safe_ratio(totals["all_ok"], total)

    print(f"Dataset: {dataset}")
    print(f"Cases: {total}")
    print(f"Intent accuracy: {_pct(intent_acc)} ({totals['intent_ok']}/{total})")
    print(f"Contract accuracy: {_pct(contract_acc)} ({totals['contract_ok']}/{total})")
    print(f"SQL-shape accuracy: {_pct(shape_acc)} ({totals['sql_shape_ok']}/{total})")
    print(f"Visualization accuracy: {_pct(viz_acc)} ({totals['viz_ok']}/{total})")
    print(f"All-match score: {_pct(all_acc)} ({totals['all_ok']}/{total})")

    if failures and show_failures > 0:
        print("")
        print(f"Top {min(show_failures, len(failures))} mismatches:")
        for row in failures[:show_failures]:
            print(f"- {row}")

    if all_acc < 0.85 or intent_acc < 0.90 or shape_acc < 0.90:
        print("\nEvaluation FAILED threshold gates.")
        return 2

    print("\nEvaluation PASSED threshold gates.")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate NL->SQL->viz robustness.")
    parser.add_argument(
        "--dataset",
        type=Path,
        default=ROOT / "data" / "evals" / "robustness_goldens.json",
        help="Path to robustness dataset json.",
    )
    parser.add_argument("--show-failures", type=int, default=20)
    args = parser.parse_args()
    raise SystemExit(evaluate(args.dataset, args.show_failures))


if __name__ == "__main__":
    main()

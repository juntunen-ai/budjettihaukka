#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.analysis_spec_utils import infer_analysis_spec
from utils.semantic_query_contracts import choose_contract, contract_template_order
from utils.visualization_plan_utils import extract_intent_signals, template_order


def _safe_ratio(ok: int, total: int) -> float:
    if total == 0:
        return 0.0
    return ok / total


def _pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def _load_cases(path: Path) -> list[dict]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        return list(raw.get("cases", []))
    if isinstance(raw, list):
        return raw
    raise ValueError("Invalid golden dataset format.")


def evaluate(golden_path: Path, show_failures: int) -> int:
    cases = _load_cases(golden_path)
    if not cases:
        print("No cases found in golden dataset.")
        return 1

    totals = {
        "intent_ok": 0,
        "contract_ok": 0,
        "template_ok": 0,
        "all_ok": 0,
        "critical_total": 0,
        "critical_ok": 0,
    }
    failures: list[str] = []

    for case in cases:
        question = str(case.get("question", ""))
        expected = case.get("expected", {}) or {}

        spec = infer_analysis_spec(question)
        predicted_intent = spec.intent
        predicted_contract = choose_contract(spec)
        contract_templates = contract_template_order(predicted_contract)
        if contract_templates:
            predicted_template = contract_templates[0]
        else:
            predicted_template = template_order(spec, extract_intent_signals(question))[0]

        expected_intent = expected.get("intent")
        expected_contract = expected.get("contract")
        expected_template = expected.get("primary_template")
        critical = bool(expected.get("critical", False))

        intent_ok = predicted_intent == expected_intent
        contract_ok = predicted_contract == expected_contract
        template_ok = predicted_template == expected_template
        all_ok = intent_ok and contract_ok and template_ok

        totals["intent_ok"] += int(intent_ok)
        totals["contract_ok"] += int(contract_ok)
        totals["template_ok"] += int(template_ok)
        totals["all_ok"] += int(all_ok)

        if critical:
            totals["critical_total"] += 1
            totals["critical_ok"] += int(all_ok)

        if not all_ok:
            failures.append(
                (
                    f"{case.get('id', '?')}: expected intent={expected_intent}, contract={expected_contract}, "
                    f"template={expected_template} | got intent={predicted_intent}, contract={predicted_contract}, "
                    f"template={predicted_template} | q={question}"
                )
            )

    total = len(cases)
    intent_acc = _safe_ratio(totals["intent_ok"], total)
    contract_acc = _safe_ratio(totals["contract_ok"], total)
    template_acc = _safe_ratio(totals["template_ok"], total)
    all_acc = _safe_ratio(totals["all_ok"], total)
    critical_acc = _safe_ratio(totals["critical_ok"], totals["critical_total"])

    print(f"Golden set: {golden_path}")
    print(f"Cases: {total}")
    print(f"Intent accuracy:   {_pct(intent_acc)} ({totals['intent_ok']}/{total})")
    print(f"Contract accuracy: {_pct(contract_acc)} ({totals['contract_ok']}/{total})")
    print(f"Template accuracy: {_pct(template_acc)} ({totals['template_ok']}/{total})")
    print(f"All-match score:   {_pct(all_acc)} ({totals['all_ok']}/{total})")
    print(
        f"Critical all-match: {_pct(critical_acc)} "
        f"({totals['critical_ok']}/{totals['critical_total']})"
    )

    if failures and show_failures > 0:
        print("")
        print(f"Top {min(show_failures, len(failures))} mismatches:")
        for line in failures[:show_failures]:
            print(f"- {line}")

    # Phase 4 acceptance gates
    if all_acc < 0.85 or critical_acc < 0.95:
        print("")
        print("Evaluation FAILED threshold gates (all-match >= 85% and critical >= 95%).")
        return 2

    print("")
    print("Evaluation PASSED threshold gates.")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate Budjettihaukka visualization intent pipeline.")
    parser.add_argument(
        "--golden",
        type=Path,
        default=ROOT / "data" / "evals" / "visualization_goldens.json",
        help="Path to golden query dataset JSON.",
    )
    parser.add_argument(
        "--show-failures",
        type=int,
        default=15,
        help="How many mismatch rows to print.",
    )
    args = parser.parse_args()
    exit_code = evaluate(args.golden, args.show_failures)
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()

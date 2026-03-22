#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from domain.contracts import AnalyticsFrame
from services.ontology_resolver import resolve_analysis
from services.semantic_parser import parse_question
from services.visualization_planner import build_visualization_plan


@dataclass
class Failure:
    case_id: str
    question: str
    message: str
    critical: bool = False


@dataclass
class Summary:
    total: int = 0
    passed: int = 0
    runtime_cases: int = 0
    runtime_passed: int = 0
    critical_failures: int = 0
    failures: list[Failure] = field(default_factory=list)


DUMMY_FRAME = AnalyticsFrame(frame_type="synthetic", columns=["value"], rows=[{"value": 1}], row_count=1)


def _load_cases(path: Path) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def _prefix_match(code: str, prefixes: list[str]) -> bool:
    return any(code.startswith(prefix) for prefix in prefixes)


def _used_moment_codes(rows: list[dict[str, Any]]) -> list[str]:
    return [str(row.get("momentti_tunnusp") or "").strip() for row in rows if str(row.get("momentti_tunnusp") or "").strip()]


def _check_case(case: dict[str, Any], run_runtime: bool) -> list[Failure]:
    failures: list[Failure] = []
    question = str(case["question"])
    parsed = parse_question(question)
    resolved = resolve_analysis(question, parsed.analysis_spec)

    def fail(message: str, critical: bool = False) -> None:
        failures.append(Failure(case_id=str(case["id"]), question=question, message=message, critical=critical))

    expected_concept_id = case.get("expected_concept_id")
    if parsed.analysis_spec.resolved_concept_id != expected_concept_id:
        fail(
            f"expected concept {expected_concept_id!r}, got {parsed.analysis_spec.resolved_concept_id!r}",
            critical=True,
        )

    expected_fiscal_side = case.get("expected_fiscal_side")
    if expected_fiscal_side and parsed.analysis_spec.fiscal_side != expected_fiscal_side:
        fail(f"expected fiscal_side {expected_fiscal_side!r}, got {parsed.analysis_spec.fiscal_side!r}")

    expected_intent = case.get("expected_intent")
    if expected_intent and parsed.analysis_spec.intent != expected_intent:
        fail(f"expected intent {expected_intent!r}, got {parsed.analysis_spec.intent!r}")

    expected_clarification = case.get("expected_clarification_required")
    if expected_clarification is not None and parsed.clarification_required != bool(expected_clarification):
        fail(
            f"expected clarification_required={bool(expected_clarification)}, got {parsed.clarification_required}",
            critical=bool(expected_clarification),
        )

    expected_observability = case.get("expected_observability_class")
    if expected_observability and resolved.observability_class != expected_observability:
        fail(f"expected observability {expected_observability!r}, got {resolved.observability_class!r}")

    expected_primary_chart = case.get("expected_primary_chart")
    if expected_primary_chart:
        plan = build_visualization_plan(question, parsed.analysis_spec, None, DUMMY_FRAME)
        got_chart = plan.primary_chart if plan else None
        if got_chart != expected_primary_chart:
            fail(f"expected primary_chart {expected_primary_chart!r}, got {got_chart!r}")

    runtime_needed = run_runtime and bool(case.get("runtime_assertions") or case.get("expected_status") or case.get("expected_verification_status") or case.get("allowed_moment_prefixes") or case.get("disallowed_moment_prefixes"))
    if runtime_needed:
        from services.analysis_orchestrator import analyze_question

        result = analyze_question(question)
        expected_status = case.get("expected_status")
        if expected_status and result.status != expected_status:
            fail(f"expected runtime status {expected_status!r}, got {result.status!r}", critical=True)
        expected_verification = case.get("expected_verification_status")
        if expected_verification and result.verification_status != expected_verification:
            fail(
                f"expected verification_status {expected_verification!r}, got {result.verification_status!r}",
                critical=True,
            )
        used_codes = _used_moment_codes(result.used_moments)
        allowed_prefixes = case.get("allowed_moment_prefixes") or []
        if allowed_prefixes and used_codes:
            invalid = [code for code in used_codes if not _prefix_match(code, allowed_prefixes)]
            if invalid:
                fail(f"used moment codes outside allowed prefixes: {invalid[:5]}", critical=True)
        disallowed_prefixes = case.get("disallowed_moment_prefixes") or []
        if disallowed_prefixes and used_codes:
            leaking = [code for code in used_codes if _prefix_match(code, disallowed_prefixes)]
            if leaking:
                fail(f"used moment codes hit disallowed prefixes: {leaking[:5]}", critical=True)

    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description="Run semantic gold regression suite.")
    parser.add_argument(
        "--cases",
        type=Path,
        default=ROOT / "data" / "evals" / "semantic_goldens_v1.json",
    )
    parser.add_argument("--skip-runtime", action="store_true")
    args = parser.parse_args()

    cases = _load_cases(args.cases)
    summary = Summary(total=len(cases))
    for index, case in enumerate(cases, start=1):
        if summary.total >= 20 and index % 10 == 0:
            print(f"Progress: evaluating case {index}/{summary.total}", flush=True)
        failures = _check_case(case, run_runtime=not args.skip_runtime)
        runtime_needed = bool(case.get("runtime_assertions") or case.get("expected_status") or case.get("expected_verification_status") or case.get("allowed_moment_prefixes") or case.get("disallowed_moment_prefixes"))
        if runtime_needed and not args.skip_runtime:
            summary.runtime_cases += 1
        if failures:
            summary.failures.extend(failures)
            summary.critical_failures += sum(1 for item in failures if item.critical)
        else:
            summary.passed += 1
            if runtime_needed and not args.skip_runtime:
                summary.runtime_passed += 1

    pass_rate = summary.passed / summary.total if summary.total else 1.0
    print(f"Semantic goldens: passed {summary.passed}/{summary.total} ({pass_rate:.1%})")
    if not args.skip_runtime:
        print(f"Runtime subset: passed {summary.runtime_passed}/{summary.runtime_cases}")
    if summary.failures:
        print("Failures:")
        for failure in summary.failures[:50]:
            critical = "CRITICAL" if failure.critical else "WARN"
            print(f"- [{critical}] {failure.case_id}: {failure.message} :: {failure.question}")
        if len(summary.failures) > 50:
            print(f"... and {len(summary.failures) - 50} more")

    if pass_rate < 0.90 or summary.critical_failures > 0:
        raise SystemExit(1)

    print("Semantic goldens PASSED")


if __name__ == "__main__":
    main()

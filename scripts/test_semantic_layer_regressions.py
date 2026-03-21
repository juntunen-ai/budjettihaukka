#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.analysis_orchestrator import analyze_question  # noqa: E402
from utils.analysis_spec_utils import infer_analysis_spec  # noqa: E402


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _moment_codes(rows: list[dict]) -> set[str]:
    codes: set[str] = set()
    for row in rows:
        value = str(row.get("momentti_tunnusp") or "").strip()
        if value:
            codes.add(value)
    return codes


def main() -> None:
    asumis_spec = infer_analysis_spec("Miten asumistuen menot ovat kehittyneet 2000-2024?")
    assert_true(asumis_spec.resolved_concept_id == "asumistuki", f"unexpected concept: {asumis_spec.resolved_concept_id}")
    assert_true(asumis_spec.fiscal_side == "expense", f"unexpected fiscal side: {asumis_spec.fiscal_side}")
    asumis_result = analyze_question("Miten asumistuen menot ovat kehittyneet 2000-2024?")
    assert_true(asumis_result.status == "success", f"unexpected asumistuki status: {asumis_result.status}")
    assert_true(asumis_result.query_source == "yearly_agg", f"unexpected asumistuki source: {asumis_result.query_source}")
    assert_true(_moment_codes(asumis_result.used_moments) == {"33.10.54.", "35.30.54."}, f"unexpected asumistuki moments: {asumis_result.used_moments}")

    defense_spec = infer_analysis_spec("Miten puolustusmenot kehittyivät 2018-2024?")
    assert_true(defense_spec.resolved_concept_id == "puolustus", f"unexpected defense concept: {defense_spec.resolved_concept_id}")
    assert_true(defense_spec.fiscal_side == "expense", f"unexpected defense fiscal side: {defense_spec.fiscal_side}")
    defense_result = analyze_question("Miten puolustusmenot kehittyivät 2018-2024?")
    assert_true(defense_result.status == "success", f"unexpected defense status: {defense_result.status}")
    defense_codes = _moment_codes(defense_result.used_moments)
    assert_true(defense_codes, "defense evidence should contain moment codes")
    assert_true(all(code.startswith("27.") for code in defense_codes), f"unexpected defense codes: {sorted(defense_codes)}")

    cuts_spec = infer_analysis_spec("Mitäs momenteista on leikattu prosentuaalisesti eniten 2008-2020?")
    assert_true(cuts_spec.intent == "top_cuts", f"unexpected cuts intent: {cuts_spec.intent}")
    assert_true(cuts_spec.fiscal_side == "expense", f"unexpected cuts fiscal side: {cuts_spec.fiscal_side}")
    cuts_result = analyze_question("Mitäs momenteista on leikattu prosentuaalisesti eniten 2008-2020?")
    assert_true(cuts_result.status == "success", f"unexpected cuts status: {cuts_result.status} / {cuts_result.error}")
    assert_true(len(cuts_result.result_rows) >= 1, "cuts result should contain rows")
    cut_codes = _moment_codes(cuts_result.used_moments)
    assert_true(
        not any(code.startswith(prefix) for code in cut_codes for prefix in ("11.", "12.", "13.", "14.", "15.")),
        f"cuts should exclude revenue and financing codes: {sorted(cut_codes)}",
    )

    revenue_spec = infer_analysis_spec("Mistä verokertymä pieneni eniten 2008-2020?")
    assert_true(revenue_spec.intent == "revenue_decline", f"unexpected revenue intent: {revenue_spec.intent}")
    assert_true(revenue_spec.fiscal_side == "revenue", f"unexpected revenue fiscal side: {revenue_spec.fiscal_side}")
    revenue_result = analyze_question("Mistä verokertymä pieneni eniten 2008-2020?")
    assert_true(revenue_result.status == "success", f"unexpected revenue status: {revenue_result.status} / {revenue_result.error}")
    assert_true(len(revenue_result.result_rows) >= 1, "revenue result should contain rows")
    revenue_codes = _moment_codes(revenue_result.used_moments)
    assert_true(revenue_codes, "revenue evidence should contain codes")
    assert_true(all(code.startswith(("11.", "12.", "13.", "14.")) for code in revenue_codes), f"revenue evidence should be revenue side: {sorted(revenue_codes)}")
    excluded_name_snippets = (
        "osinkotulot",
        "veikkauksen",
        "raha-automaattiyhdistyksen",
        "sakkorahat",
        "osakemyynnistä",
    )
    revenue_names = [str(row.get("momentti_snimi") or "").lower() for row in revenue_result.used_moments]
    assert_true(
        not any(snippet in name for name in revenue_names for snippet in excluded_name_snippets),
        f"revenue evidence should exclude non-tax revenue items: {revenue_names}",
    )

    print("Semantic layer regression tests PASSED")


if __name__ == "__main__":
    main()

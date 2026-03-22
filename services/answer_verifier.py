from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from domain.contracts import ResolvedAnalysis
from utils.analysis_spec_utils import AnalysisSpec
from utils.budget_semantics import classify_moment_fiscal_side, normalize_fiscal_side
from utils.bigquery_utils import get_concept_bridge_runtime_codes, get_concept_bridge_summary


@dataclass
class VerificationResult:
    verification_status: str
    warnings: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


def _moment_codes(rows: list[dict[str, Any]]) -> list[str]:
    codes: list[str] = []
    for row in rows:
        code = str(row.get("momentti_tunnusp") or "").strip()
        if code:
            codes.append(code)
    return codes


def verify_answer(
    *,
    question: str,
    analysis_spec: AnalysisSpec,
    resolved: ResolvedAnalysis,
    result_rows: list[dict[str, Any]],
    used_moments: list[dict[str, Any]],
    execution_error: str | None = None,
) -> VerificationResult:
    if execution_error:
        return VerificationResult("error")

    warnings: list[str] = []
    metadata: dict[str, Any] = {}
    observability = (resolved.observability_class or "").strip().lower()

    if observability == "unsupported":
        warnings.append("Kysymystä ei voida tällä hetkellä mitata luotettavasti käytettävissä olevasta budjettidatasta.")
        return VerificationResult("unsupported", warnings=warnings, metadata={"observability_class": observability})

    if not result_rows:
        if analysis_spec.intent in {"top_growth", "top_cuts", "revenue_decline"}:
            warnings.append("Semanttinen suodatus poisti kaikki tulokset; kysymys tarvitsee tarkemman rajauksen.")
            return VerificationResult("needs_clarification", warnings=warnings, metadata={"observability_class": observability})
        if observability == "proxy":
            warnings.append("Valittu käsite on vain osittain havaittavissa budjettidatasta tällä aikavälillä.")
            return VerificationResult("trusted_with_warning", warnings=warnings, metadata={"observability_class": observability})
        return VerificationResult("trusted_with_warning", warnings=["Kysely ei palauttanut rivejä."], metadata={"observability_class": observability})

    bridge_summary = get_concept_bridge_summary(
        resolved.concept_id,
        analysis_spec.time_from,
        analysis_spec.time_to,
    )
    metadata["bridge_summary"] = bridge_summary

    bridge_codes = get_concept_bridge_runtime_codes(
        resolved.concept_id,
        analysis_spec.time_from,
        analysis_spec.time_to,
    )
    used_codes = _moment_codes(used_moments)
    if bridge_codes and used_codes:
        covered = sum(1 for code in used_codes if code in bridge_codes)
        bridge_coverage = covered / max(len(used_codes), 1)
        metadata["bridge_coverage"] = round(bridge_coverage, 4)
        if bridge_coverage < 0.5:
            warnings.append("Käytetyt momentit eivät vastaa riittävän hyvin konseptin hyväksyttyä momenttijoukkoa.")
            return VerificationResult("needs_clarification", warnings=warnings, metadata=metadata)
        if bridge_coverage < 0.85:
            warnings.append("Osa käytetyistä momenteista jäi konseptin vahvimman bridge-rajauksen ulkopuolelle.")

    expected_side = normalize_fiscal_side(resolved.fiscal_side)
    if expected_side not in {"unknown", "mixed"} and used_moments:
        mismatches = 0
        classified = 0
        for row in used_moments:
            side = classify_moment_fiscal_side(row.get("momentti_tunnusp"), row.get("momentti_snimi"))
            if side in {"unknown", "mixed", "technical"}:
                continue
            classified += 1
            if side != expected_side:
                mismatches += 1
        if classified:
            mismatch_rate = mismatches / classified
            metadata["fiscal_side_mismatch_rate"] = round(mismatch_rate, 4)
            if mismatch_rate > 0.2:
                warnings.append("Tuloksessa on semanttisesti väärään budjettipuoleen kuuluvia momentteja.")
                return VerificationResult("needs_clarification", warnings=warnings, metadata=metadata)

    if observability == "proxy":
        warnings.append(resolved.observability_reason or "Käsite on vain osittain havaittavissa budjettidatasta.")
        return VerificationResult("trusted_with_warning", warnings=warnings, metadata=metadata)

    if observability == "composite":
        warnings.append(resolved.observability_reason or "Käsite koostuu useista budjettimomenteista.")
        return VerificationResult("trusted_with_warning" if warnings else "trusted", warnings=warnings, metadata=metadata)

    return VerificationResult("trusted_with_warning" if warnings else "trusted", warnings=warnings, metadata=metadata)

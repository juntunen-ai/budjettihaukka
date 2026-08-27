from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api.app import _to_response
from domain.contracts import AnalyzeResult, ResolvedAnalysis
from utils.analysis_spec_utils import infer_analysis_spec


def main() -> None:
    spec = infer_analysis_spec("Miten puolustusmenot kehittyivät 2018-2024?")
    resolved = ResolvedAnalysis(
        question="Miten puolustusmenot kehittyivät 2018-2024?",
        analysis_spec=spec,
        concept_id="puolustus",
        concept_label="Puolustus",
        observability_class="composite",
    )
    result = AnalyzeResult(
        status="success",
        question=resolved.question,
        execution_question=resolved.question,
        analysis_spec=replace(spec),
        resolved_analysis=resolved,
        analytics_frame=None,
        visualization_plan=None,
        verification_status="trusted_with_warning",
    )

    response = _to_response(result)
    assert response.verification_status == "trusted_with_warning"
    assert response.resolved_analysis["concept_id"] == "puolustus"
    print("API contract verification status: ok")


if __name__ == "__main__":
    main()

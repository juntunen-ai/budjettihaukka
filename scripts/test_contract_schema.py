#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.analysis_spec_utils import infer_analysis_spec
from utils.semantic_query_contracts import contract_template_order, normalize_contract_result


def main() -> None:
    spec = infer_analysis_spec("Mitkä ovat eniten kasvaneet momentit 2020-2024?")
    df_top = pd.DataFrame(
        {
            "momentti_tunnusp": ["27.10.01.", "29.40.50."],
            "momentti_snimi": ["Puolustusvoimien toimintamenot", "Valtionrahoitus yliopistojen toimintaan"],
            "alkuvuosi_sum": [100.0, 200.0],
            "loppuvuosi_sum": [150.0, 260.0],
            "kasvu_eur": [50.0, 60.0],
            "kasvu_pct": [50.0, 30.0],
        }
    )
    canonical_top = normalize_contract_result(df_top, "top_growth_moment", spec)
    assert list(canonical_top.columns) == ["time", "entity", "metric", "delta", "pct"]
    assert canonical_top["entity"].notna().all()
    assert canonical_top["delta"].notna().all()

    df_trend = pd.DataFrame(
        {
            "vuosi": [2020, 2021, 2020, 2021],
            "hallinnonala": ["A", "A", "B", "B"],
            "nettokertyma_sum": [10.0, 12.0, 20.0, 19.0],
        }
    )
    canonical_trend = normalize_contract_result(df_trend, "trend_by_hallinnonala", spec)
    assert canonical_trend["time"].notna().all()
    assert canonical_trend["entity"].nunique() == 2
    assert "trend" in contract_template_order("trend_by_hallinnonala")

    print("Contract schema tests PASSED")


if __name__ == "__main__":
    main()

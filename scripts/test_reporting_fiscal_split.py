from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from streamlit_app import _add_fiscal_side_columns, _fiscal_side_bucket, _pick_breakdown_value_column


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> None:
    df = pd.DataFrame(
        [
            {"momentti_tunnusp": "11.01.01.", "momentti_snimi": "Ansio- ja pääomatuloverot", "nettokertyma_sum": -1000},
            {"momentti_tunnusp": "27.10.01.", "momentti_snimi": "Puolustusvoimien toimintamenot", "nettokertyma_sum": 2500},
            {"momentti_tunnusp": "15.03.01.", "momentti_snimi": "Nettolainanotto ja velanhallinta", "nettokertyma_sum": 300},
        ]
    )

    enriched = _add_fiscal_side_columns(df)
    assert_true("fiscal_side" in enriched.columns, "expected fiscal_side column")
    assert_true("budjettipuoli" in enriched.columns, "expected budjettipuoli column")
    assert_true("budjettiryhma" in enriched.columns, "expected budjettiryhma column")

    side_map = dict(zip(enriched["momentti_tunnusp"], enriched["fiscal_side"]))
    assert_true(side_map["11.01.01."] == "revenue", f"unexpected revenue side: {side_map}")
    assert_true(side_map["27.10.01."] == "expense", f"unexpected expense side: {side_map}")
    assert_true(side_map["15.03.01."] == "financing", f"unexpected financing side: {side_map}")

    assert_true(_fiscal_side_bucket("revenue") == "revenue", "revenue should stay revenue bucket")
    assert_true(_fiscal_side_bucket("expense") == "expense", "expense should stay expense bucket")
    assert_true(_fiscal_side_bucket("financing") == "other", "financing should map to other bucket")
    assert_true(_pick_breakdown_value_column(enriched) == "nettokertyma_sum", "expected nettokertyma_sum breakdown column")

    print("Reporting fiscal split tests PASSED")


if __name__ == "__main__":
    main()

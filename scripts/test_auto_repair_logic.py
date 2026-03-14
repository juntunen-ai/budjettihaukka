#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.bigquery_utils import _classify_error, _repair_sql_from_error


def main() -> None:
    syntax_err = "Syntax error: Unexpected keyword LIMIT at [1:10]"
    assert _classify_error(syntax_err) == "syntax_error"

    limit_err = "Resources exceeded during query execution"
    sql = "SELECT SAFE_CAST(`Vuosi` AS INT64) AS vuosi FROM `x.y.z` WHERE SAFE_CAST(`Vuosi` AS INT64) BETWEEN 2000 AND 2024 LIMIT 1000"
    repaired = _repair_sql_from_error(sql, limit_err, attempt=1)
    assert repaired is not None
    assert "LIMIT" in repaired

    lower_err = "No matching signature for function LOWER for argument type INT64"
    sql_lower = "SELECT LOWER(SAFE_CAST(`Vuosi` AS INT64)) FROM `x.y.z` WHERE SAFE_CAST(`Vuosi` AS INT64) BETWEEN 2000 AND 2024 LIMIT 10"
    repaired_lower = _repair_sql_from_error(sql_lower, lower_err, attempt=1)
    assert repaired_lower is not None
    assert "CAST(" in repaired_lower

    print("Auto-repair logic tests PASSED")


if __name__ == "__main__":
    main()

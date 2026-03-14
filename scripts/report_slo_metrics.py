#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.observability_utils import read_query_events, summarize_slo


def _pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize Budjettihaukka query SLO metrics.")
    parser.add_argument("--limit", type=int, default=0, help="Only use the latest N events (0 = all).")
    args = parser.parse_args()

    limit = args.limit if args.limit > 0 else None
    events = read_query_events(limit=limit)
    metrics = summarize_slo(events)

    target_query_success = 0.99
    target_chart_success = 0.98

    print(f"Events: {metrics['total']}")
    print(
        f"query_success: {_pct(metrics['query_success'])} "
        f"(target >= {_pct(target_query_success)})"
    )
    print(
        f"chart_render_success: {_pct(metrics['chart_render_success'])} "
        f"(target >= {_pct(target_chart_success)})"
    )
    print(f"clarification_rate: {_pct(metrics['clarification_rate'])}")

    failed = []
    if metrics["query_success"] < target_query_success:
        failed.append("query_success")
    if metrics["chart_render_success"] < target_chart_success:
        failed.append("chart_render_success")

    if failed:
        print(f"SLO status: FAIL ({', '.join(failed)})")
        raise SystemExit(2)

    print("SLO status: PASS")


if __name__ == "__main__":
    main()

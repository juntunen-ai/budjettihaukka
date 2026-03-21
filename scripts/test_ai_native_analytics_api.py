#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api.app import app
from services.analysis_orchestrator import analyze_question
from utils.analysis_spec_utils import infer_analysis_spec


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> None:
    result = analyze_question('miten asumistuen menot ovat kehittyneet 2000-2024')
    assert_true(result.status == 'success', 'orchestrator status should be success')
    assert_true(result.query_source == 'yearly_agg', 'expected yearly_agg source')
    assert_true(result.analytics_frame is not None, 'analytics frame missing')
    assert_true(result.analytics_frame.frame_type == 'time_series', 'unexpected frame type')
    assert_true(len(result.used_moments) >= 1, 'used moments missing')

    client = TestClient(app)
    response = client.post('/v1/analyze', json={'question': 'miten asumistuen menot ovat kehittyneet 2000-2024'})
    assert_true(response.status_code == 200, f'unexpected status code: {response.status_code}')
    body = response.json()
    assert_true(body['status'] == 'success', 'api status should be success')
    assert_true(body['query_source'] == 'yearly_agg', 'api should expose yearly_agg source')
    assert_true(len(body.get('result_rows') or []) >= 1, 'api result rows missing')
    assert_true(len(body.get('used_moments') or []) >= 1, 'api used moments missing')

    cut_spec = infer_analysis_spec('Mitäs momenteista on leikattu prosentuaalisesti eniten 2008-2020?')
    assert_true(cut_spec.intent == 'top_cuts', f'unexpected cut intent: {cut_spec.intent}')
    assert_true(cut_spec.fiscal_side == 'expense', f'unexpected cut fiscal side: {cut_spec.fiscal_side}')
    assert_true(cut_spec.entity_level == 'momentti', f'unexpected cut entity level: {cut_spec.entity_level}')

    decline_spec = infer_analysis_spec('Mistä verokertymä pieneni eniten 2008-2020?')
    assert_true(decline_spec.intent == 'revenue_decline', f'unexpected revenue decline intent: {decline_spec.intent}')
    assert_true(decline_spec.fiscal_side == 'revenue', f'unexpected revenue fiscal side: {decline_spec.fiscal_side}')

    print('AI-native analytics API tests PASSED')


if __name__ == '__main__':
    main()

# Budjettihaukka Visualization Improvement Plan

## Goal
Improve chart relevance so visualizations match user intent reliably, especially for ambiguous natural-language questions.

## Principles
- One interpretation per query run, shown to user before execution.
- Clarify only when needed (low confidence), do not force extra steps on every query.
- Use intent-specific chart templates instead of generic defaults.
- Always show metric/unit/time coverage explicitly.

## Phase 1 (Now): Interpretation Layer + Optional Clarifications
### Scope
- Parse question into structured `AnalysisSpec`:
  - `intent`, `metric`, `entity_level`, `growth_type`, `time_range`, `ranking`.
- Show interpretation summary in UI before running query.
- Show 1-2 optional clarification controls when ambiguity is detected.
- Inject selected clarifications back into execution prompt.

### Acceptance Criteria
- For ambiguous growth questions, user can choose `€` vs `%`.
- For moment/alamoment queries, user can choose granularity when unclear.
- UI always shows the interpreted scope and assumptions.

## Phase 2: Visual Plan Templates
### Scope
- Map `AnalysisSpec.intent` to chart templates:
  - `top_growth`: horizontal rank bars + change table
  - `trend`: line chart + yoy bars
  - `composition`: stacked area/bars
  - `seasonality`: year-month heatmap
- Add chart “fit checks” (skip irrelevant charts).

### Acceptance Criteria
- `top_growth` never renders generic trend line as primary.
- Long category labels remain readable (horizontal bars).
- Unit labels and axis meaning are always visible.

## Phase 3: Semantic Query Layer
### Scope
- Build stable query contracts per intent:
  - `top_growth_moment`
  - `top_growth_alamoment`
  - `trend_by_hallinnonala`
  - `yoy_change`
- Move ad hoc SQL generation behind these contracts where possible.

### Acceptance Criteria
- Same question yields consistent shape/columns.
- Visualization engine receives predictable schema.

## Phase 4: Quality and Evaluation
### Scope
- Create gold test set (50-100 queries) with expected:
  - interpretation
  - SQL shape
  - chart type
- Add regression checks for chart relevance.

### Acceptance Criteria
- >= 85% “intent matched” on test set.
- Critical queries (top growth, trend, compare years) pass at >= 95%.

## Clarification Policy
- Ask clarifying options only if interpretation confidence < 75% or key dimensions are missing.
- Maximum two clarification controls per query.
- Provide a default recommendation to keep flow fast.

## UX Output Standard
- Always display:
  - interpreted intent
  - selected metric
  - entity level
  - time range (requested vs effective)
  - unit (`€`, `milj. €`, `mrd €`, `%`)
- If data range was clipped, show a compact notice.

## Immediate Next Tickets
1. Add ranking-mode chart template (horizontal bars + dual metric toggle `kasvu_eur`/`kasvu_pct`).
2. Add explicit coverage card (`requested`, `effective`, `missing years`).
3. Add chart titles generated from `AnalysisSpec` (not generic headings).

## Implementation Status (2026-03-09)
- Phase 1 implemented:
  - `utils/analysis_spec_utils.py` infers structured `AnalysisSpec`
  - Streamlit UI shows interpretation summary + scope cards + optional clarifications
  - Clarification choices are injected back into execution question
- Phase 2 implemented:
  - Template-driven visual plan with fit checks in `streamlit_app.py`
  - `top_growth` primary rendering is horizontal ranking bars
  - Explicit axis/unit captions and scope-aware chart titles
- Phase 3 implemented:
  - Contract-first query path in `utils/bigquery_utils.py`
  - Stable contracts in `utils/semantic_query_contracts.py`
  - Query metadata returned (`query_source`, `query_contract`, `analysis_spec`)
- Phase 4 implemented:
  - Golden set at `data/evals/visualization_goldens.json` (52 queries)
  - Evaluator script at `scripts/eval_visualization_pipeline.py`
  - Threshold gates: all-match >= 85%, critical >= 95%
- Phase 5 (robustness hardening) implemented:
  - Auto-repair loop in `utils/bigquery_utils.py` (dry-run/execute errors -> 1-2 repair attempts -> deterministic fallback-contract)
  - Contract-based canonical schema for visualization in `utils/semantic_query_contracts.py` (`time`, `entity`, `metric`, `delta`, `pct`)
  - Mandatory clarifications at low confidence in `streamlit_app.py`
  - Observability logging (`utils/observability_utils.py`) and SLO report script (`scripts/report_slo_metrics.py`)
  - Expanded robustness dataset (`data/evals/robustness_goldens.json`, 320 cases)

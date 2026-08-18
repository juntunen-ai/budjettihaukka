# Budjettihaukka — System Architecture & GCP Setup

_Last updated: 2026-07-07_

Budjettihaukka is a Finnish-language natural-language analytics service over
Finnish state budget data. A user asks a question in Finnish; the system
interprets it into a structured analysis spec, generates **deterministic SQL
via contracts** (no free-form LLM SQL), executes it on BigQuery behind a
security gate, and renders the answer as charts + explanation with an
explicit trust badge.

## 1. Component map

```
┌─────────────────────────────────────────────────────────────────┐
│ streamlit_app.py          Streamlit UI (port 8501)              │
│   theme, query input, trust badges, charts (Altair),            │
│   clarification flow, question library, usage metering          │
└──────────────┬──────────────────────────────────────────────────┘
               │ (in-process by default; optional HTTP via api/)
┌──────────────▼──────────────────────────────────────────────────┐
│ api/app.py                FastAPI "AI-native analytics API"     │
│   POST /analyze — same pipeline as the UI, JSON responses       │
│   CORS restricted via BUDJETTIHAUKKA_CORS_ORIGINS               │
└──────────────┬──────────────────────────────────────────────────┘
┌──────────────▼──────────────────────────────────────────────────┐
│ services/                 Orchestration layer                   │
│   analysis_orchestrator → semantic_parser → ontology_resolver   │
│   → analytics_engine → execution_adapter → analytics_frames     │
│   → visualization_planner → answer_verifier → explanation       │
└──────────────┬──────────────────────────────────────────────────┘
┌──────────────▼──────────────────────────────────────────────────┐
│ utils/                    Domain logic                          │
│   analysis_spec_utils     NL → AnalysisSpec (intent, entity,    │
│                           time range, growth type, confidence)  │
│   semantic_query_contracts AnalysisSpec → contract SQL          │
│   bigquery_utils          SQL security gate, auto-repair loop,  │
│                           concept bridge, execution             │
│   ontology_utils          YAML ontology, alias precision        │
│   budget_semantics        fiscal-side / intent keyword rules    │
│   schema_snapshot_utils   source schema drift detection         │
│   vertex_ai_utils         Gemini (Vertex AI or AI Studio)       │
│   observability_utils     JSONL query event log                 │
└──────────────┬──────────────────────────────────────────────────┘
┌──────────────▼──────────────────────────────────────────────────┐
│ BigQuery (GCP)            Data platform — see section 3         │
└─────────────────────────────────────────────────────────────────┘
```

**Question → answer flow:** question → `infer_analysis_spec()` (heuristic
token matching + ontology concept resolution, produces confidence score) →
mandatory clarification if confidence < 0.75 → `choose_contract()` picks a
SQL contract (`top_growth_moment`, `trend_by_hallinnonala`, `yoy_change`) or
blocks unsupported alamomentti requests before SQL and otherwise falls back to a deterministic
fallback SQL builder → SQL security gate → BigQuery → contract-shaped
DataFrame (`time`, `entity`, `metric`, `delta`, `pct`) → visualization
template + verified explanation + trust badge.

**LLM use is optional and bounded:** Gemini generates a structured QueryPlan
JSON (`BUDJETTIHAUKKA_ENABLE_LLM_QUERY_PLAN`, default off) — never raw SQL
that runs unchecked. Every executed statement passes the security gate.

### SQL security gate (`utils/bigquery_utils.enforce_sql_security`)

- Only `SELECT`/`WITH` statements
- Physical table whitelist (semantic layer + yearly agg only)
- Year bounds clamped to the data range (1998–2025)
- `LIMIT` cap (`BUDJETTIHAUKKA_SQL_MAX_LIMIT`, default 1000)
- `sqlglot` AST parse before and after transforms
- Cost gate: dry-run byte estimate must stay under
  `BUDJETTIHAUKKA_MAX_QUERY_BYTES` (default 1 GB)
- The auto-repair loop (max `BUDJETTIHAUKKA_BQ_AUTO_REPAIR_ATTEMPTS`
  attempts) re-runs **every** repaired statement through the gate

## 2. Data pipeline

```
Valtiokonttori API (api.tutkihallintoa.fi, monthly CSVs 1998→)
        │  scripts/ingest_valtiokonttori_to_bigquery.py
        │  • header discovery + ASCII snake_case normalization
        │  • SCHEMA DRIFT GATE: aborts (exit 3) if source headers change
        │    vs data/schema_snapshots/valtiokonttori_source_columns.json;
        │    accept explicitly with --accept-schema-drift
        │  • manifest table (resume/skip) — written via LOAD JOBS,
        │    not streaming inserts (sandbox-compatible)
        ▼
valtiontalous_raw                       (raw, normalized column names)
        │  scripts/build_bq_data_quality_layer.py --semantic-version N
        │  • raw-naming auto-detect: original Valtiokonttori headers
        │    (legacy budjettidata) or normalized (new ingest) — a compat
        │    CTE maps normalized → original so one curated SQL serves both
        │  • type-robust: TRIM(CAST(col AS STRING)) tolerates INT64/FLOAT
        │    autodetected raw columns
        ▼
valtiontalous_curated_dq_v              (typed, quality flags, fingerprints)
dim_hallinnonala / dim_momentti / dim_maararahalaji / dim_talousarviotili / dim_alamomentti (vain virallisesti validoitu) / dim_hierarchy_name_mapping
        ▼
valtiontalous_semantic_v{N}             (versioned analytics view:
        │                                raw-compatible aliases + canonical
        │                                names + quality columns)
        ▼
valtiontalous_semantic_current          (promotion alias — THE table the
        │                                app reads; BUDJETTIHAUKKA_TABLE)
        ▼
valtiontalous_yearly_agg_v1             (yearly aggregate for contracts)
```

**Versioning & rollback:** the app only ever reads the alias. Build a new
version with `--semantic-version N+1`; roll back with
`--semantic-version N --promote-only` (repoints the alias, no rebuild).

**Quality gates** (`scripts/run_bq_data_quality_checks.py`): validity,
freshness, duplicates, missing dimensions, plus a `schema_drift` check
comparing the live raw table against the accepted snapshot. Exit code 2 on
FAIL — designed to gate a future scheduled pipeline.

**Offline test suites** (no BigQuery needed, run before any release):

| Suite | What it guards |
|---|---|
| `scripts/eval_robustness_suite.py` (320 goldens) | intent / contract / SQL-shape / viz template accuracy — gate ≥ 95.6% |
| `scripts/eval_visualization_pipeline.py` (52 goldens) | visualization pipeline |
| `scripts/test_semantic_view_column_compat.py` | every generated SQL column exists in the semantic layer; fail-closed alamomentti requests intentionally produce no SQL |
| `scripts/test_schema_drift_detection.py` | drift detection unit tests |
| `scripts/test_ui_no_crash_smoke.py` | UI renders without crashing |

**Semantic enrichment layer** (`data/semantic_enrichment/`): full 2002–2025
crawl of budjetti.vm.fi budget documents → 19,438 evidence rows mapping
budget codes (osasto/luku/momentti) to justification text. Feeds the concept
bridge and ontology vocabulary; wiring evidence snippets into user-visible
explanations is the next milestone.

**Ontology** (`data/ontology/budjettihaukka_ontology.yaml`): concepts
(koulutus, puolustus, …), aliases with precision tiers, membership rules,
visualization recipes, guardrails. Loaded to BigQuery `ontology_*` tables by
`scripts/load_budget_ontology.py`; also read locally at runtime.

## 3. GCP settings

### Projects

| Project | Role | Notes |
|---|---|---|
| `budjettihaukka-gpt` | **Active** data project | BigQuery **sandbox (free tier)** — see limitations below |
| `valtion-budjetti-data` | Legacy | Sandbox; original `budjettidata` raw table (34-column older export, missing alamomentti columns — superseded by fresh ingest) |

### BigQuery

- **Dataset:** `valtiodata`, location **`europe-west1`**
  (note: `BUDJETTIHAUKKA_LOCATION` default `us-central1` refers to Vertex AI,
  not the BigQuery dataset)
- **Tables/views in the active project:** `valtiontalous_raw`,
  `valtiontalous_ingest_manifest`, `valtiontalous_curated_dq_v`, `dim_*`,
  `valtiontalous_semantic_v1`, `valtiontalous_semantic_current`,
  `valtiontalous_yearly_agg_v1`, plus legacy `budjettidata` (copy)
- **Auth:** Application Default Credentials via
  `gcloud auth application-default login` (user credentials, no service
  account key on disk). ADC quota project: `budjettihaukka-gpt`.
  `GOOGLE_APPLICATION_CREDENTIALS` is only needed for service-account
  deployments (e.g. Cloud Run).
- **App config:** the app reads `BUDJETTIHAUKKA_PROJECT_ID` +
  `BUDJETTIHAUKKA_DATASET` + `BUDJETTIHAUKKA_TABLE`
  (default `valtiontalous_semantic_current`). Machine-local overrides go in
  `.env.local` (gitignored), e.g. `BUDJETTIHAUKKA_PROJECT_ID=budjettihaukka-gpt`.

### BigQuery sandbox (free tier) limitations — and how the code copes

| Sandbox limitation | Impact | Accommodation in this repo |
|---|---|---|
| **No streaming inserts** (`insertAll` denied) | Ingest manifest writes failed | Manifest rows written via **load jobs** (`load_table_from_json`) — free and allowed in sandbox |
| **No DML** (UPDATE/DELETE/MERGE) | Cannot mutate rows in place | Entire pipeline is `CREATE OR REPLACE` (views/tables) + `WRITE_APPEND`/`WRITE_TRUNCATE` load/copy jobs |
| **Default table expiration ≤ 60 days** on sandbox datasets | Tables can silently vanish | `ingest --sandbox-expiration-days N` manages it; `0` skips setting expiration entirely (billed mode). **Check dataset default expiration if data disappears.** |
| **1 TB/month free query processing** | Cost/quota ceiling | App enforces a per-query dry-run byte cap (`BUDJETTIHAUKKA_MAX_QUERY_BYTES`, 1 GB default); contracts aggregate via the small yearly-agg table where possible |
| **10 GB free storage** | Raw ≈ 1.3 GB + layers — fine today; VM evidence + future sources will approach it | Keep large JSONL artifacts in git/GCS, load selectively |
| No billing export / BI Engine / scheduled queries | No native scheduling | Scheduling is planned via external cron (GitHub Actions / Cloud Scheduler) invoking the scripts |

### Known operational quirks

- `gcloud` CLI user login (`gcloud auth login`) is separate from ADC; `bq`
  CLI needs the former. All repo scripts use the Python client + ADC only.
- Vertex AI (Gemini via `BUDJETTIHAUKKA_GEMINI_MODEL`) requires the Vertex
  API enabled on the project **and billing** — on a pure sandbox project use
  AI Studio instead by setting `GEMINI_API_KEY` (the app auto-switches,
  `settings.llm_provider`). With neither, the app still works fully via
  deterministic contracts; only optional LLM query-plan assist is off.
- The robustness eval has 14 known-hard golden cases that have never passed
  (spaced compounds like "ala momentit"); the gate baseline is 95.6%.
- The visualization eval currently passes all 52 cases, including all 23
  critical cases. It is a blocking CI gate.

## 4. Runtime & deployment

- **Local dev:** `.venv` (Python 3.12, `uv`), `streamlit run streamlit_app.py`
  (port 8501). Optional API: `uvicorn api.app:app` (port 8000) +
  `BUDJETTIHAUKKA_USE_BACKEND_API=true`.
- **Dependencies:** `requirements.txt` is trimmed to what live code imports
  (streamlit, pandas, google-cloud-bigquery, google-cloud-aiplatform,
  google-genai, fastapi, sqlglot, pyyaml, altair, matplotlib).
- **Deployment target:** Cloud Run (Dockerfile pending — next milestone
  together with CI).
- **Observability:** every query logs a JSONL event
  (`agent_data/query_observability.jsonl`) with contract, confidence,
  retries, dry-run bytes, render template, error class.
  `scripts/report_slo_metrics.py` reports against SLOs
  (query_success > 99%, chart_render_success > 98%).

## 5. Next milestones

1. **CI (GitHub Actions):** run the offline suites on every push — the eval
   gate caught a 52.8% silent regression precisely because it wasn't in CI.
2. **Scheduled ingest** (monthly cron): ingest → build layer → DQ checks →
   eval gates, alert on failure.
3. **Phase 5 enrichment:** surface VM budget justification snippets in
   explanations (grounded, cited answers).
4. **Learning loop:** observed alias precision from query logs → ontology
   candidate promotion behind eval gates.

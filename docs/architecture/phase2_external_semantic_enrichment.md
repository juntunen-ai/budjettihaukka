# Phase 2 External Semantic Enrichment

Phase 2 adds controlled terminology enrichment from external public-sector concept sources.
The purpose is to improve concept resolution without allowing external aliases to flow straight
into runtime matching.

## Sources in scope

- `Finto` via the official REST API
- `sanastot.suomi.fi` concept pages, parsed from their public `__NEXT_DATA__` payload

## Guardrail

External aliases are harvested into a **review queue**, not into production ontology aliases.
They must remain outside runtime until explicitly reviewed and promoted.

## Artifacts

- Seed config:
  - `/Users/harrijuntunen/budjettihaukka/data/ontology/external_semantic_seed.yaml`
- Ingest script:
  - `/Users/harrijuntunen/budjettihaukka/scripts/ingest_external_semantic_candidates.py`
- Candidate output:
  - `/Users/harrijuntunen/budjettihaukka/data/ontology/external_semantic_candidates_v1.jsonl`
- Report:
  - `/Users/harrijuntunen/budjettihaukka/docs/reports/external_semantic_candidates_report.md`

## Candidate policy

- `pref` / `recommended` labels become `candidate` aliases with higher precision scores
- `hidden` / `synonym` labels become `candidate` aliases with medium precision scores
- `not_recommended` labels are preserved as legacy user-language evidence, but still remain `candidate`
- nothing is auto-promoted into the production ontology from these sources

## Why this is safe

This keeps external terminology useful for review and future precision work, while avoiding the
main failure mode: adding broad or ambiguous official terms directly into runtime concept matching.

## Next promotion step

A later promotion pipeline should:

1. review candidate aliases concept-by-concept
2. reject broad or collision-prone aliases
3. promote only approved aliases into `budjettihaukka_ontology.yaml`
4. rerun alias precision and semantic golden tests before release

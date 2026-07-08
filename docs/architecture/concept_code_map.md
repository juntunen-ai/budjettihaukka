# Concept→budget-code map (Phase 1a)

The map answers the question the app previously got wrong: *which budget
moments does a concept like "koulutus" actually mean?* String-matching momentti
names cannot answer it (the largest school-funding line of the 2010s,
`28.90.30`, contains no education words), so mappings are **human-decided
data**, not query-time heuristics.

## Process

1. **Investigation** — for each concept, an evidence dossier is built from the
   curated BigQuery data: hierarchy structure per era, money per year, code
   reuse, cross-branch candidates, structural breaks.
2. **Review dossier** — the genuinely political/definitional choices are
   isolated into explicit questions with money-at-stake and a recommendation.
   A human judge (project owner) decides. Dossier #1 (koulutus, 2026-07-08):
   - Q1 opintotuki → **C**: included as separable `component`
   - Q2 research/Academy → **A**: excluded, mapped to concept `tutkimus`
   - Q3 municipal VOS lump (`28.90.30`) → **C**: excluded with a standing
     disclosure; share-estimate view is future enrichment
   - Q4 cross-branch items (työvoimakoulutus, opiskelijaterveydenhuolto) →
     **A**: excluded, belong to their own concepts
3. **Encoding** — verdicts become `data/ontology/concept_code_map/<concept>.yaml`:
   rules at luku or momentti level with **inclusive year ranges** (codes are
   reused across eras: `29.10` meant university funding before 2007, basic
   education after 2016), `role: include | component | exclude`, and
   `target_concept` so excluded money pre-populates future concepts.
4. **Loading** — `scripts/load_concept_code_map.py` validates the YAML, loads
   `concept_code_map_v1` to BigQuery and builds `concept_yearly_totals_v1`
   (momentti rules override luku rules).
5. **Validation** — the yearly series is checked for continuity (renumbering
   reforms must be invisible; only real policy events may show) and
   cross-checked against Statistics Finland's COFOG aggregate.

## Koulutus validation results (2026-07-08)

- Series 1998→2025 is continuous through the 2007–2011 luku renumbering
  (mapping absorbs it). Visible events are real history: 2010 −14 % =
  VOS reform (disclosed per Q3), 2017–2019 opintotuki cuts, 2020 COVID.
- 2024 total: 6.6 B€ (opetus 5.7 + opintotuki 0.9), research separated
  ~0.5 B€.
- COFOG cross-check (StatFin 12a6, S1311 × G09, consolidated): 4.6 B€ in
  2024. Same order of magnitude and direction. The gap is expected:
  national accounts consolidate university funding (universities' own
  expenditure vs. our state-transfer view), treat opintotuki partly under
  social protection, and are accrual-based. Documented, not a defect.
- **No public momentti→COFOG mapping exists** (confirmed by source research
  2026-07-08; Statistics Finland holds one internally, unpublished). This map
  is therefore an original dataset; COFOG aggregates serve only as sanity
  anchors.

## Next concepts

Precedents set by dossier #1 (transfer-vs-service, lump-sum handling,
research-vs-teaching) carry over. Queue: terveys, sosiaaliturva, puolustus,
tutkimus (pre-populated by Q2 exclusions), kulttuuri (pre-populated),
työllisyys, maahanmuutto, kehitysapu, poliisi, maatalous.

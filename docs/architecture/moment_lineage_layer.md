# Moment Lineage Layer

Budjettihaukan `moment_lineage`-kerros mallintaa historiallisia rakennemuutoksia, jotta momenttikohtaiset trendi-, kasvu- ja leikkausanalyysit eivät sekoita toisiinsa:

- aidon leikkauksen tai kasvun
- momentin uudelleennimeämisen
- momentin siirron toiseen budjettikohtaan
- yhden momentin pilkkoutumisen useaksi momentiksi (`split`)
- usean momentin yhdistymisen yhdeksi momentiksi (`merge`)

## BigQuery-objektit

Kerros luo seuraavat view't:

- `moment_semantic_context_v1`
- `moment_node_catalog_v1`
- `moment_lineage_candidates_v1`
- `moment_lineage_v1`
- `moment_structural_change_guardrails_v1`
- `valtiontalous_yearly_agg_guarded_v1`

## Lähteet

Layer käyttää kahta lähdettä:

- numeerinen vuositasoinen aggregaatti `valtiontalous_yearly_agg_v1`
- semanttinen VM-rikastus `vm_budget_semantic_evidence`

## Heuristiikat

Lineage muodostetaan seuraavilla signaaleilla:

- nimen täsmällinen osuma
- nimien token-overlap / Jaccard-similarity
- VM-evidenssin yhteinen semanttinen konteksti
- hallinnonalan jatkuvuus
- koodin luku-/prefix-jatkuvuus
- euromääräinen jatkuvuus siirtymävuoden yli
- ajan läheisyys (`last_year -> first_year`)

## Runtime-käyttö

Ranking-kyselyissä (`top_growth`, `top_cuts`, `revenue_decline`) käytetään `valtiontalous_yearly_agg_guarded_v1`-näkymää.

Jos momentille havaitaan tarkasteluvälillä vahva rakenteellinen muutos, se suodatetaan rankingista pois. Tällä vältetään tilanteet, joissa momentin siirto tai uudelleennimeäminen näyttäytyy käyttäjälle virheellisesti leikkauksena tai kasvuna.

## Rakennus

```bash
cd /Users/harrijuntunen/budjettihaukka
.venv/bin/python scripts/build_moment_lineage_layer.py
```

## Testi

```bash
cd /Users/harrijuntunen/budjettihaukka
.venv/bin/python scripts/test_moment_lineage_layer.py
```

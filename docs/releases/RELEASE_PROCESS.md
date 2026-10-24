# Public release process (Budjettihaukka)

This repository is the **GPL-3.0-only public application shell and public reference data**.
It is **not** the deployable production analysis service. The analytical engine,
loaders, ontology, and SQL are maintained separately and must not be copied into
this repository or its Git history.

## Branches

- `main`: reviewed public source history and release tags.
- `develop`: integration branch for the next public change.
- Feature branches: branch from `develop`, open a PR into `develop`.
- Promote `develop` to `main` only through a reviewed PR after checks pass.

The `main` and `develop` branches should be protected from force-push and
deletion. Require successful CI and pull-request review for `main`. Enforce
settings in GitHub; this document does not itself configure protection.

## Public release gates

1. Confirm the precise scope: public code and reference data only; review all
   changed files for credentials, personal information, and proprietary data
   processing logic. Inspect the diff and, when needed, Git history.
2. Confirm the change is compatible with GPL-3.0-only and the data provenance
   and licensing boundaries documented in `LICENSING.md`.
3. Wait for `CI` to pass on `develop` and on the release PR to `main`:
   Python compilation, the public budget-reference consistency tests, and
   the frontend TypeScript/Vite build.
4. For a release involving public budget figures, validate source year,
   budget hierarchy, sums, rounding, and source documentation explicitly.
   Tests validate internal consistency, not the authoritative public figures.
5. Verify `scripts/deploy_firebase.sh` still **refuses** production deployment
   from this source tree and `/v1/analyze` remains clearly documented as
   unavailable in the public shell.
6. Merge the reviewed release PR; confirm `main` CI passes. Update
   `CHANGELOG.md`, `README.md`, version metadata, and
   `docs/releases/vX.Y.Z.md` to reflect exactly what the tag contains.
7. Create the annotated version tag on the reviewed `main` commit and publish
   a GitHub Release describing scope, known limitations, and provenance.
   Verify that the tag resolves to the intended commit and that release notes
   and source archives are accessible.
8. Record the released commit SHA and a link to the successful checks. Do not
   claim that publishing a public-source release updated production.

## Production is separate

The live application at https://valtion-budjetti-data.web.app is backed by
the separately maintained deployable service. Never run Firebase or Cloud Run
production deployment from this public source tree, and never interpret a
healthy public-shell build as an end-to-end analytical-system smoke test.

Before making **any** claim that a change is live in production, separately
verify the actual deployed Cloud Run revision, Firebase Hosting version,
authenticated API behavior, and dataset/semantic-layer versions with authorized
access. Keep a rollback target and deployment evidence.

## Current boundary (2026-10-24)

- Public GitHub Release `v3.0.0` was published on 2026-09-29. Its annotated
  tag targets commit `fdf911992f4ff736961c5c6e916b11015adf11f1`.
- `main` includes a later 2027 alternative-budget change at
  `97d3f897693d7a94749a2a9de101070356b9891e`. It has passed
  `main` CI but has **not** been incorporated into a newer public release.
- A future public release needs its own version, review and release checks;
  the production deployment state has not been established by GitHub CI.

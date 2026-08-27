# P0 Contract Baseline

These files are generated from source without importing the legacy application. Files with a
`.yaml` suffix intentionally contain deterministic JSON, which is a YAML 1.2 subset and keeps
the P0 exporter independent of project dependencies.

Generate and verify:

```bash
python3 scripts/refactor/export_manifests.py
python3 scripts/refactor/check_manifests.py
python3 scripts/refactor/scan_fixture_secrets.py verification/fixtures
python3 -m unittest discover -s verification/contracts -p 'test_*.py'
```

`ownership-overrides.json` is the durable lifecycle overlay. Later stages update that file to
record `new_owner`, `status`, fixture assignments, and approved retirement without losing the
source-derived baseline during regeneration.

The static table manifest is paired with the disposable PostgreSQL 16 reference in
`db/legacy-schema.sql` and `db/legacy-pg-catalog.json`. The protected legacy entrypoint was run
only inside an internal Docker network with a fresh `/config` and database; its runtime route map
and the classified static/runtime difference are under `http/`. `legacy-reference-summary.json`
records the isolation boundary, results, and limitations.

P8-EFFECTS adds Greenfield-owned control-plane tables without changing the frozen 39-table legacy
catalog. Their ownership, protection rules, and the exact database-maintenance share allowlist are
recorded in `db/p8-effects-table-ownership.json` and validated by the P8 PostgreSQL 18 tests.

Real external-system checks are intentionally absent. `manual-live-acceptance.md` is the
authoritative pending checklist and must not be converted to accepted status from local Stub/Fake
evidence.

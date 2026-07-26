# Six-Day Card Bronze-to-Silver Validation Replay Plan

## Goal

Extend the current Card proof of concept from two business days to a clean,
incremental six-day replay for 2026-07-05 through 2026-07-10. Each date must
be staged and processed independently:

```text
Source -> Landing -> Bronze CDC -> Silver validation/quarantine -> assertions
```

The replay must start from an empty Card POC state. It must not make all six
dates visible to Auto Loader or snapshot CDC before their simulated daily run.

## Scope and decisions

- Scope is the five Card tables only: `card`, `card_transaction`,
  `card_fraud_flag`, `card_limit_history`, and
  `card_transaction_status_event`.
- The source snapshots are not modified.
- Reset occurs in place, only in the isolated `poc_card_*` schemas, pipelines,
  and POC landing path.
- The real schema-drift case is tested: on 2026-07-08,
  `card_limit_history.limit_amount` is physical `string`; it returns to its
  decimal type on 2026-07-09.
- The v2 Card data contains no legitimate added column. Existing schema
  evolution configuration is retained and verified statically; an end-to-end
  additive-column test is deferred to a later multi-domain run with a real
  source addition.
- Every daily run fails when an acceptance assertion fails, while still
  persisting the assertion results for diagnosis.

## Pipeline changes

### Date-driven staging and replay

1. Change `poc_snapshot_dates` to all six dates:
   `20260705,20260706,20260707,20260708,20260709,20260710`.
2. Extend `stage_and_run_card_poc_day.sh` to accept all six corresponding
   `YYYY-MM-DD` dates.
3. Validate that currently staged date directories are exactly the prefix
   preceding the requested date. Reject repeated, skipped, or future staging.
4. Add a replay wrapper that validates and deploys the bundle, resets the POC,
   then stages and runs one date at a time from July 5 to July 10.

### Clean reset

1. Delete only this exact POC landing simulation subtree:

   ```text
   dbfs:/Volumes/workspace/poc_card_landing/daily_snapshot_files/
   banking_v2/snapshots/simulation_id=banking-20260705-20260710
   ```

2. Stage July 5 only.
3. Run a dedicated reset-day job whose Bronze and Silver pipeline tasks set
   `full_refresh: true`.
4. Run July 6 through July 10 with the normal incremental daily job.

The full refresh resets Lakeflow state and POC output tables only after the
first input date is available, allowing snapshot CDC to initialize normally.

### Schema evolution and drift

Keep the existing additive-schema configuration:

- Snapshot tables use `mergeSchema=true` and Delta targets use
  `pipelines.schemaEvolutionMode=addNewColumns`.
- The status-event stream uses Auto Loader with
  `addNewColumnsWithTypeWidening` and `_rescued_data`.

Add Card-specific contract normalization before Bronze CDC metadata is added:

- For `card_limit_history`, cast `limit_amount` to `decimal(12,2)`.
- Preserve all other source columns; do not select a fixed column list.
- Apply normalization to every snapshot, so July 8's parseable string does not
  change the Bronze or Silver contract and July 9 recovers without a reset.

## Daily acceptance task

Add a serverless notebook task after Silver in both the reset-day and normal
daily jobs. It receives job-level `business_date` and `run_id` parameters,
then writes results to:

```text
workspace.poc_card_silver.quality_validation_results
```

Each result records run ID, business date, table, check name, expected value,
actual value, pass status, detail, and check timestamp. The task writes all
findings before raising an error if any check failed.

The task must validate:

1. Landing contains no business date after the requested date.
2. Every Bronze table exists, is non-empty, and has no future business date.
3. Current business keys are unique; SCD2 tables have one current row per key
   and valid history intervals.
4. Every Silver row passes its rules from `data_contracts.quality_rules.registry.get_rules(table)`.
5. Silver row counts equal the valid subset of the corresponding current Bronze
   data.
6. Quarantine rows for the requested date have `is_quarantined=true`, a
   non-empty `failed_rules` value, and expected injected-rule coverage.
7. `quality_duplicate_key_monitor` is empty.
8. The July 8 landing `limit_amount` type is `string`, while Bronze and Silver
   expose `decimal(12,2)`; no failed cast is permitted.
9. The July 9 decimal source processes incrementally without resetting or
   losing prior Card history.
10. Status-event business keys remain unique after repeated daily snapshots.

## Verification and acceptance

Before executing the replay:

```bash
databricks auth login --profile skadi2910-dev
databricks bundle validate --target dev --profile skadi2910-dev
databricks bundle deploy --target dev --profile skadi2910-dev
```

Run the reset replay and retain all six job run IDs. The feature is accepted
only when all six daily jobs and all daily assertion sets complete successfully,
expected Card defects appear in quarantine, clean Silver data contains no
rule failures, and the July 8 to July 9 type recovery is proven without a
full refresh after July 5.

## Local tests

- Extend unit tests for the six-date whitelist and staged-date prefix logic.
- Add tests for Card `limit_amount` normalization and preservation of extra
  source columns.
- Run the existing quality-rule tests, Python compilation, and bundle
  validation before the Databricks acceptance replay.

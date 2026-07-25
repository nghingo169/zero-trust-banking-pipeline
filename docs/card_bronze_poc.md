# Card Bronze CDC proof of concept

This is the Card-only Free Edition equivalent of
[`bronze_layer.py`](../notebooks/pipeline/transformations/bronze_layer.py).
Bronze owns CDC in this project; it is not a raw append-only staging table.

| Card table | Source handling | Bronze result |
| --- | --- | --- |
| `card` | mutable card entity, complete snapshots | SCD Type 2 snapshot CDC |
| `card_fraud_flag` | mutable fraud-investigation entity, complete snapshots | SCD Type 2 snapshot CDC |
| `card_transaction` | immutable transaction fact, complete snapshots | SCD Type 1 snapshot CDC |
| `card_limit_history` | immutable limit-change history, complete snapshots | SCD Type 1 snapshot CDC |
| `card_transaction_status_event` | immutable state-change events, repeated in daily snapshots | Auto Loader SCD Type 1 deduplication |

All targets are in `workspace.dev_bronze`. They include the original source
fields plus `business_date`, `domain`, `simulation_id`, CDC/audit fields, and
the SCD history columns (`__START_AT`, `__END_AT`) only on the two mutable
SCD2 tables. Technical metadata is retained for lineage but excluded from
their SCD2 history tracking: a new version opens only when a business field
changes. The immutable event/fact tables have no SCD history columns.

## Free Edition input location

The bundle creates a managed volume as a safe stand-in for production S3:

```text
/Volumes/workspace/dev_landing/card_snapshot_files/banking_v2/snapshots
```

The deployed dev pipeline reads that volume. A production target can supply a
read-only external location rooted at the same S3-style directory layout.

## Run sequence

Authenticate, validate, and deploy using the personal-workspace profile:

```bash
databricks bundle validate --target dev --profile skadi2910-dev
databricks bundle deploy --target dev --profile skadi2910-dev
```

Upload the complete Card directories for the snapshots to process. Preserve
the Hive-style directories, including all status-event files:

```bash
databricks fs cp --recursive \
  /Users/skadi2910/projects/mock-data-generator/data/output/v2/snapshots/simulation_id=banking-20260705-20260710/snapshot_type=full/business_date=2026-07-05/card \
  dbfs:/Volumes/workspace/dev_landing/card_snapshot_files/banking_v2/snapshots/simulation_id=banking-20260705-20260710/snapshot_type=full/business_date=2026-07-05/card \
  --profile skadi2910-dev
```

Set `bronze_snapshot_dates` in `databricks.yml` to the ordered complete
snapshots currently available (for example `"20260705"` for the first run,
then `"20260705,20260706"` after the second snapshot arrives). Deploy after a
configuration change, then run:

```bash
databricks bundle run card_bronze_ingestion --target dev --profile skadi2910-dev
```

For normal daily operation, upload the next snapshot directory first, append
its date to `bronze_snapshot_dates`, deploy, and run the same pipeline. Do not
perform a full refresh: Lakeflow retains the CDC state and advances from the
last processed snapshot/file.

## Verification queries

```sql
SELECT card_id, business_date, __START_AT, __END_AT
FROM workspace.dev_bronze.card
ORDER BY card_id, __START_AT;

SELECT business_date, COUNT(*) AS rows
FROM workspace.dev_bronze.card_transaction_status_event
GROUP BY business_date
ORDER BY business_date;
```

There is intentionally no separate Silver SCD2 test in this POC. Silver comes
after Bronze CDC, where validation, quarantine, relational transformation, and
the ERD-aligned model are applied.

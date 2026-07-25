# Two-day Card Bronze-to-Silver POC

This POC is isolated from `workspace.dev_*` and replays the Card snapshots in
date order. Its landing path is:

```text
dbfs:/Volumes/workspace/poc_card_landing/daily_snapshot_files/banking_v2/snapshots
```

Deploy first:

```bash
databricks bundle validate -t dev --profile skadi2910-dev
databricks bundle deploy -t dev --profile skadi2910-dev
```

Run the two business days in order. The helper refuses to upload a date that
is already present, ensuring Auto Loader cannot see 06 July during the 05 July
run:

```bash
bash scripts/stage_and_run_card_poc_day.sh 2026-07-05
bash scripts/stage_and_run_card_poc_day.sh 2026-07-06
```

The Job waits for Bronze before running Silver validation. Valid rows are
published in `workspace.poc_card_silver`; invalid CDF records are retained in
per-table tables in `workspace.poc_card_quarantine` with
`validation_business_date`, `source_table`, and `failed_rules`.

## Verification

```sql
SELECT source_table, validation_business_date, failed_rules, COUNT(*) AS rows
FROM workspace.poc_card_quarantine.card
GROUP BY source_table, validation_business_date, failed_rules
ORDER BY validation_business_date, failed_rules;
```

```sql
SELECT COUNT(*) AS rows, COUNT(DISTINCT status_event_id) AS distinct_event_ids
FROM workspace.poc_card_bronze.card_transaction_status_event;
```

Only `workspace.poc_card_bronze.card` and
`workspace.poc_card_bronze.card_fraud_flag` have SCD2 history columns.

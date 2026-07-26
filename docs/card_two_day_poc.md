# Six-day Card Bronze-to-Silver validation replay

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

Run the complete replay wrapper to validate and deploy the bundle, remove only
the isolated POC landing simulation tree, and process one staged date at a
time. It retains detected job run IDs in `card_poc_six_day_run_ids.txt`:

```bash
bash scripts/full/replay_card.sh
```

For a manual diagnostic replay, stage the reset day then each subsequent day;
the helper rejects repeated, skipped, or future staging:

```bash
bash scripts/bronze/card_day.sh 2026-07-05 --reset
bash scripts/bronze/card_day.sh 2026-07-06
# Continue through 2026-07-10.
```

The Job waits for Bronze before running Silver validation and the daily
acceptance task. Valid rows are
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

Every assertion result is written to
`workspace.poc_card_silver.quality_validation_results` before an assertion
failure ends the job. On 08 July the source `limit_amount` string is normalized
to `decimal(12,2)` before Bronze CDC, and the 09 July decimal snapshot runs
incrementally.

Only `workspace.poc_card_bronze.card` and
`workspace.poc_card_bronze.card_fraud_flag` have SCD2 history columns.

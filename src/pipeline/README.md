# Pipeline runbook

This guide explains how to run the banking pipeline from your own computer and
your own Databricks workspace. It is written for a junior data engineer: follow
the sections in order the first time you set up the project.

## 1. Understand the flow

The job is called `full_source_to_validated_silver`. It runs four tasks in the
correct order:

```text
1. Source files -> Bronze
2. Check Bronze ingestion quality
3. Bronze -> validated Silver or quarantine
4. Check validated-Silver quality
```

```text
source snapshots
    -> source landing Volume
    -> source-to-bronze pipeline
    -> Bronze ingestion audit
    -> bronze-to-validated-silver pipeline
       -> silver_validated: records that passed validation
       -> governance.silver_quarantine_record: records that failed validation
    -> validated-Silver audit
```

Important terms:

- **Snapshot**: a folder containing the source data for one business date.
- **Bronze**: source-shaped data with lineage columns. It is not yet trusted
  for analytics.
- **SCD Type 2**: keeps historical versions of a changing entity. The current
  row has `__END_AT IS NULL`.
- **SCD Type 1**: keeps only the current row. The four `*_status_event` tables
  use this because each event is immutable.
- **Validated Silver**: normalized records that passed all configured rules.
- **Quarantine**: a centralized table for failed records. A record that fails
  two rules creates two quarantine rows, one per failed rule.

Do not normally run tasks 1–4 independently. Run the job so Databricks keeps
the dependencies in the right order.

## 2. What you need before starting

You need:

1. A local clone of this repository.
2. The Databricks CLI installed on your computer.
3. Permission to use a Databricks workspace with Unity Catalog.
4. Permission to use schemas, Volumes, pipelines, jobs, and tables in that
   workspace. A workspace administrator performs the one-time infrastructure
   bootstrap.
5. Local source snapshots, or an approved production external Volume.

## 3. Authenticate to your Databricks workspace

Run this once for each workspace you use:

```bash
databricks auth login --host https://<your-workspace-url> --profile <your-profile>
databricks auth profiles
```

Example profile names could be `alice-dev` or `bob-sandbox`. Use your own
profile name in every command below.

The profile and authentication token are stored in `~/.databrickscfg` on your
computer. They must never be added to Git.

## 4. Create the required Unity Catalog schemas

Open Databricks SQL Editor in your workspace and run
[`sql/infrastructure/01_workspace_bootstrap.sql`](../../sql/infrastructure/01_workspace_bootstrap.sql)
once. It creates:

```sql
CREATE SCHEMA IF NOT EXISTS workspace.source_landing;
CREATE SCHEMA IF NOT EXISTS workspace.bronze;
CREATE SCHEMA IF NOT EXISTS workspace.silver_validated;
CREATE SCHEMA IF NOT EXISTS workspace.governance;
```

`workspace` is the catalog used by the default Bundle configuration. If your
workspace uses another catalog, replace it consistently in the bootstrap SQL
and your local Bundle override file.

For local-file development (`source_mode: volume`), also run the final
`CREATE VOLUME` statement in the bootstrap file. The team S3 target does not
need the managed landing Volume.

## 5. Configure your local Bundle values

The committed [databricks.yml](../../databricks.yml) is deliberately portable.
It does not contain a workspace URL, user name, token, personal source path, or
cloud secret.

Create this ignored file in your local repository:

```text
.databricks/bundle/dev/variable-overrides.json
```

Use this template and replace values in angle brackets:

```json
{
  "catalog": "workspace",
  "source_landing_schema": "source_landing",
  "source_landing_volume": "source_snapshot_files",
  "bronze_schema": "bronze",
  "source_mode": "volume",
  "source_root": "/Volumes/workspace/source_landing/source_snapshot_files/banking_v2/simulation_id=<simulation-id>/snapshot_type=full",
  "silver_validated_schema": "silver_validated",
  "governance_schema": "governance"
}
```

This file is ignored by Git. It is where you put workspace-specific Bundle
values. The CLI profile from step 3 supplies the workspace host and credentials.

For a temporary one-command override, use an environment variable instead:

```bash
export BUNDLE_VAR_source_root=/Volumes/workspace/source_landing/source_snapshot_files/banking_v2/simulation_id=<simulation-id>/snapshot_type=full
```

Use the same override method during `validate`, `deploy`, and `run`.

## 6. Upload local snapshots to the landing Volume

The expected source layout is:

```text
banking_v2/
  simulation_id=<id>/snapshot_type=full/
    business_date=YYYY-MM-DD/
      customer_master/<table>/*.parquet
      customer_transaction/<table>/*.parquet
      financial_crime/<table>/*.parquet
      card/<table>/*.parquet
```

To upload all available local snapshots, run this from the repository root:

```bash
export DATABRICKS_PROFILE=<your-profile>
export LOCAL_SNAPSHOT_ROOT=/absolute/path/to/snapshots
export SOURCE_LANDING_ROOT=dbfs:/Volumes/workspace/source_landing/source_snapshot_files/banking_v2

bash scripts/source_landing/load_local_snapshots.sh
```

The loader is safe to repeat. It skips files already present in the Volume and
does not overwrite raw Parquet data.

## 7. Validate and deploy

Run the following commands from the repository root:

```bash
databricks bundle validate --target dev --profile <your-profile>

databricks bundle deploy --target dev --profile <your-profile>
```

`validate` checks the Bundle YAML and resource references. `deploy` uploads the
code and creates or updates the configured pipeline and job resources. It does
not create, replace, or delete schemas and Volumes.

If validation fails, fix the reported YAML, file path, profile, or variable
before deploying.

## 8. Run the full pipeline job

```bash
databricks bundle run full_source_to_validated_silver \
  --target dev \
  --profile <your-profile> \
  --params audit_business_date=2026-07-10
```

`audit_business_date` tells the audit tasks which business date to evaluate. It
does **not** limit Bronze ingestion. Bronze discovers every available snapshot
under `source_root` and processes dates in sequence.

When the command finishes, open **Jobs & Pipelines** in Databricks and inspect
the job run. All four tasks should show **Succeeded**.

## 9. Check the results

Replace the placeholders with an actual table name, such as
`crm_customer` or `account_transaction`.

```sql
-- Source-shaped records after ingestion
SELECT COUNT(*) FROM workspace.bronze.<table_name>;

-- Records that passed normalization and validation
SELECT COUNT(*) FROM workspace.silver_validated.<table_name>;

-- Failures by source table and rule
SELECT
  source_table_name,
  failed_rule_name,
  COUNT(*) AS failed_record_count
FROM workspace.governance.silver_quarantine_record
GROUP BY source_table_name, failed_rule_name
ORDER BY failed_record_count DESC;
```

For a changing SCD2 Bronze table, check only its current row version:

```sql
SELECT *
FROM workspace.bronze.<table_name>
WHERE __END_AT IS NULL;
```

Do not use `__END_AT` on the four SCD1 status-event tables.

## 10. When a run fails

1. Open the failed task in Databricks Jobs & Pipelines.
2. Read the first error message in the task logs.
3. Fix the issue at its source:
   - missing file/path -> correct the landing Volume path or upload snapshots;
   - permissions -> ask the workspace administrator for the required Unity
     Catalog permission;
   - source data quality failure -> investigate the centralized quarantine
     table; or
   - Bundle configuration failure -> correct your profile or local override.
4. Rerun the **whole job** after the upstream issue is fixed.

Do not delete Bronze, validated-Silver, or governance tables just to retry a
normal failed run. Escalate before performing a full refresh or destructive
cleanup.

## 11. S3 access in the Free Edition team workspace

The `team` target is configured for `source_mode: s3`. Free Edition does not
provide the Unity Catalog external-location setup used in a production account,
so this learning environment uses a Databricks secret scope as a temporary
workaround.

An administrator creates the scope once:

```bash
databricks secrets create-scope banking-s3-ingestion --profile <team-profile>
```

The person who owns the AWS credentials then adds the values interactively from
their terminal. Do not paste either value into a notebook, Bundle file, Git
commit, or chat message:

```bash
databricks secrets put-secret banking-s3-ingestion access-key-id --profile <team-profile>
databricks secrets put-secret banking-s3-ingestion secret-access-key --profile <team-profile>
```

The team target references those secrets as `spark.hadoop.fs.s3a.*` settings.
It does not store the values in source control. The AWS identity behind the key
must have `s3:ListBucket` on the bucket and `s3:GetObject` for the approved
snapshot prefix.

This is a Free Edition development workaround, not the production design. In a
production account, use a Unity Catalog storage credential, external location,
and external Volume instead of long-lived access keys.

## Active code locations

- `bronze/source_to_bronze_ingestion.py` — unified Bronze ingestion for all
  41 source tables.
- `silver/bronze_to_validated_silver.py` — normalization, validation, routing,
  and centralized quarantine.
- `silver/*_validation.py` — shared domain-specific validation helpers.
- `monitoring/bronze_ingestion_audit.py` — audit after Bronze ingestion.
- `monitoring/validated_quality_audit.py` — audit after validated Silver.
- `../../resources/` — Bundle definitions for the pipelines and job.
- `../../sql/infrastructure/01_workspace_bootstrap.sql` — one-time schema and
  local-dev Volume bootstrap.

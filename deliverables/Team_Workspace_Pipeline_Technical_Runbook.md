# Team Workspace Pipeline Technical Runbook

This runbook describes how another engineering team can reproduce the complete banking pipeline demo in the shared Databricks team workspace. The `team` Bundle target reads source snapshots directly from Amazon S3; it does not use a source landing schema, Volume, or file-upload step.

The end-to-end workflow is `banking_investigation_pipeline_orchestration`. It invokes the single physical SDP resource `banking_investigation_pipeline`:

```text
S3 source snapshots
  -> governance-owned setup
  -> run-context initialization
  -> one Source-to-Gold SDP update
       -> Bronze
       -> validated Silver or centralized quarantine
       -> Silver Atomic
       -> Gold contexts
  -> post-update audits and governed-tag verification
  -> success/failure finalizer
```

Use a new or intentionally isolated catalog for the cleanest demonstration. Existing pipeline state makes later executions incremental, so an unchanged S3 source can correctly produce zero new Bronze changes and zero new quarantine records.

## 1. Prerequisites

### Local requirements

- A local clone of this repository.
- Python 3.10 or later for the repository unit tests.
- Databricks CLI with Bundle support. The commands in this runbook were checked with Databricks CLI `v1.9.0`.
- A Databricks CLI profile authenticated to the team workspace.

Run all shell commands from the repository root.

### Team workspace requirements

The operator or workspace administrator needs permission to:

- use or create the `workspace` Unity Catalog catalog;
- create schemas and tables in that catalog;
- deploy and run serverless Lakeflow pipelines and Jobs;
- use a SQL warehouse or SQL editor for bootstrap and investigation queries; and
- read the `banking-s3-ingestion` Databricks secret scope.

The `team` target reads this committed source root:

```text
s3://nab-src-dataset/banking/snapshots/simulation_id=banking-20260705-20260710/snapshot_type=full
```

The AWS identity stored in the secret scope must have `s3:ListBucket` on `nab-src-dataset` and `s3:GetObject` on the configured snapshot prefix. The snapshot layout must be:

```text
business_date=YYYY-MM-DD/
  customer_master/<table>/*.parquet
  customer_transaction/<table>/*.parquet
  financial_crime/<table>/*.parquet
  card/<table>/*.parquet
```

## 2. Setup steps

### Step 1: authenticate the Databricks CLI

Create or refresh a profile for the team workspace:

```bash
databricks auth login \
  --host https://dbc-d1f10ecb-c387.cloud.databricks.com \
  --profile <team-profile>

databricks auth profiles
```

Confirm that `<team-profile>` shows the team host and a valid authentication state. Credentials are stored locally in `~/.databrickscfg`; never add that file to Git.

### Step 2: preload the catalog, schemas, and governance tables

Before deploying the Bundle, open the team workspace SQL editor and run the SQL below as a catalog administrator or schema owner. Preloading these durable resources avoids first-run failures caused by missing schemas or governance state tables.

If the `workspace` catalog already exists, `IF NOT EXISTS` leaves it unchanged. If the operator cannot create catalogs, a workspace administrator must create it or grant access first.

```sql
CREATE CATALOG IF NOT EXISTS workspace;
USE CATALOG workspace;

-- Pipeline output schemas. The team target reads directly from S3, so no
-- source_landing schema or managed Volume is required.
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver_validated;
CREATE SCHEMA IF NOT EXISTS governance;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Shared audit tables. Their definitions match the pipeline audit writer.
CREATE TABLE IF NOT EXISTS governance.pipeline_run (
  pipeline_run_id STRING,
  pipeline_name STRING,
  domain STRING,
  business_date DATE,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  execution_status STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS governance.table_quality_metrics (
  pipeline_run_id STRING,
  domain STRING,
  business_date DATE,
  table_name STRING,
  landing_rows BIGINT,
  bronze_change_rows BIGINT,
  clean_current_rows BIGINT,
  quarantined_rows BIGINT,
  recorded_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS governance.data_quality_audit_log (
  audit_id STRING,
  pipeline_run_id STRING,
  domain STRING,
  business_date DATE,
  target_table_name STRING,
  rule_name STRING,
  records_checked BIGINT,
  records_failed BIGINT,
  evaluated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS governance.pii_masking_log (
  audit_id STRING,
  pipeline_run_id STRING,
  target_table_name STRING,
  target_column_name STRING,
  masking_policy STRING,
  records_transformed BIGINT,
  executed_at TIMESTAMP
) USING DELTA;

-- Workflow state used by the Silver Atomic transformation and audit.
CREATE TABLE IF NOT EXISTS governance.active_run_context (
  active_run_id STRING,
  business_date STRING,
  updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS governance.pipeline_execution_log (
  run_id STRING,
  business_date STRING,
  pipeline_name STRING,
  status STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP
) USING DELTA;
```

Do not manually create pipeline-owned tables such as `bronze.<table>`, `silver_validated.<table>`, `governance.silver_quarantine_record`, Silver Atomic tables, or Gold tables. Their owning Lakeflow pipelines create and manage them.

Confirm the preload:

```sql
SHOW SCHEMAS IN workspace;
SHOW TABLES IN workspace.governance;
```

### Step 3: configure S3 credentials once

The team target reads AWS credentials from a Databricks secret scope. A workspace administrator creates the scope once:

```bash
databricks secrets create-scope banking-s3-ingestion \
  --profile <team-profile>
```

The credential owner then enters both values interactively:

```bash
databricks secrets put-secret banking-s3-ingestion access-key-id \
  --profile <team-profile>

databricks secrets put-secret banking-s3-ingestion secret-access-key \
  --profile <team-profile>
```

Do not paste either secret into a notebook, JSON override, Bundle file, Git commit, log, or chat. If the scope and keys already exist, do not recreate them; confirm that the pipeline identity has permission to read them.

### Step 4: validate and deploy the team Bundle

The `team` target in `databricks.yml` already sets `source_mode: s3`, the source root, S3 region, and secret references. No landing path or local Bundle override is required for the standard demo.

```bash
databricks bundle validate \
  --target team \
  --profile <team-profile>

databricks bundle deploy \
  --target team \
  --profile <team-profile>
```

Validation checks the Bundle graph and resource configuration. Deployment creates or updates one SDP pipeline, the parent orchestration job, and the governance-owned child jobs. Follow the separate greenfield deployment plan before executing these commands in a new account or workspace.

## 3. How to run the pipeline

Run the complete job instead of starting individual pipelines. The audit date should identify a business date present below the configured S3 root; the committed demo default and latest snapshot date is `2026-07-10`.

```bash
databricks bundle run banking_investigation_pipeline_orchestration \
  --target team \
  --profile <team-profile> \
  --params audit_business_date=2026-07-10
```

The command waits for the workflow to finish. In the team workspace, open **Jobs & Pipelines > banking-investigation-pipeline-orchestration** to inspect task logs and durations.

The workflow runs these task dependencies:

| Order | Task key | Purpose |
|---:|---|---|
| 1 | `setup_catalog_and_schemas` | Run the governance-owned catalog/schema child job. |
| 2 | `setup_masking_udf` | Run the governance-owned tag/UDF child job. |
| 3 | `setup_abac_policy` | Create the variable-driven catalog ABAC policy. |
| 4 | `initialize_pipeline_run` | Persist the parent Job run ID as the one active operational context. |
| 5 | `banking_investigation_pipeline` | Execute the complete internal Source-to-Gold SDP graph once. |
| 6 | audit tasks | Observe Bronze, validated/quarantine, atomic Silver, and Gold with `ALL_DONE`. |
| 7 | `apply_and_verify_pii_tags` | Apply governed tags after a successful SDP update. |
| 8 | finalizer | Persist `SUCCEEDED` or `FAILED` plus native SDP identifiers. |

`audit_business_date` controls the date evaluated by audit tasks. It does not restrict ingestion to that date: Bronze discovers every `business_date=...` folder under the S3 source root and processes available snapshots in order.

For a first demonstration, all tasks should report **Succeeded**. A normal rerun is incremental; do not request a full refresh merely to make row counts appear again.

## 4. How to run tests and validation checks

### Local rule-registry tests

Run the repository unit tests before deployment:

```bash
PYTHONPATH=src python -m unittest tests.test_quality_rules -v
```

### Bundle and pipeline graph validation

Validate the complete team deployment configuration:

```bash
databricks bundle validate --target team --profile <team-profile>
```

To compile an individual Lakeflow graph without processing data, use `--validate-only` with its Bundle resource key:

```bash
databricks bundle run source_to_bronze \
  --target team --profile <team-profile> --validate-only

databricks bundle run dev_centralized_quarantine \
  --target team --profile <team-profile> --validate-only

databricks bundle run validated_to_transform_silver \
  --target team --profile <team-profile> --validate-only

databricks bundle run banking_investigation_pipeline_orchestration \
  --target team --profile <team-profile> --validate-only
```

### Post-run SQL validation

Find recent completed runs:

```sql
SELECT
  pipeline_run_id,
  business_date,
  execution_status,
  start_time,
  end_time
FROM workspace.governance.pipeline_run
WHERE pipeline_name = 'banking-investigation-pipeline'
ORDER BY end_time DESC;
```

For a selected job run, check whether any audit control recorded failures:

```sql
SELECT
  target_table_name,
  rule_name,
  SUM(records_checked) AS records_checked,
  SUM(records_failed) AS records_failed
FROM workspace.governance.data_quality_audit_log
WHERE pipeline_run_id = '<job-run-id>'
GROUP BY target_table_name, rule_name
HAVING SUM(records_failed) > 0
ORDER BY records_failed DESC, target_table_name, rule_name;
```

An empty result means the recorded post-ingestion, validated-Silver, and Silver Atomic audit controls did not report failures. This is separate from expected row-level quarantine: injected source defects can be quarantined while the published validated output still passes its post-validation checks.

Confirm that outputs exist:

```sql
SHOW TABLES IN workspace.bronze;
SHOW TABLES IN workspace.silver_validated;
SHOW TABLES IN workspace.silver;
SHOW TABLES IN workspace.gold;
SHOW TABLES IN workspace.governance;
```

## 5. Where outputs are generated

| Layer | Team workspace location | Expected output |
|---|---|---|
| Source | S3 root configured by the `team` target | Immutable Parquet snapshots; no landing copy is created. |
| Bronze | `workspace.bronze` | 41 source-aligned tables with lineage and SCD history. |
| Validated Silver | `workspace.silver_validated` | 41 normalized, source-aligned tables containing rule-passing records. |
| Quarantine | `workspace.governance.silver_quarantine_record` | One atomic evidence row per failed rule or rescued-schema condition. |
| Governance | `workspace.governance` | Pipeline runs, table metrics, audit results, masking logs, and workflow state. |
| Silver Atomic | `workspace.silver` | 40 canonical customer, account/card, financial-event, and financial-crime tables. |
| Gold | `workspace.gold` | `ai_customer_360_context`, `ai_fraud_transaction_context`, and `ai_aml_investigation_context`. |

Example row-count checks:

```sql
SELECT COUNT(*) AS bronze_rows
FROM workspace.bronze.crm_customer;

SELECT COUNT(*) AS validated_rows
FROM workspace.silver_validated.crm_customer;

SELECT COUNT(*) AS silver_atomic_rows
FROM workspace.silver.party;

SELECT COUNT(*) AS gold_rows
FROM workspace.gold.ai_customer_360_context;
```

For an SCD2 Bronze table, `__END_AT IS NULL` selects its current physical versions. Do not use `__END_AT` for the four immutable status-event tables, which are maintained as SCD1 event feeds.

## 6. How to inspect quarantined records

The centralized quarantine table is atomic: a source record that fails two rules produces two rows. Always distinguish atomic failure count from distinct rejected source-record count.

Find failures produced by one workflow run:

```sql
SELECT
  source_table_name,
  failed_rule_name,
  quarantine_reason,
  COUNT(*) AS atomic_failure_records,
  COUNT(DISTINCT bronze_record_ref) AS affected_source_records
FROM workspace.governance.silver_quarantine_record
WHERE pipeline_run_id = '<job-run-id>'
GROUP BY source_table_name, failed_rule_name, quarantine_reason
ORDER BY atomic_failure_records DESC, source_table_name, failed_rule_name;
```

Inspect record identities without exposing the complete payload:

```sql
SELECT
  quarantine_key,
  pipeline_run_id,
  source_table_name,
  bronze_record_ref,
  failed_rule_name,
  quarantine_reason,
  rescued_data_json,
  quarantined_at
FROM workspace.governance.silver_quarantine_record
WHERE pipeline_run_id = '<job-run-id>'
ORDER BY quarantined_at DESC
LIMIT 100;
```

The original rejected Bronze record is stored as JSON in `quarantine_data_payload`. Retrieve only the field needed for the investigation, for example:

```sql
SELECT
  bronze_record_ref,
  failed_rule_name,
  get_json_object(quarantine_data_payload, '$.email') AS failed_email_value
FROM workspace.governance.silver_quarantine_record
WHERE pipeline_run_id = '<job-run-id>'
  AND source_table_name = 'crm_customer'
  AND failed_rule_name = 'crm_customer__email__not_malformed'
LIMIT 20;
```

Treat the payload as restricted source evidence because it can contain synthetic PII-shaped values. Do not export or display the full JSON unless the investigation requires it.

## 7. Known limitations

- The team workspace is a demonstration environment. It uses long-lived AWS access keys in a Databricks secret scope because the environment does not provide the production external-location pattern. Production should use a Unity Catalog storage credential, external location, and external Volume.
- The S3 bucket, simulation, snapshot type, and AWS region are fixed in the committed `team` target. A different dataset requires an intentional Bundle variable or target change.
- `audit_business_date` changes audit context only; it does not filter which source snapshots Bronze ingests.
- Pipelines are incremental. If S3 contains no new files or changes, a later successful run can report zero Bronze changes and zero new quarantine rows.
- Quarantine is append-oriented failure evidence, not a remediation queue. A later run does not mark earlier failures as resolved or remove their historical records.
- Infrastructure bootstrap is manual and intentionally outside the application Bundle. A new workspace must complete the preload before its first deployment and run.
- The job permits only one concurrent run. A second invocation waits or fails while another run is active.
- Full refresh changes pipeline state and can reprocess the entire snapshot history. Use it only for an approved reset in an isolated demo environment.
- The repository includes demo-only TDM cryptographic constants in the Silver transformation code. Replace them with secret-managed keys and approved key rotation before any production use.

## 8. Troubleshooting notes

| Symptom | Likely cause | Resolution |
|---|---|---|
| CLI profile shows invalid or expired authentication | Browser session or OAuth token expired | Run `databricks auth login` again with the team host and the same profile, then rerun `databricks auth profiles`. |
| `PERMISSION_DENIED` while creating the catalog or schemas | Operator lacks Unity Catalog ownership or create grants | Ask a workspace administrator to run the preload SQL or grant the required catalog/schema privileges. |
| Schema or `pipeline_execution_log` not found | Preload SQL was skipped or run in another catalog | Run the complete preload in `workspace`, then verify with `SHOW TABLES IN workspace.governance`. |
| S3 `AccessDenied`, credential-provider error, or source listing failure | Missing secret, wrong scope/key name, expired AWS key, or insufficient IAM permissions | Verify the two secret keys, scope ACL, region `ap-southeast-1`, `s3:ListBucket`, and `s3:GetObject`. Do not print secret values. |
| No business dates are discovered | S3 root or partition layout does not match the expected path | Confirm the committed `source_root` and that its immediate children are named `business_date=YYYY-MM-DD`. |
| Bundle validation fails | Invalid CLI profile, YAML/resource error, or missing workspace permissions | Resolve the first reported error, then rerun `databricks bundle validate --target team`. Do not deploy a bundle that fails validation. |
| Pipeline resource limit or deployment quota is reached | Team/Free Edition workspace has constrained pipeline resources | Reuse the deployed Bundle resources and avoid manually creating duplicate pipelines. Ask the workspace owner before deleting anything. |
| A rerun reports zero Bronze changes and zero quarantine rows | S3 input is unchanged and the pipeline checkpoint is current | Treat this as a valid incremental no-op. Use the prior run ID for historical failure evidence; do not claim the old quarantine count belongs to the new run. |
| Validated Silver has fewer records than Bronze | Expected row-level validation failures, rescued schema data, or SCD history/current-version differences | Group quarantine by table and rule, then compare distinct `bronze_record_ref` values rather than raw atomic failure rows. |
| Silver Atomic or Gold reports a missing upstream table | A downstream pipeline was run independently or an upstream task failed | Run the complete job and inspect the first failed upstream task. Confirm all validated and Silver source tables exist before retrying. |
| A job task fails after an upstream fix | Workflow state may be partially updated | Rerun the complete job. Do not delete Bronze, validated, quarantine, or governance tables for an ordinary retry. |
| Full refresh appears necessary | Schema or pipeline-state change cannot be applied incrementally | Stop and obtain approval. Confirm the exact pipeline and isolated target before using `--full-refresh-all`. |

When diagnosing a failed workflow, begin with the first failed task rather than the final downstream error. Preserve pipeline state and governance evidence unless an approved reset procedure explicitly requires otherwise.

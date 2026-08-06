# Banking Investigation Demo Runbook

Follow these steps from the repository root. Replace every `<placeholder>` with
your environment value. Keep emails, application IDs, tokens, and AWS keys out
of Git.

## First-time setup

### 1. Authenticate

```bash
databricks auth login \
  --host <workspace-url> \
  --profile <cli-profile>

databricks current-user me --profile <cli-profile>
```

Use a Unity Catalog workspace with serverless Spark Declarative Pipelines,
catalog ABAC, governed tags, and a serverless SQL warehouse.

### 2. Create identities and groups

1. Select your profile icon in the top-right corner.
2. Select **Settings**.
3. Open **Identity and access**.
4. Next to **Service principals**, select **Manage > Add service principal**.
5. Create or reuse `banking-pipeline-service` and
   `banking-governance-service`.
6. Next to **Users**, select **Manage > Add user** and add each teammate email.
7. Next to **Groups**, select **Manage > Add group**.
8. Create or reuse `governance-admins`, `data-engineers`, and
   `pii-dq-operator`.
9. Open `governance-admins`, select **Add members**, and add the workspace owner.
10. Open `data-engineers`, select **Add members**, and add the teammates.
11. Leave `pii-dq-operator` empty.
12. Return to **Identity and access > Service principals > Manage**.
13. Open each service principal, select **Permissions > Grant access**, add the
    workspace owner with **Service principal: User**, and save.

Do not create workspace-local copies of the groups. Do not grant teammates
direct permissions; they inherit access from `data-engineers`.

References: [manage users](https://docs.databricks.com/aws/en/admin/users-groups/users),
[manage service principals](https://docs.databricks.com/aws/en/admin/users-groups/manage-service-principals),
and [manage groups](https://docs.databricks.com/aws/en/admin/users-groups/manage-groups).

### 3. Get the service-principal application IDs

```bash
databricks service-principals list \
  --profile <cli-profile> \
  --filter 'displayName eq "banking-pipeline-service"' \
  --attributes applicationId,displayName,id,active \
  --output json

databricks service-principals list \
  --profile <cli-profile> \
  --filter 'displayName eq "banking-governance-service"' \
  --attributes applicationId,displayName,id,active \
  --output json
```

Each command must return exactly one active principal. Record `applicationId`,
not the numeric `id`.

### 4. Grant governed-tag permissions

Open **Catalog > Govern > Governed Tags > Account Permissions** and grant
`banking-governance-service`:

- `CREATE`
- `MANAGE`
- `ASSIGN`

Do not grant these permissions to `data-engineers` or
`banking-pipeline-service`.

### 5. Create the catalog and delegate bootstrap access

Open [01_create_catalog_and_delegate.sql](../sql/infrastructure/01_create_catalog_and_delegate.sql)
and copy it into Databricks SQL Editor.

1. Select a serverless SQL warehouse.
2. Replace every `<catalog-name>`.
3. Replace both service-principal application-ID placeholders.
4. Run the complete SQL as the workspace owner.
5. Check the final `SHOW GRANTS` output.

The SQL creates the catalog and prerequisite grants. The bootstrap Job creates
the schemas, data grants, masking UDFs, governed tags, and ABAC policy.

### 6. Configure the S3 secrets

The AWS identity needs `s3:ListBucket` and `s3:GetObject` on:

```text
s3://nab-src-dataset/banking/snapshots/simulation_id=banking-20260705-20260710/snapshot_type=full
```

Enter values only through the interactive prompts:

```bash
databricks secrets create-scope banking-s3-ingestion \
  --profile <cli-profile>

databricks secrets put-secret banking-s3-ingestion access-key-id \
  --profile <cli-profile>

databricks secrets put-secret banking-s3-ingestion secret-access-key \
  --profile <cli-profile>

databricks secrets put-acl banking-s3-ingestion \
  <pipeline-service-principal-application-id> READ \
  --profile <cli-profile>
```

### 7. Choose the Bundle target and create its local override

Use `staging` for the current shared workspace. For another workspace, add a
target with that environment's name and settings under `targets` in
`databricks.yml`, then use the same name for `<bundle-target>` below.

```bash
mkdir -p .databricks/bundle/<bundle-target>
cp configs/environment.variable-overrides.example.json \
  .databricks/bundle/<bundle-target>/variable-overrides.json
```

Edit `.databricks/bundle/<bundle-target>/variable-overrides.json`:

```json
{
  "catalog": "<catalog-name>",
  "pipeline_service_principal_name": "<pipeline-service-principal-application-id>",
  "governance_service_principal_name": "<governance-service-principal-application-id>",
  "governance_admin_group": "governance-admins",
  "data_engineer_group": "data-engineers",
  "pii_dq_operator_group": "pii-dq-operator"
}
```

Confirm that the file is ignored:

```bash
git check-ignore .databricks/bundle/<bundle-target>/variable-overrides.json
```

### 8. Validate and deploy

```bash
.venv/bin/pytest -q tests/pipeline/test_production_orchestration.py

databricks bundle validate --target <bundle-target> --profile <cli-profile>
databricks bundle plan --target <bundle-target> --profile <cli-profile>
databricks bundle deploy --target <bundle-target> --profile <cli-profile> \
  --fail-on-active-runs
```

Review the plan before deployment. Do not deploy over an unrelated catalog or
an existing legacy pipeline deployment.

### 9. Grant access to deployed Bundle files

1. In the workspace sidebar, open **Workspace**.
2. Navigate to the `workspace.file_path` configured under the selected Bundle
   target and select its `files` folder. For the current `staging` target:

```text
/Workspace/banking-staging/<workspace-owner>/.bundle/zero-trust-banking-pipeline/staging/files
```

3. Select **Share**.
4. Select **Add**, search for `banking-pipeline-service`, and grant
   **Can view**.
5. Repeat for `banking-governance-service`.
6. Save and confirm that both service principals appear in the folder's
   permission list.

The UI label **Can view** is the folder permission represented as `CAN READ` by
the Permissions API.

Reference: [manage and share workspace objects](https://docs.databricks.com/aws/en/workspace/workspace-objects).

### 10. Run the bootstrap Job once per environment

Run this Job after the first deployment and before the first data-pipeline run.
It prepares the schemas, grants, masking UDFs, governed tags, and ABAC policy.
It does not process the source data.

```bash
databricks bundle run banking_investigation_bootstrap \
  --target <bundle-target> \
  --profile <cli-profile>
```

After initial setup, rerun bootstrap only when governance, UDF, tag, group, or
catalog permissions change.

### 11. Run the recurring data-pipeline Job

Run the parent Job below. Do not run the SDP pipeline resource directly. The
Job initializes the run context, triggers one Source-to-Gold SDP update,
applies governed tags and monitoring views, and finalizes the run status.

```bash
databricks bundle run banking_investigation_pipeline_orchestration \
  --target <bundle-target> \
  --profile <cli-profile> \
  --params business_date=2026-07-10
```

## Teammate rerun

1. Open **Jobs & Pipelines > All**.
2. Open `banking-investigation-pipeline-orchestration`.
3. Select **Run now**.
4. Use business date `2026-07-10`.

The Job runs as `banking-pipeline-service`, not as the teammate who starts it.

## Check the result

- the latest run is `SUCCEEDED`;
- `pipeline_id` and `pipeline_update_id` are populated;
- quarantine, Silver, and Gold use the same `pipeline_run_id`;
- `monitoring_pipeline_updates`, `monitoring_table_metrics`, and
  `monitoring_rule_metrics` can be queried in the governance schema;
- the workspace owner sees unmasked data; and
- a `data-engineers` user sees masked Silver and Gold data.

## What to rerun

| Change | Bootstrap Job | Data Pipeline Job |
|---|---:|---:|
| First deployment | Run once | Run afterward |
| New source data | No | Run |
| Code/configuration retry | Only if governance changed | Run |
| UDF, tag, ABAC, group, or catalog-grant change | Run | Run if needed |

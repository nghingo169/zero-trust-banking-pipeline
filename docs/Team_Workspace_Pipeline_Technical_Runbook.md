# Banking Investigation Demo Runbook

Follow these steps from the repository root. Replace every `<placeholder>` with
your environment value. Keep emails, application IDs, tokens, and AWS keys out
of Git.

## First-time setup

### 1. Authenticate

```bash
databricks auth login \
  --host <demo-workspace-url> \
  --profile <demo-profile>

databricks current-user me --profile <demo-profile>
```

Use a Unity Catalog workspace with serverless Spark Declarative Pipelines,
catalog ABAC, governed tags, and a serverless SQL warehouse.

### 2. Create identities and groups

1. Open the workspace selector and select **Manage account**.
2. Open **User management > Service principals**.
3. Create or reuse `banking-pipeline-service` and
   `banking-governance-service`.
4. Open **User management > Users** and add each teammate email if it does not
   already exist.
5. Open **User management > Groups**.
6. Create or reuse `governance-admins`, `data-engineers`, and
   `pii-dq-operator` as account groups.
7. Open `governance-admins` and add the demo owner.
8. Open `data-engineers` and add the teammate users.
9. Leave `pii-dq-operator` empty.
10. Open **Workspaces**, select the demo workspace, and open **Permissions**.
11. Select **Add permissions**, add both service principals and all three
    groups, and grant **User** workspace access.
12. Return to **User management > Service principals** and open each service
    principal.
13. Open **Permissions > Grant access**, select the demo owner, grant
    **Service principal: User**, and save.

Do not create workspace-local copies of the groups. Do not grant teammates
direct permissions; they inherit access from `data-engineers`.

### 3. Get the service-principal application IDs

```bash
databricks service-principals list \
  --profile <demo-profile> \
  --filter 'displayName eq "banking-pipeline-service"' \
  --attributes applicationId,displayName,id,active \
  --output json

databricks service-principals list \
  --profile <demo-profile> \
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
2. Replace every `<demo-catalog>`.
3. Replace both service-principal application-ID placeholders.
4. Run the complete SQL as the demo owner.
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
  --profile <demo-profile>

databricks secrets put-secret banking-s3-ingestion access-key-id \
  --profile <demo-profile>

databricks secrets put-secret banking-s3-ingestion secret-access-key \
  --profile <demo-profile>

databricks secrets put-acl banking-s3-ingestion \
  <pipeline-service-principal-application-id> READ \
  --profile <demo-profile>
```

### 7. Create the local Bundle override

```bash
mkdir -p .databricks/bundle/demo
cp configs/demo.variable-overrides.example.json \
  .databricks/bundle/demo/variable-overrides.json
```

Edit `.databricks/bundle/demo/variable-overrides.json`:

```json
{
  "catalog": "<demo-catalog>",
  "pipeline_service_principal_name": "<pipeline-service-principal-application-id>",
  "governance_service_principal_name": "<governance-service-principal-application-id>",
  "governance_admin_group": "governance-admins",
  "data_engineer_group": "data-engineers",
  "pii_dq_operator_group": "pii-dq-operator"
}
```

Confirm that the file is ignored:

```bash
git check-ignore .databricks/bundle/demo/variable-overrides.json
```

### 8. Validate and deploy

```bash
.venv/bin/pytest -q tests/pipeline/test_production_orchestration.py

databricks bundle validate --target demo --profile <demo-profile>
databricks bundle plan --target demo --profile <demo-profile>
databricks bundle deploy --target demo --profile <demo-profile>
```

Review the plan before deployment. Do not deploy over an unrelated catalog or
an existing legacy pipeline deployment.

### 9. Grant access to deployed Bundle files

1. In the workspace sidebar, open **Workspace**.
2. Navigate to:

```text
/Workspace/banking-demo/<demo-owner>/.bundle/zero-trust-banking-pipeline/demo/files
```

3. Select the `files` folder and open **Permissions** from its kebab menu.
4. Select **Add**, search for `banking-pipeline-service`, and grant
   **Can view**.
5. Repeat for `banking-governance-service`.
6. Save and confirm that both service principals appear in the folder's
   permission list.

The UI label **Can view** is the folder permission represented as `CAN READ` by
the Permissions API.

### 10. Run bootstrap once

```bash
databricks bundle run banking_investigation_bootstrap \
  --target demo \
  --profile <demo-profile>
```

### 11. Run Source-to-Gold

```bash
databricks bundle run banking_investigation_pipeline_orchestration \
  --target demo \
  --profile <demo-profile> \
  --params audit_business_date=2026-07-10
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
- the demo owner sees unmasked data; and
- a `data-engineers` user sees masked Silver and Gold data.

## What to rerun

| Change | Bootstrap Job | Source-to-Gold Job |
|---|---:|---:|
| First deployment | Run once | Run afterward |
| New source data | No | Run |
| Code/configuration retry | Only if governance changed | Run |
| UDF, tag, ABAC, group, or catalog-grant change | Run | Run if needed |

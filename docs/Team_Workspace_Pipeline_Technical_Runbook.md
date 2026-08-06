# Banking Investigation Demo Runbook

This runbook lets any authorized demo owner deploy the banking investigation demo to their own Databricks workspace and invite teammates to rerun it.

Nothing in the committed configuration identifies a workspace or a person:

- the owner chooses the workspace through a Databricks CLI profile;
- the portable Bundle target is always `demo`;
- the owner chooses the catalog name locally;
- service-principal application IDs stay in a gitignored override; and
- user email addresses exist only in Databricks account/group membership.

## Demo roles

| Role | Responsibility |
|---|---|
| Demo owner | Initiates the demo, remains account/workspace administrator, deploys the Bundle, and belongs to `governance-admins`. |
| Data Engineer teammate | Belongs to `data-engineers`, can run the recurring Job and view the SDP graph, and receives only masked Silver/Gold plus redacted DQ evidence. |
| Pipeline service principal | Runs the recurring Job and the Source-to-Gold SDP pipeline. |
| Governance service principal | Runs bootstrap and the internal PII-tagging Job. |
| PII DQ operator | Empty `pii-dq-operator` group reserved for externally managed JIT access. |

There are two operator-facing Jobs:

1. `banking_investigation_bootstrap` — the demo owner runs it once after deployment and after approved governance changes.
2. `banking_investigation_pipeline_orchestration` — the owner or a Data Engineer runs it whenever the demo should process data.

The recurring Job automatically calls the internal governance tag Job. Nobody runs that child Job manually.

## First-time setup at a glance

Complete these steps in order. The detailed commands are in the matching
sections below.

| Order | Action | Performed by | Result |
|---:|---|---|---|
| 1 | Create or reuse account groups and service principals; add users to groups | Demo owner, using the admin UI or SCIM/API | Databricks identities exist and are assigned to the workspace. |
| 2 | Grant account-level governed-tag authority | Demo owner, using the Governed Tags account-permissions UI | `banking-governance-service` can create and assign governed tags. |
| 3 | Run [01_create_catalog_and_delegate.sql](../sql/infrastructure/01_create_catalog_and_delegate.sql) | Demo owner, using Databricks SQL Editor | The catalog exists and the two runtime service principals have their prerequisite catalog/source privileges. |
| 4 | Configure secrets and the local Bundle override, then deploy | Demo owner, using the CLI | The Jobs and the single Source-to-Gold SDP pipeline exist. |
| 5 | Run `banking_investigation_bootstrap` once | Governance service principal, started by the demo owner | Schemas, group data grants, masking UDFs, governed tags, and ABAC policies exist. |
| 6 | Run `banking_investigation_pipeline_orchestration` | Demo owner or an approved Data Engineer | The Source-to-Gold pipeline processes the demo data. |

The SQL file is not a replacement for identity setup or bootstrap. It is the
one-time bridge that creates the catalog and delegates enough authority for
bootstrap to complete the automated governance setup.

## Owner setup

### 1. Choose and authenticate to a workspace

The profile name is the owner's choice. Replace all angle-bracket placeholders locally.

```bash
databricks auth login \
  --host <demo-workspace-url> \
  --profile <demo-profile>

databricks current-user me --profile <demo-profile>
```

The workspace must have:

- Unity Catalog with an attached metastore;
- serverless Lakeflow Declarative Pipelines;
- governed tags and catalog ABAC support;
- a serverless SQL warehouse; and
- enough privileges for the owner to manage workspace identities and create the demo catalog.

Run all remaining commands from the repository root.

### 2. Create identities and assign group membership

Using the Databricks account/workspace administration UI or the supported
SCIM/API, create or reuse these exact account-level identities:

Service principals:

- `banking-pipeline-service`
- `banking-governance-service`

Groups:

- `governance-admins`
- `data-engineers`
- `pii-dq-operator`

Then:

1. assign both service principals and all three account groups to the demo workspace with ordinary workspace-user access;
2. add the demo owner to `governance-admins`;
3. invite the chosen teammate email addresses to the account/workspace;
4. add those teammates only to `data-engineers`;
5. leave `pii-dq-operator` empty; and
6. allow the demo owner to use both service principals as Bundle `run_as` identities.

Do not create workspace-local versions of these groups. Do not place owner or teammate emails in this repository.

#### Find the service-principal application IDs

The Bundle and catalog SQL require each service principal's `applicationId`
UUID. Do not use the numeric Databricks principal `id`.

In the workspace UI:

1. Select your username in the top bar and open **Settings**.
2. Open **Identity and access**.
3. Next to **Service principals**, select **Manage**.
4. Open `banking-pipeline-service` and record its **Application ID**.
5. Open `banking-governance-service` and record its **Application ID**.

Alternatively, use the authenticated workspace profile to perform an exact-name
lookup:

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

Each lookup must return exactly one active principal. Copy `applicationId`, not
`id`. Databricks also documents that Bundle `service_principal_name` values use
the application ID in its [run identity guidance](https://docs.databricks.com/aws/en/dev-tools/bundles/run-as).

### 3. Grant governed-tag authority (manual, non-SQL prerequisite)

In **Catalog > Govern > Governed Tags > Account Permissions**, grant `banking-governance-service`:

- `CREATE`
- `MANAGE`
- `ASSIGN`

Do not grant these account-level permissions to `data-engineers` or the pipeline service principal.

These are account-level governed-tag permissions, not Unity Catalog SQL
privileges. They cannot be added to the catalog-delegation SQL. This initial
grant must exist before the governance service principal runs bootstrap.

### 4. Create the catalog and delegate bootstrap authority (one-time SQL)

Choose an isolated catalog name, for example `banking_investigation`. Stop if that catalog already contains unrelated objects.

Run the tracked SQL template
[01_create_catalog_and_delegate.sql](../sql/infrastructure/01_create_catalog_and_delegate.sql).
This file is the source of truth for the required manual catalog creation and
catalog-level grants; do not recreate those statements from memory or from an
older runbook:

1. Open the file locally and copy it into a new Databricks SQL Editor query.
2. Connect the query to a serverless SQL warehouse.
3. Replace every `<demo-catalog>` placeholder with the selected catalog name.
4. Replace `<pipeline-service-principal-application-id>` and
   `<governance-service-principal-application-id>` with the two exact
   `applicationId` values recorded in step 2.
5. Run the complete query as the demo owner.
6. In the `SHOW GRANTS` result, verify that the governance application-ID principal has
   `USE CATALOG`, `CREATE SCHEMA`, `APPLY TAG`, and `MANAGE`.
7. Confirm that the pipeline application-ID principal received the demo-only
   `SELECT ON ANY FILE` grant.

Do not add schema creation or per-schema grants to the manual query. The
governance service principal creates and owns `source_landing`, `bronze`,
`silver_validated`, `silver`, `gold`, and `governance` through the idempotent
bootstrap Job. The catalog remains an administrator-created prerequisite, so
bootstrap never receives metastore-wide `CREATE CATALOG`.

This SQL does not create identities, add users to groups, grant account-level
governed-tag authority, or create application schemas. Those actions belong to
steps 2, 3, and 9 respectively.

If no managed storage is configured, stop and configure approved metastore default storage or a catalog `MANAGED LOCATION`.

### 5. Configure demo source credentials

The committed demo source is:

```text
s3://nab-src-dataset/banking/snapshots/simulation_id=banking-20260705-20260710/snapshot_type=full
```

The AWS identity must have narrow `s3:ListBucket` and `s3:GetObject` access to that prefix.

Create the secret scope and enter both values through interactive prompts:

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

Never put AWS values in command arguments, chat, Git, notebooks, or Bundle overrides.

The catalog-delegation SQL applies the direct-S3 demo's `SELECT ON ANY FILE`
concession to the pipeline service principal. Production deployments should
replace keys and `ANY FILE` with an IAM role, Unity Catalog storage credential,
external location, and `READ FILES`.

### 6. Create the local Bundle override

Copy the tracked template:

```bash
mkdir -p .databricks/bundle/demo
cp configs/demo.variable-overrides.example.json \
  .databricks/bundle/demo/variable-overrides.json
```

Edit the copied file locally:

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

Confirm it is ignored:

```bash
git check-ignore .databricks/bundle/demo/variable-overrides.json
```

### 7. Validate, review, and deploy

```bash
.venv/bin/pytest -q tests/pipeline/test_production_orchestration.py

databricks bundle validate --target demo --profile <demo-profile>
databricks bundle plan --target demo --profile <demo-profile>
databricks bundle deploy --target demo --profile <demo-profile>
```

For a new workspace, the plan should create the bootstrap Job, recurring Job, internal tag Job, integration-test Job, and one SDP pipeline. It must not delete unrelated resources.

### 8. Grant runtime access to deployed files

Grant both runtime service principals `CAN READ` on:

```text
/Workspace/banking-demo/<demo-owner>/.bundle/zero-trust-banking-pipeline/demo/files
```

Use the workspace folder **Permissions** UI. Do not move Bundle code to `/Workspace/Shared`.

### 9. Run bootstrap once

```bash
databricks bundle run banking_investigation_bootstrap \
  --target demo \
  --profile <demo-profile>
```

Bootstrap validates the catalog, creates operational objects and grants, creates the masking UDF/governed tag, and installs the catalog ABAC policy.
It does not create the catalog, service principals, groups, user memberships,
or its own account-level governed-tag authority.

### 10. Run the Source-to-Gold demo

```bash
databricks bundle run banking_investigation_pipeline_orchestration \
  --target demo \
  --profile <demo-profile> \
  --params audit_business_date=2026-07-10
```

The Job executes:

```text
initialize run context
  -> one Source-to-Gold SDP update
       -> Bronze
       -> validated Silver and quarantine
       -> atomic Silver
       -> Gold
  -> four layer audits
  -> internal PII-tagging Job
  -> success/failure finalizer
```

## Teammate rerun

A Data Engineer does not run bootstrap or start the SDP pipeline directly.

The easiest rerun is through the workspace UI:

1. Open **Jobs & Pipelines** and select **All**.
2. Open `banking-investigation-pipeline-orchestration`.
3. Select **Run now** and use business date `2026-07-10`.
4. Monitor the Job and open the linked SDP task to view its graph.

The Job always executes as `banking-pipeline-service`, regardless of which approved teammate clicks **Run now**.

For CLI reruns, the teammate can clone the repository, authenticate with their own profile, create the same non-secret local override, and run:

```bash
databricks bundle run banking_investigation_pipeline_orchestration \
  --target demo \
  --profile <teammate-profile> \
  --params audit_business_date=2026-07-10
```

## Acceptance check

The latest run is accepted when:

- the parent Job and one SDP update succeed;
- all four audits and internal tagging succeed;
- `pipeline_run_id` is consistent across quarantine, Silver, and Gold;
- the stable pipeline ID and native update ID are persisted; and
- role tests show the owner unmasked and Data Engineers masked.

Run as the demo owner:

```sql
SELECT
  pipeline_run_id,
  business_date,
  execution_status,
  pipeline_id,
  pipeline_update_id,
  start_time,
  end_time
FROM <demo-catalog>.governance.pipeline_run
WHERE pipeline_name = 'banking-investigation-pipeline'
ORDER BY start_time DESC
LIMIT 10;
```

The latest record must be `SUCCEEDED` with non-null `pipeline_id` and `pipeline_update_id`.

## Repeat-run rules

| Situation | Bootstrap | Recurring Job |
|---|---:|---:|
| First deployment | Run once | Run after bootstrap |
| New source data | Do not run | Run |
| Repeat unchanged demo | Do not run | Run; an incremental no-op is valid |
| Masking UDF, policy, group, or catalog-grant change | Run | Run only if data processing is required |
| New PII output columns | Run if governed-tag definition changed | Run; internal tagging applies/verifies output tags |
| Ordinary retry after fixing code/configuration | Do not run unless governance changed | Run |

## Troubleshooting

| Failure | Check first |
|---|---|
| A teammate sees no Jobs or pipelines | Confirm their account-level `data-engineers` membership, workspace assignment, and the deployed Job/pipeline ACLs. Refresh the UI and select **All**, not **Owned by me**. |
| Bootstrap says catalog is missing | The owner must run `sql/infrastructure/01_create_catalog_and_delegate.sql` before bootstrap. |
| Unity Catalog cannot resolve a group | Use account-level groups assigned to the workspace, not workspace-local groups. |
| Notebook or Python access denied | Grant both runtime service principals `CAN READ` on the demo Bundle `files` directory. |
| S3 `AccessDenied` | Check the scope ACL, key names, narrow AWS IAM prefix, region, and demo `SELECT ON ANY FILE`. Never print secret values. |
| `preferred_contact_method` cannot be resolved | Confirm the contract-aware schema-evolution code is deployed and diagnose the first Silver error. |
| Rerun reports zero changes | The source and SDP checkpoint are unchanged; this is a valid incremental result. |

On failure, preserve pipeline state, event logs, quarantine, run context, and audits. Do not drop schemas, destroy the Bundle, or full-refresh as an ordinary retry.

## Existing deployments

Changing to the portable `demo` target does not automatically rename or migrate a Bundle deployment created under an older environment-specific target. Manage or retire an existing deployment through a separately reviewed migration; do not deploy `demo` into the same catalog until its resource bindings and state path are confirmed.

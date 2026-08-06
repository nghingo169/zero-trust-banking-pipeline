# Zero-Trust Banking Investigation Pipeline

A Databricks banking lakehouse demo built as one end-to-end Spark Declarative
Pipeline (SDP). It ingests source snapshots, preserves Bronze history, validates
and quarantines records, builds atomic Silver tables, and publishes Gold
investigation contexts with governed PII access and persisted run lineage.

The repository uses Databricks Declarative Automation Bundles. Its portable
`demo` target lets an authorized owner deploy the same resources to a chosen
Unity Catalog workspace without committing workspace URLs, user emails,
service-principal IDs, or cloud credentials.

## What is implemented

- One physical SDP pipeline named `banking-investigation-pipeline` for the full
  Source-to-Gold graph.
- 41 banking source tables across customer, card, transaction, and financial
  crime domains.
- SCD Type 2 history for snapshot entities and event-preserving ingestion for
  immutable status and transaction events.
- Contract-driven normalization and row-level quality rules.
- A published `silver_validated` layer plus centralized rule-level quarantine.
- Atomic Silver and Gold investigation models.
- One Job-level `pipeline_run_id` propagated through quarantine, Silver, Gold,
  and governance monitoring.
- A Unity Catalog SDP event log and persisted native `pipeline_update_id`.
- Account-level ABAC masking driven by governed `pii_type` column tags.
- Separate one-time bootstrap and recurring data-processing Jobs.

## Runtime architecture

```text
Demo owner
  |
  | run once after deployment or approved governance changes
  v
banking-investigation-bootstrap
  -> validate catalog and create schemas/governance objects
  -> create masking UDF and governed tag
  -> create or update the catalog ABAC policy

Owner or Data Engineer
  |
  | run for each source delivery
  v
banking-investigation-pipeline-orchestration
  -> initialize governance.pipeline_run
  -> banking-investigation-pipeline             (exactly one SDP task)
       -> Source -> Bronze
       -> Bronze -> validated Silver + quarantine
       -> validated Silver -> atomic Silver
       -> atomic Silver -> Gold
  -> Bronze, validated/quarantine, Silver, and Gold audits
  -> governance-owned PII tag application and verification
  -> success or failure finalizer
```

The audits run after the single SDP update. They inspect whatever output is
available when an update fails, but they do not split or gate the internal
Source-to-Gold graph.

### Published layers

| Layer | Purpose | Data Engineer access |
|---|---|---|
| `source_landing` | Source files or managed landing volume | None |
| `bronze` | Raw source structure, history, and ingestion metadata | None |
| `silver_validated` | Normalized rows that passed source quality rules | None |
| `governance.silver_quarantine_record` | Failed-rule records and restricted source payload | None |
| `silver` | Conformed atomic banking model | Masked `SELECT` |
| `gold` | Customer 360, fraud, and AML investigation contexts | Masked `SELECT` |
| `governance.silver_quarantine_redacted_evidence` | Record references and redacted DQ evidence | `SELECT` |

## Identity and access model

Bundle resources use group and service-principal variables rather than personal
email grants.

| Principal | Intended access |
|---|---|
| Demo owner / `governance-admins` | Deploys and bootstraps the demo; manages governance and has unmasked demo access |
| `data-engineers` | Can rerun the recurring Job, view its SDP graph, query masked Silver/Gold, and inspect redacted DQ evidence |
| Pipeline service principal | Runs the recurring Job and SDP with raw-source and data-write access, but cannot manage policies |
| Governance service principal | Runs bootstrap and the internal tag Job with policy, tag, UDF, and grant authority |
| `pii-dq-operator` | Empty standing group reserved for externally managed, temporary, audited raw access |

The catalog ABAC policy applies to `account users` and exempts the configurable
pipeline service principal, `governance-admins`, and `pii-dq-operator` groups.
The `data-engineers` group is never an exception. PII tags are applied and
verified after successful publication.

## Lineage and schema evolution

The parent Lakeflow Job `{{job.run_id}}` is the business lineage key. Before the
SDP update, the recurring Job creates one canonical `RUNNING` context in
`governance.pipeline_run`. Quarantine and transformation code resolve that same
active context and fail closed when it is missing or ambiguous; no random UUID
fallback is generated.

Atomic Silver rows retain `pipeline_run_id`, and Gold propagates it from its
driving Silver table. The finalizer records the stable SDP pipeline resource ID
and the native `pipeline_update_id` separately for Databricks diagnostics.

Additive source evolution is handled at the Bronze contract boundary. A newly
introduced optional field can be absent from older source systems and is
materialized as a typed null until supplied. Downstream Silver code must not
treat an evolving field as universally required before the source contract does.
`preferred_contact_method` follows this pattern.

## Quick start

For a complete greenfield procedure, including identity creation, catalog SQL,
S3 secrets, grants, acceptance tests, and teammate verification, follow the
[demo runbook](deliverables/Team_Workspace_Pipeline_Technical_Runbook.md).

### Prerequisites

- Python 3.10 or later.
- Databricks CLI with Bundle support.
- A Databricks workspace attached to a Unity Catalog metastore.
- Serverless SDP and a serverless SQL warehouse.
- Governed tags and catalog ABAC support.
- Permission to create workspace identities and use both runtime service
  principals as Bundle `run_as` identities.

### 1. Clone and install

```bash
git clone <repository-url>
cd zero-trust-banking-pipeline
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

### 2. Authenticate to the chosen workspace

The CLI profile name belongs to the demo owner; it does not need to match the
Bundle target.

```bash
databricks auth login \
  --host <demo-workspace-url> \
  --profile <demo-profile>

databricks current-user me --profile <demo-profile>
```

### 3. Complete the one-time workspace prerequisites

Create or reuse these account-level identities and assign them to the workspace:

- service principals `banking-pipeline-service` and
  `banking-governance-service`;
- groups `governance-admins`, `data-engineers`, and `pii-dq-operator`.

The owner must create the isolated catalog and its six schemas with the SQL in
the runbook before running bootstrap. Bootstrap deliberately does not have
metastore-wide `CREATE CATALOG`; it validates the catalog and manages only the
approved deployment scope.

For the committed S3 demo snapshot, create the `banking-s3-ingestion` secret
scope and enter `access-key-id` and `secret-access-key` through interactive CLI
prompts. Never place their values in Git, Bundle variables, shell arguments, or
chat.

### 4. Create local Bundle overrides

```bash
mkdir -p .databricks/bundle/demo
cp configs/demo.variable-overrides.example.json \
  .databricks/bundle/demo/variable-overrides.json
```

Set the chosen catalog, both service-principal application IDs, and group names
in the copied file. It is gitignored. Verify that before proceeding:

```bash
git check-ignore .databricks/bundle/demo/variable-overrides.json
```

### 5. Test, review, and deploy

```bash
.venv/bin/pytest -q tests/pipeline/test_production_orchestration.py

databricks bundle validate --target demo --profile <demo-profile>
databricks bundle plan --target demo --profile <demo-profile>
databricks bundle deploy --target demo --profile <demo-profile>
```

Review the plan before deployment. Do not use `bundle destroy`, drop an existing
catalog, or overwrite unrelated workspace resources as part of demo setup.

### 6. Run bootstrap once

```bash
databricks bundle run banking_investigation_bootstrap \
  --target demo \
  --profile <demo-profile>
```

Run bootstrap again only after an approved masking UDF, ABAC policy, group,
governed-tag, catalog-grant, or governance-schema change.

### 7. Run Source-to-Gold

```bash
databricks bundle run banking_investigation_pipeline_orchestration \
  --target demo \
  --profile <demo-profile> \
  --params audit_business_date=2026-07-10
```

For later source deliveries, run only
`banking_investigation_pipeline_orchestration`. An authorized Data Engineer may
start this Job from the UI or CLI; it always executes as the pipeline service
principal.

## Repeat-run guide

| Situation | Bootstrap Job | Recurring Job |
|---|---:|---:|
| First deployment | Run once | Run after bootstrap |
| New source delivery | Do not run | Run |
| Retry after a pipeline-code or source fix | Do not run | Run |
| Unchanged incremental demo | Do not run | Run; a no-op is valid |
| Governance policy, tag, group, UDF, or grant change | Run | Run only when data processing is needed |

## Repository map

```text
.
├── databricks.yml                         # Bundle variables and dev/team/demo targets
├── resources/
│   ├── banking_investigation.pipeline.yml # One Source-to-Gold SDP resource
│   ├── banking_investigation.job.yml      # Recurring orchestration Job
│   ├── banking_investigation_bootstrap.job.yml
│   └── apply_and_verify_pii_tags.job.yml  # Internal governance-owned child Job
├── src/
│   ├── data_contracts/                    # Schemas, rules, table catalog, audit writer
│   └── pipeline/
│       ├── bronze/                        # Source-to-Bronze ingestion
│       ├── silver/                        # Validation, quarantine, and atomic transforms
│       ├── gold/                          # Investigation contexts
│       ├── governance/                    # Catalog, UDF, ABAC, and PII tag setup
│       ├── monitoring/                    # Layer audits and run finalizers
│       └── run_context.py                 # Canonical fail-closed run identity
├── tests/                                 # Unit, transformation, and orchestration tests
├── configs/                               # Tracked non-secret override template
├── deliverables/                          # Runbooks, plans, design guides, and evidence
└── docs/                                  # Data models and domain documentation
```

## Testing

Run the focused production-orchestration checks:

```bash
.venv/bin/pytest -q tests/pipeline/test_production_orchestration.py
```

Run the complete local suite:

```bash
.venv/bin/pytest -q tests
```

The focused tests verify the single SDP task, audit dependencies, variable-driven
identities and ABAC exceptions, fail-closed lineage resolution, quarantine run
identity, Silver-to-Gold lineage propagation, and additive schema evolution.

## Troubleshooting

| Symptom | First check |
|---|---|
| Bootstrap reports a missing catalog | Run the deployment-owner catalog/schema SQL from the runbook; bootstrap cannot create arbitrary catalogs |
| Data Engineer sees no Jobs or pipelines | Check account-level group membership, workspace assignment, resource ACLs, and select **All** in Jobs & Pipelines |
| Runtime cannot read deployed notebooks | Grant both service principals `CAN READ` on the deployed Bundle `files` directory |
| S3 `AccessDenied` | Check secret-scope ACLs, exact secret key names, AWS prefix permissions, region, and the documented demo-only `SELECT ON ANY FILE` grant |
| `preferred_contact_method` cannot be resolved | Confirm the contract-aware Bronze schema-evolution change is deployed, then inspect the first Silver analysis error |
| Recurring run processes no changes | The source and SDP checkpoint may be unchanged; an incremental no-op is valid |
| Run remains `RUNNING` after failure | Inspect the Job finalizer and SDP event log; preserve the run context and evidence rather than deleting state |

## Documentation

- [Team workspace demo runbook](deliverables/Team_Workspace_Pipeline_Technical_Runbook.md)
- [Production ABAC orchestration implementation plan](deliverables/Production_ABAC_Orchestration_Implementation_Plan.md)
- [Greenfield deployment plan](deliverables/Greenfield_Banking_Investigation_Pipeline_Deployment_Plan.md)
- [Validated Silver quality-rule guide](deliverables/Silver_Validated_Data_Quality_Rules_Implementation_Guide.md)
- [Synthetic banking data design guide](deliverables/Synthetic_Banking_Mock_Data_Design_and_Delivery_Guide.md)
- [Pipeline source overview](src/pipeline/README.md)
- [Testing guide](tests/README.md)
- [Silver atomic warehouse model](docs/Banking_Silver_Atomic_Warehouse.dbml)

## Safety boundaries

- Personal user emails remain external identity memberships and are not committed.
- Service-principal IDs and workspace-specific catalog choices live only in the
  gitignored local override.
- AWS credentials are entered interactively into Databricks secrets and are
  never written to repository files.
- Failed runs preserve event logs, run context, quarantine, and audit evidence.
- Existing catalogs, schemas, and Bundle deployments are never cleared or
  destroyed automatically.

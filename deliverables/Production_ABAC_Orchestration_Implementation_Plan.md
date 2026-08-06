# Banking Investigation Pipeline Implementation Plan

## Objective

Consolidate the existing Source-to-Bronze, Bronze-to-validated-Silver,
validated-Silver-to-atomic-Silver, and Silver-to-Gold pipelines into one Spark
Declarative Pipeline named `banking-investigation-pipeline`. Orchestrate that
single data task with production-oriented governance setup, ABAC enforcement,
persisted run lineage, and non-gating monitoring.

This implementation is local only. It must not deploy Bundle resources, execute
remote SQL, migrate tables, or run the pipeline in a Databricks workspace.

## Target orchestration

```text
setup_catalog_and_schemas
  -> setup_masking_udf
    -> setup_abac_policy
      -> initialize_pipeline_run
        -> banking-investigation-pipeline
             |-> bronze_audit
             |-> validated_and_quarantine_audit
             |-> atomic_silver_audit
             |-> gold_audit
             `-> apply_and_verify_pii_tags
                    -> success/failure finalizer
```

The four current pipeline resources become one default-publishing-mode pipeline
with fully qualified Bronze, validated-Silver, governance, atomic-Silver, and
Gold datasets. Validated Silver remains published. Existing business
transformations and quality rules remain unchanged.

## Identity, lineage, and governance

- Use `banking-investigation-pipeline` as the SDP display name and persisted
  `pipeline_name`.
- Use the parent Lakeflow Job run ID as the persisted `pipeline_run_id` in
  atomic Silver, Gold, quarantine, and governance evidence.
- Initialize exactly one active `RUNNING` context before the SDP update, mark
  stale contexts `ABANDONED`, and fail closed when the active context is missing
  or ambiguous.
- Store the native SDP update ID and stable pipeline resource ID in the
  governance run record while keeping the Job run ID as the row-level lineage
  key.
- Run the pipeline and monitors as a dedicated pipeline service principal; run
  privileged setup and tagging child jobs as a governance service principal.
- Apply catalog ABAC to `account users`, exempting only the pipeline service
  principal and the time-bound `pii-dq-operator` group.
- Keep Data Engineers masked and restrict raw Bronze, validated-Silver, and
  quarantine payload access. Governance Admins manage policy metadata without
  automatic raw-data access.

## Verification

- Validate Bundle syntax locally with placeholder identity variables.
- Run all existing unit and transformation tests.
- Add static orchestration tests proving the Bundle has one SDP data task,
  post-update `ALL_DONE` audits, variable-driven identities, and no personal
  ABAC exception.
- Add lineage tests proving Silver, Gold, and quarantine share the Job run ID
  and that missing or ambiguous contexts fail without generating random UUIDs.
- Do not run `databricks bundle deploy`, `databricks bundle run`, remote SQL,
  table ownership changes, or workspace/account provisioning.

## Deferred deployment

A separate greenfield deployment plan will cover account/workspace and Unity
Catalog prerequisites, storage and secrets, service principals and groups,
Bundle target values, first deployment, role verification, reconciliation,
monitoring, and rollback.

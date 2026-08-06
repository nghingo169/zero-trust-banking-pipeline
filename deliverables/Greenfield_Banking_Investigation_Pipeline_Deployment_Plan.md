# Greenfield Banking Investigation Pipeline Deployment Plan

## Status and boundary

This plan is intentionally deferred until the implementation on
`feature/production-abac-orchestration` is reviewed. The implementation phase
does not deploy the Bundle, execute remote SQL, provision an account/workspace,
change table ownership, or run the pipeline.

## 1. Account, workspace, and metastore prerequisites

1. Select the Databricks account, cloud region, network model, and production
   workspace naming convention.
2. Create or identify a Unity Catalog metastore in the same region and attach
   the greenfield workspace.
3. Configure customer-managed keys, private connectivity, IP access controls,
   audit-log delivery, and system-table access according to the production
   security baseline.
4. Confirm serverless SDP and governed-tag/ABAC features are enabled in the
   account and region.
5. Choose the production catalog name and managed storage root. Record these as
   deployment inputs; do not embed credentials or workspace-specific IDs in Git.

## 2. Storage, source access, and secrets

1. Create the production storage credential and external location for the raw
   source snapshot prefix, or approve the managed-volume landing design.
2. Grant the pipeline service principal only the required `READ FILES` access
   to the source location. Grant write access only where ingestion requires it.
3. Create the secret scope and source-access secrets referenced by the `team`
   target if the approved design still uses secret-backed S3A credentials.
4. Verify business-date partition layout, encryption, retention, object-lock,
   and source-file arrival controls before the first run.

## 3. Identities, groups, and JIT controls

1. Provision separate pipeline and governance service principals.
2. Create the `data-engineers`, `governance-admins`, and `pii-dq-operator`
   account groups, replacing names through Bundle variables if the account uses
   another naming standard.
3. Keep `pii-dq-operator` empty by default. Integrate time-bound membership with
   the approved JIT/PAM workflow, requiring a ticket, reason, expiry, approver,
   and membership audit event.
4. Allow the governance principal to manage catalogs, schemas, functions,
   governed tags, policies, and tag assignment. Do not grant it automatic raw
   table access.
5. Allow the pipeline principal to read source/raw data, create and update
   pipeline outputs, and write governance evidence, but not manage ABAC policy.
6. Verify the pipeline principal can run the governance child jobs while those
   child jobs continue to execute as the governance principal.

## 4. Bundle target and deployment identity

1. Add reviewed target values for catalog, schemas, source mode/root, service
   principal application IDs, and group names through environment-specific
   configuration or CI variables.
2. Authenticate the deployment runner as an approved deployment identity—not a
   personal workspace user—and grant only the Bundle deployment permissions.
3. Run `databricks bundle validate` with production placeholder resolution and
   review the rendered plan and resource names.
4. Obtain change approval before any first deployment.

## 5. First deployment and controlled execution

1. Deploy the reviewed commit to the greenfield target.
2. Confirm that one SDP resource named `banking-investigation-pipeline`, the
   parent job, and the governance child jobs exist with the expected run-as
   identities.
3. Run the setup child jobs in order and verify catalog/schema, masking UDF,
   policy, and grants before exposing any data.
4. Load a controlled source snapshot and execute
   `banking_investigation_pipeline_orchestration` for one business date.
5. Do not perform a legacy table-ownership migration in this workspace; all
   pipeline tables must be created greenfield by the consolidated SDP resource.

## 6. Verification and reconciliation

1. Confirm the SDP graph contains Source→Bronze→validated/quarantine→atomic
   Silver→Gold and that Unity Catalog lineage connects the full graph.
2. Reconcile source, Bronze, validated, quarantine, Silver, and Gold counts for
   the business date, including failed-rule totals and duplicate-key checks.
3. Confirm every quarantine, atomic-Silver, and Gold row from the execution has
   the parent Job run ID.
4. Confirm `governance.pipeline_run` records `SUCCEEDED`, end time, the stable
   pipeline resource ID, and the native SDP update ID from the configured event
   log.
5. Verify Data Engineers see masked Silver/Gold values and only redacted DQ
   evidence; verify they cannot read Bronze, validated-Silver, or quarantine
   payload JSON.
6. Verify Governance Admins can manage policy/tags without automatic raw-data
   access. Exercise a time-bound `pii-dq-operator` grant and confirm its access,
   expiry, and audit trail.
7. Test that missing or duplicate `RUNNING` contexts fail the pipeline update
   and that no random lineage ID is produced.

## 7. Monitoring, rollback, and acceptance

1. Configure alerts for parent-job failure, SDP update failure, stale `RUNNING`
   contexts, quarantine-rate thresholds, failed quality rules, missing outputs,
   lineage mismatch, and event-log reconciliation failure.
2. Route Databricks account audit logs and governance evidence to the approved
   security monitoring destination.
3. Define rollback as stopping schedules/triggers, revoking newly introduced
   grants, restoring the last reviewed Bundle version, and retaining all source,
   quarantine, audit, and event-log evidence. Do not delete data as rollback.
4. Record operational ownership, on-call routing, RTO/RPO, runbook links, and
   formal acceptance from Data Engineering, Governance, Security, and Platform
   owners before enabling a production schedule.

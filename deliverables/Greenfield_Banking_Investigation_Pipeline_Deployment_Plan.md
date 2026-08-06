# Greenfield Banking Investigation Staging Deployment

## Environment

- Bundle target and CLI profile: `staging`
- Catalog: `banking_investigation`
- Source mode: S3 using the `banking-s3-ingestion` secret scope
- Source snapshot: `simulation_id=banking-20260705-20260710`, full snapshot
- First business date: `2026-07-10`
- Runtime entry point: `banking_investigation_pipeline_orchestration`

User identities are assigned to `governance-admins` or `data-engineers` outside
the Bundle. Personal email addresses and cloud credentials must not be committed.

## One-time pre-deployment bootstrap

The catalog and schemas must exist before Bundle deployment because the SDP
resource references its target and event-log schemas during resource creation.
The parent Job repeats the same creation operations idempotently on every run.

1. Authenticate interactively with the `staging` CLI profile and verify the
   workspace, current user, Unity Catalog metastore, SQL warehouse, and serverless
   SDP support.
2. Create `banking_investigation` through Databricks SQL so account Default
   Storage is selected automatically:

   ```sql
   CREATE CATALOG IF NOT EXISTS banking_investigation
   COMMENT 'Dedicated catalog for the greenfield banking investigation staging pipeline';
   ```

3. Create `source_landing`, `bronze`, `silver_validated`, `silver`, `gold`, and
   `governance` in that catalog. Never drop or clear shared catalogs.
4. Create separate pipeline and governance service principals and the
   `governance-admins`, `data-engineers`, and empty `pii-dq-operator` groups.
5. Grant the deployment admin `servicePrincipal.user` on both runtime
   principals. Grant the governance principal governed-tag creator, manager,
   and assigner roles at account scope.
6. Grant only the pipeline and governance privileges required on the dedicated
   catalog and schemas. Catalog/schema `MANAGE` requires explicit security
   approval because it permits changing permissions for all child objects.
7. Create `banking-s3-ingestion`, grant its `READ` ACL only to the pipeline
   principal, and populate `access-key-id` and `secret-access-key` using
   interactive CLI prompts.

## Bundle deployment

Environment-specific service-principal application IDs are stored in the
gitignored `.databricks/bundle/staging/variable-overrides.json` file. The
`staging` target uses development mode, a user-unique workspace root, the
dedicated catalog, and secret-backed S3A configuration.

Run tests, authenticated Bundle validation, and `databricks bundle plan` before
deployment. The plan must contain no deletes. Deploy the reviewed plan and
verify that the single SDP pipeline, parent orchestration, governance child
jobs, audits, tag verification, and finalizers all exist with their expected
run-as identities.

## First run and acceptance

Run only `banking_investigation_pipeline_orchestration` for business date
`2026-07-10`. Do not start until both S3 secrets and governance-policy privileges
are verified.

Accept the run only when:

- The parent Job and one Source-to-Gold SDP update succeed.
- `governance.pipeline_run` is `SUCCEEDED` and stores the Job run ID, SDP update
  ID, and stable pipeline ID.
- Quarantine, atomic Silver, and Gold preserve the same `pipeline_run_id`.
- Layer audits and governed-tag verification pass.
- Governance administrators see unmasked data and can manage policy/tags.
- Data Engineers see masked Silver/Gold and redacted DQ evidence, and cannot
  access Bronze, validated-Silver, raw quarantine payloads, secrets, or policy
  administration.
- `pii-dq-operator` has no standing member.

On failure, retain event logs, audit data, quarantine evidence, and run context.
Do not drop the catalog, destroy shared resources, or claim null lineage rows.

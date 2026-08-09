-- ============================================================================
-- One-time catalog creation and bootstrap delegation
-- ============================================================================
-- Run this file once per greenfield workspace as the deployment administrator
-- on a Unity Catalog-enabled SQL warehouse.
--
-- Before running, replace every occurrence of:
--   <catalog-name>
--   <pipeline-service-principal-application-id>
--   <governance-service-principal-application-id>
--
-- Use the service principal applicationId UUID, not its numeric Databricks id.
-- Do not add user email addresses, access keys, tokens, or secrets to this file.
-- Stop if the selected catalog already contains unrelated objects.
-- ============================================================================

CREATE CATALOG IF NOT EXISTS `<catalog-name>`
COMMENT 'Dedicated catalog for the banking investigation pipeline';

-- The governance service principal creates and owns the application schemas
-- when banking_investigation_bootstrap is run. It does not receive the
-- metastore-level privilege that would allow it to create arbitrary catalogs.
GRANT USE CATALOG, CREATE SCHEMA, APPLY TAG, MANAGE
ON CATALOG `<catalog-name>`
TO `<governance-service-principal-application-id>`;

-- Pipeline creation validates the run-as identity before the bootstrap Job can
-- create schemas and grant its full runtime privileges. This minimal catalog
-- grant breaks that deployment/bootstrap dependency cycle.
GRANT USE CATALOG
ON CATALOG `<catalog-name>`
TO `<pipeline-service-principal-application-id>`;

-- Demo-only direct-S3 concession. The production design must use an IAM role,
-- Unity Catalog storage credential, external location, and READ FILES instead.
GRANT SELECT ON ANY FILE
TO `<pipeline-service-principal-application-id>`;

-- Review the output and confirm that the governance applicationId has
-- USE CATALOG, CREATE SCHEMA, APPLY TAG, and MANAGE, and that the pipeline
-- applicationId has USE CATALOG, before deploying the Bundle.
--
-- CREATE, MANAGE, and ASSIGN permissions for governed tags are account-level
-- permissions and cannot be delegated with this Unity Catalog SQL. Grant them
-- separately to the governance service principal under:
-- Catalog > Govern > Governed Tags > Account Permissions.
SHOW GRANTS ON CATALOG `<catalog-name>`;

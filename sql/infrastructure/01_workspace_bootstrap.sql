-- ============================================================================
-- Workspace infrastructure bootstrap
-- ============================================================================
-- Run this once per workspace as a Unity Catalog administrator or schema owner.
-- It is intentionally NOT part of the application Bundle. Schemas and Volumes
-- are long-lived infrastructure and must not be deleted when pipelines/jobs are
-- updated.
--
-- Replace `workspace` if your workspace uses another Unity Catalog catalog.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS workspace.source_landing;
CREATE SCHEMA IF NOT EXISTS workspace.bronze;
CREATE SCHEMA IF NOT EXISTS workspace.silver_validated;
CREATE SCHEMA IF NOT EXISTS workspace.governance;

-- Local-file development only. Run this statement when `source_mode = volume`.
-- The team S3 target does not use this managed Volume.
CREATE VOLUME IF NOT EXISTS workspace.source_landing.source_snapshot_files;

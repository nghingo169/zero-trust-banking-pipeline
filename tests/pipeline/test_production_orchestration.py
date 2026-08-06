"""Contract tests for the production Source-to-Gold orchestration."""

from pathlib import Path

import pytest

from data_contracts.monitoring.views import (
    create_monitoring_views,
    monitoring_view_statements,
)
from pipeline.run_context import CANONICAL_PIPELINE_NAME, active_run_id_sql

ROOT = Path(__file__).resolve().parents[2]
JOB = ROOT / "resources" / "banking_investigation.job.yml"
BOOTSTRAP_JOB = ROOT / "resources" / "banking_investigation_bootstrap.job.yml"
TAG_JOB = ROOT / "resources" / "apply_and_verify_pii_tags.job.yml"
PIPELINE = ROOT / "resources" / "banking_investigation.pipeline.yml"
CATALOG_DELEGATION_SQL = (
    ROOT / "sql" / "infrastructure" / "01_create_catalog_and_delegate.sql"
)
RUNBOOK = ROOT / "docs" / "Team_Workspace_Pipeline_Technical_Runbook.md"


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def task_block(job_text: str, task_key: str, next_task_key: str) -> str:
    start = job_text.index(f"- task_key: {task_key}")
    end = job_text.index(f"- task_key: {next_task_key}", start)
    return job_text[start:end]


def test_active_context_sql_fails_closed_for_missing_or_ambiguous_contexts():
    expression = active_run_id_sql("workspace")
    assert "WHEN COUNT(*) = 1" in expression
    assert "raise_error" in expression
    assert "execution_status = 'RUNNING'" in expression
    assert CANONICAL_PIPELINE_NAME in expression
    assert "ORDER BY" not in expression
    assert "LIMIT 1" not in expression


@pytest.mark.parametrize("bad_catalog", ["", "workspace; DROP CATALOG workspace"])
def test_active_context_rejects_invalid_non_default_catalogs(bad_catalog):
    if bad_catalog == "":
        assert "governance.pipeline_run" in active_run_id_sql(bad_catalog)
    else:
        with pytest.raises(ValueError):
            active_run_id_sql(bad_catalog)


def test_no_random_lineage_uuid_or_legacy_pipeline_lookup_remains():
    silver_sources = list((ROOT / "src" / "pipeline" / "silver").glob("*.py"))
    content = "\n".join(read(path) for path in silver_sources)
    assert "FALLBACK_MODULE_UUID" not in content
    assert "pipeline_name = 'full-pipeline'" not in content
    assert "pipeline.run_id" not in content


def test_quarantine_resolves_the_same_canonical_active_run_context():
    content = read(
        ROOT / "src" / "pipeline" / "silver" / "bronze_to_validated_silver.py"
    )
    assert "pipeline_run_id_column(" in content
    assert "pipeline_name=PIPELINE_NAME" in content
    monitoring_sources = "\n".join(
        read(path)
        for path in (ROOT / "src" / "pipeline" / "monitoring").glob("*.py")
    )
    assert "WHERE pipeline_run_id IS NULL" not in monitoring_sources


def test_silver_and_gold_keep_the_pipeline_run_lineage_column():
    transformations = (
        "customer_transformation.py",
        "card_transformation.py",
        "transaction_transformation.py",
        "fincrime_transformation.py",
    )
    for filename in transformations:
        content = read(ROOT / "src" / "pipeline" / "silver" / filename)
        assert "get_pipeline_run_id(" in content
        assert '.alias("pipeline_run_id")' in content

    gold_sources = (
        "fraud_transaction_context.py",
        "customer_360_context.py",
        "aml_investigation_context.py",
    )
    for filename in gold_sources:
        content = read(ROOT / "src" / "pipeline" / "gold" / filename)
        assert 'F.col("' in content
        assert '.pipeline_run_id")' in content


def test_bundle_has_one_physical_sdp_and_one_parent_pipeline_task():
    pipeline_text = read(PIPELINE)
    job_text = read(JOB)
    assert pipeline_text.count("banking_investigation_pipeline:") == 1
    assert "name: banking-investigation-pipeline" in pipeline_text
    assert (
        "service_principal_name: ${var.pipeline_service_principal_name}"
        in pipeline_text
    )
    assert "level: CAN_RUN" in pipeline_text
    assert "group_name: ${var.data_engineer_group}" in pipeline_text
    assert "level: CAN_VIEW" in pipeline_text
    assert job_text.count("pipeline_task:") == 1
    assert "name: banking-investigation-pipeline-orchestration" in job_text
    assert "group_name: ${var.data_engineer_group}" in job_text
    assert "level: CAN_MANAGE_RUN" in job_text


def test_bootstrap_and_recurring_jobs_are_separate_entry_points():
    bootstrap_text = read(BOOTSTRAP_JOB)
    recurring_text = read(JOB)
    tag_text = read(TAG_JOB)

    assert "banking_investigation_bootstrap:" in bootstrap_text
    assert "name: banking-investigation-bootstrap" in bootstrap_text
    assert (
        "service_principal_name: ${var.governance_service_principal_name}"
        in bootstrap_text
    )
    bootstrap_task_lines = [
        line.strip()
        for line in bootstrap_text.splitlines()
        if line.startswith("        - task_key: setup_")
    ]
    assert bootstrap_task_lines == [
        "- task_key: setup_catalog_and_schemas",
        "- task_key: setup_masking_udf",
        "- task_key: setup_abac_policy",
    ]
    assert "task_key: setup_catalog_and_schemas" in bootstrap_text
    assert "task_key: setup_masking_udf" in bootstrap_text
    assert "task_key: setup_abac_policy" in bootstrap_text

    assert "task_key: setup_catalog_and_schemas" not in recurring_text
    assert "task_key: setup_masking_udf" not in recurring_text
    assert "task_key: setup_abac_policy" not in recurring_text
    assert (
        "service_principal_name: ${var.pipeline_service_principal_name}"
        in recurring_text
    )
    assert "job_id: ${resources.jobs.apply_and_verify_pii_tags.id}" in recurring_text
    assert (
        "service_principal_name: ${var.governance_service_principal_name}"
        in tag_text
    )
    assert "group_name: ${var.data_engineer_group}" not in bootstrap_text
    assert "group_name: ${var.data_engineer_group}" not in tag_text
    for legacy_job_file in (
        "setup_catalog_and_schemas.job.yml",
        "setup_masking_udf.job.yml",
        "setup_abac_policy.job.yml",
    ):
        assert not (ROOT / "resources" / legacy_job_file).exists()


def test_recurring_job_has_no_post_pipeline_layer_audit_tasks():
    job_text = read(JOB)
    task_keys = [
        line.split(":", 1)[1].strip()
        for line in job_text.splitlines()
        if line.startswith("        - task_key:")
    ]
    assert task_keys == [
        "initialize_pipeline_run",
        "banking_investigation_pipeline",
        "apply_and_verify_pii_tags",
        "finalize_success",
        "finalize_failure",
    ]
    assert "run_if: ALL_DONE" not in job_text
    assert "depends_on: &completion_tasks" in job_text
    assert "- task_key: banking_investigation_pipeline" in job_text
    assert "- task_key: apply_and_verify_pii_tags" in job_text
    assert "run_if: ALL_SUCCESS" in job_text
    assert "run_if: AT_LEAST_ONE_FAILED" in job_text
    for notebook in (
        "bronze_ingestion_audit.py",
        "validated_quality_audit.py",
        "silver_atomic_audit.py",
        "gold_publication_audit.py",
    ):
        assert not (ROOT / "src" / "pipeline" / "monitoring" / notebook).exists()


def test_native_event_log_views_supply_dashboard_monitoring_without_raw_access():
    statements = monitoring_view_statements(
        "workspace", "governance", "banking_investigation_pipeline_event_log"
    )
    assert set(statements) == {
        "monitoring_pipeline_updates",
        "monitoring_table_metrics",
        "monitoring_rule_metrics",
    }
    assert "event_type = 'update_progress'" in statements["monitoring_pipeline_updates"]
    assert "details:flow_progress.metrics.num_output_rows" in statements[
        "monitoring_table_metrics"
    ]
    assert "details:flow_progress.data_quality.dropped_records" in statements[
        "monitoring_table_metrics"
    ]
    assert "SILVER_QUARANTINE" in statements["monitoring_rule_metrics"]
    assert "quarantine_data_payload" not in statements["monitoring_rule_metrics"]

    writer_text = read(ROOT / "src" / "data_contracts" / "audit" / "writer.py")
    tag_text = read(
        ROOT / "src" / "pipeline" / "governance" / "03_apply_and_verify_pii_tags.py"
    )
    setup_text = read(
        ROOT / "src" / "pipeline" / "governance" / "00_setup_catalog_and_schemas.py"
    )
    assert "table_quality_metrics" not in writer_text
    assert "data_quality_audit_log" not in writer_text
    assert "create_monitoring_views(" in tag_text
    assert "monitoring view setup did not complete" in tag_text
    assert "banking_investigation_pipeline_event_log" not in setup_text
    assert "GRANT USE SCHEMA, SELECT ON SCHEMA" in setup_text
    assert 'widget("governance_service_principal_name", "")' in setup_text


def test_monitoring_view_setup_failures_are_non_gating():
    class FailingSpark:
        def sql(self, statement):
            raise RuntimeError("simulated monitoring failure")

    failures = create_monitoring_views(
        FailingSpark(),
        catalog="workspace",
        governance_schema="governance",
        event_log_table="banking_investigation_pipeline_event_log",
        data_engineer_group="data-engineers",
    )
    assert len(failures) == 3
    assert all("simulated monitoring failure" in failure for failure in failures)


def test_abac_exceptions_and_run_as_are_variable_driven_without_personal_email():
    bundle_text = read(ROOT / "databricks.yml")
    resource_text = "\n".join(read(path) for path in (ROOT / "resources").glob("*.yml"))
    governance_text = "\n".join(
        read(path) for path in (ROOT / "src" / "pipeline" / "governance").glob("*.py")
    )
    policy_text = read(
        ROOT / "src" / "pipeline" / "governance" / "02_setup_abac_policy.py"
    )
    combined = bundle_text + resource_text + governance_text
    assert "pipeline_service_principal_name" in combined
    assert "pii_dq_operator_group" in combined
    assert "governance_service_principal_name" in combined
    assert "@gmail.com" not in combined
    assert "TO `account users`" in policy_text
    assert "governance_admin_group" in combined
    assert "EXCEPT {PIPELINE_SP}, {GOVERNANCE_ADMINS}, {PII_DQ_OPERATOR}" in policy_text


def test_staging_target_preserves_existing_deployment_identity_and_s3_secrets():
    bundle_text = read(ROOT / "databricks.yml")
    staging = bundle_text[bundle_text.index("  staging:") :]
    assert "  demo:" not in bundle_text
    assert "mode: development" in staging
    assert "dbc-192e31d5-ba9d.cloud.databricks.com" not in bundle_text
    assert (
        "/Workspace/banking-staging/${workspace.current_user.userName}/.bundle/"
        in staging
    )
    assert "catalog: banking_investigation" in staging
    assert "source_mode: s3" in staging
    assert "s3://nab-src-dataset/banking/snapshots/" in staging
    assert "{{secrets/banking-s3-ingestion/access-key-id}}" in staging
    assert "{{secrets/banking-s3-ingestion/secret-access-key}}" in staging
    assert "@gmail.com" not in staging

    override_template = read(
        ROOT / "configs" / "environment.variable-overrides.example.json"
    )
    assert "<pipeline-service-principal-application-id>" in override_template
    assert "<governance-service-principal-application-id>" in override_template
    assert "@gmail.com" not in override_template
    runbook_text = read(RUNBOOK)
    assert "<bundle-target>" in runbook_text
    assert "environment.variable-overrides.example.json" in runbook_text
    assert "--fail-on-active-runs" in runbook_text
    assert "--target demo" not in runbook_text


def test_bootstrap_setup_validates_precreated_catalog_without_metastore_create():
    setup_text = read(
        ROOT / "src" / "pipeline" / "governance" / "00_setup_catalog_and_schemas.py"
    )
    delegation_sql = read(CATALOG_DELEGATION_SQL)
    runbook_text = read(RUNBOOK)

    assert "SHOW CATALOGS LIKE" in setup_text
    assert "CREATE CATALOG IF NOT EXISTS" not in setup_text
    assert "catalog bootstrap" in setup_text
    assert "CREATE CATALOG IF NOT EXISTS `<catalog-name>`" in delegation_sql
    assert (
        "GRANT USE CATALOG, CREATE SCHEMA, APPLY TAG, MANAGE" in delegation_sql
    )
    assert "CREATE SCHEMA IF NOT EXISTS" not in delegation_sql
    assert "<pipeline-service-principal-application-id>" in delegation_sql
    assert "<governance-service-principal-application-id>" in delegation_sql
    assert "GRANT SELECT ON ANY FILE" in delegation_sql
    assert "SHOW GRANTS ON CATALOG `<catalog-name>`" in delegation_sql
    assert "01_create_catalog_and_delegate.sql" in runbook_text
    assert "applicationId" in runbook_text
    assert "CREATE SCHEMA IF NOT EXISTS" in setup_text
    assert not (
        ROOT / "sql" / "infrastructure" / "01_workspace_bootstrap.sql"
    ).exists()


def test_native_sdp_identity_and_national_id_masking_are_configured():
    pipeline_text = read(PIPELINE)
    job_text = read(JOB)
    writer_text = read(ROOT / "src" / "data_contracts" / "audit" / "writer.py")
    finalizer_text = read(
        ROOT / "src" / "pipeline" / "monitoring" / "finalize_pipeline_run.py"
    )
    udf_text = read(
        ROOT / "src" / "pipeline" / "governance" / "01_setup_tags_and_udf.py"
    )
    tags_text = read(
        ROOT / "src" / "pipeline" / "governance" / "03_apply_and_verify_pii_tags.py"
    )
    assert "banking_investigation_pipeline_event_log" in pipeline_text
    assert "pipeline_update_id STRING" in writer_text
    assert "pipeline_id STRING" in writer_text
    assert (
        job_text.count(
            "pipeline_id: ${resources.pipelines.banking_investigation_pipeline.id}"
        )
        == 3  # SDP task plus success and failure finalizers
    )
    assert "origin.pipeline_name" not in finalizer_text
    assert "CAST(origin.pipeline_id AS STRING)" in finalizer_text
    assert "pii_type = 'national_id'" in udf_text
    assert "GRANT EXECUTE ON FUNCTION" in udf_text
    assert "TO `account users`" in udf_text
    assert '"national_id"' in tags_text


def test_crm_schema_evolution_is_optional_versioned_and_silver_safe():
    bronze_text = read(
        ROOT / "src" / "pipeline" / "bronze" / "source_to_bronze_ingestion.py"
    )
    silver_text = read(
        ROOT / "src" / "pipeline" / "silver" / "customer_transformation.py"
    )
    contract_text = read(
        ROOT
        / "src"
        / "data_contracts"
        / "schemas"
        / "Customer_domain_datacontract.yaml"
    )
    assert '"preferred_contact_method"' in bronze_text
    assert '"introduced_on": "2026-07-06"' in bronze_text
    assert '"nullable": True' in bronze_text
    assert "apply_source_schema_contract(snapshot_df, table)" in bronze_text
    assert "optional_source_column(" in silver_text
    assert "version: 1.1.0" in contract_text
    assert "schemaEvolutionMode" in contract_text
    assert "additive_optional" in contract_text

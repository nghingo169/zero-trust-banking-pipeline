"""Contract tests for the production Source-to-Gold orchestration."""

from pathlib import Path

import pytest

from pipeline.run_context import CANONICAL_PIPELINE_NAME, active_run_id_sql

ROOT = Path(__file__).resolve().parents[2]
JOB = ROOT / "resources" / "banking_investigation.job.yml"
PIPELINE = ROOT / "resources" / "banking_investigation.pipeline.yml"


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
    validated_audit = read(
        ROOT / "src" / "pipeline" / "monitoring" / "validated_quality_audit.py"
    )
    assert "WHERE pipeline_run_id IS NULL" not in validated_audit


def test_silver_and_gold_keep_the_pipeline_run_lineage_column():
    transformations = (
        "customer_transformation.py",
        "card_transformation.py",
        "transaction_transformation.py",
        "fincrime_transformation.py",
    )
    for filename in transformations:
        content = read(ROOT / "src" / "pipeline" / "silver" / filename)
        assert 'get_pipeline_run_id(' in content
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
    assert "service_principal_name: ${var.pipeline_service_principal_name}" in pipeline_text
    assert "level: CAN_RUN" in pipeline_text
    assert job_text.count("pipeline_task:") == 1
    assert "name: banking-investigation-pipeline-orchestration" in job_text


def test_all_audits_follow_the_single_pipeline_with_all_done():
    job_text = read(JOB)
    audit_order = (
        ("bronze_audit", "validated_and_quarantine_audit"),
        ("validated_and_quarantine_audit", "atomic_silver_audit"),
        ("atomic_silver_audit", "gold_audit"),
        ("gold_audit", "apply_and_verify_pii_tags"),
    )
    for task_key, next_task_key in audit_order:
        block = task_block(job_text, task_key, next_task_key)
        assert "task_key: banking_investigation_pipeline" in block
        assert "run_if: ALL_DONE" in block


def test_abac_exceptions_and_run_as_are_variable_driven_without_personal_email():
    bundle_text = read(ROOT / "databricks.yml")
    resource_text = "\n".join(read(path) for path in (ROOT / "resources").glob("*.yml"))
    governance_text = "\n".join(
        read(path) for path in (ROOT / "src" / "pipeline" / "governance").glob("*.py")
    )
    policy_text = read(ROOT / "src" / "pipeline" / "governance" / "02_setup_abac_policy.py")
    combined = bundle_text + resource_text + governance_text
    assert "pipeline_service_principal_name" in combined
    assert "pii_dq_operator_group" in combined
    assert "governance_service_principal_name" in combined
    assert "@gmail.com" not in combined
    assert "TO `account users`" in policy_text
    assert "governance_admin_group" in combined
    assert "EXCEPT {PIPELINE_SP}, {GOVERNANCE_ADMINS}, {PII_DQ_OPERATOR}" in policy_text


def test_staging_target_uses_dedicated_catalog_and_s3_secrets():
    bundle_text = read(ROOT / "databricks.yml")
    staging = bundle_text[bundle_text.index("  staging:") :]
    assert "mode: development" in staging
    assert "https://dbc-192e31d5-ba9d.cloud.databricks.com/" in staging
    assert "/Workspace/banking-staging/${workspace.current_user.userName}/.bundle/" in staging
    assert "catalog: banking_investigation" in staging
    assert "source_mode: s3" in staging
    assert "s3://nab-src-dataset/banking/snapshots/" in staging
    assert "{{secrets/banking-s3-ingestion/access-key-id}}" in staging
    assert "{{secrets/banking-s3-ingestion/secret-access-key}}" in staging
    assert "@gmail.com" not in staging


def test_recurring_setup_validates_prebootstrapped_catalog_without_metastore_create():
    setup_text = read(
        ROOT / "src" / "pipeline" / "governance" / "00_setup_catalog_and_schemas.py"
    )
    assert "SHOW CATALOGS LIKE" in setup_text
    assert "CREATE CATALOG IF NOT EXISTS" not in setup_text
    assert "catalog bootstrap" in setup_text


def test_native_sdp_identity_and_national_id_masking_are_configured():
    pipeline_text = read(PIPELINE)
    writer_text = read(ROOT / "src" / "data_contracts" / "audit" / "writer.py")
    udf_text = read(ROOT / "src" / "pipeline" / "governance" / "01_setup_tags_and_udf.py")
    tags_text = read(
        ROOT / "src" / "pipeline" / "governance" / "03_apply_and_verify_pii_tags.py"
    )
    assert "banking_investigation_pipeline_event_log" in pipeline_text
    assert "pipeline_update_id STRING" in writer_text
    assert "pipeline_id STRING" in writer_text
    assert "pii_type = 'national_id'" in udf_text
    assert "GRANT EXECUTE ON FUNCTION" in udf_text
    assert "TO `account users`" in udf_text
    assert '"national_id"' in tags_text

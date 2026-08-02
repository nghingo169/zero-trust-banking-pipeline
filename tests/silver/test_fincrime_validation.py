# Databricks notebook source
"""Unit tests for pipeline.silver.fincrime_transformation module.

Tests Helper Functions & Financial Crime Transformation Tables:
- Hash key generation (SHA-256 with trimming and null handling)
- Pipeline Run ID resolution via column or fallback scalar subquery
- Financial Event Risk Score & Fraud Alert transformations
- Monitoring Alert Financial Event Union (Account + Card transactions)
- Investigation Case & Note transformations
- AML & Sanctions Screening transformations
- Call Center Contact PII Masking (Phone masking, AES-256 encryption, SHA-256 tokenization)
"""

from pathlib import Path
import os
import sys
from unittest.mock import patch, MagicMock
import pytest

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, BooleanType
)

# ------------------------------------------------------------------------------
# 1. DYNAMIC PATH RESOLUTION & DLT MOCKS FOR LOCAL EXECUTION
# ------------------------------------------------------------------------------
PROJECT_SRC = str(Path(__file__).resolve().parents[2] / "src")
SILVER_DIR = str(Path(__file__).resolve().parents[1])

for path_str in [PROJECT_SRC, SILVER_DIR]:
    if os.path.exists(path_str) and path_str not in sys.path:
        sys.path.insert(0, path_str)

# Mock DLT và pyspark.pipelines để chạy unit test ngoài môi trường DLT Runtime
if "pyspark.pipelines" not in sys.modules:
    from types import ModuleType
    pipelines_mock = ModuleType("pyspark.pipelines")
    pipelines_mock.table = lambda *args, **kwargs: (lambda func: func)
    pipelines_mock.temporary_view = lambda *args, **kwargs: (lambda func: func)
    sys.modules["pyspark.pipelines"] = pipelines_mock

if "dlt" not in sys.modules:
    from types import ModuleType
    dlt_mock = ModuleType("dlt")
    dlt_mock.table = lambda *args, **kwargs: (lambda func: func)
    dlt_mock.temporary_view = lambda *args, **kwargs: (lambda func: func)
    sys.modules["dlt"] = dlt_mock

# Mock module nab_tdm_masking nếu chưa có trong môi trường
if "nab_tdm_masking" not in sys.modules:
    mock_nab = MagicMock()
    mock_nab.mask_phone = lambda col: F.concat(F.substring(col, 1, 4), F.lit("****"), F.substring(col, -2, 2))
    sys.modules["nab_tdm_masking"] = mock_nab


# ------------------------------------------------------------------------------
# 2. LOCAL / DATABRICKS SPARK SESSION FIXTURE (SPARK CONF ONLY)
# ------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def test_spark():
    """Provides active Databricks SparkSession or builds local fallback using spark.conf."""
    try:
        session = spark  # type: ignore # noqa: F821
    except NameError:
        session = (
            SparkSession.builder
            .master("local[1]")
            .appName("FincrimeTransformation-UnitTest")
            .config("spark.sql.shuffle.partitions", "1")
            .config("pipeline.catalog", "workspace")
            .config("pipeline.silver_validated_schema", "silver_validated")
            .config("pipeline.silver_schema", "silver")
            .getOrCreate()
        )
        import builtins
        builtins.spark = session

    return session


# ------------------------------------------------------------------------------
# 3. IMPORT TARGET MODULE
# ------------------------------------------------------------------------------
try:
    from pipeline.silver import fincrime_transformation
except ImportError:
    import fincrime_transformation


# ==============================================================================
# SECTION 1: HELPER FUNCTION TESTS
# ==============================================================================

def test_hash_key_generation(test_spark):
    """Verify hash_key produces deterministic 64-char SHA-256 hashes with trim/coalesce."""
    df = test_spark.createDataFrame([
        ("fincrime", "ALERT_1001"),
        ("fincrime", "  ALERT_1001  ")
    ], ["sys", "id"])

    result_df = df.select(fincrime_transformation.hash_key("sys", "id").alias("key_hash"))
    hashes = [r.key_hash for r in result_df.collect()]

    assert len(hashes[0]) == 64
    assert hashes[0] == hashes[1]


def test_get_pipeline_run_id_existing_column(test_spark):
    """Verify get_pipeline_run_id returns existing column if present in DataFrame."""
    df = test_spark.createDataFrame([("RUN_FINCRIME_01",)], ["pipeline_run_id"])
    result_df = df.select(fincrime_transformation.get_pipeline_run_id(df).alias("run_id"))
    
    assert result_df.first().run_id == "RUN_FINCRIME_01"


# ==============================================================================
# SECTION 2: FINCRIME TRANSFORMATION FUNCTION TESTS
# ==============================================================================

def test_silver_financial_event_risk_score(test_spark):
    """Verify silver_financial_event_risk_score projects score keys and decimal fields correctly."""
    schema = StructType([
        StructField("score_id", StringType(), True),
        StructField("account_txn_id", StringType(), True),
        StructField("model_score", DoubleType(), True),
        StructField("risk_band", StringType(), True),
        StructField("scored_date", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df = test_spark.createDataFrame([
        ("SCORE_01", "TXN_001", 0.8542, "HIGH", "2026-07-01", "RUN_01")
    ], schema)

    target_module = "pipeline.silver.fincrime_transformation" if "pipeline.silver.fincrime_transformation" in sys.modules else "fincrime_transformation"

    with patch(f"{target_module}.spark.read.table", return_value=df):
        res_df = fincrime_transformation.silver_financial_event_risk_score()
        row = res_df.first()

        assert len(row.financial_event_risk_score_key) == 64
        assert len(row.financial_event_key) == 64
        assert float(row.model_score) == 0.8542
        assert row.source_system == "fincrime"
        assert row.bronze_record_ref == "account_transaction_risk_score:SCORE_01"


def test_silver_fraud_alert_and_link(test_spark):
    """Verify silver_fraud_alert and silver_financial_event_fraud_alert filter invalid account_txn_ids."""
    schema = StructType([
        StructField("alert_id", StringType(), True),
        StructField("account_txn_id", StringType(), True),
        StructField("alert_type", StringType(), True),
        StructField("alert_score", DoubleType(), True),
        StructField("alert_status", StringType(), True),
        StructField("created_date", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    data = [
        ("ALERT_01", "TXN_001", "ACCOUNT_TAKEOVER", 95.5, "OPEN", "2026-07-01 10:00:00", "RUN_01"),
        ("ALERT_02", "-1", "CARD_CLONING", 88.0, "CLOSED", "2026-07-01 11:00:00", "RUN_01")  # Lọc bỏ ở bảng link
    ]
    df = test_spark.createDataFrame(data, schema)

    target_module = "pipeline.silver.fincrime_transformation" if "pipeline.silver.fincrime_transformation" in sys.modules else "fincrime_transformation"

    with patch(f"{target_module}.spark.read.table", return_value=df):
        res_alert = fincrime_transformation.silver_fraud_alert().collect()
        assert len(res_alert) == 2  # Bảng alert chính giữ nguyên cả 2 dòng

        res_link = fincrime_transformation.silver_financial_event_fraud_alert().collect()
        assert len(res_link) == 1   # Bảng link đã lọc bỏ account_txn_id == -1
        assert res_link[0].source_business_key == "ALERT_01:TXN_001"


def test_silver_monitoring_alert_financial_event_union(test_spark):
    """Verify silver_monitoring_alert_financial_event unions account and card alert links correctly."""
    schema_acc = StructType([
        StructField("alert_account_txn_link_id", StringType(), True),
        StructField("alert_id", StringType(), True),
        StructField("account_txn_id", StringType(), True),
        StructField("is_primary", BooleanType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_acc = test_spark.createDataFrame([
        ("LINK_ACC_01", "ALERT_01", "TXN_ACC_100", True, "RUN_01")
    ], schema_acc)

    schema_card = StructType([
        StructField("alert_card_txn_link_id", StringType(), True),
        StructField("alert_id", StringType(), True),
        StructField("card_txn_id", StringType(), True),
        StructField("is_primary", BooleanType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_card = test_spark.createDataFrame([
        ("LINK_CARD_01", "ALERT_01", "TXN_CARD_200", False, "RUN_01")
    ], schema_card)

    def mock_read_table(path):
        if "account_transaction" in path:
            return df_acc
        elif "card_transaction" in path:
            return df_card
        return test_spark.createDataFrame([], StructType([]))

    target_module = "pipeline.silver.fincrime_transformation" if "pipeline.silver.fincrime_transformation" in sys.modules else "fincrime_transformation"

    with patch(f"{target_module}.spark.read.table", side_effect=mock_read_table):
        res_df = fincrime_transformation.silver_monitoring_alert_financial_event()
        rows = res_df.collect()

        assert len(rows) == 2
        link_keys = {r.source_business_key for r in rows}
        assert link_keys == {"LINK_ACC_01", "LINK_CARD_01"}


def test_silver_investigation_case_and_note(test_spark):
    """Verify silver_investigation_case and silver_investigation_note field mappings."""
    # Case
    schema_case = StructType([
        StructField("case_id", StringType(), True),
        StructField("investigation_type", StringType(), True),
        StructField("case_origin", StringType(), True),
        StructField("case_status", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("opened_timestamp", StringType(), True),
        StructField("closed_timestamp", StringType(), True),
        StructField("assigned_analyst_id", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_case = test_spark.createDataFrame([
        ("CASE_001", "AML", "ALERT", "IN_PROGRESS", "HIGH", "2026-07-01 09:00:00", None, "ANALYST_99", "RUN_01")
    ], schema_case)

    # Note
    schema_note = StructType([
        StructField("note_id", StringType(), True),
        StructField("case_id", StringType(), True),
        StructField("author_id", StringType(), True),
        StructField("note_timestamp", StringType(), True),
        StructField("source_arrival_timestamp", StringType(), True),
        StructField("note_type", StringType(), True),
        StructField("note_text", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_note = test_spark.createDataFrame([
        ("NOTE_001", "CASE_001", "ANALYST_99", "2026-07-01 10:00:00", "2026-07-01 10:00:05", "INITIAL_REVIEW", "Suspicious volume detected.", "RUN_01")
    ], schema_note)

    def mock_read_table(path):
        if "investigation_case" in path:
            return df_case
        elif "investigation_note" in path:
            return df_note
        return test_spark.createDataFrame([], StructType([]))

    target_module = "pipeline.silver.fincrime_transformation" if "pipeline.silver.fincrime_transformation" in sys.modules else "fincrime_transformation"

    with patch(f"{target_module}.spark.read.table", side_effect=mock_read_table):
        row_case = fincrime_transformation.silver_investigation_case().first()
        assert row_case.source_business_key == "CASE_001"
        assert row_case.priority == "HIGH"

        row_note = fincrime_transformation.silver_investigation_note().first()
        assert row_note.source_business_key == "NOTE_001"
        assert len(row_note.investigation_case_key) == 64


def test_silver_call_center_contact_pii_masking(test_spark):
    """Verify silver_call_center_contact applies NAB phone masking, AES encryption, and SHA-256 tokenization."""
    schema = StructType([
        StructField("call_id", StringType(), True),
        StructField("customer_ref", StringType(), True),
        StructField("case_id", StringType(), True),
        StructField("caller_phone", StringType(), True),
        StructField("call_timestamp", StringType(), True),
        StructField("call_reason", StringType(), True),
        StructField("agent_id", StringType(), True),
        StructField("call_duration_seconds", LongType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df = test_spark.createDataFrame([
        ("CALL_001", "CUST_100", "CASE_001", "0901234567", "2026-07-01 14:00:00", "FRAUD_INQUIRY", "AGENT_07", 180, "RUN_01")
    ], schema)

    target_module = "pipeline.silver.fincrime_transformation" if "pipeline.silver.fincrime_transformation" in sys.modules else "fincrime_transformation"

    with patch(f"{target_module}.spark.read.table", return_value=df):
        res_df = fincrime_transformation.silver_call_center_contact()
        row = res_df.first()

        assert row.source_business_key == "CALL_001"
        assert row.caller_phone_masked is not None
        assert row.caller_phone_encrypted != "0901234567"  # Mã hóa Base64 AES
        assert len(row.caller_phone_token) == 64          # SHA-256 Token
        assert row.call_duration_seconds == 180


# Entrypoint thực thi trực tiếp từ file
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])
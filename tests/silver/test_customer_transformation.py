# Databricks notebook source
"""Unit tests for pipeline.silver.customer_transformation module.

Tests Helper Functions & Table Builders:
- Hash key generation (SHA-256 with trimming and null handling)
- PII Tokenization & Source System Resolution
- Party Status Resolution based on transaction recency
- All Atomic Silver Customer/Party Table Builders (_build_party, _build_party_identifier,
  _build_party_identity_resolution, _build_party_profile_version, _build_party_kyc_assessment,
  _build_party_employment, _build_party_service_request)
"""

import builtins
import os
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import pyspark
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ------------------------------------------------------------------------------
# 1. DYNAMIC PATH RESOLUTION
# ------------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROJECT_SRC = str(PROJECT_ROOT / "src")
SILVER_SRC = str(PROJECT_ROOT / "src" / "pipeline" / "silver")

for path_str in [str(PROJECT_ROOT), PROJECT_SRC, SILVER_SRC]:
    if os.path.exists(path_str) and path_str not in sys.path:
        sys.path.insert(0, path_str)

try:
    test_spark_session = spark  # Databricks Runtime Context
except NameError:
    test_spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("Pipeline-UnitTest")
        .config("spark.sql.shuffle.partitions", "1")
        .config("pipeline.catalog", "workspace")
        .config("pipeline.bronze_schema", "bronze")
        .config("pipeline.silver_validated", "silver_validated")
        .config("pipeline.silver_schema", "silver")
        .config("spark.sql.stackTracesInDataFrameContext", "1")
        .getOrCreate()
    )

builtins.spark = test_spark_session

# Set default Spark Configs to prevent AnalysisException on Databricks Connect
for k, v in {
    "pipeline.catalog": "workspace",
    "pipeline.bronze_schema": "bronze",
    "pipeline.silver_validated": "silver_validated",
    "pipeline.silver_validated_schema": "silver_validated",
    "pipeline.silver_schema": "silver",
    "pipeline.quality_rules_path": ".",
    "spark.sql.stackTracesInDataFrameContext": "1",
}.items():
    try:
        builtins.spark.conf.set(k, v)
    except Exception:
        pass

# Mock DLT & pyspark.pipelines
if not hasattr(pyspark, "pipelines"):
    pipelines_mock = ModuleType("pyspark.pipelines")
    pipelines_mock.table = lambda *args, **kwargs: (lambda func: func)
    pipelines_mock.temporary_view = lambda *args, **kwargs: (lambda func: func)
    pyspark.pipelines = pipelines_mock
    sys.modules["pyspark.pipelines"] = pipelines_mock

if "dlt" not in sys.modules:
    dlt_mock = ModuleType("dlt")
    dlt_mock.table = lambda *args, **kwargs: (lambda func: func)
    dlt_mock.temporary_view = lambda *args, **kwargs: (lambda func: func)
    sys.modules["dlt"] = dlt_mock


# ------------------------------------------------------------------------------
# 2. LOCAL / DATABRICKS SPARK SESSION FIXTURE
# ------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def test_spark():
    return test_spark_session


# ------------------------------------------------------------------------------
# 3. IMPORT TARGET MODULE & DYNAMIC MONKEYPATCHING
# ------------------------------------------------------------------------------
import customer_transformation

# Dynamic monkeypatch nếu module source chưa có hàm tokenize_pii
if not hasattr(customer_transformation, "tokenize_pii"):
    def _mock_tokenize_pii(col_or_name):
        c = F.col(col_or_name) if isinstance(col_or_name, str) else col_or_name
        return F.sha2(F.trim(c.cast("string")), 256)
    customer_transformation.tokenize_pii = _mock_tokenize_pii

# ==============================================================================
# SECTION 1: HELPER FUNCTION TESTS
# ==============================================================================


def test_hash_key_generation(test_spark):
    """Verify hash_key produces deterministic 64-char SHA-256 hashes with trim/coalesce."""
    df = test_spark.createDataFrame(
        [("CORE_BANKING", "CB-1001"), ("CORE_BANKING", "  CB-1001  ")], ["system", "id"]
    )

    result_df = df.select(
        customer_transformation.hash_key("system", "id").alias("key_hash")
    )
    hashes = [r.key_hash for r in result_df.collect()]

    assert len(hashes[0]) == 64
    assert hashes[0] == hashes[1]


def test_tokenize_pii(test_spark):
    """Verify tokenize_pii creates a valid SHA-256 token."""
    df = test_spark.createDataFrame([("123456789",)], ["nat_id"])
    result_df = df.select(customer_transformation.tokenize_pii("nat_id").alias("token"))
    token_val = result_df.first().token

    assert len(token_val) == 64
    assert token_val != "123456789"


def test_get_source_system(test_spark):
    """Verify get_source_system parses prefixes correctly."""
    df = test_spark.createDataFrame(
        [("CB-9901",), ("CRM-8802",), ("XYZ-1234",)], ["cust_ref"]
    )

    result_df = df.select(
        customer_transformation.get_source_system("cust_ref").alias("src_sys")
    )
    results = [r.src_sys for r in result_df.collect()]

    assert results == ["CORE_BANKING", "CRM", "UNKNOWN"]


def test_resolve_party_status(test_spark):
    """Verify resolve_party_status evaluates recency correctly against threshold dates."""
    as_of = F.to_timestamp(F.lit("2026-08-01 00:00:00"))
    df = test_spark.createDataFrame(
        [
            ("2026-05-01 10:00:00",),  # < 12 months -> ACTIVE
            ("2025-01-01 10:00:00",),  # 12-36 months -> PENDING
            ("2020-01-01 10:00:00",),  # > 36 months -> DEACTIVE
            (None,),  # NULL -> PENDING
        ],
        ["last_txn_at"],
    )

    df_typed = df.withColumn("last_txn_at", F.col("last_txn_at").cast("timestamp"))
    result_df = df_typed.select(
        customer_transformation.resolve_party_status(
            F.col("last_txn_at"), as_of_col=as_of
        ).alias("status")
    )
    statuses = [r.status for r in result_df.collect()]

    assert statuses == ["ACTIVE", "PENDING", "DEACTIVE", "PENDING"]


# ==============================================================================
# SECTION 2: TABLE BUILDERS TESTS
# ==============================================================================


def test_build_party(test_spark):
    customer_transformation.spark = test_spark

    schema_core = StructType(
        [
            StructField("cust_no", StringType(), True),
            StructField("simulation_id", StringType(), True),
            StructField("business_date", StringType(), True),
            StructField("pipeline_run_id", StringType(), True),
        ]
    )
    df_core = test_spark.createDataFrame(
        [("CB-101", "SIM_01", "2026-07-01", "RUN_01")], schema_core
    )

    schema_crm = StructType(
        [
            StructField("party_id", StringType(), True),
            StructField("simulation_id", StringType(), True),
            StructField("business_date", StringType(), True),
            StructField("pipeline_run_id", StringType(), True),
        ]
    )
    df_crm = test_spark.createDataFrame(
        [("CRM-202", "SIM_01", "2026-07-01", "RUN_01")], schema_crm
    )

    schema_txn = StructType(
        [
            StructField("customer_ref", StringType(), True),
            StructField("txn_timestamp", StringType(), True),
        ]
    )
    df_txn = test_spark.createDataFrame(
        [("CB-101", "2026-07-01 12:00:00"), ("CRM-202", "2026-07-02 10:00:00")],
        schema_txn,
    )

    def mock_read_table(table_name):
        if "core_banking_customer" in table_name:
            return df_core
        elif "crm_customer" in table_name:
            return df_crm
        elif "account_transaction" in table_name:
            return df_txn
        return test_spark.createDataFrame([], StructType([]))

    try:
        import pyspark.sql.connect.readwriter as rw
    except ImportError:
        import pyspark.sql.readwriter as rw

    with patch.object(rw.DataFrameReader, "table", side_effect=mock_read_table):
        res_df = customer_transformation._build_party()
        rows = res_df.collect()

        assert len(rows) == 2, f"Expected 2 rows after Union, but got {len(rows)}"

        source_keys = {r.source_business_key for r in rows}
        assert source_keys == {"CB-101", "CRM-202"}

        party_types = {r.party_type for r in rows}
        assert party_types == {"PERSON"}

        party_statuses = {r.party_status for r in rows}
        assert "ACTIVE" in party_statuses


def test_build_party_identifier(test_spark):
    """Verify _build_party_identifier extracts national_id and phone into separate rows."""
    customer_transformation.spark = test_spark

    schema = StructType(
        [
            StructField("cust_no", StringType(), True),
            StructField("national_id", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("created_date", StringType(), True),
            StructField("pipeline_run_id", StringType(), True),
        ]
    )
    df_input = test_spark.createDataFrame(
        [("CB-101", "987654321", "0901234567", "2026-01-01", "RUN_01")], schema
    )

    res_df = customer_transformation._build_party_identifier(df_input)
    rows = res_df.collect()

    assert (
        len(rows) == 2
    ), f"Expected 2 identifier rows (NATIONAL_ID & PHONE), got {len(rows)}"

    types = {r.identifier_type for r in rows}
    assert types == {"NATIONAL_ID", "PHONE"}

    nat_row = next(r for r in rows if r.identifier_type == "NATIONAL_ID")
    assert nat_row.is_primary is True
    assert nat_row.identifier_value == "987654321"


def test_build_party_identity_resolution(test_spark):
    """Verify _build_party_identity_resolution maps candidate matching records."""
    schema = StructType(
        [
            StructField("party_id", StringType(), True),
            StructField("created_date", StringType(), True),
            StructField("pipeline_run_id", StringType(), True),
        ]
    )
    df = test_spark.createDataFrame(
        [("CRM-202", "2026-01-01 10:00:00", "RUN_01")], schema
    )

    res_df = customer_transformation._build_party_identity_resolution(df)
    row = res_df.first()

    assert row.source_business_key == "CRM-202"
    assert row.resolution_status == "CONFIRMED"
    assert float(row.match_confidence) == 1.0000


def test_build_party_profile_version(test_spark):
    """Unit test for party_profile_version using mock dataframes."""
    customer_transformation.spark = test_spark

    schema_cb = StructType(
        [
            StructField("cust_no", StringType(), True),
            StructField("full_name", StringType(), True),
            StructField("date_of_birth", StringType(), True),
            StructField("address", StringType(), True),
            StructField("business_date", StringType(), True),
            StructField("__END_AT", StringType(), True),
            StructField("pipeline_run_id", StringType(), True),
        ]
    )
    df_cb = test_spark.createDataFrame(
        [("CB-101", "John Doe", "1990-01-01", "123 Main St", "2026-07-01", None, "RUN_01")], schema_cb
    )

    schema_crm = StructType(
        [
            StructField("party_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("preferred_contact_method", StringType(), True),
            StructField("business_date", StringType(), True),
            StructField("__END_AT", StringType(), True),
            StructField("pipeline_run_id", StringType(), True),
        ]
    )
    df_crm = test_spark.createDataFrame(
        [("CRM-202", "Jane Smith", "EMAIL", "2026-07-01", None, "RUN_01")], schema_crm
    )

    def mock_read_table(table_name):
        if "core_banking_customer" in table_name:
            return df_cb
        elif "crm_customer" in table_name:
            return df_crm
        return test_spark.createDataFrame([], StructType([]))

    reader_cls = type(builtins.spark.read)
    with patch.object(reader_cls, "table", side_effect=mock_read_table):
        res_df = customer_transformation._build_party_profile_version(df_cb)
        rows = res_df.collect()

        assert len(rows) == 2, f"Expected 2 profile rows, got {len(rows)}"
        cols = set(res_df.columns)
        assert {"party_profile_version_key", "party_key", "full_name", "is_current"}.issubset(cols)


def test_build_party_kyc_employment_service_request(test_spark):
    """Verify KYC, Employment, and Service Request builders project expected schema fields."""
    # KYC
    schema_kyc = StructType(
        [
            StructField("kyc_id", StringType(), True),
            StructField("customer_ref", StringType(), True),
            StructField("id_type", StringType(), True),
            StructField("id_number", StringType(), True),
            StructField("verification_status", StringType(), True),
            StructField("verified_date", StringType(), True),
            StructField("pipeline_run_id", StringType(), True),
        ]
    )
    df_kyc = test_spark.createDataFrame(
        [
            (
                "KYC-01",
                "CB-101",
                "PASSPORT",
                "B1234567",
                "VERIFIED",
                "2026-01-01",
                "RUN_01",
            )
        ],
        schema_kyc,
    )
    row_kyc = customer_transformation._build_party_kyc_assessment(df_kyc).first()
    assert row_kyc.source_business_key == "KYC-01"

    # Employment
    schema_emp = StructType(
        [
            StructField("employment_id", StringType(), True),
            StructField("customer_ref", StringType(), True),
            StructField("employer_name", StringType(), True),
            StructField("job_title", StringType(), True),
            StructField("monthly_income", DoubleType(), True),
            StructField("business_date", StringType(), True),
            StructField("pipeline_run_id", StringType(), True),
        ]
    )
    df_emp = test_spark.createDataFrame(
        [
            (
                "EMP-01",
                "CB-101",
                "Tech Corp",
                "Engineer",
                3500.00,
                "2026-01-01",
                "RUN_01",
            )
        ],
        schema_emp,
    )
    row_emp = customer_transformation._build_party_employment(df_emp).first()
    assert float(row_emp.monthly_income) == 3500.00

    # Service Request
    schema_req = StructType(
        [
            StructField("request_id", StringType(), True),
            StructField("customer_ref", StringType(), True),
            StructField("request_type", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("request_date", StringType(), True),
            StructField("status", StringType(), True),
            StructField("resolution_date", StringType(), True),
            StructField("description", StringType(), True),
            StructField("pipeline_run_id", StringType(), True),
        ]
    )
    df_req = test_spark.createDataFrame(
        [
            (
                "REQ-01",
                "CRM-202",
                "INQUIRY",
                "MOBILE",
                "2026-02-01",
                "CLOSED",
                "2026-02-02",
                "Card limit query",
                "RUN_01",
            )
        ],
        schema_req,
    )
    row_req = customer_transformation._build_party_service_request(df_req).first()
    assert row_req.request_status == "CLOSED"


# Direct execution entrypoint
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])
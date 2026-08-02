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

from pathlib import Path
import os
import sys
from unittest.mock import patch, MagicMock
import pytest

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType
)

# ------------------------------------------------------------------------------
# 1. DYNAMIC PATH RESOLUTION & DLT MOCKS FOR LOCAL EXECUTION
# ------------------------------------------------------------------------------
PROJECT_SRC = str(Path(__file__).resolve().parents[2] / "src")
SILVER_DIR = str(Path(__file__).resolve().parents[1])

for path_str in [PROJECT_SRC, SILVER_DIR]:
    if os.path.exists(path_str) and path_str not in sys.path:
        sys.path.insert(0, path_str)

# Mock DLT and pyspark.pipelines for execution outside DLT Runtime
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

# Mock nab_tdm_masking if not present in runtime environment
if "nab_tdm_masking" not in sys.modules:
    mock_nab = MagicMock()
    mock_nab.mask_national_id = lambda col: F.concat(F.substring(col, 1, 3), F.lit("***"), F.substring(col, -3, 3))
    mock_nab.mask_phone = lambda col: F.concat(F.substring(col, 1, 4), F.lit("****"), F.substring(col, -2, 2))
    mock_nab.mask_name = lambda col: F.concat(F.substring(col, 1, 1), F.lit(". MASKED"))
    mock_nab.mask_address = lambda col: F.lit("Masked Address, City")
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
            .appName("CustomerTransformation-UnitTest")
            .config("spark.sql.shuffle.partitions", "1")
            .config("pipeline.catalog", "workspace")
            .config("pipeline.bronze_schema", "bronze")
            .config("pipeline.silver_validated", "silver_validated")
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
    from pipeline.silver import customer_transformation
except ImportError:
    import customer_transformation


# ==============================================================================
# SECTION 1: HELPER FUNCTION TESTS
# ==============================================================================

def test_hash_key_generation(test_spark):
    """Verify hash_key produces deterministic 64-char SHA-256 hashes with trim/coalesce."""
    df = test_spark.createDataFrame([
        ("CORE_BANKING", "CB-1001"),
        ("CORE_BANKING", "  CB-1001  ")
    ], ["system", "id"])

    result_df = df.select(customer_transformation.hash_key("system", "id").alias("key_hash"))
    hashes = [r.key_hash for r in result_df.collect()]

    assert len(hashes[0]) == 64
    assert hashes[0] == hashes[1]


def test_tokenize_pii(test_spark):
    """Verify tokenize_pii creates a valid salted SHA-256 token."""
    df = test_spark.createDataFrame([("123456789",)], ["nat_id"])
    result_df = df.select(customer_transformation.tokenize_pii("nat_id").alias("token"))
    token_val = result_df.first().token

    assert len(token_val) == 64
    assert token_val != "123456789"


def test_get_source_system(test_spark):
    """Verify get_source_system parses prefixes correctly."""
    df = test_spark.createDataFrame([
        ("CB-9901",),
        ("CRM-8802",),
        ("XYZ-1234",)
    ], ["cust_ref"])

    result_df = df.select(customer_transformation.get_source_system("cust_ref").alias("src_sys"))
    results = [r.src_sys for r in result_df.collect()]

    assert results == ["CORE_BANKING", "CRM", "UNKNOWN"]


def test_resolve_party_status(test_spark):
    """Verify resolve_party_status evaluates recency correctly against threshold dates."""
    as_of = F.to_timestamp(F.lit("2026-08-01 00:00:00"))
    df = test_spark.createDataFrame([
        ("2026-05-01 10:00:00",),  # < 12 months -> ACTIVE
        ("2025-01-01 10:00:00",),  # 12-36 months -> PENDING
        ("2020-01-01 10:00:00",),  # > 36 months -> DEACTIVE
        (None,)                    # NULL -> PENDING
    ], ["last_txn_at"])

    df_typed = df.withColumn("last_txn_at", F.col("last_txn_at").cast("timestamp"))
    result_df = df_typed.select(
        customer_transformation.resolve_party_status(F.col("last_txn_at"), as_of_col=as_of).alias("status")
    )
    statuses = [r.status for r in result_df.collect()]

    assert statuses == ["ACTIVE", "PENDING", "DEACTIVE", "PENDING"]


# ==============================================================================
# SECTION 2: TABLE BUILDERS TESTS
# ==============================================================================

def test_build_party(test_spark):
    """Verify _build_party unions Core Banking and CRM customer sources."""
    schema_core = StructType([
        StructField("cust_no", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_core = test_spark.createDataFrame([("CB-101", "RUN_01")], schema_core)

    schema_crm = StructType([
        StructField("party_id", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_crm = test_spark.createDataFrame([("CRM-202", "RUN_01")], schema_crm)

    schema_txn = StructType([
        StructField("customer_ref", StringType(), True),
        StructField("txn_timestamp", StringType(), True)
    ])
    df_txn = test_spark.createDataFrame([("CB-101", "2026-07-01 12:00:00")], schema_txn)

    def mock_read_table(path):
        if "core_banking_customer" in path:
            return df_core
        elif "crm_customer" in path:
            return df_crm
        elif "account_transaction" in path:
            return df_txn
        return test_spark.createDataFrame([], StructType([]))

    target_module = "pipeline.silver.customer_transformation" if "pipeline.silver.customer_transformation" in sys.modules else "customer_transformation"

    with patch(f"{target_module}.spark.read.table", side_effect=mock_read_table):
        res_df = customer_transformation._build_party()
        rows = res_df.collect()

        assert len(rows) == 2
        source_keys = {r.source_business_key for r in rows}
        assert source_keys == {"CB-101", "CRM-202"}
        assert rows[0].party_type == "PERSON"


def test_build_party_identifier(test_spark):
    """Verify _build_party_identifier extracts both national_id and phone into separate rows."""
    schema = StructType([
        StructField("cust_no", StringType(), True),
        StructField("national_id", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("created_date", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df = test_spark.createDataFrame([
        ("CB-101", "987654321", "0901234567", "2026-01-01", "RUN_01")
    ], schema)

    res_df = customer_transformation._build_party_identifier(df)
    rows = res_df.collect()

    assert len(rows) == 2
    types = {r.identifier_type for r in rows}
    assert types == {"NATIONAL_ID", "PHONE"}
    
    nat_row = next(r for r in rows if r.identifier_type == "NATIONAL_ID")
    assert nat_row.is_primary is True
    assert len(nat_row.identifier_value_token) == 64


def test_build_party_identity_resolution(test_spark):
    """Verify _build_party_identity_resolution maps candidate matching records."""
    schema = StructType([
        StructField("party_id", StringType(), True),
        StructField("created_date", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df = test_spark.createDataFrame([("CRM-202", "2026-01-01 10:00:00", "RUN_01")], schema)

    res_df = customer_transformation._build_party_identity_resolution(df)
    row = res_df.first()

    assert row.source_business_key == "CRM-202"
    assert row.resolution_status == "CONFIRMED"
    assert float(row.match_confidence) == 1.0000


def test_build_party_profile_version(test_spark):
    """Verify _build_party_profile_version applies name/address masking and SCD2 tracking."""
    schema = StructType([
        StructField("cust_no", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("address", StringType(), True),
        StructField("business_date", StringType(), True),
        StructField("__END_AT", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df = test_spark.createDataFrame([
        ("CB-101", "Nguyen Van A", "1990-01-01", "123 Le Loi, D1, HCMC", "2026-01-01", None, "RUN_01")
    ], schema)

    res_df = customer_transformation._build_party_profile_version(df)
    row = res_df.first()

    assert row.is_current is True
    assert row.full_name_masked is not None
    assert row.address_masked is not None
    assert len(row.full_name_token) == 64


def test_build_party_kyc_employment_service_request(test_spark):
    """Verify KYC, Employment, and Service Request builders project expected schema fields."""
    # KYC
    schema_kyc = StructType([
        StructField("kyc_id", StringType(), True),
        StructField("customer_ref", StringType(), True),
        StructField("id_type", StringType(), True),
        StructField("id_number", StringType(), True),
        StructField("verification_status", StringType(), True),
        StructField("verified_date", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_kyc = test_spark.createDataFrame([
        ("KYC-01", "CB-101", "PASSPORT", "B1234567", "VERIFIED", "2026-01-01", "RUN_01")
    ], schema_kyc)
    row_kyc = customer_transformation._build_party_kyc_assessment(df_kyc).first()
    assert row_kyc.source_business_key == "KYC-01"

    # Employment
    schema_emp = StructType([
        StructField("employment_id", StringType(), True),
        StructField("customer_ref", StringType(), True),
        StructField("employer_name", StringType(), True),
        StructField("job_title", StringType(), True),
        StructField("monthly_income", DoubleType(), True),
        StructField("business_date", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_emp = test_spark.createDataFrame([
        ("EMP-01", "CB-101", "Tech Corp", "Engineer", 3500.00, "2026-01-01", "RUN_01")
    ], schema_emp)
    row_emp = customer_transformation._build_party_employment(df_emp).first()
    assert float(row_emp.monthly_income) == 3500.00

    # Service Request
    schema_req = StructType([
        StructField("request_id", StringType(), True),
        StructField("customer_ref", StringType(), True),
        StructField("request_type", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("request_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("resolution_date", StringType(), True),
        StructField("description", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_req = test_spark.createDataFrame([
        ("REQ-01", "CRM-202", "INQUIRY", "MOBILE", "2026-02-01", "CLOSED", "2026-02-02", "Card limit query", "RUN_01")
    ], schema_req)
    row_req = customer_transformation._build_party_service_request(df_req).first()
    assert row_req.request_status == "CLOSED"


# Direct execution entrypoint
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])
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

import builtins
import os
import sys
# Databricks notebook source
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import pyspark
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, LongType, StringType,
                               StructField, StructType)

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
# 3. IMPORT TARGET MODULE
# ------------------------------------------------------------------------------
import fincrime_validation

# ==============================================================================
# UNIT TEST CASES
# ==============================================================================


def test_fincrime_tables_mapping():
    """Verify TABLES dict properly combines scd2 and append tables for fincrime domain."""
    assert "aml_case" in fincrime_validation.TABLES
    assert (
        "fraud_alert" in fincrime_validation.TABLES
    )  # Sửa 'aml_alert' thành 'fraud_alert'


def test_assess_attaches_failed_rule_names(test_spark):
    schema = StructType(
        [
            StructField("alert_id", StringType(), True),
            StructField("score", DoubleType(), True),
        ]
    )

    data = [("ALT_01", 85.5), ("ALT_02", -5.0), (None, 50.0)]
    df_input = test_spark.createDataFrame(data, schema)

    mock_rules = {
        "aml_alert": [
            {"name": "alert_id_not_null", "constraint": "alert_id IS NOT NULL"},
            {"name": "score_positive", "constraint": "score > 0"},
        ]
    }

    # Patch trực tiếp namespace fincrime_validation.RULES_BY_TABLE
    with patch.dict(fincrime_validation.RULES_BY_TABLE, mock_rules, clear=True):
        res_df = fincrime_validation.assess(df_input, "aml_alert")
        rows = {r.alert_id: r for r in res_df.collect()}

        # 1. Valid Record
        assert len(rows["ALT_01"]._failed_rule_names) == 0

        # 2. Score <= 0 Failed Record
        assert "score_positive" in rows["ALT_02"]._failed_rule_names
        assert len(rows["ALT_02"]._failed_rule_names) == 1

        # 3. Null alert_id Record
        null_row = next(r for r in res_df.collect() if r.alert_id is None)
        assert "alert_id_not_null" in null_row._failed_rule_names
        assert len(null_row._failed_rule_names) == 1


def test_assess_table_with_no_rules_configured(test_spark):
    """Verify assess handles tables without any configured quality rules gracefully."""
    schema = StructType(
        [
            StructField("case_id", StringType(), True),
            StructField("status", StringType(), True),
        ]
    )
    df_input = test_spark.createDataFrame([("CASE_100", "OPEN")], schema)

    with patch("data_contracts.quality_rules.registry.RULES_BY_TABLE", {}):
        res_df = fincrime_validation.assess(df_input, "unregistered_table")
        row = res_df.first()

        assert "_failed_rule_names" in res_df.columns
        assert len(row._failed_rule_names) == 0


if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])
    pytest.main(["-v", "-s", __file__])

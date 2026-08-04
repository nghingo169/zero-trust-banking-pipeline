"""
Unit Tests for ai_aml_investigation_context Gold View
=====================================================
Target View : gold.ai_aml_investigation_context
Test Focus  : Window Deduplication, Aggregations, DQ Status Logic, and Default Values.
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
GOLD_SRC = str(PROJECT_ROOT / "src" / "pipeline" / "gold")

for path_str in [str(PROJECT_ROOT), PROJECT_SRC, SILVER_SRC, GOLD_SRC]:
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
        .config("pipeline.gold_schema", "gold")
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
    "pipeline.gold_schema": "gold",
    "pipeline.quality_rules_path": ".",
}.items():
    try:
        builtins.spark.conf.set(k, v)
    except Exception:
        pass

# Mock DLT & pyspark.pipelines safely across all test execution orders
if not hasattr(pyspark, "pipelines"):
    pipelines_mock = ModuleType("pyspark.pipelines")
    pipelines_mock.table = lambda *args, **kwargs: (lambda func: func)
    pipelines_mock.temporary_view = lambda *args, **kwargs: (lambda func: func)
    pipelines_mock.expect_or_drop = lambda *args, **kwargs: (lambda func: func)
    pipelines_mock.expect_or_fail = lambda *args, **kwargs: (lambda func: func)
    pipelines_mock.expect = lambda *args, **kwargs: (lambda func: func)
    pyspark.pipelines = pipelines_mock
    sys.modules["pyspark.pipelines"] = pipelines_mock
else:
    for attr in (
        "table",
        "temporary_view",
        "expect_or_drop",
        "expect_or_fail",
        "expect",
    ):
        if not hasattr(pyspark.pipelines, attr):
            setattr(
                pyspark.pipelines, attr, lambda *args, **kwargs: (lambda func: func)
            )

if "dlt" not in sys.modules:
    dlt_mock = ModuleType("dlt")
    dlt_mock.table = lambda *args, **kwargs: (lambda func: func)
    dlt_mock.temporary_view = lambda *args, **kwargs: (lambda func: func)
    sys.modules["dlt"] = dlt_mock


def _patched_reader():
    """Return the DataFrameReader class to patch `.table()` on (Connect or classic)."""
    try:
        import pyspark.sql.connect.readwriter as rw
    except ImportError:
        import pyspark.sql.readwriter as rw
    return rw.DataFrameReader


# ------------------------------------------------------------------------------
# 2. LOCAL / DATABRICKS SPARK SESSION FIXTURE
# ------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def test_spark():
    return test_spark_session


# ------------------------------------------------------------------------------
# 3. IMPORT TARGET MODULE
# ------------------------------------------------------------------------------
import aml_investigation_context


def test_ai_aml_investigation_context_aggregations_and_dq_status(test_spark):
    """
    Verify ai_aml_investigation_context accurately performs window deduplication,
    aggregates linked alerts/notes/screenings, and assigns correct dq_status.
    """
    aml_investigation_context.spark = test_spark

    # 1. Mock Investigation Case
    schema_ic = StructType(
        [
            StructField("investigation_case_key", StringType(), True),
            StructField("investigation_type", StringType(), True),
            StructField("case_origin", StringType(), True),
            StructField("case_status", StringType(), True),
            StructField("priority", StringType(), True),
            StructField("opened_at", StringType(), True),
            StructField("closed_at", StringType(), True),
            StructField("assigned_analyst_id", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("source_business_key", StringType(), True),
            StructField("ingested_at", StringType(), True),
            StructField("pipeline_run_id", StringType(), True),
        ]
    )
    df_ic = test_spark.createDataFrame(
        [
            (
                "IC_001",
                "AML",
                "ALERT",
                "CLOSED",
                "HIGH",
                "2026-07-01 09:00:00",
                "2026-07-05 10:00:00",
                "ANALYST_01",
                "fincrime",
                "CASE_001",
                "2026-07-05 10:00:00",
                "RUN_01",
            ),
            (
                "IC_002",
                "FRAUD",
                "REFERRAL",
                "OPEN",
                "MEDIUM",
                "2026-07-02 09:00:00",
                None,
                "ANALYST_02",
                "fincrime",
                "CASE_002",
                "2026-07-02 09:00:00",
                "RUN_01",
            ),
        ],
        schema_ic,
    )

    # 2. Mock AML Case
    schema_ac = StructType(
        [
            StructField("aml_case_key", StringType(), True),
            StructField("investigation_case_key", StringType(), True),
            StructField("party_key", StringType(), True),
            StructField("risk_level", StringType(), True),
            StructField("opened_date", StringType(), True),
        ]
    )
    df_ac = test_spark.createDataFrame(
        [
            ("AML_OLD", "IC_001", "PARTY_100", "MEDIUM", "2026-06-01"),
            ("AML_NEW", "IC_001", "PARTY_100", "HIGH", "2026-07-01"),
        ],
        schema_ac,
    )

    # 3. Mock Suspicious Activity Report
    schema_sar = StructType(
        [
            StructField("sar_id", StringType(), True),
            StructField("aml_case_key", StringType(), True),
            StructField("filed_date", StringType(), True),
            StructField("regulatory_reference", StringType(), True),
            StructField("report_status", StringType(), True),
        ]
    )
    df_sar = test_spark.createDataFrame(
        [("SAR_01", "AML_NEW", "2026-07-04", None, "FILED")], schema_sar
    )

    # 4. Mock Sanctions Screening Links
    schema_icss = StructType(
        [
            StructField("investigation_case_key", StringType(), True),
            StructField("sanctions_screening_key", StringType(), True),
        ]
    )
    df_icss = test_spark.createDataFrame([("IC_001", "SCR_100")], schema_icss)

    schema_ss = StructType(
        [
            StructField("sanctions_screening_key", StringType(), True),
            StructField("watchlist_entry_key", StringType(), True),
            StructField("screening_result", StringType(), True),
            StructField("match_score", DoubleType(), True),
        ]
    )
    df_ss = test_spark.createDataFrame(
        [("SCR_100", "WL_500", "POTENTIAL_MATCH", 92.5)], schema_ss
    )

    schema_we = StructType(
        [
            StructField("watchlist_entry_key", StringType(), True),
            StructField("list_type", StringType(), True),
        ]
    )
    df_we = test_spark.createDataFrame([("WL_500", "SANCTIONS")], schema_we)

    # 5. Mock Fraud Alerts, Monitoring Alerts, Financial Events
    schema_icfa = StructType(
        [
            StructField("investigation_case_key", StringType(), True),
            StructField("fraud_alert_key", StringType(), True),
        ]
    )
    df_icfa = test_spark.createDataFrame(
        [("IC_001", "FA_01"), ("IC_001", "FA_02")], schema_icfa
    )

    schema_icma = StructType(
        [
            StructField("investigation_case_key", StringType(), True),
            StructField("monitoring_alert_key", StringType(), True),
        ]
    )
    df_icma = test_spark.createDataFrame([("IC_001", "MA_01")], schema_icma)

    schema_icfe = StructType(
        [
            StructField("investigation_case_key", StringType(), True),
            StructField("financial_event_key", StringType(), True),
        ]
    )
    df_icfe = test_spark.createDataFrame([("IC_001", "FE_01")], schema_icfe)

    # 6. Mock Analyst Notes
    schema_in = StructType(
        [
            StructField("investigation_case_key", StringType(), True),
            StructField("note_text", StringType(), True),
            StructField("note_timestamp", StringType(), True),
        ]
    )
    df_in = test_spark.createDataFrame(
        [
            ("IC_001", "Initial review completed.", "2026-07-01 10:00:00"),
            ("IC_001", "Escalated for SAR filing.", "2026-07-03 15:00:00"),
        ],
        schema_in,
    )

    def mock_read_table(table_name):
        short_name = table_name.split(".")[-1]
        if "investigation_case_sanctions_screening" in short_name:
            return df_icss
        elif "investigation_case_fraud_alert" in short_name:
            return df_icfa
        elif "investigation_case_monitoring_alert" in short_name:
            return df_icma
        elif "investigation_case_financial_event" in short_name:
            return df_icfe
        elif "investigation_case" == short_name:
            return df_ic
        elif "aml_case" == short_name:
            return df_ac
        elif "suspicious_activity_report" == short_name:
            return df_sar
        elif "sanctions_screening" == short_name:
            return df_ss
        elif "watchlist_entry" == short_name:
            return df_we
        elif "investigation_note" == short_name:
            return df_in
        return test_spark.createDataFrame([], StructType([]))

    with patch.object(_patched_reader(), "table", side_effect=mock_read_table):
        res_df = aml_investigation_context.ai_aml_investigation_context()
        rows = {r.investigation_case_key: r for r in res_df.collect()}

        # Assertion for IC_001 (Closed AML Case)
        row1 = rows["IC_001"]
        assert row1.aml_case_key == "AML_NEW"
        assert row1.aml_risk_level == "HIGH"
        assert row1.sar_filed_flag is True
        assert row1.sar_regulatory_reference is None
        assert row1.dq_status == "PENDING_REGULATORY_REF"

        # Aggregations for IC_001
        assert row1.sanctions_screening_count == 1
        assert row1.sanctions_hit_flag is True
        assert float(row1.max_sanctions_match_score) == 92.50
        assert row1.linked_watchlist_types == "SANCTIONS"
        assert row1.linked_fraud_alert_count == 2
        assert row1.linked_monitoring_alert_count == 1
        assert row1.linked_financial_event_count == 1
        assert row1.investigation_note_count == 2
        assert row1.latest_note_text == "Escalated for SAR filing."

        # Assertion for IC_002 (Open FRAUD Case)
        row2 = rows["IC_002"]
        assert row2.aml_case_key is None
        assert row2.sar_filed_flag is False
        assert row2.dq_status == "PASSED_CLEAN"
        assert row2.linked_fraud_alert_count == 0
        assert row2.linked_monitoring_alert_count == 0
        assert row2.investigation_note_count == 0


if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])

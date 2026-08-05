# Databricks notebook source
"""Unit tests for pipeline.gold.customer_360_context module (ai_customer_360_context).

Tests the ai_customer_360_context gold table transformation end-to-end:
- SCD2 "current profile" resolution (is_current filter)
- Latest KYC assessment resolution (row_number over verified_date desc)
- Current employment resolution (open record only, latest effective_from)
- Account / balance / card aggregation (active_account_count, total_current_balance,
  active_card_count)
- Service request open-count logic (excludes RESOLVED / REJECTED only)
- Call center 90-day contact count vs. all-time last_call_reason (max_by)
- AML / investigation open-flag resolution (excludes CLOSED / RESOLVED cases)
- monthly_income banding thresholds (<10M, 10-30M, 30-100M, 100M+)
- dq_status precedence (REJECTED_QUALITY > WARNING_UNRESOLVED_PARTY > PASSED_CLEAN)
"""

import builtins
import os
import sys
from datetime import date, timedelta

# Databricks notebook source
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

import pyspark
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ------------------------------------------------------------------------------
# 1. DYNAMIC PATH RESOLUTION
# ------------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROJECT_SRC = str(PROJECT_ROOT / "src")
GOLD_SRC = str(PROJECT_ROOT / "src" / "pipeline" / "gold")

for path_str in [str(PROJECT_ROOT), PROJECT_SRC, GOLD_SRC]:
    if os.path.exists(path_str) and path_str not in sys.path:
        sys.path.insert(0, path_str)

try:
    test_spark_session = spark  # Databricks Runtime Context
except NameError:
    test_spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("Pipeline-UnitTest-Gold")
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

# Mock DLT & pyspark.pipelines so @dp.table / @dp.expect_or_drop become no-op
# pass-through decorators, same approach as the silver-layer tests.
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
    # pyspark.pipelines may already have been mocked (e.g. by the silver test
    # module in the same session) without expect_or_drop -- top it up.
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
    dlt_mock.expect_or_drop = lambda *args, **kwargs: (lambda func: func)
    sys.modules["dlt"] = dlt_mock

# Mock gold_common (silver_ref / gold_target_name). Try the real project module
# first (dynamic path resolution above may have exposed it); fall back to a
# lightweight stand-in that just returns the bare short table name, so the
# DataFrameReader.table mock below can key off of it directly.
try:
    import gold_common  # noqa: F401
except ImportError:
    gold_common_mock = ModuleType("gold_common")
    gold_common_mock.silver_ref = lambda spark_session, name: name
    gold_common_mock.gold_target_name = lambda spark_session, name: name
    sys.modules["gold_common"] = gold_common_mock


# ------------------------------------------------------------------------------
# 2. LOCAL / DATABRICKS SPARK SESSION FIXTURE
# ------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def test_spark():
    return test_spark_session


# ------------------------------------------------------------------------------
# 3. IMPORT TARGET MODULE
# ------------------------------------------------------------------------------
import customer_360_context


def _patched_reader():
    """Return the DataFrameReader class to patch `.table()` on (Connect or classic)."""
    try:
        import pyspark.sql.connect.readwriter as rw
    except ImportError:
        import pyspark.sql.readwriter as rw
    return rw.DataFrameReader


# ==============================================================================
# SECTION 1: FULL END-TO-END SCENARIO TEST
# ==============================================================================
def test_ai_customer_360_context_core_scenarios(test_spark):
    """
    Exercises ai_customer_360_context() across three parties:

      P1 -> CORE_BANKING, fully resolved, VERIFIED KYC       -> dq_status PASSED_CLEAN
      P2 -> CRM, missing KYC, has an open investigation       -> dq_status WARNING_UNRESOLVED_PARTY
      P3 -> CORE_BANKING, quarantined at source                -> dq_status REJECTED_QUALITY
    """
    customer_360_context.spark = test_spark
    today = date.today()

    # ---------------------------------------------------------------- party
    schema_party = StructType(
        [
            StructField("party_key", StringType(), True),
            StructField("party_type", StringType(), True),
            StructField("party_status", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("source_business_key", StringType(), True),
            StructField("ingested_at", StringType(), True),
            StructField("pipeline_run_id", StringType(), True),
            StructField("data_quality_status", StringType(), True),
        ]
    )
    df_party = test_spark.createDataFrame(
        [
            (
                "P1",
                "PERSON",
                "ACTIVE",
                "CORE_BANKING",
                "CB-101",
                "2026-07-01 00:00:00",
                "RUN_01",
                "CLEAN",
            ),
            (
                "P2",
                "PERSON",
                "ACTIVE",
                "CRM",
                "CRM-202",
                "2026-07-01 00:00:00",
                "RUN_01",
                "CLEAN",
            ),
            (
                "P3",
                "PERSON",
                "PENDING",
                "CORE_BANKING",
                "CB-303",
                "2026-07-01 00:00:00",
                "RUN_01",
                "QUARANTINED",
            ),
        ],
        schema_party,
    )

    # ---------------------------------------------------------- profile version (SCD2)
    schema_ppv = StructType(
        [
            StructField("party_key", StringType(), True),
            StructField("is_current", BooleanType(), True),
            StructField("preferred_contact_method", StringType(), True),
            StructField("effective_from", StringType(), True),
        ]
    )
    df_ppv = test_spark.createDataFrame(
        [
            (
                "P1",
                False,
                None,
                "2025-01-01 00:00:00",
            ),  # superseded version -> must be excluded
            (
                "P1",
                True,
                None,
                "2026-01-01 00:00:00",
            ),  # current: CORE_BANKING -> no preferred_contact_method
            ("P2", True, "EMAIL", "2026-02-01 00:00:00"),  # current
            # P3 has no profile version at all -> left-join nulls
        ],
        schema_ppv,
    )

    # ---------------------------------------------------------------- KYC
    schema_kyc = StructType(
        [
            StructField("party_key", StringType(), True),
            StructField("verified_date", StringType(), True),
            StructField("verification_status", StringType(), True),
            StructField("id_type", StringType(), True),
            StructField("id_number_token", StringType(), True),
        ]
    )
    df_kyc = test_spark.createDataFrame(
        [
            (
                "P1",
                "2025-12-01",
                "PENDING",
                "NATIONAL_ID",
                "old_token_hash",
            ),  # superseded
            (
                "P1",
                "2026-01-05",
                "VERIFIED",
                "NATIONAL_ID",
                "new_token_hash",
            ),  # latest -> wins
            # P2: no KYC record at all -> triggers WARNING_UNRESOLVED_PARTY
            # P3: no KYC record either, but REJECTED_QUALITY takes precedence
        ],
        schema_kyc,
    )

    # ---------------------------------------------------------------- employment
    schema_emp = StructType(
        [
            StructField("party_key", StringType(), True),
            StructField("effective_to", StringType(), True),
            StructField("effective_from", StringType(), True),
            StructField("employer_name", StringType(), True),
            StructField("job_title", StringType(), True),
            StructField("monthly_income", DoubleType(), True),
        ]
    )
    df_emp = test_spark.createDataFrame(
        [
            (
                "P1",
                "2025-06-01",
                "2025-01-01",
                "Old Corp",
                "Junior Engineer",
                20000000.0,
            ),  # closed -> excluded
            (
                "P1",
                None,
                "2026-01-01",
                "Tech Corp",
                "Engineer",
                45000000.0,
            ),  # open/current -> 30-100M
            (
                "P2",
                None,
                "2026-01-01",
                "Retail Co",
                "Cashier",
                8000000.0,
            ),  # open/current -> <10M
            # P3: no employment record -> NULL band
        ],
        schema_emp,
    )

    # ---------------------------------------------------------------- account roles / accounts
    schema_par = StructType(
        [
            StructField("party_key", StringType(), True),
            StructField("account_key", StringType(), True),
            StructField("valid_to", StringType(), True),
        ]
    )
    df_par = test_spark.createDataFrame(
        [
            ("P1", "ACC1", None),  # open role
            (
                "P1",
                "ACC2",
                "2025-01-01",
            ),  # closed role -> excluded from active_account_count
            ("P2", "ACC3", None),  # open role
        ],
        schema_par,
    )

    schema_acct = StructType(
        [
            StructField("account_key", StringType(), True),
            StructField("account_status", StringType(), True),
        ]
    )
    df_acct = test_spark.createDataFrame(
        [
            ("ACC1", "ACTIVE"),
            ("ACC2", "CLOSED"),
            ("ACC3", "ACTIVE"),
        ],
        schema_acct,
    )

    # ---------------------------------------------------------------- balances
    schema_bal = StructType(
        [
            StructField("account_key", StringType(), True),
            StructField("balance_date", StringType(), True),
            StructField("closing_balance", DoubleType(), True),
        ]
    )
    df_bal_raw = test_spark.createDataFrame(
        [
            (
                "ACC1",
                (today - timedelta(days=20)).isoformat(),
                14000000.0,
            ),  # older snapshot, within 30d
            (
                "ACC1",
                (today - timedelta(days=5)).isoformat(),
                15000000.0,
            ),  # latest snapshot -> should win
            ("ACC3", (today - timedelta(days=3)).isoformat(), 5000000.0),
            # ACC2 has no balance snapshot at all
        ],
        schema_bal,
    )
    df_bal = df_bal_raw.withColumn("balance_date", F.col("balance_date").cast("date"))

    # ---------------------------------------------------------------- cards
    schema_card = StructType(
        [
            StructField("account_key", StringType(), True),
            StructField("payment_card_key", StringType(), True),
            StructField("card_status", StringType(), True),
        ]
    )
    df_card = test_spark.createDataFrame(
        [
            ("ACC1", "CARD1", "ACTIVE"),
            ("ACC1", "CARD2", "BLOCKED"),
            ("ACC3", "CARD3", "ACTIVE"),
        ],
        schema_card,
    )

    # ---------------------------------------------------------------- service requests
    schema_req = StructType(
        [
            StructField("party_key", StringType(), True),
            StructField("request_status", StringType(), True),
        ]
    )
    df_req = test_spark.createDataFrame(
        [
            ("P1", "RESOLVED"),  # excluded -> P1 open count = 0
            ("P2", "OPEN"),  # included -> P2 open count = 1
            ("P2", "REJECTED"),  # excluded
        ],
        schema_req,
    )

    # ---------------------------------------------------------------- call center
    schema_call = StructType(
        [
            StructField("party_key", StringType(), True),
            StructField("call_timestamp", StringType(), True),
            StructField("call_reason", StringType(), True),
        ]
    )
    df_call_raw = test_spark.createDataFrame(
        [
            (
                "P1",
                f"{(today - timedelta(days=10)).isoformat()} 09:00:00",
                "BALANCE_INQUIRY",
            ),
            (
                "P1",
                f"{(today - timedelta(days=5)).isoformat()} 14:30:00",
                "CARD_LOST",
            ),  # most recent -> last_call_reason
            (
                "P3",
                f"{(today - timedelta(days=200)).isoformat()} 11:00:00",
                "COMPLAINT",
            ),  # outside 90d, still "last" overall
            # P2 never called
        ],
        schema_call,
    )
    df_call = df_call_raw.withColumn(
        "call_timestamp", F.col("call_timestamp").cast("timestamp")
    )

    # ---------------------------------------------------------------- AML / investigation
    schema_aml = StructType(
        [
            StructField("investigation_case_key", StringType(), True),
            StructField("party_key", StringType(), True),
        ]
    )
    df_aml = test_spark.createDataFrame(
        [
            ("IC1", "P2"),  # open case
            ("IC2", "P3"),  # closed case -> must NOT raise the flag
        ],
        schema_aml,
    )

    schema_inv = StructType(
        [
            StructField("investigation_case_key", StringType(), True),
            StructField("case_status", StringType(), True),
        ]
    )
    df_inv = test_spark.createDataFrame(
        [
            ("IC1", "OPEN"),
            ("IC2", "CLOSED"),
        ],
        schema_inv,
    )

    table_map = {
        "party": df_party,
        "party_profile_version": df_ppv,
        "party_kyc_assessment": df_kyc,
        "party_employment": df_emp,
        "party_account_role": df_par,
        "account": df_acct,
        "account_balance_snapshot": df_bal,
        "payment_card": df_card,
        "party_service_request": df_req,
        "call_center_contact": df_call,
        "aml_case": df_aml,
        "investigation_case": df_inv,
    }

    def mock_read_table(table_name):
        short_name = table_name.split(".")[-1]
        return table_map[short_name]

    with patch.object(_patched_reader(), "table", side_effect=mock_read_table):
        res_df = customer_360_context.ai_customer_360_context()
        rows = {r.party_key: r for r in res_df.collect()}

    assert set(rows.keys()) == {"P1", "P2", "P3"}

    # ---- P1: fully resolved, CORE_BANKING ------------------------------------
    p1 = rows["P1"]
    assert p1.party_status == "ACTIVE"
    assert p1.preferred_contact_method is None  # CORE_BANKING sourced, NULL by design
    assert str(p1.profile_effective_from).startswith("2026-01-01")
    assert p1.kyc_verification_status == "VERIFIED"
    assert p1.kyc_id_type == "NATIONAL_ID"
    assert p1.kyc_id_number == "new_token_hash"
    assert p1.employer_name == "Tech Corp"
    assert p1.monthly_income_band == "30-100M"
    assert p1.active_account_count == 1
    assert float(p1.total_current_balance) == 15000000.0
    assert p1.active_card_count == 1
    assert p1.open_service_request_count == 0
    assert p1.call_center_contact_count_90d == 2
    assert p1.last_call_reason == "CARD_LOST"
    assert p1.open_investigation_flag is False
    assert p1.source_system == "CORE_BANKING"
    assert p1.source_business_key == "CB-101"
    assert p1.dq_status == "PASSED_CLEAN"

    # ---- P2: missing KYC, has an open investigation --------------------------
    p2 = rows["P2"]
    assert p2.preferred_contact_method == "EMAIL"
    assert p2.kyc_verification_status is None
    assert p2.monthly_income_band == "<10M"
    assert p2.active_account_count == 1
    assert float(p2.total_current_balance) == 5000000.0
    assert p2.active_card_count == 1
    assert p2.open_service_request_count == 1
    assert p2.call_center_contact_count_90d == 0
    assert p2.last_call_reason is None
    assert p2.open_investigation_flag is True
    assert p2.dq_status == "WARNING_UNRESOLVED_PARTY"

    # ---- P3: quarantined at source -> REJECTED_QUALITY regardless of KYC -----
    p3 = rows["P3"]
    assert p3.party_status == "PENDING"
    assert p3.preferred_contact_method is None
    assert p3.profile_effective_from is None
    assert p3.employer_name is None
    assert p3.monthly_income_band is None
    assert p3.active_account_count == 0
    assert p3.total_current_balance is None
    assert p3.active_card_count == 0
    assert p3.call_center_contact_count_90d == 0
    assert (
        p3.last_call_reason == "COMPLAINT"
    )  # "any time" -> still surfaces despite being 200 days old
    assert p3.open_investigation_flag is False  # its only case is CLOSED
    assert p3.dq_status == "REJECTED_QUALITY"


# ==============================================================================
# SECTION 2: MONTHLY INCOME BANDING BOUNDARY TEST
# ==============================================================================
def test_monthly_income_banding_boundaries(test_spark):
    """Verify monthly_income_band bucketing exactly at each threshold edge."""
    customer_360_context.spark = test_spark

    schema_party = StructType(
        [
            StructField("party_key", StringType(), True),
            StructField("party_type", StringType(), True),
            StructField("party_status", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("source_business_key", StringType(), True),
            StructField("ingested_at", StringType(), True),
            StructField("pipeline_run_id", StringType(), True),
            StructField("data_quality_status", StringType(), True),
        ]
    )
    df_party = test_spark.createDataFrame(
        [
            (
                "B1",
                "PERSON",
                "ACTIVE",
                "CORE_BANKING",
                "CB-1",
                "2026-01-01",
                "RUN_01",
                "CLEAN",
            ),
            (
                "B2",
                "PERSON",
                "ACTIVE",
                "CORE_BANKING",
                "CB-2",
                "2026-01-01",
                "RUN_01",
                "CLEAN",
            ),
            (
                "B3",
                "PERSON",
                "ACTIVE",
                "CORE_BANKING",
                "CB-3",
                "2026-01-01",
                "RUN_01",
                "CLEAN",
            ),
            (
                "B4",
                "PERSON",
                "ACTIVE",
                "CORE_BANKING",
                "CB-4",
                "2026-01-01",
                "RUN_01",
                "CLEAN",
            ),
            (
                "B5",
                "PERSON",
                "ACTIVE",
                "CORE_BANKING",
                "CB-5",
                "2026-01-01",
                "RUN_01",
                "CLEAN",
            ),
        ],
        schema_party,
    )

    schema_emp = StructType(
        [
            StructField("party_key", StringType(), True),
            StructField("effective_to", StringType(), True),
            StructField("effective_from", StringType(), True),
            StructField("employer_name", StringType(), True),
            StructField("job_title", StringType(), True),
            StructField("monthly_income", DoubleType(), True),
        ]
    )
    df_emp = test_spark.createDataFrame(
        [
            ("B1", None, "2026-01-01", "Co1", "Role1", 9999999.0),  # <10M
            (
                "B2",
                None,
                "2026-01-01",
                "Co2",
                "Role2",
                10000000.0,
            ),  # 10-30M (lower edge, inclusive)
            ("B3", None, "2026-01-01", "Co3", "Role3", 29999999.0),  # 10-30M
            (
                "B4",
                None,
                "2026-01-01",
                "Co4",
                "Role4",
                30000000.0,
            ),  # 30-100M (lower edge, inclusive)
            (
                "B5",
                None,
                "2026-01-01",
                "Co5",
                "Role5",
                100000000.0,
            ),  # 100M+ (lower edge, inclusive)
        ],
        schema_emp,
    )

    empty_ppv = test_spark.createDataFrame(
        [],
        StructType(
            [
                StructField("party_key", StringType(), True),
                StructField("is_current", BooleanType(), True),
                StructField("preferred_contact_method", StringType(), True),
                StructField("effective_from", StringType(), True),
            ]
        ),
    )
    empty_kyc = test_spark.createDataFrame(
        [],
        StructType(
            [
                StructField("party_key", StringType(), True),
                StructField("verified_date", StringType(), True),
                StructField("verification_status", StringType(), True),
                StructField("id_type", StringType(), True),
                StructField("id_number_token", StringType(), True),
            ]
        ),
    )
    empty_par = test_spark.createDataFrame(
        [],
        StructType(
            [
                StructField("party_key", StringType(), True),
                StructField("account_key", StringType(), True),
                StructField("valid_to", StringType(), True),
            ]
        ),
    )
    empty_acct = test_spark.createDataFrame(
        [],
        StructType(
            [
                StructField("account_key", StringType(), True),
                StructField("account_status", StringType(), True),
            ]
        ),
    )
    empty_bal = test_spark.createDataFrame(
        [],
        StructType(
            [
                StructField("account_key", StringType(), True),
                StructField("balance_date", DateType(), True),
                StructField("closing_balance", DoubleType(), True),
            ]
        ),
    )
    empty_card = test_spark.createDataFrame(
        [],
        StructType(
            [
                StructField("account_key", StringType(), True),
                StructField("payment_card_key", StringType(), True),
                StructField("card_status", StringType(), True),
            ]
        ),
    )
    empty_req = test_spark.createDataFrame(
        [],
        StructType(
            [
                StructField("party_key", StringType(), True),
                StructField("request_status", StringType(), True),
            ]
        ),
    )
    empty_call = test_spark.createDataFrame(
        [],
        StructType(
            [
                StructField("party_key", StringType(), True),
                StructField("call_timestamp", TimestampType(), True),
                StructField("call_reason", StringType(), True),
            ]
        ),
    )
    empty_aml = test_spark.createDataFrame(
        [],
        StructType(
            [
                StructField("investigation_case_key", StringType(), True),
                StructField("party_key", StringType(), True),
            ]
        ),
    )
    empty_inv = test_spark.createDataFrame(
        [],
        StructType(
            [
                StructField("investigation_case_key", StringType(), True),
                StructField("case_status", StringType(), True),
            ]
        ),
    )

    table_map = {
        "party": df_party,
        "party_profile_version": empty_ppv,
        "party_kyc_assessment": empty_kyc,
        "party_employment": df_emp,
        "party_account_role": empty_par,
        "account": empty_acct,
        "account_balance_snapshot": empty_bal,
        "payment_card": empty_card,
        "party_service_request": empty_req,
        "call_center_contact": empty_call,
        "aml_case": empty_aml,
        "investigation_case": empty_inv,
    }

    def mock_read_table(table_name):
        short_name = table_name.split(".")[-1]
        return table_map[short_name]

    with patch.object(_patched_reader(), "table", side_effect=mock_read_table):
        res_df = customer_360_context.ai_customer_360_context()
        bands = {r.party_key: r.monthly_income_band for r in res_df.collect()}

    assert bands == {
        "B1": "<10M",
        "B2": "10-30M",
        "B3": "10-30M",
        "B4": "30-100M",
        "B5": "100M+",
    }


# Direct execution entrypoint
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])

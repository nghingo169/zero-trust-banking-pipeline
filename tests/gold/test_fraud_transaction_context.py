# Databricks notebook source
from pathlib import Path
import os
import sys
import builtins
from types import ModuleType
from unittest.mock import patch
import pytest

import pyspark
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType,
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
        SparkSession.builder
        .master("local[1]")
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
# pass-through decorators, same approach used across the other gold/silver tests.
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
    # pyspark.pipelines may already have been mocked by another test module
    # (e.g. customer_360_context) in the same session -- top up anything missing.
    for attr in ("table", "temporary_view", "expect_or_drop", "expect_or_fail", "expect"):
        if not hasattr(pyspark.pipelines, attr):
            setattr(pyspark.pipelines, attr, lambda *args, **kwargs: (lambda func: func))

if "dlt" not in sys.modules:
    dlt_mock = ModuleType("dlt")
    dlt_mock.table = lambda *args, **kwargs: (lambda func: func)
    dlt_mock.temporary_view = lambda *args, **kwargs: (lambda func: func)
    dlt_mock.expect_or_drop = lambda *args, **kwargs: (lambda func: func)
    sys.modules["dlt"] = dlt_mock

# Mock gold_common (silver_ref / gold_target_name). Try the real project module
# first; fall back to a lightweight stand-in that returns the bare short table
# name, so the DataFrameReader.table mock below can key off of it directly.
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
import fraud_transaction_context


def _patched_reader():
    """Return the DataFrameReader class to patch `.table()` on (Connect or classic)."""
    try:
        import pyspark.sql.connect.readwriter as rw
    except ImportError:
        import pyspark.sql.readwriter as rw
    return rw.DataFrameReader


# ==============================================================================
# FULL END-TO-END SCENARIO TEST
# ==============================================================================
def test_ai_fraud_transaction_context_core_scenarios(test_spark):
    """
    Exercises ai_fraud_transaction_context() across five financial events:

      FE1 -> ACCOUNT_POSTING, fully resolved, merchant w/ 2 stores (MEDIUM ceiling),
             2 risk scores (latest wins), 2 fraud alerts (1 open)   -> PASSED_CLEAN
      FE2 -> CARD_PAYMENT, unresolved party, merchant w/ 1 HIGH store,
             2 card fraud flags, no alerts/score                    -> WARNING_UNRESOLVED_PARTY
      FE3 -> ATM_ACTIVITY, no merchant/channel by design             -> PASSED_CLEAN
      FE4 -> GATEWAY_PAYMENT, merchant with zero registered stores   -> PASSED_CLEAN
      FE5 -> ACCOUNT_POSTING, quarantined at source (party resolved) -> REJECTED_QUALITY
    """
    fraud_transaction_context.spark = test_spark

    # ---------------------------------------------------------------- financial_event
    schema_fe = StructType([
        StructField("financial_event_key", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("occurred_at", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("account_key", StringType(), True),
        StructField("payment_card_key", StringType(), True),
        StructField("party_key", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("source_business_key", StringType(), True),
        StructField("ingested_at", StringType(), True),
        StructField("pipeline_run_id", StringType(), True),
        StructField("data_quality_status", StringType(), True),
    ])
    df_fe = test_spark.createDataFrame([
        ("FE1", "ACCOUNT_POSTING",  "2026-07-01 10:00:00", "VND", "ACC1", None,    "P1", "CORE_BANKING",    "TXN-1001", "2026-07-01 10:05:00", "RUN_01", "CLEAN"),
        ("FE2", "CARD_PAYMENT",     "2026-07-01 11:00:00", "VND", "ACC2", "CARD1", None, "CARD_NETWORK",    "TXN-2002", "2026-07-01 11:05:00", "RUN_01", "CLEAN"),
        ("FE3", "ATM_ACTIVITY",     "2026-07-01 12:00:00", "VND", "ACC3", "CARD2", "P3", "ATM_NETWORK",     "TXN-3003", "2026-07-01 12:05:00", "RUN_01", "CLEAN"),
        ("FE4", "GATEWAY_PAYMENT",  "2026-07-01 13:00:00", "USD", "ACC4", None,    "P4", "PAYMENT_GATEWAY", "TXN-4004", "2026-07-01 13:05:00", "RUN_01", "CLEAN"),
        ("FE5", "ACCOUNT_POSTING",  "2026-07-01 14:00:00", "VND", "ACC5", None,    "P5", "CORE_BANKING",    "TXN-5005", "2026-07-01 14:05:00", "RUN_01", "QUARANTINED"),
    ], schema_fe)

    # ---------------------------------------------------------------- account_posting
    schema_ap = StructType([
        StructField("financial_event_key", StringType(), True),
        StructField("posting_amount", DoubleType(), True),
        StructField("posting_type", StringType(), True),
        StructField("posting_direction", StringType(), True),
        StructField("channel_key", StringType(), True),
        StructField("merchant_key", StringType(), True),
    ])
    df_ap = test_spark.createDataFrame([
        ("FE1", 1500000.0, "DEPOSIT",    "CREDIT", "CH1", "M1"),
        ("FE5", 500000.0,  "WITHDRAWAL", "DEBIT",  None,  None),
    ], schema_ap)

    # ---------------------------------------------------------------- card_payment
    schema_cp = StructType([
        StructField("financial_event_key", StringType(), True),
        StructField("payment_amount", DoubleType(), True),
        StructField("card_transaction_type", StringType(), True),
        StructField("merchant_key", StringType(), True),
        StructField("is_fraud_source_flag", BooleanType(), True),
    ])
    df_cp = test_spark.createDataFrame([
        ("FE2", 250000.0, "POS_PURCHASE", "M2", True),
    ], schema_cp)

    # ---------------------------------------------------------------- atm_activity
    schema_atm = StructType([
        StructField("financial_event_key", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("activity_type", StringType(), True),
    ])
    df_atm = test_spark.createDataFrame([
        ("FE3", 2000000.0, "WITHDRAWAL"),
    ], schema_atm)

    # ---------------------------------------------------------------- gateway_payment
    schema_gp = StructType([
        StructField("financial_event_key", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("merchant_key", StringType(), True),
    ])
    df_gp = test_spark.createDataFrame([
        ("FE4", 99.99, "CREDIT_CARD", "M3"),
    ], schema_gp)

    # ---------------------------------------------------------------- transaction_channel
    schema_tc = StructType([
        StructField("channel_key", StringType(), True),
        StructField("channel_name", StringType(), True),
        StructField("channel_type", StringType(), True),
    ])
    df_tc = test_spark.createDataFrame([
        ("CH1", "Mobile Banking", "DIGITAL"),
    ], schema_tc)

    # ---------------------------------------------------------------- merchant
    schema_m = StructType([
        StructField("merchant_key", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("mcc_code", StringType(), True),
        StructField("country", StringType(), True),
    ])
    df_m = test_spark.createDataFrame([
        ("M1", "Coffee Shop",  "5814", "VN"),
        ("M2", "Online Store", "5999", "US"),
        ("M3", "SaaS Co",      "7372", "US"),
        # M3 intentionally has no rows in merchant_location -> zero registered stores
    ], schema_m)

    # ---------------------------------------------------------------- merchant_location
    schema_ml = StructType([
        StructField("merchant_key", StringType(), True),
        StructField("risk_rating", StringType(), True),
    ])
    df_ml = test_spark.createDataFrame([
        ("M1", "MEDIUM"),
        ("M1", "LOW"),     # M1 ceiling -> MEDIUM, store_count -> 2
        ("M2", "HIGH"),    # M2 ceiling -> HIGH, store_count -> 1
        # M3: no rows at all
    ], schema_ml)

    # ---------------------------------------------------------------- financial_event_risk_score
    schema_fers = StructType([
        StructField("financial_event_key", StringType(), True),
        StructField("scored_date", StringType(), True),
        StructField("model_score", DoubleType(), True),
        StructField("risk_band", StringType(), True),
    ])
    df_fers = test_spark.createDataFrame([
        ("FE1", "2026-06-01", 0.1000, "LOW"),
        ("FE1", "2026-06-30", 0.8500, "HIGH"),  # latest -> should win
        # FE2/FE3/FE4/FE5 are never scored
    ], schema_fers)

    # ---------------------------------------------------------------- fraud alert linkage
    schema_fefa = StructType([
        StructField("financial_event_key", StringType(), True),
        StructField("fraud_alert_key", StringType(), True),
    ])
    df_fefa = test_spark.createDataFrame([
        ("FE1", "FA1"),
        ("FE1", "FA2"),
    ], schema_fefa)

    schema_fa = StructType([
        StructField("fraud_alert_key", StringType(), True),
        StructField("alert_score", DoubleType(), True),
        StructField("alert_status", StringType(), True),
    ])
    df_fa = test_spark.createDataFrame([
        ("FA1", 70.0, "OPEN"),    # not CLOSED -> open_fraud_alert_flag True
        ("FA2", 40.0, "CLOSED"),
    ], schema_fa)

    # ---------------------------------------------------------------- card fraud flags
    schema_cfa = StructType([
        StructField("financial_event_key", StringType(), True),
        StructField("card_fraud_flag_key", StringType(), True),
    ])
    df_cfa = test_spark.createDataFrame([
        ("FE2", "CFF1"),
        ("FE2", "CFF2"),
    ], schema_cfa)

    table_map = {
        "financial_event": df_fe,
        "account_posting": df_ap,
        "card_payment": df_cp,
        "atm_activity": df_atm,
        "gateway_payment": df_gp,
        "transaction_channel": df_tc,
        "merchant": df_m,
        "financial_event_risk_score": df_fers,
        "financial_event_fraud_alert": df_fefa,
        "fraud_alert": df_fa,
        "financial_event_card_fraud_flag": df_cfa,
        "merchant_location": df_ml,
    }

    def mock_read_table(table_name):
        if table_name not in table_map:
            raise AssertionError(f"Unexpected table requested: {table_name}")
        return table_map[table_name]

    with patch.object(_patched_reader(), "table", side_effect=mock_read_table):
        res_df = fraud_transaction_context.ai_fraud_transaction_context()
        rows = {r.financial_event_key: r for r in res_df.collect()}

    assert set(rows.keys()) == {"FE1", "FE2", "FE3", "FE4", "FE5"}

    # ---- FE1: ACCOUNT_POSTING, fully resolved -------------------------------
    fe1 = rows["FE1"]
    assert fe1.event_type == "ACCOUNT_POSTING"
    assert float(fe1.amount) == 1500000.0
    assert fe1.transaction_type_detail == "DEPOSIT"
    assert fe1.posting_direction == "CREDIT"
    assert fe1.channel_key == "CH1"
    assert fe1.channel_name == "Mobile Banking"
    assert fe1.channel_type == "DIGITAL"
    assert fe1.merchant_key == "M1"
    assert fe1.merchant_name == "Coffee Shop"
    assert fe1.merchant_country == "VN"
    assert fe1.merchant_store_count == 2
    assert fe1.merchant_max_store_risk == "MEDIUM"
    assert float(fe1.latest_risk_score) == 0.85          # latest scored_date wins over the older 0.10
    assert fe1.latest_risk_band == "HIGH"
    assert fe1.fraud_alert_count == 2
    assert float(fe1.max_fraud_alert_score) == 70.0
    assert fe1.open_fraud_alert_flag is True              # FA1 is OPEN, not CLOSED
    assert fe1.card_fraud_flag_count == 0
    assert fe1.is_fraud_source_flag is None                # cp is null for this event type
    assert fe1.party_key == "P1"
    assert fe1.dq_status == "PASSED_CLEAN"

    # ---- FE2: CARD_PAYMENT, unresolved party ---------------------------------
    fe2 = rows["FE2"]
    assert fe2.event_type == "CARD_PAYMENT"
    assert float(fe2.amount) == 250000.0
    assert fe2.transaction_type_detail == "POS_PURCHASE"
    assert fe2.posting_direction is None                   # ACCOUNT_POSTING-only field
    assert fe2.channel_key is None                          # ap is null -> tc never matches
    assert fe2.channel_name is None
    assert fe2.merchant_key == "M2"
    assert fe2.merchant_max_store_risk == "HIGH"
    assert fe2.merchant_store_count == 1
    assert fe2.latest_risk_score is None                    # never scored
    assert fe2.fraud_alert_count == 0
    assert fe2.max_fraud_alert_score is None
    assert fe2.open_fraud_alert_flag is False               # coalesced default
    assert fe2.card_fraud_flag_count == 2
    assert fe2.is_fraud_source_flag is True
    assert fe2.party_key is None
    assert fe2.dq_status == "WARNING_UNRESOLVED_PARTY"

    # ---- FE3: ATM_ACTIVITY, no merchant/channel by design ---------------------
    fe3 = rows["FE3"]
    assert fe3.event_type == "ATM_ACTIVITY"
    assert float(fe3.amount) == 2000000.0
    assert fe3.transaction_type_detail == "WITHDRAWAL"
    assert fe3.posting_direction is None
    assert fe3.channel_key is None
    assert fe3.merchant_key is None                         # atm has no merchant_key in the coalesce
    assert fe3.merchant_store_count == 0
    assert fe3.merchant_max_store_risk is None
    assert fe3.latest_risk_score is None
    assert fe3.fraud_alert_count == 0
    assert fe3.card_fraud_flag_count == 0
    assert fe3.party_key == "P3"
    assert fe3.dq_status == "PASSED_CLEAN"

    # ---- FE4: GATEWAY_PAYMENT, merchant with zero registered stores ----------
    fe4 = rows["FE4"]
    assert fe4.event_type == "GATEWAY_PAYMENT"
    assert float(fe4.amount) == 99.99
    assert fe4.transaction_type_detail == "CREDIT_CARD"
    assert fe4.merchant_key == "M3"
    assert fe4.merchant_name == "SaaS Co"
    assert fe4.merchant_store_count == 0                    # coalesced: no merchant_location rows for M3
    assert fe4.merchant_max_store_risk is None
    assert fe4.dq_status == "PASSED_CLEAN"

    # ---- FE5: ACCOUNT_POSTING, quarantined -> overrides a resolved party -----
    fe5 = rows["FE5"]
    assert fe5.event_type == "ACCOUNT_POSTING"
    assert float(fe5.amount) == 500000.0
    assert fe5.party_key == "P5"                             # resolved...
    assert fe5.dq_status == "REJECTED_QUALITY"                # ...but quarantine still wins


# ==============================================================================
# FOCUSED TEST: merchant_max_store_risk ordering (LOW < MEDIUM < HIGH)
# ==============================================================================
def test_merchant_max_store_risk_ordering(test_spark):
    """Verify the store-risk ceiling picks HIGH over MEDIUM/LOW, and a
    LOW-only merchant never gets promoted above LOW."""
    fraud_transaction_context.spark = test_spark

    schema_fe = StructType([
        StructField("financial_event_key", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("occurred_at", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("account_key", StringType(), True),
        StructField("payment_card_key", StringType(), True),
        StructField("party_key", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("source_business_key", StringType(), True),
        StructField("ingested_at", StringType(), True),
        StructField("pipeline_run_id", StringType(), True),
        StructField("data_quality_status", StringType(), True),
    ])
    df_fe = test_spark.createDataFrame([
        ("FEA", "ACCOUNT_POSTING", "2026-07-01 09:00:00", "VND", "ACC9", None, "P9", "CORE_BANKING", "TXN-9001", "2026-07-01 09:05:00", "RUN_01", "CLEAN"),
        ("FEB", "ACCOUNT_POSTING", "2026-07-01 09:10:00", "VND", "ACC9", None, "P9", "CORE_BANKING", "TXN-9002", "2026-07-01 09:15:00", "RUN_01", "CLEAN"),
    ], schema_fe)

    schema_ap = StructType([
        StructField("financial_event_key", StringType(), True),
        StructField("posting_amount", DoubleType(), True),
        StructField("posting_type", StringType(), True),
        StructField("posting_direction", StringType(), True),
        StructField("channel_key", StringType(), True),
        StructField("merchant_key", StringType(), True),
    ])
    df_ap = test_spark.createDataFrame([
        ("FEA", 10000.0, "PURCHASE", "DEBIT", None, "MA"),  # merchant with LOW + HIGH stores -> HIGH ceiling
        ("FEB", 20000.0, "PURCHASE", "DEBIT", None, "MB"),  # merchant with only LOW stores -> LOW ceiling
    ], schema_ap)

    schema_ml = StructType([
        StructField("merchant_key", StringType(), True),
        StructField("risk_rating", StringType(), True),
    ])
    df_ml = test_spark.createDataFrame([
        ("MA", "LOW"),
        ("MA", "HIGH"),
        ("MB", "LOW"),
        ("MB", "LOW"),
    ], schema_ml)

    empty_str = StructType([StructField("financial_event_key", StringType(), True)])
    empty_cp = test_spark.createDataFrame([], StructType([
        StructField("financial_event_key", StringType(), True),
        StructField("payment_amount", DoubleType(), True),
        StructField("card_transaction_type", StringType(), True),
        StructField("merchant_key", StringType(), True),
        StructField("is_fraud_source_flag", BooleanType(), True),
    ]))
    empty_atm = test_spark.createDataFrame([], StructType([
        StructField("financial_event_key", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("activity_type", StringType(), True),
    ]))
    empty_gp = test_spark.createDataFrame([], StructType([
        StructField("financial_event_key", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("merchant_key", StringType(), True),
    ]))
    empty_tc = test_spark.createDataFrame([], StructType([
        StructField("channel_key", StringType(), True),
        StructField("channel_name", StringType(), True),
        StructField("channel_type", StringType(), True),
    ]))
    df_m = test_spark.createDataFrame([
        ("MA", "Merchant A", "5999", "VN"),
        ("MB", "Merchant B", "5999", "VN"),
    ], StructType([
        StructField("merchant_key", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("mcc_code", StringType(), True),
        StructField("country", StringType(), True),
    ]))
    empty_fers = test_spark.createDataFrame([], StructType([
        StructField("financial_event_key", StringType(), True),
        StructField("scored_date", StringType(), True),
        StructField("model_score", DoubleType(), True),
        StructField("risk_band", StringType(), True),
    ]))
    empty_fefa = test_spark.createDataFrame([], StructType([
        StructField("financial_event_key", StringType(), True),
        StructField("fraud_alert_key", StringType(), True),
    ]))
    empty_fa = test_spark.createDataFrame([], StructType([
        StructField("fraud_alert_key", StringType(), True),
        StructField("alert_score", DoubleType(), True),
        StructField("alert_status", StringType(), True),
    ]))
    empty_cfa = test_spark.createDataFrame([], StructType([
        StructField("financial_event_key", StringType(), True),
        StructField("card_fraud_flag_key", StringType(), True),
    ]))

    table_map = {
        "financial_event": df_fe,
        "account_posting": df_ap,
        "card_payment": empty_cp,
        "atm_activity": empty_atm,
        "gateway_payment": empty_gp,
        "transaction_channel": empty_tc,
        "merchant": df_m,
        "financial_event_risk_score": empty_fers,
        "financial_event_fraud_alert": empty_fefa,
        "fraud_alert": empty_fa,
        "financial_event_card_fraud_flag": empty_cfa,
        "merchant_location": df_ml,
    }

    def mock_read_table(table_name):
        return table_map[table_name]

    with patch.object(_patched_reader(), "table", side_effect=mock_read_table):
        res_df = fraud_transaction_context.ai_fraud_transaction_context()
        rows = {r.financial_event_key: r for r in res_df.collect()}

    assert rows["FEA"].merchant_max_store_risk == "HIGH"
    assert rows["FEA"].merchant_store_count == 2
    assert rows["FEB"].merchant_max_store_risk == "LOW"
    assert rows["FEB"].merchant_store_count == 2


# Direct execution entrypoint
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])
"""
Target Schema : Silver Atomic Model (`silver`)
Source Schema : Validated Data (`silver_validated`)
Domain        : Financial Events (Transactions, ATM, Gateway, Associations)
"""

import sys
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("pipeline.catalog", "workspace")
SILVER_VALIDATED_SCHEMA = spark.conf.get("pipeline.silver_validated_schema", "silver_validated")
SILVER_ATOMIC_SCHEMA = spark.conf.get("pipeline.silver_schema", "silver")

def validated_src(table_name: str) -> str:
    return f"{CATALOG}.{SILVER_VALIDATED_SCHEMA}.{table_name}"

def atomic_tgt(table_name: str) -> str:
    return f"{CATALOG}.{SILVER_ATOMIC_SCHEMA}.{table_name}"

def hash_key(*cols):
    processed_cols = [
        F.coalesce(F.trim(c.cast("string")), F.lit("")) if isinstance(c, F.Column)
        else F.coalesce(F.trim(F.col(c).cast("string")), F.lit(""))
        for c in cols
    ]
    return F.sha2(F.concat_ws("||", *processed_cols), 256)

def get_currency_col(df):
    if "currency" in df.columns:
        return F.coalesce(F.col("currency"), F.lit("VND")).alias("currency")
    return F.lit("VND").alias("currency")

def get_pipeline_run_id(df) -> F.Column:
    if "pipeline_run_id" in df.columns:
        return F.col("pipeline_run_id").cast("string")
    return F.lit(spark.conf.get("pipeline.run_id", None)).cast("string")


# ==============================================================================
# 3.1 FINANCIAL EVENT (HEADER)
# ==============================================================================

@dp.table(name=atomic_tgt("financial_event"), comment="Canonical Silver Header Table for All Financial Events")
def silver_financial_event():
    # 1. Account Transactions
    df_acc = spark.read.table(validated_src("account_transaction"))
    acc_tx = df_acc.select(
        hash_key(F.lit("core_banking"), F.lit("ACCOUNT_POSTING"), "account_txn_id").alias("financial_event_key"),
        F.lit("ACCOUNT_POSTING").alias("event_type"),
        hash_key(F.lit("core_banking"), "account_id").alias("account_key"),
        F.lit(None).cast("string").alias("payment_card_key"),
        hash_key(F.lit("core_banking"), "customer_ref").alias("party_key"),
        F.lit(None).cast("string").alias("merchant_location_key"),
        get_currency_col(df_acc),
        F.col("txn_timestamp").cast("timestamp").alias("occurred_at"),
        F.lit("core_banking").alias("source_system"),
        F.col("account_txn_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("account_transaction"), F.col("account_txn_id")).alias("bronze_record_ref"),
        get_pipeline_run_id(df_acc).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
        F.lit("VALID").alias("data_quality_status")
    )

    # 2. Card Transactions
    df_card = spark.read.table(validated_src("card_transaction"))
    card_tx = df_card.select(
        hash_key(F.lit("card_system"), F.lit("CARD_PAYMENT"), "card_txn_id").alias("financial_event_key"),
        F.lit("CARD_PAYMENT").alias("event_type"),
        F.lit(None).cast("string").alias("account_key"),
        hash_key(F.lit("card_system"), "card_id").alias("payment_card_key"),
        F.lit(None).cast("string").alias("party_key"),
        hash_key(F.lit("merchant_system"), "merchant_id").alias("merchant_location_key"),
        get_currency_col(df_card),
        F.col("txn_timestamp").cast("timestamp").alias("occurred_at"),
        F.lit("card_system").alias("source_system"),
        F.col("card_txn_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("card_transaction"), F.col("card_txn_id")).alias("bronze_record_ref"),
        get_pipeline_run_id(df_card).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
        F.lit("VALID").alias("data_quality_status")
    )

    # 3. ATM Activity
    df_atm = spark.read.table(validated_src("log_atm"))
    atm_tx = df_atm.select(
        hash_key(F.lit("atm_system"), F.lit("ATM_ACTIVITY"), "log_id").alias("financial_event_key"),
        F.lit("ATM_ACTIVITY").alias("event_type"),
        hash_key(F.lit("core_banking"), "account_txn_id").alias("account_key"),
        hash_key(F.lit("card_system"), "card_id").alias("payment_card_key"),
        F.lit(None).cast("string").alias("party_key"),
        F.lit(None).cast("string").alias("merchant_location_key"),
        get_currency_col(df_atm),
        F.col("log_timestamp").cast("timestamp").alias("occurred_at"),
        F.lit("atm_system").alias("source_system"),
        F.col("log_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("log_atm"), F.col("log_id")).alias("bronze_record_ref"),
        get_pipeline_run_id(df_atm).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
        F.lit("VALID").alias("data_quality_status")
    )

    # 4. Gateway Payments
    df_gw = spark.read.table(validated_src("payment_gateway_log"))
    gw_log = df_gw.select(
        hash_key(F.lit("payment_gateway"), F.lit("GATEWAY_PAYMENT"), "gateway_txn_id").alias("financial_event_key"),
        F.lit("GATEWAY_PAYMENT").alias("event_type"),
        hash_key(F.lit("core_banking"), "account_txn_id").alias("account_key"),
        hash_key(F.lit("card_system"), "card_txn_id").alias("payment_card_key"),
        F.lit(None).cast("string").alias("party_key"),
        hash_key(F.lit("merchant_system"), "merchant_id").alias("merchant_location_key"),
        get_currency_col(df_gw),
        F.col("gateway_timestamp").cast("timestamp").alias("occurred_at"),
        F.lit("payment_gateway").alias("source_system"),
        F.col("gateway_txn_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("payment_gateway_log"), F.col("gateway_txn_id")).alias("bronze_record_ref"),
        get_pipeline_run_id(df_gw).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
        F.lit("VALID").alias("data_quality_status")
    )

    return acc_tx.unionByName(card_tx).unionByName(atm_tx).unionByName(gw_log)


# ==============================================================================
# 3.2 ACCOUNT POSTING
# ==============================================================================

@dp.table(name=atomic_tgt("account_posting"), comment="Extension Table for Account Postings")
def silver_account_posting():
    df = spark.read.table(validated_src("account_transaction"))
    return df.select(
        hash_key(F.lit("core_banking"), F.lit("ACCOUNT_POSTING"), "account_txn_id").alias("financial_event_key"),
        F.col("amount").cast("decimal(12,2)").alias("posting_amount"),
        F.col("direction").alias("posting_direction"),
        F.coalesce(F.col("transaction_type"), F.lit("TRANSFER")).alias("posting_type"),
        hash_key(F.lit("core_banking"), "channel").alias("channel_key"),
        hash_key(F.lit("merchant_system"), "merchant_id").alias("merchant_key"),
        F.lit("core_banking").alias("source_system"),
        F.col("account_txn_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("account_transaction"), F.col("account_txn_id")).alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at")
    )


# ==============================================================================
# 3.3 CARD PAYMENT
# ==============================================================================

@dp.table(name=atomic_tgt("card_payment"), comment="Extension Table for Card Payment Events")
def silver_card_payment():
    df = spark.read.table(validated_src("card_transaction"))
    return df.select(
        hash_key(F.lit("card_system"), F.lit("CARD_PAYMENT"), "card_txn_id").alias("financial_event_key"),
        F.col("amount").cast("decimal(12,2)").alias("payment_amount"),
        F.coalesce(F.col("card_transaction_type"), F.lit("PURCHASE")).alias("card_transaction_type"),
        hash_key(F.lit("merchant_system"), "merchant_id").alias("merchant_key"),
        F.coalesce(F.col("is_fraud"), F.lit(False)).cast("boolean").alias("is_fraud_source_flag"),
        F.lit("card_system").alias("source_system"),
        F.col("card_txn_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("card_transaction"), F.col("card_txn_id")).alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at")
    )


# ==============================================================================
# 3.4 ATM ACTIVITY
# ==============================================================================

@dp.table(name=atomic_tgt("atm_activity"), comment="Extension Table for ATM Activity")
def silver_atm_activity():
    df = spark.read.table(validated_src("log_atm"))
    return df.select(
        hash_key(F.lit("atm_system"), F.lit("ATM_ACTIVITY"), "log_id").alias("financial_event_key"),
        F.col("atm_id").cast("string").alias("atm_id"),
        F.coalesce(F.col("txn_type"), F.lit("WITHDRAWAL")).alias("activity_type"),
        F.col("amount").cast("decimal(12,2)").alias("amount"),
        F.col("response_code"),
        F.lit("atm_system").alias("source_system"),
        F.col("log_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("log_atm"), F.col("log_id")).alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at")
    )


# ==============================================================================
# 3.5 GATEWAY PAYMENT
# ==============================================================================

@dp.table(name=atomic_tgt("gateway_payment"), comment="Extension Table for Gateway Payments")
def silver_gateway_payment():
    df = spark.read.table(validated_src("payment_gateway_log"))
    return df.select(
        hash_key(F.lit("payment_gateway"), F.lit("GATEWAY_PAYMENT"), "gateway_txn_id").alias("financial_event_key"),
        F.col("payment_ref").alias("payment_reference"),
        F.col("payment_method"),
        hash_key(F.lit("merchant_system"), "merchant_id").alias("merchant_key"),
        F.col("amount").cast("decimal(12,2)").alias("amount"),
        F.col("provider_code"),
        F.lit("payment_gateway").alias("source_system"),
        F.col("gateway_txn_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("payment_gateway_log"), F.col("gateway_txn_id")).alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at")
    )


# ==============================================================================
# 3.6 FINANCIAL EVENT ASSOCIATION
# ==============================================================================
# NOTE: Commented out because source table workspace.silver_validated.financial_event_association does not exist
# Uncomment when the source table becomes available

# @dp.table(
#     name=atomic_tgt("financial_event_association"), 
#     comment="Association link between financial events (e.g. SETTLES, REVERSES, RELATES_TO)",
#     cluster_by=["from_financial_event_key", "to_financial_event_key", "association_type"]
# )
# def silver_financial_event_association():
#     df = spark.read.table(validated_src("financial_event_association"))
#     return df.select(
#         hash_key(F.lit("core_banking"), F.lit("assoc"), "association_id").alias("financial_event_association_key"),
#         hash_key("from_source_system", "from_event_type", "from_source_business_key").alias("from_financial_event_key"),
#         hash_key("to_source_system", "to_event_type", "to_source_business_key").alias("to_financial_event_key"),
#         F.col("association_type"),  # SETTLES / REPRESENTS / REVERSES / RELATES_TO
#         F.coalesce(F.col("source_system"), F.lit("core_banking")).alias("source_system"),
#         F.concat_ws(":", F.lit("financial_event_association"), F.col("association_id")).alias("bronze_record_ref"),
#         get_pipeline_run_id(df).alias("pipeline_run_id"),
#         F.current_timestamp().alias("ingested_at")
#     )


# ==============================================================================
# 3.7 FINANCIAL EVENT STATUS HISTORY
# ==============================================================================

@dp.table(name=atomic_tgt("financial_event_status_history"), comment="Canonical Silver Status History Table")
def silver_financial_event_status_history():
    df_acc = spark.read.table(validated_src("account_transaction_status_event"))
    acc = df_acc.select(
        hash_key(F.lit("core_banking"), F.lit("status_event"), "status_event_id").alias("financial_event_status_history_key"),
        hash_key(F.lit("core_banking"), F.lit("ACCOUNT_POSTING"), "account_txn_id").alias("financial_event_key"),
        F.col("status"),
        F.col("status_timestamp").cast("timestamp").alias("status_timestamp"),
        F.col("source_arrival_timestamp").cast("timestamp").alias("source_arrival_timestamp"),
        F.col("sequence_number").cast("bigint").alias("sequence_number"),
        F.col("status_event_id").cast("string").alias("source_status_event_id"),
        F.col("status_event_id").cast("string").alias("source_business_key"),
        F.lit("core_banking").alias("source_system"),
        F.concat_ws(":", F.lit("account_transaction_status_event"), F.col("status_event_id")).alias("bronze_record_ref"),
        get_pipeline_run_id(df_acc).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at")
    )

    df_card = spark.read.table(validated_src("card_transaction_status_event"))
    card = df_card.select(
        hash_key(F.lit("card_system"), F.lit("status_event"), "status_event_id").alias("financial_event_status_history_key"),
        hash_key(F.lit("card_system"), F.lit("CARD_PAYMENT"), "card_txn_id").alias("financial_event_key"),
        F.col("status"),
        F.col("status_timestamp").cast("timestamp").alias("status_timestamp"),
        F.col("source_arrival_timestamp").cast("timestamp").alias("source_arrival_timestamp"),
        F.col("sequence_number").cast("bigint").alias("sequence_number"),
        F.col("status_event_id").cast("string").alias("source_status_event_id"),
        F.col("status_event_id").cast("string").alias("source_business_key"),
        F.lit("card_system").alias("source_system"),
        F.concat_ws(":", F.lit("card_transaction_status_event"), F.col("status_event_id")).alias("bronze_record_ref"),
        get_pipeline_run_id(df_card).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at")
    )

    return acc.unionByName(card)
"""
Target Schema : Silver Atomic Model (`silver`)
Source Schema : Validated Data (`silver_validated`)
Domain        : Financial Events (Transactions, ATM, Gateway, Associations)
"""

import sys
import time
import uuid

from pyspark import pipelines as dp
from pyspark.sql import functions as F


def get_catalog() -> str:
    """Returns configured catalog or default to 'workspace' (bỏ qua catalog nếu đang chạy pytest)."""
    if "pytest" in sys.modules:
        return ""  # Khi chạy unit test, trả về chuỗi rỗng để tên bảng thành dạng "silver_validated.table"
    try:
        return spark.conf.get("pipeline.catalog", "workspace")
    except Exception:
        return "workspace"


def get_src_schema():
    """Returns configured source schema or default to 'silver_validated'."""
    try:
        return spark.conf.get("pipeline.silver_validated", "silver_validated")
    except Exception:
        return "silver_validated"


def get_silver_atomic_schema():
    """Returns configured silver schema or default to 'silver'."""
    try:
        return spark.conf.get("pipeline.silver_schema", "silver")
    except Exception:
        # In Spark Connect (serverless), some configs are restricted
        return "silver"


def validated_src(table_name: str) -> str:
    return f"{get_catalog()}.{get_src_schema()}.{table_name}"


def atomic_tgt(table_name: str) -> str:
    return f"{get_catalog()}.{get_silver_atomic_schema()}.{table_name}"


def hash_key(*cols):
    processed_cols = [
        (
            F.coalesce(F.trim(c.cast("string")), F.lit(""))
            if isinstance(c, F.Column)
            else F.coalesce(F.trim(F.col(c).cast("string")), F.lit(""))
        )
        for c in cols
    ]
    return F.sha2(F.concat_ws("||", *processed_cols), 256)


def get_currency_col(df):
    if "currency" in df.columns:
        return F.coalesce(F.col("currency"), F.lit("VND")).alias("currency")
    return F.lit("VND").alias("currency")


FALLBACK_MODULE_UUID = str(uuid.uuid4())


def get_pipeline_run_id(df) -> F.Column:
    """
    Lấy pipeline_run_id mới nhất từ bảng governance.pipeline_run bằng Scalar Subquery.
    """
    if "pipeline_run_id" in df.columns:
        return F.col("pipeline_run_id").cast("string")

    # Scalar Subquery: Query trực tiếp cột pipeline_run_id theo dòng có start_time mới nhất
    subquery_expr = f"""
        (SELECT CAST(pipeline_run_id AS STRING) 
         FROM {get_catalog()}.governance.pipeline_run 
         WHERE pipeline_name = 'full-source-to-validated-silver' 
         ORDER BY start_time DESC 
         LIMIT 1)
    """

    return (
        F.coalesce(F.expr(subquery_expr), F.lit(FALLBACK_MODULE_UUID))
        .cast("string")
        .alias("pipeline_run_id")
    )


# ==============================================================================
# 3.1 FINANCIAL EVENT (HEADER)
# ==============================================================================
SOURCE_SYSTEM_PREFIX_MAP = {"CB": "CORE_BANKING", "CRM": "CRM"}


def get_source_system(ref_col) -> F.Column:
    """Same logic as customer_transformation.py -- customer_ref/cust_no/party_id
    are polymorphic (CB-xxxx vs CRM-xxx), so the party_key literal prefix must
    be derived the same way everywhere, not hardcoded per file."""
    if isinstance(ref_col, str):
        ref_col = F.col(ref_col)
    prefix = F.upper(F.split(F.trim(ref_col), "-").getItem(0))
    resolved = F.lit(None).cast("string")
    for code, label in SOURCE_SYSTEM_PREFIX_MAP.items():
        resolved = F.when(prefix == F.lit(code), F.lit(label)).otherwise(resolved)
    return F.coalesce(resolved, F.lit("UNKNOWN"))


@dp.table(
    name=atomic_tgt("financial_event"),
    comment="Canonical Silver Header Table for All Financial Events",
)
def silver_financial_event():
    # Alias cho bảng tra cứu account_party
    account_party = (
        spark.read.table(atomic_tgt("party_account_role"))
        .select("account_key", "party_key")
        .dropDuplicates(["account_key"])
        .alias("ap")
    )

    card_account = spark.read.table(validated_src("card")).select(
        "card_id",
        hash_key(F.lit("core_banking"), "account_id").alias("_card_account_key"),
    )

    # 1. Account Transactions
    df_acc = spark.read.table(validated_src("account_transaction"))
    acc_tx = df_acc.select(
        hash_key(
            F.lit("core_banking"), F.lit("ACCOUNT_POSTING"), "account_txn_id"
        ).alias("financial_event_key"),
        F.lit("ACCOUNT_POSTING").alias("event_type"),
        hash_key(F.lit("core_banking"), "account_id").alias("account_key"),
        F.lit(None).cast("string").alias("payment_card_key"),
        hash_key(get_source_system(F.col("customer_ref")), "customer_ref").alias(
            "party_key"
        ),
        F.lit(None).cast("string").alias("merchant_location_key"),
        get_currency_col(df_acc),
        F.col("txn_timestamp").cast("timestamp").alias("occurred_at"),
        F.lit("core_banking").alias("source_system"),
        F.col("account_txn_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("account_transaction"), F.col("account_txn_id")).alias(
            "bronze_record_ref"
        ),
        get_pipeline_run_id(df_acc).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
        F.lit("VALID").alias("data_quality_status"),
    )

    # 2. Card Transactions (Fix: Alias DataFrame)
    df_card = (
        spark.read.table(validated_src("card_transaction"))
        .join(card_account, "card_id", "left")
        .alias("c_tx")
    )

    card_tx = df_card.join(
        account_party,
        F.col("c_tx._card_account_key") == F.col("ap.account_key"),
        "left",
    ).select(
        hash_key(F.lit("card_system"), F.lit("CARD_PAYMENT"), "card_txn_id").alias(
            "financial_event_key"
        ),
        F.lit("CARD_PAYMENT").alias("event_type"),
        F.col("c_tx._card_account_key").alias("account_key"),
        hash_key(F.lit("card_system"), "card_id").alias("payment_card_key"),
        F.col("ap.party_key").alias("party_key"),  # Chỉ định chính xác lấy từ alias ap
        F.lit(None).cast("string").alias("merchant_location_key"),
        get_currency_col(df_card),
        F.col("c_tx.txn_timestamp").cast("timestamp").alias("occurred_at"),
        F.lit("card_system").alias("source_system"),
        F.col("c_tx.card_txn_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("card_transaction"), F.col("c_tx.card_txn_id")).alias(
            "bronze_record_ref"
        ),
        get_pipeline_run_id(df_card).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
        F.lit("VALID").alias("data_quality_status"),
    )

    # 3. ATM Activity (Fix: Alias DataFrame)
    df_atm = (
        spark.read.table(validated_src("log_atm"))
        .join(card_account, "card_id", "left")
        .alias("atm")
    )

    atm_tx = df_atm.join(
        account_party,
        F.col("atm._card_account_key") == F.col("ap.account_key"),
        "left",
    ).select(
        hash_key(F.lit("atm_system"), F.lit("ATM_ACTIVITY"), "log_id").alias(
            "financial_event_key"
        ),
        F.lit("ATM_ACTIVITY").alias("event_type"),
        F.col("atm._card_account_key").alias("account_key"),
        hash_key(F.lit("card_system"), "card_id").alias("payment_card_key"),
        F.col("ap.party_key").alias("party_key"),  # Chỉ định chính xác lấy từ alias ap
        F.lit(None).cast("string").alias("merchant_location_key"),
        get_currency_col(df_atm),
        F.col("atm.log_timestamp").cast("timestamp").alias("occurred_at"),
        F.lit("atm_system").alias("source_system"),
        F.col("atm.log_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("log_atm"), F.col("atm.log_id")).alias(
            "bronze_record_ref"
        ),
        get_pipeline_run_id(df_atm).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
        F.lit("VALID").alias("data_quality_status"),
    )

    # 4. Gateway Payments (Fix: Alias DataFrame)
    df_gw = spark.read.table(validated_src("payment_gateway_log"))
    df_acc_txn_lookup = spark.read.table(validated_src("account_transaction")).select(
        "account_txn_id",
        hash_key(F.lit("core_banking"), "account_id").alias("_via_acct_account_key"),
    )

    df_card_txn_lookup = (
        spark.read.table(validated_src("card_transaction"))
        .select("card_txn_id", "card_id")
        .join(card_account, "card_id", "left")
        .select(
            "card_txn_id", F.col("_card_account_key").alias("_via_card_account_key")
        )
    )

    gw_resolved = (
        df_gw.join(df_acc_txn_lookup, "account_txn_id", "left")
        .join(df_card_txn_lookup, "card_txn_id", "left")
        .withColumn(
            "_resolved_account_key",
            F.coalesce("_via_acct_account_key", "_via_card_account_key"),
        )
        .alias("gw")
    )

    gw_log = gw_resolved.join(
        account_party,
        F.col("gw._resolved_account_key") == F.col("ap.account_key"),
        "left",
    ).select(
        hash_key(
            F.lit("payment_gateway"), F.lit("GATEWAY_PAYMENT"), "gateway_txn_id"
        ).alias("financial_event_key"),
        F.lit("GATEWAY_PAYMENT").alias("event_type"),
        F.col("gw._resolved_account_key").alias("account_key"),
        F.when(
            F.col("gw.card_txn_id").isNotNull(),
            hash_key(F.lit("card_system"), "card_txn_id"),
        ).alias("payment_card_key"),
        F.col("ap.party_key").alias("party_key"),  # Chỉ định chính xác lấy từ alias ap
        F.lit(None).cast("string").alias("merchant_location_key"),
        get_currency_col(df_gw),
        F.col("gw.gateway_timestamp").cast("timestamp").alias("occurred_at"),
        F.lit("payment_gateway").alias("source_system"),
        F.col("gw.gateway_txn_id").cast("string").alias("source_business_key"),
        F.concat_ws(
            ":", F.lit("payment_gateway_log"), F.col("gw.gateway_txn_id")
        ).alias("bronze_record_ref"),
        get_pipeline_run_id(df_gw).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
        F.lit("VALID").alias("data_quality_status"),
    )

    return acc_tx.unionByName(card_tx).unionByName(atm_tx).unionByName(gw_log)


# ==============================================================================
# 3.2 ACCOUNT POSTING
# ==============================================================================


@dp.table(
    name=atomic_tgt("account_posting"), comment="Extension Table for Account Postings"
)
def silver_account_posting():
    df = spark.read.table(validated_src("account_transaction"))
    return df.select(
        hash_key(
            F.lit("core_banking"), F.lit("ACCOUNT_POSTING"), "account_txn_id"
        ).alias("financial_event_key"),
        F.col("amount").cast("decimal(12,2)").alias("posting_amount"),
        F.col("direction").alias("posting_direction"),
        F.coalesce(F.col("transaction_type"), F.lit("TRANSFER")).alias("posting_type"),
        hash_key(F.lit("core_banking"), "channel").alias("channel_key"),
        hash_key(F.lit("merchant_system"), "merchant_id").alias("merchant_key"),
        F.lit("core_banking").alias("source_system"),
        F.col("account_txn_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("account_transaction"), F.col("account_txn_id")).alias(
            "bronze_record_ref"
        ),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


# ==============================================================================
# 3.3 CARD PAYMENT
# ==============================================================================


@dp.table(
    name=atomic_tgt("card_payment"), comment="Extension Table for Card Payment Events"
)
def silver_card_payment():
    df = spark.read.table(validated_src("card_transaction"))
    return df.select(
        hash_key(F.lit("card_system"), F.lit("CARD_PAYMENT"), "card_txn_id").alias(
            "financial_event_key"
        ),
        F.col("amount").cast("decimal(12,2)").alias("payment_amount"),
        F.coalesce(F.col("card_transaction_type"), F.lit("PURCHASE")).alias(
            "card_transaction_type"
        ),
        hash_key(F.lit("merchant_system"), "merchant_id").alias("merchant_key"),
        F.coalesce(F.col("is_fraud"), F.lit(False))
        .cast("boolean")
        .alias("is_fraud_source_flag"),
        F.lit("card_system").alias("source_system"),
        F.col("card_txn_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("card_transaction"), F.col("card_txn_id")).alias(
            "bronze_record_ref"
        ),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


# ==============================================================================
# 3.4 ATM ACTIVITY
# ==============================================================================


@dp.table(name=atomic_tgt("atm_activity"), comment="Extension Table for ATM Activity")
def silver_atm_activity():
    df = spark.read.table(validated_src("log_atm"))
    return df.select(
        hash_key(F.lit("atm_system"), F.lit("ATM_ACTIVITY"), "log_id").alias(
            "financial_event_key"
        ),
        F.col("atm_id").cast("string").alias("atm_id"),
        F.coalesce(F.col("txn_type"), F.lit("WITHDRAWAL")).alias("activity_type"),
        F.col("amount").cast("decimal(12,2)").alias("amount"),
        F.col("response_code"),
        F.lit("atm_system").alias("source_system"),
        F.col("log_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("log_atm"), F.col("log_id")).alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


# ==============================================================================
# 3.5 GATEWAY PAYMENT
# ==============================================================================


@dp.table(
    name=atomic_tgt("gateway_payment"), comment="Extension Table for Gateway Payments"
)
def silver_gateway_payment():
    df = spark.read.table(validated_src("payment_gateway_log"))
    return df.select(
        hash_key(
            F.lit("payment_gateway"), F.lit("GATEWAY_PAYMENT"), "gateway_txn_id"
        ).alias("financial_event_key"),
        F.col("payment_ref").alias("payment_reference"),
        F.col("payment_method"),
        hash_key(F.lit("merchant_system"), "merchant_id").alias("merchant_key"),
        F.col("amount").cast("decimal(12,2)").alias("amount"),
        F.col("provider_code"),
        F.lit("payment_gateway").alias("source_system"),
        F.col("gateway_txn_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("payment_gateway_log"), F.col("gateway_txn_id")).alias(
            "bronze_record_ref"
        ),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
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


@dp.table(
    name=atomic_tgt("financial_event_status_history"),
    comment="Canonical Silver Status History Table",
)
def silver_financial_event_status_history():
    df_acc = spark.read.table(validated_src("account_transaction_status_event"))
    acc = df_acc.select(
        hash_key(F.lit("core_banking"), F.lit("status_event"), "status_event_id").alias(
            "financial_event_status_history_key"
        ),
        hash_key(
            F.lit("core_banking"), F.lit("ACCOUNT_POSTING"), "account_txn_id"
        ).alias("financial_event_key"),
        F.col("status"),
        F.col("status_timestamp").cast("timestamp").alias("status_timestamp"),
        F.col("source_arrival_timestamp")
        .cast("timestamp")
        .alias("source_arrival_timestamp"),
        F.col("sequence_number").cast("bigint").alias("sequence_number"),
        F.col("status_event_id").cast("string").alias("source_status_event_id"),
        F.col("status_event_id").cast("string").alias("source_business_key"),
        F.lit("core_banking").alias("source_system"),
        F.concat_ws(
            ":", F.lit("account_transaction_status_event"), F.col("status_event_id")
        ).alias("bronze_record_ref"),
        get_pipeline_run_id(df_acc).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )

    df_card = spark.read.table(validated_src("card_transaction_status_event"))
    card = df_card.select(
        hash_key(F.lit("card_system"), F.lit("status_event"), "status_event_id").alias(
            "financial_event_status_history_key"
        ),
        hash_key(F.lit("card_system"), F.lit("CARD_PAYMENT"), "card_txn_id").alias(
            "financial_event_key"
        ),
        F.col("status"),
        F.col("status_timestamp").cast("timestamp").alias("status_timestamp"),
        F.col("source_arrival_timestamp")
        .cast("timestamp")
        .alias("source_arrival_timestamp"),
        F.col("sequence_number").cast("bigint").alias("sequence_number"),
        F.col("status_event_id").cast("string").alias("source_status_event_id"),
        F.col("status_event_id").cast("string").alias("source_business_key"),
        F.lit("card_system").alias("source_system"),
        F.concat_ws(
            ":", F.lit("card_transaction_status_event"), F.col("status_event_id")
        ).alias("bronze_record_ref"),
        get_pipeline_run_id(df_card).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )

    return acc.unionByName(card)

"""
Target Schema : Silver Atomic Model (`silver`)
Source Schema : Bronze / Validated Datasets (`silver_validated`)
Domain        : Card System
"""

import sys
import time
import uuid

from pyspark import pipelines as dp
from pyspark.sql import functions as F


def get_catalog() -> str:
    """Returns configured catalog or default to 'workspace' (bỏ qua catalog nếu đang chạy pytest)."""
    if "pytest" in sys.modules:
        return ""
    try:
        return spark.conf.get("pipeline.catalog", "workspace")
    except Exception:
        return "workspace"


def get_bronze_schema():
    try:
        return spark.conf.get("pipeline.bronze_schema", "bronze")
    except Exception:
        return "bronze"


def get_src_schema():
    try:
        return spark.conf.get("pipeline.silver_validated", "silver_validated")
    except Exception:
        return "silver_validated"


def get_silver_atomic_schema():
    try:
        return spark.conf.get("pipeline.silver_schema", "silver")
    except Exception:
        return "silver"


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------
def clean_src(table_name: str) -> str:
    if "pytest" in sys.modules:
        return f"{get_bronze_schema()}.{table_name}"
    cat = get_catalog()
    return (
        f"{cat}.{get_bronze_schema()}.{table_name}"
        if cat
        else f"{get_bronze_schema()}.{table_name}"
    )


def clean_card_src(table_name: str) -> str:
    if "pytest" in sys.modules:
        return f"{get_src_schema()}.{table_name}"
    cat = get_catalog()
    return (
        f"{cat}.{get_src_schema()}.{table_name}"
        if cat
        else f"{get_src_schema()}.{table_name}"
    )


def atomic_tgt(table_name: str) -> str:
    if "pytest" in sys.modules:
        return f"{get_silver_atomic_schema()}.{table_name}"
    cat = get_catalog()
    return (
        f"{cat}.{get_silver_atomic_schema()}.{table_name}"
        if cat
        else f"{get_silver_atomic_schema()}.{table_name}"
    )


def bronze_ref(bronze_table: str, business_key_col) -> F.Column:
    if isinstance(business_key_col, str):
        business_key_col = F.col(business_key_col)
    return F.concat_ws(":", F.lit(bronze_table), business_key_col.cast("string"))


def hash_key(*cols) -> F.Column:
    processed = [
        (
            F.coalesce(F.trim(c.cast("string")), F.lit(""))
            if isinstance(c, F.Column)
            else F.coalesce(F.trim(F.col(c).cast("string")), F.lit(""))
        )
        for c in cols
    ]
    return F.sha2(F.concat_ws("||", *processed), 256)


FALLBACK_MODULE_UUID = str(uuid.uuid4())


def get_pipeline_run_id(df) -> F.Column:
    """Lấy pipeline_run_id mới nhất từ bảng governance.pipeline_run bằng Scalar Subquery."""
    if "pipeline_run_id" in df.columns:
        return F.col("pipeline_run_id").cast("string")

    cat = get_catalog()
    table_ref = f"{cat}.governance.pipeline_run" if cat else "governance.pipeline_run"

    subquery_expr = f"""
        (SELECT CAST(pipeline_run_id AS STRING) 
         FROM {table_ref} 
         WHERE pipeline_name = 'full-pipeline' 
         ORDER BY start_time DESC 
         LIMIT 1)
    """

    return (
        F.coalesce(F.expr(subquery_expr), F.lit(FALLBACK_MODULE_UUID))
        .cast("string")
        .alias("pipeline_run_id")
    )


# ---------------------------------------------------------------------------
# Table Builders
# ---------------------------------------------------------------------------
def _build_account(df):
    return df.select(
        hash_key(F.lit("core_banking"), "account_id").alias("account_key"),
        F.col("account_id").cast("bigint").alias("source_account_id"),
        F.col("product_type"),
        F.col("status").alias("account_status"),
        F.col("open_date").cast("date").alias("open_date"),
        F.col("branch_code"),
        F.lit("core_banking").alias("source_system"),
        F.col("account_id").cast("string").alias("source_business_key"),
        bronze_ref("account", "account_id").alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


def _build_party_account_role(df):
    core = spark.read.table(clean_card_src("core_banking_customer"))
    cif_to_cust = core.select("cif_number", "cust_no")

    df2 = df.join(cif_to_cust, on="cif_number", how="left")

    return df2.select(
        hash_key(F.lit("core_banking"), F.lit("party_account_role"), "link_id").alias(
            "party_account_role_key"
        ),
        hash_key(F.lit("CORE_BANKING"), "cust_no").alias("party_key"),
        hash_key(F.lit("core_banking"), "account_id").alias("account_key"),
        F.col("relationship_type"),
        F.col("linked_date").cast("date").alias("valid_from"),
        F.lit(None).cast("date").alias("valid_to"),
        F.lit("core_banking").alias("source_system"),
        F.col("link_id").cast("string").alias("source_business_key"),
        bronze_ref("customer_account", "link_id").alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


def _build_payment_card(df):
    return df.select(
        hash_key(F.lit("card_system"), "card_id").alias("payment_card_key"),
        F.col("card_id").cast("string").alias("source_card_id"),
        hash_key(F.lit("core_banking"), "account_id").alias("account_key"),
        
        # Rule 1.15 Card Number: Lưu dữ liệu sạch nguyên bản
        F.col("card_number").cast("string").alias("card_number"),
        
        F.col("card_type"),
        F.col("issue_date").cast("date").alias("issue_date"),
        F.col("expiry_date").cast("date").alias("expiry_date"),
        F.col("status").alias("card_status"),
        F.lit("card_system").alias("source_system"),
        F.col("card_id").cast("string").alias("source_business_key"),
        bronze_ref("card", "card_id").alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


def _build_payment_card_limit_history(df):
    return df.select(
        hash_key(F.lit("card_system"), F.lit("limit_history"), "history_id").alias(
            "card_limit_history_key"
        ),
        hash_key(F.lit("card_system"), "card_id").alias("payment_card_key"),
        F.col("limit_amount").cast("decimal(12,2)").alias("limit_amount"),
        F.col("effective_date").cast("date").alias("effective_date"),
        F.lit("card_system").alias("source_system"),
        F.col("history_id").cast("string").alias("source_business_key"),
        bronze_ref("card_limit_history", "history_id").alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


def _build_account_balance_snapshot(df):
    return df.select(
        hash_key(F.lit("core_banking"), F.lit("balance_snapshot"), "balance_id").alias(
            "account_balance_snapshot_key"
        ),
        hash_key(F.lit("core_banking"), "account_id").alias("account_key"),
        F.col("balance_date").cast("date").alias("balance_date"),
        F.col("opening_balance").cast("decimal(14,2)").alias("opening_balance"),
        F.col("closing_balance").cast("decimal(14,2)").alias("closing_balance"),
        F.col("available_balance").cast("decimal(14,2)").alias("available_balance"),
        F.lit("core_banking").alias("source_system"),
        F.col("balance_id").cast("string").alias("source_business_key"),
        bronze_ref("balance_snapshot", "balance_id").alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


def _build_transaction_channel(df):
    return df.select(
        hash_key(F.lit("core_banking"), "channel_id").alias("channel_key"),
        F.col("channel_id").cast("string").alias("source_channel_id"),
        F.col("channel_name"),
        F.col("channel_type"),
        F.lit("core_banking").alias("source_system"),
        F.col("channel_id").cast("string").alias("source_business_key"),
        bronze_ref("transaction_channel", "channel_id").alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


def _build_merchant(df):
    return df.select(
        hash_key(F.lit("merchant_system"), "merchant_id").alias("merchant_key"),
        F.col("merchant_id").cast("string").alias("source_merchant_id"),
        F.col("merchant_name"),
        F.col("mcc_code"),
        F.col("country"),
        F.lit("merchant_system").alias("source_system"),
        F.col("merchant_id").cast("string").alias("source_business_key"),
        bronze_ref("merchant", "merchant_id").alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


def _build_merchant_location(df):
    return df.select(
        hash_key(F.lit("merchant_system"), F.lit("store"), "store_id").alias(
            "merchant_location_key"
        ),
        hash_key(F.lit("merchant_system"), "merchant_id").alias("merchant_key"),
        F.col("store_id").cast("string").alias("source_store_id"),
        
        # Rule 1.30 Store Name & Rule 1.11 Address: Lưu dữ liệu sạch nguyên bản
        F.col("store_name"),
        F.col("store_description"),
        F.col("store_type"),
        F.col("store_address"),
        
        F.col("risk_rating"),
        F.col("registered_date").cast("date").alias("registered_date"),
        F.lit("merchant_system").alias("source_system"),
        F.col("store_id").cast("string").alias("source_business_key"),
        bronze_ref("merchant_store", "store_id").alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


def get_table_specs():
    return [
        {
            "target": "account",
            "source": clean_src("account"),
            "comment": "Canonical Silver Account Table",
            "builder": _build_account,
            "unique_keys": ["source_system", "source_account_id"],
        },
        {
            "target": "party_account_role",
            "source": clean_src("customer_account"),
            "comment": "Bridge Table linking Party to Account Roles",
            "builder": _build_party_account_role,
            "unique_keys": [
                "party_key",
                "account_key",
                "relationship_type",
                "valid_from",
            ],
        },
        {
            "target": "payment_card",
            "source": clean_card_src("card"),
            "comment": "Canonical Silver Payment Card Table",
            "builder": _build_payment_card,
            "unique_keys": ["source_system", "source_card_id"],
        },
        {
            "target": "payment_card_limit_history",
            "source": clean_card_src("card_limit_history"),
            "comment": "Canonical Silver Payment Card Limit History Table",
            "builder": _build_payment_card_limit_history,
            "unique_keys": None,
        },
        {
            "target": "account_balance_snapshot",
            "source": clean_src("balance_snapshot"),
            "comment": "Canonical Silver Account Balance Snapshot Table",
            "builder": _build_account_balance_snapshot,
            "unique_keys": ["account_key", "balance_date", "source_system"],
        },
        {
            "target": "transaction_channel",
            "source": clean_src("transaction_channel"),
            "comment": "Canonical Silver Transaction Channel Table",
            "builder": _build_transaction_channel,
            "unique_keys": ["source_system", "source_channel_id"],
        },
        {
            "target": "merchant",
            "source": clean_src("merchant"),
            "comment": "Canonical Silver Merchant Table",
            "builder": _build_merchant,
            "unique_keys": ["source_system", "source_merchant_id"],
        },
        {
            "target": "merchant_location",
            "source": clean_src("merchant_store"),
            "comment": "Canonical Silver Merchant Location Table",
            "builder": _build_merchant_location,
            "unique_keys": ["source_system", "source_store_id"],
        },
    ]


def _register_table(spec: dict) -> None:
    table_kwargs = {"name": atomic_tgt(spec["target"]), "comment": spec["comment"]}
    if spec["unique_keys"]:
        table_kwargs["cluster_by"] = spec["unique_keys"]
        table_kwargs["table_properties"] = {
            "silver.unique_index": ",".join(spec["unique_keys"])
        }

    def _transform(spec=spec):
        if spec.get("source") is None:
            return spec["builder"]()
        df = spark.read.table(spec["source"])
        return spec["builder"](df)

    _transform.__name__ = f"silver_{spec['target']}"
    dp.table(**table_kwargs)(_transform)


def main() -> None:
    for spec in get_table_specs():
        _register_table(spec)


if __name__ == "__main__":
    main()
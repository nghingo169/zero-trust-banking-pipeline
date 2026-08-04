"""
Target Schema : Silver Atomic Model (`silver`)
Source Schema : Validated Data (`silver_validated`)
Domain        : Financial Crime, Fraud Alerts, AML, Sanctions, Case Investigation
"""

import sys
import time
import uuid

from nab_tdm_masking import mask_phone as nab_mask_phone
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
        return "silver"


AES_KEY = "NAB_SECRET_AES256_KEY_32BYTES!!!"


def validated_src(table_name: str) -> str:
    if "pytest" in sys.modules:
        return f"{get_src_schema()}.{table_name}"
    cat = get_catalog()
    schema = get_src_schema()
    return f"{cat}.{schema}.{table_name}" if cat else f"{schema}.{table_name}"


def atomic_tgt(table_name: str) -> str:
    if "pytest" in sys.modules:
        return f"{get_silver_atomic_schema()}.{table_name}"
    cat = get_catalog()
    schema = get_silver_atomic_schema()
    return f"{cat}.{schema}.{table_name}" if cat else f"{schema}.{table_name}"


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


SOURCE_SYSTEM_PREFIX_MAP = {"CB": "CORE_BANKING", "CRM": "CRM"}


def get_source_system(ref_col) -> F.Column:
    """customer_ref is polymorphic (CB-xxxx / CRM-xxx); derive the party_key
    source-system literal the same way customer_transformation.py does,
    otherwise the hash never matches silver.party.party_key."""
    if isinstance(ref_col, str):
        ref_col = F.col(ref_col)
    prefix = F.upper(F.split(F.trim(ref_col), "-").getItem(0))
    resolved = F.lit(None).cast("string")
    for code, label in SOURCE_SYSTEM_PREFIX_MAP.items():
        resolved = F.when(prefix == F.lit(code), F.lit(label)).otherwise(resolved)
    return F.coalesce(resolved, F.lit("UNKNOWN"))


@dp.table(name=atomic_tgt("financial_event_risk_score"))
def silver_financial_event_risk_score():
    df = spark.read.table(validated_src("account_transaction_risk_score"))
    return df.select(
        hash_key(F.lit("fincrime"), F.lit("score_id"), "score_id").alias(
            "financial_event_risk_score_key"
        ),
        hash_key(
            F.lit("core_banking"), F.lit("ACCOUNT_POSTING"), "account_txn_id"
        ).alias("financial_event_key"),
        F.col("model_score").cast("decimal(6,4)").alias("model_score"),
        F.col("risk_band"),
        F.col("scored_date").cast("date").alias("scored_date"),
        F.lit("fincrime").alias("source_system"),
        F.col("score_id").cast("string").alias("source_business_key"),
        F.concat_ws(
            ":", F.lit("account_transaction_risk_score"), F.col("score_id")
        ).alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


@dp.table(name=atomic_tgt("fraud_alert"))
def silver_fraud_alert():
    df = spark.read.table(validated_src("fraud_alert"))
    return df.select(
        hash_key(F.lit("fincrime"), F.lit("fraud_alert"), "alert_id").alias(
            "fraud_alert_key"
        ),
        F.col("alert_type"),
        F.col("alert_score").cast("decimal(5,2)").alias("alert_score"),
        F.col("alert_status"),
        F.col("created_date").cast("timestamp").alias("created_at"),
        F.lit("fincrime").alias("source_system"),
        F.col("alert_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("fraud_alert"), F.col("alert_id")).alias(
            "bronze_record_ref"
        ),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


@dp.table(name=atomic_tgt("financial_event_fraud_alert"))
def silver_financial_event_fraud_alert():
    df = spark.read.table(validated_src("fraud_alert")).filter(
        "account_txn_id IS NOT NULL AND account_txn_id <> -1"
    )
    return df.select(
        hash_key(
            F.lit("fincrime"),
            F.lit("financial_event_fraud_alert"),
            "alert_id",
            "account_txn_id",
        ).alias("financial_event_fraud_alert_key"),
        hash_key(
            F.lit("core_banking"), F.lit("ACCOUNT_POSTING"), "account_txn_id"
        ).alias("financial_event_key"),
        hash_key(F.lit("fincrime"), F.lit("fraud_alert"), "alert_id").alias(
            "fraud_alert_key"
        ),
        F.lit("fincrime").alias("source_system"),
        F.concat_ws(":", F.col("alert_id"), F.col("account_txn_id"))
        .cast("string")
        .alias("source_business_key"),
        F.concat_ws(":", F.lit("fraud_alert"), F.col("alert_id")).alias(
            "bronze_record_ref"
        ),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


@dp.table(name=atomic_tgt("transaction_monitoring_alert"))
def silver_transaction_monitoring_alert():
    df = spark.read.table(validated_src("transaction_monitoring_alert"))
    return df.select(
        hash_key(
            F.lit("fincrime"), F.lit("transaction_monitoring_alert"), "alert_id"
        ).alias("monitoring_alert_key"),
        hash_key(get_source_system(F.col("customer_ref")), "customer_ref").alias(
            "party_key"
        ),
        F.col("alert_type"),
        F.col("alert_score").cast("decimal(5,2)").alias("alert_score"),
        F.col("alert_status"),
        F.col("alert_timestamp").cast("timestamp").alias("alert_timestamp"),
        F.lit("fincrime").alias("source_system"),
        F.col("alert_id").cast("string").alias("source_business_key"),
        F.concat_ws(
            ":", F.lit("transaction_monitoring_alert"), F.col("alert_id")
        ).alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


@dp.table(name=atomic_tgt("monitoring_alert_financial_event"))
def silver_monitoring_alert_financial_event():
    df_acc_src = spark.read.table(
        validated_src("transaction_monitoring_alert_account_transaction")
    ).filter("account_txn_id <> -1")
    df_acc = df_acc_src.select(
        F.col("alert_account_txn_link_id").cast("string").alias("link_id"),
        F.col("alert_id").cast("string").alias("alert_id"),
        F.lit("core_banking").alias("src_sys"),
        F.lit("ACCOUNT_POSTING").alias("evt_type"),
        F.col("account_txn_id").cast("string").alias("txn_id"),
        F.col("is_primary").cast("boolean").alias("is_primary"),
        get_pipeline_run_id(df_acc_src).alias("pipeline_run_id"),
    )

    df_card_src = spark.read.table(
        validated_src("transaction_monitoring_alert_card_transaction")
    ).filter("card_txn_id <> -1")
    df_card = df_card_src.select(
        F.col("alert_card_txn_link_id").cast("string").alias("link_id"),
        F.col("alert_id").cast("string").alias("alert_id"),
        F.lit("card_system").alias("src_sys"),
        F.lit("CARD_PAYMENT").alias("evt_type"),
        F.col("card_txn_id").cast("string").alias("txn_id"),
        F.col("is_primary").cast("boolean").alias("is_primary"),
        get_pipeline_run_id(df_card_src).alias("pipeline_run_id"),
    )

    df_union = df_acc.unionByName(df_card)

    return df_union.select(
        hash_key(
            F.lit("fincrime"), F.lit("monitoring_alert_financial_event"), "link_id"
        ).alias("monitoring_alert_financial_event_key"),
        hash_key(
            F.lit("fincrime"), F.lit("transaction_monitoring_alert"), "alert_id"
        ).alias("monitoring_alert_key"),
        hash_key("src_sys", "evt_type", "txn_id").alias("financial_event_key"),
        F.col("is_primary"),
        F.lit("fincrime").alias("source_system"),
        F.col("link_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("monitoring_alert_link"), F.col("link_id")).alias(
            "bronze_record_ref"
        ),
        F.col("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


@dp.table(name=atomic_tgt("investigation_case"))
def silver_investigation_case():
    df = spark.read.table(validated_src("investigation_case"))
    return df.select(
        hash_key(F.lit("fincrime"), F.lit("investigation_case"), "case_id").alias(
            "investigation_case_key"
        ),
        F.col("investigation_type"),
        F.col("case_origin"),
        F.col("case_status"),
        F.col("priority"),
        F.col("opened_timestamp").cast("timestamp").alias("opened_at"),
        F.col("closed_timestamp").cast("timestamp").alias("closed_at"),
        F.col("assigned_analyst_id"),
        F.lit("fincrime").alias("source_system"),
        F.col("case_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("investigation_case"), F.col("case_id")).alias(
            "bronze_record_ref"
        ),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


@dp.table(name=atomic_tgt("investigation_case_financial_event"))
def silver_investigation_case_financial_event():
    df_acc_src = spark.read.table(
        validated_src("investigation_case_account_transaction")
    ).filter("account_txn_id <> -1")
    df_acc = df_acc_src.select(
        F.col("case_account_txn_link_id").cast("string").alias("link_id"),
        F.col("case_id").cast("string").alias("case_id"),
        F.lit("core_banking").alias("src_sys"),
        F.lit("ACCOUNT_POSTING").alias("evt_type"),
        F.col("account_txn_id").cast("string").alias("txn_id"),
        F.col("link_reason"),
        F.col("linked_timestamp").cast("timestamp").alias("linked_at"),
        get_pipeline_run_id(df_acc_src).alias("pipeline_run_id"),
    )

    df_card_src = spark.read.table(
        validated_src("investigation_case_card_transaction")
    ).filter("card_txn_id <> -1")
    df_card = df_card_src.select(
        F.col("case_card_txn_link_id").cast("string").alias("link_id"),
        F.col("case_id").cast("string").alias("case_id"),
        F.lit("card_system").alias("src_sys"),
        F.lit("CARD_PAYMENT").alias("evt_type"),
        F.col("card_txn_id").cast("string").alias("txn_id"),
        F.col("link_reason"),
        F.col("linked_timestamp").cast("timestamp").alias("linked_at"),
        get_pipeline_run_id(df_card_src).alias("pipeline_run_id"),
    )

    df_union = df_acc.unionByName(df_card)

    return df_union.select(
        hash_key(F.lit("fincrime"), F.lit("case_financial_event"), "link_id").alias(
            "investigation_case_financial_event_key"
        ),
        hash_key(F.lit("fincrime"), F.lit("investigation_case"), "case_id").alias(
            "investigation_case_key"
        ),
        hash_key("src_sys", "evt_type", "txn_id").alias("financial_event_key"),
        F.col("link_reason"),
        F.col("linked_at"),
        F.lit("fincrime").alias("source_system"),
        F.col("link_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("case_txn_link"), F.col("link_id")).alias(
            "bronze_record_ref"
        ),
        F.col("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


@dp.table(name=atomic_tgt("investigation_case_fraud_alert"))
def silver_investigation_case_fraud_alert():
    df = spark.read.table(validated_src("investigation_case_fraud_alert"))
    return df.select(
        hash_key(
            F.lit("fincrime"), F.lit("case_fraud_alert"), "case_alert_link_id"
        ).alias("investigation_case_fraud_alert_key"),
        hash_key(F.lit("fincrime"), F.lit("investigation_case"), "case_id").alias(
            "investigation_case_key"
        ),
        hash_key(F.lit("fincrime"), F.lit("fraud_alert"), "alert_id").alias(
            "fraud_alert_key"
        ),
        F.col("linked_timestamp").cast("timestamp").alias("linked_at"),
        F.lit("fincrime").alias("source_system"),
        F.col("case_alert_link_id").cast("string").alias("source_business_key"),
        F.concat_ws(
            ":", F.lit("investigation_case_fraud_alert"), F.col("case_alert_link_id")
        ).alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


@dp.table(name=atomic_tgt("investigation_case_monitoring_alert"))
def silver_investigation_case_monitoring_alert():
    df = spark.read.table(
        validated_src("investigation_case_transaction_monitoring_alert")
    )
    return df.select(
        hash_key(
            F.lit("fincrime"), F.lit("case_monitoring_alert"), "case_alert_link_id"
        ).alias("investigation_case_monitoring_alert_key"),
        hash_key(F.lit("fincrime"), F.lit("investigation_case"), "case_id").alias(
            "investigation_case_key"
        ),
        hash_key(
            F.lit("fincrime"), F.lit("transaction_monitoring_alert"), "alert_id"
        ).alias("monitoring_alert_key"),
        F.col("linked_timestamp").cast("timestamp").alias("linked_at"),
        F.lit("fincrime").alias("source_system"),
        F.col("case_alert_link_id").cast("string").alias("source_business_key"),
        F.concat_ws(
            ":",
            F.lit("investigation_case_transaction_monitoring_alert"),
            F.col("case_alert_link_id"),
        ).alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


@dp.table(name=atomic_tgt("investigation_note"))
def silver_investigation_note():
    df = spark.read.table(validated_src("investigation_note"))
    return df.select(
        hash_key(F.lit("fincrime"), F.lit("investigation_note"), "note_id").alias(
            "investigation_note_key"
        ),
        hash_key(F.lit("fincrime"), F.lit("investigation_case"), "case_id").alias(
            "investigation_case_key"
        ),
        F.col("author_id"),
        F.col("note_timestamp").cast("timestamp").alias("note_timestamp"),
        F.col("source_arrival_timestamp")
        .cast("timestamp")
        .alias("source_arrival_timestamp"),
        F.col("note_type"),
        F.col("note_text"),
        F.lit("fincrime").alias("source_system"),
        F.col("note_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("investigation_note"), F.col("note_id")).alias(
            "bronze_record_ref"
        ),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


@dp.table(name=atomic_tgt("aml_case"))
def silver_aml_case():
    df = spark.read.table(validated_src("aml_case"))
    return df.select(
        hash_key(F.lit("fincrime"), F.lit("aml_case"), "case_id").alias("aml_case_key"),
        hash_key(
            F.lit("fincrime"), F.lit("investigation_case"), "investigation_case_id"
        ).alias("investigation_case_key"),
        hash_key(get_source_system(F.col("customer_ref")), "customer_ref").alias(
            "party_key"
        ),
        F.col("case_type"),
        F.col("risk_level"),
        F.col("opened_date").cast("date").alias("opened_date"),
        F.col("closed_date").cast("date").alias("closed_date"),
        F.lit("fincrime").alias("source_system"),
        F.col("case_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("aml_case"), F.col("case_id")).alias(
            "bronze_record_ref"
        ),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


@dp.table(name=atomic_tgt("watchlist_entry"))
def silver_watchlist_entry():
    df = spark.read.table(validated_src("watchlist"))
    return df.select(
        hash_key(F.lit("fincrime"), F.lit("watchlist"), "watchlist_id").alias(
            "watchlist_entry_key"
        ),
        F.col("watchlist_id").cast("string").alias("source_watchlist_id"),
        F.col("entity_name"),
        F.col("list_type"),
        F.col("country"),
        F.col("added_date").cast("date").alias("added_date"),
        F.lit("fincrime").alias("source_system"),
        F.col("watchlist_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("watchlist"), F.col("watchlist_id")).alias(
            "bronze_record_ref"
        ),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


@dp.table(name=atomic_tgt("sanctions_screening"))
def silver_sanctions_screening():
    df = spark.read.table(validated_src("sanction_screening"))
    return df.select(
        hash_key(F.lit("fincrime"), F.lit("sanction_screening"), "screening_id").alias(
            "sanctions_screening_key"
        ),
        hash_key(get_source_system(F.col("customer_ref")), "customer_ref").alias(
            "party_key"
        ),
        hash_key(F.lit("fincrime"), F.lit("watchlist"), "watchlist_id").alias(
            "watchlist_entry_key"
        ),
        F.col("screened_name"),
        F.col("match_score").cast("decimal(5,2)").alias("match_score"),
        F.col("screening_date").cast("date").alias("screening_date"),
        F.col("result").alias("screening_result"),
        F.lit("fincrime").alias("source_system"),
        F.col("screening_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("sanction_screening"), F.col("screening_id")).alias(
            "bronze_record_ref"
        ),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


@dp.table(name=atomic_tgt("investigation_case_sanctions_screening"))
def silver_investigation_case_sanctions_screening():
    df = spark.read.table(validated_src("investigation_case_sanction_screening"))
    return df.select(
        hash_key(
            F.lit("fincrime"),
            F.lit("case_sanction_screening"),
            "case_screening_link_id",
        ).alias("investigation_case_sanctions_screening_key"),
        hash_key(F.lit("fincrime"), F.lit("investigation_case"), "case_id").alias(
            "investigation_case_key"
        ),
        hash_key(F.lit("fincrime"), F.lit("sanction_screening"), "screening_id").alias(
            "sanctions_screening_key"
        ),
        F.col("linked_timestamp").cast("timestamp").alias("linked_at"),
        F.lit("fincrime").alias("source_system"),
        F.col("case_screening_link_id").cast("string").alias("source_business_key"),
        F.concat_ws(
            ":",
            F.lit("investigation_case_sanction_screening"),
            F.col("case_screening_link_id"),
        ).alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


@dp.table(name=atomic_tgt("suspicious_activity_report"))
def silver_suspicious_activity_report():
    df = spark.read.table(validated_src("suspicious_activity_report"))
    return df.select(
        hash_key(
            F.lit("fincrime"), F.lit("suspicious_activity_report"), "sar_id"
        ).alias("suspicious_activity_report_key"),
        hash_key(F.lit("fincrime"), F.lit("aml_case"), "case_id").alias("aml_case_key"),
        F.col("filed_date").cast("date").alias("filed_date"),
        F.col("regulatory_ref").alias("regulatory_reference"),
        F.col("status").alias("report_status"),
        F.lit("fincrime").alias("source_system"),
        F.col("sar_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("suspicious_activity_report"), F.col("sar_id")).alias(
            "bronze_record_ref"
        ),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


@dp.table(name=atomic_tgt("card_fraud_flag"))
def silver_card_fraud_flag():
    df = spark.read.table(validated_src("card_fraud_flag"))
    return df.select(
        hash_key(F.lit("card_system"), F.lit("card_fraud_flag"), "flag_id").alias(
            "card_fraud_flag_key"
        ),
        F.col("flag_reason"),
        F.col("flag_date").cast("date").alias("flag_date"),
        F.col("resolved_status"),
        F.lit("card_system").alias("source_system"),
        F.col("flag_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("card_fraud_flag"), F.col("flag_id")).alias(
            "bronze_record_ref"
        ),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


@dp.table(name=atomic_tgt("financial_event_card_fraud_flag"))
def silver_financial_event_card_fraud_flag():
    df = spark.read.table(validated_src("card_fraud_flag")).filter(
        "card_txn_id IS NOT NULL AND card_txn_id <> -1"
    )
    return df.select(
        hash_key(
            F.lit("card_system"), F.lit("evt_card_fraud_flag"), "flag_id", "card_txn_id"
        ).alias("financial_event_card_fraud_flag_key"),
        hash_key(F.lit("card_system"), F.lit("CARD_PAYMENT"), "card_txn_id").alias(
            "financial_event_key"
        ),
        hash_key(F.lit("card_system"), F.lit("card_fraud_flag"), "flag_id").alias(
            "card_fraud_flag_key"
        ),
        F.lit("card_system").alias("source_system"),
        F.concat_ws(":", F.col("flag_id"), F.col("card_txn_id"))
        .cast("string")
        .alias("source_business_key"),
        F.concat_ws(":", F.lit("card_fraud_flag"), F.col("flag_id")).alias(
            "bronze_record_ref"
        ),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


@dp.table(name=atomic_tgt("chargeback"))
def silver_chargeback():
    df = spark.read.table(validated_src("chargeback"))
    return df.select(
        hash_key(F.lit("fincrime"), F.lit("chargeback"), "chargeback_id").alias(
            "chargeback_key"
        ),
        hash_key(F.lit("card_system"), F.lit("CARD_PAYMENT"), "card_txn_id").alias(
            "financial_event_key"
        ),
        F.col("reason_code"),
        F.col("dispute_amount").cast("decimal(12,2)").alias("dispute_amount"),
        F.col("filed_date").cast("date").alias("filed_date"),
        F.col("status").alias("chargeback_status"),
        F.col("resolved_date").cast("date").alias("resolved_date"),
        F.lit("fincrime").alias("source_system"),
        F.col("chargeback_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("chargeback"), F.col("chargeback_id")).alias(
            "bronze_record_ref"
        ),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


@dp.table(name=atomic_tgt("call_center_contact"))
def silver_call_center_contact():
    df = spark.read.table(validated_src("call_center_log"))
    return df.select(
        hash_key(F.lit("fincrime"), F.lit("call_center_log"), "call_id").alias(
            "call_center_contact_key"
        ),
        hash_key(get_source_system(F.col("customer_ref")), "customer_ref").alias(
            "party_key"
        ),
        hash_key(F.lit("fincrime"), F.lit("investigation_case"), "case_id").alias(
            "investigation_case_key"
        ),
        # Format-Preserving Masked Phone (NAB TDM Rule 1.13)
        nab_mask_phone(F.col("caller_phone")).alias("caller_phone_masked"),
        # Reversible Encrypted Phone (AES-256)
        F.base64(
            F.aes_encrypt(
                F.coalesce(F.col("caller_phone"), F.lit("")).cast("string"),
                F.lit(AES_KEY),
            )
        ).alias("caller_phone_encrypted"),
        # Legacy SHA-256 Token
        F.sha2(F.coalesce(F.col("caller_phone"), F.lit("")), 256).alias(
            "caller_phone_token"
        ),
        F.col("call_timestamp").cast("timestamp").alias("call_timestamp"),
        F.col("call_reason"),
        F.col("agent_id"),
        F.col("call_duration_seconds").cast("bigint").alias("call_duration_seconds"),
        F.lit("fincrime").alias("source_system"),
        F.col("call_id").cast("string").alias("source_business_key"),
        F.concat_ws(":", F.lit("call_center_log"), F.col("call_id")).alias(
            "bronze_record_ref"
        ),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )

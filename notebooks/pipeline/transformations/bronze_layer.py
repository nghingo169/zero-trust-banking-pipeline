import re
from typing import Optional, Tuple, Dict, Any
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# ==============================================================================
# SPARK GLOBAL CONFIGS & NAMESPACE INITIALIZATION
# ==============================================================================
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.databricks.delta.typeWidening.enabled", "true")

AWS_S3_BUCKET = "nab-src-dataset"
S3_BASE_URL = f"s3a://{AWS_S3_BUCKET}/banking_v2/snapshots"
SCHEMA_DB = "workspace.bronze"

# ==============================================================================
# BẢNG CẤU HÌNH 41 BẢNG + FULL SCHEMA HINTS THEO ĐÚNG DBML
# ==============================================================================
TABLE_CONFIGS: Dict[str, Dict[str, Any]] = {
    # --- 1. Customer Master Data ---
    "core_banking_customer": {
        "domain": "customer_master", "keys": ["cust_no"], "cdc_type": "snapshot",
        "hints": "date_of_birth DATE, created_date DATE"
    },
    "crm_customer": {
        "domain": "customer_master", "keys": ["party_id"], "cdc_type": "snapshot",
        "hints": "created_date DATE"
    },
    "customer_kyc": {
        "domain": "customer_master", "keys": ["kyc_id"], "cdc_type": "snapshot",
        "hints": "verified_date DATE"
    },
    "customer_employment": {
        "domain": "customer_master", "keys": ["employment_id"], "cdc_type": "snapshot",
        "hints": "monthly_income DECIMAL(12,2)"
    },
    "customer_request": {
        "domain": "customer_master", "keys": ["request_id"], "cdc_type": "stream",
        "hints": "request_date DATE, resolution_date DATE"
    },
    "customer_account": {
        "domain": "customer_master", "keys": ["link_id"], "cdc_type": "snapshot",
        "hints": "account_id BIGINT, linked_date DATE"
    },
    "account": {
        "domain": "customer_master", "keys": ["account_id"], "cdc_type": "snapshot",
        "hints": "account_id BIGINT, open_date DATE"
    },
    
    # --- 2. Customer Transaction Data ---
    "account_transaction": {
        "domain": "customer_transaction", "keys": ["account_txn_id"], "cdc_type": "stream",
        "hints": "account_txn_id BIGINT, account_id BIGINT, amount DECIMAL(12,2), txn_timestamp TIMESTAMP"
    },
    "account_transaction_status_event": {
        "domain": "customer_transaction", "keys": ["status_event_id"], "cdc_type": "stream",
        "hints": "account_txn_id BIGINT, status_timestamp TIMESTAMP, source_arrival_timestamp TIMESTAMP, sequence_number BIGINT"
    },
    "atm_transaction_status_event": {
        "domain": "customer_transaction", "keys": ["status_event_id"], "cdc_type": "stream",
        "hints": "account_txn_id BIGINT, status_timestamp TIMESTAMP, source_arrival_timestamp TIMESTAMP, sequence_number BIGINT"
    },
    "payment_gateway_status_event": {
        "domain": "customer_transaction", "keys": ["status_event_id"], "cdc_type": "stream",
        "hints": "account_txn_id BIGINT, card_txn_id BIGINT, status_timestamp TIMESTAMP, source_arrival_timestamp TIMESTAMP, sequence_number BIGINT"
    },
    "transaction_channel": {
        "domain": "customer_transaction", "keys": ["channel_id"], "cdc_type": "snapshot",
        "hints": ""
    },
    "merchant": {
        "domain": "customer_transaction", "keys": ["merchant_id"], "cdc_type": "snapshot",
        "hints": ""
    },
    "merchant_store": {
        "domain": "customer_transaction", "keys": ["store_id"], "cdc_type": "snapshot",
        "hints": "registered_date DATE"
    },
    "log_atm": {
        "domain": "customer_transaction", "keys": ["log_id"], "cdc_type": "stream",
        "hints": "account_txn_id BIGINT, amount DECIMAL(12,2), log_timestamp TIMESTAMP"
    },
    "payment_gateway_log": {
        "domain": "customer_transaction", "keys": ["gateway_txn_id"], "cdc_type": "stream",
        "hints": "account_txn_id BIGINT, card_txn_id BIGINT, amount DECIMAL(12,2), gateway_timestamp TIMESTAMP"
    },
    "balance_snapshot": {
        "domain": "customer_transaction", "keys": ["balance_id"], "cdc_type": "snapshot",
        "hints": "account_id BIGINT, balance_date DATE, opening_balance DECIMAL(14,2), closing_balance DECIMAL(14,2), available_balance DECIMAL(14,2)"
    },
    
    # --- 3. Financial Crime Data ---
    "fraud_alert": {
        "domain": "financial_crime", "keys": ["alert_id"], "cdc_type": "stream",
        "hints": "account_txn_id BIGINT, alert_score DECIMAL(5,2), created_date DATE"
    },
    "transaction_monitoring_alert": {
        "domain": "financial_crime", "keys": ["alert_id"], "cdc_type": "stream",
        "hints": "primary_account_txn_id BIGINT, alert_score DECIMAL(5,2), alert_timestamp TIMESTAMP"
    },
    "transaction_monitoring_alert_account_transaction": {
        "domain": "financial_crime", "keys": ["alert_account_txn_link_id"], "cdc_type": "stream",
        "hints": "account_txn_id BIGINT, is_primary BOOLEAN"
    },
    "transaction_monitoring_alert_card_transaction": {
        "domain": "financial_crime", "keys": ["alert_card_txn_link_id"], "cdc_type": "stream",
        "hints": "card_txn_id BIGINT, is_primary BOOLEAN"
    },
    "investigation_case_transaction_monitoring_alert": {
        "domain": "financial_crime", "keys": ["case_alert_link_id"], "cdc_type": "stream",
        "hints": "linked_timestamp TIMESTAMP"
    },
    "investigation_case_card_fraud_flag": {
        "domain": "financial_crime", "keys": ["case_flag_link_id"], "cdc_type": "stream",
        "hints": "linked_timestamp TIMESTAMP"
    },
    "investigation_case": {
        "domain": "financial_crime", "keys": ["case_id"], "cdc_type": "snapshot",
        "hints": "opened_timestamp TIMESTAMP, closed_timestamp TIMESTAMP"
    },
    "investigation_case_account_transaction": {
        "domain": "financial_crime", "keys": ["case_account_txn_link_id"], "cdc_type": "stream",
        "hints": "account_txn_id BIGINT, linked_timestamp TIMESTAMP"
    },
    "investigation_case_card_transaction": {
        "domain": "financial_crime", "keys": ["case_card_txn_link_id"], "cdc_type": "stream",
        "hints": "card_txn_id BIGINT, linked_timestamp TIMESTAMP"
    },
    "investigation_case_fraud_alert": {
        "domain": "financial_crime", "keys": ["case_alert_link_id"], "cdc_type": "stream",
        "hints": "linked_timestamp TIMESTAMP"
    },
    "investigation_case_sanction_screening": {
        "domain": "financial_crime", "keys": ["case_screening_link_id"], "cdc_type": "stream",
        "hints": "linked_timestamp TIMESTAMP"
    },
    "investigation_note": {
        "domain": "financial_crime", "keys": ["note_id"], "cdc_type": "stream",
        "hints": "note_timestamp TIMESTAMP, source_arrival_timestamp TIMESTAMP"
    },
    "aml_case": {
        "domain": "financial_crime", "keys": ["case_id"], "cdc_type": "snapshot",
        "hints": "opened_date DATE, closed_date DATE"
    },
    "sanction_screening": {
        "domain": "financial_crime", "keys": ["screening_id"], "cdc_type": "stream",
        "hints": "match_score DECIMAL(5,2), screening_date DATE"
    },
    "suspicious_activity_report": {
        "domain": "financial_crime", "keys": ["sar_id"], "cdc_type": "snapshot",
        "hints": "filed_date DATE"
    },
    "watchlist": {
        "domain": "financial_crime", "keys": ["watchlist_id"], "cdc_type": "snapshot",
        "hints": "added_date DATE"
    },
    "account_transaction_risk_score": {
        "domain": "financial_crime", "keys": ["score_id"], "cdc_type": "stream",
        "hints": "account_txn_id BIGINT, model_score DECIMAL(6,4), scored_date DATE"
    },
    "call_center_log": {
        "domain": "financial_crime", "keys": ["call_id"], "cdc_type": "stream",
        "hints": "call_timestamp TIMESTAMP, call_duration_seconds BIGINT"
    },
    "chargeback": {
        "domain": "financial_crime", "keys": ["chargeback_id"], "cdc_type": "stream",
        "hints": "card_txn_id BIGINT, dispute_amount DECIMAL(12,2), filed_date DATE, resolved_date DATE"
    },
    
    # --- 4. Card Data ---
    "card": {
        "domain": "card", "keys": ["card_id"], "cdc_type": "snapshot",
        "hints": "account_id BIGINT, issue_date DATE, expiry_date DATE"
    },
    "card_transaction": {
        "domain": "card", "keys": ["card_txn_id"], "cdc_type": "stream",
        "hints": "card_txn_id BIGINT, amount DECIMAL(12,2), txn_timestamp TIMESTAMP, is_fraud BOOLEAN"
    },
    "card_fraud_flag": {
        "domain": "card", "keys": ["flag_id"], "cdc_type": "stream",
        "hints": "card_txn_id BIGINT, flag_date DATE"
    },
    "card_limit_history": {
        "domain": "card", 
        "keys": ["history_id"], 
        "cdc_type": "stream",
        "hints": "limit_amount DECIMAL(12,2), effective_date DATE"
    },
    "card_transaction_status_event": {
        "domain": "card", "keys": ["status_event_id"], "cdc_type": "stream",
        "hints": "card_txn_id BIGINT, status_timestamp TIMESTAMP, source_arrival_timestamp TIMESTAMP, sequence_number BIGINT"
    }
}

try:
    dates_str = spark.conf.get("pipeline.available_business_dates")
    AVAILABLE_BUSINESS_DATES = [int(d.strip()) for d in dates_str.split(",")]
except Exception:
    AVAILABLE_BUSINESS_DATES = [20260705, 20260706, 20260707, 20260708, 20260709, 20260710]

# ==============================================================================
# HELPER: BỔ SUNG CÁC CỘT AUDIT / CDC METADATA
# ==============================================================================
def add_enterprise_cdc_metadata(
    df: DataFrame, 
    domain: str, 
    current_date_str: Optional[str] = None
) -> DataFrame:
    now_ts = F.current_timestamp()
    
    source_path_col = (
        F.col("_metadata.file_path") 
        if "_metadata" in df.columns or "_metadata" in [c.name for c in df.schema.fields]
        else F.lit("")
    )
    
    parsed_date = F.regexp_extract(source_path_col, r"business_date=(\d{4}-\d{2}-\d{2})", 1)
    if current_date_str:
        final_date_col = F.when(parsed_date != "", parsed_date).otherwise(F.lit(current_date_str))
    else:
        final_date_col = parsed_date

    sim_id_col = F.regexp_extract(source_path_col, r"/simulation_id=([^/]+)/", 1)

    return (
        df.withColumn("_source_path", source_path_col)
        .withColumn("business_date", F.to_date(final_date_col))
        .withColumn("domain", F.lit(domain))
        .withColumn("simulation_id", F.when(sim_id_col != "", sim_id_col).otherwise(F.lit("unknown")))
        .withColumn("cdc_change_op", F.lit(1).cast("int"))
        .withColumn(
            "cdc_change_sequence", 
            F.concat_ws("_", 
                F.date_format(F.col("business_date"), "yyyyMMdd"), 
                F.expr("uuid()")
            )
        )
        .withColumn("cdc_change_time", F.to_timestamp(F.col("business_date")))
        .withColumn("LOAD_DTTM", now_ts)
        .withColumn("EXTRACT_DTTM", now_ts)
        .withColumn("EXTRACT_DTE", F.to_date(now_ts))
        .drop("_source_path")
    )

# ==============================================================================
# MAIN BUILDER: CDC FLOW VỚI DYNAMIC SCHEMA HINTS
# ==============================================================================
def build_cdc_flow(table_name: str, domain: str, keys: list[str], cdc_type: str, schema_hints: str):
    target_table_identifier = f"{SCHEMA_DB}.{table_name}"

    dp.create_streaming_table(
        name=target_table_identifier,
        table_properties={
            "delta.enableChangeDataFeed": "true",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true",
            "pipelines.schemaEvolutionMode": "addNewColumns",
            "delta.enableTypeWidening": "true"
        }
    )

    # --------------------------------------------------------------------------
    # NHÁNH A: STREAM CDC (BẢNG EVENT / LOG DATA)
    # --------------------------------------------------------------------------
    if cdc_type == "stream":
        view_name = f"v_{table_name}_stream"

        @dp.view(name=view_name)
        def stream_view():
            stream_path = f"{S3_BASE_URL}/simulation_id=*/snapshot_type=full/business_date=*/{domain}/{table_name}"
            schema_location = f"/Volumes/workspace/bronze/checkpoints/{table_name}_schema"

            reader = (
                spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "parquet")
                .option("cloudFiles.schemaEvolutionMode", "addNewColumnsWithTypeWidening")
                .option("cloudFiles.schemaLocation", schema_location)
                .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            )

            # Chỉ truyền schemaHints nếu bảng đó có cấu hình hints (không rỗng)
            if schema_hints:
                reader = reader.option("cloudFiles.schemaHints", schema_hints)

            raw_df = reader.load(stream_path)
            return add_enterprise_cdc_metadata(raw_df, domain)

        dp.create_auto_cdc_flow(
            target=target_table_identifier,
            source=view_name,
            keys=keys,
            sequence_by=F.col("cdc_change_sequence"),
            stored_as_scd_type="2"
        )

    # --------------------------------------------------------------------------
    # NHÁNH B: SNAPSHOT CDC (BẢNG MASTER / STATE DATA)
    # --------------------------------------------------------------------------
    else:
        def next_snapshot_and_version(
            latest_version: Optional[int],
        ) -> Optional[Tuple[DataFrame, int]]:
            
            if latest_version is None:
                remaining_versions = sorted(AVAILABLE_BUSINESS_DATES)
            else:
                remaining_versions = sorted([v for v in AVAILABLE_BUSINESS_DATES if v > latest_version])

            if not remaining_versions:
                return None

            next_ver = remaining_versions[0]
            v_str = str(next_ver)
            date_formatted = f"{v_str[:4]}-{v_str[4:6]}-{v_str[6:]}"

            exact_snapshot_path = (
                f"{S3_BASE_URL}/simulation_id=*/snapshot_type=full/business_date={date_formatted}/{domain}/{table_name}"
            )

            try:
                df = (
                    spark.read
                    .option("mergeSchema", "true")
                    .parquet(exact_snapshot_path)
                )

                df = add_enterprise_cdc_metadata(df, domain, current_date_str=date_formatted)
                return (df, next_ver)

            except Exception as e:
                print(f"[WARN] Skip date {date_formatted} for {table_name}: {str(e)}")
                return None

        dp.create_auto_cdc_from_snapshot_flow(
            target=target_table_identifier,
            source=next_snapshot_and_version,
            keys=keys,
            stored_as_scd_type="2",
        )

# ==============================================================================
# ĐĂNG KÝ AUTOMATION TẤT CẢ 41 BẢNG VÀO DLT DAG
# ==============================================================================
for tbl_name, config in TABLE_CONFIGS.items():
    build_cdc_flow(
        table_name=tbl_name, 
        domain=config["domain"], 
        keys=config["keys"],
        cdc_type=config["cdc_type"],
        schema_hints=config.get("hints", "")
    )
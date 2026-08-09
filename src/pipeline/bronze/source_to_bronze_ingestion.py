import re
from typing import Any, Dict, List, Optional, Tuple

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# ==============================================================================
# PIPELINE CONFIGURATION HELPERS & RETRIEVAL
# ==============================================================================


def safe_conf_get(key: str, default: str = "") -> str:
    """Safely fetch Spark configuration in PySpark Connect / Serverless mode."""
    try:
        return spark.conf.get(key, default)
    except Exception:
        return default


try:
    spark.conf.set("spark.databricks.delta.typeWidening.enabled", "true")
except Exception:
    pass

SOURCE_MODE = safe_conf_get("pipeline.source_mode", "s3").lower()

if SOURCE_MODE == "s3":
    SOURCE_ROOT = safe_conf_get(
        "pipeline.source_root", "s3://landing-bucket/banking"
    ).rstrip("/")
elif SOURCE_MODE == "volume":
    SOURCE_ROOT = safe_conf_get(
        "pipeline.source_root", "/Volumes/main/default/landing"
    ).rstrip("/")
elif SOURCE_MODE == "memory":
    SOURCE_ROOT = safe_conf_get(
        "pipeline.source_root", "memory://landing"
    ).rstrip("/")
else:
    raise ValueError("pipeline.source_mode must be 's3', 'volume', or 'memory'.")

TARGET_CATALOG = safe_conf_get("pipeline.target_catalog", "workspace")
TARGET_SCHEMA = safe_conf_get("pipeline.target_schema", "bronze")

SCHEMA_DB = f"{TARGET_CATALOG}.{TARGET_SCHEMA}"

TECHNICAL_METADATA_COLUMNS = [
    "business_date",
    "domain",
    "source_file_name",
    "source_file_modified_at",
    "LOAD_DTTM",
    "EXTRACT_DTTM",
    "EXTRACT_DTE",
]

SOURCE_SCHEMA_CONTRACTS: Dict[str, Dict[str, Dict[str, Any]]] = {
    "crm_customer": {
        "preferred_contact_method": {
            "data_type": "STRING",
            "nullable": True,
            "introduced_on": "2026-07-06",
        },
    },
}

CACHED_BUSINESS_DATES: Optional[List[int]] = None


def table_configs(
    domain: str,
    stored_as_scd_type: str,
    tables: Dict[str, Tuple[List[str], str]],
) -> Dict[str, Dict[str, Any]]:
    return {
        table_name: {
            "domain": domain,
            "keys": keys,
            "stored_as_scd_type": stored_as_scd_type,
            "hints": hints,
        }
        for table_name, (keys, hints) in tables.items()
    }


TABLE_CONFIGS: Dict[str, Dict[str, Any]] = {
    **table_configs(
        "customer_master",
        "2",
        {
            "core_banking_customer": (
                ["cust_no"],
                "date_of_birth DATE, created_date DATE",
            ),
            "crm_customer": (
                ["party_id"],
                "created_date DATE",
            ),
            "customer_kyc": (
                ["kyc_id"],
                "verified_date DATE",
            ),
            "customer_employment": (
                ["employment_id"],
                "monthly_income DECIMAL(12,2)",
            ),
            "customer_request": (
                ["request_id"],
                "resolution_date DATE, resolution_date DATE",
            ),
            "customer_account": (
                ["link_id"],
                "account_id BIGINT, linked_date DATE",
            ),
            "account": (
                ["account_id"],
                "account_id BIGINT, open_date DATE",
            ),
        },
    ),
    **table_configs(
        "customer_transaction",
        "2",
        {
            "account_transaction": (
                ["account_txn_id"],
                (
                    "account_txn_id BIGINT, account_id BIGINT, "
                    "amount DECIMAL(12,2), txn_timestamp TIMESTAMP"
                ),
            ),
            "transaction_channel": (
                ["channel_id"],
                "",
            ),
            "merchant": (
                ["merchant_id"],
                "",
            ),
            "merchant_store": (
                ["store_id"],
                "registered_date DATE",
            ),
            "balance_snapshot": (
                ["balance_id"],
                (
                    "account_id BIGINT, balance_date DATE, "
                    "opening_balance DECIMAL(14,2), "
                    "closing_balance DECIMAL(14,2), "
                    "available_balance DECIMAL(14,2)"
                ),
            ),
            "log_atm": (
                ["log_id"],
                (
                    "account_txn_id BIGINT, amount DECIMAL(12,2), "
                    "log_timestamp TIMESTAMP"
                ),
            ),
            "payment_gateway_log": (
                ["gateway_txn_id"],
                (
                    "account_txn_id BIGINT, card_txn_id BIGINT, "
                    "amount DECIMAL(12,2), gateway_timestamp TIMESTAMP"
                ),
            ),
        },
    ),
    **table_configs(
        "customer_transaction",
        "1",
        {
            "account_transaction_status_event": (
                ["status_event_id", "account_txn_id"],
                (
                    "account_txn_id BIGINT, status_timestamp TIMESTAMP, "
                    "source_arrival_timestamp TIMESTAMP, "
                    "sequence_number BIGINT"
                ),
            ),
            "atm_transaction_status_event": (
                ["status_event_id", "account_txn_id"],
                (
                    "account_txn_id BIGINT, status_timestamp TIMESTAMP, "
                    "source_arrival_timestamp TIMESTAMP, "
                    "sequence_number BIGINT"
                ),
            ),
            "payment_gateway_status_event": (
                ["status_event_id", "gateway_txn_id", "event_parent_ref"],
                (
                    "account_txn_id BIGINT, card_txn_id BIGINT, "
                    "status_timestamp TIMESTAMP, "
                    "source_arrival_timestamp TIMESTAMP, "
                    "sequence_number BIGINT"
                ),
            ),
        },
    ),
    **table_configs(
        "financial_crime",
        "2",
        {
            "fraud_alert": (
                ["alert_id"],
                (
                    "account_txn_id BIGINT, alert_score DECIMAL(5,2), "
                    "created_date DATE"
                ),
            ),
            "transaction_monitoring_alert": (
                ["alert_id"],
                (
                    "primary_account_txn_id BIGINT, "
                    "alert_score DECIMAL(5,2), alert_timestamp TIMESTAMP"
                ),
            ),
            "investigation_case": (
                ["case_id"],
                "opened_timestamp TIMESTAMP, closed_timestamp TIMESTAMP",
            ),
            "aml_case": (
                ["case_id"],
                "opened_date DATE, closed_date DATE",
            ),
            "suspicious_activity_report": (
                ["sar_id"],
                "filed_date DATE",
            ),
            "watchlist": (
                ["watchlist_id"],
                "added_date DATE",
            ),
            "chargeback": (
                ["chargeback_id"],
                (
                    "card_txn_id BIGINT, dispute_amount DECIMAL(12,2), "
                    "filed_date DATE, resolved_date DATE"
                ),
            ),
        },
    ),
    **table_configs(
        "financial_crime",
        "2",
        {
            "transaction_monitoring_alert_account_transaction": (
                ["alert_account_txn_link_id"],
                "account_txn_id BIGINT, is_primary BOOLEAN",
            ),
            "transaction_monitoring_alert_card_transaction": (
                ["alert_card_txn_link_id"],
                "card_txn_id BIGINT, is_primary BOOLEAN",
            ),
            "investigation_case_transaction_monitoring_alert": (
                ["case_alert_link_id"],
                "linked_timestamp TIMESTAMP",
            ),
            "investigation_case_card_fraud_flag": (
                ["case_flag_link_id"],
                "linked_timestamp TIMESTAMP",
            ),
            "investigation_case_account_transaction": (
                ["case_account_txn_link_id"],
                "account_txn_id BIGINT, linked_timestamp TIMESTAMP",
            ),
            "investigation_case_card_transaction": (
                ["case_card_txn_link_id"],
                "card_txn_id BIGINT, linked_timestamp TIMESTAMP",
            ),
            "investigation_case_fraud_alert": (
                ["case_alert_link_id"],
                "linked_timestamp TIMESTAMP",
            ),
            "investigation_case_sanction_screening": (
                ["case_screening_link_id"],
                "linked_timestamp TIMESTAMP",
            ),
            "investigation_note": (
                ["note_id"],
                "note_timestamp TIMESTAMP, source_arrival_timestamp TIMESTAMP",
            ),
            "sanction_screening": (
                ["screening_id"],
                "match_score DECIMAL(5,2), screening_date DATE",
            ),
            "account_transaction_risk_score": (
                ["score_id"],
                (
                    "account_txn_id BIGINT, model_score DECIMAL(6,4), "
                    "scored_date DATE"
                ),
            ),
            "call_center_log": (
                ["call_id"],
                "call_timestamp TIMESTAMP, call_duration_seconds BIGINT",
            ),
        },
    ),
    **table_configs(
        "card",
        "2",
        {
            "card": (
                ["card_id"],
                "account_id BIGINT, issue_date DATE, expiry_date DATE",
            ),
            "card_transaction": (
                ["card_txn_id"],
                (
                    "card_txn_id BIGINT, amount DECIMAL(12,2), "
                    "txn_timestamp TIMESTAMP, is_fraud BOOLEAN"
                ),
            ),
            "card_fraud_flag": (
                ["flag_id"],
                "card_txn_id BIGINT, flag_date DATE",
            ),
            "card_limit_history": (
                ["history_id"],
                "limit_amount DECIMAL(12,2), effective_date DATE",
            ),
        },
    ),
    **table_configs(
        "card",
        "1",
        {
            "card_transaction_status_event": (
                ["status_event_id", "card_txn_id"],
                (
                    "card_txn_id BIGINT, status_timestamp TIMESTAMP, "
                    "source_arrival_timestamp TIMESTAMP, "
                    "sequence_number BIGINT"
                ),
            ),
        },
    ),
}


def source_path(
    domain: str,
    table_name: str,
    business_date: Optional[str] = None,
) -> str:
    date_component = business_date if business_date else "*"

    return (
        f"{SOURCE_ROOT}/"
        f"business_date={date_component}/"
        f"{domain}/"
        f"{table_name}"
    )


def get_available_business_dates() -> List[int]:
    global CACHED_BUSINESS_DATES

    if CACHED_BUSINESS_DATES is not None:
        return CACHED_BUSINESS_DATES

    snapshot_root = f"{SOURCE_ROOT}/"

    try:
        dates = set()

        for directory in dbutils.fs.ls(snapshot_root):
            match = re.search(
                r"business_date=(\d{4})-(\d{2})-(\d{2})",
                directory.path,
            )

            if match:
                dates.add(int(f"{match.group(1)}{match.group(2)}{match.group(3)}"))

        CACHED_BUSINESS_DATES = sorted(dates)
        print(f"[INFO] Discovered snapshot versions: {CACHED_BUSINESS_DATES}")
        return CACHED_BUSINESS_DATES

    except Exception as error:
        raise RuntimeError(
            f"Unable to list Bronze source snapshots at {snapshot_root}. "
            "Verify the configured source path and the pipeline identity's access."
        ) from error


def apply_schema_hints(df: DataFrame, hints: str) -> DataFrame:
    if not hints:
        return df

    pattern = r"(\w+)\s+(DECIMAL\(\d+,\d+\)|\w+)"

    for column_name, data_type in re.findall(pattern, hints):
        if column_name in df.columns:
            df = df.withColumn(
                column_name,
                F.expr(f"try_cast({column_name} AS {data_type})"),
            )

    return df


def apply_source_schema_contract(df: DataFrame, table_name: str) -> DataFrame:
    for column_name, field_contract in SOURCE_SCHEMA_CONTRACTS.get(
        table_name, {}
    ).items():
        data_type = field_contract["data_type"]
        if column_name in df.columns:
            value = F.col(column_name).cast(data_type)
        else:
            value = F.lit(None).cast(data_type)
        df = df.withColumn(column_name, value)

    return df


def add_derived_event_keys(df: DataFrame, table_name: str) -> DataFrame:
    if table_name != "payment_gateway_status_event":
        return df

    return df.withColumn(
        "event_parent_ref",
        F.when(
            F.col("account_txn_id").isNotNull(),
            F.concat(F.lit("ACCOUNT:"), F.col("account_txn_id").cast("string")),
        )
        .when(
            F.col("card_txn_id").isNotNull(),
            F.concat(F.lit("CARD:"), F.col("card_txn_id").cast("string")),
        )
        .otherwise(F.lit("<MISSING_PARENT>")),
    )


def remove_confirmed_snapshot_replays(
    df: DataFrame,
    table_name: str,
    keys: List[str],
) -> DataFrame:
    if table_name == "account_transaction_status_event":
        return df.dropDuplicates(keys)
    return df


def add_operational_metadata(
    df: DataFrame,
    domain: str,
    business_date: str,
) -> DataFrame:
    load_timestamp = F.current_timestamp()

    return (
        df.drop("simulation_id", "snapshot_type")
        .withColumn("business_date", F.to_date(F.lit(business_date)))
        .withColumn("domain", F.lit(domain))
        .withColumn("source_file_name", F.col("_metadata.file_name"))
        .withColumn(
            "source_file_modified_at",
            F.col("_metadata.file_modification_time"),
        )
        .withColumn("LOAD_DTTM", load_timestamp)
        .withColumn("EXTRACT_DTTM", load_timestamp)
        .withColumn("EXTRACT_DTE", F.to_date(load_timestamp))
    )


def read_snapshot_dataframe(path: str) -> DataFrame:
    """Helper đọc Snapshot DataFrame: 
    Nếu mode 'memory' (In-Memory Testing) -> đọc từ Spark Temp View trong RAM.
    Ngược lại ('s3'/'volume') -> đọc Parquet thực tế trên S3/Volume.
    """
    if SOURCE_MODE == "memory":
        view_name = "view_" + re.sub(r"[^a-zA-Z0-9_]", "_", path)
        return spark.table(view_name)
    
    return spark.read.option("mergeSchema", "true").parquet(path)


def build_snapshot_flow(
    table_name: str,
    domain: str,
    keys: List[str],
    stored_as_scd_type: str,
    schema_hints: str,
) -> None:
    target_table = f"{SCHEMA_DB}.{table_name}"

    dp.create_streaming_table(
        name=target_table,
        table_properties={
            "delta.enableChangeDataFeed": "true",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true",
            "delta.enableTypeWidening": "true",
            "pipelines.schemaEvolutionMode": "addNewColumns",
        },
    )

    def next_snapshot_and_version(
        latest_version: Optional[int],
        table: str = table_name,
        table_domain: str = domain,
        hints: str = schema_hints,
    ) -> Optional[Tuple[DataFrame, int]]:
        all_dates = get_available_business_dates()

        remaining_versions = (
            all_dates
            if latest_version is None
            else [date for date in all_dates if date > latest_version]
        )

        for snapshot_version in remaining_versions:
            value = str(snapshot_version)
            business_date = f"{value[:4]}-{value[4:6]}-{value[6:]}"

            try:
                snapshot_df = read_snapshot_dataframe(
                    source_path(table_domain, table, business_date)
                )

                snapshot_df = apply_schema_hints(snapshot_df, hints)
                snapshot_df = apply_source_schema_contract(snapshot_df, table)
                snapshot_df = add_derived_event_keys(snapshot_df, table)
                snapshot_df = remove_confirmed_snapshot_replays(
                    snapshot_df,
                    table,
                    keys,
                )

                return (
                    add_operational_metadata(
                        snapshot_df,
                        table_domain,
                        business_date,
                    ),
                    snapshot_version,
                )

            except Exception as error:
                if (
                    "PATH_NOT_FOUND" in str(error) 
                    or "not found" in str(error).lower()
                    or "Table or view not found" in str(error)
                ):
                    print(
                        f"[WARN] Snapshot not found: "
                        f"{table} for {business_date}; skipping."
                    )
                    continue

                raise

        return None

    flow_arguments: Dict[str, Any] = {
        "target": target_table,
        "source": next_snapshot_and_version,
        "keys": keys,
        "stored_as_scd_type": stored_as_scd_type,
    }

    if stored_as_scd_type == "2":
        flow_arguments["track_history_except_column_list"] = TECHNICAL_METADATA_COLUMNS

    dp.create_auto_cdc_from_snapshot_flow(**flow_arguments)


for table_name, config in TABLE_CONFIGS.items():
    build_snapshot_flow(
        table_name=table_name,
        domain=config["domain"],
        keys=config["keys"],
        stored_as_scd_type=config["stored_as_scd_type"],
        schema_hints=config["hints"],
    )
"""Card-domain Bronze CDC pipeline.

This is the Card-only equivalent of ``bronze_layer.py`` for the personal
workspace POC. It keeps CDC in Bronze, choosing the storage model from the
business contract for each table:

* mutable entities use SCD Type 2 snapshots;
* immutable facts use SCD Type 1 snapshots;
* immutable status events use Auto Loader and SCD Type 1 deduplication.

The source is a Unity Catalog volume in Free Edition.  In a workspace with an
external location, set ``pipeline.source_root`` to the external snapshot root
instead; the source layout is otherwise unchanged.
"""

from __future__ import annotations

from typing import Any, Optional

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


DOMAIN = "card"
CATALOG = spark.conf.get("pipeline.target_catalog")
BRONZE_SCHEMA = spark.conf.get("pipeline.target_schema")
SOURCE_ROOT = spark.conf.get("pipeline.source_root").rstrip("/")
SIMULATION_ID = spark.conf.get("pipeline.simulation_id")

TABLE_CONFIGS: dict[str, dict[str, Any]] = {
    # Mutable entities: retain only real business changes as SCD2 history.
    "card": {"keys": ["card_id"], "source_type": "snapshot", "scd_type": "2"},
    "card_fraud_flag": {
        "keys": ["flag_id"],
        "source_type": "snapshot",
        "scd_type": "2",
    },
    # These tables are already immutable facts/history records. Snapshot CDC
    # provides idempotent upsert semantics without adding a second history.
    "card_transaction": {
        "keys": ["card_txn_id"],
        "source_type": "snapshot",
        "scd_type": "1",
    },
    "card_limit_history": {
        "keys": ["history_id"],
        "source_type": "snapshot",
        "scd_type": "1",
    },
    # Each status transition has its own immutable event ID. Daily full
    # snapshots repeat old events, so this uses SCD1 upsert/deduplication.
    "card_transaction_status_event": {
        "keys": ["status_event_id"],
        "source_type": "event_stream",
        "scd_type": "1",
    },
}

# These describe delivery and processing, not a change to the banking entity.
# They are retained in Bronze for lineage/audit, but must not open an SCD2
# version when the same source business row is present in the next snapshot.
TECHNICAL_METADATA_COLUMNS = [
    "business_date",
    "domain",
    "simulation_id",
    "cdc_change_op",
    "cdc_change_sequence",
    "cdc_change_time",
    "LOAD_DTTM",
    "EXTRACT_DTTM",
    "EXTRACT_DTE",
]

configured_tables = tuple(
    name.strip()
    for name in spark.conf.get("pipeline.card_tables").split(",")
    if name.strip()
)
if len(configured_tables) != len(TABLE_CONFIGS) or set(configured_tables) != set(
    TABLE_CONFIGS
):
    raise ValueError(
        "pipeline.card_tables must list exactly: " + ",".join(TABLE_CONFIGS)
    )

try:
    AVAILABLE_BUSINESS_DATES = sorted(
        int(value.strip())
        for value in spark.conf.get("pipeline.available_business_dates").split(",")
        if value.strip()
    )
except Exception as error:
    raise ValueError(
        "pipeline.available_business_dates must be a comma-separated list "
        "such as 20260705,20260706"
    ) from error


# ==================
# Returns the configured Bronze target table name.
# ==================
def _target_name(table_name: str) -> str:
    return f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"


# ==================
# Returns the Auto Loader path for one event table.
# ==================
def _source_table_path(table_name: str) -> str:
    return (
        f"{SOURCE_ROOT}/simulation_id={SIMULATION_ID}/snapshot_type=full/"
        f"business_date=*/{DOMAIN}/{table_name}"
    )


# ==================
# Returns the source directory for one daily full snapshot.
# ==================
def _snapshot_path(table_name: str, business_date: str) -> str:
    return (
        f"{SOURCE_ROOT}/simulation_id={SIMULATION_ID}/snapshot_type=full/"
        f"business_date={business_date}/{DOMAIN}/{table_name}"
    )


# ==================
# Adds source lineage, audit, and CDC metadata to Bronze rows.
# ==================
def _add_enterprise_cdc_metadata(
    df: DataFrame,
    *,
    business_date: Optional[str] = None,
) -> DataFrame:
    """Attach the same business date, audit, and CDC fields as bronze_layer."""

    source_path = F.col("_metadata.file_path")
    path_business_date = F.regexp_extract(
        source_path, r"business_date=(\d{4}-\d{2}-\d{2})", 1
    )
    final_business_date = (
        F.coalesce(F.to_date(path_business_date), F.to_date(F.lit(business_date)))
        if business_date
        else F.to_date(path_business_date)
    )
    path_simulation_id = F.regexp_extract(source_path, r"/simulation_id=([^/]+)/", 1)
    now = F.current_timestamp()

    return (
        df.withColumn("business_date", final_business_date)
        .withColumn("domain", F.lit(DOMAIN))
        .withColumn(
            "simulation_id",
            F.coalesce(F.nullif(path_simulation_id, F.lit("")), F.lit(SIMULATION_ID)),
        )
        .withColumn("cdc_change_op", F.lit(1).cast("int"))
        .withColumn(
            "cdc_change_sequence",
            F.concat_ws(
                "_",
                F.date_format(F.col("business_date"), "yyyyMMdd"),
                F.expr("uuid()"),
            ),
        )
        .withColumn("cdc_change_time", F.to_timestamp(F.col("business_date")))
        .withColumn("LOAD_DTTM", now)
        .withColumn("EXTRACT_DTTM", now)
        .withColumn("EXTRACT_DTE", F.to_date(now))
    )


# ==================
# Creates the correct CDC flow for the table's business behaviour.
# ==================
def _create_card_cdc_flow(
    table_name: str, *, keys: list[str], source_type: str, scd_type: str
) -> None:
    target = _target_name(table_name)
    dp.create_streaming_table(
        name=target,
        table_properties={
            "delta.enableChangeDataFeed": "true",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true",
            "pipelines.schemaEvolutionMode": "addNewColumns",
            "delta.enableTypeWidening": "true",
        },
    )

    if source_type == "event_stream":
        view_name = f"v_{table_name}_stream"

        # ==================
        # Reads new status-event files and preserves schema-drift data.
        # ==================
        @dp.view(name=view_name)
        def stream_view() -> DataFrame:
            raw_df = (
                spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "parquet")
                .option("cloudFiles.schemaEvolutionMode", "addNewColumnsWithTypeWidening")
                .option("cloudFiles.partitionColumns", "business_date")
                .option("cloudFiles.useStrictGlobber", "true")
                .option("cloudFiles.rescuedDataColumn", "_rescued_data")
                .load(_source_table_path(table_name))
            )
            return _add_enterprise_cdc_metadata(raw_df)

        dp.create_auto_cdc_flow(
            target=target,
            source=view_name,
            keys=keys,
            # Do not use snapshot date or generated UUIDs here. The source
            # contract defines the business ordering and supports late arrival.
            sequence_by=F.struct(
                F.col("status_timestamp"),
                F.col("sequence_number"),
                F.col("business_date"),
                F.col("status_event_id"),
            ),
            stored_as_scd_type=scd_type,
        )
        return

    # ==================
    # Returns the next available daily snapshot, or stops when it is not staged.
    # ==================
    def next_snapshot_and_version(
        latest_version: Optional[int],
    ) -> Optional[tuple[DataFrame, int]]:
        remaining_versions = (
            AVAILABLE_BUSINESS_DATES
            if latest_version is None
            else [date for date in AVAILABLE_BUSINESS_DATES if date > latest_version]
        )
        if not remaining_versions:
            return None

        next_version = remaining_versions[0]
        value = str(next_version)
        date_formatted = f"{value[:4]}-{value[4:6]}-{value[6:]}"
        try:
            snapshot_df = spark.read.option("mergeSchema", "true").parquet(
                _snapshot_path(table_name, date_formatted)
            )
        except Exception as error:
            # The POC stages one complete business-date directory at a time.
            # Reaching the next configured date before it lands is normal and
            # tells AUTO CDC FROM SNAPSHOT to end this triggered update.
            if "PATH_NOT_FOUND" in str(error) or "not found" in str(error).lower():
                print(f"[INFO] Snapshot {date_formatted} is not available yet.")
                return None
            raise
        return (
            _add_enterprise_cdc_metadata(snapshot_df, business_date=date_formatted),
            next_version,
        )

    flow_args: dict[str, Any] = {
        "target": target,
        "source": next_snapshot_and_version,
        "keys": keys,
        "stored_as_scd_type": scd_type,
    }
    if scd_type == "2":
        flow_args["track_history_except_column_list"] = TECHNICAL_METADATA_COLUMNS
    dp.create_auto_cdc_from_snapshot_flow(**flow_args)


for _table_name, _config in TABLE_CONFIGS.items():
    _create_card_cdc_flow(
        _table_name,
        keys=_config["keys"],
        source_type=_config["source_type"],
        scd_type=_config["scd_type"],
    )

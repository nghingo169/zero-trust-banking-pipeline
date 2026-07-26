"""Card Bronze: SCD2 snapshots and append-only immutable history."""

import sys
from typing import Optional

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, functions as F
DOMAIN_PATH = spark.conf.get("pipeline.domain_path")
if DOMAIN_PATH not in sys.path: sys.path.insert(0, DOMAIN_PATH)
from data_contracts.table_catalog import DOMAINS

DOMAIN = "card"
CATALOG = spark.conf.get("pipeline.target_catalog")
BRONZE_SCHEMA = spark.conf.get("pipeline.target_schema")
SOURCE_ROOT = spark.conf.get("pipeline.source_root").rstrip("/")
SIMULATION_ID = spark.conf.get("pipeline.simulation_id")

# Business entities are compared across complete daily snapshots.
SCD2_SNAPSHOT_TABLES = DOMAINS["card"]["scd2"]
# These rows are immutable facts/events; later full snapshots replay them.
APPEND_ONLY_TABLES = DOMAINS["card"]["append"]
TECHNICAL_METADATA_COLUMNS = [
    "business_date", "domain", "simulation_id", "cdc_change_op",
    "cdc_change_sequence", "cdc_change_time", "LOAD_DTTM", "EXTRACT_DTTM",
    "EXTRACT_DTE",
]
DATES = tuple(sorted(int(value.strip()) for value in spark.conf.get(
    "pipeline.available_business_dates").split(",") if value.strip()))


def target(name: str) -> str:
    return f"{CATALOG}.{BRONZE_SCHEMA}.{name}"


def source_path(name: str, business_date: str = "*") -> str:
    return (
        f"{SOURCE_ROOT}/simulation_id={SIMULATION_ID}/snapshot_type=full/"
        f"business_date={business_date}/{DOMAIN}/{name}"
    )


def normalize(df: DataFrame, name: str) -> DataFrame:
    if name == "card_limit_history" and "limit_amount" in df.columns:
        return df.withColumn("limit_amount", F.col("limit_amount").cast("decimal(12,2)"))
    return df


def metadata(df: DataFrame, business_date: Optional[str] = None) -> DataFrame:
    from_path = F.regexp_extract(
        F.col("_metadata.file_path"), r"business_date=(\d{4}-\d{2}-\d{2})", 1
    )
    date = F.coalesce(F.to_date(from_path), F.to_date(F.lit(business_date)))
    now = F.current_timestamp()
    return (
        df.withColumn("business_date", date)
        .withColumn("domain", F.lit(DOMAIN))
        .withColumn("simulation_id", F.lit(SIMULATION_ID))
        .withColumn("cdc_change_op", F.lit(1).cast("int"))
        .withColumn("cdc_change_sequence", F.concat_ws(
            "_", F.date_format(F.col("business_date"), "yyyyMMdd"), F.expr("uuid()")
        ))
        .withColumn("cdc_change_time", F.to_timestamp("business_date"))
        .withColumn("LOAD_DTTM", now).withColumn("EXTRACT_DTTM", now)
        .withColumn("EXTRACT_DTE", F.to_date(now))
    )


for name, key in APPEND_ONLY_TABLES.items():
    dp.create_streaming_table(
        name=target(name),
        table_properties={
            "delta.enableChangeDataFeed": "true",
            "pipelines.schemaEvolutionMode": "addNewColumns",
            "delta.enableTypeWidening": "true",
        },
    )

    @dp.append_flow(target=target(name), name=f"{name}_immutable_history")
    def immutable_history(n=name, k=key):
        return metadata(
            normalize(
                spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "parquet")
                .option("cloudFiles.schemaEvolutionMode", "addNewColumnsWithTypeWidening")
                .option("cloudFiles.partitionColumns", "business_date")
                .option("cloudFiles.rescuedDataColumn", "_rescued_data")
                .load(source_path(n)),
                n,
            )
        ).dropDuplicates([k])


for name, key in SCD2_SNAPSHOT_TABLES.items():
    dp.create_streaming_table(
        name=target(name),
        table_properties={
            "delta.enableChangeDataFeed": "true",
            "pipelines.schemaEvolutionMode": "addNewColumns",
            "delta.enableTypeWidening": "true",
        },
    )

    def next_snapshot(version: Optional[int], n=name):
        remaining = DATES if version is None else [d for d in DATES if d > version]
        if not remaining:
            return None
        value = remaining[0]
        date = f"{str(value)[:4]}-{str(value)[4:6]}-{str(value)[6:]}"
        try:
            df = spark.read.option("mergeSchema", "true").parquet(source_path(n, date))
        except Exception as error:
            if "PATH_NOT_FOUND" in str(error) or "not found" in str(error).lower():
                return None
            raise
        return metadata(normalize(df, n), date), value

    dp.create_auto_cdc_from_snapshot_flow(
        target=target(name), source=next_snapshot, keys=[key], stored_as_scd_type="2",
        track_history_except_column_list=TECHNICAL_METADATA_COLUMNS,
    )

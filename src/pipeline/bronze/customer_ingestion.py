"""Isolated Customer-master Bronze CDC pipeline for the six-day replay."""
from __future__ import annotations

import sys
from typing import Any, Optional
from pyspark import pipelines as dp
from pyspark.sql import DataFrame, functions as F
DOMAIN_PATH = spark.conf.get("pipeline.domain_path")
if DOMAIN_PATH not in sys.path: sys.path.insert(0, DOMAIN_PATH)
from data_contracts.table_catalog import DOMAINS

DOMAIN = "customer_master"
CATALOG = spark.conf.get("pipeline.target_catalog")
SCHEMA = spark.conf.get("pipeline.target_schema")
ROOT = spark.conf.get("pipeline.source_root").rstrip("/")
SIMULATION_ID = spark.conf.get("pipeline.simulation_id")
TABLES: dict[str, dict[str, Any]] = {name: {"keys": [key]} for name, key in DOMAINS["customer"]["scd2"].items()}
TECHNICAL = ["business_date", "domain", "simulation_id", "cdc_change_op", "cdc_change_sequence", "cdc_change_time", "LOAD_DTTM", "EXTRACT_DTTM", "EXTRACT_DTE"]
configured = {v.strip() for v in spark.conf.get("pipeline.customer_tables").split(",") if v.strip()}
if configured != set(TABLES):
    raise ValueError("pipeline.customer_tables must list exactly: " + ",".join(TABLES))
DATES = tuple(sorted(int(v.strip()) for v in spark.conf.get("pipeline.available_business_dates").split(",") if v.strip()))

def path(table: str, date: str = "*") -> str:
    return f"{ROOT}/simulation_id={SIMULATION_ID}/snapshot_type=full/business_date={date}/{DOMAIN}/{table}"

def metadata(df: DataFrame, date: Optional[str] = None) -> DataFrame:
    file_date = F.regexp_extract(F.col("_metadata.file_path"), r"business_date=(\d{4}-\d{2}-\d{2})", 1)
    business_date = F.coalesce(F.to_date(file_date), F.to_date(F.lit(date))) if date else F.to_date(file_date)
    now = F.current_timestamp()
    return (df.withColumn("business_date", business_date).withColumn("domain", F.lit(DOMAIN))
        .withColumn("simulation_id", F.lit(SIMULATION_ID)).withColumn("cdc_change_op", F.lit(1).cast("int"))
        .withColumn("cdc_change_sequence", F.concat_ws("_", F.date_format(F.col("business_date"), "yyyyMMdd"), F.expr("uuid()")))
        .withColumn("cdc_change_time", F.to_timestamp("business_date")).withColumn("LOAD_DTTM", now)
        .withColumn("EXTRACT_DTTM", now).withColumn("EXTRACT_DTE", F.to_date(now)))

def register(name: str, config: dict[str, Any]) -> None:
    target = f"{CATALOG}.{SCHEMA}.{name}"
    dp.create_streaming_table(name=target, table_properties={"delta.enableChangeDataFeed": "true", "pipelines.schemaEvolutionMode": "addNewColumns", "delta.enableTypeWidening": "true"})
    def next_snapshot(version: Optional[int]) -> Optional[tuple[DataFrame, int]]:
        remaining = DATES if version is None else tuple(d for d in DATES if d > version)
        if not remaining: return None
        next_version = remaining[0]; value = str(next_version); formatted = f"{value[:4]}-{value[4:6]}-{value[6:]}"
        try: df = spark.read.option("mergeSchema", "true").parquet(path(name, formatted))
        except Exception as error:
            if "PATH_NOT_FOUND" in str(error) or "not found" in str(error).lower(): return None
            raise
        return metadata(df, formatted), next_version
    dp.create_auto_cdc_from_snapshot_flow(target=target, source=next_snapshot, keys=config["keys"], stored_as_scd_type="2", track_history_except_column_list=TECHNICAL)

for table_name, table_config in TABLES.items(): register(table_name, table_config)

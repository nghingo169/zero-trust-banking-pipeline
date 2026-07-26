"""Financial Crime Bronze SCD2 snapshot ingestion with schema normalization."""

import sys
from typing import Optional

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, functions as F
DOMAIN_PATH = spark.conf.get("pipeline.domain_path")
if DOMAIN_PATH not in sys.path: sys.path.insert(0, DOMAIN_PATH)
from data_contracts.table_catalog import DOMAINS

CAT = spark.conf.get("pipeline.target_catalog")
SCHEMA = spark.conf.get("pipeline.target_schema")
ROOT = spark.conf.get("pipeline.source_root").rstrip("/")
SIM = spark.conf.get("pipeline.simulation_id")
DOMAIN = "financial_crime"

# Financial Crime has no corresponding status-event tables in this development dataset, so
# every business table is compared as an SCD2 daily snapshot.
SCD2_SNAPSHOT_TABLES = DOMAINS["fincrime"]["scd2"]
TECHNICAL = ["business_date", "domain", "simulation_id"]
DATES = tuple(sorted(int(x) for x in spark.conf.get(
    "pipeline.available_business_dates").split(",") if x.strip()))


def path(name: str, business_date: str) -> str:
    return (
        f"{ROOT}/simulation_id={SIM}/snapshot_type=full/business_date={business_date}/"
        f"{DOMAIN}/{name}"
    )


def target(name: str) -> str:
    return f"{CAT}.{SCHEMA}.{name}"


def normalize(df: DataFrame, name: str, date: str) -> DataFrame:
    # The source's score representation changed across snapshots.  Preserve the
    # contract's fixed-scale probability value before Auto CDC compares versions.
    if name == "account_transaction_risk_score" and "model_score" in df.columns:
        df = df.withColumn("model_score", F.col("model_score").cast("decimal(6,4)"))
    if name == "call_center_log" and "call_duration_seconds" in df.columns:
        df = df.withColumn("call_duration_seconds", F.col("call_duration_seconds").cast("double"))
    if name == "investigation_note" and "source_arrival_timestamp" in df.columns:
        df = df.withColumn("source_arrival_timestamp", F.to_timestamp("source_arrival_timestamp"))
    return (
        df.withColumn("business_date", F.to_date(F.lit(date)))
        .withColumn("domain", F.lit(DOMAIN))
        .withColumn("simulation_id", F.lit(SIM))
    )


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
        rendered = str(value)
        date = f"{rendered[:4]}-{rendered[4:6]}-{rendered[6:]}"
        try:
            df = spark.read.option("mergeSchema", "true").parquet(path(n, date))
        except Exception as error:
            if "PATH_NOT_FOUND" in str(error) or "not found" in str(error).lower():
                return None
            raise
        return normalize(df, n, date), value

    dp.create_auto_cdc_from_snapshot_flow(
        target=target(name), source=next_snapshot, keys=[key], stored_as_scd_type="2",
        track_history_except_column_list=TECHNICAL,
    )

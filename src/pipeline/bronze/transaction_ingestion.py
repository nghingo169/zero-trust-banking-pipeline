"""Transaction Bronze: SCD2 snapshots plus immutable event history.

Daily full snapshots are compared for non-event tables, preserving SCD2
versions when business attributes change. Status events are different: each
``status_event_id`` is an immutable fact, so the append flow retains its first
arrival and drops copies replayed in later daily snapshots.
"""

import sys
from pyspark import pipelines as dp
from pyspark.sql import functions as F
DOMAIN_PATH = spark.conf.get("pipeline.domain_path")
if DOMAIN_PATH not in sys.path: sys.path.insert(0, DOMAIN_PATH)
from data_contracts.table_catalog import DOMAINS

CAT = spark.conf.get("pipeline.target_catalog")
SCHEMA = spark.conf.get("pipeline.target_schema")
ROOT = spark.conf.get("pipeline.source_root").rstrip("/")
SIM = spark.conf.get("pipeline.simulation_id")
DOMAIN = "customer_transaction"
EVENT_TABLES = DOMAINS["transaction"]["append"]
SNAPSHOT_TABLES = DOMAINS["transaction"]["scd2"]
DATES = tuple(
    sorted(
        int(x) for x in spark.conf.get("pipeline.available_business_dates").split(",")
    )
)


def p(name, date="*"):
    return f"{ROOT}/simulation_id={SIM}/snapshot_type=full/business_date={date}/{DOMAIN}/{name}"


def meta(df, date=None):
    d = F.coalesce(
        F.to_date(
            F.regexp_extract(
                F.col("_metadata.file_path"), r"business_date=(\d{4}-\d{2}-\d{2})", 1
            )
        ),
        F.to_date(F.lit(date)),
    )
    return (
        df.withColumn("business_date", d)
        .withColumn("domain", F.lit(DOMAIN))
        .withColumn("simulation_id", F.lit(SIM))
        .withColumn("cdc_change_time", F.to_timestamp("business_date"))
    )


def target(name):
    return f"{CAT}.{SCHEMA}.{name}"


for name, key in EVENT_TABLES.items():
    dp.create_streaming_table(
        name=target(name),
        table_properties={
            "delta.enableChangeDataFeed": "true",
            "pipelines.schemaEvolutionMode": "addNewColumns",
        },
    )
    @dp.append_flow(target=target(name), name=f"{name}_event_history")
    def event_history(n=name, k=key):
        return meta(
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumnsWithTypeWidening")
            .option("cloudFiles.partitionColumns", "business_date")
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            .load(p(n))
        ).dropDuplicates([k])


for name, key in SNAPSHOT_TABLES.items():
    dp.create_streaming_table(
        name=target(name),
        table_properties={
            "delta.enableChangeDataFeed": "true",
            "pipelines.schemaEvolutionMode": "addNewColumns",
        },
    )

    def next_snapshot(version, n=name):
        dates = DATES if version is None else [d for d in DATES if d > version]
        if not dates:
            return None
        v = dates[0]
        s = str(v)
        d = f"{s[:4]}-{s[4:6]}-{s[6:]}"
        try:
            return meta(spark.read.option("mergeSchema", "true").parquet(p(n, d)), d), v
        except Exception as e:
            if "not found" in str(e).lower() or "PATH_NOT_FOUND" in str(e):
                return None
            raise

    dp.create_auto_cdc_from_snapshot_flow(
        target=target(name),
        source=next_snapshot,
        keys=[key],
        stored_as_scd_type="2",
        track_history_except_column_list=[
            "business_date",
            "domain",
            "simulation_id",
            "cdc_change_time",
        ],
    )

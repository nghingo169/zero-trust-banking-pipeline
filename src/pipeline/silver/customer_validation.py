"""Customer-master clean Silver, quarantine, and duplicate-national-ID handling."""

from __future__ import annotations
import sys
from pyspark import pipelines as dp
from pyspark.sql import DataFrame, functions as F

RULE_PATH = spark.conf.get("pipeline.quality_rules_path")
if RULE_PATH not in sys.path:
    sys.path.insert(0, RULE_PATH)
from data_contracts.quality_rules.registry import get_rules
from data_contracts.table_catalog import DOMAINS

CATALOG = spark.conf.get("pipeline.catalog")
BRONZE = spark.conf.get("pipeline.bronze_schema")
QUARANTINE = spark.conf.get("pipeline.quarantine_schema")
TABLES = DOMAINS["customer"]["scd2"]


def bronze(name: str) -> str:
    return f"{CATALOG}.{BRONZE}.{name}"


def quarantine(name: str) -> str:
    return f"{CATALOG}.{QUARANTINE}.{name}"


def current(name: str) -> DataFrame:
    return all_versions(name).filter("__END_AT IS NULL")


def all_versions(name: str) -> DataFrame:
    """Return the complete Bronze SCD2 history for Silver retention."""
    return spark.read.table(bronze(name))


def duplicate_ids() -> DataFrame:
    return (
        current("core_banking_customer")
        .filter("national_id IS NOT NULL")
        .groupBy("national_id")
        .count()
        .filter("count > 1")
        .select("national_id")
    )


def with_validation_metadata(df: DataFrame, name: str) -> DataFrame:
    rules = get_rules(name)
    failed = F.concat_ws(
        ",",
        *[
            F.when(~F.coalesce(F.expr(rule), F.lit(False)), F.lit(label))
            for label, rule in rules.items()
        ],
    )
    if name == "core_banking_customer":
        df = df.join(
            F.broadcast(
                duplicate_ids().withColumn("_duplicate_national_id", F.lit(True))
            ),
            "national_id",
            "left",
        )
        failed = F.concat_ws(
            ",",
            failed,
            F.when(
                F.col("_duplicate_national_id") == True,
                F.lit("core_banking_customer__national_id__duplicate"),
            ),
        )
    return (
        df.withColumn("validation_business_date", F.col("business_date"))
        .withColumn("source_table", F.lit(name))
        .withColumn("failed_rules", failed)
        .withColumn("is_quarantined", F.col("failed_rules") != "")
        .drop("_duplicate_national_id")
    )


def validated(name: str) -> DataFrame:
    return with_validation_metadata(all_versions(name), name)


def quarantine_changes(name: str) -> DataFrame:
    changes = (
        spark.readStream.option("readChangeFeed", "true")
        .table(bronze(name))
        .filter("_change_type IN ('insert', 'update_postimage')")
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )
    return with_validation_metadata(changes, name).filter("is_quarantined")


def register(name: str) -> None:
    rules = get_rules(name)
    validation = f"validation_{name}"

    @dp.table(name=validation, temporary=True)
    @dp.expect_all(rules)
    def validation_table() -> DataFrame:
        return validated(name)

    @dp.table(name=name)
    def clean_table() -> DataFrame:
        return (
            spark.read.table(validation)
            .filter("NOT is_quarantined")
            .drop("is_quarantined", "failed_rules", "source_table")
        )

    @dp.table(name=quarantine(name))
    def quarantine_table() -> DataFrame:
        return quarantine_changes(name)


@dp.table(name="duplicate_national_id_monitor")
def duplicate_national_id_monitor() -> DataFrame:
    return (
        current("core_banking_customer")
        .join(duplicate_ids(), "national_id")
        .select(
            F.lit("core_banking_customer").alias("source_table"),
            "national_id",
            "cust_no",
            "business_date",
        )
    )


for name in TABLES:
    register(name)

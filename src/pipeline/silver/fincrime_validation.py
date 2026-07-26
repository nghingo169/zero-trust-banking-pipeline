"""Financial Crime current Silver, CDF quarantine history, and monitors."""

import sys

from pyspark import pipelines as dp
from pyspark.sql import functions as F

RULE_PATH = spark.conf.get("pipeline.quality_rules_path")
if RULE_PATH not in sys.path:
    sys.path.insert(0, RULE_PATH)
from data_contracts.quality_rules.registry import get_rules
from data_contracts.table_catalog import DOMAINS

CAT = spark.conf.get("pipeline.catalog")
BRONZE = spark.conf.get("pipeline.bronze_schema")
QUAR = spark.conf.get("pipeline.quarantine_schema")
TABLES = DOMAINS["fincrime"]["scd2"]
QUALITY_RULE_TABLES = {
    "fraud_alert", "transaction_monitoring_alert",
    "transaction_monitoring_alert_account_transaction",
    "transaction_monitoring_alert_card_transaction",
    "investigation_case_transaction_monitoring_alert", "aml_case",
    "sanction_screening", "suspicious_activity_report",
    "account_transaction_risk_score", "chargeback",
    "investigation_case_account_transaction", "investigation_case_card_transaction",
    "investigation_case_fraud_alert", "investigation_case_sanction_screening",
}


def b(name):
    return f"{CAT}.{BRONZE}.{name}"


def q(name):
    return f"{CAT}.{QUAR}.{name}"


def current(name):
    return all_versions(name).filter("__END_AT IS NULL")


def all_versions(name):
    """Return the complete Bronze SCD2 history for Silver retention."""
    return spark.read.table(b(name))


def with_validation_metadata(df, name, rules):
    failed = (
        F.concat_ws(
            ",", *[F.when(~F.coalesce(F.expr(expr), F.lit(False)), F.lit(label))
                   for label, expr in rules.items()]
        ) if rules else F.lit("")
    )
    return (
        df.withColumn("validation_business_date", F.col("business_date"))
        .withColumn("source_table", F.lit(name))
        .withColumn("failed_rules", failed)
        .withColumn("is_quarantined", failed != "")
    )


def quarantine_changes(name, rules):
    changes = (
        spark.readStream.option("readChangeFeed", "true").table(b(name))
        .filter("_change_type IN ('insert', 'update_postimage')")
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )
    return with_validation_metadata(changes, name, rules).filter("is_quarantined")


def register(name):
    validation = f"validation_{name}"
    rules = get_rules(name) if name in QUALITY_RULE_TABLES else {}

    if rules:
        @dp.table(name=validation, temporary=True)
        @dp.expect_all(rules)
        def validated(n=name, r=rules):
            return with_validation_metadata(all_versions(n), n, r)
    else:
        @dp.table(name=validation, temporary=True)
        def validated(n=name, r=rules):
            return with_validation_metadata(all_versions(n), n, r)

    @dp.table(name=name)
    def clean(v=validation):
        return spark.read.table(v).filter("NOT is_quarantined").drop(
            "is_quarantined", "failed_rules", "source_table"
        )

    @dp.table(name=q(name))
    def quarantine(n=name, r=rules):
        return quarantine_changes(n, r)


for table_name in TABLES:
    register(table_name)


@dp.table(name="stale_watchlist_monitor")
def stale_watchlist_monitor():
    return current("watchlist").filter("added_date < current_date() - INTERVAL 3650 DAYS")

"""Bronze-to-validated-Silver pipeline with one centralized quarantine output.

Each Bronze CDF source is normalized and assessed once in a temporary pipeline
view. The assessment branches into its validated Silver streaming table or the
shared governance quarantine streaming table. No second pipeline re-evaluates
the rules.
"""

from __future__ import annotations

import sys
from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

RULE_PATH = spark.conf.get("pipeline.quality_rules_path")
if RULE_PATH not in sys.path:
    sys.path.insert(0, RULE_PATH)

from data_contracts.table_catalog import DOMAINS, tables
from pipeline.silver import (card_validation, customer_validation,
                             fincrime_validation, transaction_validation)

CATALOG = spark.conf.get("pipeline.catalog")
VALIDATED_SCHEMA = spark.conf.get("pipeline.validated_schema")
GOVERNANCE_SCHEMA = spark.conf.get("pipeline.governance_schema")
BRONZE_SCHEMA = spark.conf.get("pipeline.bronze_schema")


def qualified(schema: str, table_name: str) -> str:
    return f"{CATALOG}.{schema}.{table_name}"


def bronze(domain: str, table_name: str) -> str:
    return qualified(BRONZE_SCHEMA, table_name)


def _column_or_null(df: DataFrame, column_name: str, data_type: str):
    if column_name in df.columns:
        return F.col(column_name).cast(data_type)
    return F.lit(None).cast(data_type)


def duplicate_active_business_keys(
    domain: str, table_name: str, business_key: str
) -> DataFrame | None:
    """Find invalid duplicate *current* SCD2 versions, never history versions."""

    if table_name not in DOMAINS[domain]["scd2"]:
        return None
    return (
        spark.read.table(bronze(domain, table_name))
        .filter("__END_AT IS NULL")
        .groupBy(business_key)
        .count()
        .filter("count > 1")
        .select(business_key)
    )


def active_scd2_versions(
    domain: str, table_name: str, business_key: str
) -> DataFrame | None:
    """Return the current physical version for each SCD2 business key.

    CDF retains the original insert for a version even after a later update
    closes it. Filtering the CDF record alone on ``__END_AT IS NULL`` would
    therefore still re-quarantine superseded history. The current Bronze
    snapshot provides the authoritative active ``__START_AT`` per key.
    """

    if table_name not in DOMAINS[domain]["scd2"]:
        return None
    return (
        spark.read.table(bronze(domain, table_name))
        .filter("__END_AT IS NULL")
        .select(
            F.col(business_key),
            F.col("__START_AT").alias("_active_version_start_at"),
        )
    )


def assess(domain: str, table_name: str) -> DataFrame:
    """Normalize a source and attach the single authoritative rule outcome."""

    raw_changes = (
        spark.readStream.option("readChangeFeed", "true")
        .table(bronze(domain, table_name))
        .filter("_change_type IN ('insert', 'update_postimage')")
    )
    # Preserve the entire Bronze change before normalization or rule helper
    # columns are added.  The quarantine contract has one JSON payload field;
    # this guarantees every rejected row remains reproducible, including any
    # Bronze `_rescued_data` value and CDF provenance columns.
    raw_changes = raw_changes.withColumn(
        "_quarantine_payload_json",
        F.to_json(F.struct(*[F.col(column) for column in raw_changes.columns])),
    )
    if domain == "card":
        df = card_validation.assess(raw_changes, table_name)
    elif domain == "customer":
        df = customer_validation.assess(
            raw_changes,
            table_name,
            customer_validation.duplicate_national_ids(
                spark, bronze(domain, table_name)
            ),
        )
    elif domain == "transaction":
        df = transaction_validation.assess(raw_changes, table_name)
    else:
        df = fincrime_validation.assess(raw_changes, table_name)

    business_key = tables(domain)[table_name]
    duplicate_keys = duplicate_active_business_keys(domain, table_name, business_key)
    if duplicate_keys is not None:
        df = df.join(
            duplicate_keys.withColumn("_duplicate_active_business_key", F.lit(True)),
            business_key,
            "left",
        ).withColumn(
            "_failed_rule_names",
            F.concat(
                F.col("_failed_rule_names"),
                F.when(
                    F.col("_duplicate_active_business_key")
                    & F.col("__END_AT").isNull(),
                    F.array(F.lit(f"{table_name}__{business_key}__duplicate_active")),
                ).otherwise(F.expr("CAST(array() AS ARRAY<STRING>)")),
            ),
        )

    return (
        df.withColumn("validation_business_date", F.col("business_date"))
        .withColumn(
            "_rescued_data_json", _column_or_null(df, "_rescued_data", "string")
        )
        .withColumn(
            "is_quarantined",
            (F.size("_failed_rule_names") > 0)
            | (
                F.col("_rescued_data_json").isNotNull()
                & (F.length("_rescued_data_json") > 0)
            ),
        )
        .drop("_duplicate_national_id", "_duplicate_active_business_key")
    )


ASSESSMENT_VIEWS: dict[tuple[str, str], str] = {}

# Status-event IDs are reused by the source.  Their business identity is the
# composite key used by Bronze SCD1 ingestion, and quarantine lineage must use
# the same identity rather than a colliding status_event_id alone.
EVENT_KEY_COLUMNS = {
    "account_transaction_status_event": ["status_event_id", "account_txn_id"],
    "atm_transaction_status_event": ["status_event_id", "account_txn_id"],
    "payment_gateway_status_event": [
        "status_event_id",
        "gateway_txn_id",
        "event_parent_ref",
    ],
    "card_transaction_status_event": ["status_event_id", "card_txn_id"],
}


def register_assessment(domain: str, table_name: str) -> None:
    view_name = f"_assessment__{domain}__{table_name}"
    ASSESSMENT_VIEWS[(domain, table_name)] = view_name

    @dp.temporary_view(name=view_name)
    def assessment(domain=domain, table_name=table_name) -> DataFrame:
        return assess(domain, table_name)


def register_validated_output(domain: str, table_name: str) -> None:
    view_name = ASSESSMENT_VIEWS[(domain, table_name)]

    @dp.table(
        name=qualified(VALIDATED_SCHEMA, table_name),
        comment=f"Normalized, rule-passing records from Bronze {domain}.{table_name}.",
    )
    def validated_table(view_name=view_name) -> DataFrame:
        return (
            spark.readStream.table(view_name)
            .filter("NOT is_quarantined")
            .drop(
                "is_quarantined",
                "_failed_rule_names",
                "_rescued_data_json",
                "_quarantine_payload_json",
                "_change_type",
                "_commit_version",
                "_commit_timestamp",
                # Replay-run metadata belongs to Bronze provenance, not the
                # source-aligned validated Silver contract.
                "simulation_id",
                "event_parent_ref",
            )
        )


for domain in DOMAINS:
    for source_table_name in tables(domain):
        register_assessment(domain, source_table_name)

for domain in DOMAINS:
    for source_table_name in tables(domain):
        register_validated_output(domain, source_table_name)


def quarantine_events(domain: str, table_name: str, business_key: str) -> DataFrame:
    """Project each current failed source version into atomic DLQ records."""

    assessed = spark.readStream.table(ASSESSMENT_VIEWS[(domain, table_name)])
    current_versions = active_scd2_versions(domain, table_name, business_key)
    if current_versions is not None:
        assessed = (
            assessed.join(current_versions, business_key, "inner")
            .filter(F.col("__START_AT") == F.col("_active_version_start_at"))
            .drop("_active_version_start_at")
        )

    key_columns = EVENT_KEY_COLUMNS.get(table_name, [business_key])
    source_business_key = F.concat_ws(
        ":",
        *[
            F.coalesce(F.col(column).cast("string"), F.lit("<NULL>"))
            for column in key_columns
        ],
    )
    bronze_record_ref = F.concat(
        F.lit(f"{table_name}.{'+'.join(key_columns)}="), source_business_key
    )
    record_fingerprint = F.sha2(
        F.to_json(F.struct(*[F.col(column) for column in assessed.columns])), 256
    )

    # One output row per failed rule.  Rescued source content is an atomic
    # failure condition too, so it remains visible even where all rule checks
    # otherwise pass.
    failed = (
        assessed.filter(F.col("is_quarantined"))
        .withColumn(
            "_quarantine_failure_names",
            F.concat(
                F.col("_failed_rule_names"),
                F.when(
                    F.col("_rescued_data_json").isNotNull()
                    & (F.length("_rescued_data_json") > 0),
                    F.array(F.lit("RESCUED_DATA_PRESENT")),
                ).otherwise(F.expr("CAST(array() AS ARRAY<STRING>)")),
            ),
        )
        .withColumn("failed_rule_name", F.explode("_quarantine_failure_names"))
    )

    return failed.select(
        F.sha2(
            F.concat_ws(
                "|",
                F.lit(domain),
                F.lit(table_name),
                source_business_key,
                record_fingerprint,
                F.col("failed_rule_name"),
            ),
            256,
        ).alias("quarantine_key"),
        # Python Lakeflow code cannot read a job-run pipeline parameter.
        # The dependent audit task stamps its own RUN_ID immediately after
        # this successful pipeline update.
        F.lit(spark.conf.get("pipeline.run_id", None))
        .cast("string")
        .alias("pipeline_run_id"),
        F.lit(table_name).alias("source_table_name"),
        source_business_key.alias("source_business_key"),
        bronze_record_ref.alias("bronze_record_ref"),
        F.col("failed_rule_name"),
        F.when(
            F.col("failed_rule_name") == F.lit("RESCUED_DATA_PRESENT"),
            F.lit("RESCUED_DATA_PRESENT"),
        )
        .otherwise(F.lit("VALIDATION_RULE_FAILURE"))
        .alias("quarantine_reason"),
        # Preserve only the Bronze schema-rescue content in its dedicated
        # field. It is null for ordinary rule failures.
        F.col("_rescued_data_json").alias("rescued_data_json"),
        # Preserve the whole rejected Bronze record independently, so a
        # rule failure is fully reproducible even without `_rescued_data`.
        F.col("_quarantine_payload_json").alias("quarantine_data_payload"),
        F.current_timestamp().alias("quarantined_at"),
    )


@dp.table(
    name=qualified(GOVERNANCE_SCHEMA, "silver_quarantine_record"),
    comment="Centralized rejected Bronze records, produced by the source-bound validation flow.",
)
def silver_quarantine_record() -> DataFrame:
    frames = [
        quarantine_events(domain, table_name, business_key)
        for domain in DOMAINS
        for table_name, business_key in tables(domain).items()
    ]
    return reduce(lambda left, right: left.unionByName(right), frames)

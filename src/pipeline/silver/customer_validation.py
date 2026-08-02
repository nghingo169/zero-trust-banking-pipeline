"""Reusable Customer validation logic for the source-bound Silver pipeline."""

from pyspark.sql import DataFrame, functions as F

from data_contracts.normalization import normalize
from data_contracts.quality_rules.registry import get_rules
from data_contracts.table_catalog import DOMAINS


TABLES = {**DOMAINS["customer"]["scd2"], **DOMAINS["customer"]["append"]}
IDENTITY_TABLES = {"core_banking_customer", "crm_customer"}


def duplicate_national_ids(spark_session, source_table: str) -> DataFrame | None:
    """Return duplicate active national IDs for the two customer master sources."""

    table_name = source_table.rsplit(".", 1)[-1]
    if table_name not in IDENTITY_TABLES:
        return None
    return (
        normalize(spark_session.read.table(source_table))
        .filter("__END_AT IS NULL AND national_id IS NOT NULL")
        .groupBy("national_id")
        .count()
        .filter("count > 1")
        .select("national_id")
    )


def assess(
    df: DataFrame, table_name: str, duplicate_ids: DataFrame | None = None
) -> DataFrame:
    """Normalize a Customer source and attach its failed rule names."""

    normalized = normalize(df)
    rules = get_rules(table_name)
    failed_rule_names = F.filter(
        F.array(
            *[
                F.when(
                    ~F.coalesce(F.expr(rule), F.lit(False)), F.lit(name)
                )
                for name, rule in rules.items()
            ]
        ),
        lambda rule_name: rule_name.isNotNull(),
    )

    if duplicate_ids is not None:
        normalized = normalized.join(
            F.broadcast(duplicate_ids.withColumn("_duplicate_national_id", F.lit(True))),
            "national_id",
            "left",
        )
        failed_rule_names = F.concat(
            failed_rule_names,
            F.when(
                F.col("_duplicate_national_id"),
                F.array(F.lit(f"{table_name}__national_id__duplicate")),
            ).otherwise(F.expr("CAST(array() AS ARRAY<STRING>)")),
        )

    return normalized.withColumn("_failed_rule_names", failed_rule_names).drop(
        "_duplicate_national_id"
    )

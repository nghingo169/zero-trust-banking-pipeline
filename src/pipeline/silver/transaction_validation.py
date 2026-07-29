"""Reusable Transaction validation logic for the source-bound Silver pipeline."""

from pyspark.sql import DataFrame, functions as F

from data_contracts.normalization import normalize
from data_contracts.quality_rules.registry import RULES_BY_TABLE
from data_contracts.table_catalog import DOMAINS


TABLES = {**DOMAINS["transaction"]["scd2"], **DOMAINS["transaction"]["append"]}


def assess(df: DataFrame, table_name: str) -> DataFrame:
    """Normalize a Transaction source and attach its failed rule names."""

    rules = RULES_BY_TABLE.get(table_name, ())
    failed_rule_names = (
        F.filter(
            F.array(
                *[
                    F.when(
                        ~F.coalesce(F.expr(rule["constraint"]), F.lit(False)),
                        F.lit(rule["name"]),
                    )
                    for rule in rules
                ]
            ),
            lambda rule_name: rule_name.isNotNull(),
        )
        if rules
        else F.expr("CAST(array() AS ARRAY<STRING>)")
    )
    return normalize(df).withColumn("_failed_rule_names", failed_rule_names)

"""Reusable Card validation logic for the source-bound Silver pipeline."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from data_contracts.normalization import normalize
from data_contracts.quality_rules.registry import get_rules
from data_contracts.table_catalog import DOMAINS

TABLES = {**DOMAINS["card"]["scd2"], **DOMAINS["card"]["append"]}


def assess(df: DataFrame, table_name: str) -> DataFrame:
    """Normalize a Card source and attach its failed rule names."""

    rules = get_rules(table_name)
    failed_rule_names = F.filter(
        F.array(
            *[
                F.when(~F.coalesce(F.expr(rule), F.lit(False)), F.lit(name))
                for name, rule in rules.items()
            ]
        ),
        lambda rule_name: rule_name.isNotNull(),
    )
    return normalize(df).withColumn("_failed_rule_names", failed_rule_names)

"""Bronze-to-Silver Card quarantine flow.

This is the Databricks quarantine pattern applied to ``bronze.card.card``.
The same four functions can be copied for the other Card tables by changing
the source table and ``get_rules`` argument.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import expr

from data_contracts.quality_rules.registry import get_quarantine_condition, get_rules


@dp.view
def raw_card_data():
    return spark.readStream.table("bronze.card.card")


@dp.table(
    temporary=True,
    partition_cols=["is_quarantined"],
)
@dp.expect_all(get_rules("card"))
def card_data_quarantine():
    return spark.readStream.table("raw_card_data").withColumn(
        "is_quarantined",
        expr(get_quarantine_condition("card")),
    )


@dp.table
def silver_card():
    return spark.read.table("card_data_quarantine").filter("is_quarantined=false")


@dp.table
def card_quarantine():
    return spark.read.table("card_data_quarantine").filter("is_quarantined=true")

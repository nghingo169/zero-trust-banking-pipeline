"""Shared config and path helpers for the Gold (AI-Ready Context Layer) pipeline.

silver_ref()/gold_target_name() take `spark` explicitly instead of reading it
as a module-level global -- gold_common.py is imported (not directly exec'd
by the Lakeflow graph loader) by the 3 view files, so it does NOT get `spark`
injected into its own module namespace the way a top-level library file does.
"""


def get_catalog(spark) -> str:
    return spark.conf.get("pipeline.catalog", "workspace")


def get_silver_schema(spark) -> str:
    return spark.conf.get("pipeline.silver_schema", "silver")


def get_gold_schema(spark) -> str:
    return spark.conf.get("pipeline.gold_schema", "gold")


def silver_ref(spark, table_name: str) -> str:
    """Fully-qualified Silver source table."""
    return f"{get_catalog(spark)}.{get_silver_schema(spark)}.{table_name}"


def gold_target_name(spark, table_name: str) -> str:
    """Target name for a Gold view/table (schema-qualified if GOLD_SCHEMA is set)."""
    gold_schema = get_gold_schema(spark)
    return f"{gold_schema}.{table_name}" if gold_schema else table_name
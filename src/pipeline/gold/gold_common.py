"""Shared config and path helpers for the Gold (AI-Ready Context Layer) pipeline.

silver_ref()/gold_target_name() take `spark` explicitly instead of reading it
as a module-level global -- gold_common.py is imported (not directly exec'd
by the Lakeflow graph loader) by the 3 view files, so it does NOT get `spark`
injected into its own module namespace the way a top-level library file does.
"""


import time
def get_catalog(spark=None) -> str:
    try:
        from pyspark.sql import SparkSession
        s = spark or SparkSession.getActiveSession()
        return s.conf.get("pipeline.catalog", "workspace")
    except Exception:
        return "workspace"


def get_silver_schema(spark=None) -> str:
    try:
        from pyspark.sql import SparkSession
        s = spark or SparkSession.getActiveSession()
        return s.conf.get("pipeline.silver_schema", "silver")
    except Exception:
        return "silver"


def get_gold_schema(spark=None) -> str:
    try:
        from pyspark.sql import SparkSession
        s = spark or SparkSession.getActiveSession()
        return s.conf.get("pipeline.gold_schema", "gold")
    except Exception:
        return "gold"


def silver_ref(spark, table_name: str) -> str:
    return f"{get_catalog(spark)}.{get_silver_schema(spark)}.{table_name}"


def gold_target_name(spark, table_name: str) -> str:
    return f"{get_catalog(spark)}.{get_gold_schema(spark)}.{table_name}"
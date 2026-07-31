"""Shared config and path helpers for the Gold (AI-Ready Context Layer) pipeline.

Imported by fraud_transaction_context.py / customer_360_context.py /
aml_investigation_context.py -- keeps CATALOG/schema resolution and the
silver_ref()/gold_target_name() helpers in one place instead of duplicated
per file (unlike the Silver layer's per-domain get_pipeline_run_id()
duplication, which was a deliberate tradeoff there -- this is small, static
config with no reason to diverge across the 3 Gold views).
"""

CATALOG = spark.conf.get("pipeline.catalog", "workspace")
SILVER_SCHEMA = spark.conf.get("pipeline.silver_schema", "silver")
GOLD_SCHEMA = spark.conf.get("pipeline.gold_schema", "gold")


def silver_ref(table_name: str) -> str:
    """Fully-qualified Silver source table."""
    return f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"


def gold_target_name(table_name: str) -> str:
    """Target name for a Gold view/table (schema-qualified if GOLD_SCHEMA is set)."""
    return f"{GOLD_SCHEMA}.{table_name}" if GOLD_SCHEMA else table_name

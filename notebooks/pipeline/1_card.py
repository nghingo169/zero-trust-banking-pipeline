"""
Target Schema : Silver Atomic Model (`silver`)          
Source Schema : Bronze Datasets                
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Only pull in what this module actually needs from the shared utils --
# lineage/merge/SCD2/dedup helpers live in silver_common_utils but aren't
# used here (see module docstring).

CATALOG = spark.conf.get("pipeline.catalog", "workspace")
BRONZE_SCHEMA = spark.conf.get("pipeline.bronze_schema", "bronze")
SRC__SCHEMA = spark.conf.get("pipeline.silver_validated", "silver_validated")
SILVER_ATOMIC_SCHEMA = spark.conf.get("pipeline.silver_schema", "silver")
TOKEN_SALT = "NAB_assignment_3"  # TODO: rotate this in production; keep it secret

##### These are the helpers funcntions part
# Fully-qualified name helpers
# ---------------------------------------------------------------------------
def clean_src(table_name: str) -> str:
    """Bronze table owned by the core-banking feed."""
    return f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"


def clean_card_src(table_name: str) -> str:
    """Bronze table owned by the card team's cleaned feed."""
    return f"{CATALOG}.{SRC__SCHEMA}.{table_name}"


def atomic_tgt(table_name: str) -> str:
    return f"{CATALOG}.{SILVER_ATOMIC_SCHEMA}.{table_name}"


def bronze_ref(bronze_table: str, business_key_col) -> F.Column:
    """
    Deterministic pointer back to the raw Bronze row: '<bronze_table>:<key>'. 
    (e.g. 'card', 'balance_snapshot'), regardless of which schema
    (BRONZE_SCHEMA vs SRC__SCHEMA) the table physically lives in.
    """
    if isinstance(business_key_col, str):
        business_key_col = F.col(business_key_col)
    return F.concat_ws(":", F.lit(bronze_table), business_key_col.cast("string"))


# ---------------------------------------------------------------------------
# Surrogate keys (SHA-256)
# ---------------------------------------------------------------------------
def hash_key(*cols) -> F.Column:
    """
    Build a deterministic SHA-256 surrogate key from one or more columns
    (pass string column names, F.col(...), or F.lit(...) literals such as a
    fixed source-system discriminator). Same inputs -> same key on every
    run, which is what keeps Delta MERGE upserts idempotent.
    """
    processed = [
        F.coalesce(F.trim(c.cast("string")), F.lit(""))
        if isinstance(c, F.Column)
        else F.coalesce(F.trim(F.col(c).cast("string")), F.lit(""))
        for c in cols
    ]
    return F.sha2(F.concat_ws("||", *processed), 256)


def tokenize_pii(col: str | F.Column) -> F.Column:
    """
    Produce a stable, non-reversible SHA-256 token for a PII value.

    Accepts either a column name or a Spark Column and returns the tokenized
    value using the configured salt.
    """
    if isinstance(col, str):
        col = F.col(col)

    salt = F.lit(TOKEN_SALT)
    return F.sha2(
        F.concat_ws("|", salt, F.coalesce(col.cast("string"), F.lit(""))),
        256,
    )


def mask_value(col, visible_chars: int = 4, mask_char: str = "*") -> F.Column:
    """
    Partially redact a sensitive value for human-readable display/audit use,
    e.g. mask_value(F.col("card_number")) -> '************1234'. This is a
    different masking_policy (REDACTION) than tokenize_pii (SHA256_
    TOKENIZATION): masking keeps a checkable fragment for ops/support
    screens, tokenization produces a stable join key and reveals nothing.
    No column in this table set requires it today -- payment_card only
    persists card_number_token -- but it's kept here so any future
    partially-masked display/audit column can reuse the same logic instead
    of re-deriving it.
    """
    if isinstance(col, str):
        col = F.col(col)
    as_str = F.coalesce(col.cast("string"), F.lit(""))
    length = F.length(as_str)
    tail = F.when(
        length > visible_chars, F.substring(as_str, length - visible_chars + 1, visible_chars)
    ).otherwise(as_str)
    redacted_prefix = F.when(
        length > visible_chars, F.repeat(F.lit(mask_char), (length - visible_chars).cast("int"))
    ).otherwise(F.lit(""))
    return F.concat(redacted_prefix, tail)


# ---------------------------------------------------------------------------
# Source-system resolution for mixed customer_ref identifiers
# ---------------------------------------------------------------------------
SOURCE_SYSTEM_PREFIX_MAP = {
    "CB": "CORE_BANKING",
    "CRM": "CRM",
}


def get_source_system(ref_col) -> F.Column:
    """
    Derive the owning source system from a mixed natural-key column such as
    `customer_ref` (values like 'CB-10293' from core_banking vs 'CRM-501'
    from CRM -- see the customer_ref notes on customer_kyc /
    customer_employment / customer_request / transaction_monitoring_alert in
    bronze_transform_design.txt: "Mixed cust_no or party_id; resolve in
    Silver layer").

    Example:
        df.withColumn("source_system", get_source_system("customer_ref"))
    """
    if isinstance(ref_col, str):
        ref_col = F.col(ref_col)
    prefix = F.upper(F.split(F.trim(ref_col), "-").getItem(0))
    resolved = F.lit(None).cast("string")
    for code, label in SOURCE_SYSTEM_PREFIX_MAP.items():
        resolved = F.when(prefix == F.lit(code), F.lit(label)).otherwise(resolved)
    return F.coalesce(resolved, F.lit("UNKNOWN"))





##### These are the builders funcntions part
# ---------------------------------------------------------------------------
# Table builders -- pure functions: source DataFrame in, target-schema
# DataFrame out. Kept separate from the @dp.table registration so the
# transform logic is testable/reusable on its own.
# ---------------------------------------------------------------------------
def _build_account(df):
    return df.select(
        hash_key(F.lit("core_banking"), "account_id").alias("account_key"),
        F.col("account_id").cast("bigint").alias("source_account_id"),
        F.col("product_type"),
        F.col("status").alias("account_status"),
        F.col("open_date").cast("date").alias("open_date"),
        F.col("branch_code"),
        F.lit("core_banking").alias("source_system"),
        F.col("account_id").cast("string").alias("source_business_key"),
        bronze_ref("account", "account_id").alias("bronze_record_ref"),
        F.current_timestamp().alias("ingested_at"),
    )


def _build_party_account_role(df):
    return df.select(
        hash_key(F.lit("core_banking"), F.lit("party_account_role"), "link_id").alias(
            "party_account_role_key"
        ),
        hash_key(F.lit("core_banking"), "cif_number").alias("party_key"),
        hash_key(F.lit("core_banking"), "account_id").alias("account_key"),
        F.col("relationship_type"),
        F.col("linked_date").cast("date").alias("valid_from"),
        F.lit(None).cast("date").alias("valid_to"),
        F.lit("core_banking").alias("source_system"),
        F.col("link_id").cast("string").alias("source_business_key"),
        bronze_ref("customer_account", "link_id").alias("bronze_record_ref"),
        F.current_timestamp().alias("ingested_at"),
    )


def _build_payment_card(df):
    return df.select(
        hash_key(F.lit("card_system"), "card_id").alias("payment_card_key"),
        F.col("card_id").cast("string").alias("source_card_id"),
        hash_key(F.lit("core_banking"), "account_id").alias("account_key"),
        tokenize_pii(F.col("card_number")).alias("card_number_token"),
        F.col("card_type"),
        F.col("issue_date").cast("date").alias("issue_date"),
        F.col("expiry_date").cast("date").alias("expiry_date"),
        F.col("status").alias("card_status"),
        F.lit("card_system").alias("source_system"),
        F.col("card_id").cast("string").alias("source_business_key"),
        bronze_ref("card", "card_id").alias("bronze_record_ref"),
        F.current_timestamp().alias("ingested_at"),
    )


def _build_payment_card_limit_history(df):
    return df.select(
        hash_key(F.lit("card_system"), F.lit("limit_history"), "history_id").alias(
            "card_limit_history_key"
        ),
        hash_key(F.lit("card_system"), "card_id").alias("payment_card_key"),
        F.col("limit_amount").cast("decimal(12,2)").alias("limit_amount"),
        F.col("effective_date").cast("date").alias("effective_date"),
        F.lit("card_system").alias("source_system"),
        F.col("history_id").cast("string").alias("source_business_key"),
        bronze_ref("card_limit_history", "history_id").alias("bronze_record_ref"),
        F.current_timestamp().alias("ingested_at"),
    )


def _build_account_balance_snapshot(df):
    return df.select(
        hash_key(F.lit("core_banking"), F.lit("balance_snapshot"), "balance_id").alias(
            "account_balance_snapshot_key"
        ),
        hash_key(F.lit("core_banking"), "account_id").alias("account_key"),
        F.col("balance_date").cast("date").alias("balance_date"),
        F.col("opening_balance").cast("decimal(14,2)").alias("opening_balance"),
        F.col("closing_balance").cast("decimal(14,2)").alias("closing_balance"),
        F.col("available_balance").cast("decimal(14,2)").alias("available_balance"),
        F.lit("core_banking").alias("source_system"),
        F.col("balance_id").cast("string").alias("source_business_key"),
        bronze_ref("balance_snapshot", "balance_id").alias("bronze_record_ref"),
        F.current_timestamp().alias("ingested_at"),
    )


def _build_transaction_channel(df):
    return df.select(
        hash_key(F.lit("core_banking"), "channel_id").alias("channel_key"),
        F.col("channel_id").cast("string").alias("source_channel_id"),
        F.col("channel_name"),
        F.col("channel_type"),
        F.lit("core_banking").alias("source_system"),
    )


def _build_merchant(df):
    return df.select(
        hash_key(F.lit("merchant_system"), "merchant_id").alias("merchant_key"),
        F.col("merchant_id").cast("string").alias("source_merchant_id"),
        F.col("merchant_name"),
        F.col("mcc_code"),
        F.col("country"),
        F.lit("merchant_system").alias("source_system"),
    )


def _build_merchant_location(df):
    return df.select(
        hash_key(F.lit("merchant_system"), F.lit("store"), "store_id").alias(
            "merchant_location_key"
        ),
        hash_key(F.lit("merchant_system"), "merchant_id").alias("merchant_key"),
        F.col("store_id").cast("string").alias("source_store_id"),
        F.col("store_name"),
        F.col("store_description"),
        F.col("store_type"),
        F.col("store_address"),
        F.col("risk_rating"),
        F.col("registered_date").cast("date").alias("registered_date"),
        F.lit("merchant_system").alias("source_system"),
    )


# ---------------------------------------------------------------------------
# Table registry -- one entry per Silver table. `unique_keys` mirrors the
# `indexes { ... } [unique]` block for that table in cleaned_dataset_design.txt;
# it's applied below as a Delta Liquid Clustering key (Databricks' closest
# equivalent to a traditional index for a Delta/DLT table) plus a table
# property, so the design doc's index intent is preserved and discoverable
# even though Delta doesn't enforce classic unique indexes.
# ---------------------------------------------------------------------------
TABLE_SPECS = [
    {
        "target": "account",
        "source": clean_src("account"),
        "comment": "Canonical Silver Account Table",
        "builder": _build_account,
        "unique_keys": ["source_system", "source_account_id"],
    },
    {
        "target": "party_account_role",
        "source": clean_src("customer_account"),
        "comment": "Bridge Table linking Party to Account Roles",
        "builder": _build_party_account_role,
        "unique_keys": ["party_key", "account_key", "relationship_type", "valid_from"],
    },
    {
        "target": "payment_card",
        "source": clean_card_src("card"),
        "comment": "Canonical Silver Payment Card Table",
        "builder": _build_payment_card,
        "unique_keys": ["source_system", "source_card_id"],
    },
    {
        "target": "payment_card_limit_history",
        "source": clean_card_src("card_limit_history"),
        "comment": "Canonical Silver Payment Card Limit History Table",
        "builder": _build_payment_card_limit_history,
        "unique_keys": None,
    },
    {
        "target": "account_balance_snapshot",
        "source": clean_src("balance_snapshot"),
        "comment": "Canonical Silver Account Balance Snapshot Table",
        "builder": _build_account_balance_snapshot,
        "unique_keys": ["account_key", "balance_date", "source_system"],
    },
    {
        "target": "transaction_channel",
        "source": clean_src("transaction_channel"),
        "comment": "Canonical Silver Transaction Channel Table",
        "builder": _build_transaction_channel,
        "unique_keys": ["source_system", "source_channel_id"],
    },
    {
        "target": "merchant",
        "source": clean_src("merchant"),
        "comment": "Canonical Silver Merchant Table",
        "builder": _build_merchant,
        "unique_keys": ["source_system", "source_merchant_id"],
    },
    {
        "target": "merchant_location",
        "source": clean_src("merchant_store"),
        "comment": "Canonical Silver Merchant Location Table",
        "builder": _build_merchant_location,
        "unique_keys": ["source_system", "source_store_id"],
    },
]


def _register_table(spec: dict) -> None:
    """
    Register a single Silver table with Lakeflow Declarative Pipelines.
    `spec` is captured as a default arg on `_transform` to avoid the classic
    late-binding closure bug when this runs inside a loop.
    """
    # Define the name and add the comment for the tables
    table_kwargs = {
        "name": atomic_tgt(spec["target"]),
        "comment": spec["comment"],
    }

    # Add clustering and table properties if unique keys are specified
    # Add the clustering here for the performance of queries and the table properties (metadata) for documentation purposes 
    if spec["unique_keys"]:
        table_kwargs["cluster_by"] = spec["unique_keys"]
        table_kwargs["table_properties"] = {
            "silver.unique_index": ",".join(spec["unique_keys"])
        }

    # Register the table with Lakeflow Declarative Pipelines with all the properties and the builder function
    @dp.table(**table_kwargs)
    def _transform(spec=spec):
        df = spark.read.table(spec["source"])
        return spec["builder"](df)

    _transform.__name__ = f"silver_{spec['target']}"


def main() -> None:
    """Build and register every Silver table owned by this domain module."""
    for spec in TABLE_SPECS:
        _register_table(spec)


main()
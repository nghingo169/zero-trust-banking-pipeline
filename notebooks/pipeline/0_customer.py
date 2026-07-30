"""
Target Schema : Silver Atomic Model (`silver`)
Source Schema : Bronze Datasets (`bronze`) 


`cust_no` (core_banking, e.g. 'CB-1001') and `party_id` (crm, e.g. 'CRM-501')
are different natural keys in different systems, and several Bronze tables
(customer_kyc, customer_employment, customer_request) reference either one
through a single polymorphic `customer_ref` column -- see the bronze design's
"Mixed cust_no or party_id; resolve in Silver layer" notes.
================================================================================
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("pipeline.catalog", "workspace")
BRONZE_SCHEMA = spark.conf.get("pipeline.bronze_schema", "bronze")
SRC__SCHEMA = spark.conf.get("pipeline.silver_validated", "silver_validated")
SILVER_ATOMIC_SCHEMA = spark.conf.get("pipeline.silver_schema", "silver")
TOKEN_SALT = "NAB_assignment_3"  # TODO: rotate this in production; keep it secret

# ---------------------------------------------------------------------------
# Fully-qualified name helpers
# ---------------------------------------------------------------------------
def clean_customer_src(table_name: str) -> str:
    """Bronze table owned by the customer team's cleaned feed."""
    return f"{CATALOG}.{SRC__SCHEMA}.{table_name}"

def clean_bronze_src(table_name: str) -> str:
    return f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"


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


# ---------------------------------------------------------------------------
# PII tokenization & masking
# ---------------------------------------------------------------------------
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



def _latest_transaction_by_customer():
    txn = spark.read.table(clean_bronze_src("account_transaction"))
    return txn.groupBy("customer_ref").agg(
        F.max("cdc_change_time").alias("last_txn_at")
    )
 
 
def resolve_party_status(last_txn_at_col, as_of_col=None) -> F.Column:
    """
    Classify party_status from how long it's been since the party's most
    recent account_transaction (matched on customer_ref):
        < 1 year since last txn   -> 'ACTIVE'
        1-3 years since last txn  -> 'PENDING'
        > 3 years since last txn  -> 'DEACTIVE'
        no transaction on record  -> 'PENDING' (can't confirm activity
            either way - e.g. a brand-new account, or a CRM-only contact
            with no core-banking transaction history at all; deliberately
            not defaulted to ACTIVE or DEACTIVE)
 
    as_of_col defaults to the pipeline's current_timestamp() so status is
    always "as of this run", not frozen at ingestion time.
    """
    as_of = as_of_col if as_of_col is not None else F.current_timestamp()
    months_since_last_txn = F.months_between(as_of, last_txn_at_col)
    return (
        F.when(last_txn_at_col.isNull(), F.lit("PENDING"))
        .when(months_since_last_txn < 12, F.lit("ACTIVE"))
        .when(months_since_last_txn <= 36, F.lit("PENDING"))
        .otherwise(F.lit("DEACTIVE"))
    )


# ---------------------------------------------------------------------------
# Table builders -- pure functions: source DataFrame(s) in, target-schema
# DataFrame out. `party` is the one exception that reads two sources itself
# (core_banking_customer + crm_customer), since it has to union them.
# ---------------------------------------------------------------------------
def _build_party():
    df_core = spark.read.table(clean_customer_src("core_banking_customer"))
    df_crm = spark.read.table(clean_customer_src("crm_customer"))
    last_txn = _latest_transaction_by_customer()

    core_system = get_source_system(F.col("cust_no"))
    party_core = (
        df_core.join(last_txn, df_core["cust_no"] == last_txn["customer_ref"], "left")
        .select(
            hash_key(core_system, "cust_no").alias("party_key"),
            F.lit("PERSON").alias("party_type"),
            resolve_party_status(F.col("last_txn_at")).alias("party_status"),
            core_system.alias("source_system"),
            F.col("cust_no").cast("string").alias("source_business_key"),
            bronze_ref("core_banking_customer", "cust_no").alias("bronze_record_ref"),
            F.current_timestamp().alias("ingested_at"),
            F.coalesce(F.col("simulation_id"), F.lit("batch_initial")).alias("load_batch_id"),
            F.lit("VALID").alias("data_quality_status"),
        )
    )

    crm_system = get_source_system(F.col("party_id"))
    party_crm = (
        df_crm.join(last_txn, df_crm["party_id"] == last_txn["customer_ref"], "left")
        .select(
            hash_key(crm_system, "party_id").alias("party_key"),
            F.lit("PERSON").alias("party_type"),
            resolve_party_status(F.col("last_txn_at")).alias("party_status"),
            crm_system.alias("source_system"),
            F.col("party_id").cast("string").alias("source_business_key"),
            bronze_ref("crm_customer", "party_id").alias("bronze_record_ref"),
            F.current_timestamp().alias("ingested_at"),
            F.coalesce(F.col("simulation_id"), F.lit("batch_initial")).alias("load_batch_id"),
            F.lit("VALID").alias("data_quality_status"),
        )
    )

    # unionByName, not a join/merge: each source-system-native customer
    # record stays its own party row with its own party_key. See module
    # docstring -- cross-system matching is party_identity_resolution's job.
    return party_core.unionByName(party_crm)


def _build_party_identifier(df):
    source_system = get_source_system(F.col("cust_no"))

    id_nat = df.filter("national_id IS NOT NULL").select(
        hash_key(F.lit("core_banking"), F.lit("NATIONAL_ID"), "national_id").alias(
            "party_identifier_key"
        ),
        hash_key(source_system, "cust_no").alias("party_key"),
        F.lit("NATIONAL_ID").alias("identifier_type"),
        tokenize_pii(F.trim(F.col("national_id"))).alias("identifier_value_token"),
        source_system.alias("source_system"),
        F.lit(True).alias("is_primary"),
        F.col("created_date").cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        F.col("cust_no").cast("string").alias("source_business_key"),
        bronze_ref("core_banking_customer", "cust_no").alias("bronze_record_ref"),
        F.current_timestamp().alias("ingested_at"),
    )

    id_phone = df.filter("phone IS NOT NULL").select(
        hash_key(F.lit("core_banking"), F.lit("PHONE"), "phone").alias("party_identifier_key"),
        hash_key(source_system, "cust_no").alias("party_key"),
        F.lit("PHONE").alias("identifier_type"),
        tokenize_pii(F.trim(F.col("phone"))).alias("identifier_value_token"),
        source_system.alias("source_system"),
        F.lit(False).alias("is_primary"),
        F.col("created_date").cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        F.col("cust_no").cast("string").alias("source_business_key"),
        bronze_ref("core_banking_customer", "cust_no").alias("bronze_record_ref"),
        F.current_timestamp().alias("ingested_at"),
    )

    return id_nat.unionByName(id_phone)


def _build_party_identity_resolution(df):
    source_system = get_source_system(F.col("party_id"))
    return df.select(
        hash_key(F.lit("crm"), F.lit("identity_resolution"), "party_id").alias(
            "identity_resolution_key"
        ),
        source_system.alias("source_system"),
        F.lit("crm_customer").alias("source_entity"),
        F.col("party_id").cast("string").alias("source_business_key"),
        hash_key(source_system, "party_id").alias("candidate_party_key"),
        F.lit("CONFIRMED").alias("resolution_status"),
        F.lit("DIRECT_IDENTIFIER").alias("match_method"),
        F.lit(1.0000).cast("decimal(5,4)").alias("match_confidence"),
        F.col("created_date").cast("timestamp").alias("resolved_at"),
        bronze_ref("crm_customer", "party_id").alias("bronze_record_ref"),
        F.current_timestamp().alias("ingested_at"),
    )


def _build_party_profile_version(df):
    source_system = get_source_system(F.col("cust_no"))
    return df.select(
        hash_key(F.lit("core_banking"), F.lit("profile_version"), "cust_no", "business_date").alias(
            "party_profile_version_key"
        ),
        hash_key(source_system, "cust_no").alias("party_key"),
        source_system.alias("profile_source"),
        F.col("full_name"),
        F.col("date_of_birth").cast("date").alias("date_of_birth"),
        F.col("address"),
        F.lit(None).cast("string").alias("preferred_contact_method"),
        F.col("business_date").cast("timestamp").alias("effective_from"),
        F.col("__END_AT").cast("timestamp").alias("effective_to"),
        F.when(F.col("__END_AT").isNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_current"),
        F.col("cust_no").cast("string").alias("source_business_key"),
        bronze_ref("core_banking_customer", "cust_no").alias("bronze_record_ref"),
        F.current_timestamp().alias("ingested_at"),
    )


def _build_party_kyc_assessment(df):
    # customer_ref is polymorphic (cust_no or party_id) -- resolve per row,
    # not a hardcoded "core_banking", so party_key actually matches whichever
    # party row (core_banking or crm) this KYC assessment belongs to.
    source_system = get_source_system(F.col("customer_ref"))
    return df.select(
        hash_key(F.lit("core_banking"), F.lit("kyc"), "kyc_id").alias("kyc_assessment_key"),
        hash_key(source_system, "customer_ref").alias("party_key"),
        F.col("id_type"),
        tokenize_pii(F.coalesce(F.col("id_number"), F.lit(""))).alias("id_number_token"),
        F.coalesce(F.col("verification_status"), F.lit("VERIFIED")).alias("verification_status"),
        F.col("verified_date").cast("date").alias("verified_date"),
        source_system.alias("source_system"),
        F.col("kyc_id").cast("string").alias("source_business_key"),
        bronze_ref("customer_kyc", "kyc_id").alias("bronze_record_ref"),
        F.current_timestamp().alias("ingested_at"),
    )


def _build_party_employment(df):
    source_system = get_source_system(F.col("customer_ref"))
    return df.select(
        hash_key(F.lit("core_banking"), F.lit("employment"), "employment_id").alias(
            "employment_key"
        ),
        hash_key(source_system, "customer_ref").alias("party_key"),
        F.col("employer_name"),
        F.col("job_title"),
        F.col("monthly_income").cast("decimal(12,2)").alias("monthly_income"),
        F.col("business_date").cast("date").alias("effective_from"),
        F.lit(None).cast("date").alias("effective_to"),
        source_system.alias("source_system"),
        F.col("employment_id").cast("string").alias("source_business_key"),
        bronze_ref("customer_employment", "employment_id").alias("bronze_record_ref"),
        F.current_timestamp().alias("ingested_at"),
    )


def _build_party_service_request(df):
    source_system = get_source_system(F.col("customer_ref"))
    return df.select(
        hash_key(F.lit("crm"), F.lit("request"), "request_id").alias("service_request_key"),
        hash_key(source_system, "customer_ref").alias("party_key"),
        F.col("request_type"),
        F.col("channel"),
        F.col("request_date").cast("date").alias("request_date"),
        F.col("status").alias("request_status"),
        F.col("resolution_date").cast("date").alias("resolution_date"),
        F.col("description").alias("request_description"),
        source_system.alias("source_system"),
        F.col("request_id").cast("string").alias("source_business_key"),
        bronze_ref("customer_request", "request_id").alias("bronze_record_ref"),
        F.current_timestamp().alias("ingested_at"),
    )


# ---------------------------------------------------------------------------
# Table registry -- one entry per Silver table.
# ---------------------------------------------------------------------------
TABLE_SPECS = [
    {
        "target": "party",
        "source": None,
        "comment": "Canonical Silver Enterprise Party Table",
        "builder": _build_party,
        "unique_keys": None,
    },
    {
        "target": "party_identifier",
        "source": clean_customer_src("core_banking_customer"),
        "comment": "Canonical Silver Party Identifier Table with PII Tokenization",
        "builder": _build_party_identifier,
        "unique_keys": ["source_system", "identifier_type", "identifier_value_token"],
    },
    {
        "target": "party_identity_resolution",
        "source": clean_customer_src("crm_customer"),
        "comment": "Identity Resolution Matching Results",
        "builder": _build_party_identity_resolution,
        "unique_keys": ["source_system", "source_entity", "source_business_key"],
    },
    {
        "target": "party_profile_version",
        "source": clean_customer_src("core_banking_customer"),
        "comment": "Canonical Silver Party Profile Version Table (SCD2)",
        "builder": _build_party_profile_version,
        "unique_keys": None,
    },
    {
        "target": "party_kyc_assessment",
        "source": clean_customer_src("customer_kyc"),
        "comment": "Canonical Silver Party KYC Assessment Table",
        "builder": _build_party_kyc_assessment,
        "unique_keys": None,
    },
    {
        "target": "party_employment",
        "source": clean_customer_src("customer_employment"),
        "comment": "Canonical Silver Party Employment Table",
        "builder": _build_party_employment,
        "unique_keys": None,
    },
    {
        "target": "party_service_request",
        "source": clean_customer_src("customer_request"),
        "comment": "Canonical Silver Party Service Request Table",
        "builder": _build_party_service_request,
        "unique_keys": None,
    },
]


def _register_table(spec: dict) -> None:
    """
    Register a single Silver table with Lakeflow Declarative Pipelines.
    `spec` is captured as a default arg on `_transform` to avoid the classic
    late-binding closure bug when this runs inside a loop. Builders that
    need one source df (spec["source"] set) get it read and handed in;
    `party`'s builder reads its own two sources (spec["source"] is None), so
    it's called with no arguments.
    """
    table_kwargs = {
        "name": atomic_tgt(spec["target"]),
        "comment": spec["comment"],
    }
    if spec["unique_keys"]:
        table_kwargs["cluster_by"] = spec["unique_keys"]
        table_kwargs["table_properties"] = {
            "silver.unique_index": ",".join(spec["unique_keys"])
        }

    @dp.table(**table_kwargs)
    def _transform(spec=spec):
        if spec["source"] is None:
            return spec["builder"]()
        df = spark.read.table(spec["source"])
        return spec["builder"](df)

    _transform.__name__ = f"silver_{spec['target']}"


def main() -> None:
    """Build and register every Silver table owned by this domain module."""
    for spec in TABLE_SPECS:
        _register_table(spec)


main()
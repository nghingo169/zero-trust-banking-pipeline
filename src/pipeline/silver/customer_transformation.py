"""
Target Schema : Silver Atomic Model (`silver`)
Source Schema : Bronze / Validated Datasets (`silver_validated`)
Domain        : Customer / Enterprise Party Domain
"""

import sys
import time
import uuid

from nab_tdm_masking import mask_address, mask_name, mask_national_id, mask_phone
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def get_catalog() -> str:
    """Returns configured catalog or default to 'workspace' (bỏ qua catalog nếu đang chạy pytest)."""
    if "pytest" in sys.modules:
        return ""  # Khi chạy unit test, trả về chuỗi rỗng để tên bảng thành dạng "silver_validated.table"
    try:
        return spark.conf.get("pipeline.catalog", "workspace")
    except Exception:
        return "workspace"


def get_bronze_schema():
    try:
        return spark.conf.get("pipeline.bronze_schema", "bronze")
    except Exception:
        return "bronze"


def get_src_schema():
    try:
        return spark.conf.get("pipeline.silver_validated", "silver_validated")
    except Exception:
        return "silver_validated"


def get_silver_atomic_schema():
    try:
        return spark.conf.get("pipeline.silver_schema", "silver")
    except Exception:
        return "silver"


TOKEN_SALT = "NAB_assignment_3"
AES_KEY = "NAB_SECRET_AES256_KEY_32BYTES!!!"  # Chuẩn 32 bytes cho AES-256


def clean_customer_src(table_name: str) -> str:
    if "pytest" in sys.modules:
        return f"{get_src_schema()}.{table_name}"
    cat = get_catalog()
    return (
        f"{cat}.{get_src_schema()}.{table_name}"
        if cat
        else f"{get_src_schema()}.{table_name}"
    )


def clean_bronze_src(table_name: str) -> str:
    if "pytest" in sys.modules:
        return f"{get_bronze_schema()}.{table_name}"
    cat = get_catalog()
    return (
        f"{cat}.{get_bronze_schema()}.{table_name}"
        if cat
        else f"{get_bronze_schema()}.{table_name}"
    )


def atomic_tgt(table_name: str) -> str:
    if "pytest" in sys.modules:
        return f"{get_silver_atomic_schema()}.{table_name}"
    cat = get_catalog()
    return (
        f"{cat}.{get_silver_atomic_schema()}.{table_name}"
        if cat
        else f"{get_silver_atomic_schema()}.{table_name}"
    )


def bronze_ref(bronze_table: str, business_key_col) -> F.Column:
    if isinstance(business_key_col, str):
        business_key_col = F.col(business_key_col)
    return F.concat_ws(":", F.lit(bronze_table), business_key_col.cast("string"))


def hash_key(*cols) -> F.Column:
    processed = [
        (
            F.coalesce(F.trim(c.cast("string")), F.lit(""))
            if isinstance(c, F.Column)
            else F.coalesce(F.trim(F.col(c).cast("string")), F.lit(""))
        )
        for c in cols
    ]
    return F.sha2(F.concat_ws("||", *processed), 256)


def tokenize_pii(col: str | F.Column) -> F.Column:
    """NAB PII Masking: Tokenize National ID / Phone / KYC ID."""
    if isinstance(col, str):
        col = F.col(col)
    salt = F.lit(TOKEN_SALT)
    return F.sha2(
        F.concat_ws("|", salt, F.coalesce(col.cast("string"), F.lit(""))),
        256,
    )


SOURCE_SYSTEM_PREFIX_MAP = {"CB": "CORE_BANKING", "CRM": "CRM"}


def get_source_system(ref_col) -> F.Column:
    if isinstance(ref_col, str):
        ref_col = F.col(ref_col)
    prefix = F.upper(F.split(F.trim(ref_col), "-").getItem(0))
    resolved = F.lit(None).cast("string")
    for code, label in SOURCE_SYSTEM_PREFIX_MAP.items():
        resolved = F.when(prefix == F.lit(code), F.lit(label)).otherwise(resolved)
    return F.coalesce(resolved, F.lit("UNKNOWN"))


FALLBACK_MODULE_UUID = str(uuid.uuid4())


def get_pipeline_run_id(df) -> F.Column:
    """
    Lấy pipeline_run_id mới nhất từ bảng governance.pipeline_run bằng Scalar Subquery.
    Xử lý an toàn khi get_catalog() trả về chuỗi rỗng trong môi trường test/pytest.
    """
    if "pipeline_run_id" in df.columns:
        return F.col("pipeline_run_id").cast("string")

    cat = get_catalog()
    table_ref = f"{cat}.governance.pipeline_run" if cat else "governance.pipeline_run"

    # Scalar Subquery chuẩn cú pháp SQL
    subquery_expr = f"""
        (SELECT CAST(pipeline_run_id AS STRING) 
         FROM {table_ref} 
         WHERE pipeline_name = 'full-pipeline' 
         ORDER BY start_time DESC 
         LIMIT 1)
    """

    return (
        F.coalesce(F.expr(subquery_expr), F.lit(FALLBACK_MODULE_UUID))
        .cast("string")
        .alias("pipeline_run_id")
    )


def _latest_transaction_by_customer():
    txn = spark.read.table(clean_bronze_src("account_transaction"))
    return txn.groupBy("customer_ref").agg(F.max("txn_timestamp").alias("last_txn_at"))


def resolve_party_status(last_txn_at_col, as_of_col=None) -> F.Column:
    as_of = as_of_col if as_of_col is not None else F.current_timestamp()
    months_since_last_txn = F.months_between(as_of, last_txn_at_col)
    return (
        F.when(last_txn_at_col.isNull(), F.lit("PENDING"))
        .when(months_since_last_txn < 12, F.lit("ACTIVE"))
        .when(months_since_last_txn <= 36, F.lit("PENDING"))
        .otherwise(F.lit("DEACTIVE"))
    )


def _build_party():
    df_core = spark.read.table(clean_customer_src("core_banking_customer"))
    df_crm = spark.read.table(clean_customer_src("crm_customer"))
    last_txn = _latest_transaction_by_customer()

    sim_id_core = (
        F.col("simulation_id") if "simulation_id" in df_core.columns else F.lit(None)
    )
    core_system = get_source_system(F.col("cust_no"))

    # Dedup: source tables carry one snapshot row per business_date per customer,
    # but party_key is hashed from (system, id) only -- without this, the same
    # party appears once per snapshot date and every downstream aggregate fans out.
    w_core = Window.partitionBy("cust_no").orderBy(F.col("business_date").desc())
    df_core = (
        df_core.withColumn("_rn", F.row_number().over(w_core))
        .filter("_rn = 1")
        .drop("_rn")
    )

    w_crm = Window.partitionBy("party_id").orderBy(F.col("business_date").desc())
    df_crm = (
        df_crm.withColumn("_rn", F.row_number().over(w_crm))
        .filter("_rn = 1")
        .drop("_rn")
    )
    party_core = df_core.join(
        last_txn, df_core["cust_no"] == last_txn["customer_ref"], "left"
    ).select(
        hash_key(core_system, "cust_no").alias("party_key"),
        F.lit("PERSON").alias("party_type"),
        resolve_party_status(F.col("last_txn_at")).alias("party_status"),
        core_system.alias("source_system"),
        F.col("cust_no").cast("string").alias("source_business_key"),
        bronze_ref("core_banking_customer", "cust_no").alias("bronze_record_ref"),
        get_pipeline_run_id(df_core).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
        F.coalesce(sim_id_core, F.lit("batch_initial")).alias("load_batch_id"),
        F.lit("VALID").alias("data_quality_status"),
    )

    sim_id_crm = (
        F.col("simulation_id") if "simulation_id" in df_crm.columns else F.lit(None)
    )
    crm_system = get_source_system(F.col("party_id"))

    party_crm = df_crm.join(
        last_txn, df_crm["party_id"] == last_txn["customer_ref"], "left"
    ).select(
        hash_key(crm_system, "party_id").alias("party_key"),
        F.lit("PERSON").alias("party_type"),
        resolve_party_status(F.col("last_txn_at")).alias("party_status"),
        crm_system.alias("source_system"),
        F.col("party_id").cast("string").alias("source_business_key"),
        bronze_ref("crm_customer", "party_id").alias("bronze_record_ref"),
        get_pipeline_run_id(df_crm).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
        F.coalesce(sim_id_crm, F.lit("batch_initial")).alias("load_batch_id"),
        F.lit("VALID").alias("data_quality_status"),
    )

    return party_core.unionByName(party_crm)


def _build_party_identifier(df):
    source_system = get_source_system(F.col("cust_no"))

    id_nat = df.filter("national_id IS NOT NULL").select(
        hash_key(F.lit("core_banking"), F.lit("NATIONAL_ID"), "national_id").alias(
            "party_identifier_key"
        ),
        hash_key(source_system, "cust_no").alias("party_key"),
        F.lit("NATIONAL_ID").alias("identifier_type"),
        mask_national_id(F.trim(F.col("national_id"))).alias("identifier_value_masked"),
        F.base64(F.aes_encrypt(F.trim(F.col("national_id")), F.lit(AES_KEY))).alias(
            "identifier_value_encrypted"
        ),
        tokenize_pii(F.trim(F.col("national_id"))).alias("identifier_value_token"),
        source_system.alias("source_system"),
        F.lit(True).alias("is_primary"),
        F.col("created_date").cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        F.col("cust_no").cast("string").alias("source_business_key"),
        bronze_ref("core_banking_customer", "cust_no").alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )

    id_phone = df.filter("phone IS NOT NULL").select(
        hash_key(F.lit("core_banking"), F.lit("PHONE"), "phone").alias(
            "party_identifier_key"
        ),
        hash_key(source_system, "cust_no").alias("party_key"),
        F.lit("PHONE").alias("identifier_type"),
        mask_phone(F.trim(F.col("phone"))).alias("identifier_value_masked"),
        F.base64(F.aes_encrypt(F.trim(F.col("phone")), F.lit(AES_KEY))).alias(
            "identifier_value_encrypted"
        ),
        tokenize_pii(F.trim(F.col("phone"))).alias("identifier_value_token"),
        source_system.alias("source_system"),
        F.lit(False).alias("is_primary"),
        F.col("created_date").cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        F.col("cust_no").cast("string").alias("source_business_key"),
        bronze_ref("core_banking_customer", "cust_no").alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
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
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


def _build_party_profile_version(df):
    # ---- Branch 1: core_banking (unchanged; source has no preferred_contact_method) ----
    source_system = get_source_system(F.col("cust_no"))
    cb = df.select(
        hash_key(
            F.lit("core_banking"), F.lit("profile_version"), "cust_no", "business_date"
        ).alias("party_profile_version_key"),
        hash_key(source_system, "cust_no").alias("party_key"),
        source_system.alias("source_system"),
        source_system.alias("profile_source"),
        mask_name(F.col("full_name")).alias("full_name_masked"),
        F.base64(F.aes_encrypt(F.col("full_name"), F.lit(AES_KEY))).alias(
            "full_name_encrypted"
        ),
        tokenize_pii(F.col("full_name")).alias("full_name_token"),
        F.col("date_of_birth").cast("date").alias("date_of_birth"),
        mask_address(F.col("address")).alias("address_masked"),
        F.base64(F.aes_encrypt(F.col("address"), F.lit(AES_KEY))).alias(
            "address_encrypted"
        ),
        tokenize_pii(F.col("address")).alias("address_token"),
        F.lit(None).cast("string").alias("preferred_contact_method"),
        F.col("business_date").cast("timestamp").alias("effective_from"),
        F.col("__END_AT").cast("timestamp").alias("effective_to"),
        F.when(F.col("__END_AT").isNull(), F.lit(True))
        .otherwise(F.lit(False))
        .alias("is_current"),
        F.col("cust_no").cast("string").alias("source_business_key"),
        bronze_ref("core_banking_customer", "cust_no").alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )

    # ---- Branch 2: CRM (the only source carrying preferred_contact_method) ----
    crm_df = spark.read.table(clean_customer_src("crm_customer"))
    crm_source_system = get_source_system(
        F.col("party_id")
    )  # party_id assumed CRM-xxx prefixed -> "CRM"
    crm = crm_df.select(
        hash_key(
            F.lit("crm"), F.lit("profile_version"), "party_id", "business_date"
        ).alias("party_profile_version_key"),
        hash_key(crm_source_system, "party_id").alias(
            "party_key"
        ),  # MUST match _build_party's CRM formula
        crm_source_system.alias("source_system"),
        crm_source_system.alias("profile_source"),
        mask_name(F.col("customer_name")).alias("full_name_masked"),
        F.base64(F.aes_encrypt(F.col("customer_name"), F.lit(AES_KEY))).alias(
            "full_name_encrypted"
        ),
        tokenize_pii(F.col("customer_name")).alias("full_name_token"),
        F.lit(None).cast("date").alias("date_of_birth"),  # CRM source has no DOB
        F.lit(None).cast("string").alias("address_masked"),  # CRM source has no address
        F.lit(None).cast("string").alias("address_encrypted"),
        F.lit(None).cast("string").alias("address_token"),
        F.col("preferred_contact_method"),
        F.col("business_date").cast("timestamp").alias("effective_from"),
        F.col("__END_AT").cast("timestamp").alias("effective_to"),
        F.when(F.col("__END_AT").isNull(), F.lit(True))
        .otherwise(F.lit(False))
        .alias("is_current"),
        F.col("party_id").cast("string").alias("source_business_key"),
        bronze_ref("crm_customer", "party_id").alias("bronze_record_ref"),
        get_pipeline_run_id(crm_df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )

    return cb.unionByName(crm)


def _build_party_kyc_assessment(df):
    source_system = get_source_system(F.col("customer_ref"))
    return df.select(
        hash_key(F.lit("core_banking"), F.lit("kyc"), "kyc_id").alias(
            "kyc_assessment_key"
        ),
        hash_key(source_system, "customer_ref").alias("party_key"),
        F.col("id_type"),
        tokenize_pii(F.coalesce(F.col("id_number"), F.lit(""))).alias(
            "id_number_token"
        ),
        F.coalesce(F.col("verification_status"), F.lit("VERIFIED")).alias(
            "verification_status"
        ),
        F.col("verified_date").cast("date").alias("verified_date"),
        source_system.alias("source_system"),
        F.col("kyc_id").cast("string").alias("source_business_key"),
        bronze_ref("customer_kyc", "kyc_id").alias("bronze_record_ref"),
        get_pipeline_run_id(df).alias("pipeline_run_id"),
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
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


def _build_party_service_request(df):
    source_system = get_source_system(F.col("customer_ref"))
    return df.select(
        hash_key(F.lit("crm"), F.lit("request"), "request_id").alias(
            "service_request_key"
        ),
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
        get_pipeline_run_id(df).alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


def get_table_specs():
    return [
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
            "unique_keys": [
                "source_system",
                "identifier_type",
                "identifier_value_token",
            ],
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
    table_kwargs = {"name": atomic_tgt(spec["target"]), "comment": spec["comment"]}
    if spec["unique_keys"]:
        table_kwargs["cluster_by"] = spec["unique_keys"]
        table_kwargs["table_properties"] = {
            "silver.unique_index": ",".join(spec["unique_keys"])
        }

    def _transform(spec=spec):
        if spec.get("source") is None:
            return spec["builder"]()
        df = spark.read.table(spec["source"])
        return spec["builder"](df)

    _transform.__name__ = f"silver_{spec['target']}"
    dp.table(**table_kwargs)(_transform)


def main() -> None:
    for spec in get_table_specs():
        _register_table(spec)


if __name__ == "__main__":
    main()

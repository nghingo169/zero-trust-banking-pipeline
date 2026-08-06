"""
Target : gold.ai_customer_360_context
Grain  : 1 row per party
Consumer: Customer 360 Agent (RM & contact-center Q&A)
"""

from gold_common import gold_target_name, silver_ref
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.table(
    name=gold_target_name(spark, "ai_customer_360_context"),
    comment="AI-Ready Customer 360 context: current profile + KYC + account/card/servicing overview, PII tokenized/banded.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
    cluster_by=["party_key"],
    schema="""
        party_key STRING COMMENT 'Surrogate key of the customer. Grain of this view: exactly 1 row per party.',
        party_type STRING COMMENT 'Party classification, e.g. PERSON.',
        party_status STRING COMMENT 'Lifecycle status derived from latest transaction activity: ACTIVE / DEACTIVE / PENDING.',
        preferred_contact_method STRING COMMENT 'Preferred contact channel. Sourced from CRM only; NULL for CORE_BANKING-sourced customers by design (core source does not carry this field).',
        profile_effective_from TIMESTAMP COMMENT 'Effective date of the current profile version (SCD2).',
        kyc_verification_status STRING COMMENT 'Latest KYC status: VERIFIED / PENDING / REJECTED. NULL when the customer has no KYC assessment.',
        kyc_id_type STRING COMMENT 'ID document type of the latest KYC, e.g. NATIONAL_ID / PASSPORT.',
        kyc_id_number STRING COMMENT 'SHA-256 token of the KYC ID number. PII-safe; never the raw value.',
        employer_name STRING COMMENT 'Employer from the latest open employment record.',
        job_title STRING COMMENT 'Job title from the latest open employment record.',
        monthly_income_band STRING COMMENT 'Monthly income in VND, banded for PII minimization: <10M, 10-30M, 30-100M, 100M+.',
        active_account_count BIGINT COMMENT 'Number of currently ACTIVE accounts with an open party-account role.',
        total_current_balance DECIMAL(24,2) COMMENT 'Sum of the latest closing balances (within last 30 days) across the customer accounts. NULL when no recent balance snapshot exists.',
        active_card_count BIGINT COMMENT 'Number of ACTIVE payment cards across the customer accounts.',
        open_service_request_count BIGINT COMMENT 'Service requests not yet RESOLVED or REJECTED.',
        call_center_contact_count_90d BIGINT COMMENT 'Call center contacts in the last 90 days.',
        last_call_reason STRING COMMENT 'Reason of the most recent call center contact, any time. NULL when the customer never called.',
        open_investigation_flag BOOLEAN COMMENT 'TRUE if the customer has an AML/fraud investigation case not CLOSED/RESOLVED.',
        source_system STRING COMMENT 'Source system that mastered this party: CORE_BANKING or CRM.',
        source_business_key STRING COMMENT 'Natural customer id in the source system (cust_no or party_id).',
        ingested_at TIMESTAMP COMMENT 'Timestamp this row was produced by the pipeline.',
        pipeline_run_id STRING COMMENT 'Pipeline run that produced this row, for lineage/audit.',
        dq_status STRING COMMENT 'Data quality verdict: PASSED_CLEAN, WARNING_UNRESOLVED_PARTY (KYC missing or not VERIFIED), or REJECTED_QUALITY (source row quarantined).'
    """,
)
@dp.expect_or_drop("valid_party_key", "party_key IS NOT NULL")
def ai_customer_360_context():
    p = spark.read.table(silver_ref(spark, "party"))
    ppv = spark.read.table(silver_ref(spark, "party_profile_version"))
    pka = spark.read.table(silver_ref(spark, "party_kyc_assessment"))
    pe = spark.read.table(silver_ref(spark, "party_employment"))
    par = spark.read.table(silver_ref(spark, "party_account_role"))
    a = spark.read.table(silver_ref(spark, "account"))
    abs_df = spark.read.table(silver_ref(spark, "account_balance_snapshot"))
    pc = spark.read.table(silver_ref(spark, "payment_card"))
    psr = spark.read.table(silver_ref(spark, "party_service_request"))
    ccc = spark.read.table(silver_ref(spark, "call_center_contact"))
    ac = spark.read.table(silver_ref(spark, "aml_case"))
    ic = spark.read.table(silver_ref(spark, "investigation_case"))

    current_profile = ppv.filter(F.col("is_current") == True)

    kyc_window = Window.partitionBy("party_key").orderBy(F.col("verified_date").desc())
    latest_kyc = pka.withColumn("rn", F.row_number().over(kyc_window)).filter("rn = 1")

    emp_window = Window.partitionBy("party_key").orderBy(F.col("effective_from").desc())
    current_employment = (
        pe.filter(F.col("effective_to").isNull())
        .withColumn("rn", F.row_number().over(emp_window))
        .filter("rn = 1")
    )

    abs_recent = abs_df.filter(
        F.col("balance_date") >= F.date_sub(F.current_date(), 30)
    )
    bal_window = Window.partitionBy("account_key").orderBy(F.col("balance_date").desc())
    latest_bal = abs_recent.withColumn("rn", F.row_number().over(bal_window)).filter(
        "rn = 1"
    )

    card_count_per_account = pc.groupBy("account_key").agg(
        F.countDistinct(
            F.when(F.col("card_status") == "ACTIVE", F.col("payment_card_key"))
        ).alias("active_card_count")
    )

    account_level = (
        par.alias("par")
        .join(a.alias("a"), F.col("a.account_key") == F.col("par.account_key"))
        .join(
            latest_bal.alias("lb"),
            F.col("lb.account_key") == F.col("a.account_key"),
            "left",
        )
        .join(
            card_count_per_account.alias("cca"),
            F.col("cca.account_key") == F.col("a.account_key"),
            "left",
        )
        .select(
            F.col("par.party_key"),
            F.col("a.account_key"),
            F.col("par.valid_to"),
            F.col("a.account_status"),
            F.col("lb.closing_balance"),
            F.coalesce(F.col("cca.active_card_count"), F.lit(0)).alias(
                "active_card_count"
            ),
        )
    )

    account_overview = account_level.groupBy("party_key").agg(
        F.countDistinct(
            F.when(
                F.col("valid_to").isNull() & (F.col("account_status") == "ACTIVE"),
                F.col("account_key"),
            )
        ).alias("active_account_count"),
        F.sum("closing_balance").alias("total_current_balance"),
        F.sum("active_card_count").alias("active_card_count"),
    )

    service_overview = (
        psr.filter(~F.col("request_status").isin("RESOLVED", "REJECTED"))
        .groupBy("party_key")
        .agg(F.count("*").alias("open_service_request_count"))
    )

    call_overview = ccc.groupBy("party_key").agg(
        F.count(
            F.when(F.col("call_timestamp") >= F.date_sub(F.current_date(), 90), 1)
        ).alias("call_center_contact_count_90d"),
        F.expr("max_by(call_reason, call_timestamp)").alias("last_call_reason"),
    )

    investigation_overview = (
        ac.join(ic, "investigation_case_key")
        .filter(~F.col("case_status").isin("CLOSED", "RESOLVED"))
        .select("party_key")
        .distinct()
        .withColumn("open_investigation_flag", F.lit(True))
    )

    return (
        p.alias("p")
        .join(
            current_profile.alias("cpv"),
            F.col("cpv.party_key") == F.col("p.party_key"),
            "left",
        )
        .join(
            latest_kyc.alias("lk"),
            F.col("lk.party_key") == F.col("p.party_key"),
            "left",
        )
        .join(
            current_employment.alias("ce"),
            F.col("ce.party_key") == F.col("p.party_key"),
            "left",
        )
        .join(
            account_overview.alias("ao"),
            F.col("ao.party_key") == F.col("p.party_key"),
            "left",
        )
        .join(
            service_overview.alias("so"),
            F.col("so.party_key") == F.col("p.party_key"),
            "left",
        )
        .join(
            call_overview.alias("co"),
            F.col("co.party_key") == F.col("p.party_key"),
            "left",
        )
        .join(
            investigation_overview.alias("io"),
            F.col("io.party_key") == F.col("p.party_key"),
            "left",
        )
        .select(
            F.col("p.party_key"),
            F.col("p.party_type"),
            F.col("p.party_status"),
            F.col("cpv.preferred_contact_method"),
            F.col("cpv.effective_from").alias("profile_effective_from"),
            F.col("lk.verification_status").alias("kyc_verification_status"),
            F.col("lk.id_type").alias("kyc_id_type"),
            F.col("lk.id_number").alias("kyc_id_number"),
            F.col("ce.employer_name"),
            F.col("ce.job_title"),
            F.when(F.col("ce.monthly_income").isNull(), F.lit(None))
            .when(F.col("ce.monthly_income") < 10000000, "<10M")
            .when(F.col("ce.monthly_income") < 30000000, "10-30M")
            .when(F.col("ce.monthly_income") < 100000000, "30-100M")
            .otherwise("100M+")
            .alias("monthly_income_band"),
            F.coalesce(F.col("ao.active_account_count"), F.lit(0)).alias(
                "active_account_count"
            ),
            F.col("ao.total_current_balance"),
            F.coalesce(F.col("ao.active_card_count"), F.lit(0)).alias(
                "active_card_count"
            ),
            F.coalesce(F.col("so.open_service_request_count"), F.lit(0)).alias(
                "open_service_request_count"
            ),
            F.coalesce(F.col("co.call_center_contact_count_90d"), F.lit(0)).alias(
                "call_center_contact_count_90d"
            ),
            F.col("co.last_call_reason"),
            F.coalesce(F.col("io.open_investigation_flag"), F.lit(False)).alias(
                "open_investigation_flag"
            ),
            F.col("p.source_system"),
            F.col("p.source_business_key"),
            F.col("p.ingested_at"),
            F.col("p.pipeline_run_id"),
            F.when(F.col("p.data_quality_status") == "QUARANTINED", "REJECTED_QUALITY")
            .when(
                F.col("lk.verification_status").isNull()
                | (F.col("lk.verification_status") != "VERIFIED"),
                "WARNING_UNRESOLVED_PARTY",
            )
            .otherwise("PASSED_CLEAN")
            .alias("dq_status"),
        )
    )

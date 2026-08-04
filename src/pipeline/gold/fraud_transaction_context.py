"""
Target : gold.ai_fraud_transaction_context
Grain  : 1 row per financial_event (any subtype)
Consumer: Fraud Detection Agent (real-time scoring, alert triage)
"""

from gold_common import gold_target_name, silver_ref
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.table(
    name=gold_target_name(spark, "ai_fraud_transaction_context"),
    comment="AI-Ready fraud context: one row per financial event, flattened with merchant/channel/risk/alert context. PII-free.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
    cluster_by=["occurred_at", "party_key"],
    schema="""
        financial_event_key STRING COMMENT 'Surrogate key of the financial event. Grain of this view: exactly 1 row per financial event of any subtype.',
        event_type STRING COMMENT 'Event subtype: ACCOUNT_POSTING, CARD_PAYMENT, ATM_ACTIVITY, or GATEWAY_PAYMENT. Determines which detail columns are populated.',
        occurred_at TIMESTAMP COMMENT 'Business timestamp when the transaction occurred.',
        amount DECIMAL(12,2) COMMENT 'Transaction amount, coalesced across the 4 subtypes.',
        currency STRING COMMENT 'ISO currency code of the transaction.',
        transaction_type_detail STRING COMMENT 'Subtype-specific type: posting_type / card_transaction_type / atm activity_type / gateway payment_method.',
        posting_direction STRING COMMENT 'DEBIT or CREDIT. Only populated for ACCOUNT_POSTING events; NULL for other subtypes by design.',
        account_key STRING COMMENT 'Surrogate key of the account. For card/atm events resolved via card -> account linkage.',
        payment_card_key STRING COMMENT 'Surrogate key of the payment card. NULL for ACCOUNT_POSTING events by design.',
        party_key STRING COMMENT 'Surrogate key of the customer, resolved via account-to-party bridge. Approx 90 percent resolved; NULL means the source reference could not be linked to a known customer.',
        channel_key STRING COMMENT 'Transaction channel key. Only for ACCOUNT_POSTING; NULL for other subtypes by design.',
        channel_name STRING COMMENT 'Channel display name, e.g. Mobile Banking. ACCOUNT_POSTING only.',
        channel_type STRING COMMENT 'Channel category, e.g. DIGITAL / BRANCH. ACCOUNT_POSTING only.',
        merchant_key STRING COMMENT 'Merchant surrogate key. NULL for ATM and most account postings by design.',
        merchant_name STRING COMMENT 'Merchant display name.',
        mcc_code STRING COMMENT 'Merchant Category Code.',
        merchant_country STRING COMMENT 'Merchant country.',
        merchant_store_count BIGINT COMMENT 'Number of stores registered under this merchant. 0 when no merchant is linked.',
        merchant_max_store_risk STRING COMMENT 'Highest fraud-team risk rating across all stores of the merchant, ordered LOW < MEDIUM < HIGH. Merchant-level because transaction sources never carry store_id, so store-grain context is unresolvable per event.',
        latest_risk_score DECIMAL(6,4) COMMENT 'Most recent model risk score for this event. Only ACCOUNT_POSTING events are scored (source: account_transaction_risk_score); NULL elsewhere by design.',
        latest_risk_band STRING COMMENT 'Risk band of the latest score, e.g. LOW / MEDIUM / HIGH.',
        fraud_alert_count BIGINT COMMENT 'Number of distinct fraud alerts linked to this event. 0 when none.',
        max_fraud_alert_score DECIMAL(5,2) COMMENT 'Highest alert score among linked fraud alerts.',
        open_fraud_alert_flag BOOLEAN COMMENT 'TRUE if any linked fraud alert is not CLOSED.',
        card_fraud_flag_count BIGINT COMMENT 'Number of card fraud flags linked to this event.',
        is_fraud_source_flag BOOLEAN COMMENT 'Fraud label from the card source system, CARD_PAYMENT events only.',
        source_system STRING COMMENT 'Originating source system of the event.',
        source_business_key STRING COMMENT 'Natural key of the event in the source system.',
        ingested_at TIMESTAMP COMMENT 'Timestamp this row was produced by the pipeline.',
        pipeline_run_id STRING COMMENT 'Pipeline run that produced this row, for lineage/audit.',
        dq_status STRING COMMENT 'Data quality verdict: PASSED_CLEAN, WARNING_UNRESOLVED_PARTY (party_key could not be resolved), or REJECTED_QUALITY (source row was quarantined).'
    """,
)
@dp.expect_or_drop("valid_financial_event_key", "financial_event_key IS NOT NULL")
def ai_fraud_transaction_context():
    fe = spark.read.table(silver_ref(spark, "financial_event"))

    ap = spark.read.table(silver_ref(spark, "account_posting"))
    cp = spark.read.table(silver_ref(spark, "card_payment"))
    atm = spark.read.table(silver_ref(spark, "atm_activity"))
    gp = spark.read.table(silver_ref(spark, "gateway_payment"))
    tc = spark.read.table(silver_ref(spark, "transaction_channel"))
    m = spark.read.table(silver_ref(spark, "merchant"))
    fers = spark.read.table(silver_ref(spark, "financial_event_risk_score"))
    fefa = spark.read.table(silver_ref(spark, "financial_event_fraud_alert"))
    fa = spark.read.table(silver_ref(spark, "fraud_alert"))
    cfa_src = spark.read.table(silver_ref(spark, "financial_event_card_fraud_flag"))

    risk_window = Window.partitionBy("financial_event_key").orderBy(
        F.col("scored_date").desc()
    )
    latest_risk = fers.withColumn("rn", F.row_number().over(risk_window)).filter(
        "rn = 1"
    )

    fraud_alert_agg = (
        fefa.join(fa, "fraud_alert_key")
        .groupBy("financial_event_key")
        .agg(
            F.countDistinct("fraud_alert_key").alias("fraud_alert_count"),
            F.max("alert_score").alias("max_fraud_alert_score"),
            F.max(F.col("alert_status") != "CLOSED").alias("open_fraud_alert_flag"),
        )
    )
    # Merchant-level store risk (events carry merchant_id only, never store_id --
    # store-grain context is unresolvable, so aggregate risk across the merchant's stores)
    ml = spark.read.table(silver_ref(spark, "merchant_location"))
    merchant_store_risk = (
        ml.withColumn(
            "_risk_rank",
            F.when(F.col("risk_rating") == "HIGH", 3)
            .when(F.col("risk_rating") == "MEDIUM", 2)
            .when(F.col("risk_rating") == "LOW", 1)
            .otherwise(0),
        )
        .groupBy("merchant_key")
        .agg(
            F.count("*").alias("merchant_store_count"),
            F.max("_risk_rank").alias("_max_rank"),
        )
        .withColumn(
            "merchant_max_store_risk",
            F.when(F.col("_max_rank") == 3, "HIGH")
            .when(F.col("_max_rank") == 2, "MEDIUM")
            .when(F.col("_max_rank") == 1, "LOW"),
        )
        .drop("_max_rank")
    )
    card_flag_agg = cfa_src.groupBy("financial_event_key").agg(
        F.countDistinct("card_fraud_flag_key").alias("card_fraud_flag_count")
    )

    return (
        fe.alias("fe")
        .join(
            ap.alias("ap"),
            (F.col("ap.financial_event_key") == F.col("fe.financial_event_key"))
            & (F.col("fe.event_type") == "ACCOUNT_POSTING"),
            "left",
        )
        .join(
            cp.alias("cp"),
            (F.col("cp.financial_event_key") == F.col("fe.financial_event_key"))
            & (F.col("fe.event_type") == "CARD_PAYMENT"),
            "left",
        )
        .join(
            atm.alias("atm"),
            (F.col("atm.financial_event_key") == F.col("fe.financial_event_key"))
            & (F.col("fe.event_type") == "ATM_ACTIVITY"),
            "left",
        )
        .join(
            gp.alias("gp"),
            (F.col("gp.financial_event_key") == F.col("fe.financial_event_key"))
            & (F.col("fe.event_type") == "GATEWAY_PAYMENT"),
            "left",
        )
        .join(
            tc.alias("tc"), F.col("tc.channel_key") == F.col("ap.channel_key"), "left"
        )
        .join(
            merchant_store_risk.alias("msr"),
            F.col("msr.merchant_key")
            == F.coalesce(
                F.col("ap.merchant_key"),
                F.col("cp.merchant_key"),
                F.col("gp.merchant_key"),
            ),
            "left",
        )
        .join(
            m.alias("m"),
            F.col("m.merchant_key")
            == F.coalesce(
                F.col("ap.merchant_key"),
                F.col("cp.merchant_key"),
                F.col("gp.merchant_key"),
            ),
            "left",
        )
        .join(
            latest_risk.alias("lr"),
            F.col("lr.financial_event_key") == F.col("fe.financial_event_key"),
            "left",
        )
        .join(
            fraud_alert_agg.alias("faa"),
            F.col("faa.financial_event_key") == F.col("fe.financial_event_key"),
            "left",
        )
        .join(
            card_flag_agg.alias("cfa"),
            F.col("cfa.financial_event_key") == F.col("fe.financial_event_key"),
            "left",
        )
        .select(
            F.col("fe.financial_event_key"),
            F.col("fe.event_type"),
            F.col("fe.occurred_at"),
            F.coalesce(
                F.col("ap.posting_amount"),
                F.col("cp.payment_amount"),
                F.col("atm.amount"),
                F.col("gp.amount"),
            ).alias("amount"),
            F.col("fe.currency"),
            F.coalesce(
                F.col("ap.posting_type"),
                F.col("cp.card_transaction_type"),
                F.col("atm.activity_type"),
                F.col("gp.payment_method"),
            ).alias("transaction_type_detail"),
            F.col("ap.posting_direction"),
            F.col("fe.account_key"),
            F.col("fe.payment_card_key"),
            F.col("fe.party_key"),
            F.col("ap.channel_key"),
            F.col("tc.channel_name"),
            F.col("tc.channel_type"),
            F.coalesce(
                F.col("ap.merchant_key"),
                F.col("cp.merchant_key"),
                F.col("gp.merchant_key"),
            ).alias("merchant_key"),
            F.col("m.merchant_name"),
            F.col("m.mcc_code"),
            F.col("m.country").alias("merchant_country"),
            F.coalesce(F.col("msr.merchant_store_count"), F.lit(0)).alias(
                "merchant_store_count"
            ),
            F.col("msr.merchant_max_store_risk"),
            F.col("lr.model_score").alias("latest_risk_score"),
            F.col("lr.risk_band").alias("latest_risk_band"),
            F.coalesce(F.col("faa.fraud_alert_count"), F.lit(0)).alias(
                "fraud_alert_count"
            ),
            F.col("faa.max_fraud_alert_score"),
            F.coalesce(F.col("faa.open_fraud_alert_flag"), F.lit(False)).alias(
                "open_fraud_alert_flag"
            ),
            F.coalesce(F.col("cfa.card_fraud_flag_count"), F.lit(0)).alias(
                "card_fraud_flag_count"
            ),
            F.col("cp.is_fraud_source_flag"),
            F.col("fe.source_system"),
            F.col("fe.source_business_key"),
            F.col("fe.ingested_at"),
            F.col("fe.pipeline_run_id"),
            F.when(F.col("fe.data_quality_status") == "QUARANTINED", "REJECTED_QUALITY")
            .when(F.col("fe.party_key").isNull(), "WARNING_UNRESOLVED_PARTY")
            .otherwise("PASSED_CLEAN")
            .alias("dq_status"),
        )
    )

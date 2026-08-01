"""
Target : gold.ai_aml_investigation_context
Grain  : 1 row per investigation_case
Consumer: Compliance/Legal Agent (case triage, SAR/sanctions lookup)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from gold_common import silver_ref, gold_target_name


@dp.table(
    name=gold_target_name(spark, "ai_aml_investigation_context"),
    comment="AI-Ready AML/Fraud investigation context: one row per investigation case, flattened with AML detail, SAR filing status, etc.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
    cluster_by=["case_status", "opened_at"],
)
@dp.expect_or_drop("valid_investigation_case_key", "investigation_case_key IS NOT NULL")
def ai_aml_investigation_context():
    ic = spark.read.table(silver_ref(spark, "investigation_case"))
    ac = spark.read.table(silver_ref(spark, "aml_case"))
    sar = spark.read.table(silver_ref(spark, "suspicious_activity_report"))
    icss = spark.read.table(silver_ref(spark, "investigation_case_sanctions_screening"))
    ss = spark.read.table(silver_ref(spark, "sanctions_screening"))
    we = spark.read.table(silver_ref(spark, "watchlist_entry"))
    icfa = spark.read.table(silver_ref(spark, "investigation_case_fraud_alert"))
    icma = spark.read.table(silver_ref(spark, "investigation_case_monitoring_alert"))
    icfe = spark.read.table(silver_ref(spark, "investigation_case_financial_event"))
    in_note = spark.read.table(silver_ref(spark, "investigation_note"))

    aml_case_window = Window.partitionBy("investigation_case_key").orderBy(F.col("opened_date").desc())
    aml_case_dedup = (
        ac.withColumn("rn", F.row_number().over(aml_case_window))
        .filter("rn = 1")
    )

    sar_window = Window.partitionBy("aml_case_key").orderBy(F.col("filed_date").desc())
    sar_latest = sar.withColumn("rn", F.row_number().over(sar_window)).filter("rn = 1")

    sanctions_agg = (
        icss.join(ss, "sanctions_screening_key")
        .join(we, "watchlist_entry_key", "left")
        .groupBy("investigation_case_key")
        .agg(
            F.countDistinct("sanctions_screening_key").alias("sanctions_screening_count"),
            F.max(F.when(~F.col("screening_result").isin("CLEAR", "FALSE_POSITIVE"), 1).otherwise(0)).cast("boolean").alias("sanctions_hit_flag"),
            F.max("match_score").alias("max_sanctions_match_score"),
            F.array_join(F.collect_set("list_type"), ", ").alias("linked_watchlist_types"),
        )
    )

    fraud_alert_agg = (
        icfa.groupBy("investigation_case_key")
        .agg(F.countDistinct("fraud_alert_key").alias("linked_fraud_alert_count"))
    )

    monitoring_alert_agg = (
        icma.groupBy("investigation_case_key")
        .agg(F.countDistinct("monitoring_alert_key").alias("linked_monitoring_alert_count"))
    )

    event_agg = (
        icfe.groupBy("investigation_case_key")
        .agg(F.countDistinct("financial_event_key").alias("linked_financial_event_count"))
    )

    note_agg = (
        in_note.groupBy("investigation_case_key")
        .agg(
            F.count("*").alias("investigation_note_count"),
            F.expr("max_by(note_text, note_timestamp)").alias("latest_note_text"),
        )
    )

    return (
        ic.alias("ic")
        .join(aml_case_dedup.alias("ac"), F.col("ac.investigation_case_key") == F.col("ic.investigation_case_key"), "left")
        .join(sar_latest.alias("sl"), F.col("sl.aml_case_key") == F.col("ac.aml_case_key"), "left")
        .join(sanctions_agg.alias("sa"), F.col("sa.investigation_case_key") == F.col("ic.investigation_case_key"), "left")
        .join(fraud_alert_agg.alias("faa"), F.col("faa.investigation_case_key") == F.col("ic.investigation_case_key"), "left")
        .join(monitoring_alert_agg.alias("maa"), F.col("maa.investigation_case_key") == F.col("ic.investigation_case_key"), "left")
        .join(event_agg.alias("ea"), F.col("ea.investigation_case_key") == F.col("ic.investigation_case_key"), "left")
        .join(note_agg.alias("na"), F.col("na.investigation_case_key") == F.col("ic.investigation_case_key"), "left")
        .select(
            F.col("ic.investigation_case_key"),
            F.col("ic.investigation_type"),
            F.col("ic.case_origin"),
            F.col("ic.case_status"),
            F.col("ic.priority"),
            F.col("ic.opened_at"),
            F.col("ic.closed_at"),
            F.col("ic.assigned_analyst_id"),
            F.col("ac.party_key"),
            F.col("ac.aml_case_key"),
            F.col("ac.risk_level").alias("aml_risk_level"),
            F.col("sl.filed_date").isNotNull().alias("sar_filed_flag"),
            F.col("sl.filed_date").alias("sar_filed_date"),
            F.col("sl.regulatory_reference").alias("sar_regulatory_reference"),
            F.col("sl.report_status").alias("sar_report_status"),
            F.coalesce(F.col("sa.sanctions_screening_count"), F.lit(0)).alias("sanctions_screening_count"),
            F.coalesce(F.col("sa.sanctions_hit_flag"), F.lit(False)).alias("sanctions_hit_flag"),
            F.col("sa.max_sanctions_match_score"),
            F.col("sa.linked_watchlist_types"),
            F.coalesce(F.col("faa.linked_fraud_alert_count"), F.lit(0)).alias("linked_fraud_alert_count"),
            F.coalesce(F.col("maa.linked_monitoring_alert_count"), F.lit(0)).alias("linked_monitoring_alert_count"),
            F.coalesce(F.col("ea.linked_financial_event_count"), F.lit(0)).alias("linked_financial_event_count"),
            F.coalesce(F.col("na.investigation_note_count"), F.lit(0)).alias("investigation_note_count"),
            F.col("na.latest_note_text"),
            F.col("ic.source_system"),
            F.col("ic.source_business_key"),
            F.col("ic.ingested_at"),
            F.col("ic.pipeline_run_id"),
            F.when(F.col("ac.aml_case_key").isNotNull() & F.col("sl.regulatory_reference").isNull() & (F.col("ic.case_status") == "CLOSED"), "PENDING_REGULATORY_REF")
            .otherwise("PASSED_CLEAN").alias("dq_status"),
        )
    )
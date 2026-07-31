from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -----------------------------------------------------------------------------
# CONFIGURATION & SCHEMA RESOLUTION
# -----------------------------------------------------------------------------
CATALOG = spark.conf.get("pipeline.catalog", "workspace")
SILVER_SCHEMA = spark.conf.get("pipeline.silver_schema", "silver")
GOLD_SCHEMA = spark.conf.get("pipeline.gold_schema", "gold")

def silver_ref(table_name: str) -> str:
    """Đường dẫn Source: Đọc dữ liệu từ bảng thuộc Silver Schema."""
    return f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"

def gold_target_name(table_name: str) -> str:
    """Đường dẫn Destination: Đặt tên target table kèm Gold Schema."""
    return f"{GOLD_SCHEMA}.{table_name}" if GOLD_SCHEMA else table_name


# NOTE: pipeline_run_id is now a native column on every Silver table
# (Silver transform fix), so it is selected directly from each view's
# driving table below — no more join to governance.pipeline_run needed.


# =============================================================================
# VIEW 1/3 — gold.ai_fraud_transaction_context
# =============================================================================

@dp.table(
    name=gold_target_name("ai_fraud_transaction_context"),
    comment="AI-Ready fraud context: one row per financial event, flattened with merchant/channel/risk/alert context. PII-free.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "delta.clusterBy": "occurred_at, party_key"
    }
)
@dp.expect_or_drop("valid_financial_event_key", "financial_event_key IS NOT NULL")
def ai_fraud_transaction_context():
    # 1. Source Data Streams / Views from Silver Schema
    fe = spark.read.table(silver_ref("financial_event"))

    # Các bảng lookup/dimension dùng spark.read.table (Batch)
    ap = spark.read.table(silver_ref("account_posting"))
    cp = spark.read.table(silver_ref("card_payment"))
    atm = spark.read.table(silver_ref("atm_activity"))
    gp = spark.read.table(silver_ref("gateway_payment"))
    tc = spark.read.table(silver_ref("transaction_channel"))
    m = spark.read.table(silver_ref("merchant"))
    ml = spark.read.table(silver_ref("merchant_location"))
    fers = spark.read.table(silver_ref("financial_event_risk_score"))
    fefa = spark.read.table(silver_ref("financial_event_fraud_alert"))
    fa = spark.read.table(silver_ref("fraud_alert"))
    cfa_src = spark.read.table(silver_ref("financial_event_card_fraud_flag"))

    # 2. Aggregations & Transformations
    risk_window = Window.partitionBy("financial_event_key").orderBy(F.col("scored_date").desc())
    latest_risk = fers.withColumn("rn", F.row_number().over(risk_window)).filter("rn = 1")

    fraud_alert_agg = (
        fefa.join(fa, "fraud_alert_key")
        .groupBy("financial_event_key")
        .agg(
            F.countDistinct("fraud_alert_key").alias("fraud_alert_count"),
            F.max("alert_score").alias("max_fraud_alert_score"),
            F.max(F.col("alert_status") != "CLOSED").alias("open_fraud_alert_flag")
        )
    )

    card_flag_agg = (
        cfa_src.groupBy("financial_event_key")
        .agg(F.countDistinct("card_fraud_flag_key").alias("card_fraud_flag_count"))
    )

    # 3. Main Join Query
    return (
        fe.alias("fe")
        .join(ap.alias("ap"), (F.col("ap.financial_event_key") == F.col("fe.financial_event_key")) & (F.col("fe.event_type") == "ACCOUNT_POSTING"), "left")
        .join(cp.alias("cp"), (F.col("cp.financial_event_key") == F.col("fe.financial_event_key")) & (F.col("fe.event_type") == "CARD_PAYMENT"), "left")
        .join(atm.alias("atm"), (F.col("atm.financial_event_key") == F.col("fe.financial_event_key")) & (F.col("fe.event_type") == "ATM_ACTIVITY"), "left")
        .join(gp.alias("gp"), (F.col("gp.financial_event_key") == F.col("fe.financial_event_key")) & (F.col("fe.event_type") == "GATEWAY_PAYMENT"), "left")
        .join(tc.alias("tc"), F.col("tc.channel_key") == F.col("ap.channel_key"), "left")
        .join(m.alias("m"), F.col("m.merchant_key") == F.coalesce(F.col("ap.merchant_key"), F.col("cp.merchant_key"), F.col("gp.merchant_key")), "left")
        .join(ml.alias("ml"), F.col("ml.merchant_location_key") == F.col("fe.merchant_location_key"), "left")
        .join(latest_risk.alias("lr"), F.col("lr.financial_event_key") == F.col("fe.financial_event_key"), "left")
        .join(fraud_alert_agg.alias("faa"), F.col("faa.financial_event_key") == F.col("fe.financial_event_key"), "left")
        .join(card_flag_agg.alias("cfa"), F.col("cfa.financial_event_key") == F.col("fe.financial_event_key"), "left")
        .select(
            F.col("fe.financial_event_key"),
            F.col("fe.event_type"),
            F.col("fe.occurred_at"),
            F.coalesce(F.col("ap.posting_amount"), F.col("cp.payment_amount"), F.col("atm.amount"), F.col("gp.amount")).alias("amount"),
            F.col("fe.currency"),
            F.coalesce(F.col("ap.posting_type"), F.col("cp.card_transaction_type"), F.col("atm.activity_type"), F.col("gp.payment_method")).alias("transaction_type_detail"),
            F.col("ap.posting_direction"),
            F.col("fe.account_key"),
            F.col("fe.payment_card_key"),
            F.col("fe.party_key"),
            F.col("ap.channel_key"),
            F.col("tc.channel_name"),
            F.col("tc.channel_type"),
            F.coalesce(F.col("ap.merchant_key"), F.col("cp.merchant_key"), F.col("gp.merchant_key")).alias("merchant_key"),
            F.col("m.merchant_name"),
            F.col("m.mcc_code"),
            F.col("m.country").alias("merchant_country"),
            F.col("fe.merchant_location_key"),
            F.col("ml.store_name"),
            F.col("ml.risk_rating").alias("store_risk_rating"),
            F.col("lr.model_score").alias("latest_risk_score"),
            F.col("lr.risk_band").alias("latest_risk_band"),
            F.coalesce(F.col("faa.fraud_alert_count"), F.lit(0)).alias("fraud_alert_count"),
            F.col("faa.max_fraud_alert_score"),
            F.coalesce(F.col("faa.open_fraud_alert_flag"), F.lit(False)).alias("open_fraud_alert_flag"),
            F.coalesce(F.col("cfa.card_fraud_flag_count"), F.lit(0)).alias("card_fraud_flag_count"),
            F.col("cp.is_fraud_source_flag"),
            F.col("fe.source_system"),
            F.col("fe.source_business_key"),
            F.col("fe.ingested_at"),
            F.col("fe.pipeline_run_id"),
            F.when(F.col("fe.data_quality_status") == "QUARANTINED", "REJECTED_QUALITY")
             .when(F.col("fe.party_key").isNull(), "WARNING_UNRESOLVED_PARTY")
             .otherwise("PASSED_CLEAN").alias("dq_status")
        )
    )


# =============================================================================
# VIEW 2/3 — gold.ai_customer_360_context
# =============================================================================

@dp.table(
    name=gold_target_name("ai_customer_360_context"),
    comment="AI-Ready Customer 360 context: current profile + KYC + account/card/servicing overview, PII tokenized/banded.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "delta.clusterBy": "party_key"
    }
)
@dp.expect_or_drop("valid_party_key", "party_key IS NOT NULL")
def ai_customer_360_context():
    # 1. Source Tables from Silver Schema
    p = spark.read.table(silver_ref("party"))
    ppv = spark.read.table(silver_ref("party_profile_version"))
    pka = spark.read.table(silver_ref("party_kyc_assessment"))
    pe = spark.read.table(silver_ref("party_employment"))
    par = spark.read.table(silver_ref("party_account_role"))
    a = spark.read.table(silver_ref("account"))
    abs_df = spark.read.table(silver_ref("account_balance_snapshot"))
    pc = spark.read.table(silver_ref("payment_card"))
    psr = spark.read.table(silver_ref("party_service_request"))
    ccc = spark.read.table(silver_ref("call_center_contact"))
    ac = spark.read.table(silver_ref("aml_case"))
    ic = spark.read.table(silver_ref("investigation_case"))

    # 2. Transformations
    current_profile = ppv.filter(F.col("is_current") == True)

    kyc_window = Window.partitionBy("party_key").orderBy(F.col("verified_date").desc())
    latest_kyc = pka.withColumn("rn", F.row_number().over(kyc_window)).filter("rn = 1")

    current_employment = pe.filter(F.col("effective_to").isNull())

    # account_balance_snapshot is a very large, fast-growing table (millions of
    # accounts x daily snapshots) — filter to a recent window BEFORE running
    # the window function, instead of ranking over the entire history.
    # NOTE trade-off: an account whose most recent snapshot is older than 30
    # days (e.g. a dormant/closed account not snapshotted daily) will have no
    # matching row here and will show a NULL total_current_balance below.
    # Widen the window if that's not acceptable for your dormant-account cases.
    abs_recent = abs_df.filter(F.col("balance_date") >= F.date_sub(F.current_date(), 30))
    bal_window = Window.partitionBy("account_key").orderBy(F.col("balance_date").desc())
    latest_bal = abs_recent.withColumn("rn", F.row_number().over(bal_window)).filter("rn = 1")

    # Card counts pre-aggregated PER ACCOUNT first, so joining them into the
    # per-account grain below does not fan out account_key (and therefore
    # does not multiply closing_balance when summed at party level).
    card_count_per_account = (
        pc.groupBy("account_key")
        .agg(F.countDistinct(F.when(F.col("card_status") == "ACTIVE", F.col("payment_card_key"))).alias("active_card_count"))
    )

    # One row per account (party_account_role x account), each carrying its
    # own latest balance and its own pre-aggregated card count — no fan-out.
    account_level = (
        par.alias("par")
        .join(a.alias("a"), F.col("a.account_key") == F.col("par.account_key"))
        .join(latest_bal.alias("lb"), F.col("lb.account_key") == F.col("a.account_key"), "left")
        .join(card_count_per_account.alias("cca"), F.col("cca.account_key") == F.col("a.account_key"), "left")
        .select(
            F.col("par.party_key"),
            F.col("a.account_key"),
            F.col("par.valid_to"),
            F.col("a.account_status"),
            F.col("lb.closing_balance"),
            F.coalesce(F.col("cca.active_card_count"), F.lit(0)).alias("active_card_count"),
        )
    )

    account_overview = (
        account_level.groupBy("party_key")
        .agg(
            F.countDistinct(F.when(F.col("valid_to").isNull() & (F.col("account_status") == "ACTIVE"), F.col("account_key"))).alias("active_account_count"),
            F.sum("closing_balance").alias("total_current_balance"),
            F.sum("active_card_count").alias("active_card_count"),
        )
    )

    service_overview = (
        psr.filter(~F.col("request_status").isin("RESOLVED", "REJECTED"))
        .groupBy("party_key")
        .agg(F.count("*").alias("open_service_request_count"))
    )

    call_overview = (
        ccc.groupBy("party_key")
        .agg(
            F.count(F.when(F.col("call_timestamp") >= F.date_sub(F.current_date(), 90), 1)).alias("call_center_contact_count_90d"),
            F.expr("max_by(call_reason, call_timestamp)").alias("last_call_reason")
        )
    )

    investigation_overview = (
        ac.join(ic, "investigation_case_key")
        .filter(~F.col("case_status").isin("CLOSED", "RESOLVED"))
        .select("party_key")
        .distinct()
        .withColumn("open_investigation_flag", F.lit(True))
    )

    # 3. Main Query
    return (
        p.alias("p")
        .join(current_profile.alias("cpv"), F.col("cpv.party_key") == F.col("p.party_key"), "left")
        .join(latest_kyc.alias("lk"), F.col("lk.party_key") == F.col("p.party_key"), "left")
        .join(current_employment.alias("ce"), F.col("ce.party_key") == F.col("p.party_key"), "left")
        .join(account_overview.alias("ao"), F.col("ao.party_key") == F.col("p.party_key"), "left")
        .join(service_overview.alias("so"), F.col("so.party_key") == F.col("p.party_key"), "left")
        .join(call_overview.alias("co"), F.col("co.party_key") == F.col("p.party_key"), "left")
        .join(investigation_overview.alias("io"), F.col("io.party_key") == F.col("p.party_key"), "left")
        .select(
            F.col("p.party_key"),
            F.col("p.party_type"),
            F.col("p.party_status"),
            F.col("cpv.preferred_contact_method"),
            F.col("cpv.effective_from").alias("profile_effective_from"),
            F.col("lk.verification_status").alias("kyc_verification_status"),
            F.col("lk.id_type").alias("kyc_id_type"),
            F.col("lk.id_number_token").alias("kyc_id_number_token"),
            F.col("ce.employer_name"),
            F.col("ce.job_title"),
            F.when(F.col("ce.monthly_income").isNull(), F.lit(None))
             .when(F.col("ce.monthly_income") < 10000000, "<10M")
             .when(F.col("ce.monthly_income") < 30000000, "10-30M")
             .when(F.col("ce.monthly_income") < 100000000, "30-100M")
             .otherwise("100M+").alias("monthly_income_band"),
            F.coalesce(F.col("ao.active_account_count"), F.lit(0)).alias("active_account_count"),
            F.col("ao.total_current_balance"),
            F.coalesce(F.col("ao.active_card_count"), F.lit(0)).alias("active_card_count"),
            F.coalesce(F.col("so.open_service_request_count"), F.lit(0)).alias("open_service_request_count"),
            F.coalesce(F.col("co.call_center_contact_count_90d"), F.lit(0)).alias("call_center_contact_count_90d"),
            F.col("co.last_call_reason"),
            F.coalesce(F.col("io.open_investigation_flag"), F.lit(False)).alias("open_investigation_flag"),
            F.col("p.source_system"),
            F.col("p.source_business_key"),
            F.col("p.ingested_at"),
            F.col("p.pipeline_run_id"),
            F.when(F.col("p.data_quality_status") == "QUARANTINED", "REJECTED_QUALITY")
             .when(F.col("lk.verification_status").isNull() | (F.col("lk.verification_status") != "VERIFIED"), "WARNING_UNRESOLVED_PARTY")
             .otherwise("PASSED_CLEAN").alias("dq_status")
        )
    )


# =============================================================================
# VIEW 3/3 — gold.ai_aml_investigation_context
# =============================================================================

@dp.table(
    name=gold_target_name("ai_aml_investigation_context"),
    comment="AI-Ready AML/Fraud investigation context: one row per investigation case, flattened with AML detail, SAR filing status, etc.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "delta.clusterBy": "case_status, opened_at"
    }
)
@dp.expect_or_drop("valid_investigation_case_key", "investigation_case_key IS NOT NULL")
def ai_aml_investigation_context():
    # 1. Source Tables from Silver Schema
    ic = spark.read.table(silver_ref("investigation_case"))
    ac = spark.read.table(silver_ref("aml_case"))
    sar = spark.read.table(silver_ref("suspicious_activity_report"))
    icss = spark.read.table(silver_ref("investigation_case_sanctions_screening"))
    ss = spark.read.table(silver_ref("sanctions_screening"))
    we = spark.read.table(silver_ref("watchlist_entry"))
    icfa = spark.read.table(silver_ref("investigation_case_fraud_alert"))
    icma = spark.read.table(silver_ref("investigation_case_monitoring_alert"))
    icfe = spark.read.table(silver_ref("investigation_case_financial_event"))
    in_note = spark.read.table(silver_ref("investigation_note"))

    # 2. Transformations
    # Guard against investigation_case:aml_case being 1-N (e.g. several AML
    # cases rolled into one larger investigation) — dedupe to one aml_case
    # per investigation_case_key BEFORE joining, the same pattern used for
    # account_overview in View 2, so this view's grain (1 row / investigation
    # case) can't fan out. Picks the most recently opened linked aml_case;
    # if a case genuinely has multiple aml_cases, the others' detail (e.g.
    # additional SAR filings) won't surface here — add a
    # linked_aml_case_count column if that visibility is needed later.
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
            F.array_join(F.collect_set("list_type"), ", ").alias("linked_watchlist_types")
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
            F.expr("max_by(note_text, note_timestamp)").alias("latest_note_text")
        )
    )

    # 3. Main Query
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
             .otherwise("PASSED_CLEAN").alias("dq_status")
        )
    )



"""Row-level quarantine rules for the Financial Crime domain.

Status reconciliation, stale-reference flagging, and arrival ordering are
handled downstream and therefore are deliberately not quarantine predicates.
"""

RULES = [
    {
        "name": "account_transaction_risk_score__account_txn_id__resolved",
        "constraint": "(account_txn_id <> -1)",
        "table": "account_transaction_risk_score",
    },
    {
        "name": "fraud_alert__account_txn_id__resolved",
        "constraint": "(account_txn_id <> -1)",
        "table": "fraud_alert",
    },
    {
        "name": "fraud_alert__alert_score__in_range",
        "constraint": "(alert_score >= 0 AND alert_score <= 100)",
        "table": "fraud_alert",
    },
    {
        "name": "transaction_monitoring_alert__alert_score__in_range",
        "constraint": "(alert_score >= 0 AND alert_score <= 100)",
        "table": "transaction_monitoring_alert",
    },
    {
        "name": "transaction_monitoring_alert_account_transaction__account_txn_id__resolved",
        "constraint": "(account_txn_id <> -1)",
        "table": "transaction_monitoring_alert_account_transaction",
    },
    {
        "name": "transaction_monitoring_alert_card_transaction__card_txn_id__resolved",
        "constraint": "(card_txn_id <> -1)",
        "table": "transaction_monitoring_alert_card_transaction",
    },
    {
        "name": "investigation_case_transaction_monitoring_alert__alert_id__resolved",
        "constraint": "(alert_id <> 'UNKNOWN-MONITORING-ALERT')",
        "table": "investigation_case_transaction_monitoring_alert",
    },
    {
        "name": "aml_case__investigation_case_id__resolved",
        "constraint": "(investigation_case_id <> 'UNKNOWN-CASE')",
        "table": "aml_case",
    },
    {
        "name": "sanction_screening__screened_name__not_truncated",
        "constraint": "(screened_name IS NOT NULL AND LENGTH(TRIM(screened_name)) >= 3)",
        "table": "sanction_screening",
    },
    {
        "name": "suspicious_activity_report__case_id__resolved",
        "constraint": "(case_id <> 'UNKNOWN-AML-CASE')",
        "table": "suspicious_activity_report",
    },
    {
        "name": "chargeback__reason_code__supported",
        "constraint": "(reason_code <> 'UNKNOWN')",
        "table": "chargeback",
    },
    {
        "name": "investigation_case_account_transaction__account_txn_id__resolved",
        "constraint": "(account_txn_id <> -1)",
        "table": "investigation_case_account_transaction",
    },
    {
        "name": "investigation_case_card_transaction__card_txn_id__resolved",
        "constraint": "(card_txn_id <> -1)",
        "table": "investigation_case_card_transaction",
    },
    {
        "name": "investigation_case_fraud_alert__alert_id__resolved",
        "constraint": "(alert_id <> 'UNKNOWN-ALERT')",
        "table": "investigation_case_fraud_alert",
    },
    {
        "name": "investigation_case_sanction_screening__screening_id__resolved",
        "constraint": "(screening_id <> 'UNKNOWN-SCREENING')",
        "table": "investigation_case_sanction_screening",
    },
]

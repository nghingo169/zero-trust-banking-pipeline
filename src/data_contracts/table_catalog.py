"""Single source of truth for development table identity and ingestion semantics."""

DOMAINS = {
    "card": {
        "scd2": {"card": "card_id", "card_transaction": "card_txn_id", "card_fraud_flag": "flag_id", "card_limit_history": "history_id"},
        "append": {"card_transaction_status_event": "status_event_id"},
    },
    "customer": {
        "scd2": {"core_banking_customer": "cust_no", "crm_customer": "party_id", "customer_kyc": "kyc_id", "customer_employment": "employment_id", "account": "account_id", "customer_account": "link_id", "customer_request": "request_id"},
        "append": {},
    },
    "transaction": {
        "scd2": {"account_transaction": "account_txn_id", "log_atm": "log_id", "payment_gateway_log": "gateway_txn_id", "transaction_channel": "channel_id", "merchant": "merchant_id", "merchant_store": "store_id", "balance_snapshot": "balance_id"},
        "append": {"account_transaction_status_event": "status_event_id", "atm_transaction_status_event": "status_event_id", "payment_gateway_status_event": "status_event_id"},
    },
    "fincrime": {
        "scd2": {"fraud_alert": "alert_id", "transaction_monitoring_alert": "alert_id", "transaction_monitoring_alert_account_transaction": "alert_account_txn_link_id", "transaction_monitoring_alert_card_transaction": "alert_card_txn_link_id", "investigation_case_transaction_monitoring_alert": "case_alert_link_id", "investigation_case_card_fraud_flag": "case_flag_link_id", "investigation_case": "case_id", "investigation_case_account_transaction": "case_account_txn_link_id", "investigation_case_card_transaction": "case_card_txn_link_id", "investigation_case_fraud_alert": "case_alert_link_id", "investigation_case_sanction_screening": "case_screening_link_id", "investigation_note": "note_id", "aml_case": "case_id", "sanction_screening": "screening_id", "suspicious_activity_report": "sar_id", "watchlist": "watchlist_id", "account_transaction_risk_score": "score_id", "call_center_log": "call_id", "chargeback": "chargeback_id"},
        "append": {},
    },
}


def tables(domain):
    """Return all domain tables mapped to their stable business key."""
    return {**DOMAINS[domain]["scd2"], **DOMAINS[domain]["append"]}

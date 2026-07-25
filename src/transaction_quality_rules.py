"""Row-level quarantine rules for customer transactions and operational logs."""

RULES = [
    {
        "name": "account_transaction__account_id__resolved",
        "constraint": "(account_id <> -1)",
        "table": "account_transaction",
    },
    {
        "name": "account_transaction__credit_amount__non_negative",
        "constraint": "(NOT (direction = 'CREDIT' AND amount < 0))",
        "table": "account_transaction",
    },
    {
        "name": "account_transaction_status_event__account_txn_id__resolved",
        "constraint": "(account_txn_id <> -1)",
        "table": "account_transaction_status_event",
    },
    {
        "name": "merchant_store__store_description__not_null",
        "constraint": "(store_description IS NOT NULL)",
        "table": "merchant_store",
    },
    {
        "name": "merchant_store__risk_rating__known",
        "constraint": "(risk_rating <> 'UNKNOWN')",
        "table": "merchant_store",
    },
    {
        "name": "log_atm__card_number__not_invalid_pan",
        "constraint": "(card_number <> 'INVALID-PAN')",
        "table": "log_atm",
    },
    {
        "name": "log_atm__response_code__supported",
        "constraint": "(response_code <> 'UNKNOWN')",
        "table": "log_atm",
    },
    {
        "name": "atm_transaction_status_event__log_id__resolved",
        "constraint": "(log_id <> 'UNKNOWN-ATM-LOG')",
        "table": "atm_transaction_status_event",
    },
    {
        "name": "payment_gateway_log__currency__valid",
        "constraint": "(currency <> 'XXX')",
        "table": "payment_gateway_log",
    },
    {
        "name": "payment_gateway_status_event__gateway_txn_id__resolved",
        "constraint": "(gateway_txn_id <> 'UNKNOWN-GATEWAY-LOG')",
        "table": "payment_gateway_status_event",
    },
    {
        "name": "balance_snapshot__closing_balance__non_negative",
        "constraint": "(closing_balance >= 0)",
        "table": "balance_snapshot",
    },
]

"""Lakeflow expectations for the Card domain.

This follows the Databricks Python-module pattern: rules are plain
dictionaries, and pipeline code selects the rules for one table before passing
them to ``@dp.expect_all``. No data-contract YAML is read at runtime.
"""

RULES = [
    # card
    {
        "name": "card__card_id__not_null",
        "constraint": "(card_id IS NOT NULL)",
        "table": "card",
    },
    {
        "name": "card__account_id__not_null",
        "constraint": "(account_id IS NOT NULL)",
        "table": "card",
    },
    {
        "name": "card__card_number__not_null",
        "constraint": "(card_number IS NOT NULL)",
        "table": "card",
    },
    {
        "name": "card__card_number__not_invalid_pan",
        "constraint": "(card_number <> 'INVALID-PAN')",
        "table": "card",
    },
    {
        "name": "card__account_id__resolved",
        "constraint": "(account_id <> -1)",
        "table": "card",
    },
    {
        "name": "card__card_type__not_null",
        "constraint": "(card_type IS NOT NULL)",
        "table": "card",
    },
    {
        "name": "card__issue_date__not_null",
        "constraint": "(issue_date IS NOT NULL)",
        "table": "card",
    },
    {
        "name": "card__expiry_date__not_null",
        "constraint": "(expiry_date IS NOT NULL)",
        "table": "card",
    },
    {
        "name": "card__status__not_null",
        "constraint": "(status IS NOT NULL)",
        "table": "card",
    },
    # card_transaction
    {
        "name": "card_transaction__card_txn_id__not_null",
        "constraint": "(card_txn_id IS NOT NULL)",
        "table": "card_transaction",
    },
    {
        "name": "card_transaction__card_id__not_null",
        "constraint": "(card_id IS NOT NULL)",
        "table": "card_transaction",
    },
    {
        "name": "card_transaction__merchant_id__not_null",
        "constraint": "(merchant_id IS NOT NULL)",
        "table": "card_transaction",
    },
    {
        "name": "card_transaction__amount__not_null",
        "constraint": "(amount IS NOT NULL)",
        "table": "card_transaction",
    },
    {
        "name": "card_transaction__txn_timestamp__not_null",
        "constraint": "(txn_timestamp IS NOT NULL)",
        "table": "card_transaction",
    },
    {
        "name": "card_transaction__txn_status__not_null",
        "constraint": "(txn_status IS NOT NULL)",
        "table": "card_transaction",
    },
    {
        "name": "card_transaction__is_fraud__not_null",
        "constraint": "(is_fraud IS NOT NULL)",
        "table": "card_transaction",
    },
    {
        "name": "card_transaction__card_id__resolved",
        "constraint": "(card_id <> 'UNKNOWN-CARD')",
        "table": "card_transaction",
    },
    # card_fraud_flag
    {
        "name": "card_fraud_flag__flag_id__not_null",
        "constraint": "(flag_id IS NOT NULL)",
        "table": "card_fraud_flag",
    },
    {
        "name": "card_fraud_flag__card_txn_id__not_null",
        "constraint": "(card_txn_id IS NOT NULL)",
        "table": "card_fraud_flag",
    },
    {
        "name": "card_fraud_flag__flag_reason__not_null",
        "constraint": "(flag_reason IS NOT NULL)",
        "table": "card_fraud_flag",
    },
    {
        "name": "card_fraud_flag__flag_date__not_null",
        "constraint": "(flag_date IS NOT NULL)",
        "table": "card_fraud_flag",
    },
    {
        "name": "card_fraud_flag__resolved_status__not_null",
        "constraint": "(resolved_status IS NOT NULL)",
        "table": "card_fraud_flag",
    },
    {
        "name": "card_fraud_flag__card_txn_id__resolved",
        "constraint": "(card_txn_id <> -1)",
        "table": "card_fraud_flag",
    },
    # card_limit_history
    {
        "name": "card_limit_history__history_id__not_null",
        "constraint": "(history_id IS NOT NULL)",
        "table": "card_limit_history",
    },
    {
        "name": "card_limit_history__card_id__not_null",
        "constraint": "(card_id IS NOT NULL)",
        "table": "card_limit_history",
    },
    {
        "name": "card_limit_history__limit_amount__not_null",
        "constraint": "(limit_amount IS NOT NULL)",
        "table": "card_limit_history",
    },
    {
        "name": "card_limit_history__effective_date__not_null",
        "constraint": "(effective_date IS NOT NULL)",
        "table": "card_limit_history",
    },
    {
        "name": "card_limit_history__card_id__resolved",
        "constraint": "(card_id <> 'UNKNOWN-CARD')",
        "table": "card_limit_history",
    },
    # card_transaction_status_event
    {
        "name": "card_transaction_status_event__status_event_id__not_null",
        "constraint": "(status_event_id IS NOT NULL)",
        "table": "card_transaction_status_event",
    },
    {
        "name": "card_transaction_status_event__card_txn_id__not_null",
        "constraint": "(card_txn_id IS NOT NULL)",
        "table": "card_transaction_status_event",
    },
    {
        "name": "card_transaction_status_event__status__not_null",
        "constraint": "(status IS NOT NULL)",
        "table": "card_transaction_status_event",
    },
    {
        "name": "card_transaction_status_event__status_timestamp__not_null",
        "constraint": "(status_timestamp IS NOT NULL)",
        "table": "card_transaction_status_event",
    },
    {
        "name": "card_transaction_status_event__status_timestamp__sane_range",
        "constraint": "(status_timestamp >= TIMESTAMP '2020-01-01 00:00:00' AND status_timestamp <= current_timestamp())",
        "table": "card_transaction_status_event",
    },
    {
        "name": "card_transaction_status_event__source_arrival_timestamp__not_null",
        "constraint": "(source_arrival_timestamp IS NOT NULL)",
        "table": "card_transaction_status_event",
    },
    {
        "name": "card_transaction_status_event__sequence_number__not_null",
        "constraint": "(sequence_number IS NOT NULL)",
        "table": "card_transaction_status_event",
    },
    {
        "name": "card_transaction_status_event__card_txn_id__resolved",
        "constraint": "(card_txn_id <> -1)",
        "table": "card_transaction_status_event",
    },
]

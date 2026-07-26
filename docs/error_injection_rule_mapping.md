# Error injection to quality-rule mapping

This checklist maps the deliberate defects in
[`banking_error_injection_catalog.md`](banking_error_injection_catalog.md) to
the implemented handling. A **quarantine rule** is evaluated per row and its
failure sets `is_quarantined=true`; its SQL is available through
`data_contracts.quality_rules.registry.get_rules(table)`. Other catalog actions need cross-row,
cross-table, or ingestion-level processing and are not represented as a
row-level quarantine predicate.

## Customer

Registry: [`src/domains/rules/customer.py`](../src/domains/rules/customer.py)

| Table.field | Injected value or condition | Handling | Implemented rule / check |
|---|---|---|---|
| `core_banking_customer.national_id` | Duplicate value | Deduplicate | No row predicate; requires duplicate-value detection. |
| `core_banking_customer.date_of_birth` | `NULL` | Quarantine field | `core_banking_customer__date_of_birth__not_null`: `date_of_birth IS NOT NULL` |
| `core_banking_customer.national_id` | `NULL` | Quarantine field | `core_banking_customer__national_id__not_null`: `national_id IS NOT NULL` |
| `core_banking_customer.phone` | `0000000000` | Quarantine field | `core_banking_customer__phone__not_placeholder`: `phone <> '0000000000'` |
| `crm_customer.national_id` | `NULL` | Quarantine field | `crm_customer__national_id__not_null`: `national_id IS NOT NULL` |
| `crm_customer.email` | `NULL` | Quarantine field | `crm_customer__email__not_null`: `email IS NOT NULL` |
| `crm_customer.email` | `malformed-email` | Quarantine field | `crm_customer__email__not_malformed`: `email <> 'malformed-email'` |
| `customer_kyc.customer_ref` | `UNKNOWN-CUSTOMER` | Quarantine record | `customer_kyc__customer_ref__resolved` |
| `customer_kyc.id_number` | `INVALID-ID` | Quarantine field | `customer_kyc__id_number__not_invalid` |
| `customer_employment.customer_ref` | `UNKNOWN-CUSTOMER` | Quarantine record | `customer_employment__customer_ref__resolved` |
| `customer_employment.monthly_income` | Negative (`-1000.00`) | Quarantine record | `customer_employment__monthly_income__non_negative`: `monthly_income >= 0` |
| `customer_request.customer_ref` | `UNKNOWN-CUSTOMER` | Quarantine record | `customer_request__customer_ref__resolved` |
| `customer_request.description` | `NULL` | Quarantine field | `customer_request__description__not_null` |
| `account.cif_number` | `CIF99999999` | Quarantine record | `account__cif_number__resolved` |
| `customer_account.account_id` | `-1` | Quarantine record | `customer_account__account_id__resolved` |

## Customer transactions and operational logs

Registry: [`src/domains/rules/transaction.py`](../src/domains/rules/transaction.py)

| Table.field | Injected value or condition | Handling | Implemented rule / check |
|---|---|---|---|
| `account_transaction.account_id` | `-1` | Quarantine record | `account_transaction__account_id__resolved` |
| `account_transaction.amount` | Negative on a `CREDIT` | Quarantine record | `account_transaction__credit_amount__non_negative`: not `(direction = 'CREDIT' AND amount < 0)` |
| `account_transaction.status` | `SUCCESS` changed to conflicting `FAILED` | Reconcile latest status | No row predicate; compare with status-event history. |
| `account_transaction_status_event.account_txn_id` | `-1` | Quarantine record | `account_transaction_status_event__account_txn_id__resolved` |
| `account_transaction_status_event.source_arrival_timestamp` | Late sequence-0 arrival | Process by arrival order | No quarantine; preserve event and order by arrival timestamp. |
| `account_transaction_status_event` row | Replay duplicate | Deduplicate | No row predicate; deduplicate on event identity. |
| `merchant_store.store_description` | `NULL` | Quarantine field | `merchant_store__store_description__not_null` |
| `merchant_store.risk_rating` | `UNKNOWN` | Quarantine field | `merchant_store__risk_rating__known` |
| `log_atm.card_number` | `INVALID-PAN` | Quarantine field | `log_atm__card_number__not_invalid_pan` |
| `log_atm.response_code` | `UNKNOWN` | Quarantine field | `log_atm__response_code__supported` |
| `atm_transaction_status_event.log_id` | `UNKNOWN-ATM-LOG` | Quarantine record | `atm_transaction_status_event__log_id__resolved` |
| `payment_gateway_log.currency` | `XXX` | Quarantine field | `payment_gateway_log__currency__valid` |
| `payment_gateway_status_event.gateway_txn_id` | `UNKNOWN-GATEWAY-LOG` | Quarantine record | `payment_gateway_status_event__gateway_txn_id__resolved` |
| `balance_snapshot.closing_balance` | Negative on savings (`-250.00`) | Quarantine record | `balance_snapshot__closing_balance__non_negative`: `closing_balance >= 0` |

## Card

Registry: [`src/domains/rules/card.py`](../src/domains/rules/card.py)

| Table.field | Injected value | Handling | Implemented rule |
|---|---|---|---|
| `card.account_id` | `-1` | Quarantine record | `card__account_id__resolved` |
| `card.card_number` | `INVALID-PAN` | Quarantine field | `card__card_number__not_invalid_pan` |
| `card_transaction.card_id` | `UNKNOWN-CARD` | Quarantine record | `card_transaction__card_id__resolved` |
| `card_transaction_status_event.card_txn_id` | `-1` | Quarantine record | `card_transaction_status_event__card_txn_id__resolved` |
| `card_fraud_flag.card_txn_id` | `-1` | Quarantine record | `card_fraud_flag__card_txn_id__resolved` |
| `card_limit_history.card_id` | `UNKNOWN-CARD` | Quarantine record | `card_limit_history__card_id__resolved` |

## Financial crime

Registry: [`src/domains/rules/fincrime.py`](../src/domains/rules/fincrime.py)

| Table.field | Injected value or condition | Handling | Implemented rule / check |
|---|---|---|---|
| `account_transaction_risk_score.account_txn_id` | `-1` | Quarantine record | `account_transaction_risk_score__account_txn_id__resolved` |
| `fraud_alert.account_txn_id` | `-1` | Quarantine record | `fraud_alert__account_txn_id__resolved` |
| `fraud_alert.alert_score` | `101.00` | Quarantine record | `fraud_alert__alert_score__in_range`: `0 <= alert_score <= 100` |
| `transaction_monitoring_alert.alert_score` | `101.00` | Quarantine record | `transaction_monitoring_alert__alert_score__in_range` |
| `transaction_monitoring_alert.alert_status` | Conflicting `CLOSED` | Reconcile latest status | No row predicate; compare with investigation links. |
| `transaction_monitoring_alert_account_transaction.account_txn_id` | `-1` | Quarantine record | `transaction_monitoring_alert_account_transaction__account_txn_id__resolved` |
| `transaction_monitoring_alert_card_transaction.card_txn_id` | `-1` | Quarantine record | `transaction_monitoring_alert_card_transaction__card_txn_id__resolved` |
| `investigation_case_transaction_monitoring_alert.alert_id` | `UNKNOWN-MONITORING-ALERT` | Quarantine record | `investigation_case_transaction_monitoring_alert__alert_id__resolved` |
| `aml_case.investigation_case_id` | `UNKNOWN-CASE` | Quarantine record | `aml_case__investigation_case_id__resolved` |
| `sanction_screening.screened_name` | Truncated value | Quarantine field | `sanction_screening__screened_name__not_truncated`: non-null, trimmed length ≥ 3 |
| `sanction_screening.result` | Conflicting `CLEAR` for a `HIT` | Reconcile latest status | No row predicate; compare with screening evidence. |
| `suspicious_activity_report.case_id` | `UNKNOWN-AML-CASE` | Quarantine record | `suspicious_activity_report__case_id__resolved` |
| `watchlist.added_date` | 3,650 days old | Flag stale | No quarantine; report stale reference data. |
| `chargeback.reason_code` | `UNKNOWN` | Quarantine field | `chargeback__reason_code__supported` |
| `investigation_note.source_arrival_timestamp` | Late arrival | Process by arrival order | No quarantine; preserve business chronology. |
| `investigation_case_account_transaction.account_txn_id` | `-1` | Quarantine record | `investigation_case_account_transaction__account_txn_id__resolved` |
| `investigation_case_card_transaction.card_txn_id` | `-1` | Quarantine record | `investigation_case_card_transaction__card_txn_id__resolved` |
| `investigation_case_fraud_alert.alert_id` | `UNKNOWN-ALERT` | Quarantine record | `investigation_case_fraud_alert__alert_id__resolved` |
| `investigation_case_sanction_screening.screening_id` | `UNKNOWN-SCREENING` | Quarantine record | `investigation_case_sanction_screening__screening_id__resolved` |

## Physical schema injections

These are ingestion casts, not row-quality failures; they are handled by
`sanitize_injected_schema_errors` in
[`bronze_layer.py`](../src/legacy/notebooks/pipeline/transformations/bronze_layer.py).

| Table.field | Injected physical type | Handling |
|---|---|---|
| `call_center_log.call_duration_seconds` | Parseable decimal text | Cast to `double` |
| `investigation_note.source_arrival_timestamp` | ISO-8601 text | Cast to `timestamp` |
| `card_limit_history.limit_amount` | Parseable decimal text | Cast to `decimal(12,2)` |

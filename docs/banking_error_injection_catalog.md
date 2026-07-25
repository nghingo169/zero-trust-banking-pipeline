# Banking error-injection catalog

This is the table-and-column reference for every deliberate error in the
current banking YAML configuration. It describes injected defects only:
structural nulls, valid mixed customer keys, and planned source-schema
evolution are not errors and are therefore excluded.

Row percentages are selected deterministically from the named table. Unless a
rule says **eligible**, its percentage is calculated against all rows in that
table. Rules that share a table can exclude one another, so their percentages
must not be added together as a total dirty-row rate.

The generator writes the changed row/value, rule ID, error type, scenario
cluster, and expected handling to a local-only error manifest. Neither manifest
is part of the published Bronze snapshots.

## Customer master

Source: [`customer_master_production/error_injecting.yaml`](../../configs/v2/banking_data_model/customer_master_production/error_injecting.yaml)

### `core_banking_customer`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `national_id` | Copy another row's value, creating a duplicate national ID. | 0.8333% | `DEDUPLICATE` |
| `date_of_birth` | Set to `NULL`. | 2.5% | `QUARANTINE_FIELD` |
| `national_id` | Set to `NULL`. | 4.1667% | `QUARANTINE_FIELD` |
| `phone` | Replace with the placeholder `0000000000`. | 2.5% | `QUARANTINE_FIELD` |

### `crm_customer`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `national_id` | Set to `NULL`. | 5.0% | `QUARANTINE_FIELD` |
| `email` | Set to `NULL`. | 3.125% | `QUARANTINE_FIELD` |
| `email` | Replace with `malformed-email`. | 1.875% | `QUARANTINE_FIELD` |

### `customer_kyc`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `customer_ref` | Replace with unresolved `UNKNOWN-CUSTOMER`. | 1.0% | `QUARANTINE_RECORD` |
| `id_number` | Replace with malformed `INVALID-ID`. | 2.0% | `QUARANTINE_FIELD` |

### `customer_employment`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `customer_ref` | Replace with unresolved `UNKNOWN-CUSTOMER`. | 1.0% | `QUARANTINE_RECORD` |
| `monthly_income` | Set to `-1000.00`, below the valid minimum of zero. | 1.0% | `QUARANTINE_RECORD` |

### `customer_request`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `customer_ref` | Replace with unresolved `UNKNOWN-CUSTOMER`. | 1.0% | `QUARANTINE_RECORD` |
| `description` | Set to `NULL`. | 4.0% | `QUARANTINE_FIELD` |

### `account`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `cif_number` | Replace with unresolved `CIF99999999`. | 1.0% | `QUARANTINE_RECORD` |

### `customer_account`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `account_id` | Replace with unresolved `-1`. | 1.0% | `QUARANTINE_RECORD` |

## Customer transactions and operational logs

Source: [`customer_transaction_production/error_injecting.yaml`](../../configs/v2/banking_data_model/customer_transaction_production/error_injecting.yaml)

### `account_transaction`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `account_id` | Replace with unresolved `-1`. | 1.0% | `QUARANTINE_RECORD` |
| `amount` | Set to `-100.00` for otherwise valid `CREDIT` transactions. | 1.0% of credits | `QUARANTINE_RECORD` |
| `status` | Change clean `SUCCESS` to conflicting `FAILED`; event history remains intact. | 1.0% of successful transactions | `RECONCILE_LATEST_STATUS` |

### `account_transaction_status_event`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `account_txn_id` | Replace with unresolved `-1`. | 1.0% | `QUARANTINE_RECORD` |
| `source_arrival_timestamp` | Shift the sequence-0 event 600 seconds later. Business time and sequence remain correct. | 1.0% of initial events | `PROCESS_BY_ARRIVAL_ORDER` |
| Entire row | Duplicate the status-event row as a replay. | 1.0% | `DEDUPLICATE` |

### `merchant_store`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `store_description` | Set to `NULL`. | 6.0% | `QUARANTINE_FIELD` |
| `risk_rating` | Replace with `UNKNOWN`. | 1.0% | `QUARANTINE_FIELD` |

### `log_atm`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `card_number` | Replace with malformed `INVALID-PAN`. | 1.0% | `QUARANTINE_FIELD` |
| `response_code` | Replace with unsupported `UNKNOWN`. | 2.0% | `QUARANTINE_FIELD` |

### `atm_transaction_status_event`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `log_id` | Replace with unresolved `UNKNOWN-ATM-LOG`. | 1.0% | `QUARANTINE_RECORD` |

### `payment_gateway_log`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `currency` | Replace with invalid currency `XXX`. | 0.5% | `QUARANTINE_FIELD` |

### `payment_gateway_status_event`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `gateway_txn_id` | Replace with unresolved `UNKNOWN-GATEWAY-LOG`. | 1.0% | `QUARANTINE_RECORD` |

### `balance_snapshot`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `closing_balance` | Set to `-250.00` only for snapshots whose account product is `SAVINGS`. | 0.5% of eligible savings rows | `QUARANTINE_RECORD` |

## Card

Source: [`card_production/error_injecting.yaml`](../../configs/v2/banking_data_model/card_production/error_injecting.yaml)

### `card`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `account_id` | Replace with unresolved `-1`. | 1.0% | `QUARANTINE_RECORD` |
| `card_number` | Replace with malformed `INVALID-PAN`. | 1.0% | `QUARANTINE_FIELD` |

### `card_transaction`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `card_id` | Replace with unresolved `UNKNOWN-CARD`. | 1.0% | `QUARANTINE_RECORD` |

### `card_transaction_status_event`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `card_txn_id` | Replace with unresolved `-1`. | 1.0% | `QUARANTINE_RECORD` |

### `card_fraud_flag`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `card_txn_id` | Replace with unresolved `-1`. | 1.0% | `QUARANTINE_RECORD` |

### `card_limit_history`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `card_id` | Replace with unresolved `UNKNOWN-CARD`. | 1.0% | `QUARANTINE_RECORD` |

## Financial crime

Source: [`financial_crime_production/error_injecting.yaml`](../../configs/v2/banking_data_model/financial_crime_production/error_injecting.yaml)

### `account_transaction_risk_score`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `account_txn_id` | Replace with unresolved `-1`. | 1.0% | `QUARANTINE_RECORD` |

### `fraud_alert`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `account_txn_id` | Replace with unresolved `-1`. | 1.0% | `QUARANTINE_RECORD` |
| `alert_score` | Set to `101.00`, above the valid 0–100 range. | 1.0% | `QUARANTINE_RECORD` |

### `transaction_monitoring_alert`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `alert_score` | Set to `101.00`, above the valid 0–100 range. | 1.0% | `QUARANTINE_RECORD` |
| `alert_status` | Change to `CLOSED` where the related investigation link makes that status inconsistent. | 1.0% of eligible linked alerts | `RECONCILE_LATEST_STATUS` |

### `transaction_monitoring_alert_account_transaction`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `account_txn_id` | Replace with unresolved `-1`. | 1.0% | `QUARANTINE_RECORD` |

### `transaction_monitoring_alert_card_transaction`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `card_txn_id` | Replace with unresolved `-1`. | 1.0% | `QUARANTINE_RECORD` |

### `investigation_case_transaction_monitoring_alert`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `alert_id` | Replace with unresolved `UNKNOWN-MONITORING-ALERT`. | 1.0% | `QUARANTINE_RECORD` |

### `aml_case`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `investigation_case_id` | Replace with unresolved `UNKNOWN-CASE`. | 1.0% | `QUARANTINE_RECORD` |

### `sanction_screening`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `screened_name` | Truncate the name. | 2.0% | `QUARANTINE_FIELD` |
| `result` | Change eligible `HIT` results to conflicting `CLEAR`. | 1.0% of `HIT` rows | `RECONCILE_LATEST_STATUS` |

### `suspicious_activity_report`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `case_id` | Replace with unresolved `UNKNOWN-AML-CASE`. | 1.0% | `QUARANTINE_RECORD` |

### `watchlist`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `added_date` | Shift 3,650 days into the past. | 1.0% | `FLAG_STALE` |

### `chargeback`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `reason_code` | Replace with unsupported `UNKNOWN`. | 1.0% | `QUARANTINE_FIELD` |

### `investigation_note`

| Field | Injected defect | Rate | Expected handling |
|---|---|---:|---|
| `source_arrival_timestamp` | Shift 600 seconds later; the note's business time is unchanged. | 1.0% | `PROCESS_BY_ARRIVAL_ORDER` |

### Investigation evidence bridges

| Table | Field | Injected defect | Rate | Expected handling |
|---|---|---|---:|---|
| `investigation_case_account_transaction` | `account_txn_id` | Replace with unresolved `-1`. | 1.0% | `QUARANTINE_RECORD` |
| `investigation_case_card_transaction` | `card_txn_id` | Replace with unresolved `-1`. | 1.0% | `QUARANTINE_RECORD` |
| `investigation_case_fraud_alert` | `alert_id` | Replace with unresolved `UNKNOWN-ALERT`. | 1.0% | `QUARANTINE_RECORD` |
| `investigation_case_sanction_screening` | `screening_id` | Replace with unresolved `UNKNOWN-SCREENING`. | 1.0% | `QUARANTINE_RECORD` |

## Snapshot-level physical type errors

Source: [`snapshots/schema_error_injecting.yaml`](../../configs/v2/banking_data_model/snapshots/schema_error_injecting.yaml)

These errors change a column's physical Parquet type for one snapshot date.
They are not row-level defects and are not subject to the row-rate caps.

| Business date | Table and field | Contract type | Injected physical type and value format | Expected handling |
|---|---|---|---|---|
| 2026-07-05 | `financial_crime.call_center_log.call_duration_seconds` | `bigint` | `string`; parseable decimal text | `CAST_TO_CONTRACT_TYPE` |
| 2026-07-06 | `financial_crime.investigation_note.source_arrival_timestamp` | `timestamp` | `string`; ISO-8601 text | `CAST_TO_CONTRACT_TYPE` |
| 2026-07-08 | `card.card_limit_history.limit_amount` | `decimal(12,2)` | `string`; parseable decimal text | `CAST_TO_CONTRACT_TYPE` |

## Error-category quick reference

| Error type | Meaning in this simulation | Standard handling |
|---|---|---|
| `missing` / `malformed` / `invalid_value` | A field is null, malformed, or outside its allowed code list. | `QUARANTINE_FIELD` |
| `invalid_range` / `referential_defect` | A value violates a numeric constraint or no longer resolves to its parent. | `QUARANTINE_RECORD` |
| `duplicate` | A duplicated business value or replayed complete row. | `DEDUPLICATE` |
| `inconsistent_status` | A current status conflicts with related status/history evidence. | `RECONCILE_LATEST_STATUS` |
| `late_arrival` | Arrival time is late while business chronology remains correct. | `PROCESS_BY_ARRIVAL_ORDER` |
| `stale` | A reference date is deliberately old. | `FLAG_STALE` |
| `inconsistent_data_type` | A snapshot column uses a parseable but unexpected physical type. | `CAST_TO_CONTRACT_TYPE` |

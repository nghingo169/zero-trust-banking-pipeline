# Banking daily change catalog

This catalog describes **legitimate changes to existing v3 source records**
between daily deliveries. It is the companion to the
[error-injection catalog](banking_error_injection_catalog.md): entries here are
normal after-image updates or planned source-schema evolution, not defects.

The contract applies to each business date from **2026-07-05 through
2026-07-10**. For every update, the delivery contains the existing source PK
once with the new value and all unchanged fields carried forward. It is not a
partial update or a duplicate record.

Source of truth:

- [`daily.yaml`](../../configs/v3/banking_source_model/daily.yaml) sets the
  number of existing rows updated per table and per day.
- [`v3_daily.py`](../../src/generation/v3_daily.py) defines the updated fields,
  deterministic values, state merge, and change-manifest behavior.

## Daily delivery summary

| Change kind | Rows per day | Meaning |
|---|---:|---|
| New inserts | 259,070 | New source rows with new primary keys. |
| Update after-images | 1,321 | Existing mutable records, reissued with the same primary key and changed business field(s). |
| Core status replays | 878 | Additional duplicate business events with a new replay event ID; not an update after-image. |
| Physical delivery rows | 261,269 | Inserts + update after-images + replay rows. |
| Cumulative state growth | 259,948 | New unique records only: inserts + replay IDs. |

Only the 15 tables below issue after-images. All other v3 tables are
append-only during the daily run; they receive new rows only.

## Core banking

| Table | Existing rows changed/day | Field(s) changed | New value / transition | Notes |
|---|---:|---|---|---|
| `core_banking_customer` | 200 | `updated_at` | Deterministic timestamp on the current business date. | Preserves customer identity and all other attributes. |
| `core_banking_account` | 200 | `updated_at` | Deterministic timestamp on the current business date. | Account business status is unchanged by this after-image rule. |
| `core_banking_account_transaction` | 200 | `narrative` | Existing text with `-UPDATED` appended. | Same transaction ID; models corrected/enriched transaction narration. |

## CRM

| Table | Existing rows changed/day | Field(s) changed | New value / transition | Notes |
|---|---:|---|---|---|
| `crm_customer` | 100 | `updated_at` | Deterministic timestamp on the current business date. | Customer identity/contact values are otherwise preserved. |
| `crm_customer_request` | 200 | `request_status`, `resolved_at` | 100 selected `IN_PROGRESS` rows become `RESOLVED`; 100 selected `RESOLVED` rows become `IN_PROGRESS`. `resolved_at` is populated only for the resulting `RESOLVED` rows and cleared otherwise. | Same request ID; exercises state replacement and date/null semantics. |

## KYC

| Table | Existing rows changed/day | Field(s) changed | New value / transition | Notes |
|---|---:|---|---|---|
| `kyc_customer_profile` | 100 | `updated_at` | Deterministic timestamp on the current business date. | Profile remains the same KYC subject. |
| `kyc_employment_profile` | 50 | `declared_monthly_income` | Add `1.00` in the column's native decimal type. | Same employment-profile ID; represents a small reported-income revision. |

## Card system

| Table | Existing rows changed/day | Field(s) changed | New value / transition | Notes |
|---|---:|---|---|---|
| `card` | 100 | `updated_at` | Deterministic timestamp on the current business date. | Card number, holder, and account reference are preserved. |
| `card_limit_history` | 49 | `limit_amount` | Add `1.00` in the column's native decimal type. | Same history ID; represents a corrected limit after-image. |

## Payments platform

| Table | Existing rows changed/day | Field(s) changed | New value / transition | Notes |
|---|---:|---|---|---|
| `payment_instruction` | 6 | `creditor_name` | Existing text with `-UPDATED` appended. | Same payment ID; lifecycle status is not changed by this rule. |
| `payment_merchant` | 6 | `onboarding_status` | `SUSPENDED` becomes `ACTIVE`; every other selected status becomes `SUSPENDED`. | Same merchant ID. |
| `payment_merchant_store` | 6 | `store_status` | `SUSPENDED` becomes `ACTIVE`; every other selected status becomes `SUSPENDED`. | Same store ID. |

## Financial crime

| Table | Existing rows changed/day | Field(s) changed | New value / transition | Notes |
|---|---:|---|---|---|
| `fin_crime_alert` | 40 | `alert_status`, `closed_at` | 20 selected `CLOSED` alerts become `FALSE_POSITIVE`; 20 selected `FALSE_POSITIVE` alerts become `CLOSED`. `closed_at` is restamped on the current business date. | Candidate selection excludes alerts already used by investigation-case escalation paths. |
| `fin_crime_investigation_case` | 40 | `case_status`, `closed_at` | `INVESTIGATING` becomes `ESCALATED`; any other selected status becomes `INVESTIGATING`. `closed_at` is cleared. | Same case ID; models a reopened or escalated investigation. |
| `fin_crime_regulatory_report` | 24 | `report_status`, `approved_at`, `filed_at`, `regulatory_reference` | `APPROVED` becomes `FILED`; any other selected status becomes `APPROVED`. `approved_at` is restamped. `filed_at` and `regulatory_reference` are populated only when resulting status is `FILED`, otherwise cleared. | Same regulatory-report ID; preserves lifecycle consistency. |

## Legitimate source-schema evolution

These changes apply to the physical delivery schema. They are not error
injections and have no error-manifest entries.

| Effective date | Table | Field | Change to existing data / schema |
|---|---|---|---|
| 2026-07-05 | `gateway_transaction` | `provider_payment_reference` → `provider_reference` | Source column rename. Values are preserved; only the physical column name changes. |
| 2026-07-06 | `crm_customer` | `preferred_contact_method` | New column. Existing carried-forward customers receive deterministic `EMAIL` when aligned into the new schema; new records use their configured generated value. |
| 2026-07-07 | `fin_crime_risk_score` | `risk_score` | Type widens from the earlier `decimal(6,4)` delivery contract to final `decimal(8,6)`. Values are cast without intended business-value change. |
| 2026-07-09 | `core_banking_account` | `servicing_model` | New column. Existing carried-forward accounts receive deterministic `DIGITAL` when aligned into the new schema; new records use their configured generated value. |

## Separate daily replay behavior

`core_banking_transaction_status_event` has **878 replay rows per day**. Each
replay copies a newly created clean status event's business fields but receives
a distinct `CORE_STATUS_REPLAY-YYYYMMDD-...` event ID. This intentionally tests
event deduplication while retaining unique physical primary keys. It is neither
an update to a prior record nor a change to a previous value.

## What the change manifest proves

Every daily delivery writes a private `change_manifest` with:

| Field | Meaning |
|---|---|
| `record_key` | Existing PK for an update after-image, new PK for an insert/replay. |
| `change_type` | `INSERT`, `UPDATE_AFTER`, or `REPLAY`. |
| `source` / `table` | Source owner and table of the changed record. |
| `business_date` | The delivery date that issued the change. |

Quality checks require each update key to exist in the preceding cumulative
state, each insert key to be new, and every configured update count to match
this catalog. The manifest is local evidence and never part of published source
payloads.

# Gold Layer (AI-Ready Context Layer) — Design Doc

*Revision note: this version corrects the original draft against what was actually verified in the Databricks workspace (real Silver column names, real status values, real `pipeline_run` location) and against the fixes applied to the PySpark/DLT code. See "Changelog" at the end for exactly what changed and why.*

## Part 1: Architecture Overview

| Gold View | Grain | Consuming AI Agent | Key Silver Domains |
|---|---|---|---|
| `gold.ai_fraud_transaction_context` | 1 row / `financial_event` (any subtype) | Fraud Detection Agent — real-time scoring, alert triage | `financial_event` + 4 subtype tables + `merchant`/`merchant_location`/`transaction_channel` + `financial_event_risk_score` + `fraud_alert`/`card_fraud_flag` |
| `gold.ai_customer_360_context` | 1 row / `party` | Customer 360 Agent — RM & contact-center Q&A | `party` + `party_profile_version` (current) + `party_kyc_assessment` + `party_employment` + account/card overview + `call_center_contact` |
| `gold.ai_aml_investigation_context` | 1 row / `investigation_case` | Compliance/Legal Agent — case triage, SAR/sanctions lookup | `investigation_case` + `aml_case` + `suspicious_activity_report` + `sanctions_screening`/`watchlist_entry` + `investigation_note` + linked alert/event counts |

**Principles applied across all 3 views:**
- **Zero raw PII**: only tokens (`*_token`), surrogate keys, or banded values (`monthly_income_band`) — never `full_name`, `address`, `date_of_birth`, or exact `monthly_income`.
- **Polymorphic resolution**: `financial_event` is "unpacked" via `LEFT JOIN` on all 4 subtype tables + `COALESCE` on `amount`/`transaction_type` — the core technique that lets an AI agent query one flat table instead of branching on `event_type` itself.
- **4 standard audit columns**: `source_system`, `source_business_key`, `ingested_at`, `pipeline_run_id`.
  - `source_system` — Silver's real column name, read straight through (no renaming/aliasing needed — earlier drafts used a shortened `src_sys`, which has been dropped in favor of matching Silver's actual naming).
  - `source_business_key` — the natural/business key from the source system (**not** `bronze_record_ref`; that column still exists in Silver as a separate Bronze-lineage pointer but is intentionally not surfaced in Gold). Earlier drafts used a shortened `src_rec_id` for this column; also dropped for the same reason.
  - `pipeline_run_id` ← the native `pipeline_run_id` column on each view's driving table (`financial_event`, `party`, `investigation_case`). **This column does not exist yet** — it's being added to all 40 Silver tables in an upcoming Silver notebook fix. The Gold code already assumes it's present and is meant to run *after* that fix ships, not before.
  - `governance.pipeline_run` (the audit/run-log table) is **not** joined into Gold anymore — once every Silver table carries its own `pipeline_run_id`, there's no need to approximate lineage via a date/domain join to a separate governance table.
- **`dq_status`**: derived from the native `data_quality_status` column, which only exists on `party` and `financial_event` in Silver (`investigation_case` has no such column) — so View 3's `dq_status` is limited to `PASSED_CLEAN` / `PENDING_REGULATORY_REF`, with no `REJECTED_QUALITY` state. Combined with business logic: unresolved `party_key`, KYC not yet `VERIFIED`, or a closed AML case still missing a SAR `regulatory_reference`.
- **"Device" for the fraud view**: no separate device table exists in Silver, so device/location context is represented only via `transaction_channel` + `merchant_location`, per an earlier decision to not fabricate a device concept.

## Part 2: Silver → Gold Lineage Mapping

### `gold.ai_fraud_transaction_context`

| Gold Column | Silver Source |
|---|---|
| `financial_event_key`, `event_type`, `occurred_at`, `currency`, `party_key`, `account_key`, `payment_card_key`, `merchant_location_key` | `financial_event` |
| `amount`, `transaction_type_detail`, `posting_direction`, `channel_key` | `COALESCE` across `account_posting` / `card_payment` / `atm_activity` / `gateway_payment` |
| `channel_name`, `channel_type` | `transaction_channel` |
| `merchant_name`, `mcc_code`, `merchant_country` | `merchant` |
| `merchant_store_count`, `merchant_max_store_risk` | `merchant_location` aggregated per `merchant_key` (max of LOW<MEDIUM<HIGH). Store-grain columns (`store_name`, `store_risk_rating`) were dropped: no transaction source carries `store_id` (events only know `merchant_id`, and a merchant has many stores), so store-level context is unresolvable at event grain -- `financial_event.merchant_location_key` is now NULL by design. |
| `latest_risk_score`, `latest_risk_band` | `financial_event_risk_score` (most recent by `scored_date`, via `row_number` dedup) |
| `fraud_alert_count`, `max_fraud_alert_score`, `open_fraud_alert_flag` | `financial_event_fraud_alert` ⋈ `fraud_alert` — `open_fraud_alert_flag` = `F.max(alert_status != 'CLOSED')` (real values: `CLOSED` / `OPEN` / `ESCALATED` — no `RESOLVED` state exists) |
| `card_fraud_flag_count` | `financial_event_card_fraud_flag` |
| `source_system` / `source_business_key` / `ingested_at` | `financial_event.source_system` / `source_business_key` / `ingested_at` (read directly, no renaming) |
| `pipeline_run_id` | `financial_event.pipeline_run_id` (native column, pending Silver fix) |
| `dq_status` | `financial_event.data_quality_status` + `party_key IS NULL` check |

### `gold.ai_customer_360_context`

| Gold Column | Silver Source |
|---|---|
| `party_key`, `party_type`, `party_status` | `party` |
| `preferred_contact_method`, `profile_effective_from` | `party_profile_version` (`is_current = true`) |
| `kyc_verification_status`, `kyc_id_type`, `kyc_id_number` | `party_kyc_assessment` (most recent by `verified_date`) |
| `employer_name`, `job_title`, `monthly_income_band` | `party_employment` (`effective_to IS NULL`), income banded |
| `active_account_count`, `total_current_balance`, `active_card_count` | `party_account_role` ⋈ `account` ⋈ `account_balance_snapshot` (latest by `balance_date`, filtered to the trailing 30 days *before* ranking, for performance on this very large table — accounts with no snapshot in that window show `NULL` balance) ⋈ a **pre-aggregated card count per account** (`payment_card` grouped by `account_key` *before* joining, to avoid multiplying `closing_balance` by the number of cards on an account) |
| `open_service_request_count` | `party_service_request`, excluding `RESOLVED` and `REJECTED` (real values: `RESOLVED` / `OPEN` / `REJECTED` / `IN_PROGRESS` — there is no `CLOSED` state here) |
| `call_center_contact_count_90d`, `last_call_reason` | `call_center_contact` |
| `open_investigation_flag` | `aml_case` ⋈ `investigation_case` |
| `source_system` / `source_business_key` / `ingested_at` | `party.source_system` / `source_business_key` / `ingested_at` (read directly, no renaming) |
| `pipeline_run_id` | `party.pipeline_run_id` (native column, pending Silver fix) |
| `dq_status` | `party.data_quality_status` + KYC not `VERIFIED` |

### `gold.ai_aml_investigation_context`

| Gold Column | Silver Source |
|---|---|
| `investigation_case_key` … `assigned_analyst_id` | `investigation_case` |
| `aml_case_key`, `party_key`, `aml_risk_level` | `aml_case` — **deduped to one row per `investigation_case_key`** (most recently opened, by `opened_date`) before joining, guarding against a possible 1-N `investigation_case`:`aml_case` relationship fanning out the view's grain. If a case genuinely has multiple linked `aml_case` rows, only the latest one's detail surfaces here. **Known data limitation**: `party_key` on AML-originated rows is always NULL — the `aml_case` source references customers via its own `AML-CUST-nnnnnn` namespace, whose numeric ranges are fully disjoint from both `CB-` (core banking `cust_no`) and `CRM-` (`party_id`) identifiers (verified 0/5,289 distinct refs match either system numerically). No crosswalk exists in the synthetic dataset, so these cases cannot be resolved to a party by any key formula. |
| `sar_filed_flag` / `date` / `regulatory_reference` / `report_status` | `suspicious_activity_report` (most recent by `filed_date`, joined via the deduped `aml_case`) |
| `sanctions_screening_count`, `sanctions_hit_flag`, `max_sanctions_match_score`, `linked_watchlist_types` | `investigation_case_sanctions_screening` ⋈ `sanctions_screening` ⋈ `watchlist_entry` — `sanctions_hit_flag` = any linked screening with `screening_result = 'HIT'` (real values: `CLEAR` / `FALSE_POSITIVE` / `HIT` — `FALSE_POSITIVE` must NOT be counted as a hit) |
| `linked_fraud_alert_count` | `investigation_case_fraud_alert` |
| `linked_monitoring_alert_count` | `investigation_case_monitoring_alert` |
| `linked_financial_event_count` | `investigation_case_financial_event` |
| `investigation_note_count`, `latest_note_text` | `investigation_note` |
| `source_system` / `source_business_key` / `ingested_at` | `investigation_case.source_system` / `source_business_key` / `ingested_at` (read directly, no renaming) |
| `pipeline_run_id` | `investigation_case.pipeline_run_id` (native column, pending Silver fix) |
| `dq_status` | Business logic only (closed case, SAR still missing `regulatory_reference`) — `investigation_case` has no native `data_quality_status`, so `REJECTED_QUALITY` is not derivable here |

## Part 3: Performance Optimization & AI Scaling

**Materialized View / Liquid Clustering:**
- All 3 views are currently plain views (recomputed on every query). Given real-time scoring load, `ai_fraud_transaction_context` should move to a **Materialized View** (`ai_fraud_transaction_context_mv`), since the Fraud Agent will query it continuously and the `ROW_NUMBER`/aggregate CTEs are expensive to recompute per call. `ai_customer_360_context` and `ai_aml_investigation_context` can stay as plain views — lower query volume, and freshness matters more than raw speed for case/KYC lookups.
- **Liquid Clustering** instead of hard partitioning:
  - `ai_fraud_transaction_context_mv`: `CLUSTER BY (occurred_at, party_key)` — most queries filter by a recent time window and/or a specific customer.
  - `ai_customer_360_context`: `CLUSTER BY (party_key)`.
  - `ai_aml_investigation_context`: `CLUSTER BY (case_status, opened_at)`.
- Refresh strategy: trigger the materialized-view refresh right after the `BRONZE_TO_SILVER` job completes (event-driven), rather than a fixed cron.
- **`pipeline_run_id` gap — now being fixed**: the Silver team is adding a native `pipeline_run_id` column to all 40 Silver tables, which removes the need for Gold to approximate lineage via a date-based join to `governance.pipeline_run`. The Gold code has already been updated to read `pipeline_run_id` directly from each view's driving table, and is meant to be run only after that Silver fix ships.

**Databricks Vector Search for text fields:**
- Index two free-text sources: `investigation_note.note_text` (exposed via `latest_note_text`) and `call_center_contact.call_reason` / `party_service_request.request_description`.
- Proposed architecture: a Delta Sync Index (or a dedicated `gold.ai_note_embeddings` table) built with an internal embedding model — no sensitive text leaves the workspace — enabling the Compliance Agent to do semantic search ("find cases with notes similar to case X") instead of plain full-text match.
- Since `investigation_note` may contain indirect PII (names, behavioral descriptions), a redaction/tokenization pass is needed before vectorizing — raw notes should not be embedded directly.

## Part 4: Code Artifacts

- PySpark / Lakeflow Declarative Pipelines implementation (`dp.table` decorators), split per view under `src/pipeline/gold/`: `fraud_transaction_context.py`, `customer_360_context.py`, `aml_investigation_context.py`, plus shared config in `gold_common.py`. Wired into the `silver_to_gold` Lakeflow pipeline via `resources/silver_to_gold.pipeline.yml`.
- There is no separate hand-maintained SQL DDL file for these views -- `dp.table` already defines the schema and creates the table, so the PySpark file is the single source of truth. (An earlier draft of this doc referenced a `gold_layer_ai_ready_ddl.sql` as a second artifact; that file doesn't actually exist and the reference has been removed.)

---

## Changelog (this revision vs. the original draft)

1. **`pipeline_run_id`**: originally documented as a "best-effort join to `governance.pipeline_run` by ingestion date" — this was based on `pipeline_run` not existing per-row on Silver tables. Corrected: the Silver team added a native `pipeline_run_id` to all Silver tables, so Gold now reads it directly from each driving table with no join at all. The PySpark code reflects this.
2. **`src_rec_id` source**: originally sourced from `bronze_record_ref` (a Bronze-lineage pointer). Corrected: now sourced from `source_business_key` (the natural/business key), per an explicit decision — `bronze_record_ref` remains available in Silver but is not surfaced in Gold.
3. **Real Silver column names confirmed**: `source_system` and `source_business_key` are the actual, current Silver column names (verified via `DESCRIBE TABLE`) — no rename was actually needed on the Gold code's read side for these two.
4. **Two logic bugs found and fixed** once real status values were confirmed:
   - `open_service_request_count` was treating `REJECTED` requests as still open (there is no `CLOSED` value in `request_status`, only `RESOLVED` / `OPEN` / `REJECTED` / `IN_PROGRESS`).
   - `sanctions_hit_flag` was treating `FALSE_POSITIVE` as a sanctions hit (there is no `NO_MATCH` value in `screening_result`, only `CLEAR` / `FALSE_POSITIVE` / `HIT`).
   - Minor cleanup: `open_fraud_alert_flag` simplified to check only `alert_status != 'CLOSED'`, since `RESOLVED` never actually occurs (`OPEN` / `CLOSED` / `ESCALATED` are the only real values).
5. **Two fan-out bugs found and fixed** in the PySpark/DLT code:
   - `total_current_balance` was being multiplied by the number of cards on an account, because `payment_card` was joined directly at the row level before aggregation. Fixed by pre-aggregating card counts per `account_key` first.
   - The original date-based `pipeline_run` join (before the native-column fix above) could duplicate every `financial_event` / `investigation_case` row if more than one pipeline run existed for the same calendar date. This risk is now moot since the join was removed entirely.
6. **Optimization pass (code review feedback)**:
   - **Audit columns renamed**: `src_sys` → `source_system`, `src_rec_id` → `source_business_key` throughout, matching Silver's real names exactly (no more aliasing).
   - **View 3 fan-out risk fixed**: `investigation_case` ⋈ `aml_case` could fan out if that relationship is 1-N. `aml_case` is now deduped to one row per `investigation_case_key` (most recently opened) before joining, same pattern as View 2's account aggregation.
   - **View 2 performance**: `account_balance_snapshot` is filtered to the trailing 30 days *before* the `row_number()` ranking, instead of ranking over the table's full history — this table is very large (daily snapshots × millions of accounts). Trade-off: dormant accounts with no snapshot in the last 30 days will show a `NULL` balance.
   - **View 1 simplified**: `open_fraud_alert_flag` now uses a direct boolean aggregation (`F.max(alert_status != 'CLOSED')`) instead of a `when/otherwise/cast` chain.
   - **Liquid Clustering added** via the `cluster_by` parameter on each `@dp.table(...)` decorator (not a `table_properties` key -- `delta.clusterBy` isn't a valid arbitrary property and Delta rejects it with `DELTA_UNKNOWN_CONFIGURATION` if set that way), matching what Part 3 already recommended.
   - These fixes are applied in the PySpark file, which is the single source of truth for these views (see Part 4).
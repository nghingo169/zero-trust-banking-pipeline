# Customer 360 in Silver

## Purpose

Build one governed customer record from the validated Core Banking, CRM, and
KYC datasets. This is a **Silver-layer** concern: Bronze remains the source
delivery record, while `customer_quarantine` retains rejected records and
`customer_team_silver` contains only records that can safely participate in
analytics.

This guide applies to one snapshot date at a time. Do not combine all daily
snapshot partitions before resolving customers: the same person appears in
each daily after-image by design.

## Boundary between the two schemas

| Schema | Role | May contribute to Customer 360? |
|---|---|---:|
| `customer_quarantine` | Immutable evidence of rejected customer records and unresolved KYC references, with rejection reason and processing date. | No |
| `customer_team_silver` | Validated, standardized customer records, the identity crosswalk, KYC resolution, and the Customer 360 view. | Yes |

The quarantine schema is not a fallback matching source. A record rejected for
a null, malformed, or duplicate `national_id` must never be reintroduced into
the 360 view through a looser name or phone match.

## Preconditions

The customer-cleaning step has already established the following contract for
the participating Silver tables:

- `core_banking_customer.national_id` is populated, valid, and unique within
  Core Banking for the relevant snapshot date.
- `crm_customer.national_id` is populated, valid, and unique within CRM for
  the relevant snapshot date.
- rejected Core and CRM customers are written to `customer_quarantine` with a
  reason such as `NULL_NATIONAL_ID`, `MALFORMED_NATIONAL_ID`, or
  `DUPLICATE_NATIONAL_ID`.
- `customer_kyc.customer_ref` remains source-native and mixed: a Core
  `cust_no` such as `CB-...` or a CRM `party_id` such as `CRM-...`.

`national_id` is therefore the approved Core-to-CRM identity key. Names,
emails, and phones are useful profile attributes and data-quality signals, but
they are not matching keys in this curated flow.

## Target objects and grain

The physical input table names below are examples. Map them to the actual
tables in `customer_team_silver`; the object grain and key rules are the
contract.

| Object | Grain | Key purpose |
|---|---|---|
| `customer_identity_xref` | One source customer key per `as_of_date`, `party_key`, and source system | Resolves `cust_no` and `party_id` to one canonical customer. |
| `customer_kyc_resolved` | One KYC record per `as_of_date` and `kyc_id` | Resolves mixed `customer_ref` through the crosswalk. |
| `customer_360` | One `as_of_date` and `party_key` | Presents a non-multiplying customer-level view. |
| `customer_quarantine.customer_kyc_unresolved_reference` | One rejected KYC record | Preserves KYC records whose reference cannot resolve exactly once. |

`party_key` must be stable across daily snapshots. In production, generate or
look it up in a governed identity dimension using a protected token of the
normalized national ID. Do not expose the raw national ID in the 360 view.
The `sha2` expression in the examples is a deterministic development example;
replace it with the platform's approved tokenization/key-management approach
where required.

## Processing sequence

```text
Validated Core customer ─┐
                         ├── customer_identity_xref ── customer_360
Validated CRM customer ──┘                │
                                          └── customer_kyc_resolved

Invalid customer / unresolved KYC reference ── customer_quarantine
```

1. Read one `as_of_date` from the validated Core and CRM Silver tables.
2. Normalize and tokenise the approved identity key, then create the Core/CRM
   crosswalk. The same identity token receives the same `party_key`.
3. Resolve each KYC record through the source-native key in that crosswalk.
4. Quarantine any KYC record with zero or more than one crosswalk candidate.
5. Aggregate KYC before joining it to the customer profile, so multiple KYC
   records do not duplicate Customer 360 rows.

## Reference SQL

The following is Databricks SQL. Substitute the three input table names and
the `as_of_date` filter for the team's actual layout.

### 1. Build the canonical source-key crosswalk

```sql
-- Run for a single snapshot date.  The three input tables must already be
-- validated Silver datasets; do not point this at Bronze or quarantine.
CREATE OR REPLACE TABLE customer_team_silver.customer_identity_xref AS
WITH core AS (
  SELECT
    business_date AS as_of_date,
    cust_no AS source_customer_key,
    upper(trim(national_id)) AS national_id_normalized
  FROM customer_team_silver.core_banking_customer
  WHERE business_date = DATE '2026-07-10'
),
crm AS (
  SELECT
    business_date AS as_of_date,
    party_id AS source_customer_key,
    upper(trim(national_id)) AS national_id_normalized
  FROM customer_team_silver.crm_customer
  WHERE business_date = DATE '2026-07-10'
),
source_keys AS (
  SELECT as_of_date, 'CORE_BANKING' AS source_system,
         source_customer_key, national_id_normalized
  FROM core
  UNION ALL
  SELECT as_of_date, 'CRM' AS source_system,
         source_customer_key, national_id_normalized
  FROM crm
),
resolution_counts AS (
  SELECT
    *,
    count_if(source_system = 'CORE_BANKING') OVER (
      PARTITION BY as_of_date, national_id_normalized
    ) AS core_candidates,
    count_if(source_system = 'CRM') OVER (
      PARTITION BY as_of_date, national_id_normalized
    ) AS crm_candidates
  FROM source_keys
)
SELECT
  as_of_date,
  -- Replace this with the team's protected identity-token/key service.
  sha2(concat('CUSTOMER_360|', national_id_normalized), 256) AS party_key,
  source_system,
  source_customer_key,
  CASE
    WHEN core_candidates = 1 AND crm_candidates = 1 THEN 'CONFIRMED_CORE_CRM'
    WHEN core_candidates = 1 AND crm_candidates = 0 THEN 'CORE_ONLY'
    WHEN core_candidates = 0 AND crm_candidates = 1 THEN 'CRM_ONLY'
    ELSE 'QUARANTINE_REQUIRED'
  END AS resolution_status,
  current_timestamp() AS resolved_at
FROM resolution_counts
WHERE core_candidates <= 1
  AND crm_candidates <= 1;
```

The precondition means `QUARANTINE_REQUIRED` should never occur in this
output. If it does, stop the job and route the affected source customers to
quarantine; do not silently keep an arbitrary candidate.

### 2. Resolve the mixed KYC key

```sql
CREATE OR REPLACE TEMP VIEW kyc_candidates AS
SELECT
  k.business_date AS as_of_date,
  k.kyc_id,
  k.customer_ref,
  k.id_type,
  k.id_number,
  k.verification_status,
  k.verified_date,
  x.party_key,
  count(x.party_key) OVER (
    PARTITION BY k.business_date, k.kyc_id
  ) AS candidate_count
FROM customer_team_silver.customer_kyc k
LEFT JOIN customer_team_silver.customer_identity_xref x
  ON k.business_date = x.as_of_date
 AND k.customer_ref = x.source_customer_key;

CREATE OR REPLACE TABLE customer_team_silver.customer_kyc_resolved AS
SELECT
  as_of_date, kyc_id, party_key, customer_ref, id_type, id_number,
  verification_status, verified_date
FROM kyc_candidates
WHERE candidate_count = 1;

-- Keep every non-resolvable KYC row and the reason; it is evidence, not a
-- Customer 360 profile row.
INSERT INTO customer_quarantine.customer_kyc_unresolved_reference
SELECT
  as_of_date, kyc_id, customer_ref, id_type, id_number, verification_status,
  verified_date,
  CASE
    WHEN candidate_count = 0 THEN 'UNRESOLVED_CUSTOMER_REFERENCE'
    ELSE 'AMBIGUOUS_CUSTOMER_REFERENCE'
  END AS quarantine_reason,
  current_timestamp() AS quarantined_at
FROM kyc_candidates
WHERE candidate_count <> 1;
```

The current source keys use separate `CB-...` and `CRM-...` namespaces, so a
valid reference should resolve exactly once. `UNKNOWN-CUSTOMER` and any future
unrecognised reference are expected to reach quarantine, not Customer 360.

### 3. Build one row per customer

```sql
CREATE OR REPLACE TABLE customer_team_silver.customer_360 AS
WITH kyc_ranked AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY as_of_date, party_key
      ORDER BY verified_date DESC NULLS LAST, kyc_id DESC
    ) AS row_num,
    count(*) OVER (
      PARTITION BY as_of_date, party_key
    ) AS kyc_record_count
  FROM customer_team_silver.customer_kyc_resolved
),
latest_kyc AS (
  SELECT * FROM kyc_ranked WHERE row_num = 1
),
party_spine AS (
  SELECT DISTINCT as_of_date, party_key
  FROM customer_team_silver.customer_identity_xref
)
SELECT
  p.as_of_date,
  p.party_key,

  c.cust_no,
  c.cif_number,
  c.full_name AS core_full_name,
  c.date_of_birth,
  c.address AS core_address,

  r.party_id,
  r.customer_name AS crm_customer_name,
  r.email AS crm_email,
  r.preferred_contact_method,

  k.kyc_id AS latest_kyc_id,
  k.id_type AS latest_kyc_id_type,
  k.verification_status AS latest_kyc_status,
  k.verified_date AS latest_kyc_verified_date,
  coalesce(k.kyc_record_count, 0) AS kyc_record_count,

  CASE
    WHEN c.cust_no IS NOT NULL AND r.party_id IS NOT NULL THEN 'CORE_AND_CRM'
    WHEN c.cust_no IS NOT NULL THEN 'CORE_ONLY'
    ELSE 'CRM_ONLY'
  END AS source_coverage
FROM party_spine p
LEFT JOIN customer_team_silver.customer_identity_xref xc
  ON p.as_of_date = xc.as_of_date
 AND p.party_key = xc.party_key
 AND xc.source_system = 'CORE_BANKING'
LEFT JOIN customer_team_silver.core_banking_customer c
  ON xc.as_of_date = c.business_date
 AND xc.source_customer_key = c.cust_no
LEFT JOIN customer_team_silver.customer_identity_xref xr
  ON p.as_of_date = xr.as_of_date
 AND p.party_key = xr.party_key
 AND xr.source_system = 'CRM'
LEFT JOIN customer_team_silver.crm_customer r
  ON xr.as_of_date = r.business_date
 AND xr.source_customer_key = r.party_id
LEFT JOIN latest_kyc k
  ON p.as_of_date = k.as_of_date
 AND p.party_key = k.party_key;
```

Keep Core and CRM attributes in separate columns. For example, retain
`core_full_name` and `crm_customer_name` rather than automatically replacing
one with the other. This preserves source provenance and makes a later profile
disagreement observable.

## Required quality gates

Run these checks for each `as_of_date` before publishing the 360 table:

```sql
-- 1. The source-key crosswalk must be one-to-one within a source system.
SELECT as_of_date, source_system, source_customer_key, count(*) AS mappings
FROM customer_team_silver.customer_identity_xref
GROUP BY as_of_date, source_system, source_customer_key
HAVING count(*) <> 1;

-- 2. A Customer 360 row must exist exactly once per canonical party.
SELECT as_of_date, party_key, count(*) AS rows_per_party
FROM customer_team_silver.customer_360
GROUP BY as_of_date, party_key
HAVING count(*) <> 1;

-- 3. No unresolved KYC reference may leak into the curated resolution table.
SELECT count(*) AS unresolved_rows_in_silver
FROM customer_team_silver.customer_kyc_resolved
WHERE party_key IS NULL;

-- 4. Quarantine must reconcile with non-resolved KYC candidates.
SELECT as_of_date, quarantine_reason, count(*) AS quarantined_rows
FROM customer_quarantine.customer_kyc_unresolved_reference
GROUP BY as_of_date, quarantine_reason;
```

Expected results for the first three checks are zero rows, zero rows, and
`0`, respectively. The final query is an audit count: it may be non-zero when
the data intentionally contains a broken customer reference.

## Daily snapshot behaviour

- Include `as_of_date` in every identity and 360 key; never mix July 5–10
  after-images in a single matching pass.
- Keep `party_key` stable when the same validated national identity appears on
  a later date.
- If a national ID is legitimately corrected, process it as an identity-change
  event in the crosswalk and retain the prior mapping history. Do not create a
  new customer silently.
- Rebuild `customer_360` as an after-image for each date; it is a snapshot
  view, not a transaction/event table.

## Implementation decision

The approved match rule is **validated national ID equality**, applied only
inside `customer_team_silver`. `customer_kyc` reaches that customer through
the `customer_identity_xref` mapping of its `customer_ref`. Quarantine is
strictly evidence and reconciliation, never a candidate source for identity
resolution.

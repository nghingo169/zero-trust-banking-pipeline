# Customer 360 SQL test module

Run these files in order in the Databricks SQL Editor. Each SQL file selects
the validated customer Silver catalog and schema itself:

```sql
USE CATALOG workspace;
USE SCHEMA team_customer_silver;
```

1. `01_active_customer_views.sql`
2. `02_party_candidate_view.sql`
3. `03_party_quality_checks.sql`
4. `04_party_identifier_candidate_view.sql`
5. `05_party_identifier_quality_checks.sql`
6. `06_party_identity_resolution_candidate_view.sql`
7. `07_party_identity_resolution_quality_checks.sql`
8. `08_kyc_unresolved_diagnostics.sql` (only when unresolved KYC rows exist)
9. `09_party_kyc_assessment_and_quarantine_views.sql`
10. `10_kyc_assessment_quality_checks.sql`
11. `11_party_employment_resolution_and_quarantine_views.sql`
12. `12_party_employment_quality_checks.sql`
13. `13_employment_unresolved_diagnostics.sql` (only when unresolved rows exist)
14. `14_kyc_employment_orphan_overlap_check.sql`
15. `15_party_service_request_resolution_and_quarantine_views.sql`
16. `16_party_service_request_quality_checks.sql`
17. `17_request_unresolved_diagnostics.sql` (only when unresolved rows exist)
18. `18_orphan_customer_overlap_summary.sql`

The first two files create ordinary views, so the checks can be rerun in a
different SQL Editor tab without recreating the common calculations. They do
not create or write the physical `party` table.

The views retain only a deterministic token of normalized `national_id`; they
never expose the raw national ID. The `party_key` in `party_candidate_v` is a
deterministic test key only. Replace it with the persistent party-key registry
when the mapping is promoted beyond testing.

The current customer source contract exposes `cust_no` and `party_id`; the CIF
identifier can be added later when its authoritative customer-level source is
included in this mapping.

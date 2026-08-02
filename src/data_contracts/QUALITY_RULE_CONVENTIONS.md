# Quality rule conventions

The ODCS contracts describe the expected quality of each Bronze dataset. They
are not generated from the pipeline and do not contain Spark implementation
details.

## Rule ownership

1. A developer adds or changes a quality expectation in the relevant ODCS
   contract.
2. The developer selects its handling through `banking.handling`:
   `quarantine`, `normalize`, `monitor`, `reconcile`, or `by_design`.
3. For `quarantine`, the developer implements a corresponding row predicate in
   `quality_rules`. The predicate can be deliberately more specific than the
   ODCS expectation when it targets a documented injected defect.
4. Cross-row, SCD2, and reference-resolution controls remain pipeline code;
   they are not forced into a row-level SQL predicate.

## ODCS quality rules

Use the portable ODCS library metrics whenever possible:

- `nullValues` for required values;
- `missingValues` for defined placeholders;
- `invalidValues` with `validValues` or `pattern` for value domains and
  formats;
- `duplicateValues` for key uniqueness.

Use `type: sql` only for a stable table-level assertion. Do not use a quality
rule for source behaviour that is intentionally accepted, corrected by
normalization, or monitored asynchronously.

## Traceability

Contract quality IDs must be stable. A runtime rule that enforces a contract
expectation uses the same ID when their intent is identical. Where a runtime
predicate only detects a specific injected representation, its module comment
must name the contract quality ID that it implements.

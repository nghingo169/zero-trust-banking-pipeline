# Pipeline source code

## Authoritative data definitions

[`data_contracts/`](data_contracts/) is the single source of truth for YAML
schemas, table identity, business keys, ingestion semantics, and row-quality
rules. The deployed pipeline code lives under [`pipeline/`](pipeline/).

## Source-to-Gold graph

`banking-investigation-pipeline` is the single physical Spark Declarative
Pipeline. Its graph includes Source-to-Bronze, validated Silver and quarantine,
atomic Silver, and Gold. Run `banking_investigation_bootstrap` once before the
first data run and again only after approved governance changes. The recurring
`banking_investigation_pipeline_orchestration` Job invokes exactly one SDP
update and invokes the internal governance-owned PII tag job after publication.
Pipeline updates, flow row counts, durations, and expectation metrics come from
the published native SDP event log; they are exposed through governance-owned
monitoring views and do not gate the pipeline run.

## Quarantine pattern

The validated-Silver pipeline normalizes and evaluates each Bronze record once.
Passing rows are published to `silver_validated`; every failed rule produces an
atomic record in the centralized governance quarantine table. Aggregate checks
and rule-level rejection metrics are exposed through monitoring views.
Quarantine, atomic Silver, and Gold use the same parent Job `pipeline_run_id`;
native SDP update identity is retained separately in
`governance.pipeline_run`.

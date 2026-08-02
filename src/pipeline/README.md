# Pipeline source code

## Authoritative data definitions

[`data_contracts/`](data_contracts/) is the single source of truth for YAML
schemas, table identity, business keys, ingestion semantics, and row-quality
rules. The deployed pipeline code lives under [`pipeline/`](pipeline/).

## Quarantine pattern

The validated-Silver pipeline normalizes and evaluates each Bronze record once.
Passing rows are published to `silver_validated`; every failed rule produces an
atomic record in the centralized governance quarantine table. Aggregate checks
and quality metrics are written by the governance audit notebooks.

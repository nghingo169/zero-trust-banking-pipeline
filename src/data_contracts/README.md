# Data contracts

This is the only source of truth for the data definitions used by active
pipelines:

- `schemas/`: domain YAML data contracts.
- `table_catalog.py`: table names, stable keys, and SCD2 versus append semantics.
- `quality_rules/`: row-level validation rules and their lookup registry.

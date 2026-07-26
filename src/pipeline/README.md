# Pipeline code

- `bronze/`: file ingestion, schema evolution, append-only event history, and SCD2 snapshot comparison.
- `silver/`: clean-table validation, CDF-backed quarantine, and current-status views.
- `monitoring/`: per-replay-day metrics and reconciliation checks.
- `experiments/`: explicitly non-production pipeline validation prototypes.

Each filename states its role, for example `bronze/card_ingestion.py` and
`silver/card_validation.py`. Contracts and quality rules live in
`src/data_contracts/`.

# Source-landing loader

`load_local_snapshots.sh` stages all four domains and all six replay dates from
the local Parquet source into the shared source-landing Volume. It does not run
Bronze, Silver, or audit tasks.

```bash
bash scripts/source_landing/load_local_snapshots.sh
```

To stage only a domain or date range (including after an interrupted upload):

```bash
SOURCE_LANDING_DOMAINS=customer_transaction \
SOURCE_LANDING_DATES=2026-07-05,2026-07-06 \
bash scripts/source_landing/load_local_snapshots.sh
```

The loader is resumable: it skips a table directory that already exists and
only copies missing tables. It never overwrites a raw Parquet file. Use a new
`SIMULATION_ID` for a new delivery, or explicitly clear a development-only
partition outside this script.

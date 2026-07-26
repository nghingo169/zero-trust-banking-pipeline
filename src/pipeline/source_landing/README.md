# Source landing

The source landing Volume is the sole raw-file boundary for all domains.
Local replay scripts stage immutable Parquet snapshots here today; a future S3
ingestion job will write the identical layout without changing Bronze, Silver,
or audit code.

```text
/Volumes/workspace/dev_source_landing/source_snapshot_files/
  banking_v2/snapshots/
    domain=<domain>/
      simulation_id=<id>/snapshot_type=full/business_date=YYYY-MM-DD/
        <source-domain>/<table>/*.parquet
```

Each domain owns its `domain=` partition. Full replay cleanup must only remove
that domain's `simulation_id=` subtree; it must never remove another domain's
landing files.

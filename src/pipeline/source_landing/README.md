# Source landing

The source landing Volume is the raw-file boundary for all domains. Local
development uploads immutable Parquet snapshots here; production can use an
S3-backed external Volume with the same layout.

```text
/Volumes/workspace/dev_source_landing/source_snapshot_files/
  banking_v2/
    simulation_id=<id>/snapshot_type=full/
      business_date=YYYY-MM-DD/
        <source-domain>/<table>/*.parquet
```

Bronze discovers and processes one source table and business date at a time.

# Silver scripts

Silver is intentionally invoked by each domain's Databricks job after Bronze;
there is no standalone staging script because Silver reads the Bronze tables.
Use `scripts/full/replay_<domain>.sh` for an end-to-end run or the matching
Bronze day script to stage one snapshot and invoke the domain job.

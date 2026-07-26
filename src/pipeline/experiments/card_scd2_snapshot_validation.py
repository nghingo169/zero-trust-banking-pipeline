"""Temporary two-snapshot SCD Type 2 proof of concept for Card master data.

Run this only after the Card Bronze pipeline has successfully ingested both
configured dates. It reads Bronze rather than S3 to prove the Bronze-to-Silver
boundary.
"""

from __future__ import annotations

from typing import Optional

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


BRONZE_CARD_TABLE = spark.conf.get("pipeline.bronze_card_table")
SNAPSHOT_VERSIONS = tuple(
    int(snapshot_date.replace("-", ""))
    for snapshot_date in spark.conf.get("pipeline.scd2_snapshot_dates").split(",")
)
CARD_BUSINESS_COLUMNS = (
    "card_id",
    "account_id",
    "card_number",
    "card_type",
    "issue_date",
    "expiry_date",
    "status",
)

if SNAPSHOT_VERSIONS != tuple(sorted(SNAPSHOT_VERSIONS)):
    raise ValueError("pipeline.scd2_snapshot_dates must be ordered oldest to newest")
if len(SNAPSHOT_VERSIONS) != 2:
    raise ValueError("The SCD Type 2 proof of concept requires exactly two snapshot dates")


def _next_snapshot(latest_version: Optional[int]) -> Optional[tuple[DataFrame, int]]:
    """Return the next complete Card snapshot for AUTO CDC FROM SNAPSHOT."""

    remaining_versions = [
        version
        for version in SNAPSHOT_VERSIONS
        if latest_version is None or version > latest_version
    ]
    if not remaining_versions:
        return None

    version = remaining_versions[0]
    snapshot_date = f"{version // 10000:04d}-{version // 100 % 100:02d}-{version % 100:02d}"
    snapshot = (
        spark.read.table(BRONZE_CARD_TABLE)
        .filter(F.col("snapshot_date") == F.to_date(F.lit(snapshot_date)))
        .select(*CARD_BUSINESS_COLUMNS)
    )
    return snapshot, version


dp.create_streaming_table(name="card_scd2_test")
dp.create_auto_cdc_from_snapshot_flow(
    target="card_scd2_test",
    source=_next_snapshot,
    keys=["card_id"],
    stored_as_scd_type="2",
    track_history_column_list=list(CARD_BUSINESS_COLUMNS),
)

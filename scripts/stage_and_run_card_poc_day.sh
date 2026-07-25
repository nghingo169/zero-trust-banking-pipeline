#!/usr/bin/env bash
set -euo pipefail

business_date="${1:?Usage: bash scripts/stage_and_run_card_poc_day.sh YYYY-MM-DD}"
case "$business_date" in
  2026-07-05|2026-07-06) ;;
  *)
    echo "Only 2026-07-05 and 2026-07-06 are supported by this POC." >&2
    exit 2
    ;;
esac

profile="${DATABRICKS_PROFILE:-skadi2910-dev}"
source_root="${CARD_POC_SOURCE_ROOT:-/Users/skadi2910/projects/mock-data-generator/data/output/v2/snapshots}"
simulation_id="banking-20260705-20260710"
source_path="${source_root}/simulation_id=${simulation_id}/snapshot_type=full/business_date=${business_date}/card"
volume_root="dbfs:/Volumes/workspace/poc_card_landing/daily_snapshot_files/banking_v2/snapshots"
destination_path="${volume_root}/simulation_id=${simulation_id}/snapshot_type=full/business_date=${business_date}/card"

if [[ ! -d "$source_path" ]]; then
  echo "Local Card snapshot directory not found: $source_path" >&2
  exit 1
fi

if databricks fs ls "$destination_path" --profile "$profile" >/dev/null 2>&1; then
  echo "Refusing to restage an existing POC date: $destination_path" >&2
  exit 1
fi

databricks fs cp --recursive "$source_path" "$destination_path" --profile "$profile"
databricks bundle run poc_card_daily -t dev --profile "$profile" \
  --params "business_date=${business_date}"

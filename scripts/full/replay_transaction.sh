#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
cd "$repo_root"

profile="${DATABRICKS_PROFILE:-skadi2910-dev}"
simulation_root="dbfs:/Volumes/workspace/dev_source_landing/source_snapshot_files/banking_v2/snapshots/domain=customer_transaction/simulation_id=banking-20260705-20260710"

databricks bundle validate -t dev --profile "$profile"
databricks bundle deploy -t dev --profile "$profile"
if databricks fs ls "$simulation_root" --profile "$profile" >/dev/null 2>&1; then
  databricks fs rm -r "$simulation_root" --profile "$profile"
fi

for business_date in 2026-07-05 2026-07-06 2026-07-07 2026-07-08 2026-07-09 2026-07-10; do
  if [[ "$business_date" == "2026-07-05" ]]; then
    bash scripts/bronze/transaction_day.sh "$business_date" --reset
  else
    bash scripts/bronze/transaction_day.sh "$business_date"
  fi
done

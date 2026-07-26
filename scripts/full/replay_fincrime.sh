#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$root"
profile="${DATABRICKS_PROFILE:-skadi2910-dev}"
simulation_root="dbfs:/Volumes/workspace/dev_source_landing/source_snapshot_files/banking_v2/snapshots/domain=financial_crime/simulation_id=banking-20260705-20260710"
databricks bundle validate -t dev --profile "$profile"
databricks bundle deploy -t dev --profile "$profile"
if databricks fs ls "$simulation_root" --profile "$profile" >/dev/null 2>&1; then databricks fs rm -r "$simulation_root" --profile "$profile"; fi
for d in 2026-07-05 2026-07-06 2026-07-07 2026-07-08 2026-07-09 2026-07-10; do
  if [[ "$d" == "2026-07-05" ]]; then bash scripts/bronze/fincrime_day.sh "$d" --reset; else bash scripts/bronze/fincrime_day.sh "$d"; fi
done

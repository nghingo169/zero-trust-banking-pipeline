#!/usr/bin/env bash
set -euo pipefail
business_date="${1:?Usage: $0 YYYY-MM-DD [--reset]}"
mode="${2:-daily}"
profile="${DATABRICKS_PROFILE:-skadi2910-dev}"
source_root="${FINCRIME_SOURCE_ROOT:-/Users/skadi2910/projects/mock-data-generator/data/output/v2/snapshots}"
simulation_id="banking-20260705-20260710"
source_path="$source_root/simulation_id=$simulation_id/snapshot_type=full/business_date=$business_date/financial_crime"
destination="dbfs:/Volumes/workspace/dev_source_landing/source_snapshot_files/banking_v2/snapshots/domain=financial_crime/simulation_id=$simulation_id/snapshot_type=full/business_date=$business_date/financial_crime"
[[ -d "$source_path" ]] || { echo "Missing source: $source_path" >&2; exit 1; }
databricks fs cp --recursive "$source_path" "$destination" --profile "$profile"
job="dev_fincrime_daily"; [[ "$mode" == "--reset" ]] && job="dev_fincrime_reset_day"
databricks bundle run "$job" -t dev --profile "$profile" --params "business_date=$business_date"

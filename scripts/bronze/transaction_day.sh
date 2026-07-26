#!/usr/bin/env bash
set -euo pipefail
date="${1:?Usage: $0 YYYY-MM-DD [--reset]}"; mode="${2:-daily}"; profile="${DATABRICKS_PROFILE:-skadi2910-dev}"
root="${TRANSACTION_SOURCE_ROOT:-/Users/skadi2910/projects/mock-data-generator/data/output/v2/snapshots}"; sim=banking-20260705-20260710
src="$root/simulation_id=$sim/snapshot_type=full/business_date=$date/customer_transaction"
dst="dbfs:/Volumes/workspace/dev_source_landing/source_snapshot_files/banking_v2/snapshots/domain=customer_transaction/simulation_id=$sim/snapshot_type=full/business_date=$date/customer_transaction"
[[ -d "$src" ]] || { echo "Missing source: $src" >&2; exit 1; }
databricks fs cp --recursive "$src" "$dst" --profile "$profile"
job=dev_transaction_daily; [[ "$mode" == --reset ]] && job=dev_transaction_reset_day
databricks bundle run "$job" -t dev --profile "$profile" --params "business_date=$date"

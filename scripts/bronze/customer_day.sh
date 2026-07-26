#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; date="${1:?Usage: $0 YYYY-MM-DD [--reset]}"; mode="${2:-daily}"
profile="${DATABRICKS_PROFILE:-skadi2910-dev}"; source_root="${CUSTOMER_SOURCE_ROOT:-/Users/skadi2910/projects/mock-data-generator/data/output/v2/snapshots}"; sim="banking-20260705-20260710"
volume="dbfs:/Volumes/workspace/dev_source_landing/source_snapshot_files/banking_v2/snapshots/domain=customer_master"; snapshots="$volume/simulation_id=$sim/snapshot_type=full"
staged="$({ databricks fs ls "$snapshots" --profile "$profile" 2>/dev/null || true; } | sed -nE 's#.*business_date=([0-9]{4}-[0-9]{2}-[0-9]{2}).*#\1#p' | sort -u | paste -sd, -)"
PYTHONPATH="$root/src" "$root/.venv/bin/python" - "$date" "$staged" <<'PY'
import sys
from replay_validation.customer import validate_staged_prefix
validate_staged_prefix(sys.argv[1], filter(None, sys.argv[2].split(',')))
PY
[[ "$mode" == "--reset" && "$date" != "2026-07-05" ]] && exit 2
[[ "$mode" != "--reset" && "$date" == "2026-07-05" ]] && exit 2
src="$source_root/simulation_id=$sim/snapshot_type=full/business_date=$date/customer_master"
[[ -d "$src" ]] || { echo "Missing source: $src" >&2; exit 1; }
databricks fs cp --recursive "$src" "$snapshots/business_date=$date/customer_master" --profile "$profile"
job=dev_customer_daily; [[ "$mode" == "--reset" ]] && job=dev_customer_reset_day
databricks bundle run "$job" -t dev --profile "$profile" --params "business_date=$date"

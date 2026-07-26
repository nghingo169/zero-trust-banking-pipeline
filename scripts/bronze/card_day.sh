#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
python_bin="$repo_root/.venv/bin/python"
if [[ ! -x "$python_bin" ]]; then
  python_bin="python3"
fi

business_date="${1:?Usage: bash scripts/bronze/card_day.sh YYYY-MM-DD [--reset]}"
mode="${2:-daily}"
if [[ "$mode" != "daily" && "$mode" != "--reset" ]]; then
  echo "Second argument must be --reset when running the first replay date." >&2
  exit 2
fi

profile="${DATABRICKS_PROFILE:-skadi2910-dev}"
source_root="${CARD_SOURCE_ROOT:-/Users/skadi2910/projects/mock-data-generator/data/output/v2/snapshots}"
simulation_id="banking-20260705-20260710"
source_path="${source_root}/simulation_id=${simulation_id}/snapshot_type=full/business_date=${business_date}/card"
volume_root="dbfs:/Volumes/workspace/dev_source_landing/source_snapshot_files/banking_v2/snapshots/domain=card"
snapshot_root="${volume_root}/simulation_id=${simulation_id}/snapshot_type=full"
destination_path="${volume_root}/simulation_id=${simulation_id}/snapshot_type=full/business_date=${business_date}/card"

if [[ ! -d "$source_path" ]]; then
  echo "Local Card snapshot directory not found: $source_path" >&2
  exit 1
fi

staged_dates="$({ databricks fs ls "$snapshot_root" --profile "$profile" 2>/dev/null || true; } | sed -nE 's#.*business_date=([0-9]{4}-[0-9]{2}-[0-9]{2}).*#\1#p' | sort -u | paste -sd, -)"
PYTHONPATH="$repo_root/src${PYTHONPATH:+:$PYTHONPATH}" "$python_bin" - "$business_date" "$staged_dates" <<'PY'
import sys
from replay_validation.card import validate_staged_prefix

validate_staged_prefix(sys.argv[1], filter(None, sys.argv[2].split(",")))
PY

if [[ "$mode" == "--reset" && "$business_date" != "2026-07-05" ]]; then
  echo "Only 2026-07-05 can use the reset-day job." >&2
  exit 2
fi

if [[ "$mode" == "daily" && "$business_date" == "2026-07-05" ]]; then
  echo "2026-07-05 must use --reset after a clean development reset." >&2
  exit 1
fi

databricks fs cp --recursive "$source_path" "$destination_path" --profile "$profile"
job="dev_card_daily"
if [[ "$mode" == "--reset" ]]; then
  job="dev_card_reset_day"
fi
databricks bundle run "$job" -t dev --profile "$profile" \
  --params "business_date=${business_date}"

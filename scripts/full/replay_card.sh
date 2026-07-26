#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
cd "$repo_root"
python_bin="$repo_root/.venv/bin/python"
if [[ ! -x "$python_bin" ]]; then
  python_bin="python3"
fi

# Performs the destructive reset only for Card's source-landing partition.
profile="${DATABRICKS_PROFILE:-skadi2910-dev}"
simulation_root="dbfs:/Volumes/workspace/dev_source_landing/source_snapshot_files/banking_v2/snapshots/domain=card/simulation_id=banking-20260705-20260710"
run_ids_file="${CARD_RUN_IDS_FILE:-card_dev_six_day_run_ids.txt}"
dates=(2026-07-05 2026-07-06 2026-07-07 2026-07-08 2026-07-09 2026-07-10)

wait_for_job() {
  local run_id="$1"
  local state
  while true; do
    state="$(databricks jobs get-run "$run_id" --profile "$profile" -o json | "$python_bin" -c '
import json
import sys
run = json.load(sys.stdin)
state = run["state"]
print(state["life_cycle_state"], state.get("result_state", ""))
')"
    case "$state" in
      "TERMINATED SUCCESS") return 0 ;;
      TERMINATED*) echo "Card job $run_id failed: $state" >&2; return 1 ;;
      *) sleep 15 ;;
    esac
  done
}

databricks bundle validate -t dev --profile "$profile"
databricks bundle deploy -t dev --profile "$profile"
if databricks fs ls "$simulation_root" --profile "$profile" >/dev/null 2>&1; then
  databricks fs rm -r "$simulation_root" --profile "$profile"
fi
: > "$run_ids_file"

for date in "${dates[@]}"; do
  if [[ "$date" == "2026-07-05" ]]; then
    output="$(bash scripts/bronze/card_day.sh "$date" --reset 2>&1)"
  else
    output="$(bash scripts/bronze/card_day.sh "$date" 2>&1)"
  fi
  printf '%s\n' "$output"
  # Bundle-run progress text may surround the JSON output. The run URL is
  # emitted consistently, so extract its numeric run ID rather than assuming
  # stdout is a standalone JSON document.
  run_id="$(printf '%s\n' "$output" | sed -nE 's#.*/runs/([0-9]+)(\?.*)?$#\1#p' | tail -n 1)"
  if [[ -z "$run_id" ]]; then
    echo "Could not find a Databricks run_id in bundle output for $date" >&2
    exit 1
  fi
  printf '%s\n' "$run_id" >> "$run_ids_file"
  wait_for_job "$run_id"
done

echo "Daily Databricks run IDs retained in $run_ids_file"

#!/usr/bin/env bash
# Stage the complete local six-day source snapshot set into the shared, immutable
# source-landing Volume. This command deliberately does not invoke Bronze,
# Silver, quarantine, or audit jobs.
set -euo pipefail

profile="${DATABRICKS_PROFILE:-skadi2910-dev}"
source_root="${LOCAL_SNAPSHOT_ROOT:-/Users/skadi2910/projects/mock-data-generator/data/output/v2/snapshots}"
simulation_id="${SIMULATION_ID:-banking-20260705-20260710}"
landing_root="${SOURCE_LANDING_ROOT:-dbfs:/Volumes/workspace/dev_source_landing/source_snapshot_files/banking_v2/snapshots}"
IFS=, read -r -a dates <<< "${SOURCE_LANDING_DATES:-2026-07-05,2026-07-06,2026-07-07,2026-07-08,2026-07-09,2026-07-10}"
IFS=, read -r -a domains <<< "${SOURCE_LANDING_DOMAINS:-card,customer_master,customer_transaction,financial_crime}"

source_simulation_root="${source_root}/simulation_id=${simulation_id}/snapshot_type=full"
if [[ ! -d "$source_simulation_root" ]]; then
  echo "Local snapshot root not found: $source_simulation_root" >&2
  exit 1
fi

for domain in "${domains[@]}"; do
  for business_date in "${dates[@]}"; do
    source_path="${source_simulation_root}/business_date=${business_date}/${domain}"
    target_path="${landing_root}/domain=${domain}/simulation_id=${simulation_id}/snapshot_type=full/business_date=${business_date}/${domain}"
    if [[ ! -d "$source_path" ]]; then
      echo "Missing local source directory: $source_path" >&2
      exit 1
    fi
    # A new day can be copied as one directory. For an interrupted day, resume
    # safely table-by-table and never overwrite a raw Parquet file.
    if ! databricks fs ls "$target_path" --profile "$profile" >/dev/null 2>&1; then
      echo "Staging ${domain} ${business_date}"
      databricks fs cp --recursive "$source_path" "$target_path" --profile "$profile"
      continue
    fi
    for source_table_path in "$source_path"/*; do
      source_table="$(basename "$source_table_path")"
      target_table_path="${target_path}/${source_table}"
      if databricks fs ls "$target_table_path" --profile "$profile" >/dev/null 2>&1; then
        echo "Already staged ${domain} ${business_date} ${source_table}"
        continue
      fi
      echo "Staging ${domain} ${business_date} ${source_table}"
      databricks fs cp --recursive "$source_table_path" "$target_table_path" --profile "$profile"
    done
  done
done

echo "Source landing complete: ${landing_root}"

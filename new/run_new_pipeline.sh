#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 3 ]; then
  echo "Usage: $0 <run_id> <biz_date> <old_summary_csv>"
  echo "Example: $0 full_20260309_01 2026-03-09 output/old_summary_full_20260309_01.csv"
  exit 1
fi

RUN_ID="$1"
BIZ_DATE="$2"
OLD_SUMMARY_CSV="$3"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPORT_PATH="output/compare_report_${RUN_ID}.md"

python "${PROJECT_ROOT}/new/validate_new.py" import \
  --env "${PROJECT_ROOT}/new/env.yml" \
  --run-id "${RUN_ID}" \
  --input "${OLD_SUMMARY_CSV}"

python "${PROJECT_ROOT}/new/validate_new.py" compute \
  --env "${PROJECT_ROOT}/new/env.yml" \
  --config "${PROJECT_ROOT}/new/rules.generated.yml" \
  --run-id "${RUN_ID}" \
  --biz-date "${BIZ_DATE}"

python "${PROJECT_ROOT}/new/validate_new.py" compare \
  --env "${PROJECT_ROOT}/new/env.yml" \
  --run-id "${RUN_ID}" \
  --config "${PROJECT_ROOT}/new/rules.generated.yml" \
  --batch-id "${RUN_ID}" \
  --env-tag "prod" \
  --created-by "ops" \
  --note "server run"

python "${PROJECT_ROOT}/new/validate_new.py" report \
  --env "${PROJECT_ROOT}/new/env.yml" \
  --run-id "${RUN_ID}" \
  --output "${REPORT_PATH}"

echo "Report generated: ${REPORT_PATH}"

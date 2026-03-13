#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <run_id> <biz_date> [output_csv]"
  echo "Example: $0 full_20260309_01 2026-03-09 output/old_summary_full_20260309_01.csv"
  exit 1
fi

RUN_ID="$1"
BIZ_DATE="$2"
OUTPUT_CSV="${3:-output/old_summary_${RUN_ID}.csv}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PYTHON_BIN="python3.6"
if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  PYTHON_BIN="python3"
fi

"${PYTHON_BIN}" "${PROJECT_ROOT}/old/validate_old.py" \
  --config "${PROJECT_ROOT}/old/rules.generated.yml" \
  --env "${PROJECT_ROOT}/old/env.yml" \
  --run-id "${RUN_ID}" \
  --biz-date "${BIZ_DATE}" \
  --output "${OUTPUT_CSV}"

#!/usr/bin/env bash
#
# run_etl.sh — drive the retail ETL pipeline from the command line.
#
# Uploads a local CSV to the raw/ prefix in S3, which triggers the
# retail-clean Lambda, then polls processed/ until a NEW cleaned
# Parquet file appears and prints a summary.
#
# Usage:
#
#   Run the pipeline
#   ./run_etl.sh <local-csv>
#
#   Show what would happen, change nothing
#   ./run_etl.sh --dry-run <local-csv>
#
set -euo pipefail

BUCKET="uci-clv-demo-bucket"
RAW_PREFIX="raw"
PROCESSED_KEY="processed/online_retail_clean.parquet"
LAMBDA_LOG_GROUP="/aws/lambda/retail-clean"
POLL_TIMEOUT=120
POLL_INTERVAL=5
DRY_RUN=false
LOCAL_FILE=""

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    -h|--help) echo "Usage: $0 [--dry-run] <local-csv>"; exit 0 ;;
    -*) echo "ERROR: unknown option: $1" >&2; exit 1 ;;
    *)  LOCAL_FILE="$1"; shift ;;
  esac
done

if [[ -z "$LOCAL_FILE" ]]; then
  echo "ERROR: no input file given." >&2
  echo "Usage: $0 [--dry-run] <local-csv>" >&2
  exit 1
fi
if [[ ! -f "$LOCAL_FILE" ]]; then
  echo "ERROR: file not found: $LOCAL_FILE" >&2
  exit 1
fi

FILENAME="$(basename "$LOCAL_FILE")"
DEST="s3://${BUCKET}/${RAW_PREFIX}/${FILENAME}"

echo "==> Source:      $LOCAL_FILE"
echo "==> Destination: $DEST"

# Dry run: describe & exit
if $DRY_RUN; then
  echo "[dry-run] Would upload the file above to $DEST"
  echo "[dry-run] Would then poll for a new $PROCESSED_KEY (timeout ${POLL_TIMEOUT}s)"
  exit 0
fi

# Record the processed file's current timestamp (if it exists at all) so
# we can tell a freshly-written file apart from the one already there.
BEFORE="$(aws s3 ls "s3://${BUCKET}/${PROCESSED_KEY}" 2>/dev/null | awk '{print $1" "$2}' || true)"

echo "==> Uploading to raw/ ..."
aws s3 cp "$LOCAL_FILE" "$DEST"

echo "==> Waiting for the Lambda to produce ${PROCESSED_KEY} (timeout ${POLL_TIMEOUT}s) ..."
ELAPSED=0
while (( ELAPSED < POLL_TIMEOUT )); do
  AFTER="$(aws s3 ls "s3://${BUCKET}/${PROCESSED_KEY}" 2>/dev/null | awk '{print $1" "$2}' || true)"
  if [[ -n "$AFTER" && "$AFTER" != "$BEFORE" ]]; then
    echo "==> Done. Cleaned file written at: $AFTER"
    echo "--- processed object ---"
    aws s3 ls "s3://${BUCKET}/${PROCESSED_KEY}" --human-readable
    exit 0
  fi
  sleep "$POLL_INTERVAL"
  ELAPSED=$(( ELAPSED + POLL_INTERVAL ))
  echo "    ...still waiting (${ELAPSED}s)"
done

echo "ERROR: timed out after ${POLL_TIMEOUT}s waiting for $PROCESSED_KEY" >&2
exit 1

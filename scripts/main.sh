#!/bin/bash
set -e # pipefail
set -x # echo on

RUNNER="DataflowRunner"
PROJECT="acme-fraudcop"
REGION="asia-east1"


python main.py \
  --runner "${RUNNER}" \
  --project "${PROJECT}" \
  --region "${REGION}" \
  --temp_location "${TEMP_LOCATION}" \
  --setup_file "${SETUP_FILE}" \
  --sink_table "${SINK_TABLE}" \
  --job_name "${JOB_NAME}"

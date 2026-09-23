#!/bin/bash
# Translate the cloud provider's array-job index env var into --job-index
#
#   AWS Batch:   AWS_BATCH_JOB_ARRAY_INDEX
#   Azure Batch: AZ_BATCH_TASK_ID
#   GCP Batch:   BATCH_TASK_INDEX
#
# --job-index is only injected when the env var is actually set by the cloud
# provider (i.e., for array worker jobs). Split and merge jobs, which don't
# accept --job-index, run without it.
#
# Usage:
#   docker run <image> ingest.gfm_exp.gfm_exp_col --mode batch-worker ...

# Resolve job index from whichever cloud provider's env var is set
if [ -n "$AWS_BATCH_JOB_ARRAY_INDEX" ]; then
    JOB_INDEX="$AWS_BATCH_JOB_ARRAY_INDEX"
elif [ -n "$AZ_BATCH_TASK_ID" ]; then
    JOB_INDEX="$AZ_BATCH_TASK_ID"
elif [ -n "$BATCH_TASK_INDEX" ]; then
    JOB_INDEX="$BATCH_TASK_INDEX"
fi

# Build extra CLI args from environment using an array to avoid word-splitting
EXTRA_ARGS=()
if [ -n "$JOB_INDEX" ]; then
    EXTRA_ARGS+=(--job-index "$JOB_INDEX")
fi
if [ -n "$AFTER_DATE" ]; then
    EXTRA_ARGS+=(--after-date "$AFTER_DATE")
fi
if [ -n "$BEFORE_DATE" ]; then
    EXTRA_ARGS+=(--before-date "$BEFORE_DATE")
fi
if [ -n "$DATES" ]; then
    EXTRA_ARGS+=(--dates "$DATES")
fi
if [ -n "$KEEP_PARTIALS" ] && [ "$KEEP_PARTIALS" != "0" ]; then
    EXTRA_ARGS+=(--keep-partials)
fi
if [ -n "$LIMIT" ]; then
    EXTRA_ARGS+=(--limit "$LIMIT")
fi

exec python3 -m "$@" "${EXTRA_ARGS[@]}"

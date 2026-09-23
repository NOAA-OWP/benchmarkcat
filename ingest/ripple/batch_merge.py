"""
ripple batch_merge — summarize a completed ripple batch run.

Reconciles the manifest against success/error markers, reports counts, and
clears retry-error markers for pairs that ultimately succeeded. STAC
cataloging (ripple_col.py) is separate and out of scope here.

Run this *once* after all batch-worker array jobs complete.

Usage:
    python -m ingest.ripple.batch_merge \
        --bucket_name fimc-data \
        --manifest-s3-key benchmark/ripple_v0.11.x/batch/ripple_manifest.jsonl \
        --success-markers-prefix benchmark/ripple_v0.11.x/status/success/ \
        --error-retry-prefix benchmark/ripple_v0.11.x/status/errors/retry/ \
        --error-nonretry-prefix benchmark/ripple_v0.11.x/status/errors/nonretry/
"""

import argparse
import json
import logging

from ingest.ripple.batch_utils import DEFAULT_CONFIG_PATH, get_s3_utils, list_marker_pairs, load_flow_files

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_retry_markers(s3_utils, bucket_name, error_retry_prefix, succeeded_pairs, dry_run=False):
    """Delete retry-error markers for pairs that ultimately succeeded."""
    to_clean = list_marker_pairs(s3_utils, bucket_name, error_retry_prefix) & succeeded_pairs
    if not to_clean:
        return 0

    keys = [
        {"Key": f"{error_retry_prefix.rstrip('/')}/{dir_name}_{flow_file.replace('.csv', '')}.error.json"}
        for dir_name, flow_file in to_clean
    ]

    if dry_run:
        logger.info("Would delete %d stale retry markers", len(keys))
        return len(keys)

    for i in range(0, len(keys), 1000):
        s3_utils.s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": keys[i : i + 1000], "Quiet": True})
    return len(keys)


def main():
    parser = argparse.ArgumentParser(description="Summarize ripple batch-worker outputs.")
    parser.add_argument("--bucket_name", type=str, required=True)
    parser.add_argument("--manifest-s3-key", type=str, required=True)
    parser.add_argument("--success-markers-prefix", type=str, default="benchmark/ripple_v0.11.x/status/success/")
    parser.add_argument("--error-retry-prefix", type=str, default="benchmark/ripple_v0.11.x/status/errors/retry/")
    parser.add_argument("--error-nonretry-prefix", type=str, default="benchmark/ripple_v0.11.x/status/errors/nonretry/")
    parser.add_argument("--config-path", type=str, default=DEFAULT_CONFIG_PATH, help="Path to worker_config.yaml (for flow_files list).")
    parser.add_argument("--profile", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    s3_utils = get_s3_utils(args.profile)
    all_flow_file_names = [f["name"] for f in load_flow_files(args.config_path)]

    response = s3_utils.s3_client.get_object(Bucket=args.bucket_name, Key=args.manifest_s3_key)
    manifest_dir_names = [json.loads(line)["dir_name"] for line in response["Body"].read().decode("utf-8").splitlines() if line.strip()]
    manifest_pairs = {(dir_name, flow_file) for dir_name in manifest_dir_names for flow_file in all_flow_file_names}
    logger.info("Manifest contains %d dir_name(s) (%d expected interval pair(s))", len(manifest_dir_names), len(manifest_pairs))

    succeeded = list_marker_pairs(s3_utils, args.bucket_name, args.success_markers_prefix)
    nonretry_failed = list_marker_pairs(s3_utils, args.bucket_name, args.error_nonretry_prefix)
    retry_failed = list_marker_pairs(s3_utils, args.bucket_name, args.error_retry_prefix)

    manifest_nonretry_failed = manifest_pairs & nonretry_failed
    missing = manifest_pairs - succeeded - nonretry_failed - retry_failed

    logger.info("Succeeded:            %d", len(manifest_pairs & succeeded))
    logger.info("Failed (non-retry):   %d", len(manifest_nonretry_failed))
    logger.info("Failed (retry):       %d  <- rerun batch_split + worker to retry these", len(manifest_pairs & retry_failed))
    logger.info("No marker written:    %d  <- job likely still running, timed out, or crashed", len(missing))

    cleaned = clean_retry_markers(s3_utils, args.bucket_name, args.error_retry_prefix, succeeded, args.dry_run)
    logger.info("Cleaned up %d stale retry marker(s)", cleaned)

    if manifest_nonretry_failed:
        logger.warning("Non-retry failures require manual investigation before rerun:")
        for dir_name, flow_file in sorted(manifest_nonretry_failed):
            logger.warning("  %s / %s", dir_name, flow_file)


if __name__ == "__main__":
    main()

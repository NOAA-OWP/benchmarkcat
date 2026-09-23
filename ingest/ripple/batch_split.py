"""
ripple batch_split — discover ripple library directories under the source
prefix and write a JSONL manifest of dir_names still needing processing.

A dir_name is skipped only if all 6 flow intervals already have a terminal
marker (success or non-retry error); otherwise it's re-included, since
per-interval markers make reruns idempotent.

Run this *once* before submitting the AWS Batch array job.

Usage:
    python -m ingest.ripple.batch_split \
        --bucket_name fimc-data \
        --asset_object_key ripple/v0.11.x/successes/ \
        --manifest-s3-key benchmark/ripple_v0.11.x/batch/ripple_manifest.jsonl \
        --success-markers-prefix benchmark/ripple_v0.11.x/status/success/ \
        --error-nonretry-prefix benchmark/ripple_v0.11.x/status/errors/nonretry/ \
        [--limit 10] [--profile my-profile]
"""

import argparse
import logging
from collections import defaultdict

from ingest.batch_utils import write_manifest
from ingest.ripple.batch_utils import DEFAULT_CONFIG_PATH, get_s3_utils, list_marker_pairs, load_flow_files

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def discover_ripple_work_items(s3_utils, bucket_name, asset_object_key, success_markers_prefix, error_nonretry_prefix, flow_files):
    """Discover dir_names that still have at least one incomplete flow interval."""
    all_flow_file_names = {f["name"] for f in flow_files}

    prefix = asset_object_key.rstrip("/") + "/"
    dir_names = sorted(p.rstrip("/").split("/")[-1] for p in s3_utils.list_subdirectories(bucket_name, prefix))
    logger.info("Found %d ripple library directories under s3://%s/%s", len(dir_names), bucket_name, prefix)

    completed = list_marker_pairs(s3_utils, bucket_name, success_markers_prefix)
    completed |= list_marker_pairs(s3_utils, bucket_name, error_nonretry_prefix)

    completed_by_dir = defaultdict(set)
    for dir_name, flow_file in completed:
        completed_by_dir[dir_name].add(flow_file)

    work_items = [
        {"dir_name": dir_name}
        for dir_name in dir_names
        if not completed_by_dir.get(dir_name, set()) >= all_flow_file_names
    ]

    logger.info(
        "%d of %d dir_names have all %d intervals complete; %d need processing",
        len(dir_names) - len(work_items), len(dir_names), len(flow_files), len(work_items),
    )
    return work_items


def main():
    parser = argparse.ArgumentParser(description="Build ripple dir_name manifest for batch processing.")
    parser.add_argument("--bucket_name", type=str, required=True)
    parser.add_argument("--asset_object_key", type=str, default="ripple/v0.11.x/successes/", help="S3 prefix containing ripple library directories.")
    parser.add_argument("--manifest-s3-key", type=str, required=True, help="S3 key where the output manifest JSONL will be written.")
    parser.add_argument("--success-markers-prefix", type=str, default="benchmark/ripple_v0.11.x/status/success/")
    parser.add_argument("--error-nonretry-prefix", type=str, default="benchmark/ripple_v0.11.x/status/errors/nonretry/")
    parser.add_argument("--config-path", type=str, default=DEFAULT_CONFIG_PATH, help="Path to worker_config.yaml (for flow_files list).")
    parser.add_argument("--limit", type=int, default=None, help="Only include the first N outstanding dir_names — for a small initial test batch. Omit for a real run.")
    parser.add_argument("--profile", type=str, default=None, help="AWS profile name.")
    args = parser.parse_args()

    s3_utils = get_s3_utils(args.profile)
    flow_files = load_flow_files(args.config_path)

    work_items = discover_ripple_work_items(
        s3_utils, args.bucket_name, args.asset_object_key,
        args.success_markers_prefix, args.error_nonretry_prefix, flow_files,
    )
    logger.info("Total outstanding work items (dir_names): %d", len(work_items))

    if args.limit is not None:
        work_items = work_items[: args.limit]
        logger.info("--limit %d applied — manifest will contain %d dir_name(s)", args.limit, len(work_items))

    write_manifest(s3_utils, args.bucket_name, args.manifest_s3_key, work_items)
    logger.info("Manifest written to s3://%s/%s", args.bucket_name, args.manifest_s3_key)


if __name__ == "__main__":
    main()

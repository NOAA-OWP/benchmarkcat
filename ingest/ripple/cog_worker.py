"""
ripple cog_worker — convert one or more libraries' VRT outputs (stored under
a .tif extension) to GeoTIFF COGs.

flows2fim writes VRT format (worker_config.yaml's processing.output_format)
under a .tif filename. Each VRT references hundreds of small source tiles
via /vsis3/, so this must run inside AWS (same region as the bucket) — doing
it from a laptop means hundreds of small S3 GETs per file over the open
internet, which is far too slow at this scale.

Idempotent: skips any file that isn't VRT content (already converted, or
never was VRT), so reruns only touch what's still outstanding.

Usage:
    python -m ingest.ripple.cog_worker --dir_name mip_03110203 --bucket_name fimc-data

    python -m ingest.ripple.cog_worker \
        --bucket_name fimc-data \
        --manifest-s3-key benchmark/ripple_v0.11.x/batch/ripple_manifest.jsonl \
        --items-per-job 5 --job-index 0
"""

import argparse
import json
import logging
import os
import shutil
import subprocess
import tempfile

import boto3
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "worker_config.yaml")


def load_config(config_path):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def is_vrt(local_path):
    """VRT files are XML; real GeoTIFFs start with a TIFF magic byte sequence."""
    with open(local_path, "rb") as f:
        head = f.read(16)
    return head.startswith(b"<VRTDataset") or head.startswith(b"<?xml")


def convert_library(s3_client, bucket_name, dir_name, output_base_template, output_with_common_template, source, digit_string, common_name):
    """Download, convert, and re-upload every VRT-as-.tif under this library's output prefix."""
    if common_name:
        prefix = output_with_common_template.format(source=source, id=digit_string, common_name=common_name)
    else:
        prefix = output_base_template.format(source=source, id=digit_string)

    paginator = s3_client.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".tif")
    ]

    if not keys:
        logger.warning(f"No .tif files found under s3://{bucket_name}/{prefix}")
        return True

    work_dir = tempfile.mkdtemp(prefix="cog_")
    all_ok = True
    try:
        for key in keys:
            local_in = os.path.join(work_dir, "in.tif")
            local_out = os.path.join(work_dir, "out.tif")

            s3_client.download_file(bucket_name, key, local_in)

            if not is_vrt(local_in):
                logger.info(f"Skipping (already GeoTIFF): {key}")
                os.remove(local_in)
                continue

            result = subprocess.run(
                [
                    "gdal_translate",
                    "-of", "COG",
                    "-co", "COMPRESS=DEFLATE",
                    "-co", "NUM_THREADS=ALL_CPUS",
                    "-co", "OVERVIEW_RESAMPLING=NEAREST",
                    local_in, local_out,
                ],
                capture_output=True, text=True,
            )

            if result.returncode != 0 or not os.path.exists(local_out):
                logger.error(f"Failed to convert {key}: {result.stderr}")
                all_ok = False
                os.remove(local_in)
                continue

            s3_client.upload_file(local_out, bucket_name, key)
            logger.info(f"Converted: {key}")
            os.remove(local_in)
            os.remove(local_out)
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)

    return all_ok


def process_dir_name(s3_client, bucket_name, dir_name, config):
    # id isn't always numeric (e.g. ohio_rfc, mn_Other, nc_Other), so accept any second segment.
    parts = dir_name.split("_")
    if len(parts) < 2:
        logger.error(f"Invalid directory name format: {dir_name}")
        return False

    source = parts[0]
    digit_string = parts[1]
    common_name = "_".join(parts[2:]) if len(parts) > 2 else None

    output_base_template = config["s3"]["paths"]["output_base"]
    output_with_common_template = config["s3"]["paths"]["output_with_common"]

    logger.info(f"Processing {dir_name}")
    return convert_library(s3_client, bucket_name, dir_name, output_base_template, output_with_common_template, source, digit_string, common_name)


def _read_manifest_slice(s3_client, bucket_name, manifest_s3_key, items_per_job, job_index):
    response = s3_client.get_object(Bucket=bucket_name, Key=manifest_s3_key)
    raw = response["Body"].read().decode("utf-8")
    all_items = [json.loads(line) for line in raw.splitlines() if line.strip()]

    start = job_index * items_per_job
    end = start + items_per_job
    return all_items[start:end]


def parse_args():
    parser = argparse.ArgumentParser(description="Convert ripple VRT outputs to real COGs, in place.")
    parser.add_argument("--bucket_name", type=str, required=True)
    parser.add_argument("--config-path", type=str, default=DEFAULT_CONFIG_PATH)

    # single-item mode (local testing / manual reruns)
    parser.add_argument("--dir_name", type=str, default=None)

    # array-job mode (AWS Batch)
    parser.add_argument("--manifest-s3-key", type=str, default=None, help="S3 key of the split-phase manifest JSONL.")
    parser.add_argument("--items-per-job", type=int, default=1, help="Number of libraries each array child processes.")
    parser.add_argument("--job-index", type=int, default=None, help="Array child index (injected by batch-entrypoint.sh).")

    return parser.parse_args()


def main():
    args = parse_args()
    config = load_config(args.config_path)
    s3_client = boto3.client("s3")

    if args.dir_name:
        dir_names = [args.dir_name]
    elif args.manifest_s3_key and args.job_index is not None:
        items = _read_manifest_slice(s3_client, args.bucket_name, args.manifest_s3_key, args.items_per_job, args.job_index)
        dir_names = [item["dir_name"] for item in items]
        if not dir_names:
            logger.info(f"No dir_names for job_index={args.job_index} — nothing to do.")
            return
    else:
        raise ValueError("Must provide either --dir_name or --manifest-s3-key + --job-index")

    results = [process_dir_name(s3_client, args.bucket_name, d, config) for d in dir_names]
    if not all(results):
        raise SystemExit(1)

    logger.info(f"All {len(dir_names)} librar(y/ies) converted successfully.")


if __name__ == "__main__":
    main()

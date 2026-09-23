"""Shared helpers for ripple's batch_split.py and batch_merge.py."""

import os
import re

import boto3
import yaml

DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "worker_config.yaml")

MARKER_PATTERN = re.compile(r"(.+?)_flows_(\d+year)\.(?:success|error)\.json$")


def load_flow_files(config_path=DEFAULT_CONFIG_PATH):
    """Load the flow_files list from worker_config.yaml (same list extent_worker.py uses,
    same shape as the original Nomad pipeline's coord_config.yaml)."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config["flow_files"]


def list_marker_pairs(s3_utils, bucket_name, prefix):
    """Return the set of (dir_name, flow_file) pairs with a success/error marker under prefix."""
    paginator = s3_utils.s3_client.get_paginator("list_objects_v2")
    pairs = set()
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            match = MARKER_PATTERN.match(obj["Key"].split("/")[-1])
            if match:
                dir_name, flow_base = match.groups()
                pairs.add((dir_name, f"flows_{flow_base}.csv"))
    return pairs


def get_s3_utils(profile):
    """Resolve AWS_PROFILE and return an S3Utils client."""
    from ingest.utils import S3Utils

    if profile is not None:
        os.environ["AWS_PROFILE"] = profile
    else:
        os.environ.pop("AWS_PROFILE", None)
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    return S3Utils(session.client("s3"))

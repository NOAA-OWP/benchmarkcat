#!/usr/bin/env python3
"""
Benchmarkcat — Prefect AWS Batch Pipeline.

Orchestrator for the 3-phase AWS Batch pipeline.

How to run:

  1. Start Prefect server (one terminal):
       prefect server start
     Keep it running. Default UI at http://127.0.0.1:4200

  2. Run the flow (another terminal, from repo root):
       python scripts/run_pipeline_prefect.py --pipeline gfm --dry-run
       python scripts/run_pipeline_prefect.py --pipeline gfm

  If the server runs elsewhere (e.g. EC2), set PREFECT_API_URL before running:
       export PREFECT_API_URL="http://<host>:4200/api"
"""

import argparse
import asyncio
import json
import math
import subprocess
from datetime import datetime, timezone

import boto3

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact, create_table_artifact

MAX_ARRAY_SIZE = 10000
MIN_ARRAY_SIZE = 2


# ---------------------------------------------------------------------------
# Terraform helpers
# ---------------------------------------------------------------------------

def get_terraform_outputs() -> dict:
    """Read terraform outputs. Returns empty dict if terraform is unavailable."""
    try:
        result = subprocess.run(
            ["terraform", "output", "-json"],
            cwd="terraform",
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            raw = json.loads(result.stdout)
            return {k: v["value"] for k, v in raw.items()}
    except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError, KeyError):
        pass
    return {}


def build_config(args, tf_outputs: dict) -> dict:
    """Merge terraform outputs and CLI overrides into a config dict.

    Terraform is the single source of truth for infrastructure values.
    CLI overrides take precedence over terraform outputs.
    """
    cfg = {}

    cfg["aws_region"] = args.region if args.region is not None else tf_outputs.get("aws_region")
    cfg["aws_profile"] = args.profile if args.profile is not None else tf_outputs.get("aws_profile")
    cfg["s3_profile"] = args.s3_profile if args.s3_profile is not None else cfg["aws_profile"]
    cfg["s3_bucket"] = args.bucket_name if args.bucket_name is not None else tf_outputs.get("s3_bucket")
    cfg["scenes_per_job"] = (
        args.scenes_per_job if args.scenes_per_job is not None
        else tf_outputs.get("scenes_per_job", 50)
    )
    cfg["workers"] = (
        args.workers if args.workers is not None
        else tf_outputs.get("workers", 1)
    )
    cfg["project_name"] = (
        args.project_name if args.project_name is not None
        else tf_outputs.get("project_name")
    )
    cfg["catalog_path"] = tf_outputs.get("catalog_path")
    cfg["hucs_object_key"] = tf_outputs.get("hucs_object_key")
    cfg["boundaries_object_key"] = tf_outputs.get("boundaries_object_key")
    cfg["readme_object_key"] = tf_outputs.get("gfm_readme_object_key") or ""
    cfg["job_queue_name"] = tf_outputs.get("job_queue_name")

    pipeline = args.pipeline
    tf_pipeline = tf_outputs.get(f"{pipeline}_config") or {}
    job_def_names = tf_outputs.get("job_definition_names") or {}
    project_name = cfg["project_name"] or "benchmarkcat"
    pipeline_key = pipeline.replace("_", "-")

    cfg["split_job_def"] = job_def_names.get(f"{pipeline_key}-split") or f"{project_name}-{pipeline_key}-split"
    cfg["worker_job_def"] = job_def_names.get(f"{pipeline_key}-worker") or f"{project_name}-{pipeline_key}-worker"
    cfg["merge_job_def"] = job_def_names.get(f"{pipeline_key}-merge") or f"{project_name}-{pipeline_key}-merge"

    cfg["asset_object_key"] = tf_pipeline.get("asset_object_key")
    cfg["manifest_s3_key"] = tf_pipeline.get("manifest_s3_key")
    cfg["partial_parquet_prefix"] = tf_pipeline.get("partial_parquet_prefix")
    cfg["derived_metadata_path"] = tf_pipeline.get("derived_metadata_path")
    cfg["dfo_geopackage_object_key"] = tf_pipeline.get("dfo_geopackage_object_key")

    if pipeline == "ripple":
        ripple_job_def_names = tf_outputs.get("ripple_job_definition_names") or {}
        cfg["split_job_def"] = ripple_job_def_names.get("ripple-split") or f"{project_name}-ripple-split"
        cfg["worker_job_def"] = ripple_job_def_names.get("ripple-worker") or f"{project_name}-ripple-worker"
        cfg["merge_job_def"] = ripple_job_def_names.get("ripple-merge") or f"{project_name}-ripple-merge"
        cfg["cog_job_def"] = ripple_job_def_names.get("ripple-cog") or f"{project_name}-ripple-cog"
        cfg["success_markers_prefix"] = tf_pipeline.get("success_markers_prefix")
        cfg["error_retry_prefix"] = tf_pipeline.get("error_retry_prefix")
        cfg["error_nonretry_prefix"] = tf_pipeline.get("error_nonretry_prefix")
        cfg["items_per_job"] = (
            args.items_per_job if getattr(args, "items_per_job", None) is not None
            else tf_pipeline.get("items_per_job", 5)
        )

    return cfg


# ---------------------------------------------------------------------------
# AWS Batch helpers
# ---------------------------------------------------------------------------

def get_aws_client(profile: str | None, region: str, service: str):
    """Return a boto3 client for the given AWS service."""
    if profile:
        session = boto3.Session(profile_name=profile, region_name=region)
    else:
        session = boto3.Session(region_name=region)
    return session.client(service)


def submit_job(batch_client, job_name, job_definition, job_queue, parameters,
               array_size=None, container_overrides=None, dry_run=False):
    """Submit a Batch job. Returns job_id (or None for dry-run)."""
    kwargs = {
        "jobName": job_name,
        "jobDefinition": job_definition,
        "jobQueue": job_queue,
        "parameters": parameters,
    }
    if array_size is not None and array_size >= MIN_ARRAY_SIZE:
        kwargs["arrayProperties"] = {"size": array_size}
    if container_overrides:
        kwargs["containerOverrides"] = container_overrides

    if dry_run:
        print(f"[DRY RUN] Would submit: {json.dumps(kwargs, indent=2)}")
        return None
    print(f"Submitting job: {json.dumps(kwargs, indent=2)}")
    response = batch_client.submit_job(**kwargs)
    return response["jobId"]


def read_manifest_total(s3_client, bucket: str, manifest_s3_key: str) -> int:
    """Read the manifest metadata sidecar to get total_scenes."""
    meta_key = manifest_s3_key + ".meta.json"
    try:
        response = s3_client.get_object(Bucket=bucket, Key=meta_key)
        meta = json.loads(response["Body"].read().decode("utf-8"))
        return meta["total_scenes"]
    except Exception as e:
        raise RuntimeError(
            f"Could not read manifest metadata from s3://{bucket}/{meta_key}: {e}\n"
            "Did the split job complete successfully?"
        )


REQUIRED_CFG_KEYS = [
    "job_queue_name",
    "s3_bucket",
    "catalog_path",
    "asset_object_key",
    "manifest_s3_key",
    "partial_parquet_prefix",
    "derived_metadata_path",
    "hucs_object_key",
    "boundaries_object_key",
]

# Ripple has no STAC cataloging step, so its required keys are a smaller set.
REQUIRED_CFG_KEYS_RIPPLE = [
    "job_queue_name",
    "s3_bucket",
    "asset_object_key",
    "manifest_s3_key",
    "success_markers_prefix",
    "error_retry_prefix",
    "error_nonretry_prefix",
]


# ---------------------------------------------------------------------------
# Async polling helper
# ---------------------------------------------------------------------------

async def async_poll_until_complete(
    batch_client,
    job_id: str,
    job_name: str,
    poll_interval: int = 30,
) -> dict:
    """Poll a Batch job until SUCCEEDED or FAILED."""
    logger = get_run_logger()
    MAX_NOT_FOUND_RETRIES = 10
    not_found_retries = 0

    while True:
        response = batch_client.describe_jobs(jobs=[job_id])

        if not response["jobs"]:
            not_found_retries += 1
            if not_found_retries > MAX_NOT_FOUND_RETRIES:
                raise RuntimeError(
                    f"Job {job_id} not found after {MAX_NOT_FOUND_RETRIES} retries. "
                    "It may have been deleted or the ID is invalid."
                )
            logger.warning(
                "Job %s not found (attempt %d/%d), retrying...",
                job_id, not_found_retries, MAX_NOT_FOUND_RETRIES,
            )
            await asyncio.sleep(poll_interval)
            continue

        not_found_retries = 0
        job = response["jobs"][0]
        status = job["status"]
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")

        if "arrayProperties" in job:
            summary = job["arrayProperties"].get("statusSummary", {})
            summary_str = "  ".join(f"{k}={v}" for k, v in sorted(summary.items()) if v > 0)
            logger.info("[%s] %s | %s | %s", ts, job_name, status, summary_str)
        else:
            logger.info("[%s] %s | %s", ts, job_name, status)

        if status == "FAILED":
            reason = job.get("statusReason", "unknown")
            raise RuntimeError(f"Batch job '{job_name}' ({job_id}) FAILED: {reason}")

        if status == "SUCCEEDED":
            logger.info("Job '%s' SUCCEEDED.", job_name)
            return job

        await asyncio.sleep(poll_interval)


# ---------------------------------------------------------------------------
# Phase tasks
# ---------------------------------------------------------------------------

@task(
    name="Phase 1 — Split",
    # disabled retries so the total is 1 attempt with batch job retries
    # retries=2,
    # retry_delay_seconds=60,
)
async def submit_and_poll_split(
    cfg: dict,
    pipeline: str,
    timestamp: str,
    poll_interval: int = 30,
    dry_run: bool = False,
    after_date: str | None = None,
    before_date: str | None = None,
    dates: str | None = None,
) -> dict:
    """Submit Split job to Batch and poll until complete."""
    logger = get_run_logger()
    batch_client = get_aws_client(cfg["aws_profile"], cfg["aws_region"], "batch")

    split_params = {
        "bucket_name": cfg["s3_bucket"],
        "asset_object_key": cfg["asset_object_key"],
        "manifest_s3_key": cfg["manifest_s3_key"],
    }
    date_env = []
    if after_date:
        date_env.append({"name": "AFTER_DATE", "value": after_date})
    if before_date:
        date_env.append({"name": "BEFORE_DATE", "value": before_date})
    if dates:
        date_env.append({"name": "DATES", "value": dates})
    split_overrides = {"environment": date_env} if date_env else None

    split_job_name = f"{pipeline}-split-{timestamp}"
    split_job_id = submit_job(
        batch_client,
        job_name=split_job_name,
        job_definition=cfg["split_job_def"],
        job_queue=cfg["job_queue_name"],
        parameters=split_params,
        container_overrides=split_overrides,
        dry_run=dry_run,
    )

    await create_markdown_artifact(
        key="split-job-details",
        markdown=(
            "## Phase 1 — Split Job\n\n"
            f"| Field | Value |\n|---|---|\n"
            f"| Job Name | `{split_job_name}` |\n"
            f"| Job ID | `{split_job_id or 'DRY RUN'}` |\n"
            f"| Job Definition | `{cfg['split_job_def']}` |\n"
            f"| Queue | `{cfg['job_queue_name']}` |\n"
            f"| Manifest Key | `{cfg['manifest_s3_key']}` |\n"
        ),
        description="Split phase AWS Batch job details",
    )

    if not dry_run and split_job_id:
        logger.info("Split job submitted: %s", split_job_id)
        await async_poll_until_complete(batch_client, split_job_id, split_job_name, poll_interval)

    return {"phase": "split", "job_id": split_job_id, "job_name": split_job_name}


@task(
    name="Phase 2 — Workers",
    # disabled retries so the total is 1 attempt with batch job retries
    # retries=1,
    # retry_delay_seconds=120,
)
async def submit_and_poll_workers(
    cfg: dict,
    pipeline: str,
    timestamp: str,
    poll_interval: int = 30,
    dry_run: bool = False,
) -> dict:
    """Compute array size from manifest, submit Workers job to Batch, poll until complete."""
    logger = get_run_logger()
    batch_client = get_aws_client(cfg["aws_profile"], cfg["aws_region"], "batch")
    s3_client = get_aws_client(cfg["s3_profile"], cfg["aws_region"], "s3")

    if dry_run:
        total_scenes = 100
        logger.info("[DRY RUN] Assuming %s scenes for array size calculation", total_scenes)
    else:
        total_scenes = read_manifest_total(s3_client, cfg["s3_bucket"], cfg["manifest_s3_key"])
        logger.info("Total scenes in manifest: %s", total_scenes)
        if total_scenes == 0:
            raise RuntimeError(
                "Split produced 0 scenes. Check date filters and S3 prefix."
            )

    scenes_per_job = int(cfg["scenes_per_job"])
    if total_scenes <= scenes_per_job:
        array_size = None
        array_desc = f"single job ({total_scenes} scenes ≤ {scenes_per_job}/job)"
        logger.info(
            "%s scenes <= %s/job → submitting as single job",
            total_scenes, scenes_per_job,
        )
    else:
        array_size = math.ceil(total_scenes / scenes_per_job)
        array_size = min(array_size, MAX_ARRAY_SIZE)
        actual_chunk = math.ceil(total_scenes / array_size)
        array_desc = f"array job — {array_size} children × ~{actual_chunk} scenes each"
        logger.info("Array size: %s children × ~%s scenes each", array_size, actual_chunk)

    worker_params = {
        "bucket_name": cfg["s3_bucket"],
        "catalog_path": cfg["catalog_path"],
        "asset_object_key": cfg["asset_object_key"],
        "hucs_object_key": cfg["hucs_object_key"],
        "boundaries_object_key": cfg["boundaries_object_key"],
        "derived_metadata_path": cfg["derived_metadata_path"],
        "manifest_s3_key": cfg["manifest_s3_key"],
        "partial_parquet_prefix": cfg["partial_parquet_prefix"],
        "scenes_per_job": str(scenes_per_job),
        "workers": str(cfg["workers"]),
    }
    readme_key = cfg.get("readme_object_key") or ""
    worker_params["readme_object_key"] = readme_key
    if pipeline in ("gfm", "gfm_exp") and not readme_key:
        raise RuntimeError(
            "Missing readme_object_key for GFM/GFM_EXP pipeline. "
            "Set gfm_readme_object_key in terraform.tfvars and run terraform apply."
        )
    if pipeline == "gfm":
        if not cfg.get("dfo_geopackage_object_key"):
            raise RuntimeError(
                "Missing dfo_geopackage_object_key for GFM pipeline. "
                "Set gfm_dfo_geopackage_object_key in terraform.tfvars and run terraform apply."
            )
        worker_params["dfo_geopackage_object_key"] = cfg["dfo_geopackage_object_key"]

    worker_job_name = f"{pipeline}-worker-{timestamp}"
    worker_job_id = submit_job(
        batch_client,
        job_name=worker_job_name,
        job_definition=cfg["worker_job_def"],
        job_queue=cfg["job_queue_name"],
        parameters=worker_params,
        array_size=array_size,
        dry_run=dry_run,
    )

    await create_markdown_artifact(
        key="workers-job-details",
        markdown=(
            "## Phase 2 — Workers Job\n\n"
            f"| Field | Value |\n|---|---|\n"
            f"| Job Name | `{worker_job_name}` |\n"
            f"| Job ID | `{worker_job_id or 'DRY RUN'}` |\n"
            f"| Job Definition | `{cfg['worker_job_def']}` |\n"
            f"| Queue | `{cfg['job_queue_name']}` |\n"
            f"| Total Scenes | `{total_scenes}` |\n"
            f"| Array Mode | `{array_desc}` |\n"
        ),
        description="Workers phase AWS Batch job details",
    )

    if not dry_run and worker_job_id:
        array_str = f" (array size={array_size})" if array_size else " (single job)"
        logger.info("Worker job submitted: %s%s", worker_job_id, array_str)
        await async_poll_until_complete(batch_client, worker_job_id, worker_job_name, poll_interval)

    return {"phase": "workers", "job_id": worker_job_id, "job_name": worker_job_name}


@task(
    name="Phase 3 — Merge",
    # disabled retries so the total is 1 attempt with batch job retries
    # retries=2,
    # retry_delay_seconds=60,
)
async def submit_and_poll_merge(
    cfg: dict,
    pipeline: str,
    timestamp: str,
    poll_interval: int = 30,
    dry_run: bool = False,
    keep_partials: bool = False,
) -> dict:
    """Submit Merge job to Batch and poll until complete."""
    logger = get_run_logger()
    batch_client = get_aws_client(cfg["aws_profile"], cfg["aws_region"], "batch")

    merge_params = {
        "bucket_name": cfg["s3_bucket"],
        "partial_parquet_prefix": cfg["partial_parquet_prefix"],
        "derived_metadata_path": cfg["derived_metadata_path"],
        "catalog_path": cfg["catalog_path"],
        "asset_object_key": cfg["asset_object_key"],
        "readme_object_key": cfg["readme_object_key"],
    }

    merge_overrides = (
        {"environment": [{"name": "KEEP_PARTIALS", "value": "1"}]}
        if keep_partials
        else None
    )

    merge_job_name = f"{pipeline}-merge-{timestamp}"
    merge_job_id = submit_job(
        batch_client,
        job_name=merge_job_name,
        job_definition=cfg["merge_job_def"],
        job_queue=cfg["job_queue_name"],
        parameters=merge_params,
        container_overrides=merge_overrides,
        dry_run=dry_run,
    )

    await create_markdown_artifact(
        key="merge-job-details",
        markdown=(
            "## Phase 3 — Merge Job\n\n"
            f"| Field | Value |\n|---|---|\n"
            f"| Job Name | `{merge_job_name}` |\n"
            f"| Job ID | `{merge_job_id or 'DRY RUN'}` |\n"
            f"| Job Definition | `{cfg['merge_job_def']}` |\n"
            f"| Queue | `{cfg['job_queue_name']}` |\n"
            f"| Output Path | `{cfg['derived_metadata_path']}` |\n"
        ),
        description="Merge phase AWS Batch job details",
    )

    if not dry_run and merge_job_id:
        logger.info("Merge job submitted: %s", merge_job_id)
        await async_poll_until_complete(batch_client, merge_job_id, merge_job_name, poll_interval)

    return {"phase": "merge", "job_id": merge_job_id, "job_name": merge_job_name}


# ---------------------------------------------------------------------------
# Phase tasks — ripple (flows2fim extent generation only; no STAC cataloging)
# ---------------------------------------------------------------------------

@task(name="Ripple Phase 1 — Split")
async def ripple_submit_and_poll_split(
    cfg: dict,
    timestamp: str,
    poll_interval: int = 30,
    dry_run: bool = False,
    limit: int | None = None,
) -> dict:
    """Submit ripple Split job to Batch and poll until complete."""
    logger = get_run_logger()
    batch_client = get_aws_client(cfg["aws_profile"], cfg["aws_region"], "batch")

    split_params = {
        "bucket_name": cfg["s3_bucket"],
        "ripple_asset_object_key": cfg["asset_object_key"],
        "manifest_s3_key": cfg["manifest_s3_key"],
        "ripple_success_markers_prefix": cfg["success_markers_prefix"],
        "ripple_error_nonretry_prefix": cfg["error_nonretry_prefix"],
    }
    split_overrides = None
    if limit is not None:
        logger.warning("--limit %d set — this is a TEST BATCH, not a full run.", limit)
        split_overrides = {"environment": [{"name": "LIMIT", "value": str(limit)}]}

    split_job_name = f"ripple-split-{timestamp}"
    split_job_id = submit_job(
        batch_client,
        job_name=split_job_name,
        job_definition=cfg["split_job_def"],
        job_queue=cfg["job_queue_name"],
        parameters=split_params,
        container_overrides=split_overrides,
        dry_run=dry_run,
    )

    await create_markdown_artifact(
        key="ripple-split-job-details",
        markdown=(
            "## Ripple Phase 1 — Split Job\n\n"
            f"| Field | Value |\n|---|---|\n"
            f"| Job Name | `{split_job_name}` |\n"
            f"| Job ID | `{split_job_id or 'DRY RUN'}` |\n"
            f"| Job Definition | `{cfg['split_job_def']}` |\n"
            f"| Manifest Key | `{cfg['manifest_s3_key']}` |\n"
            + (f"| **Limit** | **{limit} dir_names (TEST BATCH)** |\n" if limit is not None else "")
        ),
        description="Ripple split phase AWS Batch job details",
    )

    if not dry_run and split_job_id:
        logger.info("Ripple split job submitted: %s", split_job_id)
        await async_poll_until_complete(batch_client, split_job_id, split_job_name, poll_interval)

    return {"phase": "split", "job_id": split_job_id, "job_name": split_job_name}


@task(name="Ripple Phase 2 — Workers")
async def ripple_submit_and_poll_workers(
    cfg: dict,
    timestamp: str,
    poll_interval: int = 30,
    dry_run: bool = False,
) -> dict:
    """Compute array size from manifest, submit ripple Workers job to Batch, poll until complete."""
    logger = get_run_logger()
    batch_client = get_aws_client(cfg["aws_profile"], cfg["aws_region"], "batch")
    s3_client = get_aws_client(cfg["s3_profile"], cfg["aws_region"], "s3")

    if dry_run:
        total_items = 100
        logger.info("[DRY RUN] Assuming %s libraries for array size calculation", total_items)
    else:
        total_items = read_manifest_total(s3_client, cfg["s3_bucket"], cfg["manifest_s3_key"])
        logger.info("Total libraries (dir_names) in manifest: %s", total_items)
        if total_items == 0:
            logger.info("No outstanding dir_names — nothing to dispatch.")
            return {"phase": "workers", "job_id": None, "job_name": "SKIPPED (nothing outstanding)"}

    items_per_job = int(cfg["items_per_job"])
    if total_items <= items_per_job:
        array_size = None
        array_desc = f"single job ({total_items} librar(y/ies) <= {items_per_job}/job)"
    else:
        array_size = math.ceil(total_items / items_per_job)
        array_size = min(array_size, MAX_ARRAY_SIZE)
        actual_chunk = math.ceil(total_items / array_size)
        array_desc = f"array job — {array_size} children x ~{actual_chunk} librar(y/ies) each"
        logger.info("Array size: %s children x ~%s librar(y/ies) each", array_size, actual_chunk)

    worker_params = {
        "bucket_name": cfg["s3_bucket"],
        "manifest_s3_key": cfg["manifest_s3_key"],
        "items_per_job": str(items_per_job),
    }

    worker_job_name = f"ripple-worker-{timestamp}"
    worker_job_id = submit_job(
        batch_client,
        job_name=worker_job_name,
        job_definition=cfg["worker_job_def"],
        job_queue=cfg["job_queue_name"],
        parameters=worker_params,
        array_size=array_size,
        dry_run=dry_run,
    )

    await create_markdown_artifact(
        key="ripple-workers-job-details",
        markdown=(
            "## Ripple Phase 2 — Workers Job\n\n"
            f"| Field | Value |\n|---|---|\n"
            f"| Job Name | `{worker_job_name}` |\n"
            f"| Job ID | `{worker_job_id or 'DRY RUN'}` |\n"
            f"| Job Definition | `{cfg['worker_job_def']}` |\n"
            f"| Total Libraries | `{total_items}` |\n"
            f"| Array Mode | `{array_desc}` |\n"
        ),
        description="Ripple workers phase AWS Batch job details",
    )

    if not dry_run and worker_job_id:
        array_str = f" (array size={array_size})" if array_size else " (single job)"
        logger.info("Ripple worker job submitted: %s%s", worker_job_id, array_str)
        await async_poll_until_complete(batch_client, worker_job_id, worker_job_name, poll_interval)

    return {"phase": "workers", "job_id": worker_job_id, "job_name": worker_job_name}


@task(name="Ripple Phase 3 — Merge (summary)")
async def ripple_submit_and_poll_merge(
    cfg: dict,
    timestamp: str,
    poll_interval: int = 30,
    dry_run: bool = False,
) -> dict:
    """Submit ripple Merge (summary/cleanup) job to Batch and poll until complete."""
    logger = get_run_logger()
    batch_client = get_aws_client(cfg["aws_profile"], cfg["aws_region"], "batch")

    merge_params = {
        "bucket_name": cfg["s3_bucket"],
        "manifest_s3_key": cfg["manifest_s3_key"],
        "ripple_success_markers_prefix": cfg["success_markers_prefix"],
        "ripple_error_retry_prefix": cfg["error_retry_prefix"],
        "ripple_error_nonretry_prefix": cfg["error_nonretry_prefix"],
    }

    merge_job_name = f"ripple-merge-{timestamp}"
    merge_job_id = submit_job(
        batch_client,
        job_name=merge_job_name,
        job_definition=cfg["merge_job_def"],
        job_queue=cfg["job_queue_name"],
        parameters=merge_params,
        dry_run=dry_run,
    )

    await create_markdown_artifact(
        key="ripple-merge-job-details",
        markdown=(
            "## Ripple Phase 3 — Merge (summary) Job\n\n"
            f"| Field | Value |\n|---|---|\n"
            f"| Job Name | `{merge_job_name}` |\n"
            f"| Job ID | `{merge_job_id or 'DRY RUN'}` |\n"
            f"| Job Definition | `{cfg['merge_job_def']}` |\n"
        ),
        description="Ripple merge/summary phase AWS Batch job details",
    )

    if not dry_run and merge_job_id:
        logger.info("Ripple merge job submitted: %s", merge_job_id)
        await async_poll_until_complete(batch_client, merge_job_id, merge_job_name, poll_interval)

    return {"phase": "merge", "job_id": merge_job_id, "job_name": merge_job_name}


@task(name="Ripple Phase 4 — COG conversion")
async def ripple_submit_and_poll_cog(
    cfg: dict,
    timestamp: str,
    poll_interval: int = 30,
    dry_run: bool = False,
    limit: int | None = None,
) -> dict:
    """Convert VRT-as-.tif outputs to real COGs, in place. Standalone — run once after a raster run finishes."""
    logger = get_run_logger()
    batch_client = get_aws_client(cfg["aws_profile"], cfg["aws_region"], "batch")
    s3_client = get_aws_client(cfg["s3_profile"], cfg["aws_region"], "s3")

    if dry_run:
        total_items = 100
        logger.info("[DRY RUN] Assuming %s libraries for array size calculation", total_items)
    else:
        total_items = read_manifest_total(s3_client, cfg["s3_bucket"], cfg["manifest_s3_key"])
        logger.info("Total libraries (dir_names) in manifest: %s", total_items)
        if total_items == 0:
            logger.info("No dir_names in manifest — nothing to convert.")
            return {"phase": "cog", "job_id": None, "job_name": "SKIPPED (empty manifest)"}

    if limit is not None and limit < total_items:
        logger.warning("--limit %d set — this is a TEST BATCH, not a full conversion.", limit)
        total_items = limit

    items_per_job = int(cfg["items_per_job"])
    if total_items <= items_per_job:
        array_size = None
        array_desc = f"single job ({total_items} librar(y/ies) <= {items_per_job}/job)"
    else:
        array_size = math.ceil(total_items / items_per_job)
        array_size = min(array_size, MAX_ARRAY_SIZE)
        actual_chunk = math.ceil(total_items / array_size)
        array_desc = f"array job — {array_size} children x ~{actual_chunk} librar(y/ies) each"
        logger.info("Array size: %s children x ~%s librar(y/ies) each", array_size, actual_chunk)

    cog_params = {
        "bucket_name": cfg["s3_bucket"],
        "manifest_s3_key": cfg["manifest_s3_key"],
        "items_per_job": str(items_per_job),
    }

    cog_job_name = f"ripple-cog-{timestamp}"
    cog_job_id = submit_job(
        batch_client,
        job_name=cog_job_name,
        job_definition=cfg["cog_job_def"],
        job_queue=cfg["job_queue_name"],
        parameters=cog_params,
        array_size=array_size,
        dry_run=dry_run,
    )

    await create_markdown_artifact(
        key="ripple-cog-job-details",
        markdown=(
            "## Ripple Phase 4 — COG Conversion Job\n\n"
            f"| Field | Value |\n|---|---|\n"
            f"| Job Name | `{cog_job_name}` |\n"
            f"| Job ID | `{cog_job_id or 'DRY RUN'}` |\n"
            f"| Job Definition | `{cfg['cog_job_def']}` |\n"
            f"| Total Libraries | `{total_items}` |\n"
            f"| Array Mode | `{array_desc}` |\n"
        ),
        description="Ripple COG conversion job details",
    )

    if not dry_run and cog_job_id:
        array_str = f" (array size={array_size})" if array_size else " (single job)"
        logger.info("Ripple COG job submitted: %s%s", cog_job_id, array_str)
        await async_poll_until_complete(batch_client, cog_job_id, cog_job_name, poll_interval)

    return {"phase": "cog", "job_id": cog_job_id, "job_name": cog_job_name}


# ---------------------------------------------------------------------------
# Flow — sync
# ---------------------------------------------------------------------------

@flow(
    name="benchmarkcat-batch-pipeline",
    description="3-phase AWS Batch pipeline (Split → Workers → Merge)",
    log_prints=True,
)
def run_pipeline_flow(**kwargs) -> dict:
    """Run the benchmarkcat 3-phase Batch pipeline as a Prefect flow."""
    args = argparse.Namespace(**kwargs)
    logger = get_run_logger()

    tf_outputs = get_terraform_outputs()
    if not tf_outputs:
        raise RuntimeError(
            "Terraform outputs not available. Run from repo root with terraform initialized:\n"
            "  cd terraform && terraform init && terraform apply"
        )

    cfg = build_config(args, tf_outputs)

    required_keys = REQUIRED_CFG_KEYS_RIPPLE if args.pipeline == "ripple" else REQUIRED_CFG_KEYS
    missing = [k for k in required_keys if cfg.get(k) is None]
    if missing:
        raise RuntimeError(
            f"Missing required config values: {', '.join(missing)}. "
            "Check terraform outputs or provide CLI overrides."
        )

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    logger.info(
        "Benchmarkcat Batch Pipeline — %s (dry_run=%s)", args.pipeline.upper(), args.dry_run
    )
    logger.info(
        "Queue: %s  Bucket: %s  Manifest: %s",
        cfg["job_queue_name"], cfg["s3_bucket"], cfg["manifest_s3_key"],
    )

    # -----------------------------------------------------------------
    # Ripple: separate, smaller DAG (split -> array workers -> summary).
    # -----------------------------------------------------------------
    if args.pipeline == "ripple":
        if args.cog_only:
            cog_future = ripple_submit_and_poll_cog.submit(
                cfg=cfg, timestamp=timestamp, poll_interval=args.poll_interval, dry_run=args.dry_run,
                limit=args.limit,
            )
            cog_result = cog_future.result()
            create_table_artifact(
                key="pipeline-summary",
                table=[{"Phase": "COG", "Job Name": str(cog_result["job_name"]), "Job ID": str(cog_result["job_id"] or "DRY RUN")}],
                description="Pipeline 'ripple' — COG-only run",
            )
            return {"pipeline": "ripple", "dry_run": args.dry_run, "cog_job_id": cog_result["job_id"]}

        if args.skip_split:
            logger.info("Skipping Phase 1 (split) — using existing manifest on S3")
            split_result = {"phase": "split", "job_id": None, "job_name": "SKIPPED"}
            workers_future = ripple_submit_and_poll_workers.submit(
                cfg=cfg, timestamp=timestamp, poll_interval=args.poll_interval, dry_run=args.dry_run,
            )
        else:
            split_future = ripple_submit_and_poll_split.submit(
                cfg=cfg, timestamp=timestamp, poll_interval=args.poll_interval, dry_run=args.dry_run,
                limit=args.limit,
            )
            workers_future = ripple_submit_and_poll_workers.submit(
                cfg=cfg, timestamp=timestamp, poll_interval=args.poll_interval, dry_run=args.dry_run,
                wait_for=[split_future],
            )
            split_result = split_future.result()

        merge_future = ripple_submit_and_poll_merge.submit(
            cfg=cfg, timestamp=timestamp, poll_interval=args.poll_interval, dry_run=args.dry_run,
            wait_for=[workers_future],
        )

        workers_result = workers_future.result()
        merge_result = merge_future.result()

        # COG conversion is standalone — not part of the main split/worker/merge
        # DAG, run only when explicitly requested (e.g. once after a raster run
        # is done, not on every test batch).
        cog_result = {"phase": "cog", "job_id": None, "job_name": "SKIPPED"}
        if args.run_cog_conversion:
            cog_future = ripple_submit_and_poll_cog.submit(
                cfg=cfg, timestamp=timestamp, poll_interval=args.poll_interval, dry_run=args.dry_run,
                limit=args.limit, wait_for=[merge_future],
            )
            cog_result = cog_future.result()

        table = [
            {"Phase": "Split", "Job Name": str(split_result["job_name"]), "Job ID": str(split_result["job_id"] or "DRY RUN")},
            {"Phase": "Workers", "Job Name": str(workers_result["job_name"]), "Job ID": str(workers_result["job_id"] or "DRY RUN")},
            {"Phase": "Merge", "Job Name": str(merge_result["job_name"]), "Job ID": str(merge_result["job_id"] or "DRY RUN")},
        ]
        if args.run_cog_conversion:
            table.append({"Phase": "COG", "Job Name": str(cog_result["job_name"]), "Job ID": str(cog_result["job_id"] or "DRY RUN")})

        create_table_artifact(
            key="pipeline-summary",
            table=table,
            description="Pipeline 'ripple' — all phase job IDs",
        )

        return {
            "pipeline": "ripple",
            "dry_run": args.dry_run,
            "split_job_id": split_result["job_id"],
            "worker_job_id": workers_result["job_id"],
            "merge_job_id": merge_result["job_id"],
            "cog_job_id": cog_result["job_id"],
        }

    # -----------------------------------------------------------------
    # Task DAG (Split → Workers → Merge)
    # -----------------------------------------------------------------
    if args.skip_split:
        logger.info("Skipping Phase 1 (split) — using existing manifest on S3")
        split_result = {"phase": "split", "job_id": None, "job_name": "SKIPPED"}

        workers_future = submit_and_poll_workers.submit(
            cfg=cfg,
            pipeline=args.pipeline,
            timestamp=timestamp,
            poll_interval=args.poll_interval,
            dry_run=args.dry_run,
        )
    else:
        split_future = submit_and_poll_split.submit(
            cfg=cfg,
            pipeline=args.pipeline,
            timestamp=timestamp,
            poll_interval=args.poll_interval,
            dry_run=args.dry_run,
            after_date=args.after_date,
            before_date=args.before_date,
            dates=args.dates,
        )

        workers_future = submit_and_poll_workers.submit(
            cfg=cfg,
            pipeline=args.pipeline,
            timestamp=timestamp,
            poll_interval=args.poll_interval,
            dry_run=args.dry_run,
            wait_for=[split_future],
        )

        split_result = split_future.result()

    merge_future = submit_and_poll_merge.submit(
        cfg=cfg,
        pipeline=args.pipeline,
        timestamp=timestamp,
        poll_interval=args.poll_interval,
        dry_run=args.dry_run,
        keep_partials=args.keep_partials,
        wait_for=[workers_future],
    )

    # Collect results
    workers_result = workers_future.result()
    merge_result = merge_future.result()

    # Flow-level artifact: summary table
    create_table_artifact(
        key="pipeline-summary",
        table=[
            {
                "Phase": "Split",
                "Job Name": str(split_result["job_name"]),
                "Job ID": str(split_result["job_id"] or "DRY RUN"),
            },
            {
                "Phase": "Workers",
                "Job Name": str(workers_result["job_name"]),
                "Job ID": str(workers_result["job_id"] or "DRY RUN"),
            },
            {
                "Phase": "Merge",
                "Job Name": str(merge_result["job_name"]),
                "Job ID": str(merge_result["job_id"] or "DRY RUN"),
            },
        ],
        description=f"Pipeline '{args.pipeline}' — all three phase job IDs",
    )

    summary = {
        "pipeline": args.pipeline,
        "dry_run": args.dry_run,
        "split_job_id": split_result["job_id"],
        "worker_job_id": workers_result["job_id"],
        "merge_job_id": merge_result["job_id"]
    }

    if not args.dry_run:
        logger.info(
            "Pipeline '%s' complete. Split: %s  Workers: %s  Merge: %s",
            args.pipeline,
            summary["split_job_id"],
            summary["worker_job_id"],
            summary["merge_job_id"],
        )
    else:
        logger.info("DRY RUN complete — no jobs were submitted")

    return summary


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description=("Run benchmarkcat 3-phase Batch pipeline (Prefect)")
    )
    parser.add_argument(
        "--pipeline",
        required=True,
        choices=["gfm", "gfm_exp", "ripple"],
        help="Which pipeline to run",
    )
    parser.add_argument("--bucket-name", default=None, help="Override S3 bucket")
    parser.add_argument(
        "--scenes-per-job",
        type=int,
        default=None,
        help="Override scenes per worker (default: terraform output or 50). GFM/GFM_EXP only.",
    )
    parser.add_argument(
        "--items-per-job",
        type=int,
        default=None,
        help="Override libraries (dir_names) per ripple worker — each processes all 6 flow intervals per library (default: terraform output or 5). Ripple only.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process the first N outstanding dir_names — for a small initial test batch. Ripple only, omit for a real run.",
    )
    parser.add_argument(
        "--run-cog-conversion",
        action="store_true",
        help="After merge, run Phase 4 to convert VRT-as-.tif outputs to real COGs. Ripple only, standalone step.",
    )
    parser.add_argument(
        "--cog-only",
        action="store_true",
        help="Run only Phase 4 (COG conversion) against the existing manifest — skips split/workers/merge entirely. Ripple only.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Override parallel workers per job (default: terraform output or 1)",
    )
    parser.add_argument("--after-date", default=None, help="Only scenes >= YYYY-MM-DD")
    parser.add_argument("--before-date", default=None, help="Only scenes <= YYYY-MM-DD")
    parser.add_argument("--dates", default=None, help="Comma-separated specific dates (YYYY-MM-DD)")
    parser.add_argument("--profile", default=None, help="AWS profile for Batch operations")
    parser.add_argument(
        "--s3-profile",
        default=None,
        help="AWS profile for S3 access (defaults to --profile if not set)",
    )
    parser.add_argument("--region", default=None, help="AWS region")
    parser.add_argument(
        "--project-name",
        default=None,
        help="Project name for job definition naming (default: from terraform)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be submitted without actually submitting",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=30,
        help="Seconds between status polls (default: 30)",
    )
    parser.add_argument(
        "--keep-partials",
        action="store_true",
        help="Merge job: keep partial parquets after merging (for debugging)",
    )
    parser.add_argument(
        "--skip-split",
        action="store_true",
        help="Skip Phase 1 (split) and use the existing manifest on S3. "
             "Useful when retrying with a custom/filtered manifest.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    run_pipeline_flow(**vars(parse_args()))

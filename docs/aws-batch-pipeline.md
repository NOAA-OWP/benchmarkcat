# AWS Batch Pipeline

Scale GFM, GFM Expanded, and ripple (flows2fim benchmark extent generation) ingestion using a 3-phase AWS Batch pipeline. Infrastructure is managed with Terraform; the Python orchestrator handles job submission and polling. GFM/GFM_EXP process tens of thousands of scenes per job array; ripple processes one library (`dir_name`) per array child instead of one scene. See the top-level [`README.md`](../README.md)'s Batch pipeline section for a short overview of ripple's pattern.

**Key files**:


| File                                 | Purpose                                                                                                                                                                                                                                                                  |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `terraform/`                         | Infrastructure as code (ECR, compute env, queue, 6 job definitions for GFM/GFM_EXP; ripple adds a 7th ECR repo and 4 more job definitions)                                                                                                                              |
| `terraform/terraform.tfvars`         | All configurable values — gitignored; copy from `.tfvars.example`                                                                                                                                                                                                        |
| `terraform/terraform.tfvars.example` | Template with placeholder values for new setups                                                                                                                                                                                                                          |
| `scripts/build_and_push.sh`          | Build Docker image and push to ECR; reads `aws_account_id`, `aws_region`, `aws_profile` from terraform outputs (falls back to env vars). Pass `ripple` as an argument to build `Dockerfile.ripple` instead of the default `Dockerfile`                                 |
| `scripts/run_pipeline_prefect.py`    | Prefect orchestrator — submits split → workers → merge with async polling, retries, and UI observability                                                                                                                                                                 |
| `scripts/batch-entrypoint.sh`        | Cloud-agnostic entrypoint; injects `--job-index` from `AWS_BATCH_JOB_ARRAY_INDEX`, `AZ_BATCH_TASK_ID`, or `BATCH_TASK_INDEX`; converts optional env vars `AFTER_DATE`, `BEFORE_DATE`, `DATES` into `--after-date`, `--before-date`, `--dates` for the split job when set |
| `Dockerfile.ripple`                  | GDAL + `flows2fim` image for the ripple worker, separate from the main `Dockerfile` used by GFM/GFM_EXP                                                                                                                                                                  |
| `ingest/ripple/worker_config.yaml`   | Ripple's S3 paths, naming templates, flow intervals, and flows2fim version, baked into the image and loaded at runtime                                                                                                                                                        |


---

## Overview

```mermaid
flowchart LR
  subgraph S3
    D[Source data\nGFM / GFM-exp]
    M[Manifest\ngfm_manifest.jsonl]
    P[Partial parquets\nper worker]
    OUT[Master parquet +\ncollection.json]
  end

  subgraph Phase1["Phase 1 — Split"]
    SP[batch_split\ndiscovers scenes]
  end

  subgraph Phase2["Phase 2 — Workers (array job)"]
    W0[Child 0\nscenes 1–50]
    W1[Child 1\nscenes 51–100]
    WN[Child N-1\nscenes ...]
  end

  subgraph Phase3["Phase 3 — Merge"]
    MG[batch_merge\nconcatenates parquets]
  end

  D --> SP --> M --> Phase2
  Phase2 --> P --> MG --> OUT
```

**Split** discovers all scenes (or a filtered subset by date) and writes a manifest to S3. **Workers** run as an array job — each child reads its slice of the manifest, processes scenes in parallel, and writes a partial parquet. **Merge** concatenates all partial parquets into the master parquet and rebuilds `collection.json`.

Ripple follows the same split → workers → merge shape, but a unit of work is one ripple library (`dir_name`) rather than one scene, and each worker processes all 6 flow intervals for its library sequentially. Ripple's merge reconciles markers and reports pass/fail counts; it doesn't concatenate parquets or produce STAC output. Ripple's worker writes extent rasters directly to S3 as it runs, and STAC cataloging is a separate step (`ripple_col.py`, not part of this Batch pipeline).

---

## Prerequisites

- AWS account with permissions for Batch, ECR, S3, CloudWatch Logs
- [Terraform](https://developer.hashicorp.com/terraform/install)
- Docker (for building images)
- Python with `boto3` and `prefect` installed (`pip install boto3 prefect`)

**Note:** `run_pipeline_prefect.py` requires Terraform outputs. Run `terraform init` and `terraform apply` before submitting; otherwise the flow exits with an error.

**Ripple only:** if the source data (`s3://fimc-data`) lives in a different AWS account than the Batch infrastructure, add both as named profiles in `~/.aws/credentials` and pass `--profile`/`--s3-profile` separately.

---

## Quick Start

### 1. Configure

Copy the example and fill in your values (`terraform.tfvars` is gitignored):

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
```

Edit `terraform/terraform.tfvars` with your AWS account, IAM roles, networking, and S3 paths.

### 2. Deploy Infrastructure

```bash
terraform init
terraform plan
terraform apply
```

This creates: ECR repository, CloudWatch log group, Batch compute environment, job queue, and 6 job definitions (split/worker/merge for GFM and GFM Expanded). S3 paths are passed as `Ref::` parameters at submit time — not baked into the image.

Ripple adds a separate ECR repository (it needs GDAL + `flows2fim`, not the main image) and 4 more job definitions (split/worker/merge + COG conversion). GFM/GFM_EXP and ripple share the same compute environment and job queue, so running pipelines concurrently means they compete for the same `max_vcpus` ceiling.

### 3. Build and Push Docker Image

```bash
cd ..
chmod +x ./scripts/build_and_push.sh
./scripts/build_and_push.sh
```

The script reads `aws_account_id`, `aws_region`, and `aws_profile` from terraform outputs first, then falls back to env vars (`AWS_ACCOUNT_ID`, `AWS_REGION`, `AWS_PROFILE`). If neither terraform nor env vars are set, it fails with a clear error.

Only needed when code changes (`ingest/`, `Dockerfile`, `scripts/batch-entrypoint.sh`). Changing S3 paths in `terraform.tfvars` does **not** require a rebuild.

> **Note:** This builds and pushes the **job container** (`Dockerfile`) only. The orchestration container (`Dockerfile.orchestration`) runs locally and does not need to be pushed to ECR. Build it with:
>
> ```bash
> docker build -f Dockerfile.orchestration -t benchmarkcat:orchestration .
> ```

For ripple, build and push `Dockerfile.ripple` instead by passing `ripple`:

```bash
FLOWS2FIM_VERSION=0.5.0 ./scripts/build_and_push.sh ripple
```

`FLOWS2FIM_VERSION` pins the flows2fim binary pulled from GHCR. Tag format is `0.5.0`, no `v` prefix even though the binary's own `--version` output prints one. `worker_config.yaml` is baked into this image too, so it isn't managed by Terraform. `terraform plan` won't catch a config change here. Editing the YAML has no effect until you rebuild and push; jobs keep running whatever was baked in at the last build, silently, with no error. Always rebuild and push right after editing this file.

### 4. Submit the Pipeline

**Optional:** start the Prefect UI first (in a separate terminal) to get live task-level observability:

```bash
prefect server start
# UI available at http://127.0.0.1:4200
```

The flow runs fine without a server — Prefect just won't record state remotely.

```bash
# GFM — full run
python scripts/run_pipeline_prefect.py \
  --pipeline gfm \
  --profile <your-aws-profile> \
  --s3-profile <your-s3-profile> \
  --scenes-per-job 10 \
  --poll-interval 45

# GFM Expanded — with date filter
python scripts/run_pipeline_prefect.py \
  --pipeline gfm_exp \
  --after-date 2024-01-01 \
  --before-date 2024-06-30 \
  --profile <your-aws-profile> \
  --s3-profile <your-s3-profile>

# Dry run first (no jobs submitted)
python scripts/run_pipeline_prefect.py --pipeline gfm --dry-run
```

The orchestrator runs a Prefect flow with an explicit Split → Workers → Merge task DAG. Each phase submits a Batch job, polls asynchronously until complete, and creates a Prefect artifact with job details. If a task fails, Prefect retries it before marking the flow as failed.

Ripple uses the same orchestrator with `--pipeline ripple`:

```bash
# Ripple — small test batch first (recommended)
python scripts/run_pipeline_prefect.py \
  --pipeline ripple \
  --limit 5 \
  --items-per-job 1 \
  --profile <your-aws-profile> \
  --s3-profile <your-s3-profile>

# Ripple — full run
python scripts/run_pipeline_prefect.py \
  --pipeline ripple \
  --items-per-job 1 \
  --profile <your-aws-profile> \
  --s3-profile <your-s3-profile>
```

`--items-per-job 1` means one library per array child, the strongest failure isolation, since a bad library can't affect any other. Higher values reduce per-job container startup overhead at the cost of coarser isolation. `--limit N` truncates the manifest after split's skip-decision is made, so a small test run exercises the same skip logic as a full run, just fewer libraries. Split is idempotent: it re-derives outstanding work from S3 on every run, no separate state file, so rerunning after a partial delivery only picks up what's new.

### 5. Monitor

The orchestrator logs status every `--poll-interval` seconds (default 30). For array jobs it shows per-status counts (`RUNNING=12 SUCCEEDED=38 FAILED=0`). If Prefect server is running, the Prefect UI at `http://127.0.0.1:4200` shows the flow run with per-task artifacts and the pipeline summary table.

CloudWatch logs:

```bash
# Stream all benchmarkcat logs
aws logs tail /aws/batch/benchmarkcat --follow --profile <your-aws-profile>

# Filter to a specific phase
aws logs tail /aws/batch/benchmarkcat --follow --profile <your-aws-profile> --filter-pattern "gfm-worker"
aws logs tail /aws/batch/benchmarkcat --follow --profile <your-aws-profile> --filter-pattern "ripple-split"
```

Ripple's split job logs a one-line summary each run (`X of Y dir_names have all 6 intervals complete; Z need processing`). `X` should only grow across runs.

### 6. Cleanup

```bash
terraform destroy
```

Removes ECR repository, log group, compute environment, job queue, and all job definitions. Does **not** delete S3 data or IAM roles. For ripple this also removes its separate ECR repository and 4 job definitions.

> **Note:** If `ecr_force_delete = true`, `terraform destroy` will also delete all Docker images from ECR. After re-running `terraform apply`, you will need to rebuild and push the image: `./scripts/build_and_push.sh` (and `./scripts/build_and_push.sh ripple` if using ripple).

---

## Production Runs

Phase 2 can run for hours. For production, run the orchestrator under `tmux` on your EC2 workspace so it survives SSH disconnects:

```bash
tmux new -s benchmarkcat

python scripts/run_pipeline_prefect.py \
  --pipeline gfm \
  --scenes-per-job 50 \
  --profile <your-aws-profile> \
  --s3-profile <your-s3-profile> \
  --poll-interval 60
```

If the orchestrator itself dies mid-run (SSH drop without tmux, credential expiry, laptop sleep), Batch jobs already dispatched keep running independently. They don't depend on the orchestrator staying alive. Rerunning the same command afterward picks up wherever split left off.

---

## `run_pipeline_prefect.py` Reference

```
python scripts/run_pipeline_prefect.py [options]
```


| Flag               | Default             | Description                                         |
| ------------------ | ------------------- | --------------------------------------------------- |
| `--pipeline`       | *(required)*        | `gfm`, `gfm_exp`, or `ripple`                        |
| `--bucket-name`    | from terraform      | Override S3 bucket                                  |
| `--scenes-per-job` | from terraform      | Scenes per array child (controls array size, GFM/GFM_EXP only) |
| `--items-per-job`  | from terraform      | Libraries per array child (controls array size, ripple only) |
| `--limit`          | —                   | Cap the manifest to the first N outstanding libraries (ripple only) |
| `--workers`        | from terraform      | Parallel workers per job (passed to worker phase)   |
| `--after-date`     | —                   | Only include scenes ≥ YYYY-MM-DD (split phase only) |
| `--before-date`    | —                   | Only include scenes ≤ YYYY-MM-DD (split phase only) |
| `--dates`          | —                   | Comma-separated specific dates (split phase only)   |
| `--profile`        | from terraform      | AWS profile for Batch API calls                     |
| `--s3-profile`     | same as `--profile` | AWS profile for S3 access (if different from Batch) |
| `--region`         | from terraform      | AWS region                                          |
| `--project-name`   | from terraform      | Project name for job definition naming              |
| `--poll-interval`  | `30`                | Seconds between status polls                        |
| `--dry-run`        | false               | Print what would be submitted without submitting    |
| `--skip-split`     | false               | Skip Phase 1; use existing manifest on S3           |
| `--keep-partials`  | false               | Merge: keep partial parquets after merging          |
| `--cog-only`        | false               | Run only ripple's COG-conversion phase, skipping split/workers/merge (ripple only) |
| `--run-cog-conversion` | false             | Chain ripple's COG-conversion phase after merge (ripple only) |


Date filters apply **only to Phase 1 (split)**. They are passed to the split job via container environment (not Batch parameters), so they can be omitted when not needed; the entrypoint converts them to CLI args when set. Workers process their manifest slice as-is; they do not re-apply date filters. A sidecar `<manifest>.meta.json` is written with `total_scenes` and any active filters for auditing.

**Single source of truth:** Terraform is the source of truth for infrastructure and config. CLI overrides apply to: `--bucket-name`, `--scenes-per-job`, `--items-per-job`, `--limit`, `--workers`, `--profile`, `--region`, `--project-name`, and date filters. S3 paths (`catalog_path`, `manifest_s3_key`, etc.) come only from Terraform. Change them in `terraform.tfvars` and run `terraform apply`.

---

## Configuration Reference

All variables are declared in `terraform/variables.tf`. Required variables (no defaults) must be set in `terraform/terraform.tfvars`. Copy `terraform/terraform.tfvars.example` as a starting point.

### Compute


| Variable             | Default                                                  | Description                                                           |
| -------------------- | -------------------------------------------------------- | --------------------------------------------------------------------- |
| `instance_types`     | `["m5.xlarge", "m5.2xlarge", "r5.xlarge", "r5.2xlarge"]` | CPU instance type(s); Batch picks the best available                  |
| `max_vcpus`          | `256`                                                    | Max vCPUs across all running instances (shared by all pipelines)      |
| `use_spot`           | `false`                                                  | Use Spot instances (cheaper; risk of interruption)                    |
| `ecr_force_delete`   | `false`                                                  | Allow force delete of ECR on `terraform destroy` — set `true` for dev |
| `image_tag`          | `latest`                                                 | Docker image tag; pin to a SHA or semver for production               |
| `log_retention_days` | `365`                                                    | CloudWatch log retention in days                                      |
| `ripple_worker_vcpus`   | `2`      | vCPUs per ripple worker job                                                   |
| `ripple_worker_memory`  | `4096`   | Memory (MB) per ripple worker job, well above the ~215MB observed peak       |
| `ripple_worker_timeout` | `43200`  | Timeout (seconds) per ripple worker job, sized for `items_per_job × 6` sequential `flows2fim` runs |
| `ripple_cog_vcpus`      | `2`      | vCPUs per ripple COG-conversion job                                           |
| `ripple_cog_memory`     | `2048`   | Memory (MB) per ripple COG-conversion job; outputs are all under 1.2MB       |
| `ripple_cog_timeout`    | `14400`  | Timeout (seconds) per ripple COG-conversion job; dominated by many small S3 reads, not compute |


### S3 / Pipeline Paths

All path variables are **required** — no defaults. Set them in `terraform/terraform.tfvars`.


| Variable                        | Description                                                               |
| ------------------------------- | ------------------------------------------------------------------------- |
| `s3_bucket`                     | S3 bucket for all I/O                                                     |
| `scenes_per_job`                | Default scenes per worker; controls array size (default: `50`)            |
| `workers`                       | Default parallel workers per job (default: `1`)                           |
| `catalog_path`                  | Root catalog prefix                                                       |
| `hucs_object_key`               | S3 key for HUC8 boundaries GeoPackage                                     |
| `boundaries_object_key`         | S3 key for country boundaries GeoPackage                                  |
| `gfm_asset_object_key`          | GFM source data prefix                                                    |
| `gfm_manifest_s3_key`           | GFM manifest JSONL key                                                    |
| `gfm_partial_parquet_prefix`    | GFM partial parquets prefix                                               |
| `gfm_derived_metadata_path`     | GFM master parquet key                                                    |
| `gfm_dfo_geopackage_object_key` | S3 key for DFO USA events GeoPackage (GFM worker; required)               |
| `gfm_readme_object_key`         | Full S3 key for GFM data readme PDF (required; shared by GFM and GFM_EXP) |
| `gfm_exp`_*                     | GFM Expanded equivalents (same pattern)                                   |
| `ripple_asset_object_key`       | S3 prefix for ripple library directories                                  |
| `ripple_manifest_s3_key`        | Ripple manifest JSONL key                                                 |
| `ripple_success_markers_prefix` | S3 prefix for ripple success markers                                      |
| `ripple_error_retry_prefix`     | S3 prefix for ripple retry-eligible error markers                         |
| `ripple_error_nonretry_prefix`  | S3 prefix for ripple non-retry error markers                              |
| `ripple_items_per_job`          | Default libraries per worker; controls array size (default: `1`)          |


---

## Troubleshooting

**Terraform outputs not available**: The flow exits with this error if Terraform is not initialized or applied. Run `cd terraform && terraform init && terraform apply` from the repo root, then run `run_pipeline_prefect.py` again.

**AccessDenied on S3 (orchestrator)**: The orchestrator reads the manifest metadata from S3 locally to compute array size. Use `--s3-profile <profile>` if your Batch profile lacks S3 permissions. See "Prerequisites" above for the two-account setup ripple may need.

**Phase 2 never starts / array size wrong**: The orchestrator reads `<manifest_s3_key>.meta.json` (written by the split job) to determine total scene count. If the split succeeded but Phase 2 fails to launch, inspect the sidecar:

```bash
aws s3 cp s3://<bucket>/<manifest_s3_key>.meta.json - | python3 -m json.tool
```

It contains `total_scenes`, `created_at`, and any active date filters. If `total_scenes = 0`, the split found no matching scenes — check your date filters and S3 prefix.

**Worker OOM (exit code 137)**: Increase `worker_memory` (or `ripple_worker_memory` for ripple) in `terraform.tfvars` and run `terraform apply`. Default is 16 GB; try 32768 for very large scenes. Ripple's default is 4 GB, already well above observed usage, so an OOM there is worth investigating rather than just raising the number.

**Restarting after a crash (before merge)**: Workers automatically detect scenes already processed by loading the master parquet and any existing partial parquets at startup. Each scene is checked against the tracking parquet *and* verified via S3 `HeadObject` on its item JSON; only scenes passing both checks are skipped. Scenes from workers that crashed mid-processing are safely reprocessed. Use `--keep-partials` in the merge phase to preserve partials for post-mortem debugging. Ripple has no separate resume step. Its skip logic lives entirely in split, which re-checks S3 markers fresh on every run, so rerunning the whole pipeline after a crash is the correct move.

# -----------------------------------------------------------------------------
# Benchmarkcat — AWS Batch Infrastructure
#
# Manages: ECR repo, Batch compute environment (SPOT), job queue, 6 job definitions
# (gfm-split, gfm-worker, gfm-merge, gfm-exp-split, gfm-exp-worker, gfm-exp-merge).
# IAM roles are NOT managed here — they are referenced by ARN.
# -----------------------------------------------------------------------------

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}

# -----------------------------------------------------------------------------
# ECR Repositories
# -----------------------------------------------------------------------------
resource "aws_ecr_repository" "app" {
  name                 = var.project_name
  image_tag_mutability = "MUTABLE"
  force_delete         = var.ecr_force_delete

  image_scanning_configuration {
    scan_on_push = false
  }

  tags = local.tags
}

# Separate repo for the ripple worker image (Dockerfile.ripple, needs GDAL + flows2fim).
# TODO: doubles up if project_name contains "ripple" (e.g. "benchmarkcat-ripple-ripple"). Harmless, just ugly.
resource "aws_ecr_repository" "ripple" {
  name                 = "${var.project_name}-ripple"
  image_tag_mutability = "MUTABLE"
  force_delete         = var.ecr_force_delete

  image_scanning_configuration {
    scan_on_push = false
  }

  tags = local.tags
}

# -----------------------------------------------------------------------------
# Batch Compute Environment (SPOT or On-Demand, CPU-only)
# -----------------------------------------------------------------------------
resource "aws_batch_compute_environment" "cpu" {
  compute_environment_name = "${var.project_name}-cpu-${var.use_spot ? "spot" : "ec2"}"
  type                     = "MANAGED"
  state                    = "ENABLED"
  service_role             = var.batch_service_role_arn

  compute_resources {
    type                = var.use_spot ? "SPOT" : "EC2"
    allocation_strategy = var.use_spot ? "SPOT_CAPACITY_OPTIMIZED" : "BEST_FIT"
    min_vcpus           = 0
    max_vcpus           = var.max_vcpus
    desired_vcpus       = 0
    instance_type       = var.instance_types

    subnets              = var.subnets
    security_group_ids   = var.security_group_ids
    instance_role        = var.batch_instance_profile
    spot_iam_fleet_role  = var.use_spot ? var.spot_fleet_role_arn : null
  }

  tags = local.tags

  lifecycle {
    create_before_destroy = true
    # Batch scales desired_vcpus at runtime; without this, applying while jobs are running scales it back to 0.
    ignore_changes = [compute_resources[0].desired_vcpus]
  }
}

# -----------------------------------------------------------------------------
# Batch Job Queue
# -----------------------------------------------------------------------------
resource "aws_batch_job_queue" "pipeline" {
  name     = "${var.project_name}-queue"
  state    = "ENABLED"
  priority = 1

  compute_environment_order {
    order               = 1
    compute_environment = aws_batch_compute_environment.cpu.arn
  }

  tags = local.tags
}

# -----------------------------------------------------------------------------
# CloudWatch Log Group
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "batch" {
  name              = "/aws/batch/${var.project_name}"
  retention_in_days = var.log_retention_days
  tags              = local.tags
}

# -----------------------------------------------------------------------------
# Job Definitions — 6 total (split/worker/merge for GFM and GFM_EXP)
# -----------------------------------------------------------------------------
locals {
  tags = {
    Project = var.project_name
  }

  job_definitions = {
    "gfm-split" = {
      vcpus   = var.split_vcpus
      memory  = var.split_memory
      timeout = var.split_timeout
      command = [
        "ingest.gfm.batch_split",
        "--bucket_name", "Ref::bucket_name",
        "--asset_object_key", "Ref::asset_object_key",
        "--manifest-s3-key", "Ref::manifest_s3_key",
      ]
    }
    "gfm-worker" = {
      vcpus   = var.worker_vcpus
      memory  = var.worker_memory
      timeout = var.worker_timeout
      command = [
        "ingest.gfm.gfm_col",
        "--mode", "batch-worker",
        "--bucket_name", "Ref::bucket_name",
        "--catalog_path", "Ref::catalog_path",
        "--asset_object_key", "Ref::asset_object_key",
        "--hucs_object_key", "Ref::hucs_object_key",
        "--boundaries_object_key", "Ref::boundaries_object_key",
        "--derived_metadata_path", "Ref::derived_metadata_path",
        "--manifest-s3-key", "Ref::manifest_s3_key",
        "--partial-parquet-prefix", "Ref::partial_parquet_prefix",
        "--scenes-per-job", "Ref::scenes_per_job",
        "--workers", "Ref::workers",
        "--dfo-geopackage-object-key", "Ref::dfo_geopackage_object_key",
        "--readme-object-key", "Ref::readme_object_key",
      ]
    }
    "gfm-merge" = {
      vcpus   = var.merge_vcpus
      memory  = var.merge_memory
      timeout = var.merge_timeout
      command = concat(
        [
          "ingest.gfm.batch_merge",
          "--bucket_name", "Ref::bucket_name",
          "--partial-parquet-prefix", "Ref::partial_parquet_prefix",
          "--derived_metadata_path", "Ref::derived_metadata_path",
          "--catalog_path", "Ref::catalog_path",
          "--asset_object_key", "Ref::asset_object_key",
          "--readme-object-key", "Ref::readme_object_key",
        ],
        var.merge_keep_partials ? ["--keep-partials"] : []
      )
    }
    "gfm-exp-split" = {
      vcpus   = var.split_vcpus
      memory  = var.split_memory
      timeout = var.split_timeout
      command = [
        "ingest.gfm_exp.batch_split",
        "--bucket_name", "Ref::bucket_name",
        "--asset_object_key", "Ref::asset_object_key",
        "--manifest-s3-key", "Ref::manifest_s3_key",
      ]
    }
    "gfm-exp-worker" = {
      vcpus   = var.worker_vcpus
      memory  = var.worker_memory
      timeout = var.worker_timeout
      command = [
        "ingest.gfm_exp.gfm_exp_col",
        "--mode", "batch-worker",
        "--bucket_name", "Ref::bucket_name",
        "--catalog_path", "Ref::catalog_path",
        "--asset_object_key", "Ref::asset_object_key",
        "--hucs_object_key", "Ref::hucs_object_key",
        "--boundaries_object_key", "Ref::boundaries_object_key",
        "--derived_metadata_path", "Ref::derived_metadata_path",
        "--manifest-s3-key", "Ref::manifest_s3_key",
        "--partial-parquet-prefix", "Ref::partial_parquet_prefix",
        "--scenes-per-job", "Ref::scenes_per_job",
        "--workers", "Ref::workers",
        "--readme-object-key", "Ref::readme_object_key",
      ]
    }
    "gfm-exp-merge" = {
      vcpus   = var.merge_vcpus
      memory  = var.merge_memory
      timeout = var.merge_timeout
      command = concat(
        [
          "ingest.gfm_exp.batch_merge",
          "--bucket_name", "Ref::bucket_name",
          "--partial-parquet-prefix", "Ref::partial_parquet_prefix",
          "--derived_metadata_path", "Ref::derived_metadata_path",
          "--catalog_path", "Ref::catalog_path",
          "--asset_object_key", "Ref::asset_object_key",
          "--readme-object-key", "Ref::readme_object_key",
        ],
        var.merge_keep_partials ? ["--keep-partials"] : []
      )
    }
  }

  # Ripple job definitions use their own image, kept out of local.job_definitions above.
  # TODO: same project_name-contains-"ripple" doubling as aws_ecr_repository.ripple.
  ripple_job_definitions = {
    "ripple-split" = {
      image   = "${aws_ecr_repository.ripple.repository_url}:${var.image_tag}"
      vcpus   = var.split_vcpus
      memory  = var.split_memory
      timeout = var.split_timeout
      command = [
        "ingest.ripple.batch_split",
        "--bucket_name", "Ref::bucket_name",
        "--asset_object_key", "Ref::ripple_asset_object_key",
        "--manifest-s3-key", "Ref::manifest_s3_key",
        "--success-markers-prefix", "Ref::ripple_success_markers_prefix",
        "--error-nonretry-prefix", "Ref::ripple_error_nonretry_prefix",
      ]
    }
    "ripple-worker" = {
      image   = "${aws_ecr_repository.ripple.repository_url}:${var.image_tag}"
      vcpus   = var.ripple_worker_vcpus
      memory  = var.ripple_worker_memory
      timeout = var.ripple_worker_timeout
      command = [
        "ingest.ripple.extent_worker",
        "--bucket_name", "Ref::bucket_name",
        "--manifest-s3-key", "Ref::manifest_s3_key",
        "--items-per-job", "Ref::items_per_job",
      ]
    }
    "ripple-merge" = {
      image   = "${aws_ecr_repository.ripple.repository_url}:${var.image_tag}"
      vcpus   = var.merge_vcpus
      memory  = var.merge_memory
      timeout = var.merge_timeout
      command = [
        "ingest.ripple.batch_merge",
        "--bucket_name", "Ref::bucket_name",
        "--manifest-s3-key", "Ref::manifest_s3_key",
        "--success-markers-prefix", "Ref::ripple_success_markers_prefix",
        "--error-retry-prefix", "Ref::ripple_error_retry_prefix",
        "--error-nonretry-prefix", "Ref::ripple_error_nonretry_prefix",
      ]
    }
    # Converts VRT-as-.tif outputs to real COGs, in place. Each VRT references
    # hundreds of small source tiles over /vsis3/, so this must run in-region
    # on Batch, not locally — see docs/aws-batch-pipeline.md.
    "ripple-cog" = {
      image   = "${aws_ecr_repository.ripple.repository_url}:${var.image_tag}"
      vcpus   = var.ripple_cog_vcpus
      memory  = var.ripple_cog_memory
      timeout = var.ripple_cog_timeout
      command = [
        "ingest.ripple.cog_worker",
        "--bucket_name", "Ref::bucket_name",
        "--manifest-s3-key", "Ref::manifest_s3_key",
        "--items-per-job", "Ref::items_per_job",
      ]
    }
  }
}

resource "aws_batch_job_definition" "jobs" {
  for_each = local.job_definitions

  name            = "${var.project_name}-${each.key}"
  type            = "container"
  propagate_tags  = true
  tags            = local.tags

  timeout {
    attempt_duration_seconds = each.value.timeout
  }

  retry_strategy {
    attempts = var.retry_attempts
  }

  container_properties = jsonencode({
    image      = "${aws_ecr_repository.app.repository_url}:${var.image_tag}"
    jobRoleArn = var.batch_job_role_arn
    command    = each.value.command

    resourceRequirements = [
      { type = "VCPU", value = tostring(each.value.vcpus) },
      { type = "MEMORY", value = tostring(each.value.memory) }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = "/aws/batch/${var.project_name}"
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = each.key
      }
    }
  })
}

# Separate resource because these use the aws_ecr_repository.ripple image, not the shared app image.
resource "aws_batch_job_definition" "ripple_jobs" {
  for_each = local.ripple_job_definitions

  name           = "${var.project_name}-${each.key}"
  type           = "container"
  propagate_tags = true
  tags           = local.tags

  timeout {
    attempt_duration_seconds = each.value.timeout
  }

  retry_strategy {
    attempts = var.retry_attempts
  }

  container_properties = jsonencode({
    image      = each.value.image
    jobRoleArn = var.batch_job_role_arn
    command    = each.value.command

    resourceRequirements = [
      { type = "VCPU", value = tostring(each.value.vcpus) },
      { type = "MEMORY", value = tostring(each.value.memory) }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = "/aws/batch/${var.project_name}"
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = each.key
      }
    }
  })
}

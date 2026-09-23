# -----------------------------------------------------------------------------
# AWS / General
# -----------------------------------------------------------------------------
variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "AWS CLI profile to use (leave unset to use the default credential chain)"
  type        = string
  default     = null
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "benchmarkcat"
}

variable "ecr_force_delete" {
  description = "Allow force delete of ECR repository. Set true for dev, false for production."
  type        = bool
  default     = false
}

variable "aws_account_id" {
  description = "AWS account ID"
  type        = string
}

# -----------------------------------------------------------------------------
# IAM (existing roles — not managed by this Terraform config)
# -----------------------------------------------------------------------------
variable "batch_job_role_arn" {
  description = "IAM role ARN for Batch job containers (S3 access, CloudWatch Logs)"
  type        = string
  validation {
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:role/.+", var.batch_job_role_arn))
    error_message = "batch_job_role_arn must be a valid IAM role ARN (arn:aws:iam::ACCOUNT:role/NAME)."
  }
}

variable "batch_instance_profile" {
  description = "EC2 instance profile ARN for Batch compute instances"
  type        = string
  validation {
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:instance-profile/.+", var.batch_instance_profile))
    error_message = "batch_instance_profile must be a valid instance profile ARN."
  }
}

variable "spot_fleet_role_arn" {
  description = "IAM role ARN for EC2 Spot Fleet requests"
  type        = string
  validation {
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:role/.+", var.spot_fleet_role_arn))
    error_message = "spot_fleet_role_arn must be a valid IAM role ARN (arn:aws:iam::ACCOUNT:role/NAME)."
  }
}

variable "batch_service_role_arn" {
  description = "Service-linked role ARN for AWS Batch"
  type        = string
  validation {
    condition     = can(regex("^arn:aws(:-[a-z]+)*:iam::[0-9]{12}:role/.+", var.batch_service_role_arn))
    error_message = "batch_service_role_arn must be a valid IAM role ARN."
  }
}

# -----------------------------------------------------------------------------
# Networking
# -----------------------------------------------------------------------------
variable "subnets" {
  description = "Subnet IDs for Batch compute instances"
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security group IDs for Batch compute instances"
  type        = list(string)
}

# -----------------------------------------------------------------------------
# Batch Compute
# -----------------------------------------------------------------------------
variable "max_vcpus" {
  description = "Maximum vCPUs for the Batch compute environment"
  type        = number
  default     = 256
}

variable "instance_types" {
  description = "EC2 instance types for Batch compute (CPU-only)"
  type        = list(string)
  default     = ["m5.xlarge", "m5.2xlarge", "r5.xlarge", "r5.2xlarge"]
}

variable "use_spot" {
  description = "Use Spot instances (true) or On-Demand (false)"
  type        = bool
  default     = false
}

# -----------------------------------------------------------------------------
# Job Definition Resources — per phase
# -----------------------------------------------------------------------------
variable "split_vcpus" {
  description = "vCPUs for split job (S3 pagination, lightweight)"
  type        = number
  default     = 2
}

variable "split_memory" {
  description = "Memory (MB) for split job"
  type        = number
  default     = 4096
}

variable "split_timeout" {
  description = "Timeout (seconds) for split job"
  type        = number
  default     = 1800 # 30 min
}

variable "worker_vcpus" {
  description = "vCPUs for worker job (rasterio mosaics are CPU-intensive)"
  type        = number
  default     = 4
}

variable "worker_memory" {
  description = "Memory (MB) for worker job"
  type        = number
  default     = 16384 # 16 GB
}

variable "worker_timeout" {
  description = "Timeout (seconds) for worker job"
  type        = number
  default     = 14400 # 4 hr
}

variable "merge_vcpus" {
  description = "vCPUs for merge job (parquet concat + collection rebuild)"
  type        = number
  default     = 2
}

variable "merge_memory" {
  description = "Memory (MB) for merge job"
  type        = number
  default     = 8192 # 8 GB
}

variable "merge_timeout" {
  description = "Timeout (seconds) for merge job"
  type        = number
  default     = 3600 # 1 hr
}

variable "merge_keep_partials" {
  description = "If true, merge job keeps partial parquets after merging (for debugging)"
  type        = bool
  default     = false
}

variable "retry_attempts" {
  description = "Number of retry attempts for failed jobs (handles Spot interruptions)"
  type        = number
  default     = 2
}

# -----------------------------------------------------------------------------
# S3 / Pipeline Config
# -----------------------------------------------------------------------------
variable "s3_bucket" {
  description = "S3 bucket for all pipeline data"
  type        = string
}

variable "scenes_per_job" {
  description = "Number of scenes each worker array child processes"
  type        = number
  default     = 50
}

variable "workers" {
  description = "Number of parallel workers per scene in worker job (1 = sequential)"
  type        = number
  default     = 1
}

variable "catalog_path" {
  description = "S3 prefix for the STAC catalog"
  type        = string
}

variable "hucs_object_key" {
  description = "S3 key for the HUC8 GeoPackage"
  type        = string
}

variable "boundaries_object_key" {
  description = "S3 key for the Mexico/Canada boundaries GeoPackage"
  type        = string
}

variable "gfm_readme_object_key" {
  description = "S3 key for the GFM data readme PDF (naming conventions). Required. Shared by GFM and GFM_EXP."
  type        = string
}

# GFM pipeline paths
variable "gfm_asset_object_key" {
  description = "S3 prefix for GFM source data (DFO event directories)"
  type        = string
}

variable "gfm_manifest_s3_key" {
  description = "S3 key where the GFM manifest JSONL is written"
  type        = string
}

variable "gfm_partial_parquet_prefix" {
  description = "S3 prefix for GFM per-job partial parquets"
  type        = string
}

variable "gfm_derived_metadata_path" {
  description = "S3 key for the GFM master parquet"
  type        = string
}

variable "gfm_dfo_geopackage_object_key" {
  description = "S3 key for the DFO USA events GeoPackage used by GFM for event geometry and main cause"
  type        = string
}

# GFM_EXP pipeline paths
variable "gfm_exp_asset_object_key" {
  description = "S3 prefix for GFM_EXP source data (PI4 date directories)"
  type        = string
}

variable "gfm_exp_manifest_s3_key" {
  description = "S3 key where the GFM_EXP manifest JSONL is written"
  type        = string
}

variable "gfm_exp_partial_parquet_prefix" {
  description = "S3 prefix for GFM_EXP per-job partial parquets"
  type        = string
}

variable "gfm_exp_derived_metadata_path" {
  description = "S3 key for the GFM_EXP master parquet"
  type        = string
}

# -----------------------------------------------------------------------------
# Ripple pipeline (flows2fim extent generation)
# -----------------------------------------------------------------------------
variable "ripple_asset_object_key" {
  description = "S3 prefix containing ripple library directories (ripple.gpkg, start_reaches.csv, library_extent/)"
  type        = string
  default     = "ripple/v0.11.x/successes/"
}

variable "ripple_manifest_s3_key" {
  description = "S3 key where the ripple (dir_name, flow_file) work-item manifest JSONL is written"
  type        = string
}

variable "ripple_success_markers_prefix" {
  description = "S3 prefix for ripple success markers"
  type        = string
  default     = "benchmark/ripple_v0.11.x/status/success/"
}

variable "ripple_error_retry_prefix" {
  description = "S3 prefix for ripple retry-error markers"
  type        = string
  default     = "benchmark/ripple_v0.11.x/status/errors/retry/"
}

variable "ripple_error_nonretry_prefix" {
  description = "S3 prefix for ripple non-retry-error markers"
  type        = string
  default     = "benchmark/ripple_v0.11.x/status/errors/nonretry/"
}

variable "ripple_items_per_job" {
  description = "Number of (dir_name, flow_file) work items each ripple worker array child processes"
  type        = number
  default     = 5
}

# Sized from the largest library tested (mip_03070103): peak memory ~215MB, low CPU, I/O-bound.
variable "ripple_worker_vcpus" {
  description = "vCPUs for ripple worker job (flows2fim + GDAL, I/O-bound — see sizing note above)"
  type        = number
  default     = 2
}

variable "ripple_worker_memory" {
  description = "Memory (MB) for ripple worker job"
  type        = number
  default     = 4096 # 4 GB — ~18x headroom over the ~215MB observed peak
}

variable "ripple_worker_timeout" {
  description = "Timeout (seconds) for ripple worker job. Each job processes ripple_items_per_job libraries, looping all 6 flow intervals per library internally — size for worst-case (items_per_job x 6) sequential flows2fim runs, not a single interval. output_format is now cog (was vrt), which took ~2x as long per interval in a local timing test (2.5 min vrt vs ~5 min cog on one library/interval) — kept generous headroom here rather than tightening."
  type        = number
  default     = 43200 # 12 hr
}

variable "ripple_cog_vcpus" {
  description = "vCPUs for ripple COG-conversion job (gdal_translate, I/O-bound against many small /vsis3/ reads per file)"
  type        = number
  default     = 2
}

variable "ripple_cog_memory" {
  description = "Memory (MB) for ripple COG-conversion job"
  type        = number
  default     = 2048 # 2 GB — outputs are all <1.2MB, no chunking needed
}

variable "ripple_cog_timeout" {
  description = "Timeout (seconds) for ripple COG-conversion job. Dominated by many small S3 reads per file, not compute — size generously."
  type        = number
  default     = 14400 # 4 hr
}

# -----------------------------------------------------------------------------
# Docker / Observability
# -----------------------------------------------------------------------------
variable "image_tag" {
  description = "Docker image tag for job definitions. Pin to a specific tag in production."
  type        = string
  default     = "latest"
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days for Batch job logs"
  type        = number
  default     = 365
}

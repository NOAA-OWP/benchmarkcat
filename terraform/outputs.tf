output "aws_region" {
  description = "AWS region (consumed by submit_pipeline.py and build_and_push.sh)"
  value       = var.aws_region
}

output "aws_profile" {
  description = "AWS CLI profile (consumed by submit_pipeline.py and build_and_push.sh)"
  value       = var.aws_profile
}

output "project_name" {
  description = "Project name for resource naming (consumed by submit_pipeline.py)"
  value       = var.project_name
}

output "aws_account_id" {
  description = "AWS account ID (consumed by build_and_push.sh)"
  value       = var.aws_account_id
}

output "ecr_repository_url" {
  description = "ECR repository URL for docker push"
  value       = aws_ecr_repository.app.repository_url
}

output "job_queue_name" {
  description = "Batch job queue name (use in submit script)"
  value       = aws_batch_job_queue.pipeline.name
}

output "compute_environment_name" {
  description = "Batch compute environment name"
  value       = aws_batch_compute_environment.cpu.compute_environment_name
}

output "job_definition_names" {
  description = "Map of phase key to job definition name"
  value       = { for k, v in aws_batch_job_definition.jobs : k => v.name }
}

output "s3_bucket" {
  description = "S3 bucket for all pipeline data"
  value       = var.s3_bucket
}

output "scenes_per_job" {
  description = "Default scenes per worker job"
  value       = var.scenes_per_job
}

output "workers" {
  description = "Default workers per job"
  value       = var.workers
}

output "catalog_path" {
  description = "S3 prefix for the STAC catalog"
  value       = var.catalog_path
}

output "hucs_object_key" {
  description = "S3 key for the HUC8 GeoPackage"
  value       = var.hucs_object_key
}

output "boundaries_object_key" {
  description = "S3 key for the Mexico/Canada boundaries GeoPackage"
  value       = var.boundaries_object_key
}

output "gfm_readme_object_key" {
  description = "S3 key for the GFM data readme PDF (shared by GFM and GFM_EXP)"
  value       = var.gfm_readme_object_key
}

output "gfm_config" {
  description = "GFM pipeline S3 paths"
  value = {
    asset_object_key            = var.gfm_asset_object_key
    manifest_s3_key             = var.gfm_manifest_s3_key
    partial_parquet_prefix      = var.gfm_partial_parquet_prefix
    derived_metadata_path       = var.gfm_derived_metadata_path
    dfo_geopackage_object_key   = var.gfm_dfo_geopackage_object_key
  }
}

output "gfm_exp_config" {
  description = "GFM_EXP pipeline S3 paths"
  value = {
    asset_object_key       = var.gfm_exp_asset_object_key
    manifest_s3_key        = var.gfm_exp_manifest_s3_key
    partial_parquet_prefix = var.gfm_exp_partial_parquet_prefix
    derived_metadata_path  = var.gfm_exp_derived_metadata_path
  }
}

output "ripple_ecr_repository_url" {
  description = "ECR repository URL for the ripple worker image (docker push target)"
  value       = aws_ecr_repository.ripple.repository_url
}

output "ripple_job_definition_names" {
  description = "Map of ripple phase key to job definition name"
  value       = { for k, v in aws_batch_job_definition.ripple_jobs : k => v.name }
}

output "ripple_config" {
  description = "Ripple pipeline S3 paths"
  value = {
    asset_object_key       = var.ripple_asset_object_key
    manifest_s3_key        = var.ripple_manifest_s3_key
    success_markers_prefix = var.ripple_success_markers_prefix
    error_retry_prefix     = var.ripple_error_retry_prefix
    error_nonretry_prefix  = var.ripple_error_nonretry_prefix
    items_per_job          = var.ripple_items_per_job
  }
}

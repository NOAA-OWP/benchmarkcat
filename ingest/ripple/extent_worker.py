"""
ripple extent_worker — AWS Batch array-job worker.

Direct port of fim-misc/flows2fim-runner's src/extent_worker.py (originally a
Nomad-dispatched container's F2FWorker), kept as close to the original as
possible per-method: same config keys, same os.chdir + relative-path
workspace layout, same command strings, same error categorization. The one
intentional behavioral change: each job processes one ripple library
(`dir_name`) and loops all 6 flow intervals internally, instead of one job
per (dir_name, flow_file) pair — matching how the Nomad pipeline actually
dispatched work (one job per collection).

Reads paths/naming/version from worker_config.yaml (baked into the image —
see Dockerfile.ripple), the same config file the original Nomad worker used.

Usage (single item, for local testing):
    python -m ingest.ripple.extent_worker \
        --bucket_name fimc-data --dir_name mip_03110203

Usage (AWS Batch array job — slice of the manifest):
    python -m ingest.ripple.extent_worker \
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
import sys
from datetime import datetime, timezone

import boto3
import yaml
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "worker_config.yaml")


def load_config(config_path):
    """Load YAML configuration file"""
    if not os.path.exists(config_path):
        logger.error(f"Configuration file not found: {config_path}")
        sys.exit(1)

    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        sys.exit(1)


class F2FWorker:
    def __init__(self, dir_name, config, bucket_name):
        self.ripple_dir = dir_name
        self.config = config

        # Get S3 bucket name (can be overridden by environment variable)
        self.s3_bucket = os.environ.get("S3_BUCKET", bucket_name or self.config["s3"]["bucket"])

        # Set up work directories
        self.work_dir = self.config["work_dirs"]["base"]
        self.library_dir = self.config["work_dirs"]["ripple_library"]
        self.control_dir = self.config["work_dirs"]["control_files"]
        self.output_dir = self.config["work_dirs"]["output"]

        # Check for AWS credentials
        aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

        if aws_access_key and aws_secret_key:
            logger.info("Using AWS credentials from environment variables")
        else:
            logger.info("No AWS credentials found in environment, using IAM role if available")

        # Initialize S3 client - will use env vars if available, otherwise IAM role
        self.s3_client = boto3.client("s3")

        # Parse components from directory name. id isn't always numeric
        # (e.g. ohio_rfc, mn_Other, nc_Other), so accept any second segment.
        parts = self.ripple_dir.split("_")
        if len(parts) < 2:
            raise ValueError(f"Invalid directory name format: {self.ripple_dir}")

        self.source = parts[0]
        self.digit_string = parts[1]
        self.common_name = "_".join(parts[2:]) if len(parts) > 2 else None
        self.common_suffix = f"_{self.common_name}" if self.common_name else ""

    def setup_workspace(self):
        """Create clean workspace and required directories"""
        if os.path.exists(self.work_dir):
            shutil.rmtree(self.work_dir)

        os.makedirs(f"{self.work_dir}/{self.library_dir}", exist_ok=True)
        os.makedirs(f"{self.work_dir}/{self.control_dir}", exist_ok=True)
        os.makedirs(f"{self.work_dir}/{self.output_dir}", exist_ok=True)
        os.makedirs(f"{self.work_dir}/nwm_return_period_flows", exist_ok=True)

        # Change to work directory
        os.chdir(self.work_dir)

    def download_file(self, s3_path, local_path):
        """Download a file from S3 to local path"""
        try:
            logger.info(f"Downloading {s3_path} to {local_path}")
            self.s3_client.download_file(self.s3_bucket, s3_path, local_path)
            return True, None
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                error_msg = f"File not found in S3: {s3_path}"
                logger.error(error_msg)
                return False, error_msg
            else:
                error_msg = f"Error downloading {s3_path}: {e}"
                logger.error(error_msg)
                return False, error_msg

    def upload_file(self, local_path, s3_path):
        """Upload a file from local path to S3"""
        try:
            logger.info(f"Uploading {local_path} to {s3_path}")
            self.s3_client.upload_file(local_path, self.s3_bucket, s3_path)
            return True
        except ClientError as e:
            logger.error(f"Error uploading {local_path} to {s3_path}: {e}")
            return False

    def get_formatted_timestamp(self):
        """Return a human-readable timestamp"""
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    def mark_success(self, flow_file):
        """Create a success marker in S3 using JSON format"""
        flow_file_base = flow_file.replace(".csv", "")
        marker_name = self.config["naming"]["success_marker"].format(dir_name=self.ripple_dir, flow_file=flow_file_base)
        marker_key = f"{self.config['s3']['paths']['success_markers']}{marker_name}"

        success_data = {
            "directory": self.ripple_dir,
            "flow_file": flow_file,
            "status": "success",
            "timestamp": self.get_formatted_timestamp(),
        }

        try:
            self.s3_client.put_object(
                Bucket=self.s3_bucket, Key=marker_key, Body=json.dumps(success_data, indent=2), ContentType="application/json"
            )
            logger.info(f"Created success marker: {marker_key}")
            return True
        except ClientError as e:
            logger.error(f"Error creating success marker: {e}")
            return False

    def record_error(self, flow_file, error_type, error_message):
        """Record error details in S3 as JSON"""
        flow_file_base = flow_file.replace(".csv", "")
        error_name = self.config["naming"]["error_marker"].format(dir_name=self.ripple_dir, flow_file=flow_file_base)

        # "File not found" errors are non-retry
        if error_type == "download_error" and "File not found in S3" in error_message:
            error_category = self.config["s3"]["paths"]["error_nonretry"]
        elif error_type == "fim_error" and "error converting VRT to GTIFF: signal: killed" in error_message:
            error_category = self.config["s3"]["paths"]["error_retry"]
        elif error_type in ["controls_error", "fim_error"]:
            error_category = self.config["s3"]["paths"]["error_nonretry"]
        else:
            error_category = self.config["s3"]["paths"]["error_retry"]

        error_key = f"{error_category}{error_name}"

        error_data = {
            "directory": self.ripple_dir,
            "flow_file": flow_file,
            "error_type": error_type,
            "error_message": error_message,
            "timestamp": self.get_formatted_timestamp(),
        }

        try:
            self.s3_client.put_object(
                Bucket=self.s3_bucket, Key=error_key, Body=json.dumps(error_data, indent=2), ContentType="application/json"
            )
            logger.info(f"Recorded error: {error_key}")
            return True
        except ClientError as e:
            logger.error(f"Error recording error details: {e}")
            return False

    def download_required_files(self, flow_file):
        """Download essential files needed for processing"""
        base_path = f"{self.config['s3']['paths']['ripple_collections']}{self.ripple_dir}"

        success, error_msg = self.download_file(f"{base_path}/ripple.gpkg", f"{self.library_dir}/ripple.gpkg")
        if not success:
            self.record_error(flow_file, "download_error", error_msg)
            return False

        success, error_msg = self.download_file(f"{base_path}/start_reaches.csv", f"{self.library_dir}/start_reaches.csv")
        if not success:
            self.record_error(flow_file, "download_error", error_msg)
            return False

        flow_s3_path = f"{self.config['s3']['paths']['flow_files']}{flow_file}"
        success, error_msg = self.download_file(flow_s3_path, f"nwm_return_period_flows/{flow_file}")
        if not success:
            self.record_error(flow_file, "download_error", error_msg)
            return False

        return True

    def run_command(self, cmd):
        """Run a shell command and return result"""
        logger.info(f"Running command: {cmd}")
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.stdout:
            logger.info(f"Command output: {result.stdout[:500]}...")
        if result.stderr:
            logger.error(f"Command error: {result.stderr}")
        return result

    def generate_control_file(self, flow_file, return_period):
        """Generate control file for the extent"""
        control_filename = self.config["naming"]["control_file"].format(
            source=self.source, digit_string=self.digit_string, common_suffix=self.common_suffix, return_period=return_period,
        )
        control_file_path = f"{self.control_dir}/{control_filename}"
        flows2fim_path = self.config["processing"]["flows2fim_path"]

        cmd = (
            f"{flows2fim_path} controls "
            f"-db '{self.library_dir}/ripple.gpkg' "
            f"-f 'nwm_return_period_flows/{flow_file}' "
            f"-o '{control_file_path}' "
            f"-scsv '{self.library_dir}/start_reaches.csv'"
        )

        result = self.run_command(cmd)
        if result.returncode != 0:
            self.record_error(flow_file, "controls_error", result.stderr or "Unknown error generating control file")
            return None

        return control_file_path

    def generate_extent(self, flow_file, return_period, control_file):
        """Generate flood extent using flows2fim"""
        if self.common_name:
            output_filename = self.config["naming"]["output_with_common"].format(return_period=return_period, common_name=self.common_name)
        else:
            output_filename = self.config["naming"]["output_file"].format(return_period=return_period)
        output_path = f"{self.output_dir}/{output_filename}"

        library_path = self.config["s3"]["paths"]["library_extent"].format(dir_name=self.ripple_dir)
        vsi_library_path = f"/vsis3/{self.s3_bucket}/{library_path}"
        flows2fim_path = self.config["processing"]["flows2fim_path"]
        output_format = self.config["processing"]["output_format"]

        cmd = (
            f"{flows2fim_path} fim "
            f"-lib '{vsi_library_path}' "
            f"-c '{control_file}' "
            f"-fmt '{output_format}' "
            f"-o '{output_path}' "
            f"-type 'extent'"
        )

        result = self.run_command(cmd)
        if result.returncode != 0:
            if "The specified key does not exist" in result.stderr or "does not exist in the file system" in result.stderr:
                self.record_error(flow_file, "key_not_exist", "Required files missing in library_extent")
            else:
                self.record_error(flow_file, "fim_error", result.stderr or "Unknown error generating extent")
            return None

        if not os.path.exists(output_path):
            self.record_error(flow_file, "missing_output", "Output file not created despite successful command")
            return None

        return output_path

    def upload_extent(self, flow_file, return_period, output_path):
        """Upload the generated extent to S3"""
        # Determine S3 path based on whether there's a common name
        if self.common_name:
            s3_filename = self.config["naming"]["output_with_common"].format(
                return_period=return_period, common_name=self.common_name
            )
            s3_key_template = self.config["s3"]["paths"]["output_with_common"]
            s3_key = (
                s3_key_template.format(source=self.source, id=self.digit_string, common_name=self.common_name)
                + s3_filename
            )
        else:
            s3_filename = self.config["naming"]["output_file"].format(return_period=return_period)
            s3_key_template = self.config["s3"]["paths"]["output_base"]
            s3_key = s3_key_template.format(source=self.source, id=self.digit_string) + s3_filename

        if not self.upload_file(output_path, s3_key):
            self.record_error(flow_file, "upload_error", f"Failed to upload extent to {s3_key}")
            return False

        return True

    def process_interval(self, flow_file, return_period):
        """Run the full processing workflow for one flow interval"""
        try:
            # Download required files
            if not self.download_required_files(flow_file):
                return False

            # Generate control file
            control_file = self.generate_control_file(flow_file, return_period)
            if not control_file:
                return False

            # Generate extent
            output_path = self.generate_extent(flow_file, return_period, control_file)
            if not output_path:
                return False

            # Upload extent
            if not self.upload_extent(flow_file, return_period, output_path):
                return False

            self.mark_success(flow_file)
            logger.info(f"Successfully processed {self.ripple_dir} with {flow_file}")
            return True

        except Exception as e:
            logger.exception("Unexpected error during processing")
            self.record_error(flow_file, "unexpected_error", str(e))
            return False

    def process(self):
        """Run every flow interval for this library (per config's flow_files list). Returns True only if all succeed."""
        try:
            self.setup_workspace()
            results = [self.process_interval(f["name"], f["return_period"]) for f in self.config["flow_files"]]
            return all(results)
        finally:
            if os.path.exists(self.work_dir):
                try:
                    shutil.rmtree(self.work_dir)
                except Exception:
                    logger.warning(f"Failed to clean up work directory: {self.work_dir}")


def _read_manifest_slice(s3_client, bucket_name, manifest_s3_key, items_per_job, job_index):
    """Download the manifest and return this array child's slice of dir_names."""
    response = s3_client.get_object(Bucket=bucket_name, Key=manifest_s3_key)
    raw = response["Body"].read().decode("utf-8")
    all_items = [json.loads(line) for line in raw.splitlines() if line.strip()]

    start = job_index * items_per_job
    end = start + items_per_job
    return all_items[start:end]


def parse_args():
    parser = argparse.ArgumentParser(description="Run flows2fim (all flow intervals) for one or more ripple libraries.")
    parser.add_argument("--bucket_name", type=str, default=None)
    parser.add_argument("--config-path", type=str, default=DEFAULT_CONFIG_PATH, help="Path to worker_config.yaml.")

    # single-item mode (local testing / manual reruns)
    parser.add_argument("--dir_name", type=str, default=None)

    # array-job mode (AWS Batch)
    parser.add_argument("--manifest-s3-key", type=str, default=None, help="S3 key of the split-phase manifest JSONL.")
    parser.add_argument("--items-per-job", type=int, default=1, help="Number of libraries each array child processes.")
    parser.add_argument("--job-index", type=int, default=None, help="Array child index (injected by batch-entrypoint.sh).")

    return parser.parse_args()


def main():
    args = parse_args()
    s3_client = boto3.client("s3")
    config = load_config(args.config_path)

    if args.manifest_s3_key is not None:
        if args.job_index is None:
            logger.error("--job-index is required when --manifest-s3-key is set")
            sys.exit(1)
        work_items = _read_manifest_slice(
            s3_client, args.bucket_name or config["s3"]["bucket"], args.manifest_s3_key, args.items_per_job, args.job_index
        )
        logger.info("Job index %d: processing %d librar(y/ies)", args.job_index, len(work_items))
    else:
        if not args.dir_name:
            logger.error("Provide either --manifest-s3-key + --job-index, or --dir_name")
            sys.exit(1)
        work_items = [{"dir_name": args.dir_name}]

    failures = 0
    for item in work_items:
        dir_name = item["dir_name"]
        logger.info(f"Starting processing for {dir_name} (all {len(config['flow_files'])} flow intervals)")
        try:
            worker = F2FWorker(dir_name, config, args.bucket_name)
            success = worker.process()
        except Exception:
            logger.exception("Unhandled exception in worker")
            success = False
        if not success:
            failures += 1

    if failures:
        logger.error("%d of %d librar(y/ies) had at least one failed interval", failures, len(work_items))
        sys.exit(1)

    logger.info("All %d librar(y/ies) completed successfully (all intervals)", len(work_items))


if __name__ == "__main__":
    main()

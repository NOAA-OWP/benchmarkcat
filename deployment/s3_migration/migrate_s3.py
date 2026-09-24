"""
Simple S3 migration with explicit path mappings.

This script:
1. Downloads STAC catalog from source
2. Updates HREFs to point to the new split-bucket structure
3. Generates AWS CLI commands for asset copying
4. Uploads updated catalog to the STAC bucket

Target structure:
    s3://hv-fim-dev-stac/
    └── benchmark-stac/
        ├── catalog.json
        └── <collection-id>/
            ├── collection.json
            └── <item-id>/<item-id>.json

    s3://hv-fim-dev-data/
    └── benchmark/
        ├── shared-assets/       # Shared ingestion assets + parquet caches
        │   ├── WBDHU8_webproj.gpkg
        │   ├── Mexico_Canada_boundaries.gpkg
        │   ├── dfo_all_usa_events_post_2015.gpkg
        │   ├── gfm_data_readme.pdf
        │   └── *.parquet
        ├── ble-collection/<item-id>/
        ├── gfm-collection/<item-id>/
        ├── iceye-collection/<item-id>/
        └── ...

Usage:
    python migrate_s3.py \\
        --source-bucket fimc-data \\
        --stac-bucket hv-fim-dev-stac \\
        --stac-prefix benchmark-stac \\
        --data-bucket hv-fim-dev-data \\
        --data-prefix benchmark \\
        --working-dir ~/benchmark-catalog \\
        --aws-profile your-profile \\
        --generate-copy-commands  # Creates shell script to copy assets
"""

import os
import sys
import json
import shutil
import argparse
import logging
import subprocess
from pathlib import Path
from urllib.parse import urlparse
from typing import Dict, List, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Parquet caches are downloaded from source and copied to shared-assets/ in the data bucket
SHARED_ASSET_EXTENSIONS = ('*.parquet',)

# Shared ingestion assets — source paths are relative to the source bucket root.
# All land in s3://<data-bucket>/<data-prefix>/shared-assets/
SHARED_INGESTION_ASSETS = [
    {
        'source': 'benchmark/stac-bench-cat/assets/WBDHU8_webproj.gpkg',
        'dest': 'shared-assets/WBDHU8_webproj.gpkg',
        'description': 'HUC8 boundaries (used by GFM, GFM-Exp, HWM, RIPPLE ingestion)',
    },
    {
        'source': 'benchmark/stac-bench-cat/assets/Mexico_Canada_boundaries.gpkg',
        'dest': 'shared-assets/Mexico_Canada_boundaries.gpkg',
        'description': 'Mexico/Canada borders (used by GFM, GFM-Exp ingestion)',
    },
    {
        'source': 'benchmark/rs/dfo_all_usa_events_post_2015.gpkg',
        'dest': 'shared-assets/dfo_all_usa_events_post_2015.gpkg',
        'description': 'DFO flood events geopackage (used by GFM ingestion)',
    },
    {
        'source': 'benchmark/rs/gfm/gfm_data_readme.pdf',
        'dest': 'shared-assets/gfm_data_readme.pdf',
        'description': 'GFM data readme (linked as collection asset in GFM/GFM-Exp)',
    },
]

# Direct source-key → shared-assets filename mappings for collection-level shared assets.
# These don't follow the PATH_MAPPINGS pattern (different source prefixes, no collection subdir).
SHARED_ASSET_HREF_MAPPINGS = {
    entry['source']: entry['dest']
    for entry in SHARED_INGESTION_ASSETS
}

# Explicit source → destination mappings.
# 'dest' is relative to <data-prefix>/ in the data bucket (e.g. benchmark/).
PATH_MAPPINGS = {
    'ble-collection': {
        'source': 'benchmark/high_resolution_validation_data_ble',
        'dest': 'ble-collection'
    },
    'ripple-fim-collection': {
        'source': 'benchmark/ripple_v0.11.x',
        'dest': 'ripple-fim-collection'
    },
    'hwm-collection': {
        'source': 'benchmark/high_water_marks/usgs',
        'dest': 'hwm-collection'
    },
    'nws-fim-collection': {
        'source': 'hand_fim/test_cases/nws_test_cases/validation_data_nws',
        'dest': 'nws-fim-collection'
    },
    'usgs-fim-collection': {
        'source': 'hand_fim/test_cases/usgs_test_cases/validation_data_usgs',
        'dest': 'usgs-fim-collection'
    },
    'gfm-collection': {
        'source': 'benchmark/rs/gfm',
        'dest': 'gfm-collection'
    },
    'iceye-collection': {
        'source': 'benchmark/rs/iceye',
        'dest': 'iceye-collection'
    },
    'gfm-expanded-collection': {
        'source': 'benchmark/rs/PI4',
        'dest': 'gfm-expanded-collection'
    }
}


def _build_s3_sync_args(
    source: str,
    dest: str,
    aws_profile: Optional[str] = None,
    extra_args: Optional[list[str]] = None,
) -> list[str]:
    """Build argument list for an aws s3 sync command."""
    args = ["aws", "s3", "sync", source, dest]
    if aws_profile:
        args += ["--profile", aws_profile]
    if extra_args:
        args += extra_args
    return args


def _stac_uri(stac_bucket: str, stac_prefix: str, *parts: str) -> str:
    """Build a full S3 URI under s3://<stac_bucket>/<stac_prefix>/."""
    key = "/".join([stac_prefix] + list(parts))
    return f"s3://{stac_bucket}/{key}"


def _data_uri(data_bucket: str, data_prefix: str, *parts: str) -> str:
    """Build a full S3 URI under s3://<data_bucket>/<data_prefix>/."""
    key = "/".join([data_prefix] + list(parts))
    return f"s3://{data_bucket}/{key}"


def update_asset_href(
    old_href: str,
    source_bucket: str,
    data_bucket: str,
    data_prefix: str,
    collection_id: str,
) -> Optional[str]:
    """
    Update asset HREF to the new split-bucket structure.

    Args:
        old_href: Original S3 URI or URL
        source_bucket: Source bucket name
        data_bucket: Destination data bucket name (e.g. hv-fim-dev-data)
        data_prefix: Data bucket prefix (e.g. benchmark)
        collection_id: Collection ID for path mapping

    Returns:
        Updated HREF pointing to the data bucket
    """
    # Extract key from s3:// or https://...s3... href
    key: Optional[str] = None
    if old_href.startswith('s3://'):
        parsed = urlparse(old_href)
        if parsed.netloc != source_bucket:
            return old_href
        key = parsed.path.lstrip('/')
    else:
        parsed = urlparse(old_href)
        host = (parsed.hostname or '').lower()
        path = parsed.path.lstrip('/')

        if parsed.scheme in ('http', 'https'):
            bucket: Optional[str] = None

            # Path-style:
            #   https://s3.amazonaws.com/<bucket>/<key>
            #   https://s3-<region>.amazonaws.com/<bucket>/<key>
            if host == 's3.amazonaws.com' or host.startswith('s3-'):
                parts = path.split('/', 1)
                if len(parts) == 2:
                    bucket, key = parts[0], parts[1]

            # Virtual-hosted-style:
            #   https://<bucket>.s3.amazonaws.com/<key>
            #   https://<bucket>.s3-<region>.amazonaws.com/<key>
            elif host.endswith('.s3.amazonaws.com') or '.s3-' in host:
                bucket = host.split('.s3', 1)[0]
                key = path

            if bucket != source_bucket:
                return old_href

    if key is None:
        logger.debug(f"HREF unchanged (no matching rule): {old_href}")
        return old_href

    # Check shared ingestion assets first (collection-level, not under PATH_MAPPINGS)
    if key in SHARED_ASSET_HREF_MAPPINGS:
        return f"s3://{data_bucket}/{data_prefix}/{SHARED_ASSET_HREF_MAPPINGS[key]}"

    # Fall back to collection PATH_MAPPINGS for item-level assets
    if collection_id not in PATH_MAPPINGS:
        logger.warning(f"No path mapping for collection: {collection_id}")
        return old_href

    mapping = PATH_MAPPINGS[collection_id]
    source_path = mapping['source']
    dest_path = f"{data_prefix}/{mapping['dest']}"

    if key.startswith(source_path):
        relative_path = key[len(source_path):].lstrip('/')
    elif source_path in key:
        relative_path = key.split(source_path, 1)[1].lstrip('/')
    else:
        logger.debug(f"Asset {key} does not match source path {source_path}")
        return old_href

    new_key = f"{dest_path}/{relative_path}" if relative_path else dest_path
    return f"s3://{data_bucket}/{new_key}"


def download_catalog(
    source_bucket: str,
    source_catalog_prefix: str,
    working_dir: Path,
    aws_profile: Optional[str],
    dry_run: bool,
) -> int:
    """Download STAC catalog from S3."""
    logger.info("Phase 1: Downloading catalog from source S3...")

    source_dir = working_dir / 'source_catalog'
    source_dir.mkdir(parents=True, exist_ok=True)

    cmd_args = _build_s3_sync_args(
        f"s3://{source_bucket}/{source_catalog_prefix}/",
        f"{source_dir}/",
        aws_profile,
        ["--exclude", "*", "--include", "*.json"],
    )

    assets_extra = ["--exclude", "*"]
    for ext in SHARED_ASSET_EXTENSIONS:
        assets_extra += ["--include", ext]
    assets_cmd_args = _build_s3_sync_args(
        f"s3://{source_bucket}/{source_catalog_prefix}/assets/",
        f"{source_dir}/assets/",
        aws_profile,
        assets_extra,
    )

    if dry_run:
        logger.info(f"[DRY RUN] Would run: {' '.join(cmd_args)}")
        logger.info(f"[DRY RUN] Would run: {' '.join(assets_cmd_args)}")
        return 0

    logger.info(f"Running: {' '.join(cmd_args)}")
    result = subprocess.run(cmd_args)

    if result.returncode != 0:
        logger.error("Failed to download catalog")
        return result.returncode

    logger.info(f"Running: {' '.join(assets_cmd_args)}")
    assets_result = subprocess.run(assets_cmd_args)
    if assets_result.returncode != 0:
        logger.warning("Failed to download shared assets (non-fatal)")

    json_files = list(source_dir.rglob('*.json'))
    parquet_files = list(source_dir.rglob('*.parquet'))
    logger.info(f"Downloaded {len(json_files)} JSON files and {len(parquet_files)} parquet caches")

    return 0


def update_catalog_hrefs(
    working_dir: Path,
    source_bucket: str,
    stac_bucket: str,
    stac_prefix: str,
    data_bucket: str,
    data_prefix: str,
    dry_run: bool,
) -> None:
    """Update all HREFs in catalog to new split-bucket structure."""
    logger.info("Phase 2: Updating HREFs in catalog...")

    source_dir = working_dir / 'source_catalog'
    dest_dir = working_dir / 'dest_catalog'
    dest_dir.mkdir(parents=True, exist_ok=True)

    if not source_dir.exists():
        logger.error(f"Source catalog not found at {source_dir}")
        return

    stats = {
        'collections': 0,
        'items': 0,
        'assets_updated': 0,
        'assets_unchanged': 0
    }
    manifest_entries: list[dict] = []

    # Process all JSON files
    json_files = list(source_dir.rglob('*.json'))

    for json_file in json_files:
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)

            file_type = data.get('type', '')
            file_modified = False

            # Determine collection ID
            collection_id = None
            if file_type == 'Collection':
                collection_id = data.get('id')
                stats['collections'] += 1
            elif file_type == 'Feature':
                # Extract collection from path or links
                rel_path = json_file.relative_to(source_dir)
                collection_id = rel_path.parts[0] if len(rel_path.parts) > 0 else None

                # Also check collection link
                for link in data.get('links', []):
                    if link.get('rel') == 'collection':
                        collection_href = link.get('href', '')
                        if '/' in collection_href:
                            parts = collection_href.split('/')
                            for part in parts:
                                if 'collection' in part and part in PATH_MAPPINGS:
                                    collection_id = part
                                    break

                stats['items'] += 1

            # Update assets
            if 'assets' in data and collection_id:
                for asset_key, asset in data['assets'].items():
                    if 'href' not in asset or not asset['href']:
                        continue

                    old_href = asset['href']
                    new_href = update_asset_href(
                        old_href,
                        source_bucket,
                        data_bucket,
                        data_prefix,
                        collection_id,
                    )

                    if new_href != old_href:
                        if not dry_run:
                            asset['href'] = new_href
                        else:
                            logger.debug(f"Would update: {old_href} -> {new_href}")

                        manifest_entries.append({
                            "file": str(json_file.relative_to(source_dir)),
                            "asset_key": asset_key,
                            "old_href": old_href,
                            "new_href": new_href,
                        })
                        file_modified = True
                        stats['assets_updated'] += 1
                    else:
                        stats['assets_unchanged'] += 1

            # Update collection/item links to new stac/ structure
            if 'links' in data:
                for link in data['links']:
                    href = link.get('href', '')

                    # Update relative paths if needed
                    if href.startswith('./') or href.startswith('../'):
                        # Keep relative paths as-is for now
                        continue

                    # Update absolute S3 paths
                    if href.startswith('s3://') and source_bucket in href:
                        # Update catalog/collection links to point at the stac bucket
                        if 'catalog.json' in href or 'collection.json' in href:
                            key = href.split(source_bucket)[-1].lstrip('/')
                            if 'stac-bench-cat' in key:
                                new_key = key.replace('benchmark/stac-bench-cat', stac_prefix)
                            else:
                                new_key = f"{stac_prefix}/{key}"

                            new_link_href = f"s3://{stac_bucket}/{new_key}"
                            if not dry_run:
                                link['href'] = new_link_href
                            file_modified = True

            # Save updated file
            if True:  # Always copy to dest to preserve catalog structure
                rel_path = json_file.relative_to(source_dir)
                dest_file = dest_dir / rel_path
                dest_file.parent.mkdir(parents=True, exist_ok=True)

                if not dry_run:
                    with open(dest_file, 'w') as f:
                        json.dump(data, f, indent=2)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse {json_file}: {e}")
        except Exception as e:
            logger.error(f"Error processing {json_file}: {e}")

    # Copy parquet caches to dest directory so they get uploaded to stac/assets/
    source_assets_dir = source_dir / 'assets'
    if source_assets_dir.exists():
        dest_assets_dir = dest_dir / 'assets'
        dest_assets_dir.mkdir(parents=True, exist_ok=True)
        shared_count = 0
        for ext in SHARED_ASSET_EXTENSIONS:
            for asset_file in source_assets_dir.rglob(ext):
                rel = asset_file.relative_to(source_assets_dir)
                dest_file = dest_assets_dir / rel
                dest_file.parent.mkdir(parents=True, exist_ok=True)
                if not dry_run:
                    shutil.copy2(asset_file, dest_file)
                shared_count += 1
        logger.info(f"  Parquet caches copied: {shared_count}")

    logger.info(f"\nProcessed:")
    logger.info(f"  Collections: {stats['collections']}")
    logger.info(f"  Items: {stats['items']}")
    logger.info(f"  Assets updated: {stats['assets_updated']}")
    logger.info(f"  Assets unchanged: {stats['assets_unchanged']}")

    manifest_path = working_dir / "migration_manifest.json"
    with open(manifest_path, 'w') as f:
        json.dump(manifest_entries, f, indent=2)
    logger.info(f"Migration manifest written to: {manifest_path}")


def generate_copy_commands(
    source_bucket: str,
    data_bucket: str,
    data_prefix: str,
    working_dir: Path,
    aws_profile: Optional[str],
    dry_run: bool = False,
) -> None:
    """Generate shell script with AWS S3 sync commands for asset copying."""
    logger.info("Phase 3: Generating asset copy commands...")

    script_path = working_dir / 'copy_assets.sh'

    profile_flag = f"--profile {aws_profile}" if aws_profile else ""

    if dry_run:
        logger.info(f"[DRY RUN] Would generate copy script at: {script_path}")
        for collection_id, mapping in PATH_MAPPINGS.items():
            dest_path = f"{data_prefix}/{mapping['dest']}"
            logger.info(f"  {collection_id}: {mapping['source']} -> s3://{data_bucket}/{dest_path}")
        return

    with open(script_path, 'w') as f:
        f.write("#!/bin/bash\n\n")
        f.write("# Generated S3 asset copy commands\n")
        f.write("# Copies benchmark collection assets to hv-fim-dev-data\n\n")
        f.write("set -e\n\n")

        f.write("echo 'Starting S3 asset migration...'\n")
        f.write("echo ''\n\n")

        for collection_id, mapping in PATH_MAPPINGS.items():
            source_path = mapping['source']
            dest_path = f"{data_prefix}/{mapping['dest']}"

            f.write(f"# {collection_id}\n")
            f.write(f"echo 'Copying {collection_id}...'\n")
            f.write(
                f"aws s3 sync s3://{source_bucket}/{source_path}/ "
                f"s3://{data_bucket}/{dest_path}/ {profile_flag}\n\n"
            )

        f.write("# Shared ingestion assets\n")
        f.write("echo 'Copying shared ingestion assets...'\n")
        for entry in SHARED_INGESTION_ASSETS:
            dest_asset = f"{data_prefix}/{entry['dest']}"
            f.write(f"# {entry['description']}\n")
            f.write(
                f"aws s3 cp s3://{source_bucket}/{entry['source']} "
                f"s3://{data_bucket}/{dest_asset} {profile_flag}\n"
            )
        f.write("\n")

        f.write("echo ''\n")
        f.write("echo 'Asset migration complete!'\n")

    os.chmod(script_path, 0o755)

    logger.info(f"\nAsset copy commands saved to: {script_path}")
    logger.info("Review the script, then run it to copy assets.")
    logger.info(f"\nTo execute: {script_path}")


def upload_catalog(
    stac_bucket: str,
    stac_prefix: str,
    data_bucket: str,
    data_prefix: str,
    working_dir: Path,
    aws_profile: Optional[str],
    dry_run: bool,
) -> int:
    """Upload updated catalog JSON to the STAC bucket and parquet caches to the data bucket."""
    logger.info("Phase 4: Uploading updated catalog to STAC bucket...")

    dest_dir = working_dir / 'dest_catalog'

    if not dest_dir.exists():
        logger.error(f"Destination catalog not found at {dest_dir}")
        return 1

    stac_uri = _stac_uri(stac_bucket, stac_prefix) + "/"
    json_cmd_args = _build_s3_sync_args(
        f"{dest_dir}/",
        stac_uri,
        aws_profile,
        ["--exclude", "*", "--include", "*.json"],
    )

    # Parquet caches land in shared-assets/ in the data bucket
    shared_assets_uri = _data_uri(data_bucket, data_prefix, "shared-assets") + "/"
    parquet_cmd_args = _build_s3_sync_args(
        f"{dest_dir}/assets/",
        shared_assets_uri,
        aws_profile,
        ["--exclude", "*"] + [arg for ext in SHARED_ASSET_EXTENSIONS for arg in ("--include", ext)],
    )

    if dry_run:
        logger.info(f"[DRY RUN] Would run: {' '.join(json_cmd_args)}")
        logger.info(f"[DRY RUN] Would run: {' '.join(parquet_cmd_args)}")
        return 0

    logger.info(f"Running: {' '.join(json_cmd_args)}")
    result = subprocess.run(json_cmd_args)
    if result.returncode != 0:
        logger.error("Failed to upload catalog")
        return result.returncode

    source_assets_dir = dest_dir / 'assets'
    if source_assets_dir.exists() and any(source_assets_dir.rglob('*.parquet')):
        logger.info(f"Running: {' '.join(parquet_cmd_args)}")
        parquet_result = subprocess.run(parquet_cmd_args)
        if parquet_result.returncode != 0:
            logger.warning("Failed to upload parquet caches (non-fatal)")

    json_count = len(list(dest_dir.rglob('*.json')))
    parquet_count = len(list(dest_dir.rglob('*.parquet')))
    logger.info(f"Uploaded {json_count} JSON files to {stac_uri}")
    if parquet_count:
        logger.info(f"Uploaded {parquet_count} parquet caches to {shared_assets_uri}")

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Simple S3 migration with explicit path mappings"
    )
    parser.add_argument(
        '--source-bucket',
        required=True,
        help='Source S3 bucket (e.g., fimc-data)'
    )
    parser.add_argument(
        '--source-catalog-prefix',
        default='benchmark/stac-bench-cat',
        help='Source catalog prefix (default: benchmark/stac-bench-cat)'
    )
    parser.add_argument(
        '--stac-bucket',
        required=True,
        help='Destination S3 bucket for STAC metadata (e.g., hv-fim-dev-stac)'
    )
    parser.add_argument(
        '--stac-prefix',
        required=True,
        help='Prefix under the STAC bucket (e.g., benchmark-stac)'
    )
    parser.add_argument(
        '--data-bucket',
        required=True,
        help='Destination S3 bucket for data assets (e.g., hv-fim-dev-data)'
    )
    parser.add_argument(
        '--data-prefix',
        required=True,
        help='Prefix under the data bucket (e.g., benchmark)'
    )
    parser.add_argument(
        '--working-dir',
        default='~/benchmark-catalog',
        help='Local working directory (default: ~/benchmark-catalog)'
    )
    parser.add_argument(
        '--aws-profile',
        help='AWS profile to use (optional)'
    )
    parser.add_argument(
        '--generate-copy-commands',
        action='store_true',
        help='Generate shell script for asset copying (Phase 3 only)'
    )
    parser.add_argument(
        '--skip-download',
        action='store_true',
        help='Skip catalog download (Phase 1)'
    )
    parser.add_argument(
        '--skip-update',
        action='store_true',
        help='Skip HREF update (Phase 2)'
    )
    parser.add_argument(
        '--skip-upload',
        action='store_true',
        help='Skip catalog upload (Phase 4)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview operations without making changes'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    working_dir = Path(args.working_dir).expanduser()
    working_dir.mkdir(parents=True, exist_ok=True)

    logger.info("="*60)
    logger.info("S3 Migration with Restructuring")
    logger.info("="*60)
    logger.info(f"Source: s3://{args.source_bucket}/{args.source_catalog_prefix}/")
    logger.info(f"STAC bucket: s3://{args.stac_bucket}/{args.stac_prefix}/")
    logger.info(f"Data bucket: s3://{args.data_bucket}/{args.data_prefix}/")
    logger.info(f"Working directory: {working_dir}")
    if args.dry_run:
        logger.info("MODE: DRY RUN")
    logger.info("")

    logger.info("Path mappings:")
    for collection_id, mapping in PATH_MAPPINGS.items():
        logger.info(f"  {collection_id}:")
        logger.info(f"    s3://{args.source_bucket}/{mapping['source']}")
        logger.info(f"    -> s3://{args.data_bucket}/{args.data_prefix}/{mapping['dest']}")
    logger.info("")

    # Phase 1: Download catalog
    if not args.skip_download:
        result = download_catalog(
            args.source_bucket,
            args.source_catalog_prefix,
            working_dir,
            args.aws_profile,
            args.dry_run
        )
        if result != 0:
            sys.exit(result)
    else:
        logger.info("Skipping catalog download (--skip-download)")

    # Phase 2: Update HREFs
    if not args.skip_update:
        update_catalog_hrefs(
            working_dir,
            args.source_bucket,
            args.stac_bucket,
            args.stac_prefix,
            args.data_bucket,
            args.data_prefix,
            args.dry_run,
        )
    else:
        logger.info("Skipping HREF update (--skip-update)")

    # Phase 3: Generate copy commands
    if args.generate_copy_commands:
        generate_copy_commands(
            args.source_bucket,
            args.data_bucket,
            args.data_prefix,
            working_dir,
            args.aws_profile,
            args.dry_run,
        )

    # Phase 4: Upload catalog
    if not args.skip_upload:
        result = upload_catalog(
            args.stac_bucket,
            args.stac_prefix,
            args.data_bucket,
            args.data_prefix,
            working_dir,
            args.aws_profile,
            args.dry_run,
        )
        if result != 0:
            sys.exit(result)
    else:
        logger.info("Skipping catalog upload (--skip-upload)")

    logger.info("\n" + "="*60)
    logger.info("Migration Script Complete!")
    logger.info("="*60)
    logger.info("\nNext steps:")
    logger.info(f"1. Review updated catalog in: {working_dir}/dest_catalog/")
    logger.info(f"2. Run asset copy script: {working_dir}/copy_assets.sh")
    logger.info(f"3. Verify uploads:")
    logger.info(f"   aws s3 ls s3://{args.stac_bucket}/{args.stac_prefix}/ --recursive | wc -l")
    logger.info(f"   aws s3 ls s3://{args.data_bucket}/{args.data_prefix}/shared-assets/")
    logger.info(f"   aws s3 ls s3://{args.data_bucket}/{args.data_prefix}/ --recursive | wc -l")


if __name__ == "__main__":
    main()

# S3 Catalog Migration Guide

Migrates STAC catalog and assets from NGWPC S3 (`fimc-data`) to two dedicated OWP destination buckets.

## Overview

**Destination Structure:**
```
s3://hv-fim-dev-stac/
└── benchmark-stac/                          # STAC metadata (~23,000 files, ~200 MB)
    ├── catalog.json                         # Root catalog
    ├── ble-collection/
    │   ├── collection.json
    │   └── <item-id>/<item-id>.json
    ├── gfm-collection/
    ├── gfm-expanded-collection/
    ├── hwm-collection/
    ├── iceye-collection/
    ├── nws-fim-collection/
    ├── ripple-fim-collection/
    └── usgs-fim-collection/

s3://hv-fim-dev-data/                        # Geospatial assets (~2.08 TB)
└── benchmark/
    ├── shared-assets/                       # GPKGs, PDFs, parquet caches
    │   ├── WBDHU8_webproj.gpkg              # Shared HUC8 boundaries
    │   ├── gfm_data_readme.pdf
    │   └── *.parquet                        # Derived-asset parquet caches
    ├── backups/                             # PostgreSQL dumps
    ├── ble-collection/<item-id>/
    ├── gfm-collection/<item-id>/
    │   └── S1A_IW_GRDH_[...]/
    │       ├── *_ENSEMBLE_FLOOD_*.tif
    │       ├── *_ENSEMBLE_UNCERTAINTY_*.tif
    │       └── *_ADVFLAG_*.tif
    ├── gfm-expanded-collection/<item-id>/
    ├── hwm-collection/<item-id>/
    ├── iceye-collection/<item-id>/          # ICEYE_FSD-[...] scenes
    ├── nws-fim-collection/<item-id>/
    ├── ripple-fim-collection/<item-id>/
    └── usgs-fim-collection/<item-id>/
```

## Path Mappings

| Collection | Source Path | Destination (under `hv-fim-dev-data/benchmark/`) |
|------------|-------------|--------------------------------------------------|
| ble-collection | `benchmark/high_resolution_validation_data_ble/` | `ble-collection/` |
| ripple-fim-collection | `benchmark/ripple_v0.11.x/` | `ripple-fim-collection/` |
| hwm-collection | `benchmark/high_water_marks/usgs/` | `hwm-collection/` |
| nws-fim-collection | `hand_fim/test_cases/nws_test_cases/validation_data_nws/` | `nws-fim-collection/` |
| usgs-fim-collection | `hand_fim/test_cases/usgs_test_cases/validation_data_usgs/` | `usgs-fim-collection/` |
| gfm-collection | `benchmark/rs/gfm/` | `gfm-collection/` |
| iceye-collection | `benchmark/rs/iceye/` | `iceye-collection/` |
| gfm-expanded-collection | `benchmark/rs/PI4/` | `gfm-expanded-collection/` |

**STAC Catalog:** `benchmark/stac-bench-cat/` → `hv-fim-dev-stac/benchmark-stac/`

**Shared Assets:** GPKGs, PDFs, and parquet caches → `hv-fim-dev-data/benchmark/shared-assets/`

**Known behavior — `gfm-expanded-collection` copies some assets with no catalog item:**
`migrate_s3.py` copies the whole `benchmark/rs/PI4/` source prefix, not just the assets referenced
by a linked STAC item. 1,060 scenes (761 Canada + 299 Mexico) are deliberately excluded from the
catalog by `gfm_exp_col.py`'s `is_within_neighbor_countries()` check, so they have no STAC item
and no `collection.json` link — but their assets still exist under `rs/PI4/` and still get copied
to OWP, since the migration has no per-item filtering.

## Prerequisites

### AWS Credentials

You need access to the source bucket and both destination buckets.

**Option 1: Named AWS Profiles**
```bash
aws configure --profile ngwpc
aws configure --profile owp

python migrate_s3.py \
  --source-bucket fimc-data \
  --stac-bucket hv-fim-dev-stac \
  --stac-prefix benchmark-stac \
  --data-bucket hv-fim-dev-data \
  --data-prefix benchmark \
  --aws-profile owp
```

**Option 2: EC2 IAM Role + Cross-Account Policy**

Run from EC2 in OWP account with an IAM role that has write access to both destination buckets and read access to `fimc-data`.

**Source bucket policy** (on `fimc-data`):
```json
{
  "Effect": "Allow",
  "Principal": {
    "AWS": "arn:aws:iam::OWP-ACCOUNT-ID:role/owp-ec2-role"
  },
  "Action": ["s3:GetObject", "s3:ListBucket"],
  "Resource": [
    "arn:aws:s3:::fimc-data",
    "arn:aws:s3:::fimc-data/benchmark/*",
    "arn:aws:s3:::fimc-data/hand_fim/*"
  ]
}
```

### Local Requirements

- Python 3.7+
- AWS CLI installed and configured
- ~1GB free disk space (for catalog metadata)
- jq (for verification commands)

## Procedure

All commands assume you are in `deployment/s3_migration/`.

### Pre-flight

**1. Dry run** — preview path changes without downloading, uploading, or writing any files:
```bash
python migrate_s3.py \
  --source-bucket fimc-data \
  --stac-bucket hv-fim-dev-stac \
  --stac-prefix benchmark-stac \
  --data-bucket hv-fim-dev-data \
  --data-prefix benchmark \
  --aws-profile your-profile \
  --generate-copy-commands \
  --dry-run --verbose
```
Verify: output should show a non-zero "Assets updated" count and list the copy-command mappings. No files are created on disk during `--dry-run`.

**2. Metadata-only inspection** — download catalog and update HREFs locally, skip upload and asset copy:
```bash
python migrate_s3.py \
  --source-bucket fimc-data \
  --stac-bucket hv-fim-dev-stac \
  --stac-prefix benchmark-stac \
  --data-bucket hv-fim-dev-data \
  --data-prefix benchmark \
  --aws-profile your-profile \
  --skip-upload
```
Verify: inspect a generated JSON to confirm HREFs point to the data bucket:
```bash
grep "href" ~/benchmark-catalog/dest_catalog/gfm-collection/collection.json | head -5
# Expected: "s3://hv-fim-dev-data/benchmark/gfm-collection/..."
```

**3. Single-collection live test** — edit `PATH_MAPPINGS` in `migrate_s3.py` to keep only one small collection (e.g., `iceye-collection`), run against test buckets, execute `copy_assets.sh`, then verify:
```bash
aws s3 ls s3://hv-fim-dev-data/benchmark/iceye-collection/ --recursive
```

### Phase 1: Download Catalog (~5 min, ~200MB)

```bash
python migrate_s3.py \
  --source-bucket fimc-data \
  --stac-bucket hv-fim-dev-stac \
  --stac-prefix benchmark-stac \
  --data-bucket hv-fim-dev-data \
  --data-prefix benchmark \
  --aws-profile your-profile \
  --skip-upload
```
Downloads ~23,000 JSON files to `~/benchmark-catalog/source_catalog/` and updates HREFs (Phase 2). Add `--generate-copy-commands` to also produce `copy_assets.sh`.

Verify:
```bash
cat ~/benchmark-catalog/dest_catalog/gfm-collection/items/*/item.json | \
  jq '.assets[].href'
```

### Phase 2: Update HREFs (~2 min)

Runs automatically with Phase 1. Transforms asset paths:

- `s3://fimc-data/benchmark/rs/gfm/dfo-4336/flood.tif`
- → `s3://hv-fim-dev-data/benchmark/gfm-collection/dfo-4336/flood.tif`

Updated catalog is saved to `~/benchmark-catalog/dest_catalog/`. A manifest of all HREF changes is written to `~/benchmark-catalog/migration_manifest.json`.

### Phase 3: Copy Assets (~8-12 hours, ~2.08 TB)

Generate the copy script, then review and execute:
```bash
python migrate_s3.py \
  --source-bucket fimc-data \
  --stac-bucket hv-fim-dev-stac \
  --stac-prefix benchmark-stac \
  --data-bucket hv-fim-dev-data \
  --data-prefix benchmark \
  --aws-profile your-profile \
  --skip-download --skip-update --skip-upload \
  --generate-copy-commands
```
```bash
~/benchmark-catalog/copy_assets.sh
```

Monitor progress:
```bash
watch -n 30 'aws s3 ls s3://hv-fim-dev-data/benchmark/ --recursive | wc -l'
```

Verify:
```bash
aws s3 ls s3://hv-fim-dev-data/benchmark/
# Should list all 8 collection prefixes and shared-assets/

aws s3 ls s3://hv-fim-dev-data/benchmark/gfm-collection/ --recursive | head -20
```

### Phase 4: Upload Catalog (~5 min)

```bash
python migrate_s3.py \
  --source-bucket fimc-data \
  --stac-bucket hv-fim-dev-stac \
  --stac-prefix benchmark-stac \
  --data-bucket hv-fim-dev-data \
  --data-prefix benchmark \
  --aws-profile your-profile \
  --skip-download --skip-update
```

Verify:
```bash
aws s3 ls s3://hv-fim-dev-stac/benchmark-stac/ --recursive | wc -l
# Expected: ~23,000

aws s3 cp s3://hv-fim-dev-stac/benchmark-stac/catalog.json - | jq '.'

aws s3 cp s3://hv-fim-dev-stac/benchmark-stac/gfm-collection/collection.json - | \
  jq '.assets[].href'
# All HREFs should show: s3://hv-fim-dev-data/benchmark/gfm-collection/...

aws s3 ls s3://hv-fim-dev-data/benchmark/shared-assets/
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| HREFs still point to old paths | Check that collection IDs in STAC match `PATH_MAPPINGS` keys. Run with `--verbose` to see HREF transformations. |
| Permission errors | Verify access: `aws s3 ls s3://fimc-data/benchmark/ --profile your-profile` and `aws s3 ls s3://hv-fim-dev-stac/ --profile your-profile`. Check identity with `aws sts get-caller-identity`. |
| `copy_assets.sh` fails partway | Re-run — `aws s3 sync` is idempotent and skips existing files. Check error output and verify permissions on both destination buckets. |
| Collections missing from destination | Verify the collection exists in source STAC and has an entry in `PATH_MAPPINGS`. Add missing mapping and re-run Phase 2. |
| Partial catalog upload | Re-run Phase 4 — upload is idempotent. |

## Resume After Failure

The migration is idempotent — safely re-run any phase:

```bash
# Catalog already downloaded?
python migrate_s3.py --source-bucket fimc-data \
  --stac-bucket hv-fim-dev-stac --stac-prefix benchmark-stac \
  --data-bucket hv-fim-dev-data --data-prefix benchmark ... --skip-download

# HREFs already updated?
python migrate_s3.py --source-bucket fimc-data \
  --stac-bucket hv-fim-dev-stac --stac-prefix benchmark-stac \
  --data-bucket hv-fim-dev-data --data-prefix benchmark ... --skip-download --skip-update

# Assets partially copied?
~/benchmark-catalog/copy_assets.sh  # Skips existing files
```

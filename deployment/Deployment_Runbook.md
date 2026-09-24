# BenchmarkCat STAC: Deployment Runbook

## Overview & Architecture

BenchmarkCat is a STAC geospatial catalog (~23,000 items, 8 collections, ~2.08 TB assets) migrating from NGWPC's infrastructure to OWP's infrastructure.

| Component | Details |
|-----------|---------|
| EC2 Instance | t3.xlarge (4 vCPU, 16 GB RAM) |
| Services | PostgreSQL (5432), STAC API (8082), STAC Browser (8080), asset-proxy (8083) |
| Storage | `hv-fim-dev-stac` (STAC catalog) and `hv-fim-dev-data` (assets) |
| Bootstrap | Automated via `deployment/terraform/templates/user_data_standalone.sh.tpl` or manually via `deployment/terraform/user-data/owp-bootstrap.sh` |

---

## Phase 0: Prerequisites & Cross-Team Coordination

> **Machine: admin machine**

### 0.1 Gather OWP Environment Details
- AWS Account ID, preferred region (`us-east-1`)
- VPC name, private subnet name pattern
- Route53 hosted zone ID
- SSH key pair name
- Session Manager logging policy ARN

### 0.2 Cross-Account IAM Setup

**OWP side** — create a temporary migration role **if necessary**:
- Role name: `owp-benchmarkcat-migration-role` (EC2 trust policy)
- Policy 1 (source read): `s3:GetObject` and `s3:ListBucket` on `s3://fimc-data/benchmark/*` and `s3://fimc-data/hand_fim/test_cases/*`
- Policy 2 (dest write): `s3:PutObject`, `s3:GetObject`, `s3:ListBucket` on `s3://hv-fim-dev-stac/*` and `s3://hv-fim-dev-data/*`

**NGWPC side** — update `fimc-data` bucket policy to grant cross-account read access **if necessary**.

### 0.3 Verify Cross-Account Access
```bash
aws sts get-caller-identity
aws s3 ls s3://fimc-data/benchmark/ | head -5
aws s3 ls s3://fimc-data/benchmark/stac-bench-cat/ | head -5
```

### 0.4 Create Destination Buckets
```bash
aws s3 mb s3://hv-fim-dev-stac --region us-east-1
aws s3 mb s3://hv-fim-dev-data --region us-east-1
```

**Gate:** Do not proceed until cross-account S3 read is confirmed.

---

## Phase 1: Terraform Infrastructure

> **Machine: admin machine**

### 1.1 Clone Repository (**admin machine**)

The Terraform configuration and migration scripts are in the repo — clone it locally before proceeding.

```bash
git clone https://github.com/NGWPC/benchmarkcat.git ~/benchmarkcat -b owp-deployment
```

### 1.2 Create Configuration

Working dir: `deployment/terraform/`

Create `terraform.tfvars` (template in `deployment/terraform/TF_README.md`):
```hcl
environment        = "test"
aws_region         = "us-east-1"
api_name           = "benchmarkcat"
hosted_zone_id     = "<ZONE_ID>"
session_manager_logging_policy_arn = "<SSM_POLICY_ARN>"
vpc_name             = "<VPC_NAME>"
subnet_name_pattern  = "<SUBNET_PATTERN>*"
instance_type        = "t3.xlarge"
root_volume_size     = 100
enterprise_mode      = false
s3_read_paths        = ["hv-fim-dev-stac", "hv-fim-dev-data"]
s3_write_paths       = ["hv-fim-dev-stac/benchmark-stac/*", "hv-fim-dev-data/benchmark/*"]
backup_s3_uri        = "s3://hv-fim-dev-data/benchmark/backups/stac-db/"
stac_catalog_path    = "benchmark-stac/"
log_retention_days   = 7
# key_name = "your-aws-key-pair-name"  # Optional: required for SSH access
```

Create `backend.tf` for remote state (S3 backend recommended).

### 1.3 Deploy
```bash
cd ~/benchmarkcat/deployment/terraform
terraform init
terraform plan -var-file="terraform.tfvars"
terraform apply -var-file="terraform.tfvars"
```

Creates: Security group (8080/8082/8083 + SSH to VPC), IAM role with dynamic S3 policies, EC2 instance with bootstrap, Route53 A record, CloudWatch log group.

### 1.4 Verify Bootstrap (**admin machine**)

```bash
terraform output standalone_instance_ip

# Connect via SSH — requires key_name set in terraform.tfvars
# terraform output ssh_instructions prints the command with the correct IP
terraform output ssh_instructions
ssh -i /path/to/your-key.pem ubuntu@<standalone_instance_ip>

cat /var/log/benchmarkcat/bootstrap.log
/opt/benchmarkcat/deployment/health-check.sh
docker ps  # Expect: benchmarkcat-db, benchmarkcat-api, benchmarkcat-browser, benchmarkcat-asset-proxy
```

**Note:** The bootstrap generates utility scripts (`health-check.sh`, `backup-db.sh`, `restart-services.sh`) on the EC2 instance at `/opt/benchmarkcat/deployment/`. These are not present in the repository.

**Rollback:** `terraform destroy -var-file="terraform.tfvars"`

**State after Phase 1:** 4 containers running, empty database, API on 8082, Browser on 8080, proxy on 8083.

### 1.5 Clone Repository (**EC2 instance**)

```bash
sudo git clone https://github.com/NGWPC/benchmarkcat.git /opt/benchmarkcat/repo -b owp-deployment
```

Verify:
```bash
ls /opt/benchmarkcat/repo/deployment/scripts/
# Expected: load_catalog.py, rewrite_asset_urls.py, test_asset_proxy.sh, reset_database.sh, etc.
```

---

## Phase 2: S3 Migration

> **Machine: admin machine** — runs from your local workstation or any machine with AWS credentials. The EC2 instance does not exist yet (or has an empty DB); the OWP buckets must be populated before catalog loading.

Script: `deployment/s3_migration/migrate_s3.py`
Reference: `deployment/s3_migration/S3_README.md`

### 2.1 Dry Run
```bash
cd ~/benchmarkcat/deployment/s3_migration

python3 migrate_s3.py \
  --source-bucket fimc-data \
  --stac-bucket hv-fim-dev-stac \
  --stac-prefix benchmark-stac \
  --data-bucket hv-fim-dev-data \
  --data-prefix benchmark \
  --dry-run --verbose
```

Verify all 8 path mappings are displayed (`PATH_MAPPINGS` in `migrate_s3.py`):

| Collection | Source | Destination (under `hv-fim-dev-data/benchmark/`) |
|---|---|---|
| ble-collection | `benchmark/high_resolution_validation_data_ble` | `ble-collection/` |
| ripple-fim-collection | `benchmark/ripple_v0.11.x` | `ripple-fim-collection/` |
| hwm-collection | `benchmark/high_water_marks/usgs` | `hwm-collection/` |
| nws-fim-collection | `hand_fim/test_cases/nws_test_cases/validation_data_nws` | `nws-fim-collection/` |
| usgs-fim-collection | `hand_fim/test_cases/usgs_test_cases/validation_data_usgs` | `usgs-fim-collection/` |
| gfm-collection | `benchmark/rs/gfm` | `gfm-collection/` |
| iceye-collection | `benchmark/rs/iceye` | `iceye-collection/` |
| gfm-expanded-collection | `benchmark/rs/PI4` | `gfm-expanded-collection/` |
| STAC catalog | `benchmark/stac-bench-cat` | `hv-fim-dev-stac/benchmark-stac/` |

### 2.2 Execute Migration

```bash
# Download catalog + update HREFs + generate copy script
python3 migrate_s3.py \
  --source-bucket fimc-data \
  --stac-bucket hv-fim-dev-stac --stac-prefix benchmark-stac \
  --data-bucket hv-fim-dev-data --data-prefix benchmark \
  --generate-copy-commands --skip-upload

# Review sample HREF
cat ~/benchmark-catalog/dest_catalog/gfm-collection/items/*/item.json | jq '.assets[].href' | head -5
# Expected: s3://hv-fim-dev-data/benchmark/gfm-collection/...

# Review migration manifest
jq 'length' ~/benchmark-catalog/migration_manifest.json

# Copy assets (~8-12 hours)
~/benchmark-catalog/copy_assets.sh

# Monitor in another terminal:
watch -n 30 'aws s3 ls s3://hv-fim-dev-data/benchmark/ --recursive | wc -l'

# Upload updated catalog
python3 migrate_s3.py \
  --source-bucket fimc-data \
  --stac-bucket hv-fim-dev-stac --stac-prefix benchmark-stac \
  --data-bucket hv-fim-dev-data --data-prefix benchmark \
  --skip-download --skip-update
```

**Recovery:** `copy_assets.sh` is idempotent — re-run on failure, it skips existing files.

### 2.3 Verify Migration
```bash
aws s3 ls s3://hv-fim-dev-stac/benchmark-stac/ --recursive | wc -l    # ~23,000
aws s3 ls s3://hv-fim-dev-data/benchmark/ --recursive | wc -l
aws s3 cp s3://hv-fim-dev-stac/benchmark-stac/catalog.json - | jq '.'
aws s3 ls s3://hv-fim-dev-data/benchmark/                              # 8 collection dirs + shared-assets/
```

### 2.4 Enable S3 Versioning

```bash
aws s3api put-bucket-versioning \
  --bucket hv-fim-dev-stac \
  --versioning-configuration Status=Enabled

aws s3api put-bucket-versioning \
  --bucket hv-fim-dev-data \
  --versioning-configuration Status=Enabled
```

### 2.5 Enable Intelligent-Tiering on data bucket

Applies only to `hv-fim-dev-data` — the STAC catalog in `hv-fim-dev-stac` stays in STANDARD to avoid retrieval latency.

```bash
aws s3api put-bucket-intelligent-tiering-configuration \
  --bucket hv-fim-dev-data \
  --id data-tiering \
  --intelligent-tiering-configuration '{
    "Id": "data-tiering",
    "Status": "Enabled",
    "Filter": {"Prefix": "benchmark/"},
    # If desired, add/modify Tierings.
    # "Tierings": [
    #   {"Days": 90,  "AccessTier": "ARCHIVE_ACCESS"},
    #   {"Days": 180, "AccessTier": "DEEP_ARCHIVE_ACCESS"}
    # ]
  }'
```

**Gate:** Do not proceed until all of the following are confirmed:
- `hv-fim-dev-stac/benchmark-stac/` object count is ~23,000
- `hv-fim-dev-data/benchmark/` contains all 8 collection directories
- `catalog.json` HREFs reference `s3://hv-fim-dev-data/benchmark/...` (not `fimc-data`)
- EC2 bootstrap is healthy (Phase 1.4)

---

## Phase 3: Catalog Loading

> **Machine: EC2 instance** — SSH in (or use Session Manager) before running these steps. The database and API containers must already be healthy (Phase 1.4).

Script: `deployment/scripts/load_catalog.py`

### 3.1 Sync Catalog Locally
```bash
mkdir -p ~/stac-catalog
aws s3 sync s3://hv-fim-dev-stac/benchmark-stac/ ~/stac-catalog/ --exclude "*" --include "*.json"
```

Verify sync:
```bash
ls ~/stac-catalog/
# Expected: catalog.json + 8 collection directories

find ~/stac-catalog -name "collection.json" | wc -l   # 8
find ~/stac-catalog -name "*.json" ! -name "catalog.json" ! -name "collection.json" | wc -l  # ~23,000
```

### 3.2 Load to pgstac

Run in `tmux` or `screen` — if the terminal disconnects mid-run, verify completion via DB counts in 3.3.

```bash
export PGPASSWORD=$(sudo cat /opt/benchmarkcat/.db_password)

python3 /opt/benchmarkcat/repo/deployment/scripts/load_catalog.py \
  ~/stac-catalog --db-host localhost --db-password $PGPASSWORD --dry-run
```

Expected dry-run output:
```
Collections: 8
Items: ~23,000
[DRY RUN] No changes written
```

```bash
python3 /opt/benchmarkcat/repo/deployment/scripts/load_catalog.py \
  ~/stac-catalog --db-host localhost --db-password $PGPASSWORD
```

### 3.3 Verify
```bash
docker exec -i benchmarkcat-db psql -U pgstac -d stacdb -c \
  "SELECT COUNT(*) FROM pgstac.collections;"  # 8

docker exec -i benchmarkcat-db psql -U pgstac -d stacdb -c \
  "SELECT COUNT(*) FROM pgstac.items;"  # ~23,000

docker exec -i benchmarkcat-db psql -U pgstac -d stacdb -c \
  "SELECT collection, COUNT(*) FROM pgstac.items GROUP BY collection ORDER BY collection;"

curl http://localhost:8082/collections | jq '.collections | length'  # 8
```

**Rollback:** Reset database and re-load:
```bash
sudo bash /opt/benchmarkcat/repo/deployment/scripts/reset_database.sh --force
# Then repeat from 3.2.
```

---

## Phase 4: Asset URL Rewriting

> **Machine: EC2 instance** — continue from the same SSH session as Phase 3.

Script: `deployment/scripts/rewrite_asset_urls.py`

### 4.1 Prerequisite: max_locks_per_transaction

The rewrite triggers pgSTAC partition updates that require more locks than PostgreSQL's default allows. Without this you may see:

```
ERROR: out of shared memory
HINT: You might need to increase max_locks_per_transaction.
CONTEXT: SQL statement "REFRESH MATERIALIZED VIEW partitions"
```

Verify and apply before running the rewrite:
```bash
# Check current value (should be 256)
docker exec benchmarkcat-db psql -U pgstac -d stacdb -c \
  "SHOW max_locks_per_transaction;"

# If not 256, apply and restart
docker exec benchmarkcat-db psql -U pgstac -d stacdb -c \
  "ALTER SYSTEM SET max_locks_per_transaction = 256;"
docker restart benchmarkcat-db

until docker exec benchmarkcat-db pg_isready -U pgstac -d stacdb >/dev/null 2>&1; do sleep 2; done
echo "DB ready"
```

### 4.2 Verify Proxy

The asset-proxy service (`deployment/asset-proxy/app.py`) streams S3 content using IAM role credentials with Range request support for COG rendering.

```bash
sudo /opt/benchmarkcat/repo/deployment/scripts/test_asset_proxy.sh
```

Tests: proxy health endpoint, AWS credentials, sample asset query from DB, direct S3 access, proxy URL serving.

### 4.3 Rewrite
```bash
export HOST_IP=$(hostname -I | awk '{print $1}')
export PGPASSWORD=$(sudo cat /opt/benchmarkcat/.db_password)

sudo -E python3 /opt/benchmarkcat/repo/deployment/scripts/rewrite_asset_urls.py \
  --proxy-url http://${HOST_IP}:8083 \
  --db-host localhost --dry-run
```

Expected dry-run output: `Items needing rewrite: <N>` followed by `[DRY RUN]`.

```bash
sudo -E python3 /opt/benchmarkcat/repo/deployment/scripts/rewrite_asset_urls.py \
  --proxy-url http://${HOST_IP}:8083 \
  --db-host localhost
```

Transforms: `s3://hv-fim-dev-data/benchmark/...` → `http://<HOST_IP>:8083/s3/hv-fim-dev-data/benchmark/...`

Idempotent — re-running when nothing needs rewriting prints `Nothing to do.`

**Note:** Use private VPC IP for internal access, or domain name if DNS is configured for external users.

### 4.4 Verify
```bash
# Confirm no raw s3:// HREFs remain (should return 0)
docker exec -i benchmarkcat-db psql -U pgstac -d stacdb -c \
  "SELECT COUNT(*) FROM pgstac.items
   WHERE (content->'assets')::text LIKE '%s3://%';"

# Spot-check proxy URL format directly in pgstac
docker exec -i benchmarkcat-db psql -U pgstac -d stacdb -c \
  "SELECT content->'assets'->'thumbnail'->>'href'
   FROM pgstac.items WHERE content->'assets' ? 'thumbnail' LIMIT 3;"
# Expected: http://<HOST_IP>:8083/s3/hv-fim-dev-data/benchmark/...

curl -s "http://${HOST_IP}:8082/collections/gfm-collection/items?limit=1" | \
  jq '.features[0].assets[].href'
# All should show http://<HOST_IP>:8083/s3/hv-fim-dev-data/benchmark/...
```

---

## Phase 5: Post-Deployment

> **Machine: EC2 instance**

### 5.1 Update .env (if needed)

The `.env` file is generated during bootstrap at `/opt/benchmarkcat/deployment/.env`. If `S3_BUCKET` or `S3_CATALOG_PATH` are incorrect:
```bash
sed -i 's/^S3_BUCKET=.*/S3_BUCKET=hv-fim-dev-stac/' /opt/benchmarkcat/deployment/.env
sed -i 's/^S3_CATALOG_PATH=.*/S3_CATALOG_PATH=benchmark-stac\//' /opt/benchmarkcat/deployment/.env
sudo /opt/benchmarkcat/deployment/restart-services.sh
```

### 5.2 Initial Backup
```bash
sudo /opt/benchmarkcat/deployment/backup-db.sh
```

---

## Phase 6: Verification & Sign-Off

**Next:** work through `deployment/Verification_Checklist.md` to confirm the full deployment end-to-end. It covers:

- Bootstrap & service health
- STAC API endpoint validation
- Database integrity (item counts vs S3 catalog)
- Asset proxy & S3 access
- GDAL / raster access
- STAC Browser UI
- QGIS integration
- Performance benchmarking
- Monitoring & ops (systemd, backups, logs)
- Production readiness sign-off checklist

---

## Rollback Plan

If deployment fails:

1. Preserve logs from `/var/log/benchmarkcat/`
2. Infrastructure team destroys resources: `terraform destroy -var-file="terraform.tfvars"`
3. Clean S3 destination bucket if needed (optional)

---

## Troubleshooting

### Asset Proxy Returns 403

The proxy reads assets from `hv-fim-dev-data` using the EC2 instance role. A 403 means the instance role lacks read access — confirm `s3_read_paths` in `terraform.tfvars` includes `hv-fim-dev-data`.

Diagnose from inside the proxy container:
```bash
docker exec benchmarkcat-asset-proxy python3 -c "
import boto3
s3 = boto3.client('s3', region_name='us-east-1')
try:
    s3.head_object(Bucket='hv-fim-dev-data', Key='benchmark/gfm-collection/')
    print('OK')
except Exception as e:
    print('FAILED:', e)
"
```

### Item Links Point to Source Pipeline URLs

Item JSONs may have `self`, `collection`, `parent`, and `root` links pointing at the original ingest pipeline API. This is expected — pgSTAC discards these links on ingest and reconstructs them dynamically from the serving API URL. Links returned by the OWP API will correctly point at the OWP deployment. No action needed.

### STAC Browser — WebGL Map Not Rendering (Chrome)

If the OpenLayers map is blank in Chrome, WebGL may be disabled:

1. **Enable Hardware Acceleration:** Chrome Settings → System → turn on "Use graphics acceleration when available" → restart Chrome
2. **Enable WebGL flags:** go to `chrome://flags/#ignore-gpu-blocklist` → set "Override software rendering list" to Enabled → restart Chrome
3. Verify at `https://get.webgl.org/` — you should see a spinning cube

Alternatively, use Firefox.

---

## Operational Scripts

| Script | Path |
|--------|------|
| Health Check | `/opt/benchmarkcat/deployment/health-check.sh` |
| Restart Services | `/opt/benchmarkcat/deployment/restart-services.sh` |
| Stop Services | `/opt/benchmarkcat/deployment/stop-services.sh` |
| Start Services | `/opt/benchmarkcat/deployment/start-services.sh` |
| View Logs | `/opt/benchmarkcat/deployment/view-logs.sh` |
| Backup Database | `/opt/benchmarkcat/deployment/backup-db.sh` |
| Test Asset Proxy | `deployment/scripts/test_asset_proxy.sh` |
| Rewrite Asset URLs | `deployment/scripts/rewrite_asset_urls.py` |

---

## Key Files

| File | Role |
|---|---|
| `deployment/terraform/main.tf` | IAM, SG, EC2, ALB resources |
| `deployment/terraform/variables.tf` | All configurable inputs |
| `deployment/terraform/data.tf` | VPC, subnet, AMI lookups |
| `deployment/terraform/outputs.tf` | API URL, instance IP, SSH instructions |
| `deployment/terraform/templates/user_data_standalone.sh.tpl` | Bootstrap template (used by Terraform) |
| `deployment/terraform/user-data/owp-bootstrap.sh` | Bootstrap script (manual execution) |
| `deployment/s3_migration/migrate_s3.py` | Core migration with PATH_MAPPINGS |
| `deployment/s3_migration/S3_README.md` | Detailed migration guide |
| `deployment/scripts/load_catalog.py` | pgstac catalog loader |
| `deployment/scripts/rewrite_asset_urls.py` | S3 → proxy URL rewriter |
| `deployment/scripts/test_asset_proxy.sh` | Proxy validation |
| `deployment/scripts/reset_database.sh` | Database reset utility |
| `deployment/asset-proxy/app.py` | FastAPI S3 streaming proxy |
| `deployment/Deployment_Strategy_Overview_OWP.md` | Architecture & cost analysis |

# BenchmarkCat STAC Scripts

Utility scripts for managing STAC catalogs and deployment operations.

---

## Available Scripts

### load_catalog.py
Loads STAC catalog (catalog.json, collections, items) into pgstac database.

See more details and steps in the Deployment_Runbook.md - Phase 3

**Installation:**
```bash
# After bootstrap - clone repository
cd /opt/benchmarkcat
repo_path="/opt/benchmarkcat/repo"
git clone https://github.com/NOAA-OWP/benchmarkcat.git $repo_path

# For local development - The pip packages are likely already there as it is in the tpl scripts.
# But it does not hurt to run it again
pip install -r "${repo_path}/deployment/scripts/requirements.txt"
```

**Quick Start:**
```bash
# get the postgres_password
grep POSTGRES_PASSWORD /var/log/benchmarkcat/bootstrap.log
# Another way to do this is: 

# Dry run (preview)
mkdir -p /stac-catalog  # or wherever
cd "${repo_path}/deployment/scripts"
python3 load_catalog.py /path/to/catalog --db-host localhost --db-password < password from above > --dry-run

# Load catalog
python3 load_catalog.py /path/to/catalog  --db-host localhost --db-password < password from above >

# Verify
docker exec -i benchmarkcat-db psql -U pgstac -d stacdb -c \
  "SELECT collection, COUNT(*) FROM pgstac.items GROUP BY collection;"
```

**Common Options:**
```bash
--dry-run              # Preview without loading
--batch-size 200       # Adjust performance (default: 100)
--db-host localhost    # Custom database host (default: database)
--db-port 5432         # Custom port
--db-user pgstac       # Database user
--db-name stacdb       # Database name
```

**Password Configuration:**
- Auto-detected from `/opt/benchmarkcat/.db_password` (bootstrap deployment)
- Environment variable: `export PGPASSWORD=password`
- Command line: `--db-password password`
- ~/.pgpass file: `hostname:port:database:username:password`

**Expected Output:**
```
Step 1: Testing database connection...
  Connected to database. pgstac version: 0.8.6

Step 2: Loading catalog metadata...
  Found catalog: benchmark-catalog

Step 3: Discovering collections...
  Found 8 collection(s)

Step 4: Loading collections and items...
  Loaded 1250 items in 12.3s (101.6 items/sec)

SUMMARY
Collections loaded: 8
Items loaded: ~22,800
Items failed: 0
```

**Success Criteria:**
- Collections load without errors
- Loading rate > 10 items/second
- All items accessible via SQL and API

**Troubleshooting:**
```bash
# Database not ready
docker ps | grep benchmarkcat-db
docker exec benchmarkcat-db pg_isready -U pgstac -d stacdb

# Check password
# Note: This might not exist if the password was overridden in variables.tf
cat /opt/benchmarkcat/.db_password

# View logs
docker logs benchmarkcat-db
```

---

### test_asset_proxy.sh
Tests the asset-proxy service functionality and verifies it can access S3 assets.

**When to Use:**
- Before running rewrite_asset_urls.py
- Troubleshooting asset display issues in STAC Browser
- Verifying asset-proxy service configuration
- Testing IAM role S3 permissions

**Usage:**
```bash
# terminal is still at ${repo_path}/deployment/scripts"
sudo ./test_asset_proxy.sh
```

**What it does:**
1. Checks asset-proxy service health endpoint
2. Verifies AWS credentials in the container
3. Queries database for sample asset
4. Tests S3 access through the proxy
5. Provides next steps based on results

**Expected Output:**
```
========================================
Asset-Proxy Service Test
========================================

Test 1: Asset-Proxy Health Check
---------------------------------
✓ Asset-proxy is running
{"status":"ok"}

Test 2: AWS Credentials Check
------------------------------
✓ AWS credentials working inside container
Region: us-east-1
Identity: {...}

Test 3: Get Sample Asset from Database
---------------------------------------
Sample asset from database:
 thumbnail | s3://bucket/data/collection/thumbnail.png

Testing through proxy...
✓ SUCCESS - Asset accessible (HTTP 200)
```

**Important:**
- Asset-proxy container must be running
- Database must have items loaded
- IAM role must have S3 read permissions

---

### rewrite_asset_urls.py
Rewrites S3 asset URLs in the database to use the asset-proxy service for browser access.

**When to Use:**
- Thumbnails not showing in STAC Browser
- Leaflet map not displaying GeoTIFF files
- Assets are stored in private/requester-pays S3 buckets
- After loading a catalog with S3 URIs (s3://bucket/path)

**Problem:**
STAC items with S3 asset URLs (like `s3://bucket/path/thumbnail.png` or direct S3 HTTPS URLs) cannot be accessed by the browser due to:
- Private bucket permissions
- Requester-pays requirements
- CORS restrictions

**Solution:**
The asset-proxy service (running on port 8083) generates presigned URLs with proper authentication. This script rewrites asset hrefs to use the proxy.

**Prerequisites:**
```bash
# Test asset-proxy first
sudo ./test_asset_proxy.sh
```

**Usage:**
```bash
# Get database password
# Note: this password value may not exist if doing a test with the overwridden password
export PGPASSWORD=$(grep POSTGRES_PASSWORD /var/log/benchmarkcat/bootstrap.log | sed 's/.*POSTGRES_PASSWORD=//')

# Get host IP for VPC access
export HOST_IP=$(hostname -I | awk '{print $1}')

####################
# You can use one or more of these below. They are samples
# Dry run (preview changes)  (8083 ?)
python3 rewrite_asset_urls.py --proxy-url http://${HOST_IP}:8083 --db-host localhost --db-password $PGPASSWORD --dry-run

# Apply changes for VPC access
python3 rewrite_asset_urls.py --proxy-url http://${HOST_IP}:8083 --db-host localhost --db-password $PGPASSWORD

# For local-only access use localhost
python3 rewrite_asset_urls.py --proxy-url http://localhost:8083 --db-host localhost --db-password $PGPASSWORD

# Or use external domain name
python3 rewrite_asset_urls.py --proxy-url http://your-domain.com:8083 --db-host localhost --db-password $PGPASSWORD
####################
```

**What it does:**
1. Scans all items in pgstac database
2. Identifies S3 asset URLs (s3:// URIs or S3 HTTPS URLs)
3. Rewrites them to use asset-proxy format: `http://proxy-url/s3/bucket/path`
4. Updates items in database

**Examples:**

Original asset href:
```
s3://test-owp-benchmark-data/data/ble-collection/08020303-ble/thumbnail.png
```

Rewritten href:
```
http://localhost:8083/s3/test-owp-benchmark-data/data/ble-collection/08020303-ble/thumbnail.png
```

**Expected Output:**
```
Item: ble-collection/08020303-ble_100yr
  thumbnail:
    OLD: s3://test-owp-benchmark-data/data/ble-collection/08020303-ble/thumbnail.png
    NEW: http://localhost:8083/s3/test-owp-benchmark-data/data/ble-collection/08020303-ble/thumbnail.png
    ✓ Updated

SUMMARY
Total items:         156
Items with assets:   156
Items updated:       156
Assets rewritten:    624
Items failed:        0
```

**Verification:**
```bash
# Check asset-proxy is running
curl http://${HOST_IP}:8083/health

# Test an asset URL (should redirect to presigned S3 URL)
curl -I http://${HOST_IP}:8083/s3/test-owp-benchmark-data/data/ble-collection/08020303-ble/thumbnail.png

# Verify in STAC Browser
# Navigate to http://localhost:8080 and check thumbnails/maps display
```

**Important:**
- Run this AFTER loading your catalog with load_catalog.py
- Use the same proxy URL that browsers will access (localhost for local, domain or Private IPv4 for remote)
- Asset-proxy service must be running (part of docker-compose stack)
- IAM role on EC2 instance must have S3 read access

---

### reset_database.sh
Completely resets the pgstac database to load a new STAC catalog.

**When to Use:**
- Loading a different STAC catalog
- Database corruption or initialization errors
- Starting fresh after testing

**Usage:**
```bash
# Interactive mode (asks for confirmation)
./reset_database.sh

# Force mode (no confirmation)
./reset_database.sh --force
```

**What it does:**
1. Stops all BenchmarkCat services
2. Removes database volume/data (deletes all collections and items)
3. Restarts services with fresh database
4. Verifies pgstac initialization
5. Shows empty database stats

**Important:**
- This script DELETES ALL DATA in the database
- Make a backup first if needed
- After reset, use `load_catalog.py` to load new catalog

**Example Workflow:**
```bash
# Reset database
sudo ./reset_database.sh --force

# Load new catalog
python3 load_catalog.py /path/to/new-catalog

# Verify
curl http://localhost:8082/collections
```

---

## Common Patterns

### Database Connection
All scripts use standard PostgreSQL connection parameters:
- `--db-host` (default: `database` for Docker, `localhost` for local)
- `--db-port` (default: `5432`)
- `--db-user` (default: `pgstac`)
- `--db-name` (default: `stacdb`)

Password auto-detected from `/opt/benchmarkcat/.db_password` or `PGPASSWORD` env var.

### Dry Run Mode
Scripts that modify data should support `--dry-run` for preview.

### Batch Processing
Scripts processing large datasets should support `--batch-size` for performance tuning.

### Logging
Scripts output progress to stdout, errors to stderr.

---

## Requirements

See `requirements.txt` for Python dependencies.

Bootstrap deployment automatically installs:
- Python 3
- pip3
- psycopg2-binary

---

## References

- **Deployment Guide:** `../Benchmark STAC Deployment.txt`
- **Bootstrap Script:** `../terraform/templates/user_data_standalone.sh.tpl`
- **S3 Migration:** `../s3_migration/README.md`

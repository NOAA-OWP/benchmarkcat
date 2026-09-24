# BenchmarkCat STAC: Verification Checklist

Use this checklist after deployment is complete to confirm the system is production-ready.

---

## 1. Bootstrap & Service Health

- [ ] All 4 containers running: `benchmarkcat-db`, `benchmarkcat-api`, `benchmarkcat-browser`, `benchmarkcat-asset-proxy`
- [ ] Health check passes: `/opt/benchmarkcat/deployment/health-check.sh`
- [ ] STAC API responds: `curl http://<DOMAIN_NAME or HOST_IP>:8082/`
- [ ] STAC Browser loads: `curl http://<DOMAIN_NAME or HOST_IP>:8080/`

---

## 2. STAC API Endpoint Validation (`:8082`)

- [ ] Root endpoint returns JSON with title/metadata (`/`)
- [ ] Conformance endpoint responds (`/conformance`)
- [ ] Collections list returns all expected collections (`/collections`)
- [ ] Search works with limit (`/search?limit=10`)
- [ ] Search works with bbox filter and returns results under 500ms

---

## 3. Database Integrity & Catalog Loading

Collection count matches expected:
```sql
docker exec benchmarkcat-db psql -U pgstac -d stacdb -c "SELECT COUNT(*) FROM pgstac.collections;"
```

Item counts per collection are correct:
```sql
docker exec benchmarkcat-db psql -U pgstac -d stacdb -c "SELECT collection, COUNT(*) FROM pgstac.items GROUP BY collection;"
```

Total item count matches source catalog on S3:
```sql
docker exec benchmarkcat-db psql -U pgstac -d stacdb -c "SELECT COUNT(*) FROM pgstac.items;"
```

Compare DB item counts against S3 STAC catalog per collection:
```bash
for col in ble-collection ripple-fim-collection hwm-collection nws-fim-collection usgs-fim-collection gfm-collection iceye-collection gfm-expanded-collection; do
  s3_count=$(aws s3 ls s3://hv-fim-dev-stac/benchmark-stac/$col/ --recursive | grep '\.json$' | grep -v 'collection.json' | wc -l)
  db_count=$(docker exec benchmarkcat-db psql -U pgstac -d stacdb -t -A -c "SELECT COUNT(*) FROM pgstac.items WHERE collection='$col';")
  echo "$col: S3=$s3_count DB=$db_count $([ "$s3_count" -eq "$db_count" ] && echo 'OK' || echo 'MISMATCH')"
done
```

If mismatches found, re-run `load_catalog.py` with `--batch-size 1` to isolate failures.

---

## 4. Asset Proxy & S3 Access

Run `deployment/scripts/test_asset_proxy.sh` which covers:

- [ ] Asset-proxy health endpoint (`http://<DOMAIN_NAME or HOST_IP>:8083/health`)
- [ ] IAM instance profile / AWS credentials are valid (STS caller identity)
- [ ] Sample assets queryable from database
- [ ] Direct S3 access works via IAM role (`aws s3 ls s3://hv-fim-dev-data/benchmark/`)
- [ ] Proxy URL serves assets with correct Content-Type

---

## 5. GDAL / Raster Access

Get a sample asset URL from the API:
```bash
SAMPLE_URL=$(curl -s "http://<DOMAIN_NAME or HOST_IP>:8082/collections/gfm-collection/items?limit=1" | \
  jq -r '.features[0].assets | .[keys[0]].href')
```

Verify proxy supports HTTP Range requests (required for COG rendering):
```bash
curl -s -D - -H "Range: bytes=0-1023" "$SAMPLE_URL" -o /dev/null | grep -i "HTTP\|content-range\|content-length"
# Expect: HTTP/1.1 206 Partial Content with content-length & content-range header
```

Verify a full asset download completes without error:
```bash
curl -s -o /dev/null -w "%{http_code} %{size_download} bytes\n" "$SAMPLE_URL"
# Expect: 200 with non-zero size
```

GDAL read via proxy (from a workstation with GDAL installed):
```bash
gdalinfo /vsicurl/$SAMPLE_URL
# Expect: raster metadata (driver, size, CRS, bands) with no errors
```

GDAL read via S3 directly (from a workstation with GDAL and AWS credentials):
```bash
gdalinfo /vsis3/hv-fim-dev-data/benchmark/gfm-collection/<item-id>/<asset-filename>.tif
# Expect: same raster metadata as above
```

---

## 6. STAC Browser UI

- [ ] All collections visible and browsable
- [ ] Individual items display correctly with metadata
- [ ] Asset links resolve (either via proxy or direct S3)

---

## 7. QGIS Integration

> From a workstation with QGIS installed.

Install the QGIS STAC plugin: **Plugins > Manage and Install Plugins > search "STAC API Browser"**

If plugin fails with a pydantic `BaseSettings` error, fix with:
```bash
pip install pydantic-settings
```

Then patch the bundled planetary_computer settings file:

Edit `~/.local/share/QGIS/QGIS3/profiles/default/python/plugins/qgis_stac/lib/planetary_computer/settings.py`

```python
# Change:
class Settings(pydantic.BaseSettings):

# To:
from pydantic_settings import BaseSettings
class Settings(BaseSettings):
```

Restart QGIS after patching.

Reference: [Intro to STAC API Browser QGIS Plugin](https://stacspec.org/en/tutorials/2-intro-to-stac-api-browser-qgis-plugin)

**Steps:**

1. Launch the STAC API Browser Plugin: **Plugins > STAC API Browser Plugin > Open STAC API Browser**
2. Add connection: **New Connection > URL:** `http://<DOMAIN_NAME or HOST_IP>:8082`
3. Search on Extent: **Draw on Canvas**
4. Select and add a footprint
5. View footprint on QGIS map
6. Browse to a collection (e.g. `gfm-collection`), select an item with a raster asset
7. Click **Add Layer** to load the raster into the map canvas
8. Verify: layer renders with correct spatial extent and pixel values (use Identify tool to spot-check)

---

## 8. Performance

Install Apache Bench if not present:
```bash
sudo apt-get install -y apache2-utils
```

Concurrent load test on collections endpoint:
```bash
ab -n 1000 -c 10 http://<DOMAIN_NAME or HOST_IP>:8082/collections
# Check: "Failed requests: 0" and "Time per request" (mean) < 500ms
```

Concurrent load test on search endpoint:
```bash
ab -n 500 -c 10 "http://<DOMAIN_NAME or HOST_IP>:8082/search?limit=10"
# Check: "Failed requests: 0" and "Time per request" (mean) < 500ms
```

Concurrent load test with bbox filter:
```bash
ab -n 500 -c 10 "http://<DOMAIN_NAME or HOST_IP>:8082/search?bbox=-90,30,-80,40&limit=10"
# Check: "Failed requests: 0" and "Time per request" (mean) < 500ms
```

---

## 9. Monitoring & Ops

Systemd service enabled and active:
```bash
systemctl is-enabled benchmarkcat
# Expect: "enabled"

systemctl is-active benchmarkcat
# Expect: "active"
```

Backup cron job — verify Sunday 2 AM schedule exists:
```bash
crontab -l | grep backup-db.sh
# Expect: line containing "0 2 * * 0" and backup-db.sh
```

Manual backup test:
```bash
sudo /opt/benchmarkcat/deployment/backup-db.sh
ls -lh /opt/backups/postgres/ | tail -1
# Expect: recent non-empty .sql.gz file
```

S3 backup upload (if configured):
```bash
aws s3 ls s3://hv-fim-dev-data/benchmark/backups/stac-db/
# Expect: backup files listed with recent timestamps
```

Logging — verify log directories and recent writes:
```bash
ls -la /var/log/benchmarkcat/
tail -5 /var/log/benchmarkcat/bootstrap.log
# Expect: log directory exists with recent log files
```

Docker logs — verify no errors in container logs:
```bash
docker logs --tail 20 benchmarkcat-api 2>&1 | grep -i error
docker logs --tail 20 benchmarkcat-db 2>&1 | grep -i error
# Expect: no error lines (empty output is good)
```

System resources — disk and memory:
```bash
df -h /
free -h
# Expect: adequate free disk space and memory
```

DNS resolution (if domain configured):
```bash
DOMAIN=$(docker inspect benchmarkcat-browser --format '{{range .Config.Env}}{{println .}}{{end}}' | grep SB_catalogUrl | cut -d/ -f3 | cut -d: -f1)
nslookup $DOMAIN
# Expect: domain resolves to the server's IP
```

---

## 10. Component Test Matrix

| Test | Description | Pass Criteria |
|------|-------------|---------------|
| TC2.1 | **Database Health** — verify pgstac extension installed, check connectivity from API container | pgstac extension present, API container connects without error |
| TC2.2 | **STAC API Endpoints** — test `/`, `/collections`, `/search`; verify HTTP 200, valid GeoJSON, correct S3 HREFs | All endpoints return HTTP 200 with valid responses |
| TC2.3 | **STAC Browser** — access `http://<ec2-ip>:8080`; verify collections display, items browsable, assets accessible | Collections visible, items load, assets render |
| TC2.4 | **GDAL VSI S3 Access** — test direct S3 asset access via GDAL Virtual File System; verify no auth errors | `gdalinfo /vsis3/...` returns raster metadata |
| TC2.5 | **Asset Proxy Service** — run `test_asset_proxy.sh`; verify proxy returns S3 assets with Range header support | Script passes, HTTP 206 on range request |

---

## 11. Production Readiness Sign-Off

**Infrastructure**
- [ ] EC2 instance provisioned with correct IAM role
- [ ] Security groups configured (SSH restricted, ports 8080/8082/8083 accessible)
- [ ] Elastic IP or DNS configured (optional)

**Migration**
- [ ] S3 migration completed (~2.08 TB transferred)
- [ ] Catalog structure verified (`hv-fim-dev-stac/benchmark-stac/` and `hv-fim-dev-data/benchmark/`)
- [ ] Asset HREFs updated to new S3 bucket

**Application**
- [ ] Docker Compose stack running (4 containers)
- [ ] PostgreSQL initialized with pgstac
- [ ] STAC API responding on port 8082
- [ ] STAC Browser accessible on port 8080
- [ ] Asset-proxy service running on port 8083

**Testing**
- [ ] All component tests passed (TC2.1 – TC2.5)
- [ ] Health check script passes
- [ ] External access verified
- [ ] GDAL VSI S3 access confirmed
- [ ] Asset-proxy test passes
- [ ] Asset URLs rewritten to use proxy

**Operations**
- [ ] Systemd service enabled for auto-start
- [ ] Automated backups scheduled and tested
- [ ] Documentation updated with actual IPs/endpoints

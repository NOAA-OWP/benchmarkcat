#!/bin/bash
################################################################################
# Reset BenchmarkCat Database for New Catalog Load
#
# Purpose: Completely reset the pgstac database to load a new STAC catalog
# Usage: ./reset_database.sh [--force]
################################################################################

set -euo pipefail

INSTALL_DIR="/opt/benchmarkcat"
DEPLOYMENT_DIR="${INSTALL_DIR}/deployment"

# Check if running on EC2 or locally
if [ -d "$DEPLOYMENT_DIR" ]; then
    COMPOSE_DIR="$DEPLOYMENT_DIR"
else
    # Assume we're running from the repo directory
    COMPOSE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/deployment"
fi

# Check for --force flag
FORCE=false
if [ "${1:-}" = "--force" ]; then
    FORCE=true
fi

echo "========================================"
echo "BenchmarkCat Database Reset"
echo "========================================"
echo ""

# Confirm action unless --force is used
if [ "$FORCE" = false ]; then
    echo "WARNING: This will DELETE all data in the pgstac database!"
    echo ""
    read -p "Are you sure you want to continue? (yes/no): " confirm

    if [ "$confirm" != "yes" ]; then
        echo "Aborted."
        exit 0
    fi
fi

echo ""
echo "[$(date)] Starting database reset..."
echo ""

# Step 1: Stop the services
echo "[$(date)] Step 1: Stopping BenchmarkCat services..."
cd "$COMPOSE_DIR" || cd "$(dirname "${BASH_SOURCE[0]}")/../deployment"
docker-compose down
echo "[$(date)] Services stopped"
echo ""

# Step 2: Remove database volume/data
echo "[$(date)] Step 2: Removing database data..."

# Check if using named volume or bind mount
if docker volume ls | grep -q "pgstac-data"; then
    echo "[$(date)] Removing named volume: pgstac-data"
    docker volume rm pgstac-data 2>/dev/null || true
else
    # Using bind mount (./pgdata)
    if [ -d "$COMPOSE_DIR/pgdata" ]; then
        echo "[$(date)] Removing bind mount data: $COMPOSE_DIR/pgdata"
        rm -rf "$COMPOSE_DIR/pgdata"
    elif [ -d "$(dirname "${BASH_SOURCE[0]}")/../deployment/pgdata" ]; then
        echo "[$(date)] Removing bind mount data: $(dirname "${BASH_SOURCE[0]}")/../deployment/pgdata"
        rm -rf "$(dirname "${BASH_SOURCE[0]}")/../deployment/pgdata"
    fi
fi

echo "[$(date)] Database data removed"
echo ""

# Step 3: Restart services (database will auto-initialize)
echo "[$(date)] Step 3: Starting services with fresh database..."
docker-compose up -d
echo ""

# Step 4: Wait for database initialization
echo "[$(date)] Step 4: Waiting for database initialization..."
echo "[$(date)] This may take 30-60 seconds..."
echo ""

# Wait for database to be ready
MAX_WAIT=120
WAIT_COUNT=0
while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    if docker exec benchmarkcat-db pg_isready -U pgstac -d stacdb >/dev/null 2>&1; then
        echo "[$(date)] Database is ready"
        break
    fi
    sleep 2
    WAIT_COUNT=$((WAIT_COUNT + 2))

    # Print progress every 10 seconds
    if [ $((WAIT_COUNT % 10)) -eq 0 ]; then
        echo "[$(date)] Still waiting... ($WAIT_COUNT/${MAX_WAIT}s)"
    fi
done

if [ $WAIT_COUNT -ge $MAX_WAIT ]; then
    echo "[$(date)] ERROR: Database failed to start within ${MAX_WAIT} seconds"
    echo "[$(date)] Check logs: docker logs benchmarkcat-db"
    exit 1
fi

echo ""

# Step 5: Verify pgstac installation
echo "[$(date)] Step 5: Verifying pgstac installation..."
if docker exec benchmarkcat-db psql -U pgstac -d stacdb -c "\dx" | grep -q "pgstac"; then
    echo "[$(date)] pgstac extension verified"
elif docker exec benchmarkcat-db psql -U pgstac -d stacdb -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'pgstac';" | grep -q "pgstac"; then
    echo "[$(date)] pgstac schema verified"
else
    echo "[$(date)] WARNING: pgstac not found in database"
    echo "[$(date)] Check logs: docker logs benchmarkcat-db"
    exit 1
fi

echo ""

# Step 6: Show database stats
echo "[$(date)] Step 6: Database statistics (should be empty)..."
docker exec benchmarkcat-db psql -U pgstac -d stacdb -c "SELECT COUNT(*) as collection_count FROM pgstac.collections;" 2>/dev/null || echo "No collections table yet"
docker exec benchmarkcat-db psql -U pgstac -d stacdb -c "SELECT COUNT(*) as item_count FROM pgstac.items;" 2>/dev/null || echo "No items table yet"

echo ""
echo "========================================"
echo "Database Reset Complete!"
echo "========================================"
echo ""
echo "The database is now empty and ready to load a new catalog."
echo ""
echo "Next steps:"
echo "  1. Load your STAC catalog:"
echo "     python3 $INSTALL_DIR/repo/deployment/scripts/load_catalog.py /path/to/catalog"
echo ""
echo "  2. Verify the load:"
echo "     curl http://localhost:8082/collections"
echo ""

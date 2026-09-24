#!/usr/bin/env python3
"""
Load STAC catalog into pgstac database.

This script loads a local STAC catalog (catalog.json, collections, and items)
into a pgstac database. It should be run after bootstrap and database verification.

Usage:
    # Run from EC2/Docker after bootstrap (defaults to 'database' service)
    python3 load_catalog.py /path/to/catalog/directory

    # With custom database host (e.g., for local testing)
    python3 load_catalog.py /path/to/catalog/directory --db-host localhost

    # Dry run to see what would be loaded
    python3 load_catalog.py /path/to/catalog/directory --dry-run
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import execute_values


def test_database_connection(
    host: str, port: int, user: str, password: str, database: str
) -> tuple[bool, str]:
    """
    Test connection to PostgreSQL database and verify pgstac extension.

    Args:
        host: Database host
        port: Database port
        user: Database user
        password: Database password
        database: Database name

    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        conn = psycopg2.connect(
            host=host, port=port, user=user, password=password, database=database, connect_timeout=10
        )
        cursor = conn.cursor()

        # Check if pgstac extension or schema is installed
        # Some versions install as extension, others as schema
        cursor.execute("SELECT extname, extversion FROM pg_extension WHERE extname = 'pgstac';")
        ext_result = cursor.fetchone()

        if ext_result:
            ext_name, ext_version = ext_result
            cursor.close()
            conn.close()
            return True, f"Connected to database. pgstac version: {ext_version}"

        # Check for pgstac schema (newer versions)
        cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'pgstac';")
        schema_result = cursor.fetchone()

        # Verify pgstac functions exist
        if schema_result:
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.routines "
                "WHERE routine_schema = 'pgstac' AND routine_name IN ('upsert_collection', 'upsert_items');"
            )
            func_count = cursor.fetchone()[0]
            cursor.close()
            conn.close()

            if func_count == 2:
                return True, "Connected to database. pgstac schema installed with required functions"
            else:
                return False, "Connected to database but pgstac functions not found!"
        else:
            cursor.close()
            conn.close()
            return False, "Connected to database but pgstac extension/schema not found!"

    except psycopg2.OperationalError as e:
        return False, f"Database connection failed: {e}"
    except Exception as e:
        return False, f"Unexpected error testing database: {e}"


def load_json_file(file_path: Path) -> dict[str, Any]:
    """
    Load and parse a JSON file.

    Args:
        file_path: Path to JSON file

    Returns:
        Parsed JSON as dictionary
    """
    with open(file_path, "r") as f:
        return json.load(f)


def find_catalog_file(directory: Path) -> Path | None:
    """
    Find catalog.json in the given directory.

    Args:
        directory: Directory to search

    Returns:
        Path to catalog.json or None if not found
    """
    catalog_path = directory / "catalog.json"
    if catalog_path.exists():
        return catalog_path
    return None


def find_collection_files(directory: Path) -> list[Path]:
    """
    Find all collection.json files in directory and subdirectories.

    Args:
        directory: Root directory to search

    Returns:
        List of paths to collection.json files
    """
    collection_files = []

    # Look for collection.json files in subdirectories
    for path in directory.rglob("collection.json"):
        collection_files.append(path)

    return sorted(collection_files)


def find_item_files(collection_dir: Path) -> list[Path]:
    """
    Find all item JSON files in a collection directory.

    Args:
        collection_dir: Collection directory path

    Returns:
        List of paths to item JSON files
    """
    item_files = []

    # Look for JSON files that are not collection.json
    for json_file in collection_dir.rglob("*.json"):
        if json_file.name != "collection.json" and json_file.name != "catalog.json":
            item_files.append(json_file)

    return sorted(item_files)


def load_collection_to_pgstac(
    conn: psycopg2.extensions.connection, collection_data: dict[str, Any]
) -> tuple[bool, str]:
    """
    Load a STAC collection into pgstac using the pgstac.create_collection function.

    Args:
        conn: Database connection
        collection_data: Collection JSON data

    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        cursor = conn.cursor()

        # Use pgstac's upsert_collection function
        cursor.execute(
            "SELECT pgstac.upsert_collection(%s);",
            (json.dumps(collection_data),)
        )

        conn.commit()
        cursor.close()

        collection_id = collection_data.get("id", "unknown")
        return True, f"Loaded collection: {collection_id}"

    except Exception as e:
        conn.rollback()
        return False, f"Failed to load collection: {e}"


def load_items_to_pgstac(
    conn: psycopg2.extensions.connection, items: list[dict[str, Any]], batch_size: int = 100
) -> tuple[int, int, list[str]]:
    """
    Load STAC items into pgstac in batches.

    Args:
        conn: Database connection
        items: List of item dictionaries
        batch_size: Number of items to load per batch

    Returns:
        Tuple of (successful_count, failed_count, error_messages)
    """
    successful = 0
    failed = 0
    errors = []

    cursor = conn.cursor()

    # Process items in batches
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]

        try:
            # Use pgstac's upsert_items function with JSONB array
            items_json = json.dumps(batch)
            cursor.execute("SELECT pgstac.upsert_items(%s);", (items_json,))
            conn.commit()

            successful += len(batch)

        except Exception as e:
            conn.rollback()
            failed += len(batch)
            error_msg = f"Batch {i // batch_size + 1} failed: {e}"
            errors.append(error_msg)
            print(f"  ERROR: {error_msg}")

    cursor.close()
    return successful, failed, errors


def get_database_stats(conn: psycopg2.extensions.connection) -> dict[str, Any]:
    """
    Get statistics about loaded data from pgstac.

    Args:
        conn: Database connection

    Returns:
        Dictionary with statistics
    """
    cursor = conn.cursor()

    # Get collection count
    cursor.execute("SELECT COUNT(*) FROM pgstac.collections;")
    collection_count = cursor.fetchone()[0]

    # Get item count by collection
    cursor.execute("""
        SELECT collection, COUNT(*)
        FROM pgstac.items
        GROUP BY collection
        ORDER BY collection;
    """)
    items_by_collection = cursor.fetchall()

    # Get total items
    cursor.execute("SELECT COUNT(*) FROM pgstac.items;")
    total_items = cursor.fetchone()[0]

    cursor.close()

    return {
        "collections": collection_count,
        "total_items": total_items,
        "items_by_collection": items_by_collection
    }


def load_catalog(
    catalog_dir: Path,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    batch_size: int = 100,
    dry_run: bool = False,
) -> int:
    """
    Load entire STAC catalog into pgstac.

    Args:
        catalog_dir: Directory containing catalog.json
        host: Database host
        port: Database port
        user: Database user
        password: Database password
        database: Database name
        batch_size: Items per batch for loading
        dry_run: If True, only show what would be loaded

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    print("=" * 80)
    print("BenchmarkCat STAC Catalog Loader")
    print("=" * 80)
    print()

    # Step 1: Test database connection
    print("Step 1: Testing database connection...")
    success, message = test_database_connection(host, port, user, password, database)
    print(f"  {message}")

    if not success:
        print("\nFATAL: Database connection test failed!")
        return 1

    print()

    if dry_run:
        print("DRY RUN MODE: No data will be loaded to database")
        print()

    # Step 2: Find catalog.json
    print("Step 2: Loading catalog metadata...")
    catalog_file = find_catalog_file(catalog_dir)

    if not catalog_file:
        print(f"  ERROR: catalog.json not found in {catalog_dir}")
        return 1

    catalog_data = load_json_file(catalog_file)
    print(f"  Found catalog: {catalog_data.get('id', 'unknown')}")
    print(f"  Description: {catalog_data.get('description', 'N/A')}")
    print()

    # Step 3: Find all collections
    print("Step 3: Discovering collections...")
    collection_files = find_collection_files(catalog_dir)
    print(f"  Found {len(collection_files)} collection(s)")
    print()

    if len(collection_files) == 0:
        print("  WARNING: No collections found!")
        return 0

    # Connect to database for loading
    if not dry_run:
        try:
            conn = psycopg2.connect(
                host=host, port=port, user=user, password=password, database=database
            )
        except Exception as e:
            print(f"FATAL: Failed to connect to database: {e}")
            return 1

    # Step 4: Load collections and their items
    print("Step 4: Loading collections and items...")
    print()

    total_items_loaded = 0
    total_items_failed = 0
    total_items_skipped = 0
    collections_loaded = 0

    for collection_file in collection_files:
        collection_dir = collection_file.parent
        collection_data = load_json_file(collection_file)
        collection_id = collection_data.get("id", "unknown")

        print(f"Processing collection: {collection_id}")
        print(f"  Path: {collection_dir}")

        # Load collection
        if not dry_run:
            success, message = load_collection_to_pgstac(conn, collection_data)
            print(f"  {message}")

            if not success:
                print(f"  Skipping items for this collection due to error")
                continue

            collections_loaded += 1
        else:
            print(f"  [DRY RUN] Would load collection: {collection_id}")

        # Find and load items
        item_files = find_item_files(collection_dir)
        print(f"  Found {len(item_files)} item(s)")

        if len(item_files) > 0:
            # Load items from files, skipping any with a null/missing geometry — pgstac's
            # NOT NULL constraint on geometry rejects the whole batch otherwise (upsert_items
            # errors out for the batch, not just the offending row)
            items = []
            skipped_null_geometry = 0
            for item_file in item_files:
                try:
                    item_data = load_json_file(item_file)
                except Exception as e:
                    print(f"    WARNING: Failed to load item {item_file.name}: {e}")
                    continue

                if not item_data.get("geometry"):
                    print(f"    WARNING: skipping {item_data.get('id', item_file.name)}: null geometry")
                    skipped_null_geometry += 1
                    continue

                items.append(item_data)

            if skipped_null_geometry > 0:
                print(f"  Skipped {skipped_null_geometry} item(s) with null geometry")
                total_items_skipped += skipped_null_geometry

            if not dry_run:
                start_time = time.time()
                successful, failed, errors = load_items_to_pgstac(conn, items, batch_size)
                elapsed = time.time() - start_time

                total_items_loaded += successful
                total_items_failed += failed

                rate = successful / elapsed if elapsed > 0 else 0
                print(f"  Loaded {successful} items in {elapsed:.1f}s ({rate:.1f} items/sec)")

                if failed > 0:
                    print(f"  Failed: {failed} items")
            else:
                print(f"  [DRY RUN] Would load {len(items)} item(s)")

        print()

    # Close connection
    if not dry_run:
        conn.close()

    # Step 5: Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)

    if not dry_run:
        print(f"Collections loaded: {collections_loaded}")
        print(f"Items loaded: {total_items_loaded}")
        print(f"Items failed: {total_items_failed}")
        print(f"Items skipped (null geometry): {total_items_skipped}")
        print()

        # Get database statistics
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, database=database)
        stats = get_database_stats(conn)
        conn.close()

        print("Database Statistics:")
        print(f"  Total collections: {stats['collections']}")
        print(f"  Total items: {stats['total_items']}")
        print()
        print("  Items by collection:")
        for collection, count in stats['items_by_collection']:
            print(f"    {collection}: {count}")
    else:
        print(f"Collections found: {len(collection_files)}")
        print(f"Items that would be skipped (null geometry): {total_items_skipped}")
        print(f"[DRY RUN] No data was loaded")

    print()
    print("Done!")
    return 0


def main() -> int:
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Load STAC catalog into pgstac database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load catalog from local directory (default: connects to 'database' service)
  python3 load_catalog.py /data/stac-catalog

  # Load with custom database host (e.g., localhost for local testing)
  python3 load_catalog.py /data/stac-catalog --db-host localhost

  # Dry run to preview what would be loaded
  python3 load_catalog.py /data/stac-catalog --dry-run

  # Adjust batch size for performance
  python3 load_catalog.py /data/stac-catalog --batch-size 200
        """
    )

    parser.add_argument(
        "catalog_dir",
        type=Path,
        help="Directory containing catalog.json and collections"
    )

    parser.add_argument(
        "--db-host",
        default=os.environ.get("PGHOST", "database"),
        help="Database host (default: database, or PGHOST env var)"
    )

    parser.add_argument(
        "--db-port",
        type=int,
        default=5432,
        help="Database port (default: 5432)"
    )

    parser.add_argument(
        "--db-user",
        default="pgstac",
        help="Database user (default: pgstac). Can also set PGUSER env var"
    )

    parser.add_argument(
        "--db-password",
        default=None,
        help="Database password. Can also set PGPASSWORD env var or use password file"
    )

    parser.add_argument(
        "--db-name",
        default="stacdb",
        help="Database name (default: stacdb)"
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of items to load per batch (default: 100)"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be loaded without actually loading"
    )

    args = parser.parse_args()

    # Validate catalog directory
    if not args.catalog_dir.exists():
        print(f"ERROR: Directory not found: {args.catalog_dir}")
        return 1

    if not args.catalog_dir.is_dir():
        print(f"ERROR: Not a directory: {args.catalog_dir}")
        return 1

    # Get database password
    password = args.db_password

    # Try environment variable
    if password is None:
        password = os.environ.get("PGPASSWORD")

    # Try reading from password file (for EC2/Docker deployments)
    if password is None:
        password_file = Path("/opt/benchmarkcat/.db_password")
        if password_file.exists():
            try:
                password = password_file.read_text().strip()
                print(f"Using password from {password_file}")
            except Exception as e:
                print(f"WARNING: Could not read password file {password_file}: {e}")

    # Try .pgpass file
    if password is None:
        pgpass_file = Path.home() / ".pgpass"
        if pgpass_file.exists():
            print(f"INFO: Checking {pgpass_file} for credentials")
            # pgpass format: hostname:port:database:username:password
            try:
                with open(pgpass_file, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#"):
                            parts = line.split(":")
                            if len(parts) >= 5:
                                host_match = parts[0] == "*" or parts[0] == args.db_host
                                port_match = parts[1] == "*" or parts[1] == str(args.db_port)
                                db_match = parts[2] == "*" or parts[2] == args.db_name
                                user_match = parts[3] == args.db_user
                                if host_match and port_match and db_match and user_match:
                                    password = parts[4]
                                    print(f"Using password from {pgpass_file}")
                                    break
            except Exception as e:
                print(f"WARNING: Could not read .pgpass file: {e}")

    if password is None:
        print("ERROR: Database password not provided!")
        print("  Provide password via:")
        print("    --db-password argument")
        print("    PGPASSWORD environment variable")
        print("    /opt/benchmarkcat/.db_password file")
        print("    ~/.pgpass file")
        return 1

    # Load catalog
    return load_catalog(
        catalog_dir=args.catalog_dir,
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=password,
        database=args.db_name,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    sys.exit(main())

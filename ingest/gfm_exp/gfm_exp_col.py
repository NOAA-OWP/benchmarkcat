import time
import argparse
import json
import logging
import multiprocessing
import os
import re
import tempfile
from datetime import datetime, timezone
from functools import partial

import boto3
import geopandas as gpd
import pystac
from botocore.exceptions import ClientError
from pystac.extensions.item_assets import ItemAssetsExtension
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.sat import SatExtension
from pystac.summaries import Summaries
from shapely.geometry import shape

from ingest.gfm.gfm_stac import AssetUtils, GFMInfo, SentinelName
from ingest.gfm_exp.gfm_exp_handle_assets import GFMExpAssetHandler
from ingest.gfm_exp.gfm_qc import compute_scene_qc
from ingest.utils import S3Utils

logging.basicConfig(level=logging.INFO)


def initialize_s3_utils(profile: str | None = None):
    if profile is not None:
        session = boto3.Session(profile_name=profile)
        s3 = session.client("s3")
    else:
        s3 = boto3.client("s3")
    s3_utils = S3Utils(s3)
    return s3_utils


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--link_type", type=str, default="uri", help='Link type, either "url" or "uri"')
    parser.add_argument("--bucket_name", type=str, required=True, help="S3 bucket name")
    parser.add_argument(
        "--catalog_path",
        type=str,
        required=True,
        help="Path to the STAC catalog in the S3 bucket",
    )
    parser.add_argument(
        "--asset_object_key",
        type=str,
        required=True,
        help="S3 prefix for GFM_EXP source data (PI4 date directories)",
    )
    parser.add_argument(
        "--hucs_object_key",
        type=str,
        required=True,
        help="S3 key for the HUC8 GeoPackage",
    )
    parser.add_argument(
        "--boundaries_object_key",
        type=str,
        required=True,
        help="S3 key for the Mexico/Canada boundaries GeoPackage",
    )
    parser.add_argument(
        "--reprocess_assets",
        action="store_true",
        help="Set to true to reprocess assets using GFMAssetHandler",
    )
    parser.add_argument(
        "--derived_metadata_path",
        type=str,
        required=True,
        help="S3 key for the derived metadata Parquet file created by asset handling code.",
    )
    parser.add_argument(
        "--skip-owp-qc",
        action="store_true",
        help="Skip OWP QC grading and HUC-level metrics (faster runs).",
    )
    parser.add_argument(
        "--profile",
        type=str,
        default=None,
        help="AWS profile name for boto3 (e.g. from ~/.aws/credentials).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel workers for scene processing; 1 = sequential.",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=50,
        help="Every N merged scenes, flush item JSONs to S3 and write/upload parquet; 0 = only at end (no intermediate checkpoints).",
    )
    parser.add_argument(
        "--after-date",
        type=str,
        default=None,
        help="Process only dates >= this (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--dates",
        type=str,
        default=None,
        help="Comma-separated list of dates to process (e.g. 2024-01-01,2024-01-15).",
    )
    parser.add_argument(
        "--before-date",
        type=str,
        default=None,
        help="Process only dates <= this (YYYY-MM-DD).",
    )
    # Batch-worker mode args
    parser.add_argument(
        "--mode",
        choices=["local", "batch-worker"],
        default="local",
        help="'local' = normal run; 'batch-worker' = read manifest slice, write partial parquet.",
    )
    parser.add_argument(
        "--manifest-s3-key",
        type=str,
        default=None,
        help="S3 key of manifest JSONL produced by batch_split.py (required in batch-worker mode).",
    )
    parser.add_argument(
        "--job-index",
        type=int,
        default=None,
        help="Array job index (0-based). Set by entrypoint.sh from the cloud provider's env var.",
    )
    parser.add_argument(
        "--scenes-per-job",
        type=int,
        default=50,
        help="Number of scenes each batch-worker array child processes.",
    )
    parser.add_argument(
        "--partial-parquet-prefix",
        type=str,
        default=None,
        help="S3 prefix where per-job partial parquets are written (required in batch-worker mode).",
    )
    parser.add_argument(
        "--readme-object-key",
        type=str,
        required=True,
        help="S3 key for GFM data readme PDF.",
    )
    return parser.parse_args()


def get_conus_neighbors(gpkg_path):
    """
    Loads country boundaries of Canada and Mexico from a geopackage file. This was necessary since filter_gfm.py regions aren't perfect to make AOI creation on the GFM api easier. Thus we are going to not catalog products that lie entirely within Canada and Mexico.
    """

    try:
        # Read the geopackage
        boundaries_gdf = gpd.read_file(gpkg_path)

        canada = boundaries_gdf[boundaries_gdf["ADMIN"] == "Canada"].geometry.iloc[0]
        mexico = boundaries_gdf[boundaries_gdf["ADMIN"] == "Mexico"].geometry.iloc[0]

        return {"canada": canada, "mexico": mexico}
    except Exception as e:
        raise ValueError(f"Failed to load country boundaries from {gpkg_path}: {str(e)}")


def is_within_neighbor_countries(geometry, country_boundaries):
    """Check if geometry lies completely within Canada or Mexico"""
    geom_shape = shape(geometry) if isinstance(geometry, dict) else geometry

    # Check if geometry is completely within Canada
    if country_boundaries["canada"].contains(geom_shape):
        return True

    # Check if geometry is completely within Mexico
    if country_boundaries["mexico"].contains(geom_shape):
        return True

    return False


def create_gfm_exp_collection(link_type, bucket_name, asset_object_key, s3_utils, readme_object_key):
    collection = pystac.Collection(
        id="gfm-expanded-collection",
        description="This collection contains Global Flood Monitoring (GFM) flood tile groups contained within a given Sentinel-1 datatake footprint. For each footprint a flowfile created from NWM ANA data is provided that estimates the flows present during the data take. Each tile within a data take footprint is also associated with a flood to baseline ratio that gives the percentage of flooded pixels relative to what is normally inundated according to GFM.",
        title="Expanded Global Flood Monitoring Collection",
        keywords=["flood", "GFM"],
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-179.9, 7.2, -64.5, 61.8]]),
            temporal=pystac.TemporalExtent([[datetime(2021, 8, 1, tzinfo=timezone.utc), None]]),
        ),
        license="CC-BY-4.0",
        providers=[
            pystac.Provider(
                name="GLOFAS",
                roles=[
                    pystac.ProviderRole.PRODUCER,
                    pystac.ProviderRole.PROCESSOR,
                    pystac.ProviderRole.LICENSOR,
                ],
                description="The Global Flood Awareness System (GLOFAS) provides real-time flood monitoring and early warning information.",
                url="https://global-flood.emergency.copernicus.eu/",
            )
        ],
        summaries=Summaries(
            {
                "platform": ["Sentinel-1"],
                "constellation": ["Copernicus"],
                "instruments": ["SAR"],
                "providers": ["GLOFAS"],
                "GFM_layers": GFMInfo.layers,
            }
        ),
    )
    readme_href, is_valid = s3_utils.generate_href(bucket_name, readme_object_key, link_type)
    if is_valid:
        collection.assets["naming_conventions"] = pystac.Asset(
            href=readme_href,
            title="GFM Data Readme",
            description="This document contains the naming conventions for the GFM data.",
            media_type="application/pdf",
        )
    else:
        print("Skipping gfm readme asset creation - invalid or inaccessible")

    item_assets_ext = ItemAssetsExtension.ext(collection, add_if_missing=True)
    item_assets_ext.item_assets = GFMInfo.assets

    return collection


def get_gfm_exp_dates(s3_utils, bucket_name, asset_object_key):
    return s3_utils.list_subdirectories(bucket_name, asset_object_key)


def _date_id_from_path(date_path):
    """Extract date id (YYYY-MM-DD) from date path for filtering."""
    return date_path.strip("/").split("/")[-1]


def filter_dates_by_scope(dates, after_date=None, before_date=None, dates_list=None):
    """Filter dates by --after-date, --before-date, and --dates. Apply after_date, then before_date, then dates_list."""
    if after_date is not None:
        dates = [d for d in dates if _date_id_from_path(d) >= after_date]
    if before_date is not None:
        dates = [d for d in dates if _date_id_from_path(d) <= before_date]
    if dates_list is not None:
        allowed = set(s.strip() for s in dates_list.split(",") if s.strip())
        dates = [d for d in dates if _date_id_from_path(d) in allowed]
    return dates


def item_id_from_sent_ti_path(sent_ti_path: str) -> str:
    """Derive STAC item id from sent_ti_path (matches create_item)."""
    sent_ti = sent_ti_path.strip("/").split("/")[-1]
    return f"GFM-expanded_{sent_ti}"


def scene_already_uploaded(sent_ti_path, results_df, s3_utils, bucket_name, catalog_path, catalog_id) -> bool:
    """True if scene row is in parquet and item JSON exists on S3 (fully committed)."""
    if sent_ti_path not in results_df["sent_ti_path"].values:
        return False
    item_id = item_id_from_sent_ti_path(sent_ti_path)
    base = catalog_path.rstrip("/") + "/" + catalog_id.strip("/")
    key = f"{base}/{item_id}/{item_id}.json"
    try:
        s3_utils.s3_client.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "404":
            return False
        raise


def flush_item_batch(s3_utils, bucket_name, catalog_path, catalog_id, collection, item_buffer):
    """Upload item JSONs to S3, add item links to collection, clear buffer."""
    if not item_buffer:
        return
    base = catalog_path.rstrip("/") + "/" + catalog_id.strip("/")
    for item in item_buffer:
        key = f"{base}/{item.id}/{item.id}.json"
        item.set_parent(collection)
        d = item.to_dict()
        d["collection"] = catalog_id

        # Build links to match root, collection, parent
        links = [
            {
                "rel": "root",
                "href": "../../catalog.json",
                "type": "application/json",
                "title": "FIM Benchmark Catalog",
            },
            {
                "rel": "collection",
                "href": "../collection.json",
                "type": "application/json",
                "title": "Expanded Global Flood Monitoring Collection",
            },
            {
                "rel": "parent",
                "href": "../collection.json",
                "type": "application/json",
                "title": "Expanded Global Flood Monitoring Collection",
            },
        ]
        d["links"] = links
        body = json.dumps(d, indent=2)
        s3_utils.s3_client.put_object(Bucket=bucket_name, Key=key, Body=body, ContentType="application/json")
        collection.add_link(
            pystac.Link(
                rel=pystac.RelType.ITEM,
                target=f"./{item.id}/{item.id}.json",
                media_type="application/geo+json",
            )
        )
    item_buffer.clear()
    logging.info(f"Flushed batch of items to S3 (prefix {base})")


def process_date(
    date_path,
    s3_utils,
    bucket_name,
    link_type,
    collection,
    reprocess_assets,
    asset_handler,
    hucs_gdf,
    country_boundaries,
    skip_owp_qc=False,
    on_scene_done=None,
    catalog_path=None,
    catalog_id=None,
):
    date_id = date_path.strip("/").split("/")[-1]
    logging.info(f"Indexing date: {date_id}")

    sent_ti_list = s3_utils.list_subdirectories(bucket_name, date_path)
    for sent_ti_path in sent_ti_list:
        if catalog_path is not None and catalog_id is not None and on_scene_done is not None:
            if scene_already_uploaded(
                sent_ti_path, asset_handler.results_df, s3_utils, bucket_name, catalog_path, catalog_id
            ):
                item_id = item_id_from_sent_ti_path(sent_ti_path)
                collection.add_link(
                    pystac.Link(
                        rel=pystac.RelType.ITEM,
                        target=f"{item_id}/{item_id}.json",
                        media_type="application/geo+json",
                    )
                )
                continue
        item, asset_results = process_tile(
            sent_ti_path,
            date_id,
            s3_utils,
            bucket_name,
            link_type,
            reprocess_assets,
            asset_handler,
            hucs_gdf,
            country_boundaries,
            skip_owp_qc=skip_owp_qc,
        )
        if item is not None and asset_results is not None:
            if on_scene_done is not None:
                on_scene_done(item, sent_ti_path, asset_results)
            else:
                collection.add_item(item)
                asset_handler.merge_single_result(sent_ti_path, asset_results)


def get_flood_ratios(s3_utils, bucket_name, sent_ti_path):
    try:
        ratio_key = s3_utils.list_resources_with_string(bucket_name, sent_ti_path, ["flood_ratios.json"])[0]
        response = s3_utils.s3_client.get_object(Bucket=bucket_name, Key=ratio_key)
        flood_ratios = json.loads(response["Body"].read().decode("utf-8"))
        return flood_ratios
    except (IndexError, ClientError) as e:
        logging.warning(f"No flood_ratios.json found for {sent_ti_path}: {str(e)}")
        return {}


def process_tile(
    sent_ti_path,
    date_id,
    s3_utils,
    bucket_name,
    link_type,
    reprocess_assets,
    asset_handler,
    hucs_gdf=None,
    country_boundaries=None,
    skip_owp_qc=False,
):
    sent_ti = sent_ti_path.strip("/").split("/")[-1]
    equi7tiles_list = [
        m.group()
        for filename in s3_utils.list_resources_with_string(bucket_name, sent_ti_path, ["OBSWATER"])
        if len(os.path.basename(filename).split("_")) > 2
        for m in [re.search(r"[E]\d{3}[N]\d{3}T\d", os.path.basename(filename))]
        if m is not None
    ]

    gfm_version, orbit_state, abs_orbit_num = get_orbit_info(sent_ti_path, s3_utils, bucket_name)
    start_datetime, end_datetime = SentinelName.extract_datetimes(sent_ti)

    if asset_handler.tile_assets_processed(sent_ti_path) and not reprocess_assets:
        asset_results = asset_handler.read_data_parquet(sent_ti_path)
    else:
        asset_results = asset_handler.handle_assets(sent_ti_path, equi7tiles_list)

    bbox = asset_results["bbox"]
    flowfile_object = asset_results["flowfile_object"]
    equi7tile_areas = asset_results["equi7tile_areas"]

    geometry = asset_results["geometry"]
    huc8_list = []

    if not geometry:
        logging.warning("Skipping %s - no valid geometry (null footprint)", sent_ti)
        return (None, None)

    # Check if geometry is within Canada or Mexico before proceeding
    if country_boundaries and is_within_neighbor_countries(geometry, country_boundaries):
        logging.info(f"Skipping {sent_ti} - geometry lies completely within Canada or Mexico")
        return (None, None)

    # Find intersecting HUC8s if HUCs data is provided
    if hucs_gdf is not None:
        # Create a GeoDataFrame with the flood geometry
        flood_gdf = gpd.GeoDataFrame(geometry=[shape(geometry)], crs=hucs_gdf.crs)
        # Perform spatial join, filtering NaN from unmatched left-join rows
        huc_join = gpd.sjoin(flood_gdf, hucs_gdf, how="left", predicate="intersects")
        huc8_list = [v for v in huc_join["HUC8"].tolist() if v == v] if "HUC8" in huc_join.columns else []

    flood_ratios = get_flood_ratios(s3_utils, bucket_name, sent_ti_path)

    owp_properties = {}
    if not skip_owp_qc and huc8_list and equi7tiles_list and hucs_gdf is not None:
        try:
            owp_properties = compute_scene_qc(
                huc8_list=huc8_list,
                hucs_gdf=hucs_gdf,
                sent_ti_path=sent_ti_path,
                equi7tiles_list=equi7tiles_list,
                bucket_name=bucket_name,
                s3_utils=s3_utils,
            )
        except Exception as e:
            logging.warning("OWP QC computation failed for %s: %s", sent_ti, e)

    item = create_item(
        date_id,
        sent_ti,
        geometry,
        bbox,
        start_datetime,
        end_datetime,
        orbit_state,
        abs_orbit_num,
        gfm_version,
        flowfile_object,
        equi7tile_areas,
        flood_ratios,
        huc8_list=huc8_list,
        owp_properties=owp_properties,
    )

    SatExtension.ext(item, add_if_missing=True)
    ProjectionExtension.ext(item, add_if_missing=True)

    add_assets_to_item(
        item,
        sent_ti_path,
        equi7tiles_list,
        s3_utils,
        bucket_name,
        link_type,
        asset_results["flowfile_key"],
    )

    item.validate()
    return (item, asset_results)


def get_orbit_info(sent_ti_path, s3_utils, bucket_name):
    advflag_list = s3_utils.list_resources_with_string(bucket_name, sent_ti_path, ["ADVFLAG"])
    if advflag_list:
        gfm_version = SentinelName.extract_version_string(advflag_list[0])
        orbit_direction = SentinelName.extract_orbit_state(advflag_list[0])
        orbit_state = "ascending" if orbit_direction == "A" else "descending"
    else:
        logging.warning(f"Skipping GFM version and orbit direction for {sent_ti_path}")
        orbit_state, gfm_version = None, None

    abs_orbit_num = SentinelName.extract_orbit_number(sent_ti_path)
    return gfm_version, orbit_state, abs_orbit_num


def create_item(
    date_id,
    sent_ti,
    geometry,
    bbox,
    start_datetime,
    end_datetime,
    orbit_state,
    abs_orbit_num,
    gfm_version,
    flowfile_object,
    equi7tile_areas,
    flood_ratios,
    huc8_list,
    owp_properties=None,
):
    properties = {
        "title": f"GFM-expanded_{sent_ti}",
        "description": f"This item lists assets associated with the GFM scene {sent_ti}.",
        "gfm_data_take_start_datetime": start_datetime.isoformat(),
        "gfm_data_take_end_datetime": end_datetime.isoformat(),
        "proj:code": "EPSG:27705",
        "proj:wkt2": "+proj=aeqd +lat_0=52 +lon_0=-97.5 +x_0=8264722.17686 +y_0=4867518.35323 +datum=WGS84 +units=m +no_defs",
        "gsd (m)": 20,
        "gfm_version": gfm_version,
        "flowfiles": flowfile_object,
        "hucs": huc8_list,
        "tile_total_inundated_area (m^2)": equi7tile_areas,
        "flood_to_baseline_ratios": flood_ratios,
    }
    if owp_properties:
        properties.update(owp_properties)

    if orbit_state is not None:
        properties["sat:orbit_state"] = orbit_state
    if abs_orbit_num is not None:
        properties["sat:absolute_orbit"] = int(abs_orbit_num)

    return pystac.Item(
        id=f"GFM-expanded_{sent_ti}",
        geometry=geometry,
        bbox=bbox,
        datetime=start_datetime,
        properties=properties,
    )


def add_assets_to_item(item, sent_ti_path, equi7tiles_list, s3_utils, bucket_name, link_type, flowfile_key):
    equi7tile_assets = {}
    if flowfile_key:
        equi7tile = None
        asset_id, asset = create_asset(flowfile_key, bucket_name, link_type, equi7tile, s3_utils, flowfile=True)
        if asset:
            item.add_asset(asset_id, asset)
        else:
            print(f"Skipping creating flowfile asset for {equi7tile} - invalid or inaccessible")

    for equi7tile in equi7tiles_list:
        tile_asset_list = s3_utils.list_resources_with_string(bucket_name, sent_ti_path, [equi7tile])
        equi7tile_assets[equi7tile] = []

        for tile_asset_path in tile_asset_list:
            asset_id, asset = create_asset(tile_asset_path, bucket_name, link_type, equi7tile, s3_utils)
            equi7tile_assets[equi7tile].append(asset_id)
            if asset:
                item.add_asset(asset_id, asset)
            else:
                print(f"Skipping creating asset for {asset_id} - invalid or inaccessible")

    item.properties["equi7tile_assets"] = equi7tile_assets


def create_asset(asset_path, bucket_name, link_type, equi7tile, s3_utils, flowfile=False):
    if flowfile:
        asset_id = "NWM_ANA_flowfile"
        flowfile_href, is_valid = s3_utils.generate_href(bucket_name, asset_path, link_type)
        if is_valid:
            asset = pystac.Asset(
                href=flowfile_href,
                roles=["data"],
                description="NWM flowfile produced from ANA data, see flowfiles key in properties for more information",
            )
        else:
            asset = None
    else:
        tile_asset = asset_path.strip("/").split("/")[-1]
        asset_type = AssetUtils.determine_asset_type(tile_asset)
        role = (
            "thumbnail"
            if asset_type == "Thumbnail"
            else ("metadata" if asset_type in ["Footprint", "Metadata", "Schedule"] else "data")
        )
        media_type = AssetUtils.get_media_type(tile_asset)
        asset_id = f"{equi7tile}_{asset_type.replace(' ', '_')}"
        tile_asset_href, is_valid = s3_utils.generate_href(bucket_name, asset_path, link_type)
        if is_valid:
            asset = pystac.Asset(
                href=tile_asset_href,
                roles=[role],
                media_type=media_type,
                title=f"{equi7tile} {asset_type}",
            )
        else:
            asset = None
    return asset_id, asset


def _process_one_scene(
    work_item,
    profile,
    bucket_name,
    link_type,
    reprocess_assets,
    skip_owp_qc,
    hucs_gdf,
    country_boundaries,
    derived_metadata_path,
    initial_results_df=None,
):
    """Top-level worker for parallel scene processing. Creates own s3_utils and asset_handler."""
    date_path, date_id, sent_ti_path = work_item
    logging.info("Processing scene: %s of date: %s", sent_ti_path, date_id)
    if profile is not None:
        os.environ["AWS_PROFILE"] = profile
    else:
        os.environ.pop("AWS_PROFILE", None)

    # Prevents expensive S3 ListObjects calls when opening a file
    os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "EMPTY_DIR"

    # Increases the size of the first request to grab headers + metadata in one go
    os.environ["CPL_VSIL_CURL_CHUNK_SIZE"] = "65536"

    s3_utils = initialize_s3_utils(profile=profile)
    asset_handler = GFMExpAssetHandler(
        s3_utils, bucket_name, derived_metadata_path, initial_results_df=initial_results_df
    )
    item, asset_results = process_tile(
        sent_ti_path,
        date_id,
        s3_utils,
        bucket_name,
        link_type,
        reprocess_assets,
        asset_handler,
        hucs_gdf,
        country_boundaries,
        skip_owp_qc=skip_owp_qc,
    )
    return (item, sent_ti_path, asset_results)


def main_batch_worker(args):
    """Batch-worker entry point: process one manifest slice, flush item JSONs, write partial parquet."""
    import pandas as pd
    from ingest.batch_utils import load_partial_parquets, read_manifest, upload_partial_parquet

    if not args.manifest_s3_key:
        raise ValueError("--manifest-s3-key is required in batch-worker mode")
    if not args.partial_parquet_prefix:
        raise ValueError("--partial-parquet-prefix is required in batch-worker mode")

    job_index = args.job_index if args.job_index is not None else 0

    if args.profile is not None:
        os.environ["AWS_PROFILE"] = args.profile
    else:
        os.environ.pop("AWS_PROFILE", None)
    os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "EMPTY_DIR"
    os.environ["CPL_VSIL_CURL_CHUNK_SIZE"] = "65536"

    s3_utils = initialize_s3_utils(profile=args.profile)

    all_scenes = read_manifest(s3_utils, args.bucket_name, args.manifest_s3_key)
    start = job_index * args.scenes_per_job
    my_scenes = all_scenes[start: start + args.scenes_per_job]

    if not my_scenes:
        logging.info("Batch worker %d: no scenes in slice — exiting", job_index)
        return

    logging.info("Batch worker %d: processing %d scenes (indices %d–%d)",
                 job_index, len(my_scenes), start, start + len(my_scenes) - 1)

    asset_handler = GFMExpAssetHandler(s3_utils, args.bucket_name, args.derived_metadata_path)
    partial_df = load_partial_parquets(s3_utils, args.bucket_name, args.partial_parquet_prefix)
    if not partial_df.empty:
        asset_handler.results_df = pd.concat(
            [asset_handler.results_df, partial_df], ignore_index=True
        ).drop_duplicates(subset=["sent_ti_path"], keep="last").reset_index(drop=True)
        logging.info("Batch worker %d: merged %d rows from existing partials into results_df",
                     job_index, len(partial_df))

    # Upload full slice for this job so changing scenes_per_job between runs does not lose data when overwriting same job index.
    catalog_id = "gfm-expanded-collection"
    collection = create_gfm_exp_collection(
        args.link_type, args.bucket_name, args.asset_object_key, s3_utils,
        readme_object_key=args.readme_object_key
    )
    item_buffer = []

    _, hucs_gpkg = os.path.split(args.hucs_object_key)
    with tempfile.TemporaryDirectory() as tmpdir:
        local_hucs_path = os.path.join(tmpdir, hucs_gpkg)
        s3_utils.s3_client.download_file(args.bucket_name, args.hucs_object_key, local_hucs_path)
        hucs_gdf = gpd.read_file(local_hucs_path)

        local_boundaries_path = os.path.join(tmpdir, os.path.basename(args.boundaries_object_key))
        s3_utils.s3_client.download_file(args.bucket_name, args.boundaries_object_key, local_boundaries_path)
        country_boundaries = get_conus_neighbors(local_boundaries_path)

        work_items = [(s["date_path"], s["date_id"], s["sent_ti_path"]) for s in my_scenes]

        skipped_count = 0
        if args.workers <= 1:
            for scene in my_scenes:
                date_id = scene["date_id"]
                sent_ti_path = scene["sent_ti_path"]
                try:
                    if scene_already_uploaded(
                        sent_ti_path,
                        asset_handler.results_df,
                        s3_utils,
                        args.bucket_name,
                        args.catalog_path,
                        catalog_id,
                    ):
                        logging.info("Skipping already-uploaded scene: %s", sent_ti_path)
                        skipped_count += 1
                        continue
                    item, asset_results = process_tile(
                        sent_ti_path,
                        date_id,
                        s3_utils,
                        args.bucket_name,
                        args.link_type,
                        args.reprocess_assets,
                        asset_handler,
                        hucs_gdf,
                        country_boundaries,
                        skip_owp_qc=args.skip_owp_qc,
                    )
                    if item is not None and asset_results is not None:
                        item_buffer.append(item)
                        asset_handler.merge_single_result(sent_ti_path, asset_results)
                except Exception as e:
                    logging.warning("Scene %s failed: %s", sent_ti_path, e)
        else:
            work_items_to_process = []
            for date_path, date_id, sent_ti_path in work_items:
                if scene_already_uploaded(
                    sent_ti_path,
                    asset_handler.results_df,
                    s3_utils,
                    args.bucket_name,
                    args.catalog_path,
                    catalog_id,
                ):
                    skipped_count += 1
                else:
                    work_items_to_process.append((date_path, date_id, sent_ti_path))
            work_items = work_items_to_process

            worker = partial(
                _process_one_scene,
                profile=args.profile,
                bucket_name=args.bucket_name,
                link_type=args.link_type,
                reprocess_assets=args.reprocess_assets,
                skip_owp_qc=args.skip_owp_qc,
                hucs_gdf=hucs_gdf,
                country_boundaries=country_boundaries,
                derived_metadata_path=args.derived_metadata_path,
                initial_results_df=None,
            )
            with multiprocessing.Pool(args.workers) as pool:
                for result in pool.imap_unordered(worker, work_items):
                    try:
                        item, sent_ti_path, asset_results = result
                        if item is not None and asset_results is not None:
                            item_buffer.append(item)
                            asset_handler.merge_single_result(sent_ti_path, asset_results)
                    except Exception as e:
                        logging.warning("Worker result failed: %s", e)

    if skipped_count > 0:
        logging.info(
            "Batch worker %d: skipped %d already-uploaded scenes out of %d total",
            job_index, skipped_count, len(my_scenes),
        )

    flush_item_batch(s3_utils, args.bucket_name, args.catalog_path, catalog_id, collection, item_buffer)

    my_sent_ti_paths = {s["sent_ti_path"] for s in my_scenes}
    upload_df = asset_handler.results_df[
        asset_handler.results_df["sent_ti_path"].isin(my_sent_ti_paths)
    ]
    if not upload_df.empty:
        upload_partial_parquet(
            s3_utils,
            args.bucket_name,
            args.partial_parquet_prefix,
            job_index,
            upload_df,
        )

    logging.info("Batch worker %d done.", job_index)


def main():
    args = parse_arguments()

    if args.mode == "batch-worker":
        main_batch_worker(args)
        return

    if args.profile is not None:
        os.environ["AWS_PROFILE"] = args.profile
    else:
        os.environ.pop("AWS_PROFILE", None)

    # Prevents expensive S3 ListObjects calls when opening a file
    os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "EMPTY_DIR"

    # Increases the size of the first request to grab headers + metadata in one go
    os.environ["CPL_VSIL_CURL_CHUNK_SIZE"] = "65536"

    s3_utils = initialize_s3_utils(profile=args.profile)

    collection = create_gfm_exp_collection(
        args.link_type, args.bucket_name, args.asset_object_key, s3_utils,
        readme_object_key=args.readme_object_key
    )
    dates = get_gfm_exp_dates(s3_utils, args.bucket_name, args.asset_object_key)
    dates = filter_dates_by_scope(
        dates, after_date=args.after_date, before_date=args.before_date, dates_list=args.dates
    )
    asset_handler = GFMExpAssetHandler(s3_utils, args.bucket_name, args.derived_metadata_path)
    catalog_id = "gfm-expanded-collection"
    item_buffer = []
    items_merged = [0]  # mutable so callback can update

    def do_parquet_checkpoint():
        if args.checkpoint_every <= 0:
            return
        asset_handler.results_df.to_parquet(asset_handler.local_results_file, index=False)
        asset_handler.s3_utils.s3_client.upload_file(
            asset_handler.local_results_file,
            asset_handler.bucket_name,
            asset_handler.derived_metadata_path,
        )
        logging.info(f"Parquet checkpoint: uploaded after {items_merged[0]} scenes")

    def on_scene_done(item, sent_ti_path, asset_results):
        if args.checkpoint_every <= 0:
            collection.add_item(item)
        else:
            item_buffer.append(item)
        asset_handler.merge_single_result(sent_ti_path, asset_results)
        items_merged[0] += 1
        if (
            args.checkpoint_every > 0
            and items_merged[0] % args.checkpoint_every == 0
            and items_merged[0] > 0
        ):
            flush_item_batch(s3_utils, args.bucket_name, args.catalog_path, catalog_id, collection, item_buffer)
            do_parquet_checkpoint()

    # Download and read HUCs data and country boundaries
    _, hucs_gpkg = os.path.split(args.hucs_object_key)
    with tempfile.TemporaryDirectory() as tmpdir:
        local_hucs_path = f"{tmpdir}/{hucs_gpkg}"
        s3_utils.s3_client.download_file(args.bucket_name, args.hucs_object_key, local_hucs_path)
        hucs_gdf = gpd.read_file(local_hucs_path)

        local_boundaries_path = os.path.join(tmpdir, os.path.basename(args.boundaries_object_key))
        s3_utils.s3_client.download_file(args.bucket_name, args.boundaries_object_key, local_boundaries_path)
        country_boundaries = get_conus_neighbors(local_boundaries_path)

        if args.workers <= 1:
            for date in dates:
                logging.info("===============processing %s===============", date)
                process_date(
                    date,
                    s3_utils,
                    args.bucket_name,
                    args.link_type,
                    collection,
                    args.reprocess_assets,
                    asset_handler,
                    hucs_gdf,
                    country_boundaries,
                    skip_owp_qc=args.skip_owp_qc,
                    on_scene_done=on_scene_done,
                    catalog_path=args.catalog_path if args.checkpoint_every > 0 else None,
                    catalog_id=catalog_id if args.checkpoint_every > 0 else None,
                )
        else:
            work_items = []
            for date_path in dates:
                date_id = date_path.strip("/").split("/")[-1]
                sent_ti_list = s3_utils.list_subdirectories(args.bucket_name, date_path)
                for sent_ti_path in sent_ti_list:
                    work_items.append((date_path, date_id, sent_ti_path))
            if args.checkpoint_every > 0:
                work_items_to_process = []
                for date_path, date_id, sent_ti_path in work_items:
                    if scene_already_uploaded(
                        sent_ti_path,
                        asset_handler.results_df,
                        s3_utils,
                        args.bucket_name,
                        args.catalog_path,
                        catalog_id,
                    ):
                        item_id = item_id_from_sent_ti_path(sent_ti_path)
                        collection.add_link(
                            pystac.Link(
                                rel=pystac.RelType.ITEM,
                                target=f"./{item_id}/{item_id}.json",
                                media_type="application/geo+json",
                            )
                        )
                    else:
                        work_items_to_process.append((date_path, date_id, sent_ti_path))
                work_items = work_items_to_process

            logging.info("Work items to process: %d", len(work_items))

            worker = partial(
                _process_one_scene,
                profile=args.profile,
                bucket_name=args.bucket_name,
                link_type=args.link_type,
                reprocess_assets=args.reprocess_assets,
                skip_owp_qc=args.skip_owp_qc,
                hucs_gdf=hucs_gdf,
                country_boundaries=country_boundaries,
                derived_metadata_path=args.derived_metadata_path,
                initial_results_df=asset_handler.results_df,
            )
            with multiprocessing.Pool(args.workers) as pool:
                for result in pool.imap_unordered(worker, work_items):
                    try:
                        item, sent_ti_path, asset_results = result
                        if item is not None and asset_results is not None:
                            on_scene_done(item, sent_ti_path, asset_results)
                    except Exception as e:
                        logging.warning("Worker result failed: %s", e)

    # Flush remaining item buffer
    if args.checkpoint_every > 0:
        flush_item_batch(s3_utils, args.bucket_name, args.catalog_path, catalog_id, collection, item_buffer)

    # When using workers, main only merged into results_df; write to parquet before final upload
    if args.workers > 1:
        asset_handler.results_df.to_parquet(asset_handler.local_results_file, index=False)

    # When checkpoint_every <= 0, items were added via collection.add_item; set self href to .json so pystac writes .json files
    if args.checkpoint_every <= 0:
        for item in list(collection.get_items()):
            item.set_self_href(f"{catalog_id}/{item.id}/{item.id}.json")

    s3_utils.update_collection(collection, catalog_id, args.catalog_path, args.bucket_name)
    collection.validate()

    asset_handler.upload_modified_parquet(remove_local=True)


if __name__ == "__main__":
    start_time = time.time()
    try:
        multiprocessing.set_start_method("spawn", force=True)
        main()
    finally:
        end_time = time.time()
        elapsed_time = end_time - start_time
        hours, rem = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(rem, 60)
        logging.info(f"Total execution time: {int(hours)}h {int(minutes)}m {seconds:.2f}s")

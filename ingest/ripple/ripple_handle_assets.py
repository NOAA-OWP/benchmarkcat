import copy
import json
import logging
import os
import tempfile
from typing import Any, Dict, List

import geopandas as gpd
import pandas as pd
import rasterio
from shapely.geometry import shape

from ingest.flows import FlowfileUtils
from ingest.utils import RasterUtils, S3Utils

from .ripple_stac import RasterHandler, RippleInfo


class RippleFIMAssetHandler:
    def __init__(self, s3_utils, bucket_name, derived_metadata_path) -> None:
        self.s3_utils = s3_utils
        self.bucket_name = bucket_name
        self.derived_metadata_path = derived_metadata_path
        self.results_file = "ripple_fim_collection.parquet"
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.local_results_file = os.path.join(script_dir, self.results_file)
        self.results_df = self.load_results()

    def load_results(self):
        try:
            self.s3_utils.s3_client.download_file(self.bucket_name, self.derived_metadata_path, self.local_results_file)
            logging.info(f"Successfully downloaded {self.derived_metadata_path}")
        except Exception as e:
            logging.warning(f"Failed to download {self.derived_metadata_path}: {e}")
            logging.info("Creating new local results file.")

        if os.path.exists(self.local_results_file):
            return pd.read_parquet(self.local_results_file)
        else:
            columns = {
                "item_path": pd.Series(dtype="str"),
                "source": pd.Series(dtype="str"),
                "geometry": pd.Series(dtype="str"),
                "bbox": pd.Series(dtype="str"),
                "magnitudes": pd.Series(dtype="str"),
                "extent_areas": pd.Series(dtype="str"),
                "wkt2_string": pd.Series(dtype="str"),
                "thumbnail": pd.Series(dtype="str"),
                "resolution": pd.Series(dtype="float"),
            }
            return pd.DataFrame(columns)

    def assets_processed(self, item_path: str) -> bool:
        """Check if assets for this item path have been processed"""
        return item_path in self.results_df["item_path"].values

    def read_data_parquet(self, item_path: str) -> Dict[str, Any]:
        """Read processed data for an item path from the parquet file"""
        row = self.results_df[self.results_df["item_path"] == item_path]
        if not row.empty:
            result = row.to_dict(orient="records")[0]
            # Convert JSON strings back to objects
            for field in ["geometry", "bbox", "magnitudes", "extent_areas"]:
                if result.get(field):
                    try:
                        result[field] = json.loads(result[field])
                    except (json.JSONDecodeError, TypeError):
                        logging.warning(f"Could not decode {field} for {item_path}")
            return result
        return {}

    def handle_assets(self, item_path: str, source: str) -> Dict[str, Any]:
        results = {}
        magnitudes = []
        extent_areas = {}

        # Get list of tiff files and their magnitudes
        tiff_files = self.s3_utils.list_files_with_extensions(self.bucket_name, item_path, [".tif"])

        # Create thumbnail from first extent raster
        if tiff_files:
            first_extent_path = tiff_files[0]
            thumbnail_path = self.create_and_add_thumbnail(first_extent_path)
        else:
            thumbnail_path = None

        # Process first tiff to get domain information that's consistent across magnitudes
        with tempfile.TemporaryDirectory() as tmpdir:
            first_tiff = tiff_files[0]
            local_tiff = os.path.join(tmpdir, os.path.basename(first_tiff))
            self.s3_utils.s3_client.download_file(self.bucket_name, first_tiff, local_tiff)

            # Get geometry, bbox, and model domain once
            convex_hull, bbox, domain = RasterHandler.create_domain_geometry(local_tiff)
            wkt2_string = RasterHandler.get_wkt2_string(local_tiff)

            # Create and upload gpkg for the model domain
            gpkg_name = "model_domain.gpkg"
            local_gpkg = os.path.join(tmpdir, gpkg_name)

            # Create GeoDataFrame with model domain MultiPolygon
            with rasterio.open(local_tiff) as src:
                gdf = gpd.GeoDataFrame({"geometry": [shape(domain)]}, crs=src.crs)
                gdf.to_file(local_gpkg, driver="GPKG")
                resolution = abs(src.res[0])

            s3_gpkg_path = os.path.join(item_path, gpkg_name)
            self.s3_utils.s3_client.upload_file(local_gpkg, self.bucket_name, s3_gpkg_path)

        # Process all tiffs just for magnitudes and extent areas
        for tiff in tiff_files:
            magnitude = os.path.basename(tiff).split("_")[0]
            magnitudes.append(magnitude)

            with tempfile.TemporaryDirectory() as tmpdir:
                local_tiff = os.path.join(tmpdir, os.path.basename(tiff))
                self.s3_utils.s3_client.download_file(self.bucket_name, tiff, local_tiff)

                extent_areas[magnitude] = RasterHandler.calculate_extent_area(local_tiff)

        results[item_path] = {
            "source": source,
            "geometry": convex_hull,
            "bbox": bbox,
            "magnitudes": magnitudes,
            "extent_areas": extent_areas,
            "wkt2_string": wkt2_string,
            "thumbnail": thumbnail_path,
            "resolution": resolution,
        }

        self.write_data_parquet(results)
        return results[item_path]

    def process_collection_flowfiles(self, asset_object_key: str) -> Dict:
        """Process collection-level CONUS flow files"""
        flowfile_ids = []
        flowfile_keys = []

        # Get all CONUS flow files
        flow_files = self.s3_utils.list_files_with_extensions(self.bucket_name, asset_object_key, [".csv"])

        for flow_file in flow_files:
            if "nwm_return_period_flows" in flow_file:
                flowfile_id = flow_file.replace(".csv", "")  # Remove .csv extension
                flowfile_ids.append(flowfile_id)
                flowfile_keys.append(flow_file)

        # Process flowfiles using existing utilities
        flowfile_dfs = FlowfileUtils.download_flowfiles(self.bucket_name, flowfile_keys, self.s3_utils.s3_client)
        flowstats_list = FlowfileUtils.extract_flowstats(flowfile_dfs)

        # Create flowfile object
        flowfile_object = FlowfileUtils.create_flowfile_object(flowfile_ids, flowstats_list, RippleInfo.columns_list)

        return {"flowfile_ids": flowfile_ids, "flowfile_keys": flowfile_keys, "flowfile_object": flowfile_object}

    def create_and_add_thumbnail(self, first_extent_path: str) -> str:
        """Create and upload a thumbnail for the first extent raster."""
        with tempfile.TemporaryDirectory() as tmpdir:
            local_extent_path = os.path.join(tmpdir, os.path.basename(first_extent_path))
            local_thumbnail_path = os.path.join(tmpdir, "thumbnail.png")

            # Download extent raster
            self.s3_utils.s3_client.download_file(self.bucket_name, first_extent_path, local_extent_path)

            # Create and upload thumbnail
            thumbnail_s3_path = self.s3_utils.make_and_upload_thumbnail(
                local_extent_path, local_thumbnail_path, self.bucket_name, first_extent_path
            )

            return thumbnail_s3_path

    def write_data_parquet(self, results):
        results_copy = copy.deepcopy(results)

        # Custom JSON encoder to handle numpy types
        class NumpyJSONEncoder(json.JSONEncoder):
            def default(self, obj):
                import numpy as np

                if isinstance(obj, np.integer):
                    return int(obj)
                if isinstance(obj, np.floating):
                    return float(obj)
                if isinstance(obj, np.ndarray):
                    return obj.tolist()
                return super().default(obj)

        for path, data in results_copy.items():
            # Ensure geometry and bbox are serialized
            if data.get("geometry"):
                data["geometry"] = json.dumps(data["geometry"])
            if data.get("bbox"):
                data["bbox"] = json.dumps(data["bbox"])
            if data.get("magnitudes"):
                data["magnitudes"] = json.dumps(data["magnitudes"])
            if data.get("extent_areas"):
                data["extent_areas"] = json.dumps(data["extent_areas"], cls=NumpyJSONEncoder)

        new_df = (
            pd.DataFrame.from_dict(results_copy, orient="index").reset_index().rename(columns={"index": "item_path"})
        )

        for item_path in new_df["item_path"]:
            self.results_df = self.results_df[self.results_df["item_path"] != item_path]

        self.results_df = pd.concat([self.results_df, new_df], ignore_index=True)
        self.results_df.to_parquet(self.local_results_file, index=False)

    def upload_modified_parquet(self):
        try:
            self.s3_utils.s3_client.upload_file(self.local_results_file, self.bucket_name, self.derived_metadata_path)
            logging.info(f"Successfully uploaded {self.local_results_file}")
        except Exception as e:
            logging.error(f"Failed to upload {self.local_results_file}: {e}")
        finally:
            if os.path.exists(self.local_results_file):
                os.remove(self.local_results_file)
                logging.info(f"Removed local file {self.local_results_file}")

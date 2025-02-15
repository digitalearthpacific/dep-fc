import os
import odc.geo.xr  # noqa: F401
from pathlib import Path
import requests
import re
import urllib
from xarray import Dataset
from geopandas import GeoDataFrame
import geopandas as gpd
import xarray as xr
from pystac_client import Client
import rasterio.features
from pystac.extensions.projection import ProjectionExtension as proj
from dep_tools.searchers import PystacSearcher, LandsatPystacSearcher
from dep_tools.loaders import OdcLoader as DEPLoader
import dep_tools.grids as grid

catalog = "https://earth-search.aws.element84.com/v1"
landsat_collection = "landsat-c2-l2"


def get_s2_data(
    aoi: GeoDataFrame,
    year="2024",
    cloud_cover=50,
) -> Dataset:

    # bbox = rasterio.features.bounds(aoi)
    bbox = aoi.to_crs(4326).boundingbox.bbox

    # client = Client.open(catalog)

    # Search for Landsat/S2 items
    searcher_ls = LandsatPystacSearcher(
        catalog=catalog,
        collections=landsat_collection,
        datetime=year,
        query={"eo:cloud_cover": {"lt": cloud_cover}},
    )

    ls_items = searcher_ls.search(aoi)

    print(f"LS Items : {len(ls_items)}")

    # Load STAC Items
    loader_ls = DEPLoader(
        chunks={"x": 2048, "y": 2048},
        groupby="solar_day",
        resampling={"qa_pixel": "nearest", "SCL": "nearest", "*": "cubic"},
        bands=["green", "red", "nir08", "swir16", "swir22", "qa_pixel"],
        fail_on_error=False,
    )

    ls_data = loader_ls.load(ls_items, aoi)

    # Cloud Mask
    bitflags = 0b00011000
    cloud_mask = (ls_data.qa_pixel & bitflags) != 0
    ls_data = ls_data.where(~cloud_mask).drop_vars("qa_pixel")

    # Scale and Offset
    ds_ls = (ls_data.where(ls_data.red != 0) * 0.0000275 + -0.2).clip(0, 1)

    # coastal clip
    ds_ls = get_clipped(ds_ls)

    ds_ls = ds_ls.odc.reproject(3832)

    ds_ls = cleanup(ds_ls)

    # add nodata
    for var in ds_ls.data_vars:
        ds_ls[var].attrs["nodata"] = -9999

    return ds_ls


def get_clipped(ds) -> GeoDataFrame:
    buffer = grid.gadm()
    buffer = buffer.to_crs(3832)
    try:
        ds = ds.rio.clip(buffer.geometry.values, buffer.crs, drop=True, invert=False)
    except:
        pass
    return ds


def cleanup(ds: Dataset) -> Dataset:
    # ds = ds.drop_vars("qa_count_clear")
    ds = ds.rename_vars({"nir08": "nir"})
    ds = ds.rename_vars({"swir16": "swir1"})
    ds = ds.rename_vars({"swir22": "swir2"})
    return ds


def write_locally(ds, tile_id, year) -> None:
    output_dir = f"data/{tile_id}/{year}"
    os.makedirs(output_dir, exist_ok=True)
    ds_prepared = prepare_for_export(ds, output_location=output_dir)


def download_if_not_exists(url, filepath):
    # Downloads a JSON file from a URL if it doesn't already exist locally.
    if not os.path.exists(filepath):
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
            with open(filepath, mode="wb") as file:
                for chunk in response.iter_content(chunk_size=10 * 1024):
                    file.write(chunk)
            print(f"Geo file downloaded and saved to: {filepath}")
        except requests.exceptions.RequestException as e:
            print(f"Error downloading JSON: {e}")
        except Exception as e:  # Catch other potential errors (like file writing)
            print(f"An unexpected error occurred: {e}")
    else:
        pass

from typing import Any

import dagster as dg
import numpy as np
import rasterio as rio
from dagster import ConfigurableResource, asset
from pystac import Item
from pystac_client import Client


class STACResource(ConfigurableResource):
    """
    Dagster resource which wraps pystac_client.
    """
    catalog_url: str

    def get_client(self):
        return Client.open(self.catalog_url)

COG_ASSET_KEY = "Cloud optimized GeoTiff"

@asset
def fetch_stac_item(stac_client: STACResource) -> Item:
    """
    Find a Wyvern HSI scene in cloud storage. Returns a STAC Item.
    """
    # TODO: configure the spatio-temporal search from the environment or pipeline args.

    # TODO: implement actual search logic. The STAC is a static catalog which does not confirm to
    # ITEM_SEARCH. You can use client.get_collections() and collection.get_items() to iterate, and
    # then filter items by properties (like datetime and bbox) yourself.
    
    # hardcoded shortcut to Tampico, MX scene which was used in Part 1 blog:
    item_url = "https://wyvern-prod-public-open-data-program.s3.ca-central-1.amazonaws.com/wyvern_dragonette-003_20241229T165203_12324bcb/wyvern_dragonette-003_20241229T165203_12324bcb.json"
    result = Item.from_file(item_url)
    cog_asset = result.assets.get(COG_ASSET_KEY) 
    if cog_asset is None:
        raise ValueError(f"{COG_ASSET_KEY} asset not found in STAC item")
    return result


@dg.asset
def nodata_value(fetch_stac_item: Item) -> float:
    """
    Confirm all the bands have same nodata value, so we can just use a common variable for all
    bands.
    """
    cog_asset = fetch_stac_item.assets.get(COG_ASSET_KEY) 
    if cog_asset is None:
        raise ValueError("Cloud Optimized GeoTiff asset not found in STAC item.")

    with rio.open(cog_asset.href) as src:
        for i in range(0, 31):
            assert src.nodatavals[i] == src.nodatavals[0]
        return src.nodatavals[0]


@dg.asset
def blue_r464_raster(fetch_stac_item: Item) -> tuple[np.ndarray, dict[str, Any]]:
    """
    Read band from cloud object storage (Blue 464nm for RGB preview). Returns tuple of (Numpy
    ndarray, and raster metadata.)
    """
    cog_asset = fetch_stac_item.assets[COG_ASSET_KEY]

    with rio.open(cog_asset.href) as src:
        band_blue_r464 = src.descriptions.index("Band_464")
        blue_r464 = src.read(band_blue_r464 + 1)
        src_meta: dict[str, object] = src.meta.copy()
        return (blue_r464, src_meta)

@dg.asset
def green_r550_raster(fetch_stac_item: Item) -> tuple[np.ndarray, dict[str, Any]]:
    """
    Read band from cloud object storage (Green 550nm for NDWI and RGB preview). Returns tuple of
    (Numpy ndarray, and raster metadata.)
    """
    cog_asset = fetch_stac_item.assets[COG_ASSET_KEY]

    with rio.open(cog_asset.href) as src:
        band_green_r550 = src.descriptions.index("Band_550")
        green_r550 = src.read(band_green_r550 + 1)
        src_meta: dict[str, object] = src.meta.copy()
        return (green_r550, src_meta)

@dg.asset
def red_r650_raster(fetch_stac_item: Item) -> tuple[np.ndarray, dict[str, Any]]:
    """
    Read band from cloud object storage (Red 650nm for RGB preview). Returns tuple of (Numpy
    ndarray, and raster metadata.)
    """
    cog_asset = fetch_stac_item.assets[COG_ASSET_KEY]
    with rio.open(cog_asset.href) as src:
        band_red_r650 = src.descriptions.index("Band_650")
        red_r650 = src.read(band_red_r650 + 1)
        src_meta: dict[str, object] = src.meta.copy()
        return (red_r650, src_meta)

@dg.asset
def red_r669_raster(fetch_stac_item: Item) -> tuple[np.ndarray, dict[str, Any]]:
    """
    Read band from cloud object storage (Dark Red 669nm for NDCI). Returns tuple of (Numpy ndarray,
    and raster metadata.)
    """
    cog_asset = fetch_stac_item.assets[COG_ASSET_KEY]
    with rio.open(cog_asset.href) as src:
        band_red_r669 = src.descriptions.index("Band_669")
        red_r669 = src.read(band_red_r669 + 1)
        src_meta: dict[str, object] = src.meta.copy()
        return (red_r669, src_meta)

@dg.asset
def red_edge_r712_raster(fetch_stac_item: Item) -> tuple[np.ndarray, dict[str, Any]]:
    """
    Read band from cloud object storage (Red Edge 712nm for NDCI). Returns tuple of (Numpy ndarray,
    and raster metadata.)
    """
    cog_asset = fetch_stac_item.assets[COG_ASSET_KEY]
    with rio.open(cog_asset.href) as src:
        band_red_edge_r712 = src.descriptions.index("Band_712")
        red_edge_r712 = src.read(band_red_edge_r712 + 1)
        src_meta: dict[str, object] = src.meta.copy()
        return (red_edge_r712, src_meta)

@dg.asset
def nir_r849_raster(fetch_stac_item: Item) -> tuple[np.ndarray, dict[str, Any]]:
    """
    Read band from cloud object storage (NIR 849nm for NDWI). Returns tuple of (Numpy ndarray, and
    raster metadata.)
    """
    cog_asset = fetch_stac_item.assets[COG_ASSET_KEY]
    with rio.open(cog_asset.href) as src:
        band_nir_r849 = src.descriptions.index("Band_849")
        nir_r849 = src.read(band_nir_r849 + 1)
        src_meta: dict[str, object] = src.meta.copy()
        return (nir_r849, src_meta)


@dg.asset
def visual_rgb_preview_raster(
    red_r650_raster: tuple[np.ndarray, dict[str, Any]],
    green_r550_raster: tuple[np.ndarray, dict[str, Any]],
    blue_r464_raster: tuple[np.ndarray, dict[str, Any]],
) -> tuple[np.ndarray, dict[str, Any]]:
    """
    Make visual RGB preview raster (EPSG:4326). Returns tuple of (Numpy ndarray, and
    raster metadata.)
    """
    (red_r650, _) = red_r650_raster
    (green_r550, _) = green_r550_raster
    (blue_r464, src_meta) = blue_r464_raster

    # Scale the RGB bands to 8-bit unsigned integer range
    red_uint8 = scale_to_8bit(red_r650, 0)
    green_uint8 = scale_to_8bit(green_r550, 0)
    blue_uint8 = scale_to_8bit(blue_r464, 0)

    # Stack the bands to create RGB image
    # Make a copy of the input arrays to avoid modifying the originals
    result_arr = np.stack([red_uint8, green_uint8, blue_uint8], axis=0)

    # Reshape from scientific to image format (bands, height, width) -> (height, width, bands)
    result_arr = np.transpose(result_arr, (1, 2, 0))
    return (result_arr, src_meta)


# @dg.asset(deps=[visual_rgb_preview_raster])
# def visual_rgb_preview_geotiff(
#     context: dg.AssetExecutionContext,
# ) -> dg.MaterializeResult:
#     """
#     Make geotiff files for visual RGB preview raster.
#     """


# @dg.asset(deps=[visual_rgb_preview_geotiff])
# def visual_rgb_preview_geotiff_for_web(
#     context: dg.AssetExecutionContext,
# ) -> dg.MaterializeResult:
#     """
#     Transform visual RGB preview geotiff for web (EPSG:3857).
#     """
#     pass


# @dg.asset()
# def ndwi_raster(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
#     """
#     Calculate NDWI index raster.
#     """
#     pass


# @dg.asset(deps=[ndwi_raster])
# def water_mask_raster(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
#     """
#     Make water mask raster from NDWI threshold.
#     """
#     pass


# @dg.asset(deps=[water_mask_raster])
# def water_mask_geotiff(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
#     """
#     Make geotiff files from water mask raster.
#     """
#     pass


# @dg.asset(deps=[water_mask_geotiff])
# def water_mask_geotiff_for_web(
#     context: dg.AssetExecutionContext,
# ) -> dg.MaterializeResult:
#     """
#     Transform water mask geotiff for web (EPSG:3857)
#     """
#     pass


# @dg.asset(deps=[water_mask_raster])
# def water_mask_geojson(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
#     """
#     Polygonize water mask raster and make GeoJSON file.
#     """
#     pass


# @dg.asset(deps=[raw_raster_bands, water_mask_raster, validity_masks])
# def ndci_raster(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
#     """
#     Make NDCI raster from raw bands, water mask, and validity masks.
#     """
#     pass


# @dg.asset(deps=[ndci_raster])
# def ndci_geotiff(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
#     """
#     Make Geotiffs from NDCI raster (EPSG: 4326).
#     """
#     pass


# @dg.asset(deps=[ndci_geotiff])
# def ndci_geotiff_for_web(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
#     """
#     Transform NDCI geotiff for web (EPSG:3857).
#     """
#     pass

def scale_to_8bit(arr: np.ndarray, nodata_val: int = 0) -> np.ndarray:
    """
    Scale a numpy array to 8-bit (0-255) range using percentile-based contrast stretching.

    Parameters:
        arr (numpy.ndarray): Input array with potential NaN values
        nodata_val (int): Value to use for NaN areas in output (default: 0)
        
    Returns:
        numpy.ndarray: 8-bit unsigned integer array with NaN values replaced by nodata_val
    """
    # Create a copy to avoid modifying the input
    arr_copy = arr.copy()
    # Create a mask for valid values
    valid_mask = ~np.isnan(arr_copy)
    # Initialize output array with nodata value
    output = np.full_like(arr_copy, nodata_val, dtype=np.uint8)
    # Process only if we have valid data
    if np.any(valid_mask):
        # Calculate percentiles only on valid data
        min_val = np.nanpercentile(arr_copy, 2)  # 2nd percentile
        max_val = np.nanpercentile(arr_copy, 98)  # 98th percentile
        # Apply scaling only to valid data
        if max_val > min_val:  # Avoid division by zero
            if nodata_val == 0:
                # Scale to 1-255 range instead of 0-255
                scaled_valid = (arr_copy[valid_mask] - min_val) / (max_val - min_val) * 254 + 1
            else:
                scaled_valid = (arr_copy[valid_mask] - min_val) / (max_val - min_val) * 255
            scaled_valid = np.clip(scaled_valid, 1, 255)
            output[valid_mask] = scaled_valid.astype(np.uint8)
    return output

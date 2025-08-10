import subprocess
import tempfile
from pathlib import Path
from typing import Any

import dagster as dg
import geopandas as gpd
import matplotlib
import numpy as np
import rasterio as rio
from dagster import asset
from pystac import Item
from rasterio.features import shapes
from shapely.geometry import shape

from .utils import STACResource, file_size, scale_to_8bit

COG_ASSET_KEY = "Cloud optimized GeoTiff"

EPSG_4326="EPSG:4326"
"""
WGS84 (decimal degrees) CRS
"""

EPSG_3857="EPSG:3857"
"""
Web-mercator CRS
"""

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
def rgb_preview_raster(
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

    # Stack the bands to create RGB image (np.stack returns a copy)
    result_arr = np.stack([red_uint8, green_uint8, blue_uint8], axis=0)

    return (result_arr, src_meta)


@dg.asset
def rgb_preview_geotiff(
    context: dg.AssetExecutionContext,
    fetch_stac_item: Item,
    rgb_preview_raster: tuple[np.ndarray, dict[str, Any]],
) -> dg.MaterializeResult:
    """
    Write visual RGB preview raster to GeoTiff file.
    """
    io_manager = context.resources.io_manager
    if hasattr(io_manager, "base_dir"):
        base_dir = Path(io_manager.base_dir)
    else:
        base_dir = Path(tempfile.gettempdir())
    output_dir = base_dir / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{fetch_stac_item.id}-l1b-viz.tiff"
    output_path.unlink(missing_ok=True)

    (result_arr, src_meta) = rgb_preview_raster

    # re-use the metadata from the src, except update the datatype (uint8) and the nodata value (0).
    dest_meta = src_meta.copy()
    dest_meta = src_meta.copy()
    dest_meta.update(
        dict(
            count=len(result_arr),
            dtype=result_arr.dtype,
            nodata=0,
        )
    )

    with rio.open(output_path, "w", **dest_meta) as dst:
        dst.write(result_arr)

    return dg.MaterializeResult(
        metadata={
            "crs": str(dest_meta["crs"]),
            "dimensions": f"{result_arr.shape[1]} x {result_arr.shape[0]}",
            "dtype": str(result_arr.dtype),
            "file_size": file_size(output_path),
            "output_path": str(output_path),
        }
    )


@dg.asset(deps=["rgb_preview_geotiff"])
def rgb_preview_web_geotiff(
    context: dg.AssetExecutionContext,
    fetch_stac_item: Item,
) -> dg.MaterializeResult:
    io_manager = context.resources.io_manager
    if hasattr(io_manager, "base_dir"):
        base_dir = Path(io_manager.base_dir)
    else:
        base_dir = Path(tempfile.gettempdir())
    output_dir = base_dir / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{fetch_stac_item.id}-l1b-viz-web.tiff"
    output_path.unlink(missing_ok=True)

    # Use gdalwarp to re-project the GeoTIFF to EPSG:3857 for web visualization
    # TODO: optionally port this to use rasterio and remove the gdal CLI dependency.

    input_path = output_dir / f"{fetch_stac_item.id}-l1b-viz.tiff"

    cmd = [
        "gdalwarp",
        "-t_srs",
        EPSG_3857,
        "-r",
        "bilinear",
        "-of",
        "GTiff",
        "-co",
        "COMPRESS=LZW",
        "-co",
        "BLOCKXSIZE=256",
        "-co",
        "BLOCKYSIZE=256",
        str(input_path),
        str(output_path),
    ]
    subprocess.run(cmd, check=True)

    return dg.MaterializeResult(
        metadata={
            "output_path": str(output_path),
            "file_size": file_size(output_path),
            "crs": EPSG_3857,
        }
    )


@dg.asset
def ndwi_raster(
    green_r550_raster: tuple[np.ndarray, dict[str, Any]],
    nir_r849_raster: tuple[np.ndarray, dict[str, Any]],
    nodata_value: float,
) -> tuple[np.ndarray, dict[str, Any]]:
    """
    Calculate NDWI index raster.
    """
    (green_r550, src_meta) = green_r550_raster
    (nir_r849, _) = nir_r849_raster

    # generate validity masks
    green_r550[green_r550 == nodata_value] = np.nan
    green_r550_valid_mask = ~np.isnan(green_r550)
    nir_r849[nir_r849 == nodata_value] = np.nan
    nir_r849_valid_mask = ~np.isnan(nir_r849)
    valid_mask = green_r550_valid_mask & nir_r849_valid_mask

    # ndwi formula
    nir_r849_clamped = np.clip(nir_r849, 0, None)
    ndwi = np.full_like(green_r550, np.nan)
    ndwi[valid_mask] = (green_r550[valid_mask] - nir_r849_clamped[valid_mask]) / (
        green_r550[valid_mask] + nir_r849_clamped[valid_mask]
    )

    return (ndwi, src_meta)


@dg.asset
def water_mask_raster(
    ndwi_raster: tuple[np.ndarray, dict[str, Any]],
) -> tuple[np.ndarray, dict[str, Any]]:
    """
    Make water mask raster from NDWI threshold value (0.3)
    """
    (ndwi, src_meta) = ndwi_raster
    water_threshold = 0.3
    water_mask = np.zeros_like(ndwi, dtype=bool)
    water_mask = ndwi > water_threshold
    return (water_mask, src_meta)


@dg.asset
def water_mask_vector_geojson(
    context: dg.AssetExecutionContext,
    fetch_stac_item: Item,
    water_mask_raster: tuple[np.ndarray, dict[str, Any]],
) -> dg.MaterializeResult:
    io_manager = context.resources.io_manager
    if hasattr(io_manager, "base_dir"):
        base_dir = Path(io_manager.base_dir)
    else:
        base_dir = Path(tempfile.gettempdir())
    output_dir = base_dir / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{fetch_stac_item.id}-l1b-water-mask.geojson"
    output_path.unlink(missing_ok=True)

    (water_mask, src_meta) = water_mask_raster

    results = (
        {"properties": {"value": v}, "geometry": s}
        for s, v in shapes(
            water_mask.astype(np.uint8),
            mask=water_mask,
            transform=src_meta["transform"],
        )
    )
    geometries = [
        shape(result["geometry"])
        for result in results
        if result["properties"]["value"] == 1
    ]
    gdf = gpd.GeoDataFrame(geometry=geometries, crs=src_meta["crs"])
    gdf.to_file(output_path, driver="GeoJSON")

    return dg.MaterializeResult(
        metadata={
            "output_path": str(output_path),
            "file_size": file_size(output_path),
            "crs": EPSG_4326,
        }
    )


@dg.asset
def water_mask_geotiff(
    context: dg.AssetExecutionContext,
    fetch_stac_item: Item,
    water_mask_raster: tuple[np.ndarray, dict[str, Any]],
) -> dg.MaterializeResult:
    """
    Make geotiff files from water mask raster.
    """
    io_manager = context.resources.io_manager
    if hasattr(io_manager, "base_dir"):
        base_dir = Path(io_manager.base_dir)
    else:
        base_dir = Path(tempfile.gettempdir())
    output_dir = base_dir / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{fetch_stac_item.id}-l1b-water-mask.tiff"
    output_path.unlink(missing_ok=True)

    (water_mask, src_meta) = water_mask_raster

    with rio.open(
        output_path,
        "w",
        driver="GTiff",
        height=water_mask.shape[0],
        width=water_mask.shape[1],
        count=1,
        dtype="uint8",
        crs=src_meta["crs"],
        transform=src_meta["transform"],
        compress="lzw",
    ) as dst:
        dst.write(water_mask.astype("uint8"), 1)

    return dg.MaterializeResult(
        metadata={
            "output_path": str(output_path),
            "dimensions": f"{water_mask.shape[1]} x {water_mask.shape[0]}",
            "dtype": str(water_mask.dtype),
            "file_size": file_size(output_path),
            "crs": str(src_meta["crs"]),
        }
    )


@dg.asset
def water_mask_viz_geotiff(
    context: dg.AssetExecutionContext,
    fetch_stac_item: Item,
    water_mask_raster: tuple[np.ndarray, dict[str, Any]],
) -> dg.MaterializeResult:
    """
    Make Geotiff raster for visualization (3 bands RGB).
    """
    io_manager = context.resources.io_manager
    if hasattr(io_manager, "base_dir"):
        base_dir = Path(io_manager.base_dir)
    else:
        base_dir = Path(tempfile.gettempdir())
    output_dir = base_dir / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{fetch_stac_item.id}-l1b-water-mask-viz.tiff"
    output_path.unlink(missing_ok=True)

    (water_mask, src_meta) = water_mask_raster

    water_mask_rgb = np.zeros(
        (3, water_mask.shape[0], water_mask.shape[1]), dtype=np.uint8
    )

    # Set water pixels to blue-ish (1,1,255). Note: don't use 0 because that's the nodata value.
    water_mask_rgb[0, water_mask] = 1
    water_mask_rgb[1, water_mask] = 1
    water_mask_rgb[2, water_mask] = 255

    # Write the RGB water mask to a Geotiff
    with rio.open(
        output_path,
        "w",
        driver="GTiff",
        height=water_mask.shape[0],
        width=water_mask.shape[1],
        count=3,
        dtype="uint8",
        crs=src_meta["crs"],
        transform=src_meta["transform"],
        compress="lzw",
        photometric="RGB",
        nodata=0,
    ) as dst:
        dst.write(water_mask_rgb)

    return dg.MaterializeResult(
        metadata={
            "crs": str(src_meta["crs"]),
            "dimensions": f"{water_mask.shape[1]} x {water_mask.shape[0]}",
            "dtype": str(water_mask.dtype),
            "file_size": file_size(output_path),
            "output_path": str(output_path),
        }
    )


@dg.asset(deps=["water_mask_viz_geotiff"])
def water_mask_viz_web_geotiff(
    context: dg.AssetExecutionContext,
    fetch_stac_item: Item,
) -> dg.MaterializeResult:

    io_manager = context.resources.io_manager
    if hasattr(io_manager, "base_dir"):
        base_dir = Path(io_manager.base_dir)
    else:
        base_dir = Path(tempfile.gettempdir())
    output_dir = base_dir / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{fetch_stac_item.id}-l1b-water-mask-viz-web.tiff"
    output_path.unlink(missing_ok=True)

    input_path = output_dir / f"{fetch_stac_item.id}-l1b-water-mask-viz.tiff"

    # Use gdalwarp to re-project the GeoTIFF to EPSG:3857 for web visualization
    # TODO: port this to use rasterio and remove the gdal CLI dependency.
    cmd = [
        "gdalwarp",
        "-t_srs",
        EPSG_3857,
        "-r",
        "bilinear",
        "-of",
        "GTiff",
        "-co",
        "COMPRESS=LZW",
        "-co",
        "BLOCKXSIZE=256",
        "-co",
        "BLOCKYSIZE=256",
        str(input_path),
        str(output_path),
    ]
    subprocess.run(cmd, check=True)

    return dg.MaterializeResult(
        metadata={
            "output_path": str(output_path),
            "file_size": file_size(output_path),
            "crs": EPSG_3857,
        }
    )


@dg.asset
def ndci_raster(
    context: dg.AssetExecutionContext,
    red_r669_raster: tuple[np.ndarray, dict[str, Any]],
    red_edge_r712_raster: tuple[np.ndarray, dict[str, Any]],
    water_mask_raster: tuple[np.ndarray, dict[str, Any]],
    nodata_value: float,
) -> tuple[np.ndarray, dict[str, Any]]:
    """
    Make NDCI raster from red_edge, and dark red, water mask, and validity masks.
    """
    (red_edge_r712, src_meta) = red_edge_r712_raster
    (red_r669, _) = red_r669_raster
    (water_mask, _) = water_mask_raster

    # calculate validity masks
    red_r669[red_r669 == nodata_value] = np.nan
    red_r669_valid_mask = ~np.isnan(red_r669)
    red_edge_r712[red_edge_r712 == nodata_value] = np.nan
    red_edge_r712_valid_mask = ~np.isnan(red_edge_r712)

    # calculate NDCI formula
    red_edge_r712_clamped = np.clip(red_edge_r712, 0, None)
    valid_mask = red_edge_r712_valid_mask & red_r669_valid_mask & water_mask
    ndci = np.full_like(red_r669, np.nan)
    ndci[valid_mask] = (red_edge_r712_clamped[valid_mask] - red_r669[valid_mask]) / (
        red_edge_r712_clamped[valid_mask] + red_r669[valid_mask]
    )

    return (ndci, src_meta)


@dg.asset
def ndci_geotiff(
    context: dg.AssetExecutionContext,
    fetch_stac_item: Item,
    ndci_raster: tuple[np.ndarray, dict[str, Any]],
) -> dg.MaterializeResult:
    """
    Make Geotiff from NDCI raster (EPSG: 4326).
    """
    io_manager = context.resources.io_manager
    if hasattr(io_manager, "base_dir"):
        base_dir = Path(io_manager.base_dir)
    else:
        base_dir = Path(tempfile.gettempdir())
    output_dir = base_dir / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{fetch_stac_item.id}-l1b-ndci.tiff"
    output_path.unlink(missing_ok=True)

    (ndci, src_meta) = ndci_raster

    with rio.open(
        output_path,
        "w",
        driver="GTiff",
        height=ndci.shape[0],
        width=ndci.shape[1],
        count=1,
        dtype=ndci.dtype,
        crs=src_meta["crs"],
        transform=src_meta["transform"],
        compress="lzw",
    ) as dst:
        dst.write(ndci, 1)

    return dg.MaterializeResult(
        metadata={
            "crs": str(src_meta["crs"]),
            "dimensions": f"{ndci.shape[1]} x {ndci.shape[0]}",
            "dtype": str(ndci.dtype),
            "file_size": file_size(output_path),
            "output_path": str(output_path),
        }
    )


@dg.asset
def ndci_viz_geotiff(
    context: dg.AssetExecutionContext,
    fetch_stac_item: Item,
    ndci_raster: tuple[np.ndarray, dict[str, Any]],
) -> dg.MaterializeResult:
    """
    Make Geotiff from NDCI raster, RGB visualization, web-ready (EPSG: 4326).
    """
    io_manager = context.resources.io_manager
    if hasattr(io_manager, "base_dir"):
        base_dir = Path(io_manager.base_dir)
    else:
        base_dir = Path(tempfile.gettempdir())
    output_dir = base_dir / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{fetch_stac_item.id}-l1b-ndci-viz.tiff"
    output_path.unlink(missing_ok=True)

    (ndci, src_meta) = ndci_raster

    # Normalize the NDCI values to the range [0, 1] for the colormap
    ndci_normalized = np.full_like(ndci, np.nan)
    valid_ndci_mask = ~np.isnan(ndci)
    ndci_min, ndci_max = -1, 0.3
    ndci_normalized[valid_ndci_mask] = (ndci[valid_ndci_mask] - ndci_min) / (
        ndci_max - ndci_min
    )
    ndci_normalized = np.clip(ndci_normalized, 0, 1)

    # Map the normalized NDCI values to RGB using the viridis colormap
    viridis_cmap = matplotlib.colormaps["viridis"]  # type: ignore
    ndci_rgb = np.full((3, ndci.shape[0], ndci.shape[1]), 0, dtype=np.uint8)
    ndci_rgb[:, valid_ndci_mask] = (
        viridis_cmap(ndci_normalized[valid_ndci_mask])[:, :3] * 255
    ).T.astype(np.uint8)

    with rio.open(
        output_path,
        "w",
        driver="GTiff",
        height=ndci.shape[0],
        width=ndci.shape[1],
        count=3,
        dtype=str(ndci_rgb.dtype),
        crs=src_meta["crs"],
        transform=src_meta["transform"],
        compress="lzw",
        photometric="RGB",
        nodata=0,
    ) as dst:
        dst.write(ndci_rgb)

    return dg.MaterializeResult(
        metadata={
            "crs": str(src_meta["crs"]),
            "dimensions": f"{ndci.shape[1]} x {ndci.shape[0]}",
            "dtype": str(ndci.dtype),
            "file_size": file_size(output_path),
            "output_path": str(output_path),
        }
    )


@dg.asset(deps=["ndci_viz_geotiff"])
def ndci_viz_web_geotiff(
    context: dg.AssetExecutionContext,
    fetch_stac_item: Item,
) -> dg.MaterializeResult:
    """
    Make Geotiff from NDCI raster (visualization, RGB, web-ready)
    """
    io_manager = context.resources.io_manager
    if hasattr(io_manager, "base_dir"):
        base_dir = Path(io_manager.base_dir)
    else:
        base_dir = Path(tempfile.gettempdir())
    output_dir = base_dir / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{fetch_stac_item.id}-l1b-ndci-viz-web.tiff"
    output_path.unlink(missing_ok=True)

    input_path = output_dir / f"{fetch_stac_item.id}-l1b-ndci-viz.tiff"

    # Use gdalwarp to re-project the GeoTIFF to EPSG:3857 for web visualization
    # TODO: port this to use rasterio and remove the gdal CLI dependency.
    cmd = [
        "gdalwarp",
        "-t_srs",
        EPSG_3857,
        "-r",
        "bilinear",
        "-of",
        "GTiff",
        "-co",
        "COMPRESS=LZW",
        "-co",
        "BLOCKXSIZE=256",
        "-co",
        "BLOCKYSIZE=256",
        str(input_path),
        str(output_path),
    ]
    subprocess.run(cmd, check=True)

    return dg.MaterializeResult(
        metadata={
            "output_path": str(output_path),
            "file_size": file_size(output_path),
            "crs": EPSG_3857,
        }
    )

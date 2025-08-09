import dagster as dg
from dagster import ConfigurableResource, asset, resource
from pystac import Item
from pystac_client import Client
import numpy as np

class STACResource(ConfigurableResource):
    catalog_url: str

    def get_client(self):
        return Client.open(self.catalog_url)

COG_ASSET_KEY = "Cloud optimized GeoTiff"

@asset
def fetch_stac_item(stac_client: STACResource) -> Item | None:
    """
    Find a URL of a Wyvern HSI scene in cloud storage.
    """
    # TODO: configure the spatio-temporal search from the environment or pipeline args.

    # TODO: implement actual search logic. The STAC is a static catalog which does not confirm to
    # ITEM_SEARCH. You can use client.get_collections() and collection.get_items() to iterate, and
    # then filter items by properties (like datetime and bbox) yourself.

    # client = stac_client.get_client()
    # # Traverse the static catalog manually
    # for collection in client.get_collections():
    #     if collection.id == "Extended VNIR":
    #         for item in collection.get_items():
    #             # Optionally filter by bbox/datetime here
    #             # Example: check item.datetime or item.bbox
    #             cog = item.assets.get("Cloud optimized GeoTiff")
    #             if cog:
    #                 return cog.href

    # shortcut to Tampico, MX scene which was used in Part 1 blog
    item_url = "https://wyvern-prod-public-open-data-program.s3.ca-central-1.amazonaws.com/wyvern_dragonette-003_20241229T165203_12324bcb/wyvern_dragonette-003_20241229T165203_12324bcb.json"
    return Item.from_file(item_url)

import rasterio as rio

@dg.asset
def nodata_value(fetch_stac_item: Item) -> float:
    """
    Confirm all the bands have same nodata value, so we can just use a common variable for all bands.
    """
    cog_asset = fetch_stac_item.assets.get(COG_ASSET_KEY) 
    if cog_asset is None:
        raise ValueError("Cloud Optimized GeoTiff asset not found in STAC item.")

    with rio.open(cog_asset.href) as src:
        for i in range(0, 31):
            assert src.nodatavals[i] == src.nodatavals[0]
        return src.nodatavals[0]


@dg.asset(deps=[fetch_stac_item])
def raw_raster_bands(context: dg.AssetExecutionContext) -> np.ndarray:
    """
    Read 6 of 32 raster bands from cloud object storage.
    """
    with rio.open(tampico_mx_url) as src:

        # Blue 464nm for RGB preview
        band_blue_r464 = src.descriptions.index("Band_464")
        blue_r464 = src.read(band_blue_r464 + 1)

        # Green 550nm for NDWI and RGB preview
        band_green_r550 = src.descriptions.index("Band_550")
        green_r550 = src.read(band_green_r550 + 1)

        # Red 650nm for RGB preview
        band_red_r650 = src.descriptions.index("Band_650")
        red_r650 = src.read(band_red_r650 + 1)

        # Dark Red 669nm for NDCI
        band_red_r669 = src.descriptions.index("Band_669")
        red_r669 = src.read(band_red_r669 + 1)

        # Red Edge 712nm for NDCI
        band_red_edge_r712 = src.descriptions.index("Band_712")
        red_edge_r712 = src.read(band_red_edge_r712 + 1)

        # NIR 849nm for NDWI
        band_nir_r849 = src.descriptions.index("Band_849")
        nir_r849 = src.read(band_nir_r849 + 1)

        # save scene metadata
        src_meta = src.meta.copy()

        # confirm all the bands have same nodata value, so we can just use a common variable for all bands
        for i in range(0, 31):
            assert src.nodatavals[i] == src.nodatavals[0]

        # save nodata value
        nodata_val = src.nodatavals[0]

        nodata_val


@dg.asset(deps=[raw_raster_bands])
def validity_masks(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    Make validity masks from raw raster bands.
    """

@dg.asset(deps=[raw_raster_bands])
def visual_rgb_preview_raster(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    """
    Make visual RGB preview raster (EPSG:4326).
    """
    pass


@dg.asset(deps=[visual_rgb_preview_raster])
def visual_rgb_preview_geotiff(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    """
    Make geotiff files for visual RGB preview raster.
    """


@dg.asset(deps=[visual_rgb_preview_geotiff])
def visual_rgb_preview_geotiff_for_web(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    """
    Transform visual RGB preview geotiff for web (EPSG:3857).
    """
    pass


@dg.asset(deps=[raw_raster_bands, validity_masks])
def ndwi_raster(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    Calculate NDWI index raster.
    """
    pass


@dg.asset(deps=[ndwi_raster])
def water_mask_raster(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    Make water mask raster from NDWI threshold.
    """
    pass


@dg.asset(deps=[water_mask_raster])
def water_mask_geotiff(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    Make geotiff files from water mask raster.
    """
    pass


@dg.asset(deps=[water_mask_geotiff])
def water_mask_geotiff_for_web(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    """
    Transform water mask geotiff for web (EPSG:3857)
    """
    pass


@dg.asset(deps=[water_mask_raster])
def water_mask_geojson(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    Polygonize water mask raster and make GeoJSON file.
    """
    pass


@dg.asset(deps=[raw_raster_bands, water_mask_raster, validity_masks])
def ndci_raster(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    Make NDCI raster from raw bands, water mask, and validity masks.
    """
    pass


@dg.asset(deps=[ndci_raster])
def ndci_geotiff(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    Make Geotiffs from NDCI raster (EPSG: 4326).
    """
    pass


@dg.asset(deps=[ndci_geotiff])
def ndci_geotiff_for_web(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    Transform NDCI geotiff for web (EPSG:3857).
    """
    pass

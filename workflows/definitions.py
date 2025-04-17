import os
from typing import Any

import dagster as dg
import rasterio as rio

# Prevent GDAL from looking for sidecar files
os.environ['GDAL_DISABLE_READDIR_ON_OPEN'] = 'TRUE'
os.environ['CPL_VSIL_CURL_ALLOWED_EXTENSIONS'] = '.tiff,.tif'

# For public buckets
os.environ['AWS_NO_SIGN_REQUEST'] = 'YES'

# Define band numbers for NDWI
GREEN_BAND = 9  # 550 nm
NIR_BAND = 31   # 869 nm


@dg.multi_asset(
    name="geotiff_reader",
    partitions_def=dg.DynamicPartitionsDefinition(name="uris"),
    outs={
        "green": dg.AssetOut(),
        "nir": dg.AssetOut(),
    },
)
def geotiff_reader(context: dg.AssetExecutionContext) -> dict[str, Any]:
    # The partition_key will be the URI or a slugified version of it
    uri = context.partition_key
    context.log.info(f"Processing GeoTIFF file: {uri}")

    with rio.open(uri) as src:
        green = src.read(GREEN_BAND)
        nir = src.read(NIR_BAND)
        context.log.info(f"GeoTIFF dimensions: {green.shape}")
    return {"green": green, "nir": nir}

@dg.asset
def hello(context: dg.AssetExecutionContext):
    context.log.info("Hello!")

@dg.asset(deps=[geotiff_reader])
def world(context: dg.AssetExecutionContext):
    context.log.info("World!")

@dg.resource(config_schema={"base_dir": str})
def fs_io_manager(init_context):
    return dg.fs_io_manager.configured({"base_dir": init_context.resource_config["base_dir"]})

defs = dg.Definitions(
    assets=[hello, world, geotiff_reader],
    resources={
        "fs_io_manager": fs_io_manager.configured({"base_dir": "./storage"})
    }
)

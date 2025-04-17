from dagster import materialize
from definitions import geotiff_reader

result = materialize(
    [geotiff_reader],
    run_config={
        "ops": {
            "geotiff_reader": {
                "config": {
                    # "uri": "/vsis3/wyvern-prod-public-open-data-program/wyvern_dragonette-003_20241229T165203_12324bcb/wyvern_dragonette-003_20241229T165203_12324bcb.tiff"
                    "uri": "wyvern_dragonette-003_20241229T165203_12324bcb.tiff"
                }
            }
        }
    }
)

from dagster import Definitions, load_assets_from_modules
from hab_detection_wyvern_hsi_dag.defs import assets, resources

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources={
        "stac_client": resources.STACResource(),
        "io_manager": resources.FilesystemIOManager(base_dir="/tmp/dagster/hab-detection-wyvern-hsi-dag"),
    },
)
from dagster import Definitions, FilesystemIOManager, load_assets_from_modules
from hab_detection_wyvern_hsi_dag.defs import assets, resources

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources={
        "stac_client": resources.STACResource(),
        "io_manager": FilesystemIOManager(base_dir="./dagster-data/hab-detection-wyvern-hsi-dag"),
    },
)

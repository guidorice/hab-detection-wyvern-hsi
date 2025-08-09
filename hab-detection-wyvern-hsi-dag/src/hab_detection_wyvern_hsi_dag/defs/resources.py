import dagster as dg

from dagster import ConfigurableResource, FilesystemIOManager
from pystac_client import Client

class STACResource(ConfigurableResource):
    """
    Dagster resource with a STAC client configured by default for Wyvern's open data program.
    """
    catalog_url: str = "https://wyvern-prod-public-open-data-program.s3.ca-central-1.amazonaws.com/catalog.json"
    
    def get_client(self):
        return Client.open(self.catalog_url)

@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "stac_client": STACResource(),
            "io_manager": FilesystemIOManager(base_dir="/tmp/dagster/hab-detection-wyvern-hsi-dag")
        }
    )

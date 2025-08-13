from dagster import ConfigurableResource
from pystac_client import Client


class STACResource(ConfigurableResource):
    """
    Dagster resource with a STAC client configured by default for Wyvern's open data program.
    """

    catalog_url: str = (
        "https://wyvern-prod-public-open-data-program.s3.ca-central-1.amazonaws.com/catalog.json"
    )

    def get_client(self) -> Client:
        return Client.open(self.catalog_url)

from fastapi import APIRouter
from pyiceberg.catalog import load_catalog

api_router = APIRouter(prefix="streamflow_observations")

catalog_settings: dict = {
    "type": "sql",
    "uri": "sqlite:///../../../../icefabric_manage/data/warehouse/pyiceberg_catalog.db",
    "warehouse": "file://../../../../icefabric_manage/data/warehouse/pyiceberg_catalog.db",
}


@api_router.get("/usgs_hourly")
async def get_gauge_data(gauge_id: str):
    """Returns the gauge data from a specific USGS gauge

    Parameters
    ----------
    gauge_id : str
        A Gauge ID (ex: 01563500)
    """
    namespace = "observations"
    catalog = load_catalog(namespace, **catalog_settings)
    observations = catalog.load_table(f"{namespace}.usgs_hourly")
    observations.scan()

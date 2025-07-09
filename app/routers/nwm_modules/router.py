from fastapi import APIRouter, Depends, Query
from pyiceberg.catalog import Catalog

from app import get_catalog
from icefabric.modules import get_sft_parameters
from icefabric.schemas import SFT, HydrofabricDomains

sft_router = APIRouter(prefix="/modules/sft")


@sft_router.get("/")
async def get_sft_ipes(
    identifier: str = Query(
        ...,
        description="Gauge ID to trace upstream catchments from",
        examples=["01010000"],
    ),
    domain: HydrofabricDomains = Query(
        HydrofabricDomains.CONUS, description="The iceberg namespace used to query the hydrofabric"
    ),
    use_schaake: bool = Query(
        False,
        description="Whether to use Schaake for the Ice Fraction Scheme. Defaults to False to use Xinanjiang",
    ),
    catalog: Catalog = Depends(get_catalog),
) -> list[SFT]:
    """An endpoint to return configurations for SFT

    Parameters
    ----------
    identifier : str, optional
        The Gauge ID to trace upstream from to get all catchments, by default Query( ..., description="Gauge ID to trace upstream catchments from", examples=["01010000"], )
    domain : HydrofabricDomains, optional
        The geographic domain to search for catchments from, by default Query( HydrofabricDomains.CONUS, description="The iceberg namespace used to query the hydrofabric" )
    use_schaake : bool, optional
        A parameter to determine if we're using Shaake or Xinanjiang to calculate ice fraction, by default Query( False, description="Whether to use Schaake for the Ice Fraction Scheme. Defaults to False to use Xinanjiang" )
    catalog : Catalog, optional
        The pyiceberg catalog, by default Depends(get_catalog)

    Returns
    -------
    list[SFT]
        A list of SFT pydantic objects for each catchment
    """
    return get_sft_parameters(
        catalog=catalog,
        domain=domain,
        identifier=identifier,
        use_schaake=use_schaake,
    )

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
        openapi_examples={"sft_example": {"summary": "SFT Example", "value": "01010000"}},
    ),
    domain: HydrofabricDomains = Query(
        HydrofabricDomains.CONUS,
        description="The iceberg namespace used to query the hydrofabric",
        openapi_examples={"sft_example": {"summary": "SFT Example", "value": "conus_hf"}},
    ),
    use_schaake: bool = Query(
        False,
        description="Whether to use Schaake for the Ice Fraction Scheme. Defaults to False to use Xinanjiang",
        openapi_examples={"sft_example": {"summary": "SFT Example", "value": False}},
    ),
    catalog: Catalog = Depends(get_catalog),
) -> list[SFT]:
    """
    An endpoint to return configurations for SFT.

    This endpoint traces upstream from a given gauge ID to get all catchments
    and returns SFT (Soil Freeze-Thaw) parameter configurations for each catchment.

    **Parameters:**
    - **identifier**: The Gauge ID to trace upstream from to get all catchments
    - **domain**: The geographic domain to search for catchments from
    - **use_schaake**: Determines if we're using Schaake or Xinanjiang to calculate ice fraction

    **Returns:**
    A list of SFT pydantic objects for each catchment
    """
    return get_sft_parameters(
        catalog=catalog,
        domain=domain,
        identifier=identifier,
        use_schaake=use_schaake,
    )

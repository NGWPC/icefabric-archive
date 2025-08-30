from fastapi import APIRouter, Path, Query

from icefabric.schemas import XsType

api_router = APIRouter(prefix="/ras_xs")


@api_router.get("/{identifier}/")
async def get_xs_subset_gpkg(
    identifier: str = Path(
        ...,
        description="HUC-8 identifier to filter by huc ID",
        examples=["02040106"],
        openapi_examples={"huc": {"summary": "XS Example", "value": "02040106"}},
    ),
    xstype: XsType = Query(XsType.MIP, description="The iceberg namespace used to query the cross-sections"),
):
    """
    Get geopackage subset from the mip xs iceberg catalog by table identifier (aka huc ID).

    This endpoint will query cross-sections from the mip xs iceberg catalog by huc & return
    the data subset as a downloadable geopackage file.

    """
    raise NotImplementedError("RAS-XS API not implemented")


@api_router.get("/{identifier}/dsreachid={ds_reach_id}")
async def get_xs_subset_by_huc_reach_gpkg(
    identifier: str = Path(
        ...,
        description="Identifier to filter data by huc ID",
        examples=["02040106"],
        openapi_examples={"xs": {"summary": "XS Example", "value": "02040106"}},
    ),
    ds_reach_id: str = Path(
        ...,
        description="Identifier to filter data by downstream reach ID)",
        examples=["4188251"],
        openapi_examples={"xs": {"summary": "XS Example", "value": "4188251"}},
    ),
    xstype: XsType = Query(XsType.MIP, description="The iceberg namespace used to query the cross-sections"),
):
    """
    Get geopackage subset from the mip xs iceberg catalog by reach ID at huc ID.

    This endpoint will query cross-sections from the mip xs iceberg catalog by
    downstream reach ID at given huc ID -- returning the data subset as a downloadable geopackage file.

    """
    raise NotImplementedError("RAS-XS API not implemented")

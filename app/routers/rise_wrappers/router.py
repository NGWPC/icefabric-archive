import os
import requests
from typing import Optional

from fastapi import APIRouter, HTTPException, Path, Query, Depends
from fastapi.responses import Response
from pydantic import BaseModel, Field, ConfigDict
from urllib.parse import urlencode

api_router = APIRouter(prefix="/rise")
EXT_RISE_BASE_URL = "https://data.usbr.gov/rise/api"
RISE_HEADERS = {"accept": "application/vnd.api+json"}
PARAM_CONV = {
    "orderUpdateDate": "order[updateDate]",
    "orderRecordTitle": "order[recordTitle]",
    "orderLocationName": "order[locationName]",
    "orderId": "order[id]",
    "orderFulltext": "order[fulltext]",
    "orderLikematch": "order[likematch]",
    "orderDateTime": "order[dateTime]",
    "updateDateBefore": "updateDate[before]",
    "updateDateStrictlyBefore": "updateDate[strictly_before]",
    "updateDateAfter": "updateDate[after]",
    "updateDateStrictlyAfter": "updateDate[strictly_after]",
    "dateTimeBefore": "dateTime[before]",
    "dateTimeStrictlyBefore": "dateTime[strictly_before]",
    "dateTimeAfter": "dateTime[after]",
    "dateTimeStrictlyAfter": "dateTime[strictly_after]",
    "catalogItemIsModeled": "catalogItem.isModeled"
}


class LocItemParams(BaseModel):
    page: Optional[int] = None
    itemsPerPage: Optional[int] = None
    id: Optional[str] = None
    stateId: Optional[str] = None
    regionId: Optional[str] = None
    locationTypeId: Optional[str] = None
    themeId: Optional[str] = None
    parameterId: Optional[str] = None
    parameterTimestepId: Optional[str] = None
    parameterGroupId: Optional[str] = None
    itemStructureId: Optional[str] = None
    unifiedRegionId: Optional[str] = None
    generationEffortId: Optional[str] = None
    search: Optional[str] = None
    catalogItemsIsModeled: Optional[bool] = None
    hasItemStructure: Optional[bool] = None
    hasCatalogItems: Optional[bool] = None
    orderUpdateDate: Optional[str] = None
    orderLocationName: Optional[str] = None
    orderId: Optional[str] = None
    orderFulltext: Optional[str] = None
    orderLikematch: Optional[str] = None


class CatItemParams(BaseModel):
    page: Optional[int] = None
    itemsPerPage: Optional[int] = None
    id: Optional[str] = None
    hasItemStructure: Optional[bool] = None


class CatRecParams(BaseModel):
    page: Optional[int] = None
    itemsPerPage: Optional[int] = None
    updateDateBefore: Optional[str] = None
    updateDateStrictlyBefore: Optional[str] = None
    updateDateAfter: Optional[str] = None
    updateDateStrictlyAfter: Optional[str] = None
    id: Optional[str] = None
    stateId: Optional[str] = None
    regionId: Optional[str] = None
    unifiedRegionId: Optional[str] = None
    locationTypeId: Optional[str] = None
    themeId: Optional[str] = None
    itemStructureId: Optional[str] = None
    generationEffortId: Optional[str] = None
    search: Optional[str] = None
    hasItemStructure: Optional[bool] = None
    hasCatalogItems: Optional[bool] = None
    orderUpdateDate: Optional[str] = None
    orderRecordTitle: Optional[str] = None
    orderId: Optional[str] = None
    orderFulltext: Optional[str] = None
    orderLikematch: Optional[str] = None


class ResParams(BaseModel):
    page: Optional[int] = None
    itemsPerPage: Optional[int] = None
    id: Optional[str] = None
    itemId: Optional[str] = None
    modelRunId: Optional[str] = None
    locationId: Optional[str] = None
    parameterId: Optional[str] = None
    dateTimeBefore: Optional[str] = None
    dateTimeStrictlyBefore: Optional[str] = None
    dateTimeAfter: Optional[str] = None
    dateTimeStrictlyAfter: Optional[str] = None
    orderDateTime: Optional[str] = None
    orderId: Optional[str] = None
    catalogItemIsModeled: Optional[bool] = None


def basemodel_to_query_string(model: BaseModel) -> str:
    filtered_params = model.model_dump(exclude_none=True)
    for k in PARAM_CONV.keys():
        if k in filtered_params:
            filtered_params[PARAM_CONV[k]] = filtered_params.pop(k)
    q_str = urlencode(filtered_params)
    if q_str != "":
        q_str = f"?{q_str}"
    return q_str


def make_get_req_to_rise(full_url: str, err_detail: str) -> dict | list:
    response = requests.get(full_url, headers=RISE_HEADERS)
    if response.status_code == 200:
        return response.json()
    else:
        raise HTTPException(status_code=404, detail=err_detail)


@api_router.get("/location/{id}")
async def get_loc_by_id(id: str):
    return make_get_req_to_rise(
        full_url=f"{EXT_RISE_BASE_URL}/location/{id}",
        err_detail="Location not found"
    )


@api_router.get("/location")
async def get_loc(query: LocItemParams = Depends()):
    query_url_portion = basemodel_to_query_string(query)
    return make_get_req_to_rise(
        full_url=f"{EXT_RISE_BASE_URL}/location{query_url_portion}",
        err_detail="Location(s) not found"
    )


@api_router.get("/catalog-item/{id}")
async def get_cat_item_by_id(id: str):
    return make_get_req_to_rise(
        full_url=f"{EXT_RISE_BASE_URL}/catalog-item/{id}",
        err_detail="Catalog item not found"
    )


@api_router.get("/catalog-item")
async def get_cat_item(query: CatItemParams = Depends()):
    query_url_portion = basemodel_to_query_string(query)
    return make_get_req_to_rise(
        full_url=f"{EXT_RISE_BASE_URL}/catalog-item{query_url_portion}",
        err_detail="Catalog item(s) not found"
    )


@api_router.get("/catalog-record/{id}")
async def get_cat_rec_by_id(id: str):
    return make_get_req_to_rise(
        full_url=f"{EXT_RISE_BASE_URL}/catalog-record/{id}",
        err_detail="Catalog record not found"
    )


@api_router.get("/catalog-record")
async def get_cat_rec(query: CatRecParams = Depends()):
    query_url_portion = basemodel_to_query_string(query)
    return make_get_req_to_rise(
        full_url=f"{EXT_RISE_BASE_URL}/catalog-record{query_url_portion}",
        err_detail="Catalog record(s) not found"
    )


@api_router.get("/result/{id}")
async def get_res_by_id(id: str):
    return make_get_req_to_rise(
        full_url=f"{EXT_RISE_BASE_URL}/result/{id}",
        err_detail="Result not found"
    )


@api_router.get("/result")
async def get_res(query: ResParams = Depends()):
    query_url_portion = basemodel_to_query_string(query)
    return make_get_req_to_rise(
        full_url=f"{EXT_RISE_BASE_URL}/result{query_url_portion}",
        err_detail="Result(s) not found"
    )

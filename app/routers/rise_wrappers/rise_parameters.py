from typing import Optional
from pydantic import BaseModel

# Conversion dict for python-incompatible names found in the RISE API
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


class CatItemParams(BaseModel):
    """Parameters for the catalog-item resource"""
    page: Optional[int] = 1
    itemsPerPage: Optional[int] = 25
    id: Optional[str] = None
    hasItemStructure: Optional[bool] = None


class CatRecParams(BaseModel):
    """Parameters for the catalog-record resource"""
    page: Optional[int] = 1
    itemsPerPage: Optional[int] = 25
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


class LocItemParams(BaseModel):
    """Parameters for the location resource"""
    page: Optional[int] = 1
    itemsPerPage: Optional[int] = 25
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


class ResParams(BaseModel):
    """Parameters for the result resource"""
    page: Optional[int] = 1
    itemsPerPage: Optional[int] = 25
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

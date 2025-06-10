import json
import os
import re

from fastapi import APIRouter, HTTPException, Path, Query
from fastapi.responses import Response
from pydantic import BaseModel

from icefabric_api.create_ipes import get_hydrofabric_attributes, module_ipe

api_router = APIRouter(prefix="/modules")
ROOT_DIR = os.path.abspath(os.curdir)


class Parameters(BaseModel):
    gage_id: str
    version: str
    source: str
    domain: str
    modules: list[str]


@api_router.post("/parameters")
async def get_ipes(
    query: Parameters
):
    if query.version != '2.1' and query.version != '2.2':
        raise HTTPException(status_code=422, detail="Icefabric version must be 2.2 or 2.1")
    elif query.version == '2.1' and query.domain != 'CONUS':
        raise HTTPException(status_code=422, detail="oCONUS domains not availiable in Icefabric version 2.1")

    gpkg_file = f"{ROOT_DIR}/data/gauge_{query.gage_id}.gpkg"
    if not os.path.exists(gpkg_file):
        raise HTTPException(status_code=422, detail="Gage ID/geopackage not found")

    # Get divide attributes from the geopackage
    attr = get_hydrofabric_attributes(gpkg_file, query.version, query.domain)

    all_modules = []
    for mod in query.modules:
        mod_json = module_ipe(mod, attr, query.domain, query.version)
        all_modules.append(mod_json)

    final_json = json.dumps({"modules": all_modules})
    final_json = re.sub("NaN", "null", final_json)

    return final_json

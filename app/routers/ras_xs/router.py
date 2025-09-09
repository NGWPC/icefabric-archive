import pathlib
import tempfile
import uuid

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, field_validator
from pyiceberg.catalog import load_catalog
from shapely.geometry import box
from starlette.background import BackgroundTask

from icefabric.ras_xs import subset_xs
from icefabric.schemas import XsType

api_router = APIRouter(prefix="/ras_xs")

NEW_MEXICO_BOUNDS = {
    "min_lat": {"summary": "New Mexico Bounds", "value": 31.3323},
    "min_lon": {"summary": "New Mexico Bounds", "value": -109.0502},
    "max_lat": {"summary": "New Mexico Bounds", "value": 37.0002},
    "max_lon": {"summary": "New Mexico Bounds", "value": -103.0020},
}


class BoundingBox(BaseModel):
    """Pydantic representation of a lat/lon geospatial bounding box."""

    min_lat: float
    min_lon: float
    max_lat: float
    max_lon: float

    @field_validator("max_lat", "max_lon", mode="after")
    @classmethod
    def max_must_be_greater(cls, v, values):
        """Validation function to make sure that the max bounds values are less than min bounds values"""
        max_name, max_val = values.field_name, v
        min_name = f"min_{max_name[4:]}"
        min_val = values.data[min_name]
        if max_val <= min_val:
            raise ValueError(f"{max_name} must be greater than {min_name}")
        return max_val


def get_bbox_query_params(
    min_lat: float = Query(
        ...,
        description="The minimum latitudinal coordinate that defines the bounding box.",
        openapi_examples={"New Mexico Bounds Example": NEW_MEXICO_BOUNDS["min_lat"]},
    ),
    min_lon: float = Query(
        ...,
        description="The minimum longitudinal coordinate that defines the bounding box.",
        openapi_examples={"New Mexico Bounds Example": NEW_MEXICO_BOUNDS["min_lon"]},
    ),
    max_lat: float = Query(
        ...,
        description="The maximum latitudinal coordinate that defines the bounding box.",
        openapi_examples={"New Mexico Bounds Example": NEW_MEXICO_BOUNDS["max_lat"]},
    ),
    max_lon: float = Query(
        ...,
        description="The maximum longitudinal coordinate that defines the bounding box.",
        openapi_examples={"New Mexico Bounds Example": NEW_MEXICO_BOUNDS["max_lon"]},
    ),
) -> BoundingBox:
    """Dependency function that parses BoundingBox query parameters"""
    try:
        return BoundingBox(min_lat=min_lat, min_lon=min_lon, max_lat=max_lat, max_lon=max_lon)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=f"Bounding box validation failed - {e}") from e


def filesystem_check(tmp_path: pathlib.PosixPath):
    """Wraps temp file validations in a helper function"""
    if not tmp_path.exists():
        raise HTTPException(status_code=500, detail=f"Failed to create geopackage file at {tmp_path}.")
    if tmp_path.stat().st_size == 0:
        tmp_path.unlink(missing_ok=True)
        raise HTTPException(status_code=404, detail="No data found for subset attempt.")
    # Verify it's actually a file, not a directory
    if not tmp_path.is_file():
        raise HTTPException(status_code=500, detail=f"Expected file, but got directory at {tmp_path}.")


@api_router.get("/{identifier}/", tags=["HEC-RAS XS"])
async def get_xs_subset_gpkg(
    identifier: str = Path(
        ...,
        description="The flowpath ID from the reference hydrofabric that the current RAS XS aligns is conflated to",
        examples=["20059822"],
        openapi_examples={"huc": {"summary": "XS Example", "value": "20059822"}},
    ),
    schema_type: XsType = Query(
        XsType.CONFLATED, description="The schema type used to query the cross-sections"
    ),
):
    """
    Get geopackage subset from the HEC-RAS XS iceberg catalog by table identifier (aka flowpath ID).

    This endpoint will query cross-sections from the HEC-RAS XS iceberg catalog by flowpath ID & return
    the data subset as a downloadable geopackage file.

    """
    catalog = load_catalog("glue")
    unique_id = str(uuid.uuid4())[:8]
    temp_dir = pathlib.Path(tempfile.gettempdir())
    tmp_path = temp_dir / f"ras_xs_{identifier}_{unique_id}.gpkg"
    try:
        # Create data subset
        data_gdf = subset_xs(
            catalog=catalog, identifier=f"{identifier}", output_file=tmp_path, xstype=schema_type
        )

        filesystem_check(tmp_path=tmp_path)

        print(f"Returning file: {tmp_path} (size: {tmp_path.stat().st_size} bytes)")
        download_filename = f"ras_xs_{identifier}.gpkg"
        return FileResponse(
            path=str(tmp_path),
            filename=download_filename,
            media_type="application/geopackage+sqlite3",
            headers={
                "Data Source": f"ras_xs.{schema_type.value}",
                "Flowpath ID": identifier,
                "Description": f"RAS XS ({schema_type.value} schema) Geopackage",
                "Total Records": f"{len(data_gdf)}",
            },
            background=BackgroundTask(lambda: tmp_path.unlink(missing_ok=True)),
        )
    except HTTPException:
        raise
    except Exception:
        # Clean up temp file if it exists
        if "tmp_path" in locals() and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise


@api_router.get("/within", tags=["HEC-RAS XS"])
async def get_by_geospatial_query(
    bbox: BoundingBox = Depends(get_bbox_query_params),
    schema_type: XsType = Query(
        XsType.CONFLATED, description="The schema type used to query the cross-sections"
    ),
):
    """
    Get geopackage subset from a lat/lon bounding box geospatial query.

    This endpoint will query cross-sections from the HEC-RAS XS iceberg catalog by bounding box. All
    data selected will be within the bounding box. Returns the data subset as a downloadable
    geopackage file.
    """
    catalog = load_catalog("glue")
    unique_id = str(uuid.uuid4())[:8]
    temp_dir = pathlib.Path(tempfile.gettempdir())
    tmp_path = temp_dir / f"ras_xs_bbox_{unique_id}.gpkg"
    try:
        # Create data subset
        bbox = box(bbox.min_lat, bbox.min_lon, bbox.max_lat, bbox.max_lon)
        data_gdf = subset_xs(catalog=catalog, bbox=bbox, output_file=tmp_path, xstype=schema_type)

        filesystem_check(tmp_path=tmp_path)

        print(f"Returning file: {tmp_path} (size: {tmp_path.stat().st_size} bytes)")
        download_filename = f"ras_xs_{unique_id}.gpkg"
        return FileResponse(
            path=str(tmp_path),
            filename=download_filename,
            media_type="application/geopackage+sqlite3",
            headers={
                "Data Source": f"ras_xs.{schema_type.value}",
                "Bounding Box": str(bbox),
                "Description": f"RAS XS ({schema_type.value} schema) Geopackage",
                "Total Records": f"{len(data_gdf)}",
            },
            background=BackgroundTask(lambda: tmp_path.unlink(missing_ok=True)),
        )
    except HTTPException:
        raise
    except Exception:
        # Clean up temp file if it exists
        if "tmp_path" in locals() and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise

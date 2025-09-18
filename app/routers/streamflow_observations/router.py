import os
from datetime import datetime

import icechunk
import numpy as np
import polars as pl
import xarray as xr
from botocore.exceptions import ClientError
from fastapi import APIRouter, HTTPException, Path, Query
from fastapi.background import BackgroundTasks
from fastapi.responses import FileResponse, Response

from icefabric.cli.streamflow import TIME_FORMATS, NoResultsFoundError, streamflow_observations
from icefabric.schemas.hydrofabric import StreamflowDataSources

api_router = APIRouter(prefix="/streamflow_observations")

BUCKET = "edfs-data"
PREFIX = "streamflow_observations"


# Configuration for each data source
DATA_SOURCE_CONFIG = {
    StreamflowDataSources.USGS: {
        "repo": "usgs_observations",
        "description": "USGS stream gauge hourly data",
        "units": "cms",
    },
    StreamflowDataSources.ENVCA: {
        "repo": "envca_cadwr_txdot_observations",
        "description": "ENVCA stream gauge hourly data",
        "units": "cms",
    },
    StreamflowDataSources.CADWR: {
        "repo": "envca_cadwr_txdot_observations",
        "description": "CADWR stream gauge hourly data",
        "units": "cms",
    },
    StreamflowDataSources.TXDOT: {
        "repo": "envca_cadwr_txdot_observations",
        "description": "TXDOT stream gauge hourly data",
        "units": ["cms", "cms_denoted_3"],
    },
}


def cleanup(file_path: str):
    """Background task helper to cleanup CSV/Parquet files"""
    try:
        os.remove(file_path)
    except OSError:
        pass


def validate_datetime(date_string: str, start_or_end: str):
    """Validation helper function to make sure datetimes are in an acceptable format"""
    for fmt in TIME_FORMATS:
        try:
            datetime_obj = datetime.strptime(date_string, fmt)
            return datetime_obj
        except ValueError:
            continue
    raise ValueError(f"{start_or_end} must match one of the formats: {', '.join(TIME_FORMATS)}")


def integrate_time_range(sd: str, ed: str, filename_parts: list, command_args: list, file_ext: str):
    """Takes a time range (start/end times) and injects them into a filename/command args list"""
    if sd:
        sd = validate_datetime(sd, "start_date")
        filename_parts.append(f"from_{sd.strftime('%Y%m%d_%H%M')}")
        command_args.append("-s")
        command_args.append(str(sd))
    if ed:
        ed = validate_datetime(ed, "end_date")
        filename_parts.append(f"to_{ed.strftime('%Y%m%d_%H%M')}")
        command_args.append("-e")
        command_args.append(str(ed))
    filename = "_".join(filename_parts) + file_ext
    output_file = f"{os.getcwd()}/{filename}"
    command_args.append("-o")
    command_args.append(output_file)

    return command_args, output_file


def get_data_and_repo_hist(data_source: StreamflowDataSources):
    """Get repo/data from icechunk for a given data source"""
    # Determine IC store
    ic_store_type = "usgs" if data_source.value == "USGS" else "envca_cadwr_txdot"
    config = DATA_SOURCE_CONFIG[data_source]
    try:
        storage_config = icechunk.s3_storage(
            bucket=BUCKET, prefix=f"{PREFIX}/{ic_store_type}_observations", region="us-east-1", from_env=True
        )
        repo = icechunk.Repository.open(storage_config)
        session = repo.writable_session("main")
        ds = xr.open_zarr(session.store, consolidated=False)
    except ClientError as e:
        msg = "AWS Test account credentials expired. Can't access remote S3 Table"
        print(msg)
        raise e
    return ds, repo, config


def validate_identifier(data_source: StreamflowDataSources, identifier: str):
    """Check if identifier exists in the dataset"""
    ds, repo, config = get_data_and_repo_hist(data_source)
    if identifier not in ds.coords["id"]:
        raise HTTPException(
            status_code=404,
            detail=f"ID '{identifier}' not found in {data_source} dataset.",
        )
    return ds, repo, config


@api_router.get("/{data_source}/available", tags=["Streamflow Observations"])
async def get_available_identifiers(
    data_source: StreamflowDataSources = Path(..., description="Data source type"),
    limit: int = Query(100, description="Maximum number of IDs to return"),
):
    """
    Get list of available identifiers for a data source

    Examples
    --------
    GET /v1/streamflow_observations/USGS/available
    GET /v1/streamflow_observations/CADWR/available?limit=50
    """
    try:
        ds, _, config = get_data_and_repo_hist(data_source)
        ds = ds.drop_vars(["q_cms", "q_cms_denoted_3"])

        if data_source.value != "USGS":
            fds = ds.where((ds.gage_type == data_source.value).compute(), drop=True)
            df = fds.to_dataframe().reset_index()
            pl_df = pl.from_pandas(df)
            pl_df = pl_df.filter(pl.col("gage_type") == data_source.value)
            ids = np.unique(pl_df["id"]).tolist()
        else:
            ids = np.unique(ds.coords["id"]).tolist()

        return {
            "data_source": data_source.value,
            "description": config["description"],
            "total_identifiers": len(ids),
            "identifiers": sorted(ids)[:limit],
            "showing": min(limit, len(ids)),
            "units": config["units"],
        }
    except HTTPException as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@api_router.get("/{data_source}/csv", tags=["Streamflow Observations"])
async def get_data_csv(
    data_source: StreamflowDataSources = Path(..., description="Data source type"),
    identifier: str = Query(
        ...,
        description="Station/gauge ID",
        examples=["01010000"],
        openapi_examples={"station_example": {"summary": "USGS Gauge", "value": "01010000"}},
    ),
    start_date: str | None = Query(
        None,
        description="Start Date",
        openapi_examples={"sample_date": {"summary": "Sample Date", "value": "2021-12-31 14:00:00"}},
    ),
    end_date: str | None = Query(
        None,
        description="End Date",
        openapi_examples={"sample_date": {"summary": "Sample Date", "value": "2022-01-01 14:00:00"}},
    ),
    include_headers: bool = Query(True, description="Include CSV headers"),
):
    """
    Get data as CSV file for any data source

    Examples
    --------
    GET /v1/streamflow_observations/USGS/csv?identifier=01031500&include_headers=true
    GET /v1/streamflow_observations/ENVCA/csv?identifier=02GC002&start_date=2010&end_date=2011
    """
    try:
        # Construct filename and arg list for CSV construction
        filename_parts = [data_source.value, identifier, "data"]
        command_args = ["-d", data_source, "-g", identifier, "-h", include_headers]
        command_args, output_file = integrate_time_range(
            start_date, end_date, filename_parts, command_args, ".csv"
        )

        # Call click command to create CSV file
        total_records = streamflow_observations(command_args, standalone_mode=False)

        # Generate response
        response = FileResponse(
            output_file,
            media_type="text/csv",
            filename=output_file.split("/")[-1],
            headers={
                "Content-Disposition": f"attachment; filename={output_file.split('/')[-1]}",
                "X-Total-Records": str(total_records),
                "X-Data-Source": data_source.value,
                "X-Units": "cms",
            },
        )

        # Cleanup file after delivery
        background_tasks = BackgroundTasks()
        background_tasks.add_task(cleanup, output_file)
        response.background = background_tasks

        return response
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=422, detail=f"Date range validation failed - {e}") from e
    except NoResultsFoundError as e:
        raise HTTPException(status_code=404, detail=f"No records found - {e}") from e


@api_router.get("/{data_source}/parquet", tags=["Streamflow Observations"])
async def get_data_parquet(
    data_source: StreamflowDataSources = Path(..., description="Data source type"),
    identifier: str = Query(
        ...,
        description="Station/gauge ID",
        examples=["01010000"],
        openapi_examples={"station_example": {"summary": "USGS Gauge", "value": "01010000"}},
    ),
    start_date: str | None = Query(
        None,
        description="Start Date",
        openapi_examples={"sample_date": {"summary": "Sample Date", "value": "2021-12-31 14:00:00"}},
    ),
    end_date: str | None = Query(
        None,
        description="End Date",
        openapi_examples={"sample_date": {"summary": "Sample Date", "value": "2022-01-01 14:00:00"}},
    ),
):
    """
    Get data as Parquet file for any data source

    Examples
    --------
    GET /v1/streamflow_observations/TXDOT/parquet?identifier=08102730&start_date=2023-06&end_date=2023-11
    GET /v1/streamflow_observations/CADWR/parquet?identifier=JBR
    """
    try:
        # Construct filename and arg list for CSV construction
        filename_parts = [data_source.value, identifier, "data"]
        command_args = ["-d", data_source, "-g", identifier]
        command_args, output_file = integrate_time_range(
            start_date, end_date, filename_parts, command_args, ".parquet"
        )

        # Call click command to create CSV file
        total_records = streamflow_observations(command_args, standalone_mode=False)

        # Generate response
        response = FileResponse(
            output_file,
            media_type="application/vnd.apache.parquet",
            filename=output_file.split("/")[-1],
            headers={
                "Content-Disposition": f"attachment; filename={output_file.split('/')[-1]}",
                "X-Total-Records": str(total_records),
                "X-Data-Source": data_source.value,
                "X-Compression": "lz4",
                "X-Units": "cms",
            },
        )

        # Cleanup file after delivery
        background_tasks = BackgroundTasks()
        background_tasks.add_task(cleanup, output_file)
        response.background = background_tasks

        return response
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=422, detail=f"Date range validation failed - {e}") from e
    except NoResultsFoundError as e:
        raise HTTPException(status_code=404, detail=f"No records found - {e}") from e


@api_router.get("/{data_source}/info", tags=["Streamflow Observations"])
async def get_data_source_info(
    data_source: StreamflowDataSources = Path(..., description="Data source type"),
):
    """
    Get information about dataset size and recommendations

    Examples
    --------
    GET /v1/streamflow_observations/USGS/info
    GET /v1/streamflow_observations/ENVCA/info
    GET /v1/streamflow_observations/CADWR/info
    GET /v1/streamflow_observations/TXDOT/info
    """
    try:
        _, repo, config = get_data_and_repo_hist(data_source)
        snapshots = []
        hist = repo.ancestry(branch="main")
        for ancestor in hist:
            snapshots.append(
                {
                    "snapshot_id": ancestor.id,
                    "commit_message": ancestor.message,
                    "timestamp": ancestor.written_at,
                }
            )

        return {
            "data_source": data_source.value,
            "latest_snapshot": snapshots[0]["snapshot_id"],
            "description": config["description"],
            "units": config["units"],
            "snapshots": snapshots,
        }
    except HTTPException as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@api_router.get("/{data_source}/{identifier}/info", tags=["Streamflow Observations"])
async def get_data_info(
    data_source: StreamflowDataSources = Path(..., description="Data source type"),
    identifier: str = Path(
        ...,
        description="Station/gauge ID",
        examples=["01010000"],
        openapi_examples={"station_example": {"summary": "USGS Gauge", "value": "01010000"}},
    ),
):
    """
    Get information about dataset size and recommendations

    Examples
    --------
    GET /v1/streamflow_observations/USGS/01010000/info
    GET /v1/streamflow_observations/CADWR/JBR/info
    GET /v1/streamflow_observations/TXDOT/08102730/info
    """
    try:
        ds, repo, config = validate_identifier(data_source, identifier)
        ds = ds.sel(id=identifier)
        df = ds.to_dataframe().reset_index()
        pl_df = pl.from_pandas(df)
        pl_df = pl_df.filter(pl.col("gage_type") == data_source)
        pl_df = pl_df.filter(pl.col("id") == identifier)
        df_clean = pl_df.drop_nulls(subset=["q_cms"])

        return {
            "data_source": data_source.value,
            "identifier": identifier,
            "description": config["description"],
            "total_records": len(df_clean),
            "units": config["units"],
            "date_range": {
                "start": df_clean["time"].min().isoformat() if not df_clean.is_empty() else None,
                "end": df_clean["time"].max().isoformat() if not df_clean.is_empty() else None,
            },
            "estimated_sizes": {
                "csv_mb": round(len(df_clean) * 25 / 1024 / 1024, 2),
                "parquet_mb": round(len(df_clean) * 8 / 1024 / 1024, 2),
            },
        }
    except HTTPException:
        raise
    except ValueError as e:
        Response(content=f"Error: {str(e)}", status_code=500, media_type="text/plain")


@api_router.get("/sources", tags=["Streamflow Observations"])
async def get_available_sources():
    """
    Get list of all available data sources

    Examples
    --------
    GET /v1/streamflow_observations/data/sources
    """
    sources = []
    for source, config in DATA_SOURCE_CONFIG.items():
        sources.append(
            {
                "name": source.value,
                "description": config["description"],
                "repo": config["repo"],
                "units": config["units"],
            }
        )

    return {"available_sources": sources, "total_sources": len(sources)}

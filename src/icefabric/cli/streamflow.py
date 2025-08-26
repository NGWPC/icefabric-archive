"""Contains all click CLI code for NWM modules"""

import click
import icechunk
import polars as pl
import xarray as xr
from dotenv import load_dotenv

from icefabric.schemas.hydrofabric import StreamflowDataSources

load_dotenv()

BUCKET = "edfs-data"
PREFIX = "streamflow_observations"
TIME_FORMATS = ["%Y", "%Y-%m%Y-%m-%d", "%Y-%m-%d %H", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"]


@click.command()
@click.option(
    "--data-source",
    type=click.Choice([module.value for module in StreamflowDataSources], case_sensitive=False),
    help="The data source for the USGS gage id",
)
@click.option(
    "--gage-id",
    type=str,
    help="The Gauge ID you want the hourly streamflow data from",
)
@click.option(
    "--start-date",
    type=click.DateTime(formats=TIME_FORMATS),
    help="Start of the hourly streamflow timestamp range to be included",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=TIME_FORMATS),
    help="End of the hourly streamflow timestamp range to be included",
)
def streamflow_observations(data_source: str, gage_id: str, start_date: str, end_date: str):
    """Generates a CSV file containing the hourly streamflow data for a specific gage ID"""
    ic_store_type = "usgs" if data_source == "USGS" else "envca_cadwr_txdot"
    storage_config = icechunk.s3_storage(
        bucket=BUCKET, prefix=f"{PREFIX}/{ic_store_type}_observations", region="us-east-1", from_env=True
    )
    repo = icechunk.Repository.open(storage_config)
    session = repo.writable_session("main")

    ds = xr.open_zarr(session.store, consolidated=False)
    if ic_store_type.lower() == "usgs":
        ds = ds.sel(time=slice(start_date, end_date), id=int(gage_id))
    else:
        ds = ds.sel(time=slice(start_date, end_date), id=gage_id)
    df = ds.to_dataframe().reset_index()
    pl_df = pl.from_pandas(df)
    has_q_cms_denoted_3_vals = (~pl_df["q_cms_denoted_3"].is_null()).any()
    pl_df = pl_df.filter(pl.col("gage_type") == data_source)
    if ic_store_type.lower() == "usgs":
        pl_df = pl_df.filter(pl.col("id") == int(gage_id))
    else:
        pl_df = pl_df.filter(pl.col("id") == gage_id)
    pl_df = pl_df.filter((pl.col("time") >= start_date) & (pl.col("time") <= end_date))
    pl_df = pl_df.drop_nulls(subset=["q_cms"])
    if has_q_cms_denoted_3_vals:
        pl_df_reordered = pl_df.select(["time", "q_cms", "q_cms_denoted_3"])
    else:
        pl_df_reordered = pl_df.select(["time", "q_cms"])
    pl_df_reordered.write_csv(f"{gage_id}.csv")

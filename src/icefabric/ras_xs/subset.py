"""Script to feature any ras cross-section related tools"""

from pathlib import Path

import geopandas as gpd
import pandas as pd
from botocore.exceptions import ClientError
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import EqualTo

from icefabric.helpers.geopackage import to_geopandas


def subset_xs(
    catalog: Catalog, identifier: str, output_file: Path | None = None, **kwarg
) -> dict[str, pd.DataFrame | gpd.GeoDataFrame] | None:
    """Returns a geopackage subset from the mip xs iceberg catalog.

    This function delivers a subset of the mip cross-sections data by tracing from a
    given HUC identifier & collecting all relevant cross-section information based on
    additional identifiers (e.g. downstream reach ID) provided.

    Parameters
    ----------
    catalog : Catalog
        The mip xs iceberg catalog comprised of the ras cross-sections data tables.
    identifier : str
        The HUC identifier.
    output_file : Path, optional
        The output file path where the geopackage will be saved.

    Returns
    -------
    GeoDataFrame
        Subset of the mip cross-sections data based on identifiers.

    """
    ds_reach_id = kwarg.get("ds_reach_id", None)

    try:
        xs_table = catalog.load_table(f"mip_xs.{identifier}")

    except ClientError as e:
        msg = "AWS Test account credentials expired. Can't access remote S3 endpoint"
        print(msg)
        raise e

    # By default, mip xs iceberg tables are referenced by huc
    df = xs_table.scan().to_pandas()

    if ds_reach_id:
        filter_cond = EqualTo("ds_reach_id", ds_reach_id)
        df = xs_table.scan(row_filter=filter_cond).to_pandas()
    data_gdf = to_geopandas(df)

    # Save data.
    if output_file:
        if len(data_gdf) > 0:
            gpd.GeoDataFrame(data_gdf).to_file(output_file, layer="ras_xs", driver="GPKG")
        else:
            print("Warning: Dataframe is empty")
        return data_gdf
    else:
        return

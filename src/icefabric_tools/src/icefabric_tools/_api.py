"""Contains all api functions that can be called outside of the icefabric_tools package"""

import geopandas as gpd
from pyiceberg.table import Table
from shapely import wkb


def to_geopandas(
    table: Table,
    row_filter: str | None = "",
    case_sensitive: bool | None = True,
    snapshot_id: int | None = None,
    limit: int | None = None
) -> gpd.DataFrame:
    """Converts a table to a geopandas dataframe

    Parameters
    ----------
    table : Table
        The iceberg table you are trying to read from
    row_filter : str | None, optional
        A string or BooleanExpression that describes the desired rows, by default ""
    case_sensitive : bool | None, optional
        If True column matching is case sensitive, by default True
    snapshot_id : int | None, optional
        Optional Snapshot ID to time travel to.
        If None, scans the table as of the current snapshot ID, by default None
    limit : int | None, optional
        An integer representing the number of rows to return in the scan result.
        If None, fetches all matching rows., by default None

    Returns
    -------
    gpd.DataFrame
        The resulting queried row, but in a geodataframe

    Raises
    ------
    ValueError
        Raised if the table does not have a geometry column
    """
    df = table.scan(
        row_filter=row_filter,
        case_sensitive=case_sensitive,
        snapshot_id=snapshot_id,
        limit=limit,
    ).to_pandas()
    if "geometry" in df.columns:
        df["geometry"] = df["geometry"].apply(lambda x: wkb.loads(x) if x is not None else None)
        return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:5070")
    else:
        raise ValueError("The provided table does not have a geometery column.")
    return df

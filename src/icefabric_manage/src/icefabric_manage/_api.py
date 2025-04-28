"""Contains all api functions that can be called outside of the icefabric_manage package"""

from pathlib import Path

import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table


def _add_parquet_to_catalog(catalog: Catalog, file_path: Path, table_name: str):
    """Adding a parquet file to the hydrofabric catalog

    Parameters
    ----------
    catalog : Catalog
        A PyIceberg catalog
    file_path : Path
        The path to the parquet file
    table_name : str
        The table name that's wanted

    Returns
    -------
    Table
        A PyIceberg table

    Raises
    ------
    FileNotFoundError
        The parquet file given doesn't exist
    """
    if file_path.exists():
        arrow_table = pq.read_table(file_path)
        iceberg_table = catalog.create_table(
            f"hydrofabric.{table_name}",
            schema=arrow_table.schema,
        )
        iceberg_table.append(arrow_table)
    else:
        raise FileNotFoundError(f"Cannot find file: {file_path}")


def build(file_dir: Path, catalog_settings: dict[str, str]) -> Catalog:
    """Builds the hydrofabric catalog based on the .pyiceberg.yaml config and defined parquet files.

    Parameters
    ----------
    file_dir : Path
        The path to the parquet files to add into the iceberg catalog
    catalog_settings : dict[str, str]
        The settings for the catalog entry
    """
    catalog = load_catalog("hydrofabric", **catalog_settings)

    if not any(ns == ('hydrofabric',) for ns in catalog.list_namespaces()):
        catalog.create_namespace('hydrofabric')
        print("Created 'hydrofabric' namespace")

    parquet_files = list(file_dir.glob("*.parquet"))

    for parquet_file in parquet_files:
        table_name = parquet_file.stem  # Get filename without extension
        if catalog.table_exists(f"hydrofabric.{table_name}"):
            print(f"Table {table_name} already exists. Skipping build")
        else:
            _add_parquet_to_catalog(catalog, parquet_file, table_name)
    return catalog


def load_table_from_catalog(catalog: Catalog, identifier: str) -> Table:
    """_summary_

    Parameters
    ----------
    catalog : Catalog
        The Iceberg catalog
    identifier : str
        The catalog name and table. Ex: hydrofabric.network

    Returns
    -------
    Table
        The requested table

    Raises
    ------
    NoSuchTableError
        The table does not exist in the catalog
    """
    try:
        return catalog.load_table(identifier)
    except NoSuchTableError as e:
        raise NoSuchTableError from e

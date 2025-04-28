"""Contains all api functions that can be called outside of the icefabric_manage package"""

from pyiceberg.catalog import Catalog, load_catalog, load_table
from pyiceberg.excpetions import NoSuchTableError
from pyiceberg.table import Table
import pyarrow.parquet as pq
from pathlib import Path

def _add_parquet_to_catalog(catalog: Catalog, file_path: Path, table_name: str) -> Table:
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
        return iceberg_table
    else:
        raise FileNotFoundError(f"Cannot find file: {file_path}")

def build(file_dir: Path):
    catalog = load_catalog("hydrofabric")
    parquet_files = list(file_dir.glob("*.parquet"))

    tables = {}
    for parquet_file in parquet_files:
        table_name = parquet_file.stem  # Get filename without extension
        tables[table_name] = _add_parquet_to_catalog(catalog, parquet_file, table_name)

def load_table_from_catalog(identifier: str) -> Table:
    try:
        return load_table(identifier)
    except NoSuchTableError as e:
        raise NoSuchTableError from e

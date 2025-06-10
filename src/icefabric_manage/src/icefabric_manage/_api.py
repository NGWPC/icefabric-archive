"""Contains all api functions that can be called outside of the icefabric_manage package"""

import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog


def build(catalog: Catalog, parquet_file: str, namespace: str, table_name: str, location: str) -> None:
    """Builds the hydrofabric catalog based on the .pyiceberg.yaml config and defined parquet files.

    Parameters
    ----------
    catalog: Catalog
        The Apache Iceberg Catalog
    file_dir : Path
        The path to the parquet files to add into the iceberg catalog
    """
    if catalog.table_exists(f"{namespace}.{table_name}"):
        print(f"Table {table_name} already exists. Skipping build")
    else:
        arrow_table = pq.read_table(parquet_file)
        iceberg_table = catalog.create_table(
            f"{namespace}.{table_name}",
            schema=arrow_table.schema,
            location=location,
        )
        iceberg_table.append(arrow_table)

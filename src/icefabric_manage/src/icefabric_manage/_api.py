"""Contains all api functions that can be called outside of the icefabric_manage package"""

import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog


def build(catalog: Catalog, parquet_file: str, namespace: str, table_name: str) -> None:
    """Builds the hydrofabric catalog based on the .pyiceberg.yaml config and defined parquet files.

    Parameters
    ----------
    catalog: Catalog
        The Apache Iceberg Catalog
    file_dir : Path
        The path to the parquet files to add into the iceberg catalog
    """
    if not any(ns == (namespace,) for ns in catalog.list_namespaces()):
        catalog.create_namespace(namespace)
        print(f"Created {namespace} namespace")

    if catalog.table_exists(f"{namespace}.{table_name}"):
        print(f"Table {table_name} already exists. Skipping build")
    else:
        arrow_table = pq.read_table(parquet_file)
        iceberg_table = catalog.create_table(
            f"{namespace}.{table_name}",
            schema=arrow_table.schema,
        )
        iceberg_table.append(arrow_table)

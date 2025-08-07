import argparse
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import yaml
from pyiceberg.catalog import load_catalog
from pyiceberg.transforms import IdentityTransform

from icefabric.helpers import load_creds
from icefabric.schemas import (
    DivideAttributes,
    Divides,
    FlowpathAttributes,
    FlowpathAttributesML,
    Flowpaths,
    HydrofabricSnapshot,
    Hydrolocations,
    Lakes,
    Network,
    Nexus,
    POIs,
)

load_creds(dir=Path.cwd())

LOCATION = {"glue": "s3://edfs-data/icefabric_catalog"}


def clean_parquet_schema(parquet_file: str) -> pa.Table:
    """Clean parquet file to handle null types (ex: WaterbodyID and waterbody_nex_id)

    Parameters
    ----------
    parquet_file : str
        Path to the parquet file

    Returns
    -------
    pa.Table
        Cleaned Arrow table
    """
    # Read with pandas first to handle null types easily
    df = pd.read_parquet(parquet_file)
    null_columns = []
    for col in df.columns:
        if df[col].dtype == "object" and df[col].isna().all():
            df[col] = df[col].astype("string")
            null_columns.append(col)
    if null_columns:
        print(f"Converted null-type columns to string: {null_columns}")
    return pa.Table.from_pandas(df, preserve_index=False)


def build_hydrofabric(catalog_type: str, file_dir: str, domain: str):
    """Builds the hydrofabric Iceberg tables

    Parameters
    ----------
    catalog_type : str
        the type of catalog. sql is local, glue is production
    file_dir : str
        where the files are located
    domain : str
        the HF domain to be built
    """
    catalog = load_catalog(catalog_type)
    namespace = f"{domain}_hf"
    catalog.create_namespace_if_not_exists(namespace)
    layers = [
        ("divide-attributes", DivideAttributes),
        ("divides", Divides),
        ("flowpath-attributes-ml", FlowpathAttributesML),
        ("flowpath-attributes", FlowpathAttributes),
        ("flowpaths", Flowpaths),
        ("hydrolocations", Hydrolocations),
        ("lakes", Lakes),
        ("network", Network),
        ("nexus", Nexus),
        ("pois", POIs),
    ]
    snapshots = {}
    for layer, schema in layers:
        print(f"Building layer: {layer}")
        try:
            cleaned_table = clean_parquet_schema(f"{file_dir}/{layer}.parquet")
        except FileNotFoundError:
            print(f"Cannot find {layer} in the given file dir {file_dir}")
            continue
        schema = schema.schema()
        if catalog.table_exists(f"{namespace}.{layer}"):
            print(f"Table {layer} already exists. Skipping build")
            current_snapshot = catalog.load_table(f"{namespace}.{layer}").current_snapshot()
            snapshots[layer] = current_snapshot.snapshot_id
        else:
            iceberg_table = catalog.create_table(
                f"{namespace}.{layer}",
                schema=cleaned_table.schema,
                location=f"{LOCATION[catalog_type]}/{namespace.lower()}/{layer}",
            )
            partition_spec = iceberg_table.spec()
            if len(partition_spec.fields) == 0:
                with iceberg_table.update_spec() as update:
                    update.add_field("vpuid", IdentityTransform(), "vpuid_partition")
            iceberg_table.append(cleaned_table)
            current_snapshot = iceberg_table.current_snapshot()
            snapshots[layer] = current_snapshot.snapshot_id

    snapshot_namespace = f"{domain}_snapshots"
    catalog.create_namespace_if_not_exists(snapshot_namespace)
    tbl = catalog.create_table(f"{snapshot_namespace}.ids", schema=HydrofabricSnapshot.schema())
    df = pa.Table.from_pylist([snapshots], schema=HydrofabricSnapshot.arrow_schema())
    tbl.append(df)
    tbl.manage_snapshots().create_tag(tbl.current_snapshot().snapshot_id, "base").commit()
    print(f"Build complete. Files written into metadata store on {catalog.name} @ {namespace}")
    print(f"Snapshots written to: {snapshot_namespace}.ids")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build a PyIceberg catalog in the S3 endpoint for the Hydrofabric"
    )

    parser.add_argument(
        "--catalog",
        choices=["sql", "glue"],
        default="sql",
        help="Catalog type to use (default: sql for local build)",
    )
    parser.add_argument(
        "--files",
        type=Path,
        required=True,
        help="Path to the folder containing Hydrofabric parquet files",
    )
    parser.add_argument(
        "--domain",
        type=str,
        required=True,
        choices=["conus", "ak", "hi", "prvi", "gl"],
        help="The hydrofabric domain to be used for the namespace",
    )

    args = parser.parse_args()

    if args.catalog == "sql":
        with open(os.environ["PYICEBERG_HOME"]) as f:
            config = yaml.safe_load(f)
        warehouse = Path(config["catalog"]["sql"]["warehouse"].replace("file://", ""))
        warehouse.mkdir(parents=True, exist_ok=True)
        LOCATION["sql"] = config["catalog"]["sql"]["warehouse"]

    build_hydrofabric(catalog_type=args.catalog, file_dir=args.files, domain=args.domain)

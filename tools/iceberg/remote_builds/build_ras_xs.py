import argparse
from pathlib import Path

from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError

from icefabric.builds import build_iceberg_table

load_dotenv(Path.cwd())


def build_table(file: str):
    """Builds the FEMA MIP XS tables

    Parameters
    ----------
    file_dir : str
        The directory to hydrofabric parquet files
    """
    catalog = load_catalog("glue")  # Using an AWS Glue Endpoint
    namespace = "ras_xs"
    try:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        print(f"Namespace {namespace} already exists")
    print("Building XS table")
    build_iceberg_table(
        catalog=catalog,
        parquet_file=file,
        namespace=namespace,
        table_name="conflated",
        location="s3://edfs-data/icefabric_catalog/hydrofabric_conflated_xs",
    )
    print(f"Build successful. Files written into metadata store @ {catalog.name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A script to build a pyiceberg catalog in the S3 endpoint for the FEMA-BLE data"
    )

    parser.add_argument("--file", help="The local parquet file containing all processed cross sections")

    args = parser.parse_args()
    build_table(file=args.file)

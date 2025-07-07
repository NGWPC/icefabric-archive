import argparse

from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError

from icefabric.builds import build_iceberg_table

load_dotenv()


def build_table(file_dir: str):
    """Builds the hydrofabric namespace and tables

    Parameters
    ----------
    file_dir : str
        The directory to hydrofabric parquet files
    """
    catalog = load_catalog("glue")  # Using an AWS Glue Endpoint
    namespace = "hydrofabric"
    try:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        print(f"Namespace {namespace} already exists")
    layers = [
        "divide-attributes",
        "divides",
        "flowpath-attributes-ml",
        "flowpath-attributes",
        "flowpaths",
        "hydrolocations",
        "lakes",
        "network",
        "nexus",
        "pois",
    ]
    for layer in layers:
        print(f"building layer: {layer}")
        build_iceberg_table(
            catalog=catalog,
            parquet_file=f"{file_dir}/{layer}.parquet",
            namespace=namespace,
            table_name=layer,
            location="s3://fim-services-data-test/icefabric_metadata/",
        )
    print(f"Build successful. Files written into metadata store @ {catalog.name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A script to build a pyiceberg catalog in the S3 endpoint")

    parser.add_argument("--files", help="The local file dir where the files are located")

    args = parser.parse_args()
    build_table(file_dir=args.files)

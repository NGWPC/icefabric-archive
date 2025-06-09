import argparse

from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError

from icefabric_manage import build

load_dotenv()


def build_table(file_dir):
    catalog = load_catalog("glue")  # Using an AWS Glue Endpoint
    namespace = "streamflow_observations"
    try:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        print(f"Namespace {namespace} already exists")
    build(
        catalog=catalog,
        parquet_file=f"{file_dir}/usgs_hourly.parquet",
        namespace=namespace,
        table_name="usgs_hourly",
        location="s3://fim-services-data-test/icefabric_metadata/",
    )
    print(f"Build successful. Files written into metadata store @ {catalog.name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A script to build a pyiceberg catalog in the S3 endpoint")

    parser.add_argument("--files", help="The local file dir where the files are located")

    args = parser.parse_args()
    build_table(file_dir=args.files)

This folder is set up to build remote iceberg catalogs to the S3 endpoints. The following template can be used to easily create a build script:

The following variables must always be set when building

```python
import argparse

from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError

from icefabric_manage import build

load_dotenv()

def build_table(file_dir: str):
    """Builds the hydrofabric namespace and tables

    Parameters
    ----------
    file_dir : str
        The directory to hydrofabric parquet files
    """
    catalog = load_catalog("glue")  # Using an AWS Glue Endpoint
    namespace = "<INSERT NAMESPACE HERE>"
    .
    .
    .
    build(
        catalog=catalog,
        parquet_file=f"{file_dir}/{layer}.parquet",
        namespace=namespace,
        table_name=layer,
        location="s3://fim-services-data-test/icefabric_metadata/"
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A script to build a pyiceberg catalog in the S3 endpoint"
    )

    parser.add_argument("--files", help="The local file dir where the files are located")

    args = parser.parse_args()
    build_table(file_dir=args.files)

```

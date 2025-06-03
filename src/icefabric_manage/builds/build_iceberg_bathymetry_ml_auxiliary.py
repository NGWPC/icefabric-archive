from pathlib import Path

from dotenv import load_dotenv
from pyiceberg.exceptions import NamespaceAlreadyExistsError

from icefabric_manage import IcebergTable

load_dotenv()

if __name__ == "__main__":
    warehouse_path = Path("/tmp/warehouse")
    warehouse_path.mkdir(exist_ok=True)
    catalog_name = "pyiceberg_catalog"
    catalog_settings = {
        "type": "sql",
        "uri": f"sqlite:///{warehouse_path}/{catalog_name}.db",
        "warehouse": f"file://{warehouse_path}",
    }

    iceberg_table = IcebergTable()

    try:
        # Establish new Iceberg catalog
        iceberg_table.establish_catalog(
            catalog_name=catalog_name,
            namespace="bathymetry_ml_auxiliary",
            catalog_settings=catalog_settings,
        )

        # Generate tables w/in the new Iceberg catalog for all parquets detected in s3
        iceberg_table.create_table_for_all_s3parquets(
            app_name="bathymetry_ml_auxiliary", bucket_name="ngwpc-bathymetry"
        )

    except NamespaceAlreadyExistsError as e:
        raise FileExistsError(
            f"Iceberg table and namespace already exist. Remove catalog from {warehouse_path} to continue"
        ) from e

    print(f"Build successful. Files written into metadata store @ {warehouse_path}")

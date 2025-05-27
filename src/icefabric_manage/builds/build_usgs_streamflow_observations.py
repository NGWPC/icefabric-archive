from pathlib import Path

from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog

from icefabric_manage import build

load_dotenv()

if __name__ == "__main__":
    warehouse_path = Path("/tmp/warehouse")
    warehouse_path.mkdir(exist_ok=True)
    catalog_settings = {
        "type": "sql",
        "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
        "warehouse": f"file://{warehouse_path}",
    }

    namespace = "observations"
    catalog = load_catalog(namespace, **catalog_settings)
    build(
        catalog=catalog,
        parquet_file="s3://hydrofabric-data/icefabric/observations/usgs_hourly.parquet",
        namespace=namespace,
        table_name="usgs_hourly",
    )
    print(f"Build successful. Files written into metadata store @ {warehouse_path}")

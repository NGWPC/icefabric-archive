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

    namespace = "hydrofabric"
    catalog = load_catalog(namespace, **catalog_settings)
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
        build(
            catalog=catalog,
            parquet_file=f"s3://hydrofabric-data/icefabric/hydrofabric/{layer}.parquet",
            namespace=namespace,
            table_name=layer,
        )
    print(f"Build successful. Files written into metadata store @ {warehouse_path}")

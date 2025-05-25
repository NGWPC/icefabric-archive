from pathlib import Path

from pyiceberg.catalog import load_catalog

from icefabric_manage import build

if __name__ == "__main__":
    warehouse_path = Path("../data/warehouse")
    warehouse_path.mkdir(exist_ok=True)
    catalog_settings = {
        "type": "sql",
        "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
        "warehouse": f"file://{warehouse_path}",
    }

    namespace = "hydrofabric"
    catalog = load_catalog(namespace, **catalog_settings)

    data_dir = Path("../data/hydrofabric/")
    if data_dir.exists() is False:
        raise FileNotFoundError("Cannot find parquet data dir")
    build(
        catalog=catalog,
        file_dir=data_dir,
        namespace=namespace
    )
    print("Build successful")

"""A file to build the landcover stores using Icechunk"""

from pathlib import Path

import icechunk as ic
import rioxarray as rxr
from icechunk.xarray import to_icechunk

from icefabric.helpers import load_creds

load_creds(dir=Path.cwd())

# def _build_remote_topobathy_stores(file: str, output_path: str) -> None:
#     path_parts = output_path[5:].split("/")
#     bucket = path_parts[0]
#     prefix = (
#         "/".join(path_parts[1:]) if len(path_parts) > 1 else ""
#     )  # Join all remaining parts as the prefix
#     storage = ic.s3_storage(bucket="my-bucket", prefix="my-prefix")
#     repo = ic.Repository.open_or_create(storage_config)
#     session = repo.writable_session("main")
#     if Path(file).exists() is False:
#         raise FileNotFoundError(f"Cannot find topobathy tif: {file}")
#     _ds = rxr.open_rasterio(file)
#     ds = _ds.to_dataset(name="elevation")
#     to_icechunk(ds, session)
#     snapshot = session.commit("Created initial topobathy file")
#     print(f"Dataset is uploaded. Commit: {snapshot}")


def _build_local_topobathy_stores(file: str, output_path: str) -> None:
    """Creates a landcover store based on the NLCD data

    Parameters
    ----------
    virtual_files : str
        The path to where the virtual files live
    output_path : _type_
        _description_
    """
    storage_config = ic.local_filesystem_storage(output_path)
    repo = ic.Repository.open_or_create(storage_config)
    session = repo.writable_session("main")
    if Path(file).exists() is False:
        raise FileNotFoundError(f"Cannot find topobathy tif: {file}")
    _ds = rxr.open_rasterio(file)
    ds = _ds.to_dataset(name="elevation")
    to_icechunk(ds, session)
    snapshot = session.commit("Created initial topobathy file")
    print(f"Dataset is uploaded. Commit: {snapshot}")


if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="A script to build a topobathy tifs")

    # parser.add_argument(
    #     "file", type=str, help="The path to the file we're writing to icechunk"
    # )
    # parser.add_argument("output_path", type=str, help="The Path to where the repo should be created")

    # args = parser.parse_args()
    # if "s3://" in args.output_path:
    #     _build_remote_topobathy_stores(args.file, args.output_path)
    # else:
    #     _build_local_topobathy_stores(args.file, args.output_path)
    file = "/Users/taddbindas/data/icefabric/topobathy_tifs/tbdem_conus_atlantic_gulf_30m.tif"
    output_path = (
        "/Users/taddbindas/projects/NGWPC/icefabric/tests/data/topo_tifs/tbdem_conus_atlantic_gulf_30m"
    )
    _build_local_topobathy_stores(file, output_path)

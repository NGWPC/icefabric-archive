import os
from pathlib import Path

import numpy as np
import pytest
import rasterio
from dotenv import load_dotenv

from icefabric.builds import IcechunkRepo
from icefabric.schemas import NGWPCTestLocations

load_dotenv()

ic_rasters = [pytest.param(f, id=f) for f in NGWPCTestLocations._member_names_ if "TOPO" in f]


@pytest.mark.parametrize("ic_raster", ic_rasters)
@pytest.mark.slow
def test_topobathy(ic_raster: str) -> None:
    """This test is SLOW. It will temporarily download all topobathy layers, up to 9 GB individually.
    To run, call `pytest tests --run-slow`
    Corrupted rasters will load correctly in xarray but incorrectly in rasterio and cannot be exported.
    This test checks that when exported, a dataset has values that are non-no data.
    """
    data_dir = Path(__file__).parent / "data"
    os.makedirs(data_dir, exist_ok=True)

    temp_path = data_dir / "temp_raster.tif"

    try:
        # export icechunk zarr to geotiff raster
        repo = IcechunkRepo(location=NGWPCTestLocations[ic_raster].path)
        ds = repo.retrieve_dataset()
        raster = ds.elevation
        raster.rio.to_raster(temp_path, tiled=True, compress="LZW", bigtiff="YES")

        # open raster version
        with rasterio.open(temp_path, "r") as f:
            profile = f.profile
            ras = f.read(1)

        # assert all values are not nodata
        total_nd = np.where(ras == profile["nodata"], 1, 0).sum()
        assert total_nd != ras.size
        assert ras.min() != ras.max()

    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

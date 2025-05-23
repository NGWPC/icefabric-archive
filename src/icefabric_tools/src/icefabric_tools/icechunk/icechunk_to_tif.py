from pathlib import Path
from typing import Any

from icefabric_tools.icechunk import NGWPCLocations, get_icechunk_data


def icechunk_to_tiff(
    src: NGWPCLocations,
    dest: Path | str,
    var_name: str = None,
    compress: str = "lzw",
    profile_kwargs: dict[Any, Any] = {"tiled": True},
) -> None:
    """Export a layer from an icechunk repo to a GeoTIF.
    Defaults to a compressed, cloud-optimized geotiff via specifying compression and tiling.

    Parameters
    ----------
    src
       Source icechunk dataset file path
    dest
        Destination TIF; s3 or local path
    var_name, optional
        Name of xarray variable. If unspecified, first data array will be taken
    compress, optional
        Compresion for tif, by default "lzw"
    profile_kwargs, optional
        Any GDAL geotiff driver profile keywords supported by GDAL geotiff driver (https://gdal.org/en/stable/drivers/raster/gtiff.html#creation-options)
        Note for tiffs over 4 GB will be BIGTIFF
        by default {"tiled": True}
    """
    ds = get_icechunk_data(src)
    ds[var_name].rio.to_raster(dest, compress, **profile_kwargs)

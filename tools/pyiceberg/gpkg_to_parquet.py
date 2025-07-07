"""A simple script to convert the v2.2 hydrofabric to parquet"""

import argparse

import fiona
import geopandas as gpd
from dotenv import load_dotenv

load_dotenv()


def gpkg_to_parquet(input_file: str) -> None:
    """A function to convert geopackages to parquet files

    Parameters
    ----------
    input_file : str
        the gpkg to convert
    """
    available_layers = fiona.listlayers(input_file)
    print(f"Layers in GeoPackage: {available_layers}")

    for layer in available_layers:
        gdf = gpd.read_file(input_file, layer=layer)
        output_file = f"../../data/{layer}.parquet"
        gdf.to_parquet(output_file)
        print(f"Converted layer '{layer}' to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A script to build a pyiceberg catalog in the S3 endpoint")

    parser.add_argument("--gpkg", help="The local gpkg to convert into a parquet file")

    args = parser.parse_args()
    gpkg_to_parquet(input_file=args.gpkg)

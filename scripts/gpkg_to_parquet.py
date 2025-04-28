"""A simple script to convert the v2.2 hydrofabric to parquet"""

import geopandas as gpd
import fiona

input_file = "data/conus_nextgen.gpkg"
available_layers = fiona.listlayers(input_file)
print(f"Layers in GeoPackage: {available_layers}")

for layer in available_layers:
    gdf = gpd.read_file(input_file, layer=layer)
    output_file = f"data/parquet/{layer}.parquet"
    gdf.to_parquet(output_file)
    print(f"Converted layer '{layer}' to {output_file}")

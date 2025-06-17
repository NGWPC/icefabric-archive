"""A simple script to convert the v2.2 hydrofabric to parquet"""

import fiona
import geopandas as gpd

#Location of Hydrofabric geopackage files organized by domain
input_file_root = '/home/daniel.cumpton/Hydrofabric/data/hydrofabric/v2.2/nextgen'

#Location of data/domain directories for output Parquet files
output_file_root = '/home/daniel.cumpton/icefabric'

input_file = {'CONUS':'CONUS/conus_nextgen.gpkg', 'GL':'CONUS/gl_nextgen_divide_attr.gpkg',
              'Alaska':'Alaska/ak_nextgen_workaround.gpkg', 'Hawaii':'Hawaii/hi_nextgen_workaround.gpkg',
              'Puerto_Rico':'Puerto_Rico/prvi_nextgen_workaround.gpkg'}

domains = ['CONUS', 'GL', 'Alaska', 'Hawaii', 'Puerto_Rico']

for domain in domains: 

    file = f"{input_file_root}/{input_file[domain]}"
    available_layers = fiona.listlayers(file)
    print(f"For {domain}, layers in GeoPackage: {available_layers}")

    for layer in available_layers:
        gdf = gpd.read_file(file, layer=layer)
        output_file = f"{output_file_root}/data/parquet/{domain}/{layer}.parquet"
        gdf.to_parquet(output_file)
        print(f"For {domain}, converted layer '{layer}' to {output_file}")

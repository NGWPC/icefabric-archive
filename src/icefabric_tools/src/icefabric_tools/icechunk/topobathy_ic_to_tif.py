import os
from icefabric_tools.icechunk import IcechunkS3Repo, NGWPCLocations

from dotenv import load_dotenv

load_dotenv()


def convert_topobathy_to_tiff(output_dir: str, ic_rasters: list[NGWPCLocations]) -> None:
    for ic_raster in ic_rasters:
        try:
            repo = IcechunkS3Repo(location=NGWPCLocations[ic_raster].path)
        except Exception as e:
            raise e
        print(repo)
        output = os.path.join(output_dir, f"{str.split(str(NGWPCLocations[ic_raster].path), "/")[-1]}.tif")

        repo.retrieve_and_convert_to_tif(dest=output, var_name='elevation')


if __name__ == '__main__':
    output_dir = "./temp_tb_data"
    os.makedirs(output_dir, exist_ok=True)
    ic_rasters = [f for f in NGWPCLocations._member_names_ if "TOPO" and "30M" in f][2:3]

    convert_topobathy_to_tiff(output_dir, ic_rasters)

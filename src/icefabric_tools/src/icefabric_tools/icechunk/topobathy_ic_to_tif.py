import os

from dotenv import load_dotenv
from tqdm import tqdm

from icefabric_tools.icechunk import IcechunkS3Repo, NGWPCLocations

load_dotenv()


def convert_topobathy_to_tiff(output_dir: str, ic_rasters: list[NGWPCLocations]) -> None:
    """Converts topobathy layers from icechunk to tiff for use in tiles

    Parameters
    ----------
    output_dir : str
        Directory to save outputs to
    ic_rasters : list[NGWPCLocations]
        list of NGWPCLocation raster paths. eg. [NGWPCLocations[TOPO_AK_30M_IC].path]
    """
    for ic_raster in tqdm(ic_rasters, desc="Downloading IC Rasters to .tif"):
        repo = IcechunkS3Repo(location=NGWPCLocations[ic_raster].path)
        output = os.path.join(output_dir, f"{str.split(str(NGWPCLocations[ic_raster].path), '/')[-1]}.tif")

        repo.retrieve_and_convert_to_tif(dest=output, var_name="elevation")


if __name__ == "__main__":
    output_dir = "./temp_tb_data"
    os.makedirs(output_dir, exist_ok=True)

    # all 30 m topobathy layers
    ic_rasters = [f for f in NGWPCLocations._member_names_ if "TOPO" and "30M" in f]

    convert_topobathy_to_tiff(output_dir, ic_rasters)

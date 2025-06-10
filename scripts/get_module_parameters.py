"""A sample script to generate CFE IPEs"""

import geopandas as gpd
import pandas as pd

param_file = "../src/icefabric_api/data/cfe_params.csv"
gpkg_file = "../src/icefabric_tools/test/data/gages-08070000.gpkg"


divides = gpd.read_file(gpkg_file, layer="divides")
divides = divides["divide_id"].to_list()


module_params = pd.read_csv(param_file)
param_values = module_params[["name", "default_value"]]

for divide in divides:
    cfg_file = f"{divide}_bmi_cfg_cfe.txt"
    f = open(cfg_file, "x")

    for _, row in param_values.iterrows():
        key = row["name"]
        value = row["default_value"]
        f.write(f"{key}={value}\n")

    f.close()

params_calibratable = module_params.loc[module_params["calibratable"] == "TRUE"]
params_calibratable.to_json("out.json", orient="split")

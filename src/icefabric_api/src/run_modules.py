import pandas as pd
from hf_attributes import get_hydrofabric_attributes
import json
import re
import os

ROOT_DIR = os.path.abspath(os.curdir)


def module_ipe(module, attr, domain, version, cfg_write=None):
    # Create empty dataframe for csv parameters
    csv_params = pd.DataFrame()

    # Get list of divides
    divides = attr["divide_id"].to_list()

    # Look up parameter csv file for module
    has_output_vars = True
    module_df = pd.read_csv(f"{ROOT_DIR}/data/modules.csv")
    param_file = module_df.loc[module_df["module"] == module]["file"].to_string(index=False)
    out_file = module_df.loc[module_df["module"] == module]["outputs"].to_string(index=False)
    if out_file == "NaN":
        has_output_vars = False

    # Get module parameters
    datatypes = {
        "name": "object",
        "description": "object",
        "units": "object",
        "data_type": "object",
        "calibratable": "bool",
        "source": "object",
        "min": "float64",
        "max": "float64",
        "default_value": "object",
        "divide_attr_name": "object",
        "source_file": "object",
    }
    # Get output vars
    out_datatypes = {
        "variable": "object",
        "description": "object"
    }

    module_params = pd.read_csv(f"{ROOT_DIR}/data/{param_file}", dtype=datatypes)
    output_vars = pd.DataFrame()
    if has_output_vars:
        output_vars = pd.read_csv(f"{ROOT_DIR}/data/{out_file}", dtype=out_datatypes)

    # Filter data frame for parameter names and default values.  Create dictionary to collect
    # parameters for cfg files.
    param_values = module_params[["name", "default_value"]]
    param_values = pd.Series(param_values["default_value"].values, index=param_values["name"]).to_dict()

    # Get sources
    attr_list = module_params["source"].to_list()

    # Create dictionary with divide IDs as the keys and parameter dictionary for the values.
    all_cats = dict.fromkeys(divides, param_values)

    # For attributes, get list of attribute names
    attr_list = module_params.loc[module_params["source"] == "attr"]["divide_attr_name"].to_list()
    attr_list.append("divide_id")
    # Get corresponding list of parameter names to create a dictionary for mapping
    attr_names = module_params.loc[module_params["source"] == "attr"]["name"].to_list()
    attr_name_map = dict(zip(attr_list, attr_names, strict=False))
    # Filter attributes for those needed for this module
    attr = attr[attr_list]
    # Rename attribute names to module names
    attr.rename(columns=attr_name_map, inplace=True)

    # Get CSV file parameters
    if "csv" in module_params["source"].values:
        csv_file = module_params.loc[module_params["source"] == "csv"]["source_file"].to_list()
        csv_file = csv_file[0]
        csv_file = f"{csv_file}_{domain}_{version}.csv"
        csv_params = pd.read_csv(f"{ROOT_DIR}/data/{csv_file}")

    for divide in divides:
        attr_div = attr.loc[attr["divide_id"] == divide]
        attr_div = attr_div.to_dict(orient="list")
        all_cats[divide].update(attr_div)
        if not csv_params.empty:
            csv_params_div = csv_params.loc[csv_params["divide_id"] == divide]
            csv_params_div = csv_params_div.to_dict(orient="list")
            all_cats[divide].update(csv_params_div)
        # print(all_cats[divide])

    if cfg_write:
        write_cfg(all_cats)
    s3_uri = "s3://"
    module_json = write_json(all_cats[divides[1]], module_params, output_vars, module, s3_uri)
    return module_json


def write_cfg(params):
    divides = params.keys()
    for divide in divides:
        cfg_file = f"{divide}_bmi_config_cfe.txt"
        f = open(cfg_file, "w")

        for key, value in params[divide].items():
            f.write(f"{key}={value}\n")

    f.close()


def write_json(params, all_params, output_vars, module, s3_uri):
    json_columns = ["name", "description", "default_value", "min", "max", "data_type", "units"]
    all_params = all_params.loc[all_params["calibratable"] == True, json_columns]
    all_params = all_params.to_dict(orient="records")

    for item in all_params:
        name = item["name"]
        value = params[name]
        item["default_value"] = value

    module_dict = {
        "module_name": module,
        "parameter_file": {"uri": s3_uri},
        "calibrate_parameters": all_params,
    }
    if not output_vars.empty:
        output_vars = output_vars.to_dict(orient="records")
        module_dict.update({"output_variables": output_vars})

    return module_dict
